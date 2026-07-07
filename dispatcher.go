package augusta

import (
	"context"
	"io"
	"log"
	"sync"
	"time"

	"github.com/knightfall22/augusta/internal"
	pb "github.com/knightfall22/augusta/internal/api/v1"
	"github.com/knightfall22/augusta/internal/domain"
	"github.com/sirupsen/logrus"
)

type SessionStore interface {
	SetWorkerSession(workerID string, session *WorkerSession) (bool, error)
	GetWorkerSession(workerID string) (*WorkerSession, error)
	UpdateWorkerSession(workerID string) error
	DeleteWorkerSession(workerID string) error
	GetAllSessions() []*WorkerSession
	DiscardFaultyWorkers() error
}

type SchedulerAlgorithm interface {
	SelectCandidateWorkers(t []*domain.Task, workers []*WorkerSession) []*WorkerSession
	Score(t []*domain.Task, workers []*WorkerSession) map[string]float64
	Pick(scores map[string]float64, candidates []*WorkerSession) *WorkerSession
}

type Dispatcher struct {
	Store internal.StorageEngine

	WorkerStore SessionStore

	Scheduler SchedulerAlgorithm

	faultyWorkerTimeout time.Duration

	wg      *sync.WaitGroup
	logger  *logrus.Entry
	timeout int
	done    chan struct{}
	pb.UnimplementedSchedulerServiceServer
}

type DispatcherOption struct {
	Store               internal.StorageEngine
	WorkerStore         SessionStore
	Scheduler           SchedulerAlgorithm
	Logger              *logrus.Entry
	Timeout             int
	Wg                  *sync.WaitGroup
	FaultyWorkerTimeout time.Duration
}

func NewDispatcher(opts DispatcherOption) *Dispatcher {

	if opts.Timeout == 0 {
		opts.Timeout = 5
	}

	if opts.FaultyWorkerTimeout == 0 {
		opts.FaultyWorkerTimeout = 10
	}

	return &Dispatcher{
		Store:               opts.Store,
		timeout:             opts.Timeout,
		done:                make(chan struct{}, 1),
		logger:              opts.Logger,
		wg:                  opts.Wg,
		faultyWorkerTimeout: opts.FaultyWorkerTimeout,
		WorkerStore:         NewWorkerSessionStore(0),
		Scheduler:           &RoundRobin{Name: "RoundRobin"},
	}
}
func (p *Dispatcher) Run(ctx context.Context) {
	p.run(ctx)
	p.reaper(ctx)
	p.reapDeadWorkers(ctx)
}

func (p *Dispatcher) run(ctx context.Context) {
	logger := p.logger.WithContext(ctx).WithField("method", "run")

	p.wg.Go(func() {
		for {
			select {
			case <-time.After(time.Duration(p.timeout) * time.Second):
				tasks, err := p.Store.GetPendingTasks(ctx)
				if err != nil {
					logger.Error(err)
					return
				}

				p.dispatch(ctx, tasks)

			case <-ctx.Done():
				logger.Info("Context Cancelled Stopping dispatcher")
				return
			case <-p.done:
				logger.Info("Dispatcher Stopped")
				return

			}
		}
	})
}

func (p *Dispatcher) reaper(ctx context.Context) {
	logger := p.logger.WithContext(ctx).WithField("method", "reaper")
	reaperTimeout := time.Duration((p.timeout * 2)) * time.Second

	p.wg.Go(func() {
		for {
			select {
			case <-time.After(reaperTimeout):
				if err := p.Store.GetLeaseExpiredTasks(ctx); err != nil {
					logger.Error(err)
					return
				}
			case <-ctx.Done():
				logger.Info("Context Cancelled Stopping reaper")
				return
			}
		}
	})
}

func (p *Dispatcher) reapDeadWorkers(ctx context.Context) {
	logger := p.logger.WithContext(ctx).WithField("method", "reapDeadWorkers")

	p.wg.Go(func() {
		for {
			select {
			case <-time.After(p.faultyWorkerTimeout):
				if err := p.WorkerStore.DiscardFaultyWorkers(); err != nil {
					logger.Error(err)
					return
				}
			case <-ctx.Done():
				logger.Info("Context Cancelled Stopping reaper")
				return
			}
		}
	})
}

func (p *Dispatcher) dispatch(ctx context.Context, tasks []*domain.Task) {
	if len(tasks) > 0 {
		workers := p.WorkerStore.GetAllSessions()
		log.Println("Workers", workers)
		if len(workers) > 0 {
			//TODO: Might need to remove `SelectCandidateWorkers`
			workers = p.Scheduler.SelectCandidateWorkers(tasks, workers)
			scores := p.Scheduler.Score(tasks, workers)
			worker := p.Scheduler.Pick(scores, workers)

			p.logger.Debugf("worker selected: %s", worker.WorkerID)

			worker.taskChannel <- tasks
		}
	}
	p.logger.Debug("no workers available")
}

func (p *Dispatcher) dispatchListener(workerID string) {
	workerSession, err := p.WorkerStore.GetWorkerSession(workerID)
	if err != nil {
		return
	}

	p.logger.Infof("beginning listener for sending tasks to worker: %s", workerID)
	p.wg.Go(func() {
		for {
			select {
			case tasks := <-workerSession.taskChannel:
				if err := workerSession.Stream.Send(&pb.ServerMessage{
					Payload: &pb.ServerMessage_Tasks{
						Tasks: &pb.TaskBatch{
							Tasks: domain.SerializeTasksToProtobuf(tasks),
						},
					},
				}); err != nil {
					p.WorkerStore.DeleteWorkerSession(workerID)
					return
				}

			case <-workerSession.Stream.Context().Done():
				p.WorkerStore.DeleteWorkerSession(workerID)
				return

			case <-p.done:
				p.WorkerStore.DeleteWorkerSession(workerID)
				return
			}
		}
	})
}

func (p *Dispatcher) Stop() {
	p.done <- struct{}{}
}

// Meet grpc interface
func (p *Dispatcher) ConnectSession(stream pb.SchedulerService_ConnectSessionServer) error {
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		switch msg := message.Payload.(type) {
		case *pb.ClientMessage_Register:
			existPrev, err := p.registerWorker(msg, stream)
			if err != nil {
				return err
			}
			if err := stream.Send(&pb.ServerMessage{
				Payload: &pb.ServerMessage_Ack{},
			}); err != nil {
				p.logger.Errorf("failed to send ack: %v", err)
				return err
			}

			p.logger.Printf("registering worker[%v]", msg.Register.GetWorkerId())
			if !existPrev {
				p.dispatchListener(msg.Register.GetWorkerId())
			}
		case *pb.ClientMessage_TaskHeartbeat:
			activeTasks := msg.TaskHeartbeat.GetActiveTaskIds()

			if len(activeTasks) > 0 {
				p.Store.ExtendTaskLease(stream.Context(), activeTasks)
			}
		case *pb.ClientMessage_TaskResult:
			p.logger.Debugf("received task result from worker[%s]: %v", msg.TaskResult.WorkerId, msg.TaskResult)

			err := p.Store.ProcessTaskResult(stream.Context(), msg.TaskResult)
			if err != nil {
				p.logger.Errorf("error processing tasks %v", err)
				return err
			}
		case *pb.ClientMessage_TaskResultBatch:
			p.logger.Debugf("received result of %d tasks from worker[%s]", len(msg.TaskResultBatch.Results), msg.TaskResultBatch.WorkerId)
			err := p.Store.ProcessBatchTaskResult(stream.Context(), msg.TaskResultBatch.GetResults())
			if err != nil {
				p.logger.Errorf("error processing batch tasks %v", err)
				return err
			}
		}
	}
}

func (p *Dispatcher) registerWorker(msg *pb.ClientMessage_Register, stream pb.SchedulerService_ConnectSessionServer) (bool, error) {
	return p.WorkerStore.SetWorkerSession(msg.Register.GetWorkerId(), &WorkerSession{
		WorkerID:      msg.Register.GetWorkerId(),
		Tags:          msg.Register.GetSupportedTags(),
		taskChannel:   make(chan []*domain.Task, 100),
		Stream:        stream,
		LastHeartbeat: time.Now(),
	})

}
