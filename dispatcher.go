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

// Todo: refactor
type WorkerSession struct {
	WorkerID      string
	Tags          []string
	Stream        pb.SchedulerService_ConnectSessionServer
	taskChannel   chan []*domain.Task
	LastHeartbeat time.Time
}

type WorkerSessionStore struct {
	Workers map[string]*WorkerSession
	sync.RWMutex
}

func NewWorkerSessionStore() *WorkerSessionStore {
	return &WorkerSessionStore{
		Workers: make(map[string]*WorkerSession),
	}
}

func (w *WorkerSessionStore) GetWorkerSession(workerID string) *WorkerSession {
	w.RLock()
	defer w.RUnlock()
	return w.Workers[workerID]
}

func (w *WorkerSessionStore) SetWorkerSession(workerID string, session *WorkerSession) error {
	w.Lock()
	defer w.Unlock()
	if _, ok := w.Workers[workerID]; !ok {
		w.Workers[workerID] = session
	} else {
		w.Workers[workerID].LastHeartbeat = time.Now()
	}

	return nil
}

func (w *WorkerSessionStore) DeleteWorkerSession(workerID string) error {
	w.Lock()
	defer w.Unlock()
	delete(w.Workers, workerID)

	return nil
}

func (w *WorkerSessionStore) UpdateWorkerSession(workerID string) error {
	w.Lock()
	defer w.Unlock()

	if _, ok := w.Workers[workerID]; ok {
		w.Workers[workerID].LastHeartbeat = time.Now()
	}

	return nil
}

func (w *WorkerSessionStore) GetAllSessions() []*WorkerSession {
	if len(w.Workers) > 0 {
		result := make([]*WorkerSession, 0)

		for _, v := range w.Workers {
			result = append(result, v)
		}

		return result
	}

	return nil
}

type SchedulerAlgorithm interface {
	SelectCandidateWorkers(t []*domain.Task, workers []*WorkerSession) []*WorkerSession
	Score(t []*domain.Task, workers []*WorkerSession) map[string]float64
	Pick(scores map[string]float64, candidates []*WorkerSession) *WorkerSession
}

type Dispatcher struct {
	Store internal.StorageEngine

	WorkerStore *WorkerSessionStore

	Scheduler SchedulerAlgorithm

	wg      *sync.WaitGroup
	logger  *logrus.Entry
	timeout int
	done    chan struct{}
	pb.UnimplementedSchedulerServiceServer
}

func NewDispatcher(store internal.StorageEngine, timeout int, logger *logrus.Entry, wg *sync.WaitGroup) *Dispatcher {

	return &Dispatcher{
		Store:       store,
		timeout:     timeout,
		done:        make(chan struct{}, 1),
		logger:      logger,
		wg:          wg,
		WorkerStore: NewWorkerSessionStore(),
		Scheduler:   &RoundRobin{Name: "RoundRobin"},
	}
}
func (p *Dispatcher) Run(ctx context.Context) {
	p.run(ctx)
	p.reaper(ctx)
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

func (p *Dispatcher) dispatch(ctx context.Context, tasks []*domain.Task) {
	if len(tasks) > 0 {
		workers := p.WorkerStore.GetAllSessions()
		if len(workers) > 0 {
			//TODO: Might need to remove `SelectCandidateWorkers`
			workers = p.Scheduler.SelectCandidateWorkers(tasks, workers)
			scores := p.Scheduler.Score(tasks, workers)
			worker := p.Scheduler.Pick(scores, workers)

			worker.taskChannel <- tasks
		}
	}
}

func (p *Dispatcher) dispatchListener(workerID string) {
	workerSession := p.WorkerStore.GetWorkerSession(workerID)
	if workerSession == nil {
		return
	}

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
	log.Printf("HHHHHHHHHH")
	for {
		message, err := stream.Recv()
		if err == io.EOF {
			return nil
		}

		if err != nil {
			return err
		}

		// if message.Payload == &pb.ClientMessage_Register{} {

		// }
		switch msg := message.Payload.(type) {
		case *pb.ClientMessage_Register:
			if err := p.registerWorker(msg, stream); err != nil {
				return err
			}
			if err := stream.Send(&pb.ServerMessage{
				Payload: &pb.ServerMessage_Ack{},
			}); err != nil {
				return err
			}

			//Todo: Prevent multiple dispatching
			p.dispatchListener(msg.Register.GetWorkerId())
			log.Printf("[INFO] Registering worker[%v]\n", msg.Register.GetWorkerId())
		case *pb.ClientMessage_Heartbeat:
			if err := p.WorkerStore.UpdateWorkerSession(msg.Heartbeat.GetWorkerId()); err != nil {
				return err
			}
		case *pb.ClientMessage_Result:
			log.Printf("[INFO] Result %+v\n", msg.Result)
		}
	}
}

func (p *Dispatcher) registerWorker(msg *pb.ClientMessage_Register, stream pb.SchedulerService_ConnectSessionServer) error {
	if err := p.WorkerStore.SetWorkerSession(msg.Register.GetWorkerId(), &WorkerSession{
		WorkerID:      msg.Register.GetWorkerId(),
		Tags:          msg.Register.GetSupportedTags(),
		taskChannel:   make(chan []*domain.Task, 100),
		Stream:        stream,
		LastHeartbeat: time.Now(),
	}); err != nil {
		return err
	}

	return nil
}
