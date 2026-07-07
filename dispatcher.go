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

// Dispatcher is the central Control Plane of the Augusta scheduler.
// It acts as the bridge between three different worlds:
// 1. The persistent store where task state lives.
// 2. The in-memory network registry where live worker TCP sockets live.
// 3. The scheduling algorithm that decides which worker gets which task.
type Dispatcher struct {

	// Store is the interface to your persistent database (e.g., MongoDB).
	// The Dispatcher uses this to query for PENDING tasks, extend visibility
	// leases when workers heartbeat, and finalize task states (SUCCESS/FAILED).
	Store internal.StorageEngine

	// WorkerStore is the thread-safe, in-memory Connection Registry.
	// When a worker connects via gRPC, its active session (and Go channel) is
	// stored here. The Dispatcher queries this store to know who is online
	// before attempting to route any tasks.
	WorkerStore SessionStore

	// Scheduler is the pluggable routing brain (e.g., RoundRobin, LeastLoaded).
	// Once the Dispatcher pulls tasks from the Store and active workers from
	// the WorkerStore, it passes them to this algorithm to calculate the exact
	// task-to-worker assignments.
	Scheduler SchedulerAlgorithm

	// faultyWorkerTimeout defines the threshold for the background Sweeper.
	// If a worker's TCP connection drops silently (a "ghost" connection), it
	// won't send heartbeats. The Sweeper compares this timeout against the
	// worker's LastHeartbeat and forcibly evicts dead sessions to prevent
	// tasks from being routed into a black hole.
	faultyWorkerTimeout time.Duration

	wg     *sync.WaitGroup
	logger *logrus.Entry

	// timeout is the polling interval (in seconds) that dictates how often the
	// background 'run' loop hits the Store asking for new PENDING tasks.
	// It also dictates the pacing of the database lease 'reaper' loop.
	timeout int

	done chan struct{}
	pb.UnimplementedSchedulerServiceServer
}

// DispatcherOption is the configuration payload used to initialize a new Dispatcher.
type DispatcherOption struct {
	// Store is the mandatory persistent storage engine (e.g., MongoDB, PostgreSQL).
	// It holds the golden state of all tasks (QUEUED, PENDING, SUCCESS, FAILED).
	Store internal.StorageEngine

	// WorkerStore is the interface for the connection registry.
	// Allowing this to be injected makes it significantly easier to write unit tests
	// for the Dispatcher by passing in a mocked in-memory store.
	WorkerStore SessionStore

	// Scheduler is the routing algorithm interface (e.g., RoundRobin, LeastLoaded).
	// Injecting this allows you to easily hot-swap scheduling strategies based on
	// your cluster's specific needs without rewriting the Dispatcher logic.
	Scheduler SchedulerAlgorithm

	// Logger is the pre-configured structured logger (Logrus).
	// Injecting it allows the parent application to set global log levels,
	// output formats (like JSON), and hooks (like sending fatal errors to Sentry).
	Logger *logrus.Entry

	// Timeout defines the core heartbeat rhythm of the Control Plane (in seconds).
	// 1. It dictates how often the Dispatcher polls the Store for new tasks.
	// 2. It is multiplied by 2 to determine the Reaper's interval (how often the
	//    scheduler checks the database for abandoned tasks with expired leases).
	// If set to 0, it defaults to 5 seconds.
	Timeout int

	// Wg is the global WaitGroup passed down from the parent application.
	// The Dispatcher will register all of its background pollers, reapers, and
	// gRPC streams to this WaitGroup, ensuring the main application does not
	// exit until the Control Plane has cleanly drained all network connections.
	Wg *sync.WaitGroup

	// FaultyWorkerTimeout dictates the strictness of the background Sweeper.
	// It defines how much time must pass without a network ping before the
	// Dispatcher assumes a worker's TCP socket is a "ghost" connection and
	// aggressively evicts it from the WorkerStore.
	// If set to 0, it defaults to 10 seconds.
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

// Run acts as the ignition switch for the Augusta Control Plane.
// It is a non-blocking method that spins up three critical, independent
// background loops required to maintain the distributed state machine:
//
//  1. The Poller (p.run): Periodically queries the Store for new PENDING tasks,
//     passes them through the routing algorithm, and drops them into the
//     appropriate worker's Go channel for network delivery.
//
//  2. The Task Reaper (p.reaper): Periodically scans the Store for tasks whose
//     visibility leases have expired. If a worker crashes before finishing a task,
//     this loop catches it and safely re-queues the task for another worker.
//
//  3. The Worker Sweeper (p.reapDeadWorkers): Scans the in-memory Connection Registry
//     for "ghost" network sockets. If a worker hasn't sent a heartbeat recently,
//     this loop forcibly evicts the session so the Poller stops routing tasks to a dead node.
//
// All three background loops monitor the provided context.Context. If the parent
// application cancels this context, the Dispatcher will gracefully halt all polling
// and sweeping operations.
func (p *Dispatcher) Run(ctx context.Context) {
	p.run(ctx)
	p.reaper(ctx)
	p.reapDeadWorkers(ctx)
}

// Periodically queries the Store for new PENDING tasks,
// passes them through the routing algorithm, and drops them into the
// appropriate worker's Go channel for network delivery.
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

// Periodically scans the Store for tasks whose
// visibility leases have expired. If a worker crashes before finishing a task,
// this loop catches it and safely re-queues the task for another worker.
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

// Scans the in-memory Connection Registry
// for "ghost" network sockets. If a worker hasn't sent a heartbeat recently,
// this loop forcibly evicts the session so the Poller stops routing tasks to a dead node.
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

// dispatch is the "Routing Brain" of the Control Plane.
// It is called by the background database poller whenever new PENDING tasks are found.
//
// CRITICAL ARCHITECTURE: This function performs ZERO network I/O.
// If this function tried to write directly to a gRPC stream, a slow worker
// could freeze the entire Dispatcher, preventing it from polling the database.
// Instead, this function acts purely as an in-memory mail sorter.
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

// dispatchListener is the "Dedicated Mailman" for a specific worker connection.
// This function is spawned exactly once when a worker registers, and it runs
// as a background goroutine for the entire lifespan of that worker's TCP session.
//
// It acts as the bridge between internal Go memory (the taskChannel) and
// external network I/O (the gRPC stream).
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

// ConnectSession is the gRPC entry point for all worker nodes. When a worker
// dials the Control Plane, this function is invoked and stays alive for the
// entire duration of that TCP connection.
//
// CRITICAL ARCHITECTURE: This function acts as the "Receiver Loop". It sits
// completely blocked on network reads (stream.Recv()). When it receives a payload,
// it unwraps the gRPC 'oneof' envelope and routes the data directly to the database.
// It runs concurrently alongside the 'dispatchListener' (the Sender Loop) on
// the exact same thread-safe network stream.
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
