package augusta

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	pb "github.com/knightfall22/augusta/internal/api/v1"
	"github.com/knightfall22/augusta/internal/domain"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Processor interface {
	Process(ctx context.Context, task *domain.Task) error
}

// Worker represents a single distributed execution node. It connects to the
// Augusta scheduler via a long-lived gRPC bidirectional stream to receive
// tasks, execute them concurrently, and report results or heartbeats.
type Worker struct {
	// ID is the unique identifier for this specific worker instance (usually a UUID).
	// The scheduler uses this to track the worker's session in its internal registry.
	ID string

	// Tags represent the capabilities or labels of this worker (e.g., ["gpu", "high-mem"]).
	// The scheduler's routing algorithm uses these to assign specific tasks to this node.
	Tags []string

	// SchedulerAddr is the network address of the control plane (e.g., "augusta.local:50051").
	// In a production environment, this usually points to an L4 Load Balancer.
	SchedulerAddr string

	// WorkerHeartbeatInterval dictates how often this worker sends a basic "Register"
	// payload to the scheduler. This keeps the worker's TCP session alive in the
	// scheduler's connection registry and prevents the sweeper from evicting it.
	WorkerHeartbeatInterval time.Duration

	// BatchTask is a flag determining if task results should be buffered in memory
	// and sent in chunks (true) or streamed back to the scheduler immediately (false).
	BatchTask bool

	// BatchTaskTimeout is the "Time Trigger" for the batch flusher. If this duration
	// elapses, any pending results in the currentBatch are sent over the network,
	// ensuring low-volume periods don't leave results hanging in memory forever.
	BatchTaskTimeout time.Duration

	// BatchMaxSize is the "Volume Trigger" for the batch flusher. If the currentBatch
	// reaches this exact size, it is immediately flushed to prevent memory bloat and
	// exceeding the gRPC frame size limits.
	BatchMaxSize int

	// currentBatch is the in-memory buffer holding finished task results waiting
	// to be flushed. It is protected by the `mu` Mutex to prevent data races.
	currentBatch []*pb.TaskResult

	// resultCh is a funnel channel. Independent execution goroutines drop their
	// finished results into this channel, allowing the background batchProcessor
	// to safely aggregate them without blocking the worker's CPU threads.
	resultCh chan *pb.TaskResult

	// ActiveWorkerHeartBeatTimeout dictates how often the worker sends an array of
	// currently executing Task IDs to the scheduler. This tells the database to
	// extend the "visibility lease" on those tasks so they aren't marked as dead
	// and retried by another worker.
	ActiveWorkerHeartBeatTimeout time.Duration

	// Processor is the interface provided by the user. It contains the actual opaque,
	// business-logic execution code (Process(ctx, task)) that runs when a task arrives.
	Processor Processor

	// active_tasks is an in-memory set tracking the IDs of all tasks currently in-flight.
	// It is used to generate the active heartbeat payload and is fiercely protected
	// by the `mu` Mutex to prevent panics during concurrent reads/writes.
	active_tasks map[string]struct{}

	mu sync.RWMutex

	// client is the underlying gRPC stub used to establish the initial connection
	// to the scheduler cluster.
	client pb.SchedulerServiceClient

	// conn is the raw TCP connection to the gRPC server. It is stored on the struct
	// so it can be cleanly terminated during the worker's graceful shutdown phase.
	conn *grpc.ClientConn

	// stream is the active, long-lived bidirectional gRPC socket. It is heavily
	// multiplexed: the msgListener loop blocks on stream.Recv(), while the heartbeat
	// and result routines safely call stream.Send() concurrently.
	stream pb.SchedulerService_ConnectSessionClient

	// Logger is a structured logger pre-populated with the Worker's ID for highly
	// traceable distributed debugging.
	Logger *logrus.Entry

	// wg tracks all background goroutines (listeners, flushers, heartbeats). During
	// a shutdown, Close() waits for this counter to hit zero, guaranteeing no memory
	// leaks or orphaned processes are left behind.
	wg sync.WaitGroup

	// quit is a broadcast channel used for graceful shutdown. When w.Close() closes
	// this channel, every infinite select loop inside the worker instantly catches
	// the signal and cleanly terminates.
	quit chan struct{}
}

type WorkerOpts struct {
	ID                           string
	WorkerHeartbeatInterval      time.Duration
	Tags                         []string
	SchedulerAddr                string
	BatchTask                    bool
	BatchTaskTimeout             time.Duration
	BatchMaxSize                 int
	ActiveWorkerHeartBeatTimeout time.Duration
	Processor                    Processor
}

func NewWorker(opts WorkerOpts) (*Worker, error) {
	logger := logrus.New()
	logger.SetFormatter(logrus.StandardLogger().Formatter)

	grpcOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	conn, err := grpc.NewClient(opts.SchedulerAddr, grpcOpts...)
	if err != nil {
		logger.Errorf("Cannot connect to scheduler: %v", err)
		return nil, err
	}

	client := pb.NewSchedulerServiceClient(conn)

	if opts.ID == "" {
		opts.ID = uuid.New().String()
	}

	if opts.WorkerHeartbeatInterval == 0 {
		opts.WorkerHeartbeatInterval = 15 * time.Second
	}

	if opts.BatchTaskTimeout == 0 {
		opts.BatchTaskTimeout = 5 * time.Second
	}

	if opts.BatchMaxSize == 0 {
		opts.BatchMaxSize = 50
	}

	if opts.ActiveWorkerHeartBeatTimeout == 0 {
		opts.ActiveWorkerHeartBeatTimeout = 2 * time.Second
	}

	return &Worker{
		ID:                           opts.ID,
		Tags:                         opts.Tags,
		WorkerHeartbeatInterval:      opts.WorkerHeartbeatInterval,
		SchedulerAddr:                opts.SchedulerAddr,
		BatchTask:                    opts.BatchTask,
		BatchTaskTimeout:             opts.BatchTaskTimeout,
		BatchMaxSize:                 opts.BatchMaxSize,
		Processor:                    opts.Processor,
		currentBatch:                 make([]*pb.TaskResult, 0, opts.BatchMaxSize),
		resultCh:                     make(chan *pb.TaskResult),
		ActiveWorkerHeartBeatTimeout: opts.ActiveWorkerHeartBeatTimeout,
		Logger:                       logger.WithField("ID", opts.ID),
		conn:                         conn,
		client:                       client,
		quit:                         make(chan struct{}),
		active_tasks:                 make(map[string]struct{}),
	}, nil
}

func (w *Worker) Start(ctx context.Context) error {
	stream, err := w.client.ConnectSession(ctx)
	if err != nil {
		w.Logger.Errorf("Cannot start connect session: %v", err)
		return err
	}

	w.stream = stream

	//TODO: look into adding context to these functions or wrap ctx with stream context
	w.workerHeartbeat()
	w.msgListener()
	w.activeTaskHeartbeat()

	if w.BatchTask {
		w.batchProcessor()
	}
	return nil
}

// workerHeartbeat serves two critical purposes in the distributed lifecycle:
//  1. Handshake: It sends the initial Registration payload so the Control Plane
//     can create a WorkerSession in its Connection Registry.
//  2. Keep-Alive: It spins up a background goroutine to periodically ping the
//     Control Plane. This prevents the L4 Load Balancer from dropping an idle TCP
//     socket and prevents the Scheduler's Sweeper from prematurely evicting the session.
func (w *Worker) workerHeartbeat() {
	if err := w.stream.Send(&pb.ClientMessage{
		Payload: &pb.ClientMessage_Register{
			Register: &pb.RegisterWorker{
				WorkerId: w.ID,
			},
		},
	}); err != nil {
		w.Logger.Errorf("failed to registration message: %v", err)
		w.Close()
		return
	}

	w.wg.Go(func() {
		for {
			select {
			case <-w.quit:
				return
			case <-w.stream.Context().Done():
				return
			case <-time.After(w.WorkerHeartbeatInterval):
				if err := w.stream.Send(&pb.ClientMessage{
					Payload: &pb.ClientMessage_Register{
						Register: &pb.RegisterWorker{
							WorkerId: w.ID,
						},
					},
				}); err != nil {
					w.Logger.Errorf("failed to heartbeat message: %v", err)

					return
				}
				w.Logger.Infof("Sent heartbeat to scheduler")
			}
		}
	})
}

// msgListener is the primary network ingress engine for the worker.
// It runs as a dedicated, infinite background goroutine that constantly listens
// to the gRPC bidirectional stream for incoming commands from the Control Plane.
//
// Crucially, this function acts ONLY as a router. It never executes the tasks
// itself.
func (w *Worker) msgListener() {
	w.wg.Go(func() {
		for {
			select {
			case <-w.quit:
				return
			case <-w.stream.Context().Done():
				return
			default:
			}
			msg, err := w.stream.Recv()
			if err == io.EOF {
				w.Logger.Errorf("server closed the stream: %v", err)
				return
			}
			if err != nil {
				w.Logger.Errorf("failed to receive message: %v", err)
				return
			}

			switch msg.Payload.(type) {
			case *pb.ServerMessage_Tasks:
				if w.BatchTask == true {
					w.performBatchWork(msg.GetTasks().GetTasks())
				} else {
					w.Logger.Infof("received tasks: %d", len(msg.GetTasks().GetTasks()))
					w.performWork(msg.GetTasks().GetTasks())
				}
			case *pb.ServerMessage_Ack:
				w.Logger.Infof("Received ack from scheduler")
			}
		}
	})
}

// performWork is responsible for executing a batch of tasks immediately and
// reporting their results back to the Control Plane one by one.
//
// This function implements the "Fan-Out" concurrency pattern. It iterates over
// the incoming batch and spins up an isolated, independent goroutine for each
// task, returning almost instantly so the msgListener is never blocked.
func (w *Worker) performWork(tasks []*pb.Task) {
	logger := w.Logger.WithField("method", "performWork")

	for _, t := range tasks {
		w.mu.Lock()
		w.active_tasks[t.Id] = struct{}{}
		w.mu.Unlock()

		go func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Errorf("CRITICAL: Processor panicked on task %s: %v", t.Id, r)

					w.sendTaskResult(t.Id, pb.Status_ERROR, "worker panic")
				}

				w.mu.Lock()
				delete(w.active_tasks, t.Id)
				w.mu.Unlock()

			}()

			ctx, cancel := context.WithCancel(w.stream.Context())
			defer cancel()

			task := domain.DeserializeFromProtobuf(t)

			if err := w.Processor.Process(ctx, task); err != nil {
				logger.Errorf("failed to process task %s: %v", task.ID, err)

				w.sendTaskResult(task.ID, pb.Status_ERROR, err.Error())
				return
			}

			if err := w.sendTaskResult(task.ID, pb.Status_OK, ""); err != nil {
				logger.Errorf("failed to send task result for task %s: %v", task.ID, err)
				return
			}
		}()
	}

}

func (w *Worker) sendTaskResult(id string, status pb.Status, errMsg string) error {
	return w.stream.Send(&pb.ClientMessage{
		Payload: &pb.ClientMessage_TaskResult{
			TaskResult: &pb.TaskResult{
				TaskId:       id,
				Status:       status,
				ErrorMessage: errMsg,
				WorkerId:     w.ID,
			},
		},
	})
}

// performBatchWork implements a high-throughput, fan-out execution model.
// Unlike performWork (which writes results directly to the network), this function
// funnels all finished results into an internal Go channel (w.resultCh).
// This allows the background 'batchProcessor' loop to aggregate them and send them
// to the Control Plane in large, network-efficient chunks.
func (w *Worker) performBatchWork(tasks []*pb.Task) {
	logger := w.Logger.WithField("method", "performBatchWork")

	for _, t := range tasks {
		w.mu.Lock()
		w.active_tasks[t.Id] = struct{}{}
		w.mu.Unlock()

		go func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Errorf("CRITICAL: Processor panicked on task %s: %v", t.Id, r)

					result := &pb.TaskResult{
						TaskId:       t.Id,
						Status:       pb.Status_ERROR,
						ErrorMessage: "worker panic",
					}

					select {
					case w.resultCh <- result:
					case <-w.stream.Context().Done():
						logger.Error("stream context cancelled, closing batch woker")
					case <-w.quit:
						logger.Error("closing batch woker")
					}

				}

				w.mu.Lock()
				delete(w.active_tasks, t.Id)
				w.mu.Unlock()

			}()

			ctx, cancel := context.WithCancel(w.stream.Context())
			defer cancel()

			task := domain.DeserializeFromProtobuf(t)

			if err := w.Processor.Process(ctx, task); err != nil {
				logger.Errorf("failed to process task %s: %v", task.ID, err)
				result := &pb.TaskResult{
					TaskId:       t.Id,
					Status:       pb.Status_ERROR,
					ErrorMessage: err.Error(),
				}

				select {
				case w.resultCh <- result:
				case <-w.stream.Context().Done():
					logger.Error("stream context cancelled, closing batch woker")
					return
				case <-w.quit:
					logger.Error("closing batch woker")
					return
				}
			}

			result := &pb.TaskResult{
				TaskId:       t.Id,
				Status:       pb.Status_OK,
				ErrorMessage: "",
			}

			select {
			case w.resultCh <- result:
			case <-w.stream.Context().Done():
				logger.Error("stream context cancelled, closing batch woker")
				return
			case <-w.quit:
				logger.Error("closing batch woker")
				return
			}
		}()
	}

}

func (w *Worker) batchProcessor() {
	ticker := time.NewTicker(w.BatchTaskTimeout)

	w.wg.Go(func() {
		for {
			select {
			case <-w.quit:
				return
			case <-w.stream.Context().Done():
				return
			case <-ticker.C:
				w.mu.RLock()
				batchLength := len(w.currentBatch)
				w.mu.RUnlock()
				if batchLength > 0 {
					w.flushBatchTask()
				}
			case result := <-w.resultCh:
				w.mu.Lock()
				w.currentBatch = append(w.currentBatch, result)
				batchLength := len(w.currentBatch)
				w.mu.Unlock()

				if batchLength >= w.BatchMaxSize {
					w.flushBatchTask()
				}
			}
		}
	})
}

func (w *Worker) flushBatchTask() {
	w.mu.Lock()
	batch := w.currentBatch
	w.currentBatch = make([]*pb.TaskResult, 0)
	w.mu.Unlock()

	if err := w.stream.Send(&pb.ClientMessage{
		Payload: &pb.ClientMessage_TaskResultBatch{
			TaskResultBatch: &pb.TaskResultBatch{
				WorkerId: w.ID,
				Results:  batch,
			},
		},
	}); err != nil {
		w.Logger.Errorf("error flushing task batch: %v", err)
		return
	}

}

// activeTaskHeartbeat runs as a continuous background goroutine. Its sole purpose
// is to aggregate the IDs of all tasks currently executing on this worker and
// send them to the Control Plane at a regular interval (ActiveWorkerHeartBeatTimeout).
//
// The Control Plane uses this payload to execute an update of the tasks in the storage engine, pushing the
// `visibility_timeout` of these specific tasks further into the future.
// If this loop stops, the Scheduler's Reaper will assume the worker died,
// mark the tasks as abandoned, and re-queue them for other workers to pick up.
func (w *Worker) activeTaskHeartbeat() {
	ticker := time.NewTicker(w.ActiveWorkerHeartBeatTimeout)
	w.wg.Go(func() {

		for {
			select {
			case <-w.quit:
				return
			case <-w.stream.Context().Done():
				return
			case <-ticker.C:
				activeTasks := w.activeTasksSlice()

				if len(activeTasks) == 0 {
					continue
				}

				if err := w.stream.Send(&pb.ClientMessage{
					Payload: &pb.ClientMessage_TaskHeartbeat{
						TaskHeartbeat: &pb.TaskHeartbeat{
							WorkerId:      w.ID,
							ActiveTaskIds: activeTasks,
						},
					},
				}); err != nil {
					w.Logger.Errorf("failed to task heartbeat: %v", err)
					return

				}
				w.Logger.Debugf("Sent task heartbeat to scheduler for %d tasks", len(activeTasks))
			}
		}
	})
}

func (w *Worker) activeTasksSlice() []string {
	w.mu.RLock()
	result := make([]string, len(w.active_tasks))

	i := 0
	for k := range w.active_tasks {
		result[i] = k
		i++
	}
	w.mu.RUnlock()
	return result
}

func (w *Worker) Close() {
	close(w.quit)
	close(w.resultCh)
	w.conn.Close()
	w.wg.Wait()
	w.Logger.Info("worker closed")
}
