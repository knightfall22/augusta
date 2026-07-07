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

type Worker struct {
	ID string

	Tags []string

	SchedulerAddr string

	WorkerHeartbeatInterval time.Duration

	BatchTask        bool
	BatchTaskTimeout time.Duration
	BatchMaxSize     int
	currentBatch     []*pb.TaskResult
	resultCh         chan *pb.TaskResult

	ActiveWorkerHeartBeatTimeout time.Duration

	Processor Processor

	active_tasks map[string]struct{}
	mu           sync.RWMutex

	client pb.SchedulerServiceClient
	conn   *grpc.ClientConn
	stream pb.SchedulerService_ConnectSessionClient

	Logger *logrus.Entry

	wg sync.WaitGroup

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
		currentBatch:                 make([]*pb.TaskResult, 0),
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
