package augusta

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/knightfall22/augusta/internal"
	pb "github.com/knightfall22/augusta/internal/api/v1"
	"github.com/knightfall22/augusta/internal/domain"
	"github.com/sirupsen/logrus"
	"github.com/sosodev/duration"
	"google.golang.org/grpc"
)

type state int

const (
	Follower state = 0
	Leader   state = 1
	Dead     state = 2
)

func (s state) String() string {

	switch s {
	case Follower:
		return "Follower"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		return "Unknown"
	}
}

// Scheduler represents a single, independent control plane node within the distributed cluster.
// It is designed to be highly available and masterless, meaning any node can safely ingest
// and persist new tasks into the storage engine.
//
// To prevent double-dispatching and database contention, the Scheduler utilizes a lease-based
// leader election mechanism. While all nodes can accept inbound API requests, only the actively
// elected Leader is permitted to run the polling loop that transitions tasks from pending to
// assigned.
//
// If the current Leader crashes, experiences network partition, or fails to renew its lease
// before the TTL expires, a Follower node will automatically promote itself and safely resume
// the polling loop.
//
// The Scheduler is strictly agnostic regarding execution logic. It treats all task commands as
// opaque payloads, focusing entirely on managing execution state, temporal routing (ISO 8601),
// and exponential backoff, while leaving the payload interpretation entirely to the worker nodes.
type Scheduler struct {
	// ID is the unique identifier of the task. Uses UUID
	ID    string
	State state

	StorageEngine internal.StorageEngine
	Elector       *Elector
	Dispatcher    *Dispatcher

	Logger *logrus.Entry

	grpcPort   int
	grpcServer *grpc.Server
	listener   net.Listener

	//todo: watch the context as development continues and gauge if it needed
	ctx    context.Context
	cancel context.CancelFunc
	sync.RWMutex
	watcher chan Watcher
	wg      sync.WaitGroup
}

type SchedulerOptions struct {
	//Identifier of the scheduler. Is optional and default to UUID.
	ID string

	StorageEngine internal.StorageEngine

	LeaseStorage internal.LeaseStorage

	LeaseDuration int

	DispatcherTimeout int

	GRPCPort int

	//Logger to be used. Default to log.Default()
	Logger *logrus.Logger
}

// NewScheduler creates a new Scheduler instance
func NewScheduler(opts SchedulerOptions) *Scheduler {
	if opts.ID == "" {
		opts.ID = uuid.New().String()
	}

	ctx, cancel := context.WithCancel(context.Background())

	if opts.Logger == nil {
		opts.Logger = logrus.New()
		opts.Logger.SetFormatter(&logrus.JSONFormatter{})
	}
	logger := opts.Logger.WithContext(ctx).WithField("scheduler", opts.ID)

	if opts.DispatcherTimeout == 0 {
		opts.DispatcherTimeout = 5
	}

	if opts.GRPCPort == 0 {
		//Use high level port for grpc
		opts.GRPCPort = 50051
	}

	scheduler := &Scheduler{
		ID:            opts.ID,
		ctx:           ctx,
		cancel:        cancel,
		StorageEngine: opts.StorageEngine,
		Logger:        logger,
		grpcPort:      opts.GRPCPort,
	}

	dispatch := NewDispatcher(opts.StorageEngine, opts.DispatcherTimeout, logger, &scheduler.wg)
	scheduler.Dispatcher = dispatch

	elector := NewElector(opts.ID, opts.LeaseStorage, opts.LeaseDuration, logger, &scheduler.wg)
	scheduler.Elector = elector

	scheduler.grpcServer = grpc.NewServer()
	pb.RegisterSchedulerServiceServer(scheduler.grpcServer, dispatch)

	return scheduler
}

func (s *Scheduler) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", s.grpcPort))
	if err != nil {
		log.Fatal(err)
	}

	s.Logger.Infof("Starting gRPC server on port %+v", s.listener.Addr())

	s.wg.Go(func() {
		if err := s.grpcServer.Serve(s.listener); err != nil {
			select {
			case <-s.ctx.Done():
				s.Logger.Info("Context Cancelled Stopping gRPC server")
				return
			default:
				s.Logger.Errorf("server error: %+v", err)
				return
			}
		}
	})
	s.startElector()
	return nil
}

func (s *Scheduler) startElector() {
	s.wg.Go(func() {
		s.stateTransition()
	})
	s.watcher = s.Elector.Run(s.ctx)

}

func (s *Scheduler) stateTransition() {
	for {
		s.Lock()
		select {
		case <-s.ctx.Done():
			s.State = Dead
			s.Unlock()
			s.Logger.Info("Stopping scheduler")
			return
		case watch := <-s.watcher:
			switch watch.Err {
			case internal.ErrCannotAquireLock:
				if s.State == Leader {
					s.Logger.Info("Leader lost")
					s.Dispatcher.Stop()
				}
				s.State = Follower
			case nil:
				if s.State != Leader {
					s.Logger.Info("Leader Acquired")
					s.Dispatcher.Run(s.ctx)
				}
				s.State = Leader
			default:
				s.Logger.Errorf("error as occured %v", watch.Err)
				s.Stop()
				s.Unlock()
				return
			}
			s.Unlock()
		}

	}
}

func (s *Scheduler) Stop() error {
	s.cancel()
	s.listener.Close()
	s.wg.Wait()
	return nil
}

func (s *Scheduler) GetState() state {
	s.RLock()
	defer s.RUnlock()
	return s.State
}

// AddTask adds a task to the storage engine.
// All schedulers regardless of role are able to add tasks
func (s *Scheduler) AddTask(ctx context.Context, addedTask *domain.AddTask) error {
	epsilon := addedTask.Epsilon
	if epsilon == "" {
		epsilon = internal.DefaultEpsilon
	}

	retries := addedTask.Retries
	if retries == 0 {
		retries = internal.DefaultRetries
	}

	if addedTask.Schedule == "" {
		secJitter := rand.IntN(20)
		addedTask.Schedule = fmt.Sprintf("PT%dS", secJitter)
	}
	nextRunDuration, _ := duration.Parse(addedTask.Schedule)
	nextRun := time.Now().UTC().Add(nextRunDuration.ToTimeDuration())

	task := &domain.Task{
		ID:        uuid.New().String(),
		Name:      addedTask.Name,
		TaskType:  addedTask.TaskType,
		Command:   addedTask.Command,
		Disabled:  addedTask.Disabled,
		Epsilon:   epsilon,
		Retries:   retries,
		Schedule:  addedTask.Schedule,
		NextRunAt: nextRun,
	}

	if err := s.StorageEngine.AddTask(ctx, task); err != nil {
		return err
	}
	return nil
}

// DeleteTask deletes a task from the storage engine
func (s *Scheduler) DeleteTask(ctx context.Context, taskID string) error {
	return s.StorageEngine.DeleteTask(ctx, taskID)
}

// GetTask gets a task from the storage engine
func (s *Scheduler) GetTask(ctx context.Context, taskID string) (*domain.Task, error) {
	return s.StorageEngine.GetTask(ctx, taskID)
}
