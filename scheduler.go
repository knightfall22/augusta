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
	// ID is the unique identifier of this specific Scheduler node (usually a UUID).
	// It is used to identify who currently owns the database Leader lease.
	ID string

	// State represents the current role of this node (Follower, Leader, or Dead).
	// This state dictates whether the Dispatcher is actively polling the database
	// for tasks, or standing by waiting for the current Leader to fail.
	State state

	// StorageEngine is the interface to the persistent database.
	// The Scheduler uses this to directly add, delete, and query tasks via its API,
	// operations that are completely independent of its Follower/Leader state.
	StorageEngine internal.StorageEngine

	// Elector handles the distributed lease-acquisition loop. It continuously
	// tries to grab or renew the leadership lock in the database and sends
	// role-change events down the 'watcher' channel.
	Elector *Elector

	// Dispatcher is the routing engine. The Scheduler will only start() this
	// dispatcher when promoted to Leader, and will stop() it if demoted to Follower.
	Dispatcher *Dispatcher

	// Logger is a structured logger pre-configured with the Scheduler's ID
	// to make debugging cluster-wide leader transitions much easier.
	Logger *logrus.Entry

	// grpcPort dictates which TCP port the gRPC server will bind to.
	grpcPort int

	// grpcServer is the underlying gRPC engine that accepts incoming connections
	// from worker nodes. Even if this Scheduler is a Follower, the gRPC server
	// remains active to accept initial connections (which the L4 Load Balancer might route here).
	grpcServer *grpc.Server

	// listener is the raw network socket bound to the grpcPort. It is stored
	// on the struct so it can be forcefully closed during a graceful shutdown.
	listener net.Listener

	// ctx and cancel manage the global lifecycle of this Scheduler node.
	// Calling Stop() invokes cancel(), which cascades down to the Elector,
	// the Dispatcher, and the gRPC server to cleanly halt all background operations.
	ctx    context.Context
	cancel context.CancelFunc

	sync.RWMutex

	// watcher is the communication pipeline from the Elector.
	// The Elector drops events into this channel whenever the database lease
	// is successfully acquired or lost, triggering the start/stop of the Dispatcher.
	watcher chan Watcher

	// wg tracks the background routines of the Scheduler itself (e.g., the gRPC
	// serving loop and the stateTransition loop). Used to ensure Stop() blocks
	// until everything is cleanly terminated.
	wg sync.WaitGroup
}

// SchedulerOptions provides the configuration necessary to initialize a new
// distributed Scheduler node.
type SchedulerOptions struct {
	// ID is the unique identifier of this Scheduler node. It is highly recommended
	// to provide a stable, unique ID (like a Kubernetes Pod name or EC2 instance ID)
	// because this value is written to the LeaseStorage to claim the Leader lock.
	// If left empty, it defaults to a newly generated UUID.
	ID string

	// StorageEngine is the primary data store (e.g., MongoDB, PostgreSQL) where
	// the actual tasks and their execution states (PENDING, SUCCESS, FAILED) are persisted.
	StorageEngine internal.StorageEngine

	// LeaseStorage is the coordination store (e.g., Redis, etcd, or a dedicated
	// MongoDB collection) used exclusively for Leader Election. It ensures that
	// only one Scheduler node in the cluster acts as the active Dispatcher at a time.
	LeaseStorage internal.LeaseStorage

	// LeaseDuration defines the Time-To-Live (TTL) in seconds for the leader lock.
	// If the active Leader crashes or experiences a network partition, the
	// lock will expire after this duration, allowing a Follower node to promote itself.
	LeaseDuration int

	// DispatcherTimeout dictates the primary polling rhythm of the Control Plane.
	// It determines how often the active Leader queries the StorageEngine for new tasks
	// and how frequently the Reaper checks for expired task leases.
	// If set to 0, it defaults to 5 seconds.
	DispatcherTimeout int

	// GRPCPort is the local TCP port the Scheduler will bind to for accepting
	// bidirectional streams from worker nodes.
	// If set to 0, it defaults to 50051.
	GRPCPort int

	// FaultyWorkerTimeout defines the strictness of the ghost connection Sweeper.
	// If a worker fails to send a heartbeat ping within this duration, the Leader
	// will assume the network socket is dead and forcibly evict the worker's session
	// to prevent routing tasks into a black hole.
	// If set to 0, it defaults to 10 seconds.
	FaultyWorkerTimeout time.Duration

	// Logger provides structured observability for the node.
	// Injecting it allows the parent application to control log levels, JSON formatting,
	// and centralized log aggregation hooks (e.g., sending fatal errors to Sentry).
	// If left nil, it defaults to a standard Logrus instance.
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
		opts.Logger.SetFormatter(logrus.StandardLogger().Formatter)
	}
	logger := opts.Logger.WithContext(ctx).WithField("scheduler", opts.ID)

	if opts.DispatcherTimeout == 0 {
		opts.DispatcherTimeout = 5
	}

	if opts.GRPCPort == 0 {
		//Use high level port for grpc
		opts.GRPCPort = 50051
	}

	if opts.FaultyWorkerTimeout == 0 {
		opts.FaultyWorkerTimeout = 10 * time.Second
	}

	scheduler := &Scheduler{
		ID:            opts.ID,
		ctx:           ctx,
		cancel:        cancel,
		StorageEngine: opts.StorageEngine,
		Logger:        logger,
		grpcPort:      opts.GRPCPort,
	}

	dispatch := NewDispatcher(DispatcherOption{
		Store:               opts.StorageEngine,
		Timeout:             opts.DispatcherTimeout,
		Logger:              logger,
		Wg:                  &scheduler.wg,
		FaultyWorkerTimeout: opts.FaultyWorkerTimeout,
	})
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

	s.Logger.Infof("Starting gRPC server on port %s", s.listener.Addr().String())

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

	s.stateTransition()

	s.watcher = s.Elector.Run(s.ctx)

}

func (s *Scheduler) stateTransition() {
	s.wg.Go(func() {
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
	})
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

	if addedTask.ID == "" {
		addedTask.ID = uuid.New().String()
	}

	task := &domain.Task{
		ID:           addedTask.ID,
		Name:         addedTask.Name,
		TaskType:     addedTask.TaskType,
		Reoccurrence: addedTask.Reoccurrence,
		Command:      addedTask.Command,
		Disabled:     addedTask.Disabled,
		Epsilon:      epsilon,
		Retries:      retries,
		Schedule:     addedTask.Schedule,
		NextRunAt:    nextRun,
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
