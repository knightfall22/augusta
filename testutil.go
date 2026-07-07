package augusta

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/knightfall22/augusta/internal"
	"github.com/knightfall22/augusta/internal/domain"
	inMemoryStorage "github.com/knightfall22/augusta/internal/storage/inmemory"
	mongoStorage "github.com/knightfall22/augusta/internal/storage/mongodb"
	"github.com/sirupsen/logrus"
)

type TestHarness struct {
	//List of schedulers parcipating in the election
	scheduler []*Scheduler

	//Number of schedulers
	n int

	t *testing.T
}

type testHarnessOptions struct {
	StorageEngine     string
	MongoURI          string
	DispatcherTimeout int
}

func NewTestHarness(t *testing.T, n int, opt testHarnessOptions) *TestHarness {
	schedulers := make([]*Scheduler, n)

	var se internal.StorageEngine
	var le internal.LeaseStorage
	if opt.StorageEngine == "mongodb" {
		store, err := mongoStorage.NewMongoStore("augusta", opt.MongoURI)
		if err != nil {
			t.Fatal(err)
		}
		se = store
		le = store
	} else {
		store := inMemoryStorage.NewInMemStorage()
		le = store
		se = store
	}

	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	for id := range n {
		schedulers[id] = NewScheduler(SchedulerOptions{
			ID:                fmt.Sprintf("%d", id),
			StorageEngine:     se,
			LeaseStorage:      le,
			LeaseDuration:     10,
			Logger:            logger,
			GRPCPort:          50051 + id + 1,
			DispatcherTimeout: opt.DispatcherTimeout,
		})
	}

	for _, sh := range schedulers {
		sh.Start()
	}

	return &TestHarness{
		scheduler: schedulers,
		n:         n,
		t:         t,
	}
}

func (th *TestHarness) CheckLeader() string {
	leaderID := ""
	for range 5 {
		for _, s := range th.scheduler {
			if s.GetState() == Leader {
				if leaderID != "" {
					th.t.Fatalf("Multiple leaders found")
				}
				leaderID = s.ID
			}

		}

		sleepMs(100)
		if leaderID != "" {
			return leaderID
		}

	}
	th.t.Fatalf("Leader not found")
	return ""
}

func (th *TestHarness) StopScheduler(id string) {
	for _, s := range th.scheduler {
		if s.ID == id {
			s.Stop()
		}

	}
}

// Picks a scheduler at random and adds task to it. Returns the ids of added tasks
func (th *TestHarness) AddTask(tasks []*domain.AddTask) []string {
	rndIdx := rand.Intn(th.n)

	sch := th.scheduler[rndIdx]

	ids := make([]string, len(tasks))
	for i, tsk := range tasks {
		if err := sch.AddTask(context.Background(), tsk); err != nil {
			th.t.Fatalf("failed to add task: %v", err)
		}
		ids[i] = tsk.ID
	}

	return ids
}

type TaskGenOptions struct {
	TaskType     string
	N            int
	Schedule     string
	Epsilon      string
	Retries      int
	Reoccurrence int
	Command      []byte
}

func generateDummyTasks(opts TaskGenOptions) []*domain.AddTask {
	tasks := make([]*domain.AddTask, opts.N)
	for i := range tasks {
		tasks[i] = &domain.AddTask{
			ID:           uuid.New().String(),
			TaskType:     opts.TaskType,
			Schedule:     opts.Schedule,
			Epsilon:      opts.Epsilon,
			Retries:      opts.Retries,
			Reoccurrence: opts.Reoccurrence,
			Command:      opts.Command,
		}
	}
	return tasks
}

func (th *TestHarness) Report(id string) (state, string) {
	for _, s := range th.scheduler {
		if s.ID == id {
			return s.GetState(), s.listener.Addr().String()
		}
	}
	return 0, ""
}

func (th *TestHarness) Stop() {
	for _, s := range th.scheduler {
		s.StorageEngine.Flush(context.Background())
		s.Elector.LeaseStorage.Flush(context.Background())
		s.Stop()
	}
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}

type WorkerTestHarness struct {
	workers []*Worker

	n int
	t *testing.T
}

type workerTestOptions struct {
	WorkerHeartbeatInterval      time.Duration
	BatchTask                    bool
	BatchTaskTimeout             time.Duration
	BatchMaxSize                 int
	ActiveWorkerHeartBeatTimeout time.Duration
	SchedulerAddr                string
	Processor                    Processor
}

func NewWorkerTestHarness(t *testing.T, n int, opt workerTestOptions) *WorkerTestHarness {
	workers := make([]*Worker, n)
	for id := range n {
		worker, err := NewWorker(WorkerOpts{
			ID:                           fmt.Sprintf("%d", id),
			WorkerHeartbeatInterval:      opt.WorkerHeartbeatInterval,
			BatchTask:                    opt.BatchTask,
			BatchTaskTimeout:             opt.BatchTaskTimeout,
			BatchMaxSize:                 opt.BatchMaxSize,
			ActiveWorkerHeartBeatTimeout: opt.ActiveWorkerHeartBeatTimeout,
			SchedulerAddr:                opt.SchedulerAddr,
			Processor:                    opt.Processor,
		})
		if err != nil {
			t.Fatal(err)
		}

		workers[id] = worker
	}

	for _, w := range workers {
		if err := w.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	return &WorkerTestHarness{
		workers: workers,
		n:       n,
		t:       t,
	}
}

func (wth *WorkerTestHarness) CompareActiveTasks(ids []string) {
	allActiveTasks := make(map[string]struct{})
	for _, w := range wth.workers {
		for _, t := range w.activeTasksSlice() {
			if _, ok := allActiveTasks[t]; !ok {
				allActiveTasks[t] = struct{}{}
				continue
			}
			wth.t.Fatal("Duplicate task found")
		}
	}

	for _, id := range ids {
		if _, ok := allActiveTasks[id]; !ok {
			wth.t.Fatal("Task not found")
		}
	}
}

func (wth *WorkerTestHarness) Stop(id string) {
	for _, w := range wth.workers {
		if w.ID == id {
			w.Close()
		}
	}
}

func (wth *WorkerTestHarness) Shutdown() {
	for _, w := range wth.workers {
		w.Close()
	}
}

type SlowProcessor struct {
	close chan struct{}
}

func (p *SlowProcessor) Process(ctx context.Context, task *domain.Task) error {
	log.Println("Sleeping for 2seconds")
	ticker := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-ticker.C:
			return nil
		case <-p.close:
			return nil
		}
	}
}

func (p *SlowProcessor) Close() {
	close(p.close)
}
