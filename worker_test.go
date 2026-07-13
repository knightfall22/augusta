package augusta

import (
	"context"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/google/uuid"
	"github.com/knightfall22/augusta/internal/domain"
	"github.com/sosodev/duration"
)

func TestWorkerInitialization(t *testing.T) {
	h := NewTestHarness(t, 3, testHarnessOptions{StorageEngine: stoageEngine, MongoURI: mongoURI})

	leaderID := h.CheckLeader()

	_, addr := h.Report(leaderID)

	w, err := NewWorker(WorkerOpts{
		ID:            uuid.New().String(),
		Tags:          []string{"tag1", "tag2"},
		SchedulerAddr: addr,
	})

	if err != nil {
		t.Fatal(err)
	}

	w.Close()

	h.Stop()
}

func TestActiveTasks(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()
	h := NewTestHarness(t, 3, testHarnessOptions{
		StorageEngine:     stoageEngine,
		MongoURI:          mongoURI,
		DispatcherTimeout: 1,
	})
	defer h.Stop()

	leaderID := h.CheckLeader()

	_, addr := h.Report(leaderID)

	slowProcessor := &SlowProcessor{
		close: make(chan struct{}),
	}
	wth := NewWorkerTestHarness(t, 3, workerTestOptions{
		ActiveWorkerHeartBeatTimeout: 1 * time.Second,
		Processor:                    slowProcessor,
		SchedulerAddr:                addr,
		WorkerHeartbeatInterval:      1 * time.Second,
	})
	defer wth.Shutdown()

	tasks := generateDummyTasks(TaskGenOptions{
		N:        5,
		TaskType: "test",
		Schedule: "PT2S",
	})

	ids := h.AddTask(tasks)

	sleepMs(1500)
	wth.CompareActiveTasks(ids)

	slowProcessor.Close()
	sleepMs(1000)
	wth.CompareActiveTasks([]string{})
}

type ChannelMockProcessor struct {
	tasks chan *domain.Task
	close chan struct{}
}

func (p *ChannelMockProcessor) Process(ctx context.Context, task *domain.Task) error {
	select {
	case p.tasks <- task:
	case <-p.close:
	}
	return nil
}

func (p *ChannelMockProcessor) Close() {
	close(p.close)
}

func TestTaskReoccurrence(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()
	h := NewTestHarness(t, 3, testHarnessOptions{
		StorageEngine:     stoageEngine,
		MongoURI:          mongoURI,
		DispatcherTimeout: 1,
	})
	defer h.Stop()

	leaderID := h.CheckLeader()
	_, addr := h.Report(leaderID)

	mockProcessor := &ChannelMockProcessor{
		tasks: make(chan *domain.Task, 10),
		close: make(chan struct{}),
	}

	wth := NewWorkerTestHarness(t, 1, workerTestOptions{
		ActiveWorkerHeartBeatTimeout: 1 * time.Second,
		Processor:                    mockProcessor,
		SchedulerAddr:                addr,
		WorkerHeartbeatInterval:      1 * time.Second,
	})
	defer wth.Shutdown()

	tasks := generateDummyTasks(TaskGenOptions{
		N:            1,
		TaskType:     "recurring_test",
		Schedule:     "PT2S",
		Reoccurrence: 5,
	})

	// Add the task
	h.AddTask(tasks)

	// Since PT2S is used, NextRunAt is updated by adding 2 seconds to the previous run time.
	// We will track the expected NextRunAt for each iteration.

	scheduleDur, err := duration.Parse("PT2S")
	if err != nil {
		t.Fatalf("Failed to parse duration: %v", err)
	}
	dur := scheduleDur.ToTimeDuration()

	iterations := 3
	var previousNextRun time.Time

	for i := range iterations {
		timeout := 5 * time.Second
		if i == 0 {
			timeout = 25 * time.Second
		}

		select {
		case task := <-mockProcessor.tasks:
			t.Logf("Iteration %d received task. NextRunAt: %v", i+1, task.NextRunAt)

			if task.NextRunAt.IsZero() {
				t.Errorf("Iteration %d: expected NextRunAt to be set, got zero time", i+1)
			}

			if i > 0 {
				expectedNextRun := previousNextRun.Add(dur)
				// Allow a small margin of error if they aren't exactly matching, though they should be
				diff := task.NextRunAt.Sub(expectedNextRun)
				acceptableMargin := 2 * time.Second
				if diff < -acceptableMargin || diff > acceptableMargin {
					t.Errorf("Iteration %d: NextRunAt %v deviates from expected %v by %v", i+1, task.NextRunAt, expectedNextRun, diff)
				}
			}
			previousNextRun = task.NextRunAt

		case <-time.After(timeout):
			t.Fatalf("Timeout waiting for task iteration %d", i+1)
		}
	}

	sleepMs(500)

	for _, tsk := range tasks {
		count, err := h.scheduler[0].StorageEngine.CountTaskStats(context.Background(), tsk.ID)
		if err != nil {
			t.Fatalf("Failed to count task stats: %v", err)
		}
		if count != int64(iterations) {
			t.Fatalf("Expected task stats count to be %d, got %d", iterations, count)
		}
	}
	mockProcessor.Close()
}

func TestBatchTaskProcessing(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewTestHarness(t, 3, testHarnessOptions{
		StorageEngine:     stoageEngine,
		MongoURI:          mongoURI,
		DispatcherTimeout: 1,
	})
	defer h.Stop()

	leaderID := h.CheckLeader()
	_, addr := h.Report(leaderID)

	mockProcessor := &ChannelMockProcessor{
		tasks: make(chan *domain.Task, 10),
		close: make(chan struct{}),
	}

	batchSize := 3
	batchTimeout := 2 * time.Second

	wth := NewWorkerTestHarness(t, 1, workerTestOptions{
		ActiveWorkerHeartBeatTimeout: 1 * time.Second,
		Processor:                    mockProcessor,
		SchedulerAddr:                addr,
		WorkerHeartbeatInterval:      1 * time.Second,
		BatchTask:                    true,
		BatchMaxSize:                 batchSize,
		BatchTaskTimeout:             batchTimeout,
	})
	defer wth.Shutdown()

	t.Log("Testing batch flush by Max Size...")

	sizeTasks := generateDummyTasks(TaskGenOptions{
		N:        batchSize,
		TaskType: "batch_size_test",
		Schedule: "PT2S",
	})
	h.AddTask(sizeTasks)

	for i := 0; i < batchSize; i++ {
		select {
		case task := <-mockProcessor.tasks:
			t.Logf("Max Size Scenario: Processed task %s", task.ID)
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for batch task %d to be processed", i+1)
		}
	}

	t.Log("Testing batch flush by Timeout...")

	timeoutTasks := generateDummyTasks(TaskGenOptions{
		N:        1,
		TaskType: "batch_timeout_test",
		Schedule: "PT2S",
	})
	h.AddTask(timeoutTasks)

	select {
	case task := <-mockProcessor.tasks:
		t.Logf("Timeout Scenario: Processed task %s", task.ID)
	case <-time.After(batchTimeout + (3 * time.Second)): // generous buffer for scheduler network delay
		t.Fatalf("Timeout waiting for partial batch task to be processed and flushed")
	}

	mockProcessor.Close()
}
