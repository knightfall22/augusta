package augusta

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/knightfall22/augusta/internal/domain"
	inMemoryStorage "github.com/knightfall22/augusta/internal/storage/inmemory"
)

var (
	stoageEngine string
	mongoURI     string
)

func init() {
	flag.StringVar(&stoageEngine, "storage", "in-memory", "Used to set the storage engine used by the scheduler can be either in-memory or mongodb")
	flag.StringVar(&mongoURI, "mongo", "mongodb://localhost:27017", "Used to set the mongo uri for the mongodb storage engine")

}
func TestLeaderLeaseElection(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewTestHarness(t, 3, testHarnessOptions{StorageEngine: stoageEngine, MongoURI: mongoURI})

	h.CheckLeader()

	h.Stop()

}

func TestLeaderLeaseShutdownAndNewLeader(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewTestHarness(t, 3, testHarnessOptions{StorageEngine: stoageEngine, MongoURI: mongoURI})

	leaderID := h.CheckLeader()

	h.StopScheduler(leaderID)

	if state, _ := h.Report(leaderID); state != Dead {
		t.Fatalf("Previous Leader not dead")
	}

	sleepMs(100)
	newLeaderID := h.CheckLeader()

	if newLeaderID == leaderID {
		t.Fatalf("Leader not changed")
	}

	h.Stop()

}

func TestAddTaskToScheduler(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	scheduler := NewScheduler(SchedulerOptions{
		StorageEngine: inMemoryStorage.NewInMemStorage(),
		LeaseStorage:  inMemoryStorage.NewInMemStorage(),
	})

	tests := []struct {
		name string
		task *domain.AddTask
		want time.Time
	}{
		{
			name: "Add task to scheduler with empty schedule",
			task: &domain.AddTask{
				Name:     "test",
				TaskType: "test",
				// Command:  "test",
			},
		},
		{
			name: "Add task to scheduler with schedule",
			task: &domain.AddTask{
				Name:     "test2",
				TaskType: "test",
				// Command:  "test",
				Schedule: "PT30M",
			},
			want: time.Now().UTC().Add(30 * time.Minute),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := scheduler.AddTask(context.Background(), tt.task); err != nil {
				t.Fatalf("Failed to add to schdeduler %v", err)
			}

			if !tt.want.IsZero() {
				task, err := scheduler.StorageEngine.GetTaskByName(context.Background(), tt.task.Name)
				if err != nil {
					t.Fatalf("Failed to get task from scheduler %v", err)
				}

				if task.NextRunAt.Minute() != tt.want.Minute() &&
					task.NextRunAt.Hour() != tt.want.Hour() {
					t.Errorf("NextRunAt = %v, want %v", task.NextRunAt, tt.want)
				}
			}
		})
	}

	scheduler.StorageEngine.Flush(context.Background())
}
