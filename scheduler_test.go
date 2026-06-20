package augusta

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/knightfall22/augusta/internal/domain"
	storage "github.com/knightfall22/augusta/internal/storage/inmemory"
)

func TestAddTaskToScheduler(t *testing.T) {
	scheduler := NewScheduler(storage.NewInMemStorage())

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
				Command:  "test",
			},
		},
		{
			name: "Add task to scheduler with schedule",
			task: &domain.AddTask{
				Name:     "test2",
				TaskType: "test",
				Command:  "test",
				Schedule: "PT2M",
			},
			want: time.Now().UTC().Add(2 * time.Minute),
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

				log.Println("HHHH", task.NextRunAt)
				log.Printf("%+v", task)

				if task.NextRunAt.Minute() != tt.want.Minute() &&
					task.NextRunAt.Hour() != tt.want.Hour() {
					t.Errorf("NextRunAt = %v, want %v", task.NextRunAt, tt.want)
				}
			}
		})
	}

}
