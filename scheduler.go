package augusta

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/knightfall22/augusta/internal"
	"github.com/knightfall22/augusta/internal/domain"
	"github.com/sosodev/duration"
)

type Scheduler struct {
	StorageEngine internal.StorageEngine
}

func NewScheduler(storage internal.StorageEngine) *Scheduler {
	return &Scheduler{
		StorageEngine: storage,
	}
}

// AddTask adds a task to the storage engine.
// All schedulers regardless of role are able to add tasks
func (s *Scheduler) AddTask(ctx context.Context, addedTask *domain.AddTask) error {

	epsilon := addedTask.Epsilon
	if epsilon == "" {
		epsilon = domain.DefaultEpsilon
	}

	retries := addedTask.Retries
	if retries == 0 {
		retries = domain.DefaultRetries
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
