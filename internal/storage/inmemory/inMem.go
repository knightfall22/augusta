package inmemory

import (
	"context"
	"sync"
	"time"

	"github.com/knightfall22/augusta/internal"
	pb "github.com/knightfall22/augusta/internal/api/v1"
	"github.com/knightfall22/augusta/internal/domain"
)

type InMemStorage struct {
	sync.RWMutex
	lease *domain.Lease
	tasks map[string]*domain.Task
}

func NewInMemStorage() *InMemStorage {
	return &InMemStorage{
		tasks: make(map[string]*domain.Task),
	}
}

func (i *InMemStorage) AddTask(ctx context.Context, task *domain.Task) error {
	i.Lock()
	defer i.Unlock()

	i.tasks[task.ID] = task
	return nil
}

func (i *InMemStorage) GetTask(ctx context.Context, taskID string) (*domain.Task, error) {
	i.Lock()
	defer i.Unlock()

	return i.tasks[taskID], nil
}

func (i *InMemStorage) DeleteTask(ctx context.Context, taskID string) error {
	i.Lock()
	defer i.Unlock()

	delete(i.tasks, taskID)
	return nil
}

func (i *InMemStorage) GetTaskByName(ctx context.Context, taskName string) (*domain.Task, error) {
	i.Lock()
	defer i.Unlock()

	for _, task := range i.tasks {
		if task.Name == taskName {
			return task, nil
		}
	}
	return nil, nil
}

func (i *InMemStorage) AquireLease(ctx context.Context, lease *domain.Lease) error {
	i.Lock()
	defer i.Unlock()

	if i.lease == nil || time.Now().After(i.lease.ExpiresAt) {
		i.lease = lease
		return nil
	}

	if i.lease.CandidateID == lease.CandidateID {
		i.lease = lease
		return nil
	}

	return internal.ErrCannotAquireLock
}

func (i *InMemStorage) GetLease(ctx context.Context) (*domain.Lease, error) {
	i.Lock()
	defer i.Unlock()

	return i.lease, nil
}

func (i *InMemStorage) GetPendingTasks(ctx context.Context) ([]*domain.Task, error) {
	i.Lock()
	defer i.Unlock()

	return nil, nil
}

func (i *InMemStorage) GetLeaseExpiredTasks(ctx context.Context) error {
	i.Lock()
	defer i.Unlock()

	return nil
}

func (i *InMemStorage) DeleteLease(ctx context.Context, candidateID string) error {
	i.Lock()
	defer i.Unlock()

	i.lease = nil
	return nil
}

func (i *InMemStorage) ExtendTaskLease(ctx context.Context, taskID []string) error {
	i.Lock()
	defer i.Unlock()

	return nil
}

func (i *InMemStorage) ProcessTaskResult(ctx context.Context, result *pb.TaskResult) error {
	i.Lock()
	defer i.Unlock()

	return nil
}

func (i *InMemStorage) Flush() error {
	i.Lock()
	defer i.Unlock()

	i.tasks = make(map[string]*domain.Task)
	i.lease = nil
	return nil
}
