package inmemory

import (
	"context"

	"github.com/knightfall22/augusta/internal/domain"
)

type InMemStorage struct {
	tasks map[string]*domain.Task
}

func NewInMemStorage() *InMemStorage {
	return &InMemStorage{
		tasks: make(map[string]*domain.Task),
	}
}

func (i *InMemStorage) AddTask(ctx context.Context, task *domain.Task) error {
	i.tasks[task.ID] = task
	return nil
}

func (i *InMemStorage) GetTask(ctx context.Context, taskID string) (*domain.Task, error) {
	return i.tasks[taskID], nil
}

func (i *InMemStorage) GetTaskByName(ctx context.Context, taskName string) (*domain.Task, error) {
	for _, task := range i.tasks {
		if task.Name == taskName {
			return task, nil
		}
	}
	return nil, nil
}
