package internal

import (
	"context"

	"github.com/knightfall22/augusta/internal/domain"
)

type StorageEngine interface {
	AddTask(ctx context.Context, task *domain.Task) error
	GetTask(ctx context.Context, taskID string) (*domain.Task, error)

	//Used only for testing
	GetTaskByName(ctx context.Context, taskName string) (*domain.Task, error)
}
