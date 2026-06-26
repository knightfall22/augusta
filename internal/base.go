package internal

import (
	"context"

	"github.com/knightfall22/augusta/internal/domain"
)

type StorageEngine interface {
	AddTask(ctx context.Context, task *domain.Task) error
	GetTask(ctx context.Context, taskID string) (*domain.Task, error)
	DeleteTask(ctx context.Context, taskID string) error

	//Used only for testing
	GetTaskByName(ctx context.Context, taskName string) (*domain.Task, error)
	Flush() error
}

// LeaseStorage provides the atomic storage interface required to facilitate
// distributed leader election. It abstracts the underlying database (e.g., MongoDB)
// to manage the lifecycle of the shared lease document. By utilizing atomic
// Compare-And-Swap (CAS) operations, this layer ensures that only one scheduler
// instance can successfully claim or renew the leadership lock at any given time.
// It allows the Elector to safely heartbeat its ownership, or steal the lease if
// the current leader crashes and its lock Time-To-Live (TTL) expires, completely
// preventing split-brain scenarios across the cluster.
type LeaseStorage interface {
	AquireLease(ctx context.Context, lease *domain.Lease) error
	GetLease(ctx context.Context) (*domain.Lease, error)
	DeleteLease(ctx context.Context, candidateID string) error
	Flush() error
}
