package internal

import (
	"context"
	"math/rand/v2"
	"time"

	pb "github.com/knightfall22/augusta/internal/api/v1"
	"github.com/knightfall22/augusta/internal/domain"
	"github.com/sosodev/duration"
)

const DefaultRetries = 3

const DefaultEpsilon = "PT10S"

var DefaultWorkerLeaseDuration = time.Duration(60 * time.Second)

func CalculateExponientialBackoff(epsilon string, attempts int) time.Time {
	ep, err := duration.Parse(epsilon)
	if err != nil {
		ep, _ = duration.Parse(DefaultEpsilon)
	}
	epsilonDuration := int(ep.ToTimeDuration().Seconds())

	delay := epsilonDuration * (2 << attempts)
	max := 600

	capped := min(delay, max)

	jitter := time.Duration(capped/2+rand.IntN(capped/2)) * time.Second

	return time.Now().UTC().Add(time.Duration(capped) * time.Second).Add(jitter)

}

type StorageEngine interface {
	AddTask(ctx context.Context, task *domain.Task) error
	GetTask(ctx context.Context, taskID string) (*domain.Task, error)
	DeleteTask(ctx context.Context, taskID string) error

	GetPendingTasks(ctx context.Context) ([]*domain.Task, error)
	GetLeaseExpiredTasks(ctx context.Context) error
	ExtendTaskLease(ctx context.Context, taskID []string) error

	ProcessTaskResult(ctx context.Context, result *pb.TaskResult) error
	ProcessBatchTaskResult(ctx context.Context, results []*pb.TaskResult) error

	//Used only for testing
	GetTaskByName(ctx context.Context, taskName string) (*domain.Task, error)
	Flush(ctx context.Context) error
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
	Flush(ctx context.Context) error
}

// type WokerSessionStore interface {}
