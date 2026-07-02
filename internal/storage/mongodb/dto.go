package storage

import (
	"time"

	"github.com/knightfall22/augusta/internal/domain"
)

type tasks struct {
	ID                string        `bson:"_id"`
	Name              string        `bson:"name"`
	TaskType          string        `bson:"task_type"`
	Command           []byte        `bson:"command"`
	Disabled          bool          `bson:"disabled"`
	CurrentRetries    int           `bson:"current_retries"`
	Reoccurrence      int           `bson:"reoccurrence"`
	Retries           int           `bson:"retries"`
	Epsilon           string        `bson:"epsilon"`
	LastSuccess       time.Time     `bson:"last_success"`
	LastError         time.Time     `bson:"last_error"`
	Schedule          string        `bson:"schedule"`
	VisibilityTimeout time.Time     `bson:"visibility_timeout"`
	Status            domain.Status `bson:"status"`
	NextRunAt         time.Time     `bson:"next_run_at"`
	LastRunAt         time.Time     `bson:"last_run_at"`
}

func (t *tasks) toDomain() *domain.Task {
	return &domain.Task{
		ID:             t.ID,
		Name:           t.Name,
		TaskType:       t.TaskType,
		Command:        t.Command,
		Disabled:       t.Disabled,
		Retries:        t.Retries,
		CurrentRetries: t.CurrentRetries,
		Reoccurrence:   t.Reoccurrence,
		Epsilon:        t.Epsilon,
		LastSuccess:    t.LastSuccess,
		LastError:      t.LastError,
		Schedule:       t.Schedule,
		NextRunAt:      t.NextRunAt,
		LastRunAt:      t.LastRunAt,
		Status:         t.Status,
	}
}

func (t *tasks) fromDomain(task *domain.Task) {
	t.ID = task.ID
	t.Name = task.Name
	t.TaskType = task.TaskType
	t.Command = task.Command
	t.Disabled = task.Disabled
	t.Retries = task.Retries
	t.CurrentRetries = task.CurrentRetries
	t.Epsilon = task.Epsilon
	t.LastSuccess = task.LastSuccess
	t.LastError = task.LastError
	t.Schedule = task.Schedule
	t.NextRunAt = task.NextRunAt
	t.LastRunAt = task.LastRunAt
	t.Status = task.Status
	t.Reoccurrence = task.Reoccurrence
}

type leaseModel struct {
	ID          string    `bson:"_id"`
	CandidateID string    `bson:"candidate_id"`
	LastRenewed time.Time `bson:"last_renewed"`
	LastAquired time.Time `bson:"last_aquired"`
	ExpiresAt   time.Time `bson:"expires_at"`
}

func (l *leaseModel) toDomain() *domain.Lease {
	if l == nil {
		return nil
	}
	return &domain.Lease{
		CandidateID: l.CandidateID,
		LastRenewed: l.LastRenewed,
		LastAquired: l.LastAquired,
		ExpiresAt:   l.ExpiresAt,
	}
}

func (l *leaseModel) fromDomain(lease *domain.Lease) {
	l.ID = leaseID
	l.CandidateID = lease.CandidateID
	l.LastRenewed = lease.LastRenewed
	l.LastAquired = lease.LastAquired
	l.ExpiresAt = lease.ExpiresAt
}
