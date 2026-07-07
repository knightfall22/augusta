package domain

import (
	"encoding/json"
	"errors"
	"time"

	pb "github.com/knightfall22/augusta/internal/api/v1"
	"github.com/sosodev/duration"
)

type Status string

const (
	Pending  Status = "pending"
	Queued   Status = "queued"
	Running  Status = "running"
	Failed   Status = "failed"
	Finished Status = "finished"
)

type Task struct {
	// ID is the unique identifier of the task. Uses UUID
	ID string

	// Name is the name of the task
	Name string

	// TaskType is the type of the task
	TaskType string

	// Command is the command to be executed. The scheduler is agnostic to the type of command.
	// Command should work in conjuction with the task type. Its up to the worker to determine how to execute the command
	Command []byte

	// Disabled is a boolean indicating whether the task is disabled
	Disabled bool

	// Retries is the number of times the task should be retried
	Retries int

	// CurrentRetries is the number of times the task has been retried in a session
	CurrentRetries int

	// Epsilon is the time interval after which the task should be retried.
	Epsilon string

	// LastSuccess is the time at which the task was last successful
	LastSuccess time.Time

	// LastError is the time at which the task was last failed
	LastError time.Time

	Reoccurrence int

	//Schedule is the time interval at which the task should be run. Use ISO8601 interval format.
	// Example:
	// "PT30M" for 30 minutes
	Schedule string

	//NextRunAt is the time at which the task should be run
	NextRunAt time.Time

	//LastRunAt is the time at which the task was last run
	LastRunAt time.Time

	Status Status
}

type AddTask struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	TaskType string          `json:"task_type"`
	Command  json.RawMessage `json:"command"`
	Disabled bool            `json:"disabled"`
	Retries  int             `json:"retries"`
	Epsilon  string          `json:"epsilon"`

	Schedule string `json:"schedule"`

	//Reoccurrence is the number of times the task should be run.
	//If set to 0, the task will run only once, otherwise it will run the specified number of times.
	//If set to -1, the task will run indefinitely
	Reoccurrence int `json:"reoccurrence"`
}

func (t *AddTask) Validate() error {
	if t.Name == "" {
		return errors.New("name is required")
	}
	if t.TaskType == "" {
		return errors.New("task type is required")
	}
	if t.Command == nil {
		return errors.New("command is required")
	}

	//If the schedule is not empty, validate it.
	//An empty schedule means the task to ran immediately and only once
	//Used with Reoccurence to run the task on a schedule
	if t.Schedule != "" {
		_, err := duration.Parse(t.Schedule)
		if err != nil {
			return err
		}
	}
	return nil
}

func SerializeTasksToProtobuf(tasks []*Task) []*pb.Task {
	pbTasks := make([]*pb.Task, len(tasks))
	for i, task := range tasks {
		pbTasks[i] = SerializeToProtobuf(task)
	}
	return pbTasks
}

func SerializeToProtobuf(task *Task) *pb.Task {
	return &pb.Task{
		Id:             task.ID,
		Name:           task.Name,
		TaskType:       task.TaskType,
		Command:        task.Command,
		Disabled:       task.Disabled,
		Retries:        int32(task.Retries),
		CurrentRetries: int32(task.CurrentRetries),
		Reoccurrence:   int64(task.Reoccurrence),
		Epsilon:        task.Epsilon,
		LastSuccess:    task.LastSuccess.Unix(),
		LastError:      task.LastError.Unix(),
		Schedule:       task.Schedule,
		Status:         string(task.Status),
		NextRunAt:      task.NextRunAt.Unix(),
		LastRunAt:      task.LastRunAt.Unix(),
	}
}

func DeserializeFromProtobuf(task *pb.Task) *Task {
	return &Task{
		ID:             task.Id,
		Name:           task.Name,
		TaskType:       task.TaskType,
		Command:        task.Command,
		Disabled:       task.Disabled,
		Retries:        int(task.Retries),
		CurrentRetries: int(task.CurrentRetries),
		Epsilon:        task.Epsilon,
		LastSuccess:    time.Unix(task.LastSuccess, 0),
		LastError:      time.Unix(task.LastError, 0),
		Schedule:       task.Schedule,
		Status:         Status(task.Status),
		NextRunAt:      time.Unix(task.NextRunAt, 0),
		LastRunAt:      time.Unix(task.LastRunAt, 0),
		Reoccurrence:   int(task.Reoccurrence),
	}
}
