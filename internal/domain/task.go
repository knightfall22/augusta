package domain

import (
	"errors"
	"time"

	"github.com/sosodev/duration"
)

type Status string

const (
	Pending Status = "pending"
	Queued  Status = "queued"
	Failed  Status = "failed"
	Success Status = "success"
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
	Command any

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
	Name     string `json:"name"`
	TaskType string `json:"task_type"`
	Command  any    `json:"command"`
	Disabled bool   `json:"disabled"`
	Retries  int    `json:"retries"`
	Epsilon  string `json:"epsilon"`

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
