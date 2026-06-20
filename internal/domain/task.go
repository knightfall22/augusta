package domain

import (
	"errors"
	"time"

	"github.com/sosodev/duration"
)

const DefaultRetries = 3
const DefaultEpsilon = "1m"

type Task struct {
	ID          string
	Name        string
	TaskType    string
	Command     any
	Disabled    bool
	Retries     int
	Epsilon     string
	LastSuccess time.Time
	LastError   time.Time
	Schedule    string
	NextRunAt   time.Time
	LastRunAt   time.Time
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
