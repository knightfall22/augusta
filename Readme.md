# Augusta

Augusta is an asynchronous task schdeduler, it uses a lease based leader election and Kubernetes style task allocation. It runs light-weight tasks and it takes an agnostic approach to commands being ran.

Highlevel overview:

- Client adds a task using rest API
- Leader scheduler allocates tasks on available workers using a scheduling alogrithm(eg. roundrobin)
- Worker runs a task and reports the result to the leader

## Features

- [x] Guaranteed exactly once delivery
- [x] Scheduling of tasks
- [x] Scheduler uses RoundRobin task allocation
- [x] Automatic task recovery in case of worker crash
- [x] Lease based leader election of schedulers
- [x] Enable and Disable tasks
- [x] Worker task batching
- [ ] Monitoring dashboard
- [ ] mTLS
- [ ] EPVM scheduling algorithm

## Quickstart

Install library

```bash
go get -u https://github.com/knightfall22/augusta
```

Start scheduler server:

```go
package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/knightfall22/augusta"
	mongoStorage "github.com/knightfall22/augusta/internal/storage/mongodb"
	"github.com/sirupsen/logrus"
)

func main() {
	dbName := "augusta"
	uri := "mongodb://127.0.0.1:27017"
	store, err := mongoStorage.NewMongoStore(dbName, uri)
	if err != nil {
		panic(err)
	}

	scheduler := augusta.NewScheduler(augusta.SchedulerOptions{
		GRPCPort:      50051,
		Logger:        logrus.New(),
		StorageEngine: store,
		LeaseStorage:  store,
		LeaseDuration: 5,
	})

	schedulerServer := augusta.NewSchedulerServer("127.0.0.1:8080", scheduler, nil)

	if err := schedulerServer.Start(); err != nil {
		panic(err)
	}

	//Note: this is not a blocking call and will return immediately
	//ensure you have a to prevent the program from exiting

	quit := make(chan os.Signal, 1)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	schedulerServer.Stop()

}
```

Start worker

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/knightfall22/augusta"
	"github.com/knightfall22/augusta/internal/domain"
)

type Processor struct {
}

func (p Processor) Process(ctx context.Context, task *domain.Task) error {
	log.Printf("Processing Task: %s", task.ID)
	return nil
}

func main() {

	worker, err := augusta.NewWorker(augusta.WorkerOpts{
		SchedulerAddr: "127.0.0.1:50051",
		Processor:     Processor{},
	})

	if err != nil {
		log.Fatal(err)
	}

	if err := worker.Start(context.Background()); err != nil {
		log.Fatal(err)
	}

	//Note: this is not a blocking call and will return immediately
	//ensure you have a to prevent the program from exiting

	quit := make(chan os.Signal, 1)

	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	worker.Close()
}

```

Here is the API documentation for the Augusta Scheduler Server, based on the provided source files.

### **API Endpoints Overview**

| HTTP Method | Endpoint              | Description                                                     |
| ----------- | --------------------- | --------------------------------------------------------------- |
| **POST**    | `/tasks`              | Creates and schedules a new task.                               |
| **GET**     | `/tasks/{id}`         | Retrieves the details and current status of a specific task.    |
| **DELETE**  | `/tasks/{id}`         | Permanently deletes a task from the scheduler.                  |
| **DELETE**  | `/tasks/{id}/disable` | Disables a task, preventing it from executing until re-enabled. |
| **PATCH**   | `/tasks/{id}/enable`  | Enables a previously disabled task.                             |

---

### **Endpoint Details & Examples**

The following examples assume the server is running on the default address from the quickstart guide: `http://127.0.0.1:8080`.

#### **1. Add a Task (`POST /tasks`)**

Creates a new task in the Augusta scheduler. The request body must be a JSON object matching the `AddTask` structure.

**AddTask Payload Fields**

| JSON Field     | Type       | Required | Description                                                                                                                                    |
| -------------- | ---------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `name`         | String     | **Yes**  | The descriptive name of the task.                                                                                                              |
| `task_type`    | String     | **Yes**  | The type of task. The scheduler uses this to route to the appropriate worker.                                                                  |
| `command`      | Raw Binary | **Yes**  | The specific command payload to be executed. The scheduler is agnostic to this data.                                                           |
| `id`           | String     | No       | The unique identifier (UUID) for the task. If omitted, the system typically generates one.                                                     |
| `disabled`     | Boolean    | No       | If set to `true`, the task is created but will not be scheduled to run.                                                                        |
| `retries`      | Integer    | No       | The maximum number of times the task should be retried upon failure.                                                                           |
| `epsilon`      | String     | No       | The time interval to wait before retrying a failed task.                                                                                       |
| `schedule`     | String     | No       | The execution schedule using ISO8601 interval format (e.g., `"PT30M"` for 30 minutes). If left empty, the task runs immediately and only once. |
| `reoccurrence` | Integer    | No       | How many times the task should run. `0` = runs once, `-1` = runs indefinitely. Used in conjunction with `schedule`.                            |

**Example Request:**

```bash
curl -X POST http://127.0.0.1:8080/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "process_video_queue",
    "task_type": "video_transcode",
    "command": {
      "video_id": "vid_98765",
      "resolution": "1080p"
    },
    "schedule": "PT15M",
    "reoccurrence": -1,
    "retries": 3
  }'

```

---

#### **2. Get Task Details (`GET /tasks/{id}`)**

Fetches the complete current state of a task by its ID, including its execution history (`last_success`, `last_error`, `status`, etc.).

**Example Request:**

```bash
curl -X GET http://127.0.0.1:8080/tasks/123e4567-e89b-12d3-a456-426614174000

```

---

#### **3. Delete a Task (`DELETE /tasks/{id}`)**

Removes a task entirely from the scheduler and the underlying storage engine.

**Example Request:**

```bash
curl -X DELETE http://127.0.0.1:8080/tasks/123e4567-e89b-12d3-a456-426614174000

```

---

#### **4. Disable a Task (`DELETE /tasks/{id}/disable`)**

Flags an existing task as disabled. The task will remain in the database but will not be picked up by the scheduler or allocated to workers.

**Example Request:**

```bash
curl -X DELETE http://127.0.0.1:8080/tasks/123e4567-e89b-12d3-a456-426614174000/disable

```

---

#### **5. Enable a Task (`PATCH /tasks/{id}/enable`)**

Re-enables a previously disabled task, allowing the scheduler to resume evaluating it for execution based on its `schedule` and `reoccurrence` rules.

**Example Request:**

```bash
curl -X PATCH http://127.0.0.1:8080/tasks/123e4567-e89b-12d3-a456-426614174000/enable

```
