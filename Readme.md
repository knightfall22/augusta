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
