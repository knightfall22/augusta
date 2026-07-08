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
