package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

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
		GRPCPort:          50051,
		Logger:            logrus.New(),
		StorageEngine:     store,
		LeaseStorage:      store,
		LeaseDuration:     5,
		DispatcherTimeout: 5,
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

	time.Sleep(5 * time.Second)
}
