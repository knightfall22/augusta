package main

import (
	"github.com/knightfall22/augusta"
	storage "github.com/knightfall22/augusta/internal/storage/mongodb"
)

func main() {

	se, _ := storage.NewMongoStore("augusta", "mongodb+srv://viktorhadrian066_db_user:348HStKudVJbgJC@augusta.n3qffzj.mongodb.net/?appName=Augusta")
	scheduler := augusta.NewScheduler(se)
	schedulerServer := augusta.SchedulerServer{
		Scheduler: scheduler,
		Address:   "localhost:8080",
	}
	schedulerServer.Start()

	quit := make(chan struct{})
	<-quit
}
