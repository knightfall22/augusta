package augusta

import (
	"fmt"
	"testing"
	"time"

	"github.com/knightfall22/augusta/internal"
	inMemoryStorage "github.com/knightfall22/augusta/internal/storage/inmemory"
	mongoStorage "github.com/knightfall22/augusta/internal/storage/mongodb"
	"github.com/sirupsen/logrus"
)

type TestHarness struct {
	//List of schedulers parcipating in the election
	scheduler []*Scheduler

	//Number of schedulers
	n int

	t *testing.T
}

type testHarnessOptions struct {
	StorageEngine string
	MongoURI      string
}

func NewTestHarness(t *testing.T, n int, opt testHarnessOptions) *TestHarness {
	schedulers := make([]*Scheduler, n)

	var se internal.StorageEngine
	var le internal.LeaseStorage
	if opt.StorageEngine == "mongodb" {
		store, err := mongoStorage.NewMongoStore("augusta", opt.MongoURI)
		if err != nil {
			t.Fatal(err)
		}
		se = store
		le = store
	} else {
		store := inMemoryStorage.NewInMemStorage()
		le = store
		se = store
	}

	logger := logrus.New()
	logger.SetFormatter(&logrus.JSONFormatter{})
	for id := range n {
		schedulers[id] = NewScheduler(SchedulerOptions{
			ID:            fmt.Sprintf("%d", id),
			StorageEngine: se,
			LeaseStorage:  le,
			LeaseDuration: 10,
			Logger:        logger,
			GRPCPort:      50051 + id + 1,
		})
	}

	for _, sh := range schedulers {
		sh.Start()
	}

	return &TestHarness{
		scheduler: schedulers,
		n:         n,
		t:         t,
	}
}

func (th *TestHarness) CheckLeader() string {
	leaderID := ""
	for range 5 {
		for _, s := range th.scheduler {
			if s.GetState() == Leader {
				if leaderID != "" {
					th.t.Fatalf("Multiple leaders found")
				}
				leaderID = s.ID
			}

		}

		sleepMs(100)
		if leaderID != "" {
			return leaderID
		}

	}
	th.t.Fatalf("Leader not found")
	return ""
}

func (th *TestHarness) StopScheduler(id string) {
	for _, s := range th.scheduler {
		if s.ID == id {
			s.Stop()
		}

	}
}

func (th *TestHarness) Report(id string) (state, string) {
	for _, s := range th.scheduler {
		if s.ID == id {
			return s.GetState(), s.listener.Addr().String()
		}
	}
	return 0, ""
}

func (th *TestHarness) Stop() {
	for _, s := range th.scheduler {
		s.StorageEngine.Flush()
		s.Elector.LeaseStorage.Flush()
		s.Stop()
	}
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}
