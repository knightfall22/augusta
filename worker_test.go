package augusta

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestWorkerInitialization(t *testing.T) {
	h := NewTestHarness(t, 3, testHarnessOptions{StorageEngine: stoageEngine, MongoURI: mongoURI})

	leaderID := h.CheckLeader()

	_, addr := h.Report(leaderID)

	w, err := NewWorker(WorkerOpts{
		ID:            uuid.New().String(),
		Tags:          []string{"tag1", "tag2"},
		SchedulerAddr: addr,
	})

	if err != nil {
		t.Fatal(err)
	}

	w.Close()

	h.Stop()
}

func TestWorkerStart(t *testing.T) {
	// h := NewTestHarness(t, 3, testHarnessOptions{StorageEngine: stoageEngine, MongoURI: mongoURI})

	// leaderID := h.CheckLeader()

	// _, addr := h.Report(leaderID)
	// _ = addr

	w, err := NewWorker(WorkerOpts{
		ID:                      uuid.New().String(),
		Tags:                    []string{"tag1", "tag2"},
		SchedulerAddr:           "localhost:50051",
		WorkerHeartbeatInterval: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	w.Start(context.Background())

	t.Log("Closing worker")
	time.Sleep(150 * time.Second)
	w.Close()

	// h.Stop()
}
