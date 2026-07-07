package augusta

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWorkerSessionStore(t *testing.T) {
	t.Run("NewWorkerSessionStore", func(t *testing.T) {
		store := NewWorkerSessionStore(10 * time.Second)
		assert.NotNil(t, store)
		assert.Equal(t, 10*time.Second, store.aggregiousTime)
		assert.NotNil(t, store.Workers)
		assert.NotNil(t, store.notice)

		storeDefault := NewWorkerSessionStore(0)
		assert.Equal(t, 60*time.Second, storeDefault.aggregiousTime)
	})

	t.Run("SetWorkerSession and GetWorkerSession", func(t *testing.T) {
		store := NewWorkerSessionStore(10 * time.Second)
		session := &WorkerSession{WorkerID: "worker-1"}
		
		// Add new session
		isUpdate, err := store.SetWorkerSession("worker-1", session)
		assert.NoError(t, err)
		assert.False(t, isUpdate)

		// Get existing session
		retrieved, err := store.GetWorkerSession("worker-1")
		assert.NoError(t, err)
		assert.Equal(t, session, retrieved)

		// Get non-existing session
		_, err = store.GetWorkerSession("worker-2")
		assert.Error(t, err)
		assert.Equal(t, "worker not found", err.Error())

		// Update existing session
		store.notice["worker-1"] = struct{}{}
		isUpdate, err = store.SetWorkerSession("worker-1", session)
		assert.NoError(t, err)
		assert.True(t, isUpdate)
		assert.NotZero(t, retrieved.LastHeartbeat)
		
		// notice should be cleared upon receiving new session info / update
		_, hasNotice := store.notice["worker-1"]
		assert.False(t, hasNotice)
	})

	t.Run("DeleteWorkerSession", func(t *testing.T) {
		store := NewWorkerSessionStore(10 * time.Second)
		session := &WorkerSession{WorkerID: "worker-1"}
		store.SetWorkerSession("worker-1", session)

		err := store.DeleteWorkerSession("worker-1")
		assert.NoError(t, err)

		_, err = store.GetWorkerSession("worker-1")
		assert.Error(t, err)
	})

	t.Run("UpdateWorkerSession", func(t *testing.T) {
		store := NewWorkerSessionStore(10 * time.Second)
		session := &WorkerSession{WorkerID: "worker-1"}
		store.SetWorkerSession("worker-1", session)

		// Manually set an old heartbeat to see if it updates
		store.Lock()
		store.Workers["worker-1"].LastHeartbeat = time.Now().Add(-1 * time.Hour)
		oldHeartbeat := store.Workers["worker-1"].LastHeartbeat
		store.Unlock()

		err := store.UpdateWorkerSession("worker-1")
		assert.NoError(t, err)

		retrieved, _ := store.GetWorkerSession("worker-1")
		assert.True(t, retrieved.LastHeartbeat.After(oldHeartbeat))
	})

	t.Run("DiscardFaultyWorkers", func(t *testing.T) {
		store := NewWorkerSessionStore(1 * time.Second)
		
		// Active worker
		session1 := &WorkerSession{WorkerID: "worker-1"}
		store.SetWorkerSession("worker-1", session1)
		
		// Manually configure test scenarios
		store.Lock()
		// Set to future to bypass the time.Now().UTC().After(...) check which currently fires immediately
		store.Workers["worker-1"].LastHeartbeat = time.Now().Add(1 * time.Minute)
		
		// Notice worker (heartbeat older than now but within aggregiousTime)
		session2 := &WorkerSession{WorkerID: "worker-2", LastHeartbeat: time.Now().Add(-500 * time.Millisecond)}
		store.Workers["worker-2"] = session2

		// Aggregious worker (heartbeat way older)
		session3 := &WorkerSession{WorkerID: "worker-3", LastHeartbeat: time.Now().Add(-2 * time.Second)}
		store.Workers["worker-3"] = session3
		store.notice["worker-3"] = struct{}{}
		store.Unlock()

		err := store.DiscardFaultyWorkers()
		assert.NoError(t, err)

		store.RLock()
		// worker-1 should be active (no notice)
		_, ok := store.Workers["worker-1"]
		assert.True(t, ok)
		_, ok = store.notice["worker-1"]
		assert.False(t, ok)

		// worker-2 should be on notice
		_, ok = store.Workers["worker-2"]
		assert.True(t, ok)
		_, ok = store.notice["worker-2"]
		assert.True(t, ok)

		// worker-3 should be deleted
		_, ok = store.Workers["worker-3"]
		assert.False(t, ok)
		_, ok = store.notice["worker-3"]
		assert.False(t, ok)
		store.RUnlock()
	})

	t.Run("GetAllSessions", func(t *testing.T) {
		store := NewWorkerSessionStore(10 * time.Second)
		
		session1 := &WorkerSession{WorkerID: "worker-1"}
		session2 := &WorkerSession{WorkerID: "worker-2"}
		session3 := &WorkerSession{WorkerID: "worker-3"}
		
		store.Lock()
		store.Workers["worker-1"] = session1
		store.Workers["worker-2"] = session2
		store.Workers["worker-3"] = session3
		store.notice["worker-2"] = struct{}{} // put worker-2 on notice
		store.Unlock()

		sessions := store.GetAllSessions()
		
		assert.Len(t, sessions, 2)
		
		// Should contain worker-1 and worker-3, not worker-2
		var workerIDs []string
		for _, s := range sessions {
			workerIDs = append(workerIDs, s.WorkerID)
		}
		assert.Contains(t, workerIDs, "worker-1")
		assert.Contains(t, workerIDs, "worker-3")
		assert.NotContains(t, workerIDs, "worker-2")
	})

	t.Run("GetAllSessions Empty", func(t *testing.T) {
		store := NewWorkerSessionStore(10 * time.Second)
		
		sessions := store.GetAllSessions()
		assert.Nil(t, sessions)
	})
}
