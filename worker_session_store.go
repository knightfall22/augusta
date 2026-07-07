package augusta

import (
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/knightfall22/augusta/internal/api/v1"
	"github.com/knightfall22/augusta/internal/domain"
)

type WorkerSession struct {
	WorkerID      string
	Tags          []string
	Stream        pb.SchedulerService_ConnectSessionServer
	taskChannel   chan []*domain.Task
	LastHeartbeat time.Time
	started       bool
}

type WorkerSessionStore struct {
	Workers map[string]*WorkerSession
	// TODO: find a way to prevent tasks to be scheduled on workers that are on notice
	notice         map[string]struct{}
	aggregiousTime time.Duration
	sync.RWMutex
}

func NewWorkerSessionStore(aggregiousTime time.Duration) *WorkerSessionStore {
	if aggregiousTime == 0 {
		aggregiousTime = time.Duration(60 * time.Second)
	}
	return &WorkerSessionStore{
		Workers:        make(map[string]*WorkerSession),
		aggregiousTime: aggregiousTime,
		notice:         make(map[string]struct{}),
	}
}

func (w *WorkerSessionStore) GetWorkerSession(workerID string) (*WorkerSession, error) {
	w.RLock()
	defer w.RUnlock()
	if _, ok := w.Workers[workerID]; !ok {
		return nil, fmt.Errorf("worker not found")
	}

	return w.Workers[workerID], nil
}

func (w *WorkerSessionStore) SetWorkerSession(workerID string, session *WorkerSession) (bool, error) {
	w.Lock()
	defer w.Unlock()
	if _, ok := w.Workers[workerID]; !ok {
		w.Workers[workerID] = session
		log.Printf("Worker session created: %v", len(w.Workers))
		return false, nil
	}

	delete(w.notice, workerID)

	w.Workers[workerID].LastHeartbeat = time.Now()

	return true, nil
}

func (w *WorkerSessionStore) DeleteWorkerSession(workerID string) error {
	w.Lock()
	defer w.Unlock()
	delete(w.Workers, workerID)

	return nil
}

func (w *WorkerSessionStore) DiscardFaultyWorkers() error {
	w.Lock()
	defer w.Unlock()

	for k, v := range w.Workers {
		if time.Now().UTC().After(v.LastHeartbeat) {
			if time.Now().UTC().Sub(v.LastHeartbeat) > w.aggregiousTime {
				delete(w.Workers, k)
				delete(w.notice, k)
				continue
			}
			w.notice[k] = struct{}{}
		}
	}

	return nil
}

func (w *WorkerSessionStore) UpdateWorkerSession(workerID string) error {
	w.Lock()
	defer w.Unlock()

	if _, ok := w.Workers[workerID]; ok {
		w.Workers[workerID].LastHeartbeat = time.Now()
	}

	return nil
}

func (w *WorkerSessionStore) GetAllSessions() []*WorkerSession {
	if len(w.Workers) > 0 {
		result := make([]*WorkerSession, 0)

		for _, v := range w.Workers {
			if _, ok := w.notice[v.WorkerID]; ok {
				continue
			}
			result = append(result, v)
		}

		return result
	}

	return nil
}
