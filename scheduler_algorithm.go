package augusta

import (
	"github.com/knightfall22/augusta/internal/domain"
)

type RoundRobin struct {
	Name       string
	LastWorker int
}

func (r *RoundRobin) SelectCandidateWorkers(t []*domain.Task, workers []*WorkerSession) []*WorkerSession {
	return workers
}

func (r *RoundRobin) Score(t []*domain.Task, workers []*WorkerSession) map[string]float64 {
	workerScores := make(map[string]float64)

	var newWorker int
	if r.LastWorker+1 < len(workers) {
		newWorker = r.LastWorker + 1
		r.LastWorker++
	} else {
		newWorker = 0
		r.LastWorker = 0
	}

	for idx, worker := range workers {
		if idx == newWorker {
			workerScores[worker.WorkerID] = 0.1
		} else {
			workerScores[worker.WorkerID] = 1
		}
	}

	return workerScores
}

func (r *RoundRobin) Pick(scores map[string]float64, candidates []*WorkerSession) *WorkerSession {
	var bestWorker *WorkerSession
	var lowestScore float64

	for idx, worker := range candidates {
		if idx == 0 {
			bestWorker = worker
			lowestScore = scores[worker.WorkerID]
			continue
		}

		if scores[worker.WorkerID] < lowestScore {
			bestWorker = worker
			lowestScore = scores[worker.WorkerID]
		}
	}

	return bestWorker
}
