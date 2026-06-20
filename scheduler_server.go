package augusta

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"log"

	"github.com/knightfall22/augusta/internal/domain"
)

type SchedulerServer struct {
	Router    *http.ServeMux
	Address   string
	Scheduler *Scheduler

	srv *http.Server

	//TODO: add logger interface
}

func (s *SchedulerServer) initRouter() error {
	s.Router = http.NewServeMux()

	s.Router.HandleFunc("POST /tasks", s.AddTask)
	return nil
}

func (s *SchedulerServer) Start() error {

	s.initRouter()

	s.srv = &http.Server{
		Addr:    s.Address,
		Handler: s.Router,
	}

	go func() {
		log.Printf("[INFO] Starting scheduler server on %s", s.Address)
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Listen and serve error: %v\n", err)
		}
	}()

	return nil
}

func (s *SchedulerServer) AddTask(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	var task *domain.AddTask
	if err := d.Decode(&task); err != nil {
		//Todo: proper error management
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := task.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log.Printf("[INFO] Adding task %s", task.Name)
	if err := s.Scheduler.AddTask(r.Context(), task); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func (s *SchedulerServer) Stop() error {
	return s.srv.Shutdown(context.Background())
}
