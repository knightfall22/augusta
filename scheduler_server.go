package augusta

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"log"

	"github.com/knightfall22/augusta/internal"
	"github.com/knightfall22/augusta/internal/domain"
	"github.com/sirupsen/logrus"
)

type SchedulerServer struct {
	Router    *http.ServeMux
	Address   string
	Scheduler *Scheduler

	srv *http.Server

	logger *logrus.Logger
}

func (s *SchedulerServer) initRouter() error {
	s.Router = http.NewServeMux()

	s.Router.HandleFunc("POST /tasks", s.AddTask)
	s.Router.HandleFunc("GET /tasks/{id}", s.GetTask)
	s.Router.HandleFunc("DELETE /tasks/{id}", s.DeleteTask)
	return nil
}

func (s *SchedulerServer) Start() error {

	s.initRouter()

	s.srv = &http.Server{
		Addr:    s.Address,
		Handler: s.Router,
	}

	logger := logrus.New()
	logger.SetFormatter(logrus.StandardLogger().Formatter)
	s.logger = logger

	go func() {
		logger.Infof("[INFO] Starting scheduler server on %s", s.Address)
		if err := s.srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Listen and serve error: %v\n", err)
		}
	}()

	return nil
}

func (s *SchedulerServer) AddTask(w http.ResponseWriter, r *http.Request) {
	logger := s.logger.WithContext(r.Context()).WithField("method", "AddTask")
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()

	var task *domain.AddTask
	if err := d.Decode(&task); err != nil {
		logger.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		response := domain.APIResponse{
			Status:  "error",
			Error:   true,
			Message: "error decoding request body",
			Data:    nil,
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	if err := task.Validate(); err != nil {
		logger.Error(err)
		w.WriteHeader(http.StatusBadRequest)
		response := domain.APIResponse{
			Status:  "error",
			Error:   true,
			Message: err.Error(),
			Data:    nil,
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	if err := s.Scheduler.AddTask(r.Context(), task); err != nil {
		logger.Error(err)
		w.WriteHeader(http.StatusInternalServerError)
		response := domain.APIResponse{
			Status:  "error",
			Error:   true,
			Message: "error adding task",
			Data:    nil,
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	w.WriteHeader(http.StatusCreated)
	response := domain.APIResponse{
		Status:  "ok",
		Error:   false,
		Message: "Task added successfully",
		Data:    nil,
	}
	json.NewEncoder(w).Encode(response)
}

func (s *SchedulerServer) GetTask(w http.ResponseWriter, r *http.Request) {
	logger := s.logger.WithContext(r.Context()).WithField("method", "GetTask")
	taskID := r.PathValue("id")

	task, err := s.Scheduler.GetTask(r.Context(), taskID)
	if err != nil {
		logger.Error(err)
		var response domain.APIResponse
		if err == internal.ErrNoTaskFound {
			w.WriteHeader(http.StatusNotFound)
			response = domain.APIResponse{
				Status:  "error",
				Error:   true,
				Message: "task not found",
				Data:    nil,
			}
			json.NewEncoder(w).Encode(response)
			return
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			response := domain.APIResponse{
				Status:  "error",
				Error:   true,
				Message: "error getting task",
				Data:    nil,
			}
			json.NewEncoder(w).Encode(response)

			return
		}
	}

	w.WriteHeader(http.StatusOK)
	response := domain.APIResponse{
		Status:  "ok",
		Error:   false,
		Message: "Task fetched successfully",
		Data:    task,
	}
	json.NewEncoder(w).Encode(response)
}

func (s *SchedulerServer) DeleteTask(w http.ResponseWriter, r *http.Request) {
	logger := s.logger.WithContext(r.Context()).WithField("method", "DeleteTask")
	taskID := r.PathValue("id")

	if err := s.Scheduler.DeleteTask(r.Context(), taskID); err != nil {
		logger.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	response := domain.APIResponse{
		Status:  "ok",
		Error:   false,
		Message: "Task deleted successfully",
		Data:    nil,
	}
	json.NewEncoder(w).Encode(response)
}

func (s *SchedulerServer) Stop() error {
	return s.srv.Shutdown(context.Background())
}
