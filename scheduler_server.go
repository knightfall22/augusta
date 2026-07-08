package augusta

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"log"

	"github.com/knightfall22/augusta/internal/domain"
	"github.com/knightfall22/augusta/utils"
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
	s.Router.HandleFunc("DELETE /tasks/{id}/disable", s.DisableTask)
	s.Router.HandleFunc("PATCH /tasks/{id}/enable", s.EnableTask)
	return nil
}

func NewSchedulerServer(address string, scheduler *Scheduler, logger *logrus.Logger) *SchedulerServer {
	if logger == nil {
		logger = logrus.New()
		logger.SetFormatter(logrus.StandardLogger().Formatter)
	}

	return &SchedulerServer{
		Address:   address,
		Scheduler: scheduler,
		logger:    logger,
	}
}

func (s *SchedulerServer) Start() error {

	if err := s.Scheduler.Start(); err != nil {
		return err
	}

	s.initRouter()

	s.srv = &http.Server{
		Addr:    s.Address,
		Handler: s.Router,
	}

	go func() {
		s.logger.Infof("[INFO] Starting scheduler server on %s", s.Address)
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

	w.Header().Set("Content-Type", "application/json")
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

	w.Header().Set("Content-Type", "application/json")
	task, err := s.Scheduler.GetTask(r.Context(), taskID)
	if err != nil {
		logger.Error(err)
		var response domain.APIResponse
		if err == utils.ErrNoTaskFound {
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

func (s *SchedulerServer) DisableTask(w http.ResponseWriter, r *http.Request) {
	logger := s.logger.WithContext(r.Context()).WithField("method", "DeleteTask")
	taskID := r.PathValue("id")

	w.Header().Set("Content-Type", "application/json")
	if err := s.Scheduler.DisableTask(r.Context(), taskID); err != nil {
		logger.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	response := domain.APIResponse{
		Status:  "ok",
		Error:   false,
		Message: "Task disables successfully",
		Data:    nil,
	}
	json.NewEncoder(w).Encode(response)
}

func (s *SchedulerServer) DeleteTask(w http.ResponseWriter, r *http.Request) {
	logger := s.logger.WithContext(r.Context()).WithField("method", "DeleteTask")
	taskID := r.PathValue("id")

	w.Header().Set("Content-Type", "application/json")
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

func (s *SchedulerServer) EnableTask(w http.ResponseWriter, r *http.Request) {
	logger := s.logger.WithContext(r.Context()).WithField("method", "EnableTask")
	taskID := r.PathValue("id")

	w.Header().Set("Content-Type", "application/json")
	if err := s.Scheduler.EnableTask(r.Context(), taskID); err != nil {
		logger.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	response := domain.APIResponse{
		Status:  "ok",
		Error:   false,
		Message: "Task enabled successfully",
		Data:    nil,
	}
	json.NewEncoder(w).Encode(response)
}

func (s *SchedulerServer) HTTPHealthCheck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if err := s.Scheduler.ctx.Err(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		response := domain.APIResponse{
			Status:  "down",
			Error:   true,
			Message: s.Scheduler.ctx.Err().Error(),
			Data:    nil,
		}
		json.NewEncoder(w).Encode(response)
		return
	} else if err := s.Scheduler.StorageEngine.CheckConnection(r.Context()); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		response := domain.APIResponse{
			Status:  "down",
			Error:   true,
			Message: err.Error(),
			Data:    nil,
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	w.WriteHeader(http.StatusOK)
	response := domain.APIResponse{
		Status:  "up",
		Error:   false,
		Message: "OK",
		Data:    nil,
	}
	json.NewEncoder(w).Encode(response)
}

// The Leader Probe (For the gRPC Worker Target Group)
// The Load Balancer uses this to route long-lived worker streams.
// It returns 200 OK ONLY if this node holds the database lease.
func (s *SchedulerServer) HTTPLeaderProbe(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	state := s.Scheduler.GetState()
	if state == Leader {
		w.WriteHeader(http.StatusOK)
		response := domain.APIResponse{
			Status:  "ok",
			Error:   false,
			Message: "OK",
			Data:    nil,
		}
		json.NewEncoder(w).Encode(response)
		return
	}

	w.WriteHeader(http.StatusServiceUnavailable)
	response := domain.APIResponse{
		Status:  "error",
		Error:   true,
		Message: "Not the leader",
		Data:    nil,
	}
	json.NewEncoder(w).Encode(response)
}

func (s *SchedulerServer) Stop() error {
	if err := s.Scheduler.Stop(); err != nil {
		return err
	}

	return s.srv.Shutdown(context.Background())
}
