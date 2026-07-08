package augusta

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/knightfall22/augusta/internal"
	"github.com/knightfall22/augusta/internal/domain"
	inMemoryStorage "github.com/knightfall22/augusta/internal/storage/inmemory"
	mongoStorage "github.com/knightfall22/augusta/internal/storage/mongodb"
	"github.com/sirupsen/logrus"
)

func setup(t *testing.T) internal.StorageEngine {
	var store internal.StorageEngine
	if stoageEngine == "mongodb" {
		se, err := mongoStorage.NewMongoStore("augusta-test", mongoURI)
		if err != nil {
			t.Fatal(err)
		}

		store = se

	} else {
		se := inMemoryStorage.NewInMemStorage()

		store = se
	}

	return store
}

func TestAddTask(t *testing.T) {
	store := setup(t)
	defer store.Flush(context.Background())

	tests := []struct {
		name           string
		payload        string
		expectedStatus int
		expectedMsg    string
		expectedErr    bool
	}{
		{
			name:           "Success - Valid Task",
			payload:        `{"name": "db-backup", "task_type": "script", "command": "ZWNobyBoZWxsbw=="}`,
			expectedStatus: http.StatusCreated,
			expectedMsg:    "Task added successfully",
			expectedErr:    false,
		},
		{
			name:           "Error - Invalid JSON format",
			payload:        `{"name": "broken-task", `,
			expectedStatus: http.StatusInternalServerError,
			expectedMsg:    "error decoding request body",
			expectedErr:    true,
		},
		{
			name:           "Error - Unknown Field (Disallowed)",
			payload:        `{"name": "test", "task_type": "script", "command": "ZWNobyBoZWxsbw==", "unknown_flag": true}`,
			expectedStatus: http.StatusInternalServerError,
			expectedMsg:    "error decoding request body",
			expectedErr:    true,
		},
		{
			name:           "Error - Validation Failure (Missing Name)",
			payload:        `{"task_type": "script", "command": "ZWNobyBoZWxsbw=="}`,
			expectedStatus: http.StatusBadRequest,
			expectedMsg:    "name is required",
			expectedErr:    true,
		},
		{
			name:           "Error - Validation Failure (Missing Task Type)",
			payload:        `{"name": "db-backup", "command": "ZWNobyBoZWxsbw=="}`,
			expectedStatus: http.StatusBadRequest,
			expectedMsg:    "task type is required",
			expectedErr:    true,
		},
		{
			name:           "Error - Validation Failure (Missing Command)",
			payload:        `{"name": "db-backup", "task_type": "script"}`,
			expectedStatus: http.StatusBadRequest,
			expectedMsg:    "command is required",
			expectedErr:    true,
		},
		{
			name:           "Error - Validation Failure (Invalid Schedule Format)",
			payload:        `{"name": "db-backup", "task_type": "script", "command": "ZWNobyBoZWxsbw==", "schedule": "invalid-iso8601"}`,
			expectedStatus: http.StatusBadRequest,
			expectedMsg:    "invalid ISO 8601 input", // or "expected 'P'" depending on your exact duration parser output
			expectedErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			logger := logrus.New()
			logger.SetOutput(io.Discard)

			sched := &Scheduler{
				StorageEngine: store,
				Logger:        logrus.NewEntry(logger),
			}

			server := &SchedulerServer{
				Scheduler: sched,
				logger:    logger,
			}

			req := httptest.NewRequest(http.MethodPost, "/tasks", bytes.NewBufferString(tt.payload))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			server.AddTask(rec, req)

			res := rec.Result()
			defer res.Body.Close()

			if res.StatusCode != tt.expectedStatus {
				t.Errorf("Expected status code %d, but got %d", tt.expectedStatus, res.StatusCode)
			}

			var resp domain.APIResponse
			if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
				t.Fatalf("Failed to decode response body: %v", err)
			}

			if !strings.Contains(resp.Message, tt.expectedMsg) {
				t.Errorf("Expected message to contain %q, but got %q", tt.expectedMsg, resp.Message)
			}

			if resp.Error != tt.expectedErr {
				t.Errorf("Expected error flag to be %v, but got %v", tt.expectedErr, resp.Error)
			}
		})
	}
}

func TestGetTask(t *testing.T) {
	store := setup(t)
	defer store.Flush(context.Background())

	validTask := &domain.Task{
		ID:       "123e4567-e89b-12d3-a456-426614174000",
		Name:     "db-backup",
		TaskType: "script",
		Status:   domain.Pending,
	}

	tests := []struct {
		name           string
		taskID         string
		expectedStatus int
		expectedMsg    string
		expectedErr    bool
	}{
		{
			name:           "Success - Task Found",
			taskID:         "123e4567-e89b-12d3-a456-426614174000",
			expectedStatus: http.StatusOK,
			expectedMsg:    "Task fetched successfully",
			expectedErr:    false,
		},
		{
			name:           "Error - Task Not Found",
			taskID:         "missing-task-id",
			expectedStatus: http.StatusNotFound,
			expectedMsg:    "task not found",
			expectedErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Seed the storage engine with our valid task to test fetching
			_ = store.AddTask(context.Background(), validTask)

			logger := logrus.New()
			logger.SetOutput(io.Discard)

			sched := &Scheduler{
				StorageEngine: store,
				Logger:        logrus.NewEntry(logger),
			}

			server := &SchedulerServer{
				Scheduler: sched,
				logger:    logger,
			}

			req := httptest.NewRequest(http.MethodGet, "/tasks/"+tt.taskID, nil)
			req.SetPathValue("id", tt.taskID)

			rec := httptest.NewRecorder()
			server.GetTask(rec, req)

			res := rec.Result()
			defer res.Body.Close()

			if res.StatusCode != tt.expectedStatus {
				t.Errorf("Expected status code %d, but got %d", tt.expectedStatus, res.StatusCode)
			}

			var resp domain.APIResponse
			if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
				t.Fatalf("Failed to decode response body: %v", err)
			}

			if !strings.Contains(resp.Message, tt.expectedMsg) {
				t.Errorf("Expected message to contain %q, but got %q", tt.expectedMsg, resp.Message)
			}

			if resp.Error != tt.expectedErr {
				t.Errorf("Expected error flag to be %v, but got %v", tt.expectedErr, resp.Error)
			}
		})
	}
}

func TestDeleteTask(t *testing.T) {
	store := setup(t)
	defer store.Flush(context.Background())

	validTask := &domain.Task{
		ID:   "123e4567-e89b-12d3-a456-426614174000",
		Name: "db-backup",
	}

	tests := []struct {
		name           string
		taskID         string
		expectedStatus int
		expectedMsg    string
	}{
		{
			name:           "Success - Task Deleted",
			taskID:         "123e4567-e89b-12d3-a456-426614174000",
			expectedStatus: http.StatusOK,
			expectedMsg:    "Task deleted successfully",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = store.AddTask(context.Background(), validTask)

			logger := logrus.New()
			logger.SetOutput(io.Discard)

			sched := &Scheduler{
				StorageEngine: store,
				Logger:        logrus.NewEntry(logger),
			}

			server := &SchedulerServer{
				Scheduler: sched,
				logger:    logger,
			}

			req := httptest.NewRequest(http.MethodDelete, "/tasks/"+tt.taskID, nil)
			req.SetPathValue("id", tt.taskID)

			rec := httptest.NewRecorder()
			server.DeleteTask(rec, req)

			res := rec.Result()
			defer res.Body.Close()

			if res.StatusCode != tt.expectedStatus {
				t.Errorf("Expected status code %d, but got %d", tt.expectedStatus, res.StatusCode)
			}

			if tt.expectedStatus == http.StatusOK {
				var resp domain.APIResponse
				if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
					t.Fatalf("Failed to decode success response body: %v", err)
				}
				if !strings.Contains(resp.Message, tt.expectedMsg) {
					t.Errorf("Expected message to contain %q, but got %q", tt.expectedMsg, resp.Message)
				}

				// Optional: Verify that it was actually deleted from storage
				fetchedTask, _ := store.GetTask(context.Background(), tt.taskID)
				if fetchedTask != nil {
					t.Errorf("Expected task to be deleted, but it was still found in storage")
				}
			}
		})
	}
}

func TestDisableTask(t *testing.T) {
	store := setup(t)
	defer store.Flush(context.Background())

	validTask := &domain.Task{
		ID:   "123e4567-e89b-12d3-a456-426614174000",
		Name: "db-backup",
	}

	tests := []struct {
		name           string
		taskID         string
		expectedStatus int
		expectedMsg    string
	}{
		{
			name:           "Success - Task Disabled",
			taskID:         "123e4567-e89b-12d3-a456-426614174000",
			expectedStatus: http.StatusOK,
			expectedMsg:    "Task disables successfully",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = store.AddTask(context.Background(), validTask)

			logger := logrus.New()
			logger.SetOutput(io.Discard)

			sched := &Scheduler{
				StorageEngine: store,
				Logger:        logrus.NewEntry(logger),
			}

			server := &SchedulerServer{
				Scheduler: sched,
				logger:    logger,
			}

			req := httptest.NewRequest(http.MethodDelete, "/tasks/disable/"+tt.taskID, nil)
			req.SetPathValue("id", tt.taskID)

			rec := httptest.NewRecorder()
			server.DisableTask(rec, req)

			res := rec.Result()
			defer res.Body.Close()

			if res.StatusCode != tt.expectedStatus {
				t.Errorf("Expected status code %d, but got %d", tt.expectedStatus, res.StatusCode)
			}

			if tt.expectedStatus == http.StatusOK {
				var resp domain.APIResponse
				if err := json.NewDecoder(res.Body).Decode(&resp); err != nil {
					t.Fatalf("Failed to decode success response body: %v", err)
				}

				if !strings.Contains(resp.Message, tt.expectedMsg) {
					t.Errorf("Expected message to contain %q, but got %q", tt.expectedMsg, resp.Message)
				}

				//check if it was actually disabled
				fetchedTask, _ := store.GetTask(context.Background(), tt.taskID)
				if fetchedTask.Disabled != true {
					t.Errorf("Expected task to be disabled, but it was still found in storage")
				}
			}
		})
	}
}
