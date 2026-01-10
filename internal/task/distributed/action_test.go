package distributed

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/esadakcam/conductor/internal/task"
	"github.com/google/uuid"
)

func TestActionConfigValueSum_GetType(t *testing.T) {
	action := &ActionConfigValueSum{}
	if action.GetType() != task.ActionTypeConfigValueSum {
		t.Errorf("expected GetType() to return ActionTypeConfigValueSum, got %v", action.GetType())
	}
}

func TestActionK8sRestartDeployment_GetType(t *testing.T) {
	action := &ActionK8sRestartDeployment{}
	if action.GetType() != task.ActionTypeK8sRestartDeployment {
		t.Errorf("expected GetType() to return ActionTypeK8sRestartDeployment, got %v", action.GetType())
	}
}

func TestActionK8sWaitDeploymentRollout_GetType(t *testing.T) {
	action := &ActionK8sWaitDeploymentRollout{}
	if action.GetType() != task.ActionTypeK8sWaitDeploymentRollout {
		t.Errorf("expected GetType() to return ActionTypeK8sWaitDeploymentRollout, got %v", action.GetType())
	}
}

func TestActionK8sExecDeployment_GetType(t *testing.T) {
	action := &ActionK8sExecDeployment{}
	if action.GetType() != task.ActionTypeK8sExecDeployment {
		t.Errorf("expected GetType() to return ActionTypeK8sExecDeployment, got %v", action.GetType())
	}
}

func TestActionK8sExecDeployment_Execute(t *testing.T) {
	tests := []struct {
		name          string
		action        *ActionK8sExecDeployment
		epoch         int64
		serverHandler http.HandlerFunc
		expectedError bool
		errorContains string
	}{
		{
			name: "successful execution with default namespace",
			action: &ActionK8sExecDeployment{
				ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
					Deployment: "my-deployment",
					Command:    []string{"echo", "hello"},
				},
			},
			epoch: 123,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" {
					t.Errorf("expected POST method, got %s", r.Method)
				}
				expectedPath := "/api/v1/exec/deployments/default/my-deployment"
				if r.URL.Path != expectedPath {
					t.Errorf("expected path %s, got %s", expectedPath, r.URL.Path)
				}
				if r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("expected Content-Type application/json, got %s", r.Header.Get("Content-Type"))
				}
				var body map[string]interface{}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("failed to decode request body: %v", err)
				}
				if epoch, ok := body["epoch"].(float64); !ok || epoch != 123 {
					t.Errorf("expected epoch 123, got %v", body["epoch"])
				}
				cmd, ok := body["command"].([]interface{})
				if !ok {
					t.Errorf("expected command in body")
				}
				if len(cmd) != 2 || cmd[0] != "echo" || cmd[1] != "hello" {
					t.Errorf("expected command [echo hello], got %v", cmd)
				}
				if _, ok := body["container"]; ok {
					t.Errorf("expected no container in body when not specified")
				}
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"deploymentName": "my-deployment",
					"namespace":      "default",
					"results":        []interface{}{},
				})
			},
			expectedError: false,
		},
		{
			name: "successful execution with custom namespace",
			action: &ActionK8sExecDeployment{
				ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
					Deployment: "my-deployment",
					Namespace:  "custom-ns",
					Command:    []string{"ls", "-la"},
				},
			},
			epoch: 456,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				expectedPath := "/api/v1/exec/deployments/custom-ns/my-deployment"
				if r.URL.Path != expectedPath {
					t.Errorf("expected path %s, got %s", expectedPath, r.URL.Path)
				}
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{})
			},
			expectedError: false,
		},
		{
			name: "successful execution with container specified",
			action: &ActionK8sExecDeployment{
				ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
					Deployment: "my-deployment",
					Container:  "sidecar",
					Command:    []string{"cat", "/etc/config"},
				},
			},
			epoch: 789,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				var body map[string]interface{}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("failed to decode request body: %v", err)
				}
				container, ok := body["container"].(string)
				if !ok || container != "sidecar" {
					t.Errorf("expected container 'sidecar', got %v", body["container"])
				}
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{})
			},
			expectedError: false,
		},
		{
			name: "missing deployment name returns error",
			action: &ActionK8sExecDeployment{
				ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
					Deployment: "",
					Command:    []string{"echo"},
				},
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "deployment name is required",
		},
		{
			name: "missing member returns error",
			action: &ActionK8sExecDeployment{
				ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
					Deployment: "my-deployment",
					Command:    []string{"echo"},
				},
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "member is required",
		},
		{
			name: "missing command returns error",
			action: &ActionK8sExecDeployment{
				ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
					Member:     "http://localhost:8080",
					Deployment: "my-deployment",
					Command:    []string{},
				},
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "command is required",
		},
		{
			name: "nil command returns error",
			action: &ActionK8sExecDeployment{
				ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
					Member:     "http://localhost:8080",
					Deployment: "my-deployment",
					Command:    nil,
				},
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "command is required",
		},
		{
			name: "HTTP 400 Bad Request returns error",
			action: &ActionK8sExecDeployment{
				ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
					Deployment: "my-deployment",
					Command:    []string{"echo"},
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error": "command is required",
					"code":  400,
				})
			},
			expectedError: true,
			errorContains: "status code 400",
		},
		{
			name: "HTTP 404 Not Found returns error",
			action: &ActionK8sExecDeployment{
				ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
					Deployment: "nonexistent",
					Command:    []string{"echo"},
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("deployment not found"))
			},
			expectedError: true,
			errorContains: "status code 404",
		},
		{
			name: "HTTP 409 Conflict (stale epoch) returns error",
			action: &ActionK8sExecDeployment{
				ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
					Deployment: "my-deployment",
					Command:    []string{"echo"},
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusConflict)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error": "stale epoch",
					"code":  409,
				})
			},
			expectedError: true,
			errorContains: "status code 409",
		},
		{
			name: "HTTP 500 Internal Server Error returns error",
			action: &ActionK8sExecDeployment{
				ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
					Deployment: "my-deployment",
					Command:    []string{"echo"},
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("internal error"))
			},
			expectedError: true,
			errorContains: "status code 500",
		},
		{
			name: "complex command with arguments",
			action: &ActionK8sExecDeployment{
				ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
					Deployment: "my-deployment",
					Command:    []string{"sh", "-c", "echo $HOME && ls -la /tmp"},
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				var body map[string]interface{}
				json.NewDecoder(r.Body).Decode(&body)
				cmd := body["command"].([]interface{})
				if len(cmd) != 3 {
					t.Errorf("expected 3 command parts, got %d", len(cmd))
				}
				if cmd[0] != "sh" || cmd[1] != "-c" || cmd[2] != "echo $HOME && ls -la /tmp" {
					t.Errorf("unexpected command: %v", cmd)
				}
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{})
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up test server if handler is provided
			if tt.serverHandler != nil {
				server := httptest.NewServer(tt.serverHandler)
				defer server.Close()
				tt.action.Member = server.URL
			}

			payload := map[string]any{"idempotencyId": uuid.New().String(), "epoch": tt.epoch}
			err := tt.action.Execute(context.Background(), payload)

			if tt.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("expected error containing %q, got %q", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestActionK8sExecDeployment_Execute_InvalidEndpoint(t *testing.T) {
	action := &ActionK8sExecDeployment{
		ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
			Member:     "http://invalid-endpoint-that-does-not-exist-12345.local",
			Deployment: "my-deployment",
			Command:    []string{"echo"},
		},
	}

	payload := map[string]any{"idempotencyId": uuid.New().String(), "epoch": 1}
	err := action.Execute(context.Background(), payload)

	if err == nil {
		t.Errorf("expected error for invalid endpoint, got none")
	}
}

func TestActionK8sRestartDeployment_Execute(t *testing.T) {
	tests := []struct {
		name          string
		action        *ActionK8sRestartDeployment
		epoch         int64
		serverHandler http.HandlerFunc
		expectedError bool
		errorContains string
	}{
		{
			name: "successful deployment restart with default namespace",
			action: &ActionK8sRestartDeployment{
				ActionK8sRestartDeploymentData: task.ActionK8sRestartDeploymentData{
					Deployment: "test-deployment",
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "PATCH" {
					t.Errorf("expected PATCH method, got %s", r.Method)
				}
				expectedPath := "/api/v1/deployments/default/test-deployment"
				if r.URL.Path != expectedPath {
					t.Errorf("expected path %s, got %s", expectedPath, r.URL.Path)
				}
				var body map[string]interface{}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("failed to decode request body: %v", err)
				}
				if epoch, ok := body["epoch"].(float64); !ok || epoch != 1 {
					t.Errorf("expected epoch 1, got %v", body["epoch"])
				}
				patch, ok := body["patch"].(map[string]interface{})
				if !ok {
					t.Errorf("expected patch in body")
				}
				spec, ok := patch["spec"].(map[string]interface{})
				if !ok {
					t.Errorf("expected spec in patch")
				}
				template, ok := spec["template"].(map[string]interface{})
				if !ok {
					t.Errorf("expected template in spec")
				}
				metadata, ok := template["metadata"].(map[string]interface{})
				if !ok {
					t.Errorf("expected metadata in template")
				}
				annotations, ok := metadata["annotations"].(map[string]interface{})
				if !ok {
					t.Errorf("expected annotations in metadata")
				}
				if _, ok := annotations["kubectl.kubernetes.io/restartedAt"]; !ok {
					t.Errorf("expected restartedAt annotation")
				}
				w.WriteHeader(http.StatusOK)
			},
			expectedError: false,
		},
		{
			name: "successful deployment restart with custom namespace",
			action: &ActionK8sRestartDeployment{
				ActionK8sRestartDeploymentData: task.ActionK8sRestartDeploymentData{
					Deployment: "test-deployment",
					Namespace:  "custom-ns",
				},
			},
			epoch: 123,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				expectedPath := "/api/v1/deployments/custom-ns/test-deployment"
				if r.URL.Path != expectedPath {
					t.Errorf("expected path %s, got %s", expectedPath, r.URL.Path)
				}
				w.WriteHeader(http.StatusOK)
			},
			expectedError: false,
		},
		{
			name: "missing deployment name returns error",
			action: &ActionK8sRestartDeployment{
				ActionK8sRestartDeploymentData: task.ActionK8sRestartDeploymentData{
					Deployment: "",
				},
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "deployment name is required",
		},
		{
			name: "missing member returns error",
			action: &ActionK8sRestartDeployment{
				ActionK8sRestartDeploymentData: task.ActionK8sRestartDeploymentData{
					Deployment: "test-deployment",
				},
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "member is required",
		},
		{
			name: "patch failure returns error",
			action: &ActionK8sRestartDeployment{
				ActionK8sRestartDeploymentData: task.ActionK8sRestartDeploymentData{
					Deployment: "test-deployment",
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("Internal Server Error"))
			},
			expectedError: true,
			errorContains: "failed to restart deployment",
		},
		{
			name: "HTTP 409 Conflict (stale epoch) returns error",
			action: &ActionK8sRestartDeployment{
				ActionK8sRestartDeploymentData: task.ActionK8sRestartDeploymentData{
					Deployment: "test-deployment",
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusConflict)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error": "stale epoch",
					"code":  409,
				})
			},
			expectedError: true,
			errorContains: "failed to restart deployment",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up test server if handler is provided
			if tt.serverHandler != nil {
				server := httptest.NewServer(tt.serverHandler)
				defer server.Close()
				tt.action.Member = server.URL
			}

			payload := map[string]any{"idempotencyId": uuid.New().String(), "epoch": tt.epoch}
			err := tt.action.Execute(context.Background(), payload)

			if tt.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("expected error containing %q, got %q", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestActionK8sWaitDeploymentRollout_Execute(t *testing.T) {
	tests := []struct {
		name          string
		action        *ActionK8sWaitDeploymentRollout
		epoch         int64
		serverHandler http.HandlerFunc
		expectedError bool
		errorContains string
	}{
		{
			name: "successful wait with default namespace and timeout",
			action: &ActionK8sWaitDeploymentRollout{
				ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
					Deployment: "my-deployment",
				},
			},
			epoch: 123,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" {
					t.Errorf("expected POST method, got %s", r.Method)
				}
				expectedPath := "/api/v1/rollout/deployments/default/my-deployment"
				if r.URL.Path != expectedPath {
					t.Errorf("expected path %s, got %s", expectedPath, r.URL.Path)
				}
				if r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("expected Content-Type application/json, got %s", r.Header.Get("Content-Type"))
				}
				if r.Header.Get("X-Idempotency-Id") == "" {
					t.Errorf("expected X-Idempotency-Id header to be set")
				}
				var body map[string]interface{}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("failed to decode request body: %v", err)
				}
				if epoch, ok := body["epoch"].(float64); !ok || epoch != 123 {
					t.Errorf("expected epoch 123, got %v", body["epoch"])
				}
				// Default timeout is 5m
				if timeout, ok := body["timeout"].(string); !ok || timeout != "5m0s" {
					t.Errorf("expected timeout '5m0s', got %v", body["timeout"])
				}
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"deploymentName": "my-deployment",
					"namespace":      "default",
					"status":         "completed",
				})
			},
			expectedError: false,
		},
		{
			name: "successful wait with custom namespace",
			action: &ActionK8sWaitDeploymentRollout{
				ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
					Deployment: "my-deployment",
					Namespace:  "custom-ns",
				},
			},
			epoch: 456,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				expectedPath := "/api/v1/rollout/deployments/custom-ns/my-deployment"
				if r.URL.Path != expectedPath {
					t.Errorf("expected path %s, got %s", expectedPath, r.URL.Path)
				}
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"status": "completed",
				})
			},
			expectedError: false,
		},
		{
			name: "successful wait with custom timeout",
			action: &ActionK8sWaitDeploymentRollout{
				ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
					Deployment: "my-deployment",
					Timeout:    10 * time.Minute,
				},
			},
			epoch: 789,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				var body map[string]interface{}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("failed to decode request body: %v", err)
				}
				if timeout, ok := body["timeout"].(string); !ok || timeout != "10m0s" {
					t.Errorf("expected timeout '10m0s', got %v", body["timeout"])
				}
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{})
			},
			expectedError: false,
		},
		{
			name: "successful wait with short timeout",
			action: &ActionK8sWaitDeploymentRollout{
				ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
					Deployment: "my-deployment",
					Timeout:    30 * time.Second,
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				var body map[string]interface{}
				json.NewDecoder(r.Body).Decode(&body)
				if timeout, ok := body["timeout"].(string); !ok || timeout != "30s" {
					t.Errorf("expected timeout '30s', got %v", body["timeout"])
				}
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{})
			},
			expectedError: false,
		},
		{
			name: "missing deployment name returns error",
			action: &ActionK8sWaitDeploymentRollout{
				ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
					Deployment: "",
				},
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "deployment name is required",
		},
		{
			name: "missing member returns error",
			action: &ActionK8sWaitDeploymentRollout{
				ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
					Deployment: "my-deployment",
				},
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "member is required",
		},
		{
			name: "HTTP 400 Bad Request returns error",
			action: &ActionK8sWaitDeploymentRollout{
				ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
					Deployment: "my-deployment",
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error": "invalid request",
					"code":  400,
				})
			},
			expectedError: true,
			errorContains: "status code 400",
		},
		{
			name: "HTTP 404 Not Found returns error",
			action: &ActionK8sWaitDeploymentRollout{
				ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
					Deployment: "nonexistent",
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("deployment not found"))
			},
			expectedError: true,
			errorContains: "status code 404",
		},
		{
			name: "HTTP 409 Conflict (stale epoch) returns error",
			action: &ActionK8sWaitDeploymentRollout{
				ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
					Deployment: "my-deployment",
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusConflict)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error": "stale epoch",
					"code":  409,
				})
			},
			expectedError: true,
			errorContains: "status code 409",
		},
		{
			name: "HTTP 500 Internal Server Error returns error",
			action: &ActionK8sWaitDeploymentRollout{
				ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
					Deployment: "my-deployment",
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("timed out waiting for deployment"))
			},
			expectedError: true,
			errorContains: "status code 500",
		},
		{
			name: "HTTP 504 Gateway Timeout returns error",
			action: &ActionK8sWaitDeploymentRollout{
				ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
					Deployment: "my-deployment",
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusGatewayTimeout)
				w.Write([]byte("gateway timeout"))
			},
			expectedError: true,
			errorContains: "status code 504",
		},
		{
			name: "verifies idempotency header is sent",
			action: &ActionK8sWaitDeploymentRollout{
				ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
					Deployment: "my-deployment",
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				idempotencyId := r.Header.Get("X-Idempotency-Id")
				if idempotencyId == "" {
					t.Errorf("expected X-Idempotency-Id header to be set")
				}
				// UUID format validation (basic check)
				if len(idempotencyId) < 32 {
					t.Errorf("expected X-Idempotency-Id to be a UUID, got %s", idempotencyId)
				}
				w.WriteHeader(http.StatusOK)
				json.NewEncoder(w).Encode(map[string]interface{}{})
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up test server if handler is provided
			if tt.serverHandler != nil {
				server := httptest.NewServer(tt.serverHandler)
				defer server.Close()
				tt.action.Member = server.URL
			}

			payload := map[string]any{"idempotencyId": uuid.New().String(), "epoch": tt.epoch}
			err := tt.action.Execute(context.Background(), payload)

			if tt.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("expected error containing %q, got %q", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestActionK8sWaitDeploymentRollout_Execute_InvalidEndpoint(t *testing.T) {
	action := &ActionK8sWaitDeploymentRollout{
		ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
			Member:     "http://invalid-endpoint-that-does-not-exist-12345.local",
			Deployment: "my-deployment",
		},
	}

	payload := map[string]any{"idempotencyId": uuid.New().String(), "epoch": 1}
	err := action.Execute(context.Background(), payload)

	if err == nil {
		t.Errorf("expected error for invalid endpoint, got none")
	}
}

func TestActionK8sUpdateConfigMap_GetType(t *testing.T) {
	action := &ActionK8sUpdateConfigMap{}
	if action.GetType() != task.ActionTypeK8sUpdateConfigMap {
		t.Errorf("expected GetType() to return ActionTypeK8sUpdateConfigMap, got %v", action.GetType())
	}
}

func TestActionK8sUpdateConfigMap_Execute(t *testing.T) {
	tests := []struct {
		name          string
		action        *ActionK8sUpdateConfigMap
		epoch         int64
		serverHandler http.HandlerFunc
		expectedError bool
		errorContains string
	}{
		{
			name: "successful update with default namespace",
			action: &ActionK8sUpdateConfigMap{
				ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
					ConfigMap: "my-configmap",
					Key:       "my-key",
					Value:     "my-value",
				},
			},
			epoch: 123,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "PATCH" {
					t.Errorf("expected PATCH method, got %s", r.Method)
				}
				expectedPath := "/api/v1/configmaps/default/my-configmap"
				if r.URL.Path != expectedPath {
					t.Errorf("expected path %s, got %s", expectedPath, r.URL.Path)
				}
				if r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("expected Content-Type application/json, got %s", r.Header.Get("Content-Type"))
				}
				if r.Header.Get("X-Idempotency-Id") == "" {
					t.Errorf("expected X-Idempotency-Id header to be set")
				}
				var body map[string]interface{}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("failed to decode request body: %v", err)
				}
				if epoch, ok := body["epoch"].(float64); !ok || epoch != 123 {
					t.Errorf("expected epoch 123, got %v", body["epoch"])
				}
				patch, ok := body["patch"].(map[string]interface{})
				if !ok {
					t.Errorf("expected patch in body")
				}
				data, ok := patch["data"].(map[string]interface{})
				if !ok {
					t.Errorf("expected data in patch")
				}
				if value, ok := data["my-key"].(string); !ok || value != "my-value" {
					t.Errorf("expected data.my-key to be 'my-value', got %v", data["my-key"])
				}
				w.WriteHeader(http.StatusOK)
			},
			expectedError: false,
		},
		{
			name: "successful update with custom namespace",
			action: &ActionK8sUpdateConfigMap{
				ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
					ConfigMap: "my-configmap",
					Namespace: "custom-ns",
					Key:       "config-key",
					Value:     "config-value",
				},
			},
			epoch: 456,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				expectedPath := "/api/v1/configmaps/custom-ns/my-configmap"
				if r.URL.Path != expectedPath {
					t.Errorf("expected path %s, got %s", expectedPath, r.URL.Path)
				}
				w.WriteHeader(http.StatusOK)
			},
			expectedError: false,
		},
		{
			name: "successful update with multiline value",
			action: &ActionK8sUpdateConfigMap{
				ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
					ConfigMap: "my-configmap",
					Key:       "yaml-config",
					Value:     "- item1\n- item2\n- item3",
				},
			},
			epoch: 789,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				var body map[string]interface{}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("failed to decode request body: %v", err)
				}
				patch := body["patch"].(map[string]interface{})
				data := patch["data"].(map[string]interface{})
				expectedValue := "- item1\n- item2\n- item3"
				if value, ok := data["yaml-config"].(string); !ok || value != expectedValue {
					t.Errorf("expected multiline value, got %v", data["yaml-config"])
				}
				w.WriteHeader(http.StatusOK)
			},
			expectedError: false,
		},
		{
			name: "successful update with empty value",
			action: &ActionK8sUpdateConfigMap{
				ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
					ConfigMap: "my-configmap",
					Key:       "empty-key",
					Value:     "",
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				var body map[string]interface{}
				json.NewDecoder(r.Body).Decode(&body)
				patch := body["patch"].(map[string]interface{})
				data := patch["data"].(map[string]interface{})
				if value, ok := data["empty-key"].(string); !ok || value != "" {
					t.Errorf("expected empty value, got %v", data["empty-key"])
				}
				w.WriteHeader(http.StatusOK)
			},
			expectedError: false,
		},
		{
			name: "missing config_map name returns error",
			action: &ActionK8sUpdateConfigMap{
				ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
					ConfigMap: "",
					Key:       "my-key",
					Value:     "my-value",
				},
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "config_map name is required",
		},
		{
			name: "missing member returns error",
			action: &ActionK8sUpdateConfigMap{
				ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
					ConfigMap: "my-configmap",
					Key:       "my-key",
					Value:     "my-value",
				},
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "member is required",
		},
		{
			name: "missing key returns error",
			action: &ActionK8sUpdateConfigMap{
				ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
					Member:    "http://localhost:8080",
					ConfigMap: "my-configmap",
					Key:       "",
					Value:     "my-value",
				},
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "key is required",
		},
		{
			name: "HTTP 400 Bad Request returns error",
			action: &ActionK8sUpdateConfigMap{
				ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
					ConfigMap: "my-configmap",
					Key:       "my-key",
					Value:     "my-value",
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error": "invalid request",
					"code":  400,
				})
			},
			expectedError: true,
			errorContains: "failed to update ConfigMap",
		},
		{
			name: "HTTP 404 Not Found returns error",
			action: &ActionK8sUpdateConfigMap{
				ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
					ConfigMap: "nonexistent",
					Key:       "my-key",
					Value:     "my-value",
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("configmap not found"))
			},
			expectedError: true,
			errorContains: "failed to update ConfigMap",
		},
		{
			name: "HTTP 409 Conflict (stale epoch) returns error",
			action: &ActionK8sUpdateConfigMap{
				ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
					ConfigMap: "my-configmap",
					Key:       "my-key",
					Value:     "my-value",
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusConflict)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error": "stale epoch",
					"code":  409,
				})
			},
			expectedError: true,
			errorContains: "failed to update ConfigMap",
		},
		{
			name: "HTTP 500 Internal Server Error returns error",
			action: &ActionK8sUpdateConfigMap{
				ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
					ConfigMap: "my-configmap",
					Key:       "my-key",
					Value:     "my-value",
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("internal error"))
			},
			expectedError: true,
			errorContains: "failed to update ConfigMap",
		},
		{
			name: "verifies idempotency header is sent",
			action: &ActionK8sUpdateConfigMap{
				ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
					ConfigMap: "my-configmap",
					Key:       "my-key",
					Value:     "my-value",
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				idempotencyId := r.Header.Get("X-Idempotency-Id")
				if idempotencyId == "" {
					t.Errorf("expected X-Idempotency-Id header to be set")
				}
				// UUID format validation (basic check)
				if len(idempotencyId) < 32 {
					t.Errorf("expected X-Idempotency-Id to be a UUID, got %s", idempotencyId)
				}
				w.WriteHeader(http.StatusOK)
			},
			expectedError: false,
		},
		{
			name: "special characters in key and value",
			action: &ActionK8sUpdateConfigMap{
				ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
					ConfigMap: "my-configmap",
					Key:       "config.yaml",
					Value:     `{"key": "value", "nested": {"a": 1}}`,
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				var body map[string]interface{}
				json.NewDecoder(r.Body).Decode(&body)
				patch := body["patch"].(map[string]interface{})
				data := patch["data"].(map[string]interface{})
				expectedValue := `{"key": "value", "nested": {"a": 1}}`
				if value, ok := data["config.yaml"].(string); !ok || value != expectedValue {
					t.Errorf("expected JSON value, got %v", data["config.yaml"])
				}
				w.WriteHeader(http.StatusOK)
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up test server if handler is provided
			if tt.serverHandler != nil {
				server := httptest.NewServer(tt.serverHandler)
				defer server.Close()
				tt.action.Member = server.URL
			}

			payload := map[string]any{"idempotencyId": uuid.New().String(), "epoch": tt.epoch}
			err := tt.action.Execute(context.Background(), payload)

			if tt.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("expected error containing %q, got %q", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestActionK8sUpdateConfigMap_Execute_InvalidEndpoint(t *testing.T) {
	action := &ActionK8sUpdateConfigMap{
		ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
			Member:    "http://invalid-endpoint-that-does-not-exist-12345.local",
			ConfigMap: "my-configmap",
			Key:       "my-key",
			Value:     "my-value",
		},
	}

	payload := map[string]any{"idempotencyId": uuid.New().String(), "epoch": 1}
	err := action.Execute(context.Background(), payload)

	if err == nil {
		t.Errorf("expected error for invalid endpoint, got none")
	}
}

func TestActionK8sScaleDeployment_GetType(t *testing.T) {
	action := &ActionK8sScaleDeployment{}
	if action.GetType() != task.ActionTypeK8sScaleDeployment {
		t.Errorf("expected GetType() to return ActionTypeK8sScaleDeployment, got %v", action.GetType())
	}
}

func TestActionK8sScaleDeployment_Execute(t *testing.T) {
	tests := []struct {
		name          string
		action        *ActionK8sScaleDeployment
		epoch         int64
		serverHandler http.HandlerFunc
		expectedError bool
		errorContains string
	}{
		{
			name: "successful scaling with default namespace",
			action: &ActionK8sScaleDeployment{
				ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
					Deployment: "my-deployment",
					Replicas:   3,
				},
			},
			epoch: 123,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "PATCH" {
					t.Errorf("expected PATCH method, got %s", r.Method)
				}
				expectedPath := "/api/v1/deployments/default/my-deployment"
				if r.URL.Path != expectedPath {
					t.Errorf("expected path %s, got %s", expectedPath, r.URL.Path)
				}
				if r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("expected Content-Type application/json, got %s", r.Header.Get("Content-Type"))
				}
				if r.Header.Get("X-Idempotency-Id") == "" {
					t.Errorf("expected X-Idempotency-Id header to be set")
				}
				var body map[string]interface{}
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("failed to decode request body: %v", err)
				}
				if epoch, ok := body["epoch"].(float64); !ok || epoch != 123 {
					t.Errorf("expected epoch 123, got %v", body["epoch"])
				}
				patch, ok := body["patch"].(map[string]interface{})
				if !ok {
					t.Errorf("expected patch in body")
				}
				spec, ok := patch["spec"].(map[string]interface{})
				if !ok {
					t.Errorf("expected spec in patch")
				}
				if replicas, ok := spec["replicas"].(float64); !ok || replicas != 3 {
					t.Errorf("expected replicas to be 3, got %v", spec["replicas"])
				}
				w.WriteHeader(http.StatusOK)
			},
			expectedError: false,
		},
		{
			name: "successful scaling with custom namespace",
			action: &ActionK8sScaleDeployment{
				ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
					Deployment: "my-deployment",
					Namespace:  "custom-ns",
					Replicas:   5,
				},
			},
			epoch: 456,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				expectedPath := "/api/v1/deployments/custom-ns/my-deployment"
				if r.URL.Path != expectedPath {
					t.Errorf("expected path %s, got %s", expectedPath, r.URL.Path)
				}
				var body map[string]interface{}
				json.NewDecoder(r.Body).Decode(&body)
				patch := body["patch"].(map[string]interface{})
				spec := patch["spec"].(map[string]interface{})
				if replicas, ok := spec["replicas"].(float64); !ok || replicas != 5 {
					t.Errorf("expected replicas to be 5, got %v", spec["replicas"])
				}
				w.WriteHeader(http.StatusOK)
			},
			expectedError: false,
		},
		{
			name: "successful scaling to zero replicas",
			action: &ActionK8sScaleDeployment{
				ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
					Deployment: "my-deployment",
					Replicas:   0,
				},
			},
			epoch: 789,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				var body map[string]interface{}
				json.NewDecoder(r.Body).Decode(&body)
				patch := body["patch"].(map[string]interface{})
				spec := patch["spec"].(map[string]interface{})
				if replicas, ok := spec["replicas"].(float64); !ok || replicas != 0 {
					t.Errorf("expected replicas to be 0, got %v", spec["replicas"])
				}
				w.WriteHeader(http.StatusOK)
			},
			expectedError: false,
		},
		{
			name: "successful scaling to one replica",
			action: &ActionK8sScaleDeployment{
				ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
					Deployment: "my-deployment",
					Replicas:   1,
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				var body map[string]interface{}
				json.NewDecoder(r.Body).Decode(&body)
				patch := body["patch"].(map[string]interface{})
				spec := patch["spec"].(map[string]interface{})
				if replicas, ok := spec["replicas"].(float64); !ok || replicas != 1 {
					t.Errorf("expected replicas to be 1, got %v", spec["replicas"])
				}
				w.WriteHeader(http.StatusOK)
			},
			expectedError: false,
		},
		{
			name: "successful scaling to many replicas",
			action: &ActionK8sScaleDeployment{
				ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
					Deployment: "my-deployment",
					Replicas:   100,
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				var body map[string]interface{}
				json.NewDecoder(r.Body).Decode(&body)
				patch := body["patch"].(map[string]interface{})
				spec := patch["spec"].(map[string]interface{})
				if replicas, ok := spec["replicas"].(float64); !ok || replicas != 100 {
					t.Errorf("expected replicas to be 100, got %v", spec["replicas"])
				}
				w.WriteHeader(http.StatusOK)
			},
			expectedError: false,
		},
		{
			name: "missing deployment name returns error",
			action: &ActionK8sScaleDeployment{
				ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
					Deployment: "",
					Replicas:   3,
				},
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "deployment name is required",
		},
		{
			name: "missing member returns error",
			action: &ActionK8sScaleDeployment{
				ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
					Deployment: "my-deployment",
					Replicas:   3,
				},
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "member is required",
		},
		{
			name: "HTTP 400 Bad Request returns error",
			action: &ActionK8sScaleDeployment{
				ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
					Deployment: "my-deployment",
					Replicas:   3,
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error": "invalid request",
					"code":  400,
				})
			},
			expectedError: true,
			errorContains: "failed to scale deployment",
		},
		{
			name: "HTTP 404 Not Found returns error",
			action: &ActionK8sScaleDeployment{
				ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
					Deployment: "nonexistent",
					Replicas:   3,
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("deployment not found"))
			},
			expectedError: true,
			errorContains: "failed to scale deployment",
		},
		{
			name: "HTTP 409 Conflict (stale epoch) returns error",
			action: &ActionK8sScaleDeployment{
				ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
					Deployment: "my-deployment",
					Replicas:   3,
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusConflict)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error": "stale epoch",
					"code":  409,
				})
			},
			expectedError: true,
			errorContains: "failed to scale deployment",
		},
		{
			name: "HTTP 500 Internal Server Error returns error",
			action: &ActionK8sScaleDeployment{
				ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
					Deployment: "my-deployment",
					Replicas:   3,
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("internal error"))
			},
			expectedError: true,
			errorContains: "failed to scale deployment",
		},
		{
			name: "verifies idempotency header is sent",
			action: &ActionK8sScaleDeployment{
				ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
					Deployment: "my-deployment",
					Replicas:   3,
				},
			},
			epoch: 1,
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				idempotencyId := r.Header.Get("X-Idempotency-Id")
				if idempotencyId == "" {
					t.Errorf("expected X-Idempotency-Id header to be set")
				}
				// UUID format validation (basic check)
				if len(idempotencyId) < 32 {
					t.Errorf("expected X-Idempotency-Id to be a UUID, got %s", idempotencyId)
				}
				w.WriteHeader(http.StatusOK)
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up test server if handler is provided
			if tt.serverHandler != nil {
				server := httptest.NewServer(tt.serverHandler)
				defer server.Close()
				tt.action.Member = server.URL
			}

			payload := map[string]any{"idempotencyId": uuid.New().String(), "epoch": tt.epoch}
			err := tt.action.Execute(context.Background(), payload)

			if tt.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				} else if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("expected error containing %q, got %q", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestActionK8sScaleDeployment_Execute_InvalidEndpoint(t *testing.T) {
	action := &ActionK8sScaleDeployment{
		ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
			Member:     "http://invalid-endpoint-that-does-not-exist-12345.local",
			Deployment: "my-deployment",
			Replicas:   3,
		},
	}

	payload := map[string]any{"idempotencyId": uuid.New().String(), "epoch": 1}
	err := action.Execute(context.Background(), payload)

	if err == nil {
		t.Errorf("expected error for invalid endpoint, got none")
	}
}
