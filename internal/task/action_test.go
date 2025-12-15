package task

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

	"github.com/google/uuid"
)

func TestActionEndpoint_Execute(t *testing.T) {
	tests := []struct {
		name          string
		action        *ActionEndpoint
		serverHandler http.HandlerFunc
		expectedError bool
		validateReq   func(*testing.T, *http.Request) // Optional function to validate request
	}{
		{
			name: "successful GET request",
			action: &ActionEndpoint{
				Endpoint: "",
				Method:   "GET",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					t.Errorf("expected GET method, got %s", r.Method)
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("OK"))
			},
			expectedError: false,
		},
		{
			name: "successful POST request with body",
			action: &ActionEndpoint{
				Endpoint: "",
				Method:   "POST",
				Body:     `{"key": "value"}`,
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "POST" {
					t.Errorf("expected POST method, got %s", r.Method)
				}
				bodyBytes, _ := io.ReadAll(r.Body)
				if string(bodyBytes) != `{"key": "value"}` {
					t.Errorf("expected body %q, got %q", `{"key": "value"}`, string(bodyBytes))
				}
				if r.Header.Get("Content-Type") != "application/json" {
					t.Errorf("expected Content-Type application/json, got %s", r.Header.Get("Content-Type"))
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("Created"))
			},
			expectedError: false,
		},
		{
			name: "successful PUT request with body",
			action: &ActionEndpoint{
				Endpoint: "",
				Method:   "PUT",
				Body:     `{"id": 1, "name": "test"}`,
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "PUT" {
					t.Errorf("expected PUT method, got %s", r.Method)
				}
				bodyBytes, _ := io.ReadAll(r.Body)
				if string(bodyBytes) != `{"id": 1, "name": "test"}` {
					t.Errorf("expected body %q, got %q", `{"id": 1, "name": "test"}`, string(bodyBytes))
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("Updated"))
			},
			expectedError: false,
		},
		{
			name: "successful DELETE request",
			action: &ActionEndpoint{
				Endpoint: "",
				Method:   "DELETE",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "DELETE" {
					t.Errorf("expected DELETE method, got %s", r.Method)
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("Deleted"))
			},
			expectedError: false,
		},
		{
			name: "successful request with custom headers",
			action: &ActionEndpoint{
				Endpoint: "",
				Method:   "GET",
				Headers: map[string]string{
					"Authorization":   "Bearer token123",
					"X-Custom-Header": "custom-value",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer token123" {
					t.Errorf("expected Authorization header 'Bearer token123', got %s", r.Header.Get("Authorization"))
				}
				if r.Header.Get("X-Custom-Header") != "custom-value" {
					t.Errorf("expected X-Custom-Header 'custom-value', got %s", r.Header.Get("X-Custom-Header"))
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("OK"))
			},
			expectedError: false,
		},
		{
			name: "successful POST request with custom Content-Type header",
			action: &ActionEndpoint{
				Endpoint: "",
				Method:   "POST",
				Body:     `{"data": "test"}`,
				Headers: map[string]string{
					"Content-Type": "application/xml",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Content-Type") != "application/xml" {
					t.Errorf("expected Content-Type application/xml, got %s", r.Header.Get("Content-Type"))
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("OK"))
			},
			expectedError: false,
		},
		{
			name: "default method is GET when not specified",
			action: &ActionEndpoint{
				Endpoint: "",
				Method:   "",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "GET" {
					t.Errorf("expected GET method (default), got %s", r.Method)
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("OK"))
			},
			expectedError: false,
		},
		{
			name: "empty endpoint returns error",
			action: &ActionEndpoint{
				Endpoint: "",
				Method:   "GET",
			},
			serverHandler: nil,
			expectedError: true,
		},
		{
			name: "HTTP 4xx status code returns error",
			action: &ActionEndpoint{
				Endpoint: "",
				Method:   "GET",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("Not Found"))
			},
			expectedError: true,
		},
		{
			name: "HTTP 5xx status code returns error",
			action: &ActionEndpoint{
				Endpoint: "",
				Method:   "GET",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("Internal Server Error"))
			},
			expectedError: true,
		},
		{
			name: "HTTP 400 Bad Request returns error",
			action: &ActionEndpoint{
				Endpoint: "",
				Method:   "POST",
				Body:     `{"invalid": "data"}`,
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("Bad Request"))
			},
			expectedError: true,
		},
		{
			name: "successful request with empty body",
			action: &ActionEndpoint{
				Endpoint: "",
				Method:   "POST",
				Body:     "",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				bodyBytes, _ := io.ReadAll(r.Body)
				if len(bodyBytes) != 0 {
					t.Errorf("expected empty body, got %q", string(bodyBytes))
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("OK"))
			},
			expectedError: false,
		},
		{
			name: "successful request with multiple headers",
			action: &ActionEndpoint{
				Endpoint: "",
				Method:   "GET",
				Headers: map[string]string{
					"Header1": "Value1",
					"Header2": "Value2",
					"Header3": "Value3",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Header1") != "Value1" {
					t.Errorf("expected Header1 'Value1', got %s", r.Header.Get("Header1"))
				}
				if r.Header.Get("Header2") != "Value2" {
					t.Errorf("expected Header2 'Value2', got %s", r.Header.Get("Header2"))
				}
				if r.Header.Get("Header3") != "Value3" {
					t.Errorf("expected Header3 'Value3', got %s", r.Header.Get("Header3"))
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("OK"))
			},
			expectedError: false,
		},
		{
			name: "successful POST with large body",
			action: &ActionEndpoint{
				Endpoint: "",
				Method:   "POST",
				Body:     strings.Repeat("a", 10000),
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				bodyBytes, _ := io.ReadAll(r.Body)
				if len(bodyBytes) != 10000 {
					t.Errorf("expected body length 10000, got %d", len(bodyBytes))
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("OK"))
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
				tt.action.Endpoint = server.URL
			}

			err := tt.action.Execute(context.Background(), 0, uuid.New().String())

			if tt.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestActionEndpoint_Execute_InvalidEndpoint(t *testing.T) {
	action := &ActionEndpoint{
		Endpoint: "http://invalid-endpoint-that-does-not-exist-12345.local",
		Method:   "GET",
	}

	err := action.Execute(context.Background(), 0, uuid.New().String())

	if err == nil {
		t.Errorf("expected error for invalid endpoint, got none")
	}
}

func TestActionEndpoint_Execute_HTTPStatusCodes(t *testing.T) {
	statusCodes := []struct {
		code          int
		expectedError bool
		name          string
	}{
		{200, false, "OK"},
		{201, false, "Created"},
		{204, false, "No Content"},
		{301, false, "Moved Permanently"},
		{302, false, "Found"},
		{400, true, "Bad Request"},
		{401, true, "Unauthorized"},
		{403, true, "Forbidden"},
		{404, true, "Not Found"},
		{500, true, "Internal Server Error"},
		{502, true, "Bad Gateway"},
		{503, true, "Service Unavailable"},
	}

	for _, tc := range statusCodes {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.code)
				w.Write([]byte(tc.name))
			}))
			defer server.Close()

			action := &ActionEndpoint{
				Endpoint: server.URL,
				Method:   "GET",
			}

			err := action.Execute(context.Background(), 0, uuid.New().String())

			if tc.expectedError {
				if err == nil {
					t.Errorf("expected error for status code %d, got none", tc.code)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for status code %d: %v", tc.code, err)
				}
			}
		})
	}
}

func TestActionConfigValueSum_Execute(t *testing.T) {
	tests := []struct {
		name          string
		action        *ActionConfigValueSum
		setupServers  func() (map[string]*httptest.Server, func())
		expectedError bool
		validateFunc  func(*testing.T, *ActionConfigValueSum, map[string]*httptest.Server)
	}{
		{
			name: "sum already matches target - no action needed",
			action: &ActionConfigValueSum{
				ConfigMapName: "test-config",
				Key:           "replicas",
				Sum:           10,
				Members:       []string{"member1", "member2"},
			},
			setupServers: func() (map[string]*httptest.Server, func()) {
				servers := make(map[string]*httptest.Server)
				// Member1 returns 5
				servers["member1"] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "GET" && strings.Contains(r.URL.Path, "/api/v1/configmaps/default/test-config") {
						response := map[string]interface{}{
							"data": map[string]string{
								"replicas": "5",
							},
						}
						json.NewEncoder(w).Encode(response)
					}
				}))
				// Member2 returns 5
				servers["member2"] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "GET" && strings.Contains(r.URL.Path, "/api/v1/configmaps/default/test-config") {
						response := map[string]interface{}{
							"data": map[string]string{
								"replicas": "5",
							},
						}
						json.NewEncoder(w).Encode(response)
					}
				}))
				cleanup := func() {
					for _, s := range servers {
						s.Close()
					}
				}
				return servers, cleanup
			},
			expectedError: false,
		},
		{
			name: "sum is less than target - increment values",
			action: &ActionConfigValueSum{
				ConfigMapName: "test-config",
				Key:           "replicas",
				Sum:           10,
				Members:       []string{"member1", "member2"},
			},
			setupServers: func() (map[string]*httptest.Server, func()) {
				servers := make(map[string]*httptest.Server)
				patchRequests := make(map[string][]map[string]interface{})
				var mu sync.Mutex

				// Member1 returns 3
				servers["member1"] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "GET" && strings.Contains(r.URL.Path, "/api/v1/configmaps/default/test-config") {
						response := map[string]interface{}{
							"data": map[string]string{
								"replicas": "3",
							},
						}
						json.NewEncoder(w).Encode(response)
					} else if r.Method == "PATCH" {
						var body map[string]interface{}
						json.NewDecoder(r.Body).Decode(&body)
						mu.Lock()
						patchRequests["member1"] = append(patchRequests["member1"], body)
						mu.Unlock()
						w.WriteHeader(http.StatusOK)
					}
				}))
				// Member2 returns 3
				servers["member2"] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "GET" && strings.Contains(r.URL.Path, "/api/v1/configmaps/default/test-config") {
						response := map[string]interface{}{
							"data": map[string]string{
								"replicas": "3",
							},
						}
						json.NewEncoder(w).Encode(response)
					} else if r.Method == "PATCH" {
						var body map[string]interface{}
						json.NewDecoder(r.Body).Decode(&body)
						mu.Lock()
						patchRequests["member2"] = append(patchRequests["member2"], body)
						mu.Unlock()
						w.WriteHeader(http.StatusOK)
					}
				}))
				cleanup := func() {
					for _, s := range servers {
						s.Close()
					}
				}
				return servers, cleanup
			},
			expectedError: false,
		},
		{
			name: "sum is greater than target - decrement values",
			action: &ActionConfigValueSum{
				ConfigMapName: "test-config",
				Key:           "replicas",
				Sum:           10,
				Members:       []string{"member1", "member2"},
			},
			setupServers: func() (map[string]*httptest.Server, func()) {
				servers := make(map[string]*httptest.Server)
				// Member1 returns 7
				servers["member1"] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "GET" && strings.Contains(r.URL.Path, "/api/v1/configmaps/default/test-config") {
						response := map[string]interface{}{
							"data": map[string]string{
								"replicas": "7",
							},
						}
						json.NewEncoder(w).Encode(response)
					} else if r.Method == "PATCH" {
						w.WriteHeader(http.StatusOK)
					}
				}))
				// Member2 returns 7
				servers["member2"] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "GET" && strings.Contains(r.URL.Path, "/api/v1/configmaps/default/test-config") {
						response := map[string]interface{}{
							"data": map[string]string{
								"replicas": "7",
							},
						}
						json.NewEncoder(w).Encode(response)
					} else if r.Method == "PATCH" {
						w.WriteHeader(http.StatusOK)
					}
				}))
				cleanup := func() {
					for _, s := range servers {
						s.Close()
					}
				}
				return servers, cleanup
			},
			expectedError: false,
		},
		{
			name: "fetch config value failure",
			action: &ActionConfigValueSum{
				ConfigMapName: "test-config",
				Key:           "replicas",
				Sum:           10,
				Members:       []string{"member1", "member2"},
			},
			setupServers: func() (map[string]*httptest.Server, func()) {
				servers := make(map[string]*httptest.Server)
				// Member1 returns error
				servers["member1"] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "GET" {
						w.WriteHeader(http.StatusInternalServerError)
					}
				}))
				// Member2 returns 5
				servers["member2"] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "GET" && strings.Contains(r.URL.Path, "/api/v1/configmaps/default/test-config") {
						response := map[string]interface{}{
							"data": map[string]string{
								"replicas": "5",
							},
						}
						json.NewEncoder(w).Encode(response)
					}
				}))
				cleanup := func() {
					for _, s := range servers {
						s.Close()
					}
				}
				return servers, cleanup
			},
			expectedError: false, // Errors are logged but don't stop execution
		},
		{
			name: "patch config value failure",
			action: &ActionConfigValueSum{
				ConfigMapName: "test-config",
				Key:           "replicas",
				Sum:           10,
				Members:       []string{"member1", "member2"},
			},
			setupServers: func() (map[string]*httptest.Server, func()) {
				servers := make(map[string]*httptest.Server)
				// Member1 returns 3
				servers["member1"] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "GET" && strings.Contains(r.URL.Path, "/api/v1/configmaps/default/test-config") {
						response := map[string]interface{}{
							"data": map[string]string{
								"replicas": "3",
							},
						}
						json.NewEncoder(w).Encode(response)
					} else if r.Method == "PATCH" {
						w.WriteHeader(http.StatusInternalServerError)
					}
				}))
				// Member2 returns 3
				servers["member2"] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "GET" && strings.Contains(r.URL.Path, "/api/v1/configmaps/default/test-config") {
						response := map[string]interface{}{
							"data": map[string]string{
								"replicas": "3",
							},
						}
						json.NewEncoder(w).Encode(response)
					} else if r.Method == "PATCH" {
						w.WriteHeader(http.StatusOK)
					}
				}))
				cleanup := func() {
					for _, s := range servers {
						s.Close()
					}
				}
				return servers, cleanup
			},
			expectedError: true,
		},
		{
			name: "decrement prevents negative values",
			action: &ActionConfigValueSum{
				ConfigMapName: "test-config",
				Key:           "replicas",
				Sum:           5,
				Members:       []string{"member1", "member2"},
			},
			setupServers: func() (map[string]*httptest.Server, func()) {
				servers := make(map[string]*httptest.Server)
				// Member1 returns 1 (target sum 5 / 2 members = 2, remainder 1)
				// Map iteration order is non-deterministic, so remainder goes to whichever member is processed first
				servers["member1"] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "GET" && strings.Contains(r.URL.Path, "/api/v1/configmaps/default/test-config") {
						response := map[string]interface{}{
							"data": map[string]string{
								"replicas": "1",
							},
						}
						json.NewEncoder(w).Encode(response)
					} else if r.Method == "PATCH" {
						var body map[string]interface{}
						json.NewDecoder(r.Body).Decode(&body)
						patch := body["patch"].(map[string]interface{})
						data := patch["data"].(map[string]interface{})
						value := data["replicas"].(string)
						// Target sum 5 / 2 = 2, remainder 1
						// One member gets 3 (2+1), the other gets 2
						// Map iteration order determines which gets the remainder
						if value != "2" && value != "3" {
							t.Errorf("expected value to be 2 or 3 (target sum 5 distributed evenly: 5/2=2 remainder 1), got %s", value)
						}
						w.WriteHeader(http.StatusOK)
					}
				}))
				// Member2 returns 10 (target sum 5 / 2 members = 2, remainder 1)
				servers["member2"] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "GET" && strings.Contains(r.URL.Path, "/api/v1/configmaps/default/test-config") {
						response := map[string]interface{}{
							"data": map[string]string{
								"replicas": "10",
							},
						}
						json.NewEncoder(w).Encode(response)
					} else if r.Method == "PATCH" {
						var body map[string]interface{}
						json.NewDecoder(r.Body).Decode(&body)
						patch := body["patch"].(map[string]interface{})
						data := patch["data"].(map[string]interface{})
						value := data["replicas"].(string)
						// Target sum 5 / 2 = 2, remainder 1
						// One member gets 3 (2+1), the other gets 2
						// Map iteration order determines which gets the remainder
						if value != "2" && value != "3" {
							t.Errorf("expected value to be 2 or 3 (target sum 5 distributed evenly: 5/2=2 remainder 1), got %s", value)
						}
						w.WriteHeader(http.StatusOK)
					}
				}))
				cleanup := func() {
					for _, s := range servers {
						s.Close()
					}
				}
				return servers, cleanup
			},
			expectedError: false,
		},
		{
			name: "three members with remainder distribution",
			action: &ActionConfigValueSum{
				ConfigMapName: "test-config",
				Key:           "replicas",
				Sum:           10,
				Members:       []string{"member1", "member2", "member3"},
			},
			setupServers: func() (map[string]*httptest.Server, func()) {
				servers := make(map[string]*httptest.Server)
				// All members return 2 (sum=6, need to add 4, remainder=1)
				for _, member := range []string{"member1", "member2", "member3"} {
					servers[member] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.Method == "GET" && strings.Contains(r.URL.Path, "/api/v1/configmaps/default/test-config") {
							response := map[string]interface{}{
								"data": map[string]string{
									"replicas": "2",
								},
							}
							json.NewEncoder(w).Encode(response)
						} else if r.Method == "PATCH" {
							w.WriteHeader(http.StatusOK)
						}
					}))
				}
				cleanup := func() {
					for _, s := range servers {
						s.Close()
					}
				}
				return servers, cleanup
			},
			expectedError: false,
		},
		{
			name: "key not found in configmap",
			action: &ActionConfigValueSum{
				ConfigMapName: "test-config",
				Key:           "nonexistent",
				Sum:           10,
				Members:       []string{"member1"},
			},
			setupServers: func() (map[string]*httptest.Server, func()) {
				servers := make(map[string]*httptest.Server)
				servers["member1"] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "GET" && strings.Contains(r.URL.Path, "/api/v1/configmaps/default/test-config") {
						response := map[string]interface{}{
							"data": map[string]string{
								"otherkey": "5",
							},
						}
						json.NewEncoder(w).Encode(response)
					} else if r.Method == "PATCH" {
						// Handle PATCH request for creating/updating the nonexistent key
						w.WriteHeader(http.StatusOK)
					}
				}))
				cleanup := func() {
					for _, s := range servers {
						s.Close()
					}
				}
				return servers, cleanup
			},
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			servers, cleanup := tt.setupServers()
			defer cleanup()

			// Update action members with server URLs
			for i, member := range tt.action.Members {
				if server, ok := servers[member]; ok {
					tt.action.Members[i] = server.URL
				}
			}

			err := tt.action.Execute(context.Background(), 1, uuid.New().String())

			if tt.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}

			if tt.validateFunc != nil {
				tt.validateFunc(t, tt.action, servers)
			}
		})
	}
}

func TestActionConfigValueSum_GetType(t *testing.T) {
	action := &ActionConfigValueSum{}
	if action.GetType() != ActionTypeConfigValueSum {
		t.Errorf("expected GetType() to return ActionTypeConfigValueSum, got %v", action.GetType())
	}
}

func TestActionK8sRestartDeployment_GetType(t *testing.T) {
	action := &ActionK8sRestartDeployment{}
	if action.GetType() != ActionTypeK8sRestartDeployment {
		t.Errorf("expected GetType() to return ActionTypeK8sRestartDeployment, got %v", action.GetType())
	}
}

func TestActionK8sWaitDeploymentRollout_GetType(t *testing.T) {
	action := &ActionK8sWaitDeploymentRollout{}
	if action.GetType() != ActionTypeK8sWaitDeploymentRollout {
		t.Errorf("expected GetType() to return ActionTypeK8sWaitDeploymentRollout, got %v", action.GetType())
	}
}

func TestActionK8sExecDeployment_GetType(t *testing.T) {
	action := &ActionK8sExecDeployment{}
	if action.GetType() != ActionTypeK8sExecDeployment {
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
				Deployment: "my-deployment",
				Command:    []string{"echo", "hello"},
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
				Deployment: "my-deployment",
				Namespace:  "custom-ns",
				Command:    []string{"ls", "-la"},
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
				Deployment: "my-deployment",
				Container:  "sidecar",
				Command:    []string{"cat", "/etc/config"},
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
				Deployment: "",
				Command:    []string{"echo"},
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "deployment name is required",
		},
		{
			name: "missing member returns error",
			action: &ActionK8sExecDeployment{
				Deployment: "my-deployment",
				Command:    []string{"echo"},
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "member is required",
		},
		{
			name: "missing command returns error",
			action: &ActionK8sExecDeployment{
				Member:     "http://localhost:8080",
				Deployment: "my-deployment",
				Command:    []string{},
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "command is required",
		},
		{
			name: "nil command returns error",
			action: &ActionK8sExecDeployment{
				Member:     "http://localhost:8080",
				Deployment: "my-deployment",
				Command:    nil,
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "command is required",
		},
		{
			name: "HTTP 400 Bad Request returns error",
			action: &ActionK8sExecDeployment{
				Deployment: "my-deployment",
				Command:    []string{"echo"},
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
				Deployment: "nonexistent",
				Command:    []string{"echo"},
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
				Deployment: "my-deployment",
				Command:    []string{"echo"},
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
				Deployment: "my-deployment",
				Command:    []string{"echo"},
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
				Deployment: "my-deployment",
				Command:    []string{"sh", "-c", "echo $HOME && ls -la /tmp"},
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

			err := tt.action.Execute(context.Background(), tt.epoch, uuid.New().String())

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
		Member:     "http://invalid-endpoint-that-does-not-exist-12345.local",
		Deployment: "my-deployment",
		Command:    []string{"echo"},
	}

	err := action.Execute(context.Background(), 1, uuid.New().String())

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
				Deployment: "test-deployment",
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
				Deployment: "test-deployment",
				Namespace:  "custom-ns",
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
				Deployment: "",
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "deployment name is required",
		},
		{
			name: "missing member returns error",
			action: &ActionK8sRestartDeployment{
				Deployment: "test-deployment",
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "member is required",
		},
		{
			name: "patch failure returns error",
			action: &ActionK8sRestartDeployment{
				Deployment: "test-deployment",
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
				Deployment: "test-deployment",
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

			err := tt.action.Execute(context.Background(), tt.epoch, uuid.New().String())

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
				Deployment: "my-deployment",
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
				Deployment: "my-deployment",
				Namespace:  "custom-ns",
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
				Deployment: "my-deployment",
				Timeout:    10 * time.Minute,
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
				Deployment: "my-deployment",
				Timeout:    30 * time.Second,
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
				Deployment: "",
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "deployment name is required",
		},
		{
			name: "missing member returns error",
			action: &ActionK8sWaitDeploymentRollout{
				Deployment: "my-deployment",
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "member is required",
		},
		{
			name: "HTTP 400 Bad Request returns error",
			action: &ActionK8sWaitDeploymentRollout{
				Deployment: "my-deployment",
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
				Deployment: "nonexistent",
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
				Deployment: "my-deployment",
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
				Deployment: "my-deployment",
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
				Deployment: "my-deployment",
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
				Deployment: "my-deployment",
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

			err := tt.action.Execute(context.Background(), tt.epoch, uuid.New().String())

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
		Member:     "http://invalid-endpoint-that-does-not-exist-12345.local",
		Deployment: "my-deployment",
	}

	err := action.Execute(context.Background(), 1, uuid.New().String())

	if err == nil {
		t.Errorf("expected error for invalid endpoint, got none")
	}
}

func TestActionK8sUpdateConfigMap_GetType(t *testing.T) {
	action := &ActionK8sUpdateConfigMap{}
	if action.GetType() != ActionTypeK8sUpdateConfigMap {
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
				ConfigMap: "my-configmap",
				Key:       "my-key",
				Value:     "my-value",
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
				ConfigMap: "my-configmap",
				Namespace: "custom-ns",
				Key:       "config-key",
				Value:     "config-value",
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
				ConfigMap: "my-configmap",
				Key:       "yaml-config",
				Value:     "- item1\n- item2\n- item3",
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
				ConfigMap: "my-configmap",
				Key:       "empty-key",
				Value:     "",
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
				ConfigMap: "",
				Key:       "my-key",
				Value:     "my-value",
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "config_map name is required",
		},
		{
			name: "missing member returns error",
			action: &ActionK8sUpdateConfigMap{
				ConfigMap: "my-configmap",
				Key:       "my-key",
				Value:     "my-value",
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "member is required",
		},
		{
			name: "missing key returns error",
			action: &ActionK8sUpdateConfigMap{
				Member:    "http://localhost:8080",
				ConfigMap: "my-configmap",
				Key:       "",
				Value:     "my-value",
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "key is required",
		},
		{
			name: "HTTP 400 Bad Request returns error",
			action: &ActionK8sUpdateConfigMap{
				ConfigMap: "my-configmap",
				Key:       "my-key",
				Value:     "my-value",
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
				ConfigMap: "nonexistent",
				Key:       "my-key",
				Value:     "my-value",
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
				ConfigMap: "my-configmap",
				Key:       "my-key",
				Value:     "my-value",
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
				ConfigMap: "my-configmap",
				Key:       "my-key",
				Value:     "my-value",
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
				ConfigMap: "my-configmap",
				Key:       "my-key",
				Value:     "my-value",
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
				ConfigMap: "my-configmap",
				Key:       "config.yaml",
				Value:     `{"key": "value", "nested": {"a": 1}}`,
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

			err := tt.action.Execute(context.Background(), tt.epoch, uuid.New().String())

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
		Member:    "http://invalid-endpoint-that-does-not-exist-12345.local",
		ConfigMap: "my-configmap",
		Key:       "my-key",
		Value:     "my-value",
	}

	err := action.Execute(context.Background(), 1, uuid.New().String())

	if err == nil {
		t.Errorf("expected error for invalid endpoint, got none")
	}
}

func TestActionK8sScaleDeployment_GetType(t *testing.T) {
	action := &ActionK8sScaleDeployment{}
	if action.GetType() != ActionTypeK8sScaleDeployment {
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
				Deployment: "my-deployment",
				Replicas:   3,
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
				Deployment: "my-deployment",
				Namespace:  "custom-ns",
				Replicas:   5,
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
				Deployment: "my-deployment",
				Replicas:   0,
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
				Deployment: "my-deployment",
				Replicas:   1,
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
				Deployment: "my-deployment",
				Replicas:   100,
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
				Deployment: "",
				Replicas:   3,
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "deployment name is required",
		},
		{
			name: "missing member returns error",
			action: &ActionK8sScaleDeployment{
				Deployment: "my-deployment",
				Replicas:   3,
			},
			epoch:         1,
			serverHandler: nil,
			expectedError: true,
			errorContains: "member is required",
		},
		{
			name: "HTTP 400 Bad Request returns error",
			action: &ActionK8sScaleDeployment{
				Deployment: "my-deployment",
				Replicas:   3,
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
				Deployment: "nonexistent",
				Replicas:   3,
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
				Deployment: "my-deployment",
				Replicas:   3,
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
				Deployment: "my-deployment",
				Replicas:   3,
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
				Deployment: "my-deployment",
				Replicas:   3,
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

			err := tt.action.Execute(context.Background(), tt.epoch, uuid.New().String())

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
		Member:     "http://invalid-endpoint-that-does-not-exist-12345.local",
		Deployment: "my-deployment",
		Replicas:   3,
	}

	err := action.Execute(context.Background(), 1, uuid.New().String())

	if err == nil {
		t.Errorf("expected error for invalid endpoint, got none")
	}
}
