package task

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
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

			err := tt.action.Execute(context.Background(), 0)

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

	err := action.Execute(context.Background(), 0)

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

			err := action.Execute(context.Background(), 0)

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

// mockOnChange is a test implementation of OnChange interface
type mockOnChange struct {
	mu           sync.Mutex
	executed     bool
	executedFor  []string
	executeError error
	executeCalls int
	namespace    string
	epoch        int64
}

func (m *mockOnChange) GetType() OnChangeType {
	return OnChangeTypeDeploymentRestart
}

func (m *mockOnChange) Execute(ctx context.Context, member string, namespace string, epoch int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executed = true
	m.executedFor = append(m.executedFor, member)
	m.executeCalls++
	m.namespace = namespace
	m.epoch = epoch
	return m.executeError
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
			name: "onChange is executed when values change",
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
			validateFunc: func(t *testing.T, action *ActionConfigValueSum, servers map[string]*httptest.Server) {
				mockOnChange, ok := action.OnChange.(*mockOnChange)
				if !ok {
					t.Fatalf("expected mockOnChange, got %T", action.OnChange)
				}
				if !mockOnChange.executed {
					t.Error("expected onChange to be executed")
				}
				if mockOnChange.executeCalls != 2 {
					t.Errorf("expected onChange to be called 2 times, got %d", mockOnChange.executeCalls)
				}
				if len(mockOnChange.executedFor) != 2 {
					t.Errorf("expected onChange to be executed for 2 members, got %d", len(mockOnChange.executedFor))
				}
			},
		},
		{
			name: "onChange execution failure returns error",
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
				// Member1 returns 1 (will be decremented to 0, not negative)
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
						if value != "0" {
							t.Errorf("expected value to be 0 (not negative), got %s", value)
						}
						w.WriteHeader(http.StatusOK)
					}
				}))
				// Member2 returns 10
				servers["member2"] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "GET" && strings.Contains(r.URL.Path, "/api/v1/configmaps/default/test-config") {
						response := map[string]interface{}{
							"data": map[string]string{
								"replicas": "10",
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
					}
				}))
				cleanup := func() {
					for _, s := range servers {
						s.Close()
					}
				}
				return servers, cleanup
			},
			expectedError: false, // Error is logged but doesn't stop execution
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

			// Set up mock onChange if needed
			if strings.Contains(tt.name, "onChange") {
				mock := &mockOnChange{}
				if strings.Contains(tt.name, "failure") {
					mock.executeError = fmt.Errorf("onChange execution failed")
				}
				tt.action.OnChange = mock
			}

			err := tt.action.Execute(context.Background(), 1)

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

func TestOnChangeDeploymentRestart_GetType(t *testing.T) {
	onChange := &OnChangeDeploymentRestart{}
	if onChange.GetType() != OnChangeTypeDeploymentRestart {
		t.Errorf("expected GetType() to return OnChangeTypeDeploymentRestart, got %v", onChange.GetType())
	}
}

func TestOnChangeDeploymentRestart_Execute(t *testing.T) {
	tests := []struct {
		name          string
		onChange      *OnChangeDeploymentRestart
		setupServer   func() (*httptest.Server, func())
		expectedError bool
		validateReq   func(*testing.T, *http.Request)
	}{
		{
			name: "successful deployment restart",
			onChange: &OnChangeDeploymentRestart{
				Deployment: "test-deployment",
			},
			setupServer: func() (*httptest.Server, func()) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
				}))
				return server, server.Close
			},
			expectedError: false,
		},
		{
			name: "missing deployment name returns error",
			onChange: &OnChangeDeploymentRestart{
				Deployment: "",
			},
			setupServer: func() (*httptest.Server, func()) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusOK)
				}))
				return server, server.Close
			},
			expectedError: true,
		},
		{
			name: "patch failure returns error",
			onChange: &OnChangeDeploymentRestart{
				Deployment: "test-deployment",
			},
			setupServer: func() (*httptest.Server, func()) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte("Internal Server Error"))
				}))
				return server, server.Close
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, cleanup := tt.setupServer()
			defer cleanup()

			// Create a context with member URL
			ctx := context.Background()
			member := server.URL

			err := tt.onChange.Execute(ctx, member, "default", 1)

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
