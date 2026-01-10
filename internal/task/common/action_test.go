package common

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/esadakcam/conductor/internal/task"
	"github.com/google/uuid"
)

func TestActionEndpoint_Execute(t *testing.T) {
	tests := []struct {
		name          string
		action        *ActionEndpoint
		serverHandler http.HandlerFunc
		expectedError bool
	}{
		{
			name: "successful GET request",
			action: &ActionEndpoint{
				ActionEndpointData: task.ActionEndpointData{
					Endpoint: "",
					Method:   "GET",
				},
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
				ActionEndpointData: task.ActionEndpointData{
					Endpoint: "",
					Method:   "POST",
					Body:     `{"key": "value"}`,
				},
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
				ActionEndpointData: task.ActionEndpointData{
					Endpoint: "",
					Method:   "PUT",
					Body:     `{"id": 1, "name": "test"}`,
				},
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
				ActionEndpointData: task.ActionEndpointData{
					Endpoint: "",
					Method:   "DELETE",
				},
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
				ActionEndpointData: task.ActionEndpointData{
					Endpoint: "",
					Method:   "GET",
					Headers: map[string]string{
						"Authorization":   "Bearer token123",
						"X-Custom-Header": "custom-value",
					},
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
				ActionEndpointData: task.ActionEndpointData{
					Endpoint: "",
					Method:   "POST",
					Body:     `{"data": "test"}`,
					Headers: map[string]string{
						"Content-Type": "application/xml",
					},
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
				ActionEndpointData: task.ActionEndpointData{
					Endpoint: "",
					Method:   "",
				},
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
				ActionEndpointData: task.ActionEndpointData{
					Endpoint: "",
					Method:   "GET",
				},
			},
			serverHandler: nil,
			expectedError: true,
		},
		{
			name: "HTTP 4xx status code returns error",
			action: &ActionEndpoint{
				ActionEndpointData: task.ActionEndpointData{
					Endpoint: "",
					Method:   "GET",
				},
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
				ActionEndpointData: task.ActionEndpointData{
					Endpoint: "",
					Method:   "GET",
				},
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
				ActionEndpointData: task.ActionEndpointData{
					Endpoint: "",
					Method:   "POST",
					Body:     `{"invalid": "data"}`,
				},
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
				ActionEndpointData: task.ActionEndpointData{
					Endpoint: "",
					Method:   "POST",
					Body:     "",
				},
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
				ActionEndpointData: task.ActionEndpointData{
					Endpoint: "",
					Method:   "GET",
					Headers: map[string]string{
						"Header1": "Value1",
						"Header2": "Value2",
						"Header3": "Value3",
					},
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
				ActionEndpointData: task.ActionEndpointData{
					Endpoint: "",
					Method:   "POST",
					Body:     strings.Repeat("a", 10000),
				},
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
			payload := map[string]any{"idempotencyId": uuid.New().String(), "epoch": 0}
			err := tt.action.Execute(context.Background(), payload)

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
		ActionEndpointData: task.ActionEndpointData{
			Endpoint: "http://invalid-endpoint-that-does-not-exist-12345.local",
			Method:   "GET",
		},
	}

	payload := map[string]any{"idempotencyId": uuid.New().String(), "epoch": 0}

	err := action.Execute(context.Background(), payload)

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
				ActionEndpointData: task.ActionEndpointData{
					Endpoint: server.URL,
					Method:   "GET",
				},
			}
			payload := map[string]any{"idempotencyId": uuid.New().String(), "epoch": 0}
			err := action.Execute(context.Background(), payload)

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

func TestActionEndpoint_GetType(t *testing.T) {
	action := &ActionEndpoint{}
	if action.GetType() != task.ActionTypeEndpoint {
		t.Errorf("expected GetType() to return ActionTypeEndpoint, got %v", action.GetType())
	}
}

func TestActionEcho_Execute(t *testing.T) {
	action := &ActionEcho{
		ActionEchoData: task.ActionEchoData{
			Message: "test message",
		},
	}

	err := action.Execute(context.Background(), nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestActionEcho_GetType(t *testing.T) {
	action := &ActionEcho{}
	if action.GetType() != task.ActionTypeEcho {
		t.Errorf("expected GetType() to return ActionTypeEcho, got %v", action.GetType())
	}
}

func TestActionDelay_Execute(t *testing.T) {
	t.Run("successful delay", func(t *testing.T) {
		action := &ActionDelay{
			ActionDelayData: task.ActionDelayData{
				Time: 100 * time.Millisecond,
			},
		}

		start := time.Now()
		err := action.Execute(context.Background(), nil)
		elapsed := time.Since(start)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		if elapsed < 100*time.Millisecond {
			t.Errorf("expected delay of at least 100ms, got %v", elapsed)
		}
	})

	t.Run("context cancellation", func(t *testing.T) {
		action := &ActionDelay{
			ActionDelayData: task.ActionDelayData{
				Time: 5 * time.Second,
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := action.Execute(ctx, nil)
		if err == nil {
			t.Errorf("expected error due to context cancellation, got none")
		}
	})
}

func TestActionDelay_GetType(t *testing.T) {
	action := &ActionDelay{}
	if action.GetType() != task.ActionTypeDelay {
		t.Errorf("expected GetType() to return ActionTypeDelay, got %v", action.GetType())
	}
}
