package task

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestConditionEndpointSuccess_Evaluate(t *testing.T) {
	tests := []struct {
		name           string
		condition      *ConditionEndpointSuccess
		serverHandler  http.HandlerFunc
		expectedResult bool
		expectedError  bool
	}{
		{
			name: "successful evaluation with matching status code",
			condition: &ConditionEndpointSuccess{
				Endpoint: "",
				Status:   200,
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("OK"))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "failed evaluation with non-matching status code",
			condition: &ConditionEndpointSuccess{
				Endpoint: "",
				Status:   200,
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte("Not Found"))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "successful evaluation with matching status and response body",
			condition: &ConditionEndpointSuccess{
				Endpoint:     "",
				Status:       200,
				ResponseBody: "Hello World",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("Hello World"))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "successful evaluation with matching status and response body (with whitespace)",
			condition: &ConditionEndpointSuccess{
				Endpoint:     "",
				Status:       200,
				ResponseBody: "Hello World",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("  Hello World  "))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "failed evaluation with non-matching response body",
			condition: &ConditionEndpointSuccess{
				Endpoint:     "",
				Status:       200,
				ResponseBody: "Expected Body",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("Different Body"))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "successful evaluation with custom status code",
			condition: &ConditionEndpointSuccess{
				Endpoint: "",
				Status:   201,
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusCreated)
				w.Write([]byte("Created"))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "empty endpoint returns error",
			condition: &ConditionEndpointSuccess{
				Endpoint: "", // Empty endpoint should trigger error
				Status:   200,
			},
			serverHandler:  nil, // Not used for this test
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "successful evaluation with empty response body",
			condition: &ConditionEndpointSuccess{
				Endpoint:     "",
				Status:       204,
				ResponseBody: "",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			},
			expectedResult: true,
			expectedError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up test server if handler is provided
			if tt.serverHandler != nil {
				server := httptest.NewServer(tt.serverHandler)
				defer server.Close()
				tt.condition.Endpoint = server.URL
			}

			result, err := tt.condition.Evaluate(context.Background())

			if tt.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if result != tt.expectedResult {
					t.Errorf("expected result %v, got %v", tt.expectedResult, result)
				}
			}
		})
	}
}

func TestConditionEndpointSuccess_Evaluate_InvalidEndpoint(t *testing.T) {
	condition := &ConditionEndpointSuccess{
		Endpoint: "http://invalid-endpoint-that-does-not-exist-12345.local",
		Status:   200,
	}

	result, err := condition.Evaluate(context.Background())

	if err == nil {
		t.Errorf("expected error for invalid endpoint, got none")
	}
	if result {
		t.Errorf("expected false result for invalid endpoint, got true")
	}
}

func TestConditionEndpointValue_Evaluate(t *testing.T) {
	tests := []struct {
		name           string
		condition      *ConditionEndpointValue
		serverHandler  http.HandlerFunc
		expectedResult bool
		expectedError  bool
	}{
		{
			name: "successful evaluation with eq operator (equal)",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    42,
				Operator: "eq",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("42"))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "failed evaluation with eq operator (not equal)",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    42,
				Operator: "eq",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("100"))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "successful evaluation with ne operator (not equal)",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    42,
				Operator: "ne",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("100"))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "failed evaluation with ne operator (equal)",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    42,
				Operator: "ne",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("42"))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "successful evaluation with lt operator (less than)",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    50,
				Operator: "lt",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("30"))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "failed evaluation with lt operator (not less than)",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    50,
				Operator: "lt",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("60"))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "successful evaluation with gt operator (greater than)",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    50,
				Operator: "gt",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("60"))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "failed evaluation with gt operator (not greater than)",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    50,
				Operator: "gt",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("30"))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "successful evaluation with le operator (less than or equal - less)",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    50,
				Operator: "le",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("30"))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "successful evaluation with le operator (less than or equal - equal)",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    50,
				Operator: "le",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("50"))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "failed evaluation with le operator (greater than)",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    50,
				Operator: "le",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("60"))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "successful evaluation with ge operator (greater than or equal - greater)",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    50,
				Operator: "ge",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("60"))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "successful evaluation with ge operator (greater than or equal - equal)",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    50,
				Operator: "ge",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("50"))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "failed evaluation with ge operator (less than)",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    50,
				Operator: "ge",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("30"))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "successful evaluation with whitespace in response",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    42,
				Operator: "eq",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("  42  "))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "empty endpoint returns error",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    42,
				Operator: "eq",
			},
			serverHandler:  nil,
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "invalid response body (non-numeric) returns error",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    42,
				Operator: "eq",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("not a number"))
			},
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "unsupported operator returns error",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    42,
				Operator: "invalid",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("42"))
			},
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "negative numbers work correctly",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    -10,
				Operator: "lt",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("-20"))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "zero value works correctly",
			condition: &ConditionEndpointValue{
				Endpoint: "",
				Value:    0,
				Operator: "eq",
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("0"))
			},
			expectedResult: true,
			expectedError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up test server if handler is provided
			if tt.serverHandler != nil {
				server := httptest.NewServer(tt.serverHandler)
				defer server.Close()
				tt.condition.Endpoint = server.URL
			}

			result, err := tt.condition.Evaluate(context.Background())

			if tt.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if result != tt.expectedResult {
					t.Errorf("expected result %v, got %v", tt.expectedResult, result)
				}
			}
		})
	}
}

func TestConditionEndpointValue_Evaluate_InvalidEndpoint(t *testing.T) {
	condition := &ConditionEndpointValue{
		Endpoint: "http://invalid-endpoint-that-does-not-exist-12345.local",
		Value:    42,
		Operator: "eq",
	}

	result, err := condition.Evaluate(context.Background())

	if err == nil {
		t.Errorf("expected error for invalid endpoint, got none")
	}
	if result {
		t.Errorf("expected false result for invalid endpoint, got true")
	}
}
