package centralized

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/esadakcam/conductor/internal/k8s"
	"github.com/esadakcam/conductor/internal/task"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
				ConditionEndpointSuccessData: task.ConditionEndpointSuccessData{
					Endpoint: "",
					Status:   200,
				},
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
				ConditionEndpointSuccessData: task.ConditionEndpointSuccessData{
					Endpoint: "",
					Status:   200,
				},
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
				ConditionEndpointSuccessData: task.ConditionEndpointSuccessData{
					Endpoint:     "",
					Status:       200,
					ResponseBody: "Hello World",
				},
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
				ConditionEndpointSuccessData: task.ConditionEndpointSuccessData{
					Endpoint:     "",
					Status:       200,
					ResponseBody: "Hello World",
				},
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
				ConditionEndpointSuccessData: task.ConditionEndpointSuccessData{
					Endpoint:     "",
					Status:       200,
					ResponseBody: "Expected Body",
				},
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
				ConditionEndpointSuccessData: task.ConditionEndpointSuccessData{
					Endpoint: "",
					Status:   201,
				},
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
				ConditionEndpointSuccessData: task.ConditionEndpointSuccessData{
					Endpoint: "",
					Status:   200,
				},
			},
			serverHandler:  nil,
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "successful evaluation with empty response body check",
			condition: &ConditionEndpointSuccess{
				ConditionEndpointSuccessData: task.ConditionEndpointSuccessData{
					Endpoint:     "",
					Status:       204,
					ResponseBody: "",
				},
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
			if tt.serverHandler != nil {
				server := httptest.NewServer(tt.serverHandler)
				defer server.Close()
				tt.condition.Endpoint = server.URL
			}

			result, err := tt.condition.Evaluate(context.Background(), nil)

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
		ConditionEndpointSuccessData: task.ConditionEndpointSuccessData{
			Endpoint: "http://invalid-endpoint-that-does-not-exist-12345.local",
			Status:   200,
		},
	}

	result, err := condition.Evaluate(context.Background(), nil)

	if err == nil {
		t.Errorf("expected error for invalid endpoint, got none")
	}
	if result {
		t.Errorf("expected false result for invalid endpoint, got true")
	}
}

func TestConditionEndpointSuccess_GetType(t *testing.T) {
	condition := &ConditionEndpointSuccess{}
	if condition.GetType() != task.ConditionTypeEndpointSuccess {
		t.Errorf("expected type %v, got %v", task.ConditionTypeEndpointSuccess, condition.GetType())
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    42,
					Operator: "eq",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    42,
					Operator: "eq",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    42,
					Operator: "ne",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    42,
					Operator: "ne",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    50,
					Operator: "lt",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    50,
					Operator: "lt",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    50,
					Operator: "gt",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    50,
					Operator: "gt",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    50,
					Operator: "le",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    50,
					Operator: "le",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    50,
					Operator: "le",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    50,
					Operator: "ge",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    50,
					Operator: "ge",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    50,
					Operator: "ge",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    42,
					Operator: "eq",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    42,
					Operator: "eq",
				},
			},
			serverHandler:  nil,
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "invalid response body (non-numeric) returns error",
			condition: &ConditionEndpointValue{
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    42,
					Operator: "eq",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    42,
					Operator: "invalid",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    -10,
					Operator: "lt",
				},
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
				ConditionEndpointValueData: task.ConditionEndpointValueData{
					Endpoint: "",
					Value:    0,
					Operator: "eq",
				},
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
			if tt.serverHandler != nil {
				server := httptest.NewServer(tt.serverHandler)
				defer server.Close()
				tt.condition.Endpoint = server.URL
			}

			result, err := tt.condition.Evaluate(context.Background(), nil)

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
		ConditionEndpointValueData: task.ConditionEndpointValueData{
			Endpoint: "http://invalid-endpoint-that-does-not-exist-12345.local",
			Value:    42,
			Operator: "eq",
		},
	}

	result, err := condition.Evaluate(context.Background(), nil)

	if err == nil {
		t.Errorf("expected error for invalid endpoint, got none")
	}
	if result {
		t.Errorf("expected false result for invalid endpoint, got true")
	}
}

func TestConditionEndpointValue_GetType(t *testing.T) {
	condition := &ConditionEndpointValue{}
	if condition.GetType() != task.ConditionTypeEndpointValue {
		t.Errorf("expected type %v, got %v", task.ConditionTypeEndpointValue, condition.GetType())
	}
}

func TestConditionPrometheusMetric_Evaluate(t *testing.T) {
	// Sample Prometheus metrics responses
	gaugeMetrics := `# HELP ues_active Number of active UEs
# TYPE ues_active gauge
ues_active 42
`
	counterMetrics := `# HELP http_requests_total Total number of HTTP requests
# TYPE http_requests_total counter
http_requests_total 100
`
	untypedMetrics := `# HELP some_metric Some untyped metric
# TYPE some_metric untyped
some_metric 55.5
`
	histogramMetrics := `# HELP request_duration_seconds Request duration histogram
# TYPE request_duration_seconds histogram
request_duration_seconds_bucket{le="0.1"} 10
request_duration_seconds_bucket{le="0.5"} 50
request_duration_seconds_bucket{le="+Inf"} 100
request_duration_seconds_sum 25.5
request_duration_seconds_count 100
`
	multipleMetrics := `# HELP metric_a First metric
# TYPE metric_a gauge
metric_a 10
# HELP metric_b Second metric
# TYPE metric_b gauge
metric_b 20
`
	floatGaugeMetrics := `# HELP cpu_usage CPU usage percentage
# TYPE cpu_usage gauge
cpu_usage 75.5
`

	tests := []struct {
		name           string
		condition      *ConditionPrometheusMetric
		serverHandler  http.HandlerFunc
		expectedResult bool
		expectedError  bool
	}{
		// Gauge metric tests
		{
			name: "successful evaluation with eq operator on gauge metric",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      42,
					Operator:   "eq",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "failed evaluation with eq operator on gauge metric (not equal)",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      100,
					Operator:   "eq",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "successful evaluation with ne operator",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      100,
					Operator:   "ne",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "failed evaluation with ne operator (equal)",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      42,
					Operator:   "ne",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "successful evaluation with lt operator",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      50,
					Operator:   "lt",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "failed evaluation with lt operator (not less than)",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      30,
					Operator:   "lt",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "successful evaluation with gt operator",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      30,
					Operator:   "gt",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "failed evaluation with gt operator (not greater than)",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      50,
					Operator:   "gt",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "successful evaluation with le operator (less)",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      50,
					Operator:   "le",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "successful evaluation with le operator (equal)",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      42,
					Operator:   "le",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "failed evaluation with le operator (greater)",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      30,
					Operator:   "le",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "successful evaluation with ge operator (greater)",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      30,
					Operator:   "ge",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "successful evaluation with ge operator (equal)",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      42,
					Operator:   "ge",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "failed evaluation with ge operator (less)",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      50,
					Operator:   "ge",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: false,
			expectedError:  false,
		},
		// Counter metric tests
		{
			name: "successful evaluation on counter metric",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "http_requests_total",
					Value:      100,
					Operator:   "eq",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(counterMetrics))
			},
			expectedResult: true,
			expectedError:  false,
		},
		// Untyped metric tests
		{
			name: "successful evaluation on untyped metric",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "some_metric",
					Value:      55.5,
					Operator:   "eq",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(untypedMetrics))
			},
			expectedResult: true,
			expectedError:  false,
		},
		// Float value tests
		{
			name: "successful evaluation with float value",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "cpu_usage",
					Value:      75.5,
					Operator:   "eq",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(floatGaugeMetrics))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "successful evaluation with float comparison (gt)",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "cpu_usage",
					Value:      70.0,
					Operator:   "gt",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(floatGaugeMetrics))
			},
			expectedResult: true,
			expectedError:  false,
		},
		// Multiple metrics - selecting specific one
		{
			name: "successful evaluation selecting specific metric from multiple",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "metric_b",
					Value:      20,
					Operator:   "eq",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(multipleMetrics))
			},
			expectedResult: true,
			expectedError:  false,
		},
		// Error cases
		{
			name: "empty endpoint returns error",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      42,
					Operator:   "eq",
				},
			},
			serverHandler:  nil,
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "empty metric name returns error",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "will-be-replaced",
					MetricName: "",
					Value:      42,
					Operator:   "eq",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "metric not found returns error",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "nonexistent_metric",
					Value:      42,
					Operator:   "eq",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "unsupported operator returns error",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      42,
					Operator:   "invalid",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(gaugeMetrics))
			},
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "unsupported metric type (histogram) returns error",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "request_duration_seconds",
					Value:      100,
					Operator:   "eq",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(histogramMetrics))
			},
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "invalid prometheus format returns error",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      42,
					Operator:   "eq",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("this is not prometheus format {{{"))
			},
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "non-200 status code returns error",
			condition: &ConditionPrometheusMetric{
				ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
					Endpoint:   "",
					MetricName: "ues_active",
					Value:      42,
					Operator:   "eq",
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte("Service Unavailable"))
			},
			expectedResult: false,
			expectedError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.serverHandler != nil {
				server := httptest.NewServer(tt.serverHandler)
				defer server.Close()
				if tt.condition.Endpoint == "" || tt.condition.Endpoint == "will-be-replaced" {
					tt.condition.Endpoint = server.URL
				}
			}

			result, err := tt.condition.Evaluate(context.Background(), nil)

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

func TestConditionPrometheusMetric_Evaluate_InvalidEndpoint(t *testing.T) {
	condition := &ConditionPrometheusMetric{
		ConditionPrometheusMetricData: task.ConditionPrometheusMetricData{
			Endpoint:   "http://invalid-endpoint-that-does-not-exist-12345.local",
			MetricName: "ues_active",
			Value:      42,
			Operator:   "eq",
		},
	}

	result, err := condition.Evaluate(context.Background(), nil)

	if err == nil {
		t.Errorf("expected error for invalid endpoint, got none")
	}
	if result {
		t.Errorf("expected false result for invalid endpoint, got true")
	}
}

func TestConditionPrometheusMetric_GetType(t *testing.T) {
	condition := &ConditionPrometheusMetric{}
	if condition.GetType() != task.ConditionTypePrometheusMetric {
		t.Errorf("expected type %v, got %v", task.ConditionTypePrometheusMetric, condition.GetType())
	}
}

func TestParsePrometheusMetric(t *testing.T) {
	tests := []struct {
		name          string
		body          []byte
		metricName    string
		expectedValue float64
		expectedError bool
	}{
		{
			name: "parse gauge metric",
			body: []byte(`# HELP test_gauge A test gauge
# TYPE test_gauge gauge
test_gauge 123.45
`),
			metricName:    "test_gauge",
			expectedValue: 123.45,
			expectedError: false,
		},
		{
			name: "parse counter metric",
			body: []byte(`# HELP test_counter A test counter
# TYPE test_counter counter
test_counter 999
`),
			metricName:    "test_counter",
			expectedValue: 999,
			expectedError: false,
		},
		{
			name: "parse untyped metric",
			body: []byte(`# HELP test_untyped A test untyped metric
# TYPE test_untyped untyped
test_untyped 42.5
`),
			metricName:    "test_untyped",
			expectedValue: 42.5,
			expectedError: false,
		},
		{
			name: "metric not found",
			body: []byte(`# HELP existing_metric An existing metric
# TYPE existing_metric gauge
existing_metric 10
`),
			metricName:    "missing_metric",
			expectedValue: 0,
			expectedError: true,
		},
		{
			name:          "empty body",
			body:          []byte(""),
			metricName:    "any_metric",
			expectedValue: 0,
			expectedError: true,
		},
		{
			name: "metric with labels - takes first sample",
			body: []byte(`# HELP labeled_metric A metric with labels
# TYPE labeled_metric gauge
labeled_metric{env="prod"} 100
labeled_metric{env="dev"} 50
`),
			metricName:    "labeled_metric",
			expectedValue: 100,
			expectedError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := parsePrometheusMetric(tt.body, tt.metricName)

			if tt.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if value != tt.expectedValue {
					t.Errorf("expected value %v, got %v", tt.expectedValue, value)
				}
			}
		})
	}
}

func TestConditionAlwaysTrue_Evaluate(t *testing.T) {
	condition := &ConditionAlwaysTrue{}

	result, err := condition.Evaluate(context.Background(), nil)

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !result {
		t.Errorf("expected true result, got false")
	}
}

func TestConditionAlwaysTrue_Evaluate_WithPayload(t *testing.T) {
	condition := &ConditionAlwaysTrue{}

	// Should work with any payload
	result, err := condition.Evaluate(context.Background(), map[string]any{"key": "value"})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !result {
		t.Errorf("expected true result, got false")
	}
}

func TestConditionAlwaysTrue_GetType(t *testing.T) {
	condition := &ConditionAlwaysTrue{}
	if condition.GetType() != task.ConditionTypeAlwaysTrue {
		t.Errorf("expected type %v, got %v", task.ConditionTypeAlwaysTrue, condition.GetType())
	}
}

// K8s deployment ready condition tests using Kind cluster
func TestConditionK8sDeploymentReady_GetType(t *testing.T) {
	condition := &ConditionK8sDeploymentReady{}
	if condition.GetType() != task.ConditionTypeK8sDeploymentReady {
		t.Errorf("expected type %v, got %v", task.ConditionTypeK8sDeploymentReady, condition.GetType())
	}
}

func TestConditionK8sDeploymentReady_Evaluate(t *testing.T) {
	client := setupTestCluster(t)
	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	t.Run("deployment is ready with expected replicas", func(t *testing.T) {
		deployName := fmt.Sprintf("test-ready-deploy-%d", time.Now().UnixNano())
		deployObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      deployName,
					"namespace": ns,
				},
				"spec": map[string]interface{}{
					"replicas": int64(2),
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "ready-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "ready-test",
							},
						},
						"spec": map[string]interface{}{
							"containers": []map[string]interface{}{
								{
									"name":    "busybox",
									"image":   "busybox:latest",
									"command": []interface{}{"sleep", "3600"},
								},
							},
						},
					},
				},
			},
		}

		_, err := client.Create(ctx, "deployments", ns, deployObj)
		if err != nil {
			t.Fatalf("Failed to create deployment: %v", err)
		}
		defer client.Delete(ctx, "deployments", ns, deployName)

		// Wait for deployment to be ready
		err = waitForDeploymentReady(t, client, ns, deployName, 3*time.Minute)
		if err != nil {
			t.Fatalf("Deployment did not become ready: %v", err)
		}

		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: deployName,
				Namespace:  ns,
				Replicas:   2,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		result, err := condition.Evaluate(ctx, payload)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !result {
			t.Errorf("expected deployment to be ready, got false")
		}
	})

	t.Run("deployment is ready but with different replica count", func(t *testing.T) {
		deployName := fmt.Sprintf("test-diff-replicas-%d", time.Now().UnixNano())
		deployObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      deployName,
					"namespace": ns,
				},
				"spec": map[string]interface{}{
					"replicas": int64(1),
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "diff-replicas-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "diff-replicas-test",
							},
						},
						"spec": map[string]interface{}{
							"containers": []map[string]interface{}{
								{
									"name":    "busybox",
									"image":   "busybox:latest",
									"command": []interface{}{"sleep", "3600"},
								},
							},
						},
					},
				},
			},
		}

		_, err := client.Create(ctx, "deployments", ns, deployObj)
		if err != nil {
			t.Fatalf("Failed to create deployment: %v", err)
		}
		defer client.Delete(ctx, "deployments", ns, deployName)

		// Wait for deployment to be ready
		err = waitForDeploymentReady(t, client, ns, deployName, 3*time.Minute)
		if err != nil {
			t.Fatalf("Deployment did not become ready: %v", err)
		}

		// Expect 3 replicas but deployment has 1
		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: deployName,
				Namespace:  ns,
				Replicas:   3,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		result, err := condition.Evaluate(ctx, payload)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result {
			t.Errorf("expected deployment to NOT be ready (wrong replica count), got true")
		}
	})

	t.Run("deployment is not ready (pending pods)", func(t *testing.T) {
		deployName := fmt.Sprintf("test-pending-deploy-%d", time.Now().UnixNano())
		deployObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      deployName,
					"namespace": ns,
				},
				"spec": map[string]interface{}{
					"replicas": int64(1),
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "pending-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "pending-test",
							},
						},
						"spec": map[string]interface{}{
							"containers": []map[string]interface{}{
								{
									"name":    "busybox",
									"image":   "invalid-image-that-does-not-exist:v999",
									"command": []interface{}{"sleep", "3600"},
								},
							},
						},
					},
				},
			},
		}

		_, err := client.Create(ctx, "deployments", ns, deployObj)
		if err != nil {
			t.Fatalf("Failed to create deployment: %v", err)
		}
		defer client.Delete(ctx, "deployments", ns, deployName)

		// Give Kubernetes a moment to start the deployment
		time.Sleep(2 * time.Second)

		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: deployName,
				Namespace:  ns,
				Replicas:   1,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		result, err := condition.Evaluate(ctx, payload)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result {
			t.Errorf("expected deployment to NOT be ready (pending pods), got true")
		}
	})

	t.Run("deployment with default namespace", func(t *testing.T) {
		deployName := fmt.Sprintf("test-default-ns-%d", time.Now().UnixNano())
		deployObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      deployName,
					"namespace": "default",
				},
				"spec": map[string]interface{}{
					"replicas": int64(1),
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "default-ns-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "default-ns-test",
							},
						},
						"spec": map[string]interface{}{
							"containers": []map[string]interface{}{
								{
									"name":    "busybox",
									"image":   "busybox:latest",
									"command": []interface{}{"sleep", "3600"},
								},
							},
						},
					},
				},
			},
		}

		_, err := client.Create(ctx, "deployments", "default", deployObj)
		if err != nil {
			t.Fatalf("Failed to create deployment: %v", err)
		}
		defer client.Delete(ctx, "deployments", "default", deployName)

		// Wait for deployment to be ready
		err = waitForDeploymentReady(t, client, "default", deployName, 3*time.Minute)
		if err != nil {
			t.Fatalf("Deployment did not become ready: %v", err)
		}

		// Test with empty namespace (should default to "default")
		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: deployName,
				Namespace:  "", // Should default to "default"
				Replicas:   1,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		result, err := condition.Evaluate(ctx, payload)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !result {
			t.Errorf("expected deployment to be ready, got false")
		}
	})

	t.Run("missing member returns error", func(t *testing.T) {
		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "",
				Deployment: "test-deployment",
				Namespace:  ns,
				Replicas:   1,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		_, err := condition.Evaluate(ctx, payload)
		if err == nil {
			t.Error("expected error for missing member")
		}
	})

	t.Run("missing deployment name returns error", func(t *testing.T) {
		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: "",
				Namespace:  ns,
				Replicas:   1,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		_, err := condition.Evaluate(ctx, payload)
		if err == nil {
			t.Error("expected error for missing deployment name")
		}
	})

	t.Run("missing k8s client for member returns error", func(t *testing.T) {
		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "nonexistent",
				Deployment: "test-deployment",
				Namespace:  ns,
				Replicas:   1,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		_, err := condition.Evaluate(ctx, payload)
		if err == nil {
			t.Error("expected error for missing k8s client")
		}
	})

	t.Run("non-existent deployment returns error", func(t *testing.T) {
		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: "non-existent-deployment",
				Namespace:  ns,
				Replicas:   1,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		_, err := condition.Evaluate(ctx, payload)
		if err == nil {
			t.Error("expected error for non-existent deployment")
		}
	})

	t.Run("invalid payload format returns error", func(t *testing.T) {
		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: "test-deployment",
				Namespace:  ns,
				Replicas:   1,
			},
		}

		_, err := condition.Evaluate(ctx, "invalid payload")
		if err == nil {
			t.Error("expected error for invalid payload format")
		}
	})

	t.Run("missing k8sClients in payload returns error", func(t *testing.T) {
		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: "test-deployment",
				Namespace:  ns,
				Replicas:   1,
			},
		}

		payload := map[string]any{
			"other": "data",
		}

		_, err := condition.Evaluate(ctx, payload)
		if err == nil {
			t.Error("expected error for missing k8sClients in payload")
		}
	})

	t.Run("deployment with zero replicas expected", func(t *testing.T) {
		deployName := fmt.Sprintf("test-zero-replicas-%d", time.Now().UnixNano())
		deployObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      deployName,
					"namespace": ns,
				},
				"spec": map[string]interface{}{
					"replicas": int64(0),
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "zero-replicas-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "zero-replicas-test",
							},
						},
						"spec": map[string]interface{}{
							"containers": []map[string]interface{}{
								{
									"name":    "busybox",
									"image":   "busybox:latest",
									"command": []interface{}{"sleep", "3600"},
								},
							},
						},
					},
				},
			},
		}

		_, err := client.Create(ctx, "deployments", ns, deployObj)
		if err != nil {
			t.Fatalf("Failed to create deployment: %v", err)
		}
		defer client.Delete(ctx, "deployments", ns, deployName)

		// Wait a moment for the deployment to be processed
		time.Sleep(2 * time.Second)

		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: deployName,
				Namespace:  ns,
				Replicas:   0,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		result, err := condition.Evaluate(ctx, payload)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !result {
			t.Errorf("expected deployment with 0 replicas to be ready, got false")
		}
	})
}
