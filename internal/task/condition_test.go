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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      42,
				Operator:   "eq",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      100,
				Operator:   "eq",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      100,
				Operator:   "ne",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      42,
				Operator:   "ne",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      50,
				Operator:   "lt",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      30,
				Operator:   "lt",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      30,
				Operator:   "gt",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      50,
				Operator:   "gt",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      50,
				Operator:   "le",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      42,
				Operator:   "le",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      30,
				Operator:   "le",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      30,
				Operator:   "ge",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      42,
				Operator:   "ge",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      50,
				Operator:   "ge",
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
				Endpoint:   "",
				MetricName: "http_requests_total",
				Value:      100,
				Operator:   "eq",
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
				Endpoint:   "",
				MetricName: "some_metric",
				Value:      55.5,
				Operator:   "eq",
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
				Endpoint:   "",
				MetricName: "cpu_usage",
				Value:      75.5,
				Operator:   "eq",
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
				Endpoint:   "",
				MetricName: "cpu_usage",
				Value:      70.0,
				Operator:   "gt",
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
				Endpoint:   "",
				MetricName: "metric_b",
				Value:      20,
				Operator:   "eq",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      42,
				Operator:   "eq",
			},
			serverHandler:  nil,
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "empty metric name returns error",
			condition: &ConditionPrometheusMetric{
				Endpoint:   "will-be-replaced",
				MetricName: "",
				Value:      42,
				Operator:   "eq",
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
				Endpoint:   "",
				MetricName: "nonexistent_metric",
				Value:      42,
				Operator:   "eq",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      42,
				Operator:   "invalid",
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
				Endpoint:   "",
				MetricName: "request_duration_seconds",
				Value:      100,
				Operator:   "eq",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      42,
				Operator:   "eq",
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
				Endpoint:   "",
				MetricName: "ues_active",
				Value:      42,
				Operator:   "eq",
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
			// Set up test server if handler is provided
			if tt.serverHandler != nil {
				server := httptest.NewServer(tt.serverHandler)
				defer server.Close()
				if tt.condition.Endpoint == "" || tt.condition.Endpoint == "will-be-replaced" {
					tt.condition.Endpoint = server.URL
				}
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

func TestConditionPrometheusMetric_Evaluate_InvalidEndpoint(t *testing.T) {
	condition := &ConditionPrometheusMetric{
		Endpoint:   "http://invalid-endpoint-that-does-not-exist-12345.local",
		MetricName: "ues_active",
		Value:      42,
		Operator:   "eq",
	}

	result, err := condition.Evaluate(context.Background())

	if err == nil {
		t.Errorf("expected error for invalid endpoint, got none")
	}
	if result {
		t.Errorf("expected false result for invalid endpoint, got true")
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

func TestConditionPrometheusMetric_GetType(t *testing.T) {
	condition := &ConditionPrometheusMetric{}
	if condition.GetType() != ConditionTypePrometheusMetric {
		t.Errorf("expected type %v, got %v", ConditionTypePrometheusMetric, condition.GetType())
	}
}

func TestConditionK8sDeploymentReady_Evaluate(t *testing.T) {
	// Sample deployment responses
	readyDeployment := `{
		"metadata": {
			"name": "test-deployment",
			"namespace": "default",
			"generation": 1
		},
		"spec": {
			"replicas": 3
		},
		"status": {
			"observedGeneration": 1,
			"replicas": 3,
			"updatedReplicas": 3,
			"readyReplicas": 3,
			"availableReplicas": 3
		}
	}`

	rollingDeployment := `{
		"metadata": {
			"name": "test-deployment",
			"namespace": "default",
			"generation": 2
		},
		"spec": {
			"replicas": 3
		},
		"status": {
			"observedGeneration": 2,
			"replicas": 4,
			"updatedReplicas": 2,
			"readyReplicas": 3,
			"availableReplicas": 3
		}
	}`

	pendingDeployment := `{
		"metadata": {
			"name": "test-deployment",
			"namespace": "default",
			"generation": 1
		},
		"spec": {
			"replicas": 3
		},
		"status": {
			"observedGeneration": 1,
			"replicas": 3,
			"updatedReplicas": 3,
			"readyReplicas": 2,
			"availableReplicas": 2
		}
	}`

	staleGeneration := `{
		"metadata": {
			"name": "test-deployment",
			"namespace": "default",
			"generation": 3
		},
		"spec": {
			"replicas": 3
		},
		"status": {
			"observedGeneration": 2,
			"replicas": 3,
			"updatedReplicas": 3,
			"readyReplicas": 3,
			"availableReplicas": 3
		}
	}`

	wrongReplicaCount := `{
		"metadata": {
			"name": "test-deployment",
			"namespace": "default",
			"generation": 1
		},
		"spec": {
			"replicas": 5
		},
		"status": {
			"observedGeneration": 1,
			"replicas": 5,
			"updatedReplicas": 5,
			"readyReplicas": 5,
			"availableReplicas": 5
		}
	}`

	noStatusDeployment := `{
		"metadata": {
			"name": "test-deployment",
			"namespace": "default",
			"generation": 1
		},
		"spec": {
			"replicas": 3
		}
	}`

	tests := []struct {
		name           string
		condition      *ConditionK8sDeploymentReady
		serverHandler  http.HandlerFunc
		expectedResult bool
		expectedError  bool
	}{
		{
			name: "deployment is ready with expected replicas",
			condition: &ConditionK8sDeploymentReady{
				Member:     "",
				Deployment: "test-deployment",
				Namespace:  "default",
				Replicas:   3,
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(readyDeployment))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "deployment is rolling out",
			condition: &ConditionK8sDeploymentReady{
				Member:     "",
				Deployment: "test-deployment",
				Namespace:  "default",
				Replicas:   3,
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(rollingDeployment))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "deployment has pending pods",
			condition: &ConditionK8sDeploymentReady{
				Member:     "",
				Deployment: "test-deployment",
				Namespace:  "default",
				Replicas:   3,
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(pendingDeployment))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "deployment has stale observed generation",
			condition: &ConditionK8sDeploymentReady{
				Member:     "",
				Deployment: "test-deployment",
				Namespace:  "default",
				Replicas:   3,
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(staleGeneration))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "deployment has wrong replica count",
			condition: &ConditionK8sDeploymentReady{
				Member:     "",
				Deployment: "test-deployment",
				Namespace:  "default",
				Replicas:   3,
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(wrongReplicaCount))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "deployment has no status yet",
			condition: &ConditionK8sDeploymentReady{
				Member:     "",
				Deployment: "test-deployment",
				Namespace:  "default",
				Replicas:   3,
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(noStatusDeployment))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "empty member returns error",
			condition: &ConditionK8sDeploymentReady{
				Member:     "",
				Deployment: "test-deployment",
				Namespace:  "default",
				Replicas:   3,
			},
			serverHandler:  nil,
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "empty deployment returns error",
			condition: &ConditionK8sDeploymentReady{
				Member:     "will-be-replaced",
				Deployment: "",
				Namespace:  "default",
				Replicas:   3,
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(readyDeployment))
			},
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "server returns error status",
			condition: &ConditionK8sDeploymentReady{
				Member:     "",
				Deployment: "test-deployment",
				Namespace:  "default",
				Replicas:   3,
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"error": "deployment not found"}`))
			},
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "server returns invalid JSON",
			condition: &ConditionK8sDeploymentReady{
				Member:     "",
				Deployment: "test-deployment",
				Namespace:  "default",
				Replicas:   3,
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("not valid json"))
			},
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "default namespace is used when not specified",
			condition: &ConditionK8sDeploymentReady{
				Member:     "",
				Deployment: "test-deployment",
				Namespace:  "",
				Replicas:   3,
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				// Verify the URL contains default namespace
				if r.URL.Path != "/api/v1/deployments/default/test-deployment" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(readyDeployment))
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
				if tt.condition.Member == "" || tt.condition.Member == "will-be-replaced" {
					tt.condition.Member = server.URL
				}
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

func TestConditionK8sDeploymentReady_Evaluate_InvalidEndpoint(t *testing.T) {
	condition := &ConditionK8sDeploymentReady{
		Member:     "http://invalid-endpoint-that-does-not-exist-12345.local",
		Deployment: "test-deployment",
		Namespace:  "default",
		Replicas:   3,
	}

	result, err := condition.Evaluate(context.Background())

	if err == nil {
		t.Errorf("expected error for invalid endpoint, got none")
	}
	if result {
		t.Errorf("expected false result for invalid endpoint, got true")
	}
}

func TestConditionK8sDeploymentReady_GetType(t *testing.T) {
	condition := &ConditionK8sDeploymentReady{}
	if condition.GetType() != ConditionTypeK8sDeploymentReady {
		t.Errorf("expected type %v, got %v", ConditionTypeK8sDeploymentReady, condition.GetType())
	}
}
