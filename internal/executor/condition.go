package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"github.com/esadakcam/conductor/internal/k8s"
	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/task"
	"github.com/esadakcam/conductor/internal/utils/httpclient"
)

// ConditionEndpointSuccess evaluates endpoint success condition
type ConditionEndpointSuccess struct {
	task.ConditionEndpointSuccess
}

func (c *ConditionEndpointSuccess) Evaluate(ctx context.Context, execCtx task.ExecutionContext) (bool, error) {
	if c.Endpoint == "" {
		return false, fmt.Errorf("endpoint is required")
	}

	client := httpclient.Get()
	req, err := http.NewRequestWithContext(ctx, "GET", c.Endpoint, nil)
	if err != nil {
		logger.Errorf("ConditionEndpointSuccess: failed to create request to %s: %v", c.Endpoint, err)
		return false, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		logger.Errorf("ConditionEndpointSuccess: failed to make request to %s: %v", c.Endpoint, err)
		return false, fmt.Errorf("failed to make request to %s: %w", c.Endpoint, err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != c.Status {
		return false, nil
	}

	// If ResponseBody is specified, check if it matches
	if c.ResponseBody != "" {
		bodyBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			logger.Errorf("ConditionEndpointSuccess: failed to read response body from %s: %v", c.Endpoint, err)
			return false, fmt.Errorf("failed to read response body: %w", err)
		}
		if strings.TrimSpace(string(bodyBytes)) != strings.TrimSpace(c.ResponseBody) {
			return false, nil
		}
	}

	return true, nil
}

func (c *ConditionEndpointSuccess) GetType() task.ConditionType {
	return task.ConditionTypeEndpointSuccess
}

// ConditionAlwaysTrue always evaluates to true
type ConditionAlwaysTrue struct {
	task.ConditionAlwaysTrue
}

func (c *ConditionAlwaysTrue) Evaluate(ctx context.Context, execCtx task.ExecutionContext) (bool, error) {
	return true, nil
}

func (c *ConditionAlwaysTrue) GetType() task.ConditionType {
	return task.ConditionTypeAlwaysTrue
}

// ConditionEndpointValue evaluates endpoint integer value condition
type ConditionEndpointValue struct {
	task.ConditionEndpointValue
}

func (c *ConditionEndpointValue) Evaluate(ctx context.Context, execCtx task.ExecutionContext) (bool, error) {
	if c.Endpoint == "" {
		return false, fmt.Errorf("endpoint is required")
	}

	client := httpclient.Get()
	req, err := http.NewRequestWithContext(ctx, "GET", c.Endpoint, nil)
	if err != nil {
		logger.Errorf("ConditionEndpointValue: failed to create request to %s: %v", c.Endpoint, err)
		return false, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		logger.Errorf("ConditionEndpointValue: failed to make request to %s: %v", c.Endpoint, err)
		return false, fmt.Errorf("failed to make request to %s: %w", c.Endpoint, err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Errorf("ConditionEndpointValue: failed to read response body from %s: %v", c.Endpoint, err)
		return false, fmt.Errorf("failed to read response body: %w", err)
	}

	responseValue, err := strconv.Atoi(strings.TrimSpace(string(bodyBytes)))
	if err != nil {
		logger.Errorf("ConditionEndpointValue: failed to parse response body as integer from %s: %v", c.Endpoint, err)
		return false, fmt.Errorf("failed to parse response body as integer: %w", err)
	}

	return compareInt(responseValue, c.Value, c.Operator)
}

func (c *ConditionEndpointValue) GetType() task.ConditionType {
	return task.ConditionTypeEndpointValue
}

// ConditionPrometheusMetric evaluates Prometheus metric condition
type ConditionPrometheusMetric struct {
	task.ConditionPrometheusMetric
}

func (c *ConditionPrometheusMetric) Evaluate(ctx context.Context, execCtx task.ExecutionContext) (bool, error) {
	if c.Endpoint == "" {
		return false, fmt.Errorf("endpoint is required")
	}
	if c.MetricName == "" {
		return false, fmt.Errorf("metric_name is required")
	}

	client := httpclient.Get()
	req, err := http.NewRequestWithContext(ctx, "GET", c.Endpoint, nil)
	if err != nil {
		logger.Errorf("ConditionPrometheusMetric: failed to create request to %s: %v", c.Endpoint, err)
		return false, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		logger.Errorf("ConditionPrometheusMetric: failed to make request to %s: %v", c.Endpoint, err)
		return false, fmt.Errorf("failed to make request to %s: %w", c.Endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Errorf("ConditionPrometheusMetric: endpoint %s returned non-success status code: %d", c.Endpoint, resp.StatusCode)
		return false, fmt.Errorf("endpoint returned non-success status code: %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Errorf("ConditionPrometheusMetric: failed to read response body from %s: %v", c.Endpoint, err)
		return false, fmt.Errorf("failed to read response body: %w", err)
	}

	metricValue, err := parsePrometheusMetric(bodyBytes, c.MetricName)
	if err != nil {
		logger.Errorf("ConditionPrometheusMetric: failed to parse metric %s from %s: %v", c.MetricName, c.Endpoint, err)
		return false, err
	}

	return compareFloat(metricValue, c.Value, c.Operator)
}

func (c *ConditionPrometheusMetric) GetType() task.ConditionType {
	return task.ConditionTypePrometheusMetric
}

// ConditionK8sDeploymentReady evaluates deployment readiness
type ConditionK8sDeploymentReady struct {
	task.ConditionK8sDeploymentReady
}

func (c *ConditionK8sDeploymentReady) Evaluate(ctx context.Context, execCtx task.ExecutionContext) (bool, error) {
	if c.Member == "" {
		return false, fmt.Errorf("member is required")
	}
	if c.Deployment == "" {
		return false, fmt.Errorf("deployment is required")
	}

	namespace := c.Namespace
	if namespace == "" {
		namespace = "default"
	}

	switch execCtx.GetMode() {
	case task.ModeCentralized:
		return c.evaluateCentralized(ctx, execCtx, namespace)
	case task.ModeDistributed:
		return c.evaluateDistributed(ctx, namespace)
	default:
		return false, fmt.Errorf("unsupported mode: %s", execCtx.GetMode())
	}
}

func (c *ConditionK8sDeploymentReady) evaluateCentralized(ctx context.Context, execCtx task.ExecutionContext, namespace string) (bool, error) {
	centralCtx, ok := execCtx.(*CentralizedContext)
	if !ok {
		return false, fmt.Errorf("invalid execution context for centralized mode")
	}

	client, ok := centralCtx.GetK8sClient(c.Member)
	if !ok {
		return false, fmt.Errorf("no k8s client for member %s", c.Member)
	}

	deployment, err := client.Get(ctx, "deployments", namespace, c.Deployment)
	if err != nil {
		logger.Errorf("ConditionK8sDeploymentReady: failed to get deployment %s/%s: %v", namespace, c.Deployment, err)
		return false, fmt.Errorf("failed to get deployment %s/%s: %w", namespace, c.Deployment, err)
	}

	return k8s.IsDeploymentReady(deployment.Object, &c.Replicas)
}

func (c *ConditionK8sDeploymentReady) evaluateDistributed(ctx context.Context, namespace string) (bool, error) {
	client := httpclient.Get()

	url := fmt.Sprintf("%s/api/v1/deployments/%s/%s", c.Member, namespace, c.Deployment)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		logger.Errorf("ConditionK8sDeploymentReady: failed to create request to %s: %v", url, err)
		return false, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		logger.Errorf("ConditionK8sDeploymentReady: failed to make request to %s: %v", url, err)
		return false, fmt.Errorf("failed to make request to %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Errorf("ConditionK8sDeploymentReady: endpoint %s returned non-success status code: %d", url, resp.StatusCode)
		return false, fmt.Errorf("endpoint returned non-success status code: %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Errorf("ConditionK8sDeploymentReady: failed to read response body from %s: %v", url, err)
		return false, fmt.Errorf("failed to read response body: %w", err)
	}

	var deployment map[string]interface{}
	if err := json.Unmarshal(bodyBytes, &deployment); err != nil {
		logger.Errorf("ConditionK8sDeploymentReady: failed to parse deployment JSON from %s: %v", url, err)
		return false, fmt.Errorf("failed to parse deployment JSON: %w", err)
	}

	return k8s.IsDeploymentReady(deployment, &c.Replicas)
}

func (c *ConditionK8sDeploymentReady) GetType() task.ConditionType {
	return task.ConditionTypeK8sDeploymentReady
}

// Helper functions

func compareInt(actual, expected int, operator string) (bool, error) {
	switch operator {
	case "eq", "":
		return actual == expected, nil
	case "ne":
		return actual != expected, nil
	case "lt":
		return actual < expected, nil
	case "gt":
		return actual > expected, nil
	case "le":
		return actual <= expected, nil
	case "ge":
		return actual >= expected, nil
	default:
		return false, fmt.Errorf("unsupported operator: %s", operator)
	}
}

func compareFloat(actual, expected float64, operator string) (bool, error) {
	switch operator {
	case "eq", "":
		return actual == expected, nil
	case "ne":
		return actual != expected, nil
	case "lt":
		return actual < expected, nil
	case "gt":
		return actual > expected, nil
	case "le":
		return actual <= expected, nil
	case "ge":
		return actual >= expected, nil
	default:
		return false, fmt.Errorf("unsupported operator: %s", operator)
	}
}

func parsePrometheusMetric(body []byte, metricName string) (float64, error) {
	var tp expfmt.TextParser
	mfs, err := tp.TextToMetricFamilies(bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("failed to parse prometheus metrics: %w", err)
	}

	mf, ok := mfs[metricName]
	if !ok {
		return 0, fmt.Errorf("metric %q not found", metricName)
	}
	if len(mf.Metric) == 0 {
		return 0, fmt.Errorf("metric %q has no samples", metricName)
	}

	m := mf.Metric[0]

	switch mf.GetType() {
	case dto.MetricType_GAUGE:
		return m.GetGauge().GetValue(), nil
	case dto.MetricType_COUNTER:
		return m.GetCounter().GetValue(), nil
	case dto.MetricType_UNTYPED:
		return m.GetUntyped().GetValue(), nil
	default:
		return 0, fmt.Errorf("metric %q has unsupported type %v", metricName, mf.GetType())
	}
}
