package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/esadakcam/conductor/internal/k8s"
	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/task"
	"github.com/esadakcam/conductor/internal/utils/httpclient"
)

func (c *ConditionK8sDeploymentReady) Evaluate(ctx context.Context, ec task.ExecutionContext) (bool, error) {
	if c.Member == "" {
		err := fmt.Errorf("member is required")
		logger.Error("ConditionK8sDeploymentReady: member is required")
		return false, err
	}

	if c.Deployment == "" {
		err := fmt.Errorf("deployment is required")
		logger.Error("ConditionK8sDeploymentReady: deployment is required")
		return false, err
	}

	namespace := c.Namespace
	if namespace == "" {
		namespace = "default"
	}

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
