package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/task"
	"github.com/esadakcam/conductor/internal/utils/httpclient"
	"k8s.io/apimachinery/pkg/types"
)

// ActionEndpoint executes HTTP endpoint action
type ActionEndpoint struct {
	task.ActionEndpoint
}

func (a *ActionEndpoint) Execute(ctx context.Context, execCtx task.ExecutionContext) error {
	if a.Endpoint == "" {
		return fmt.Errorf("endpoint is required")
	}

	method := a.Method
	if method == "" {
		method = "GET"
	}

	var reqBody io.Reader
	if a.Body != "" {
		reqBody = bytes.NewBufferString(a.Body)
	}

	req, err := http.NewRequestWithContext(ctx, method, a.Endpoint, reqBody)
	if err != nil {
		logger.Errorf("ActionEndpoint: failed to create request to %s: %v", a.Endpoint, err)
		return fmt.Errorf("failed to create request: %w", err)
	}

	for key, value := range a.Headers {
		req.Header.Set(key, value)
	}
	if idempotencyID := execCtx.GetIdempotencyID(); idempotencyID != "" {
		req.Header.Set("X-Idempotency-Id", idempotencyID)
	}
	if a.Body != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	client := httpclient.Get()
	resp, err := client.Do(req)
	if err != nil {
		logger.Errorf("ActionEndpoint: failed to execute request to %s: %v", a.Endpoint, err)
		return fmt.Errorf("failed to execute request to %s: %w", a.Endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("request failed with status code %d", resp.StatusCode)
	}

	return nil
}

func (a *ActionEndpoint) GetType() task.ActionType {
	return task.ActionTypeEndpoint
}

// ActionEcho logs a message
type ActionEcho struct {
	task.ActionEcho
}

func (a *ActionEcho) Execute(ctx context.Context, execCtx task.ExecutionContext) error {
	logger.Info(a.Message)
	return nil
}

func (a *ActionEcho) GetType() task.ActionType {
	return task.ActionTypeEcho
}

// ActionDelay pauses execution
type ActionDelay struct {
	task.ActionDelay
}

func (a *ActionDelay) Execute(ctx context.Context, execCtx task.ExecutionContext) error {
	logger.Infof("ActionDelay: sleeping for %s", a.Time)
	select {
	case <-time.After(a.Time):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *ActionDelay) GetType() task.ActionType {
	return task.ActionTypeDelay
}

// ActionK8sRestartDeployment restarts a deployment
type ActionK8sRestartDeployment struct {
	task.ActionK8sRestartDeployment
}

func (a *ActionK8sRestartDeployment) Execute(ctx context.Context, execCtx task.ExecutionContext) error {
	if a.Deployment == "" {
		return fmt.Errorf("deployment name is required")
	}
	if a.Member == "" {
		return fmt.Errorf("member is required")
	}

	namespace := a.Namespace
	if namespace == "" {
		namespace = "default"
	}

	patchData := map[string]interface{}{
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{
					"annotations": map[string]string{
						"kubectl.kubernetes.io/restartedAt": time.Now().Format(time.RFC3339),
					},
				},
			},
		},
	}

	switch execCtx.GetMode() {
	case task.ModeCentralized:
		return a.executeCentralized(ctx, execCtx, namespace, patchData)
	case task.ModeDistributed:
		return a.executeDistributed(ctx, execCtx, namespace, patchData)
	default:
		return fmt.Errorf("unsupported mode: %s", execCtx.GetMode())
	}
}

func (a *ActionK8sRestartDeployment) executeCentralized(ctx context.Context, execCtx task.ExecutionContext, namespace string, patchData map[string]interface{}) error {
	centralCtx, ok := execCtx.(*CentralizedContext)
	if !ok {
		return fmt.Errorf("invalid execution context for centralized mode")
	}

	client, ok := centralCtx.GetK8sClient(a.Member)
	if !ok {
		return fmt.Errorf("no k8s client for member %s", a.Member)
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		return fmt.Errorf("failed to marshal patch data: %w", err)
	}

	_, err = client.Patch(ctx, "deployments", namespace, a.Deployment, types.MergePatchType, patchBytes)
	if err != nil {
		logger.Errorf("ActionK8sRestartDeployment: failed to restart deployment %s/%s: %v", namespace, a.Deployment, err)
		return fmt.Errorf("failed to restart deployment: %w", err)
	}

	logger.Infof("successfully restarted deployment %s/%s via member %s", namespace, a.Deployment, a.Member)
	return nil
}

func (a *ActionK8sRestartDeployment) executeDistributed(ctx context.Context, execCtx task.ExecutionContext, namespace string, patchData map[string]interface{}) error {
	return patchResource(ctx, a.Member, "deployments", namespace, a.Deployment, patchData, execCtx.GetEpoch(), execCtx.GetIdempotencyID())
}

func (a *ActionK8sRestartDeployment) GetType() task.ActionType {
	return task.ActionTypeK8sRestartDeployment
}

// ActionK8sScaleDeployment scales a deployment
type ActionK8sScaleDeployment struct {
	task.ActionK8sScaleDeployment
}

func (a *ActionK8sScaleDeployment) Execute(ctx context.Context, execCtx task.ExecutionContext) error {
	if a.Deployment == "" {
		return fmt.Errorf("deployment name is required")
	}
	if a.Member == "" {
		return fmt.Errorf("member is required")
	}

	namespace := a.Namespace
	if namespace == "" {
		namespace = "default"
	}

	patchData := map[string]interface{}{
		"spec": map[string]interface{}{
			"replicas": a.Replicas,
		},
	}

	logger.Infof("Scaling deployment %s/%s to %d replicas via %s", namespace, a.Deployment, a.Replicas, a.Member)

	switch execCtx.GetMode() {
	case task.ModeCentralized:
		return a.executeCentralized(ctx, execCtx, namespace, patchData)
	case task.ModeDistributed:
		return a.executeDistributed(ctx, execCtx, namespace, patchData)
	default:
		return fmt.Errorf("unsupported mode: %s", execCtx.GetMode())
	}
}

func (a *ActionK8sScaleDeployment) executeCentralized(ctx context.Context, execCtx task.ExecutionContext, namespace string, patchData map[string]interface{}) error {
	centralCtx, ok := execCtx.(*CentralizedContext)
	if !ok {
		return fmt.Errorf("invalid execution context for centralized mode")
	}

	client, ok := centralCtx.GetK8sClient(a.Member)
	if !ok {
		return fmt.Errorf("no k8s client for member %s", a.Member)
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		return fmt.Errorf("failed to marshal patch data: %w", err)
	}

	_, err = client.Patch(ctx, "deployments", namespace, a.Deployment, types.MergePatchType, patchBytes)
	if err != nil {
		logger.Errorf("ActionK8sScaleDeployment: failed to scale deployment %s/%s: %v", namespace, a.Deployment, err)
		return fmt.Errorf("failed to scale deployment: %w", err)
	}

	logger.Infof("Successfully scaled deployment %s/%s to %d replicas via member %s", namespace, a.Deployment, a.Replicas, a.Member)
	return nil
}

func (a *ActionK8sScaleDeployment) executeDistributed(ctx context.Context, execCtx task.ExecutionContext, namespace string, patchData map[string]interface{}) error {
	return patchResource(ctx, a.Member, "deployments", namespace, a.Deployment, patchData, execCtx.GetEpoch(), execCtx.GetIdempotencyID())
}

func (a *ActionK8sScaleDeployment) GetType() task.ActionType {
	return task.ActionTypeK8sScaleDeployment
}

// ActionK8sUpdateConfigMap updates a configmap
type ActionK8sUpdateConfigMap struct {
	task.ActionK8sUpdateConfigMap
}

func (a *ActionK8sUpdateConfigMap) Execute(ctx context.Context, execCtx task.ExecutionContext) error {
	if a.ConfigMap == "" {
		return fmt.Errorf("config_map name is required")
	}
	if a.Member == "" {
		return fmt.Errorf("member is required")
	}
	if a.Key == "" {
		return fmt.Errorf("key is required")
	}

	namespace := a.Namespace
	if namespace == "" {
		namespace = "default"
	}

	patchData := map[string]interface{}{
		"data": map[string]string{
			a.Key: a.Value,
		},
	}

	logger.Infof("Updating ConfigMap %s/%s key %s via %s", namespace, a.ConfigMap, a.Key, a.Member)

	switch execCtx.GetMode() {
	case task.ModeCentralized:
		return a.executeCentralized(ctx, execCtx, namespace, patchData)
	case task.ModeDistributed:
		return a.executeDistributed(ctx, execCtx, namespace, patchData)
	default:
		return fmt.Errorf("unsupported mode: %s", execCtx.GetMode())
	}
}

func (a *ActionK8sUpdateConfigMap) executeCentralized(ctx context.Context, execCtx task.ExecutionContext, namespace string, patchData map[string]interface{}) error {
	centralCtx, ok := execCtx.(*CentralizedContext)
	if !ok {
		return fmt.Errorf("invalid execution context for centralized mode")
	}

	client, ok := centralCtx.GetK8sClient(a.Member)
	if !ok {
		return fmt.Errorf("no k8s client for member %s", a.Member)
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		return fmt.Errorf("failed to marshal patch data: %w", err)
	}

	_, err = client.Patch(ctx, "configmaps", namespace, a.ConfigMap, types.MergePatchType, patchBytes)
	if err != nil {
		logger.Errorf("ActionK8sUpdateConfigMap: failed to update ConfigMap %s/%s: %v", namespace, a.ConfigMap, err)
		return fmt.Errorf("failed to update ConfigMap: %w", err)
	}

	logger.Infof("Successfully updated ConfigMap %s/%s key %s via member %s", namespace, a.ConfigMap, a.Key, a.Member)
	return nil
}

func (a *ActionK8sUpdateConfigMap) executeDistributed(ctx context.Context, execCtx task.ExecutionContext, namespace string, patchData map[string]interface{}) error {
	return patchResource(ctx, a.Member, "configmaps", namespace, a.ConfigMap, patchData, execCtx.GetEpoch(), execCtx.GetIdempotencyID())
}

func (a *ActionK8sUpdateConfigMap) GetType() task.ActionType {
	return task.ActionTypeK8sUpdateConfigMap
}

// ActionK8sWaitDeploymentRollout waits for deployment rollout
type ActionK8sWaitDeploymentRollout struct {
	task.ActionK8sWaitDeploymentRollout
}

func (a *ActionK8sWaitDeploymentRollout) Execute(ctx context.Context, execCtx task.ExecutionContext) error {
	if a.Deployment == "" {
		return fmt.Errorf("deployment name is required")
	}
	if a.Member == "" {
		return fmt.Errorf("member is required")
	}

	namespace := a.Namespace
	if namespace == "" {
		namespace = "default"
	}

	timeout := a.Timeout
	if timeout == 0 {
		timeout = 5 * time.Minute
	}

	logger.Infof("Waiting for deployment %s/%s rollout via %s (timeout: %s)", namespace, a.Deployment, a.Member, timeout)

	switch execCtx.GetMode() {
	case task.ModeCentralized:
		return a.executeCentralized(ctx, execCtx, namespace, timeout)
	case task.ModeDistributed:
		return a.executeDistributed(ctx, execCtx, namespace, timeout)
	default:
		return fmt.Errorf("unsupported mode: %s", execCtx.GetMode())
	}
}

func (a *ActionK8sWaitDeploymentRollout) executeCentralized(ctx context.Context, execCtx task.ExecutionContext, namespace string, timeout time.Duration) error {
	centralCtx, ok := execCtx.(*CentralizedContext)
	if !ok {
		return fmt.Errorf("invalid execution context for centralized mode")
	}

	client, ok := centralCtx.GetK8sClient(a.Member)
	if !ok {
		return fmt.Errorf("no k8s client for member %s", a.Member)
	}

	err := client.WaitForDeploymentRollout(ctx, namespace, a.Deployment, timeout)
	if err != nil {
		logger.Errorf("ActionK8sWaitDeploymentRollout: failed to wait for deployment %s/%s rollout: %v", namespace, a.Deployment, err)
		return fmt.Errorf("failed to wait for deployment rollout: %w", err)
	}

	logger.Infof("ActionK8sWaitDeploymentRollout: deployment %s/%s rollout completed via member %s", namespace, a.Deployment, a.Member)
	return nil
}

func (a *ActionK8sWaitDeploymentRollout) executeDistributed(ctx context.Context, execCtx task.ExecutionContext, namespace string, timeout time.Duration) error {
	reqPayload := map[string]interface{}{
		"epoch":   execCtx.GetEpoch(),
		"timeout": timeout.String(),
	}

	reqBody, err := json.Marshal(reqPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal request payload: %w", err)
	}

	httpCtx, cancel := context.WithTimeout(ctx, timeout+30*time.Second)
	defer cancel()

	url := fmt.Sprintf("%s/api/v1/rollout/deployments/%s/%s", a.Member, namespace, a.Deployment)
	req, err := http.NewRequestWithContext(httpCtx, "POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Idempotency-Id", execCtx.GetIdempotencyID())

	client := httpclient.GetLongRunner()
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request to %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("wait rollout request failed with status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	logger.Infof("ActionK8sWaitDeploymentRollout: deployment %s/%s rollout completed via %s", namespace, a.Deployment, a.Member)
	return nil
}

func (a *ActionK8sWaitDeploymentRollout) GetType() task.ActionType {
	return task.ActionTypeK8sWaitDeploymentRollout
}

// ActionK8sExecDeployment executes commands on deployment pods
type ActionK8sExecDeployment struct {
	task.ActionK8sExecDeployment
}

func (a *ActionK8sExecDeployment) Execute(ctx context.Context, execCtx task.ExecutionContext) error {
	if a.Deployment == "" {
		return fmt.Errorf("deployment name is required")
	}
	if a.Member == "" {
		return fmt.Errorf("member is required")
	}
	if len(a.Command) == 0 {
		return fmt.Errorf("command is required")
	}

	namespace := a.Namespace
	if namespace == "" {
		namespace = "default"
	}

	logger.Infof("Executing k8s exec deployment %s/%s via %s: %v", namespace, a.Deployment, a.Member, a.Command)

	switch execCtx.GetMode() {
	case task.ModeCentralized:
		return a.executeCentralized(ctx, execCtx, namespace)
	case task.ModeDistributed:
		return a.executeDistributed(ctx, execCtx, namespace)
	default:
		return fmt.Errorf("unsupported mode: %s", execCtx.GetMode())
	}
}

func (a *ActionK8sExecDeployment) executeCentralized(ctx context.Context, execCtx task.ExecutionContext, namespace string) error {
	centralCtx, ok := execCtx.(*CentralizedContext)
	if !ok {
		return fmt.Errorf("invalid execution context for centralized mode")
	}

	client, ok := centralCtx.GetK8sClient(a.Member)
	if !ok {
		return fmt.Errorf("no k8s client for member %s", a.Member)
	}

	results, err := client.ExecDeployment(ctx, namespace, a.Deployment, a.Container, a.Command)
	if err != nil {
		logger.Errorf("ActionK8sExecDeployment: failed to exec on deployment %s/%s: %v", namespace, a.Deployment, err)
		return fmt.Errorf("failed to exec on deployment: %w", err)
	}

	for _, result := range results {
		if result.Error != nil {
			logger.Errorf("ActionK8sExecDeployment: exec failed on pod %s: %v", result.PodName, result.Error)
			return fmt.Errorf("exec failed on pod %s: %w", result.PodName, result.Error)
		}
		logger.Infof("ActionK8sExecDeployment: exec succeeded on pod %s, stdout: %s", result.PodName, result.Result.Stdout)
	}

	logger.Infof("ActionK8sExecDeployment: successfully executed command on deployment %s/%s", namespace, a.Deployment)
	return nil
}

func (a *ActionK8sExecDeployment) executeDistributed(ctx context.Context, execCtx task.ExecutionContext, namespace string) error {
	reqPayload := map[string]interface{}{
		"epoch":   execCtx.GetEpoch(),
		"command": a.Command,
	}
	if a.Container != "" {
		reqPayload["container"] = a.Container
	}

	reqBody, err := json.Marshal(reqPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal request payload: %w", err)
	}

	url := fmt.Sprintf("%s/api/v1/exec/deployments/%s/%s", a.Member, namespace, a.Deployment)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Idempotency-Id", execCtx.GetIdempotencyID())

	client := httpclient.Get()
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request to %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("exec request failed with status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	logger.Infof("ActionK8sExecDeployment: successfully executed command on deployment %s/%s via %s", namespace, a.Deployment, a.Member)
	return nil
}

func (a *ActionK8sExecDeployment) GetType() task.ActionType {
	return task.ActionTypeK8sExecDeployment
}

// Helper function for distributed mode HTTP patching
func patchResource(ctx context.Context, member, resource, namespace, name string, patchData map[string]interface{}, epoch int64, idempotencyID string) error {
	patchPayload := map[string]interface{}{
		"epoch": epoch,
		"patch": patchData,
	}

	patchBody, err := json.Marshal(patchPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal patch payload: %w", err)
	}

	client := httpclient.Get()
	patchURL := fmt.Sprintf("%s/api/v1/%s/%s/%s", member, resource, namespace, name)
	req, err := http.NewRequestWithContext(ctx, "PATCH", patchURL, bytes.NewBuffer(patchBody))
	if err != nil {
		return fmt.Errorf("failed to create PATCH request to %s: %w", member, err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Idempotency-Id", idempotencyID)

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute PATCH request to %s: %w", member, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PATCH request failed with status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return nil
}
