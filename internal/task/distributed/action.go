package distributed

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/task"
	"github.com/esadakcam/conductor/internal/utils/httpclient"
)

func (a *ActionEndpoint) GetType() task.ActionType {
	return task.ActionTypeEndpoint
}

func (a *ActionEndpoint) Execute(ctx context.Context, payload any) error {
	_, idempotencyId, err := GetPayload(payload)
	if err != nil {
		logger.Errorf("ActionEndpoint: failed to get payload: %v", err)
		return fmt.Errorf("failed to get payload: %w", err)
	}
	if a.Endpoint == "" {
		err := fmt.Errorf("endpoint is required")
		logger.Error("ActionEndpoint: endpoint is required")
		return err
	}

	// Default to GET if method is not specified
	method := a.Method
	if method == "" {
		method = "GET"
	}

	// Create request body if provided
	var reqBody io.Reader
	if a.Body != "" {
		reqBody = bytes.NewBufferString(a.Body)
	}

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, method, a.Endpoint, reqBody)
	if err != nil {
		logger.Errorf("ActionEndpoint: failed to create request to %s: %v", a.Endpoint, err)
		return fmt.Errorf("failed to create request: %w", err)
	}

	// Add headers if provided
	for key, value := range a.Headers {
		req.Header.Set(key, value)
	}
	req.Header.Set("X-Idempotency-Id", idempotencyId)

	// Set Content-Type header if body is provided and Content-Type is not already set
	if a.Body != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	// Execute request
	client := httpclient.Get()

	resp, err := client.Do(req)
	if err != nil {
		logger.Errorf("ActionEndpoint: failed to execute request to %s: %v", a.Endpoint, err)
		return fmt.Errorf("failed to execute request to %s: %w", a.Endpoint, err)
	}
	defer resp.Body.Close()

	// Check for HTTP error status codes (4xx, 5xx)
	if resp.StatusCode >= 400 {
		err := fmt.Errorf("request failed with status code %d", resp.StatusCode)
		logger.Errorf("ActionEndpoint: request to %s failed with status code %d", a.Endpoint, resp.StatusCode)
		return err
	}

	return nil
}

func (a *ActionEcho) GetType() task.ActionType {
	return task.ActionTypeEcho
}

func (a *ActionEcho) Execute(ctx context.Context, payload any) error {
	logger.Info(a.Message)
	return nil
}

func (a *ActionDelay) GetType() task.ActionType {
	return task.ActionTypeDelay
}

func (a *ActionDelay) Execute(ctx context.Context, payload any) error {
	logger.Infof("ActionDelay: sleeping for %s", a.Time)
	select {
	case <-time.After(a.Time):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *ActionConfigValueSum) GetType() task.ActionType {
	return task.ActionTypeConfigValueSum
}

func (a *ActionConfigValueSum) Execute(ctx context.Context, payload any) error {
	epoch, idempotencyId, err := GetPayload(payload)
	if err != nil {
		logger.Errorf("ActionConfigValueSum: failed to get payload: %v", err)
		return fmt.Errorf("failed to get payload: %w", err)
	}
	curSumMap := a.fetchCurrentValues(ctx)

	curSum := 0
	for _, value := range curSumMap {
		curSum += value
	}

	if curSum == a.Sum {
		logger.Info("config value sum is equal to the sum, no action needed")
		return nil
	}

	return a.distributeAndApplyChanges(ctx, curSumMap, epoch, idempotencyId)
}

func (a *ActionK8sExecDeployment) GetType() task.ActionType {
	return task.ActionTypeK8sExecDeployment
}

func (a *ActionK8sExecDeployment) Execute(ctx context.Context, payload any) error {
	epoch, idempotencyId, err := GetPayload(payload)

	if err != nil {
		logger.Errorf("ActionK8sExecDeployment: failed to get payload: %v", err)
		return fmt.Errorf("failed to get payload: %w", err)
	}

	if a.Deployment == "" {
		err := fmt.Errorf("deployment name is required")
		logger.Error("ActionK8sExecDeployment: deployment name is required")
		return err
	}

	if a.Member == "" {
		err := fmt.Errorf("member is required")
		logger.Error("ActionK8sExecDeployment: member is required")
		return err
	}

	if len(a.Command) == 0 {
		err := fmt.Errorf("command is required")
		logger.Error("ActionK8sExecDeployment: command is required")
		return err
	}

	namespace := a.Namespace
	if namespace == "" {
		namespace = "default"
	}

	logger.Infof("Executing k8s exec deployment %s/%s via %s: %v", namespace, a.Deployment, a.Member, a.Command)

	// Build request payload
	reqPayload := map[string]interface{}{
		"epoch":   epoch,
		"command": a.Command,
	}
	if a.Container != "" {
		reqPayload["container"] = a.Container
	}

	reqBody, err := json.Marshal(reqPayload)
	if err != nil {
		logger.Errorf("ActionK8sExecDeployment: failed to marshal request payload: %v", err)
		return fmt.Errorf("failed to marshal request payload: %w", err)
	}

	// Make POST request to member
	url := fmt.Sprintf("%s/api/v1/exec/deployments/%s/%s", a.Member, namespace, a.Deployment)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		logger.Errorf("ActionK8sExecDeployment: failed to create request to %s: %v", url, err)
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Idempotency-Id", idempotencyId)

	client := httpclient.Get()
	resp, err := client.Do(req)
	if err != nil {
		logger.Errorf("ActionK8sExecDeployment: failed to execute request to %s: %v", url, err)
		return fmt.Errorf("failed to execute request to %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		logger.Errorf("ActionK8sExecDeployment: request to %s failed with status %d: %s", url, resp.StatusCode, string(bodyBytes))
		return fmt.Errorf("exec request failed with status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	logger.Infof("ActionK8sExecDeployment: successfully executed command on deployment %s/%s via %s", namespace, a.Deployment, a.Member)
	return nil
}

func (a *ActionConfigValueSum) fetchCurrentValues(ctx context.Context) map[string]int {
	var mutex sync.Mutex
	var curSumMap = make(map[string]int)
	var wg sync.WaitGroup

	for _, member := range a.Members {
		wg.Add(1)
		go func(member string) {
			defer wg.Done()
			value, err := fetchConfigValue(ctx, member, a.ConfigMapName, a.Key)
			if err != nil {
				logger.Errorf("error fetching config value from %s: %v", member, err)
				return
			}
			mutex.Lock()
			curSumMap[member] = value
			mutex.Unlock()
		}(member)
	}
	wg.Wait()
	return curSumMap
}

func (a *ActionConfigValueSum) distributeAndApplyChanges(ctx context.Context, curSumMap map[string]int, epoch int64, idempotencyId string) error {
	if len(curSumMap) == 0 {
		logger.Warnf("no members available to patch %s/%s", a.ConfigMapName, a.Key)
		return nil
	}
	availableMemberSize := len(curSumMap)
	perMemberValue := a.Sum / availableMemberSize
	remainder := a.Sum % availableMemberSize
	for member, currentValue := range curSumMap {
		newValue := perMemberValue
		if remainder > 0 {
			newValue++
			remainder--
		}

		if newValue == currentValue {
			continue
		}

		logger.Infof("patched %s/%s on %s from %d to %d", a.ConfigMapName, a.Key, member, currentValue, newValue)

		if err := patchConfigValue(ctx, member, a.ConfigMapName, a.Key, newValue, epoch, idempotencyId); err != nil {
			logger.Errorf("ActionConfigValueSum: failed to patch config value %s/%s on %s: %v", a.ConfigMapName, a.Key, member, err)
			return fmt.Errorf("failed to patch config value on %s: %w", member, err)
		}

		logger.Infof("successfully patched %s/%s on %s from %d to %d", a.ConfigMapName, a.Key, member, currentValue, newValue)
	}
	return nil
}

func (a *ActionK8sRestartDeployment) GetType() task.ActionType {
	return task.ActionTypeK8sRestartDeployment
}

func (a *ActionK8sRestartDeployment) Execute(ctx context.Context, payload any) error {
	epoch, idempotencyId, err := GetPayload(payload)
	if err != nil {
		logger.Errorf("ActionK8sRestartDeployment: failed to get payload: %v", err)
		return fmt.Errorf("failed to get payload: %w", err)
	}

	if a.Deployment == "" {
		err := fmt.Errorf("deployment name is required")
		logger.Error("ActionK8sRestartDeployment: deployment name is required")
		return err
	}

	if a.Member == "" {
		err := fmt.Errorf("member is required")
		logger.Error("ActionK8sRestartDeployment: member is required")
		return err
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

	if err := patchResource(ctx, a.Member, "deployments", namespace, a.Deployment, patchData, epoch, idempotencyId); err != nil {
		logger.Errorf("ActionK8sRestartDeployment: failed to restart deployment %s/%s via %s: %v", namespace, a.Deployment, a.Member, err)
		return fmt.Errorf("failed to restart deployment: %w", err)
	}

	logger.Infof("successfully restarted deployment %s/%s via %s", namespace, a.Deployment, a.Member)
	return nil
}

func (a *ActionK8sWaitDeploymentRollout) GetType() task.ActionType {
	return task.ActionTypeK8sWaitDeploymentRollout
}

func (a *ActionK8sWaitDeploymentRollout) Execute(ctx context.Context, payload any) error {
	epoch, idempotencyId, err := GetPayload(payload)
	if err != nil {
		logger.Errorf("ActionK8sWaitDeploymentRollout: failed to get payload: %v", err)
		return fmt.Errorf("failed to get payload: %w", err)
	}

	if a.Deployment == "" {
		err := fmt.Errorf("deployment name is required")
		logger.Error("ActionK8sWaitDeploymentRollout: deployment name is required")
		return err
	}

	if a.Member == "" {
		err := fmt.Errorf("member is required")
		logger.Error("ActionK8sWaitDeploymentRollout: member is required")
		return err
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

	// Build request payload
	reqPayload := map[string]interface{}{
		"epoch":   epoch,
		"timeout": timeout.String(),
	}

	reqBody, err := json.Marshal(reqPayload)
	if err != nil {
		logger.Errorf("ActionK8sWaitDeploymentRollout: failed to marshal request payload: %v", err)
		return fmt.Errorf("failed to marshal request payload: %w", err)
	}

	// Create context with timeout for the HTTP request (add buffer for network)
	httpCtx, cancel := context.WithTimeout(ctx, timeout+30*time.Second)
	defer cancel()

	// Make POST request to member
	url := fmt.Sprintf("%s/api/v1/rollout/deployments/%s/%s", a.Member, namespace, a.Deployment)
	req, err := http.NewRequestWithContext(httpCtx, "POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		logger.Errorf("ActionK8sWaitDeploymentRollout: failed to create request to %s: %v", url, err)
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Idempotency-Id", idempotencyId)

	// Use long-runner client to allow context timeout to control the request
	// instead of the default 30s client timeout
	client := httpclient.GetLongRunner()
	resp, err := client.Do(req)
	if err != nil {
		logger.Errorf("ActionK8sWaitDeploymentRollout: failed to execute request to %s: %v", url, err)
		return fmt.Errorf("failed to execute request to %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		logger.Errorf("ActionK8sWaitDeploymentRollout: request to %s failed with status %d: %s", url, resp.StatusCode, string(bodyBytes))
		return fmt.Errorf("wait rollout request failed with status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	logger.Infof("ActionK8sWaitDeploymentRollout: deployment %s/%s rollout completed via %s", namespace, a.Deployment, a.Member)
	return nil
}

func (a *ActionK8sUpdateConfigMap) GetType() task.ActionType {
	return task.ActionTypeK8sUpdateConfigMap
}

func (a *ActionK8sUpdateConfigMap) Execute(ctx context.Context, payload any) error {
	epoch, idempotencyId, err := GetPayload(payload)
	if err != nil {
		logger.Errorf("ActionK8sUpdateConfigMap: failed to get payload: %v", err)
		return fmt.Errorf("failed to get payload: %w", err)
	}

	if a.ConfigMap == "" {
		err := fmt.Errorf("config_map name is required")
		logger.Error("ActionK8sUpdateConfigMap: config_map name is required")
		return err
	}

	if a.Member == "" {
		err := fmt.Errorf("member is required")
		logger.Error("ActionK8sUpdateConfigMap: member is required")
		return err
	}

	if a.Key == "" {
		err := fmt.Errorf("key is required")
		logger.Error("ActionK8sUpdateConfigMap: key is required")
		return err
	}

	namespace := a.Namespace
	if namespace == "" {
		namespace = "default"
	}

	logger.Infof("Updating ConfigMap %s/%s key %s via %s", namespace, a.ConfigMap, a.Key, a.Member)

	patchData := map[string]interface{}{
		"data": map[string]string{
			a.Key: a.Value,
		},
	}

	if err := patchResource(ctx, a.Member, "configmaps", namespace, a.ConfigMap, patchData, epoch, idempotencyId); err != nil {
		logger.Errorf("ActionK8sUpdateConfigMap: failed to update ConfigMap %s/%s via %s: %v", namespace, a.ConfigMap, a.Member, err)
		return fmt.Errorf("failed to update ConfigMap: %w", err)
	}

	logger.Infof("Successfully updated ConfigMap %s/%s key %s via %s", namespace, a.ConfigMap, a.Key, a.Member)
	return nil
}

func (a *ActionK8sScaleDeployment) GetType() task.ActionType {
	return task.ActionTypeK8sScaleDeployment
}

func (a *ActionK8sScaleDeployment) Execute(ctx context.Context, payload any) error {
	epoch, idempotencyId, err := GetPayload(payload)
	if err != nil {
		logger.Errorf("ActionK8sScaleDeployment: failed to get payload: %v", err)
		return fmt.Errorf("failed to get payload: %w", err)
	}

	if a.Deployment == "" {
		err := fmt.Errorf("deployment name is required")
		logger.Error("ActionK8sScaleDeployment: deployment name is required")
		return err
	}

	if a.Member == "" {
		err := fmt.Errorf("member is required")
		logger.Error("ActionK8sScaleDeployment: member is required")
		return err
	}

	namespace := a.Namespace
	if namespace == "" {
		namespace = "default"
	}

	logger.Infof("Scaling deployment %s/%s to %d replicas via %s", namespace, a.Deployment, a.Replicas, a.Member)

	patchData := map[string]interface{}{
		"spec": map[string]interface{}{
			"replicas": a.Replicas,
		},
	}

	if err := patchResource(ctx, a.Member, "deployments", namespace, a.Deployment, patchData, epoch, idempotencyId); err != nil {
		logger.Errorf("ActionK8sScaleDeployment: failed to scale deployment %s/%s via %s: %v", namespace, a.Deployment, a.Member, err)
		return fmt.Errorf("failed to scale deployment: %w", err)
	}

	logger.Infof("Successfully scaled deployment %s/%s to %d replicas via %s", namespace, a.Deployment, a.Replicas, a.Member)
	return nil
}

// Returns epoch and idempotencyId from payload
func GetPayload(payload any) (int64, string, error) {
	data, ok := payload.(map[string]any)
	if !ok {
		return 0, "", fmt.Errorf("invalid payload format")
	}

	idempotencyID, ok := data["idempotencyId"].(string)
	if !ok || idempotencyID == "" {
		return 0, "", fmt.Errorf("invalid or missing idempotencyId in payload")
	}

	switch v := data["epoch"].(type) {
	case int:
		return int64(v), idempotencyID, nil
	case int32:
		return int64(v), idempotencyID, nil
	case int64:
		return v, idempotencyID, nil
	case float64:
		return int64(v), idempotencyID, nil
	case float32:
		return int64(v), idempotencyID, nil
	default:
		return 0, idempotencyID, fmt.Errorf("invalid or missing epoch in payload")
	}
}
