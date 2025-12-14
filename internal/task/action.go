package task

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
	"github.com/esadakcam/conductor/internal/utils/httpclient"
)

func (a *ActionEndpoint) GetType() ActionType {
	return ActionTypeEndpoint
}

func (a *ActionEndpoint) Execute(ctx context.Context, epoch int64, idempotencyId string) error {
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

func (a *ActionEcho) GetType() ActionType {
	return ActionTypeEcho
}

func (a *ActionEcho) Execute(ctx context.Context, epoch int64, idempotencyId string) error {
	logger.Info(a.Message)
	return nil
}

func (a *ActionConfigValueSum) GetType() ActionType {
	return ActionTypeConfigValueSum
}

func (a *ActionConfigValueSum) Execute(ctx context.Context, epoch int64, idempotencyId string) error {
	curSumMap := a.fetchCurrentValues(ctx)

	curSum := 0
	for _, value := range curSumMap {
		curSum += value
	}

	if curSum == a.Sum {
		logger.Info("config value sum is equal to the sum, no action needed")
		return nil
	}

	return a.distributeAndApplyChanges(ctx, curSumMap, epoch)
}

func (a *ActionK8sExecDeployment) GetType() ActionType {
	return ActionTypeK8sExecDeployment
}

func (a *ActionK8sExecDeployment) Execute(ctx context.Context, epoch int64, idempotencyId string) error {
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

func (a *ActionConfigValueSum) distributeAndApplyChanges(ctx context.Context, curSumMap map[string]int, epoch int64) error {
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

		if err := patchConfigValue(ctx, member, a.ConfigMapName, a.Key, newValue, epoch); err != nil {
			logger.Errorf("ActionConfigValueSum: failed to patch config value %s/%s on %s: %v", a.ConfigMapName, a.Key, member, err)
			return fmt.Errorf("failed to patch config value on %s: %w", member, err)
		}

		logger.Infof("successfully patched %s/%s on %s from %d to %d", a.ConfigMapName, a.Key, member, currentValue, newValue)

		if a.OnChange != nil {
			if err := a.OnChange.Execute(ctx, member, "default", epoch); err != nil {
				logger.Errorf("ActionConfigValueSum: failed to execute onChange action on %s: %v", member, err)
				return fmt.Errorf("failed to execute onChange action on %s: %w", member, err)
			}
		}
	}
	return nil
}

// TODO: Don't need on change, just concat with the next action
func (o *OnChangeDeploymentRestart) GetType() OnChangeType {
	return OnChangeTypeDeploymentRestart
}

func (o *OnChangeDeploymentRestart) Execute(ctx context.Context, member string, namespace string, epoch int64) error {
	if o.Deployment == "" {
		err := fmt.Errorf("deployment name is required")
		logger.Error("OnChangeDeploymentRestart: deployment name is required")
		return err
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

	if err := patchResource(ctx, member, "deployments", namespace, o.Deployment, patchData, epoch); err != nil {
		logger.Errorf("OnChangeDeploymentRestart: failed to restart deployment %s/%s via %s: %v", namespace, o.Deployment, member, err)
		return fmt.Errorf("failed to restart deployment: %w", err)
	}

	logger.Infof("successfully restarted deployment %s/%s via %s", namespace, o.Deployment, member)
	return nil
}
