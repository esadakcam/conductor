package task

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/esadakcam/conductor/internal/logger"
)

func (c *ConditionEndpointSuccess) Evaluate(ctx context.Context) (bool, error) {
	if c.Endpoint == "" {
		err := fmt.Errorf("endpoint is required")
		logger.Error("ConditionEndpointSuccess: endpoint is required")
		return false, err
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

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
		bodyStr := strings.TrimSpace(string(bodyBytes))
		expectedBody := strings.TrimSpace(c.ResponseBody)
		if bodyStr != expectedBody {
			return false, nil
		}
	}

	return true, nil
}

func (c *ConditionEndpointValue) Evaluate(ctx context.Context) (bool, error) {
	if c.Endpoint == "" {
		err := fmt.Errorf("endpoint is required")
		logger.Error("ConditionEndpointValue: endpoint is required")
		return false, err
	}

	client := &http.Client{
		Timeout: 30 * time.Second,
	}

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

	// Read response body
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Errorf("ConditionEndpointValue: failed to read response body from %s: %v", c.Endpoint, err)
		return false, fmt.Errorf("failed to read response body: %w", err)
	}

	// Parse response body as integer
	bodyStr := strings.TrimSpace(string(bodyBytes))
	responseValue, err := strconv.Atoi(bodyStr)
	if err != nil {
		logger.Errorf("ConditionEndpointValue: failed to parse response body as integer from %s: %v", c.Endpoint, err)
		return false, fmt.Errorf("failed to parse response body as integer: %w", err)
	}

	// Compare using operator
	switch c.Operator {
	case "eq":
		return responseValue == c.Value, nil
	case "ne":
		return responseValue != c.Value, nil
	case "lt":
		return responseValue < c.Value, nil
	case "gt":
		return responseValue > c.Value, nil
	case "le":
		return responseValue <= c.Value, nil
	case "ge":
		return responseValue >= c.Value, nil
	default:
		err := fmt.Errorf("unsupported operator: %s", c.Operator)
		logger.Errorf("ConditionEndpointValue: %v", err)
		return false, err
	}
}

func (c *ConditionAlwaysTrue) Evaluate(ctx context.Context) (bool, error) {
	return true, nil
}

func (c *ConditionEndpointSuccess) GetType() ConditionType {
	return ConditionTypeEndpointSuccess
}

func (c *ConditionEndpointValue) GetType() ConditionType {
	return ConditionTypeEndpointValue
}

func (c *ConditionAlwaysTrue) GetType() ConditionType {
	return ConditionTypeAlwaysTrue
}

func (a *ActionEndpoint) Execute(ctx context.Context, epoch int64) error {
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
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

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

func (a *ActionEcho) Execute(ctx context.Context, epoch int64) error {
	logger.Info(a.Message)
	return nil
}

func (a *ActionEndpoint) GetType() ActionType {
	return ActionTypeEndpoint
}

func (a *ActionEcho) GetType() ActionType {
	return ActionTypeEcho
}

func (a *ActionConfigValueSum) Execute(ctx context.Context, epoch int64) error {
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

func (a *ActionConfigValueSum) GetType() ActionType {
	return ActionTypeConfigValueSum
}

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

func fetchConfigValue(ctx context.Context, member string, configMapName string, key string) (int, error) {
	const (
		maxRetries     = 3
		initialBackoff = 500 * time.Millisecond
		maxBackoff     = 4 * time.Second
	)

	client := &http.Client{
		Timeout: 30 * time.Second,
	}
	url := fmt.Sprintf("%s/api/v1/configmaps/default/%s", member, configMapName)

	var lastErr error
	backoff := initialBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			logger.Infof("fetchConfigValue: retrying attempt %d/%d for %s after %v", attempt, maxRetries, url, backoff)
			select {
			case <-ctx.Done():
				return 0, fmt.Errorf("context cancelled while retrying: %w", ctx.Err())
			case <-time.After(backoff):
			}
			// Exponential backoff with jitter
			backoff = time.Duration(float64(backoff) * 2.0)
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			lastErr = fmt.Errorf("failed to create request to %s: %w", member, err)
			continue
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("failed to make request to %s: %w", member, err)
			logger.Warnf("fetchConfigValue: request failed (attempt %d/%d): %v", attempt+1, maxRetries+1, err)
			continue
		}

		// Check for retryable HTTP status codes
		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			resp.Body.Close()
			lastErr = fmt.Errorf("received retryable status code %d from %s", resp.StatusCode, member)
			logger.Warnf("fetchConfigValue: received status %d (attempt %d/%d)", resp.StatusCode, attempt+1, maxRetries+1)
			continue
		}

		// Non-retryable error status codes
		if resp.StatusCode >= 400 {
			resp.Body.Close()
			return 0, fmt.Errorf("received non-retryable status code %d from %s", resp.StatusCode, member)
		}

		bodyBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("failed to read response body from %s: %w", member, err)
			logger.Warnf("fetchConfigValue: failed to read response body (attempt %d/%d): %v", attempt+1, maxRetries+1, err)
			continue
		}

		var configMap struct {
			Data map[string]string `json:"data"`
		}
		if err := json.Unmarshal(bodyBytes, &configMap); err != nil {
			lastErr = fmt.Errorf("failed to parse ConfigMap JSON from %s: %w", member, err)
			logger.Warnf("fetchConfigValue: failed to parse JSON (attempt %d/%d): %v", attempt+1, maxRetries+1, err)
			continue
		}

		if key == "" {
			return 0, fmt.Errorf("key is required but not specified for ConfigMap %s from %s", configMapName, member)
		}

		valueStr, exists := configMap.Data[key]
		if !exists {
			return 0, nil
		}

		value, err := strconv.Atoi(strings.TrimSpace(valueStr))
		if err != nil {
			return 0, fmt.Errorf("failed to parse value '%s' as integer from %s: %w", valueStr, member, err)
		}

		if attempt > 0 {
			logger.Infof("fetchConfigValue: successfully fetched config value after %d retries from %s", attempt, member)
		}

		return value, nil
	}

	return 0, fmt.Errorf("failed to fetch config value after %d attempts: %w", maxRetries+1, lastErr)
}

func patchResource(ctx context.Context, member string, resource string, namespace string, name string, patchData map[string]interface{}, epoch int64) error {
	// Create patch payload
	patchPayload := map[string]interface{}{
		"epoch": epoch,
		"patch": patchData,
	}

	patchBody, err := json.Marshal(patchPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal patch payload: %w", err)
	}

	// Make PATCH request
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	patchURL := fmt.Sprintf("%s/api/v1/%s/%s/%s", member, resource, namespace, name)
	req, err := http.NewRequestWithContext(ctx, "PATCH", patchURL, bytes.NewBuffer(patchBody))
	if err != nil {
		return fmt.Errorf("failed to create PATCH request to %s: %w", member, err)
	}

	req.Header.Set("Content-Type", "application/json")

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

func patchConfigValue(ctx context.Context, member string, configMapName string, key string, newValue int, epoch int64) error {
	patchData := map[string]interface{}{
		"data": map[string]string{
			key: strconv.Itoa(newValue),
		},
	}

	return patchResource(ctx, member, "configmaps", "default", configMapName, patchData, epoch)
}
