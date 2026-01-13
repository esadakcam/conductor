package executor

import (
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
	"github.com/esadakcam/conductor/internal/task"
	"github.com/esadakcam/conductor/internal/utils/httpclient"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// ActionConfigValueSum distributes a sum value across multiple cluster configmaps
type ActionConfigValueSum struct {
	task.ActionConfigValueSum
}

func (a *ActionConfigValueSum) Execute(ctx context.Context, execCtx task.ExecutionContext) error {
	namespace := a.Namespace
	if namespace == "" {
		namespace = "default"
	}

	switch execCtx.GetMode() {
	case task.ModeCentralized:
		return a.executeCentralized(ctx, execCtx, namespace)
	case task.ModeDistributed:
		return a.executeDistributed(ctx, execCtx, namespace)
	default:
		return fmt.Errorf("unsupported mode: %s", execCtx.GetMode())
	}
}

func (a *ActionConfigValueSum) executeCentralized(ctx context.Context, execCtx task.ExecutionContext, namespace string) error {
	centralCtx, ok := execCtx.(*CentralizedContext)
	if !ok {
		return fmt.Errorf("invalid execution context for centralized mode")
	}

	curSumMap := a.fetchCurrentValuesCentralized(ctx, centralCtx, namespace)

	curSum := 0
	for _, value := range curSumMap {
		curSum += value
	}

	if curSum == a.Sum {
		logger.Info("config value sum is equal to the sum, no action needed")
		return nil
	}

	return a.distributeAndApplyChangesCentralized(ctx, centralCtx, curSumMap, namespace)
}

func (a *ActionConfigValueSum) fetchCurrentValuesCentralized(ctx context.Context, centralCtx *CentralizedContext, namespace string) map[string]int {
	var mutex sync.Mutex
	curSumMap := make(map[string]int)
	var wg sync.WaitGroup

	for _, member := range a.Members {
		wg.Add(1)
		go func(member string) {
			defer wg.Done()
			client, ok := centralCtx.GetK8sClient(member)
			if !ok {
				logger.Errorf("ActionConfigValueSum: no k8s client for member %s", member)
				return
			}

			configMap, err := client.Get(ctx, "configmaps", namespace, a.ConfigMapName)
			if err != nil {
				logger.Errorf("ActionConfigValueSum: failed to get configmap %s/%s from %s: %v", namespace, a.ConfigMapName, member, err)
				return
			}

			data, found, err := unstructured.NestedStringMap(configMap.Object, "data")
			if err != nil || !found {
				logger.Errorf("ActionConfigValueSum: failed to get data from configmap %s/%s: %v", namespace, a.ConfigMapName, err)
				return
			}

			valueStr, ok := data[a.Key]
			if !ok {
				logger.Errorf("ActionConfigValueSum: key %s not found in configmap %s/%s", a.Key, namespace, a.ConfigMapName)
				return
			}

			value, err := strconv.Atoi(valueStr)
			if err != nil {
				logger.Errorf("ActionConfigValueSum: failed to parse value %s as int: %v", valueStr, err)
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

func (a *ActionConfigValueSum) distributeAndApplyChangesCentralized(ctx context.Context, centralCtx *CentralizedContext, curSumMap map[string]int, namespace string) error {
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

		client, ok := centralCtx.GetK8sClient(member)
		if !ok {
			return fmt.Errorf("no k8s client for member %s", member)
		}

		patchData := map[string]interface{}{
			"data": map[string]string{
				a.Key: strconv.Itoa(newValue),
			},
		}

		patchBytes, err := json.Marshal(patchData)
		if err != nil {
			return fmt.Errorf("failed to marshal patch data: %w", err)
		}

		_, err = client.Patch(ctx, "configmaps", namespace, a.ConfigMapName, types.MergePatchType, patchBytes)
		if err != nil {
			logger.Errorf("ActionConfigValueSum: failed to patch configmap %s/%s on %s: %v", namespace, a.ConfigMapName, member, err)
			return fmt.Errorf("failed to patch configmap on %s: %w", member, err)
		}

		logger.Infof("successfully patched %s/%s on %s from %d to %d", a.ConfigMapName, a.Key, member, currentValue, newValue)
	}
	return nil
}

func (a *ActionConfigValueSum) executeDistributed(ctx context.Context, execCtx task.ExecutionContext, namespace string) error {
	curSumMap := a.fetchCurrentValuesDistributed(ctx)

	curSum := 0
	for _, value := range curSumMap {
		curSum += value
	}

	if curSum == a.Sum {
		logger.Info("config value sum is equal to the sum, no action needed")
		return nil
	}

	return a.distributeAndApplyChangesDistributed(ctx, execCtx, curSumMap)
}

func (a *ActionConfigValueSum) fetchCurrentValuesDistributed(ctx context.Context) map[string]int {
	var mutex sync.Mutex
	curSumMap := make(map[string]int)
	var wg sync.WaitGroup

	for _, member := range a.Members {
		wg.Add(1)
		go func(member string) {
			defer wg.Done()
			value, err := fetchConfigValueWithRetry(ctx, member, a.ConfigMapName, a.Key)
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

func (a *ActionConfigValueSum) distributeAndApplyChangesDistributed(ctx context.Context, execCtx task.ExecutionContext, curSumMap map[string]int) error {
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

		logger.Infof("patching %s/%s on %s from %d to %d", a.ConfigMapName, a.Key, member, currentValue, newValue)

		patchData := map[string]interface{}{
			"data": map[string]string{
				a.Key: strconv.Itoa(newValue),
			},
		}

		if err := patchResource(ctx, member, "configmaps", "default", a.ConfigMapName, patchData, execCtx.GetEpoch(), execCtx.GetIdempotencyID()); err != nil {
			logger.Errorf("ActionConfigValueSum: failed to patch config value %s/%s on %s: %v", a.ConfigMapName, a.Key, member, err)
			return fmt.Errorf("failed to patch config value on %s: %w", member, err)
		}

		logger.Infof("successfully patched %s/%s on %s from %d to %d", a.ConfigMapName, a.Key, member, currentValue, newValue)
	}
	return nil
}

func (a *ActionConfigValueSum) GetType() task.ActionType {
	return task.ActionTypeConfigValueSum
}

// fetchConfigValueWithRetry fetches a config value with exponential backoff retry
func fetchConfigValueWithRetry(ctx context.Context, member, configMapName, key string) (int, error) {
	const (
		maxRetries     = 3
		initialBackoff = 500 * time.Millisecond
		maxBackoff     = 4 * time.Second
	)

	client := httpclient.Get()
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

		if resp.StatusCode >= 500 || resp.StatusCode == 429 {
			resp.Body.Close()
			lastErr = fmt.Errorf("received retryable status code %d from %s", resp.StatusCode, member)
			logger.Warnf("fetchConfigValue: received status %d (attempt %d/%d)", resp.StatusCode, attempt+1, maxRetries+1)
			continue
		}

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
