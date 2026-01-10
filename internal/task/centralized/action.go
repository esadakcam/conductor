package centralized

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/esadakcam/conductor/internal/k8s"
	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/task"
	"github.com/esadakcam/conductor/internal/utils/httpclient"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func (a *ActionEndpoint) Execute(ctx context.Context, payload any) error {
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

func (a *ActionEndpoint) GetType() task.ActionType {
	return task.ActionTypeEndpoint
}

func (a *ActionConfigValueSum) Execute(ctx context.Context, payload any) error {
	k8sClients, err := getK8sClients(payload)
	if err != nil {
		logger.Errorf("ActionConfigValueSum: failed to get k8s clients: %v", err)
		return fmt.Errorf("failed to get k8s clients: %w", err)
	}

	namespace := a.Namespace
	if namespace == "" {
		namespace = "default"
	}

	curSumMap := a.fetchCurrentValues(ctx, k8sClients, namespace)

	curSum := 0
	for _, value := range curSumMap {
		curSum += value
	}

	if curSum == a.Sum {
		logger.Info("config value sum is equal to the sum, no action needed")
		return nil
	}

	return a.distributeAndApplyChanges(ctx, k8sClients, curSumMap, namespace)
}

func (a *ActionConfigValueSum) fetchCurrentValues(ctx context.Context, k8sClients map[string]*k8s.Client, namespace string) map[string]int {
	var mutex sync.Mutex
	var curSumMap = make(map[string]int)
	var wg sync.WaitGroup

	for _, member := range a.Members {
		wg.Add(1)
		go func(member string) {
			defer wg.Done()
			client, ok := k8sClients[member]
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

func (a *ActionConfigValueSum) distributeAndApplyChanges(ctx context.Context, k8sClients map[string]*k8s.Client, curSumMap map[string]int, namespace string) error {
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

		client, ok := k8sClients[member]
		if !ok {
			logger.Errorf("ActionConfigValueSum: no k8s client for member %s", member)
			return fmt.Errorf("no k8s client for member %s", member)
		}

		patchData := map[string]interface{}{
			"data": map[string]string{
				a.Key: strconv.Itoa(newValue),
			},
		}

		patchBytes, err := json.Marshal(patchData)
		if err != nil {
			logger.Errorf("ActionConfigValueSum: failed to marshal patch data: %v", err)
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

func (a *ActionConfigValueSum) GetType() task.ActionType {
	return task.ActionTypeConfigValueSum
}

func (a *ActionEcho) Execute(ctx context.Context, payload any) error {
	logger.Info(a.Message)
	return nil
}

func (a *ActionEcho) GetType() task.ActionType {
	return task.ActionTypeEcho
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

func (a *ActionDelay) GetType() task.ActionType {
	return task.ActionTypeDelay
}

func (a *ActionK8sExecDeployment) Execute(ctx context.Context, payload any) error {
	k8sClients, err := getK8sClients(payload)
	if err != nil {
		logger.Errorf("ActionK8sExecDeployment: failed to get k8s clients: %v", err)
		return fmt.Errorf("failed to get k8s clients: %w", err)
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

	client, ok := k8sClients[a.Member]
	if !ok {
		err := fmt.Errorf("no k8s client for member %s", a.Member)
		logger.Errorf("ActionK8sExecDeployment: %v", err)
		return err
	}

	namespace := a.Namespace
	if namespace == "" {
		namespace = "default"
	}

	logger.Infof("Executing command on deployment %s/%s via member %s: %v", namespace, a.Deployment, a.Member, a.Command)

	results, err := client.ExecDeployment(ctx, namespace, a.Deployment, a.Container, a.Command)
	if err != nil {
		logger.Errorf("ActionK8sExecDeployment: failed to exec on deployment %s/%s: %v", namespace, a.Deployment, err)
		return fmt.Errorf("failed to exec on deployment: %w", err)
	}

	// Check if any pod execution failed
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

func (a *ActionK8sExecDeployment) GetType() task.ActionType {
	return task.ActionTypeK8sExecDeployment
}

func (a *ActionK8sRestartDeployment) Execute(ctx context.Context, payload any) error {
	k8sClients, err := getK8sClients(payload)
	if err != nil {
		logger.Errorf("ActionK8sRestartDeployment: failed to get k8s clients: %v", err)
		return fmt.Errorf("failed to get k8s clients: %w", err)
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

	client, ok := k8sClients[a.Member]
	if !ok {
		err := fmt.Errorf("no k8s client for member %s", a.Member)
		logger.Errorf("ActionK8sRestartDeployment: %v", err)
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

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		logger.Errorf("ActionK8sRestartDeployment: failed to marshal patch data: %v", err)
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

func (a *ActionK8sRestartDeployment) GetType() task.ActionType {
	return task.ActionTypeK8sRestartDeployment
}

func (a *ActionK8sWaitDeploymentRollout) Execute(ctx context.Context, payload any) error {
	k8sClients, err := getK8sClients(payload)
	if err != nil {
		logger.Errorf("ActionK8sWaitDeploymentRollout: failed to get k8s clients: %v", err)
		return fmt.Errorf("failed to get k8s clients: %w", err)
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

	client, ok := k8sClients[a.Member]
	if !ok {
		err := fmt.Errorf("no k8s client for member %s", a.Member)
		logger.Errorf("ActionK8sWaitDeploymentRollout: %v", err)
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

	logger.Infof("Waiting for deployment %s/%s rollout via member %s (timeout: %s)", namespace, a.Deployment, a.Member, timeout)

	err = client.WaitForDeploymentRollout(ctx, namespace, a.Deployment, timeout)
	if err != nil {
		logger.Errorf("ActionK8sWaitDeploymentRollout: failed to wait for deployment %s/%s rollout: %v", namespace, a.Deployment, err)
		return fmt.Errorf("failed to wait for deployment rollout: %w", err)
	}

	logger.Infof("ActionK8sWaitDeploymentRollout: deployment %s/%s rollout completed via member %s", namespace, a.Deployment, a.Member)
	return nil
}

func (a *ActionK8sWaitDeploymentRollout) GetType() task.ActionType {
	return task.ActionTypeK8sWaitDeploymentRollout
}

func (a *ActionK8sUpdateConfigMap) Execute(ctx context.Context, payload any) error {
	k8sClients, err := getK8sClients(payload)
	if err != nil {
		logger.Errorf("ActionK8sUpdateConfigMap: failed to get k8s clients: %v", err)
		return fmt.Errorf("failed to get k8s clients: %w", err)
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

	client, ok := k8sClients[a.Member]
	if !ok {
		err := fmt.Errorf("no k8s client for member %s", a.Member)
		logger.Errorf("ActionK8sUpdateConfigMap: %v", err)
		return err
	}

	namespace := a.Namespace
	if namespace == "" {
		namespace = "default"
	}

	logger.Infof("Updating ConfigMap %s/%s key %s via member %s", namespace, a.ConfigMap, a.Key, a.Member)

	patchData := map[string]interface{}{
		"data": map[string]string{
			a.Key: a.Value,
		},
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		logger.Errorf("ActionK8sUpdateConfigMap: failed to marshal patch data: %v", err)
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

func (a *ActionK8sUpdateConfigMap) GetType() task.ActionType {
	return task.ActionTypeK8sUpdateConfigMap
}

func (a *ActionK8sScaleDeployment) Execute(ctx context.Context, payload any) error {
	k8sClients, err := getK8sClients(payload)
	if err != nil {
		logger.Errorf("ActionK8sScaleDeployment: failed to get k8s clients: %v", err)
		return fmt.Errorf("failed to get k8s clients: %w", err)
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

	client, ok := k8sClients[a.Member]
	if !ok {
		err := fmt.Errorf("no k8s client for member %s", a.Member)
		logger.Errorf("ActionK8sScaleDeployment: %v", err)
		return err
	}

	namespace := a.Namespace
	if namespace == "" {
		namespace = "default"
	}

	logger.Infof("Scaling deployment %s/%s to %d replicas via member %s", namespace, a.Deployment, a.Replicas, a.Member)

	patchData := map[string]interface{}{
		"spec": map[string]interface{}{
			"replicas": a.Replicas,
		},
	}

	patchBytes, err := json.Marshal(patchData)
	if err != nil {
		logger.Errorf("ActionK8sScaleDeployment: failed to marshal patch data: %v", err)
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

func (a *ActionK8sScaleDeployment) GetType() task.ActionType {
	return task.ActionTypeK8sScaleDeployment
}
