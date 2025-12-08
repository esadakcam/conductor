package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/esadakcam/conductor/internal/k8s"
	"github.com/esadakcam/conductor/internal/logger"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/kind/pkg/cluster"
)

// MockEpochValidator satisfies EpochChecker
type MockEpochValidator struct {
	ValidateFunc func(ctx context.Context, requestEpoch int64) (bool, error)
}

func (m *MockEpochValidator) Validate(ctx context.Context, requestEpoch int64) (bool, error) {
	if m.ValidateFunc != nil {
		return m.ValidateFunc(ctx, requestEpoch)
	}
	return true, nil
}

var (
	kubeconfigPath string
)

func TestMain(m *testing.M) {
	// Setup embedded kind cluster if not running in CI or if explicit skip is not set
	if os.Getenv("SKIP_INTEGRATION") != "" {
		os.Exit(m.Run())
	}

	clusterName := fmt.Sprintf("test-cluster-%d", time.Now().Unix())
	provider := cluster.NewProvider()

	// Check if cluster already exists (cleanup from previous failed run?)
	// We'll just create a new one.
	logger.Infof("Creating kind cluster %s...", clusterName)
	err := provider.Create(
		clusterName,
		cluster.CreateWithWaitForReady(time.Minute*5),
	)
	if err != nil {
		logger.Errorf("Failed to create kind cluster: %v", err)
		// Fallback to run without cluster if creation fails?
		// Or better, exit with error as these are integration tests.
		// But let's try to run tests anyway, maybe they mock things?
		// No, we are doing integration tests.
		os.Exit(1)
	}

	// Export kubeconfig
	kubeconfig, err := provider.KubeConfig(clusterName, false)
	if err != nil {
		logger.Errorf("Failed to get kubeconfig: %v", err)
		provider.Delete(clusterName, "")
		os.Exit(1)
	}

	// Write kubeconfig to a temp file
	tmpFile, err := os.CreateTemp("", "kind-kubeconfig-*")
	if err != nil {
		logger.Errorf("Failed to create temp kubeconfig file: %v", err)
		provider.Delete(clusterName, "")
		os.Exit(1)
	}
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(kubeconfig); err != nil {
		logger.Errorf("Failed to write kubeconfig: %v", err)
		provider.Delete(clusterName, "")
		os.Exit(1)
	}
	kubeconfigPath = tmpFile.Name()

	// Run tests
	code := m.Run()

	// Cleanup
	logger.Infof("Deleting kind cluster %s...", clusterName)
	provider.Delete(clusterName, "")
	os.Remove(kubeconfigPath)

	os.Exit(code)
}

func setupTestNamespace(t *testing.T, client KubernetesClient) string {
	ns := fmt.Sprintf("test-ns-%d-%d", time.Now().UnixNano(), rand.Intn(10000))
	nsObj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Namespace",
			"metadata": map[string]interface{}{
				"name": ns,
			},
		},
	}

	_, err := client.Create(context.Background(), "namespaces", "", nsObj)
	if err != nil {
		t.Fatalf("Failed to create test namespace %s: %v", ns, err)
	}
	return ns
}

func cleanupTestNamespace(t *testing.T, client KubernetesClient, ns string) {
	err := client.Delete(context.Background(), "namespaces", "", ns)
	if err != nil {
		t.Logf("Failed to cleanup test namespace %s: %v", ns, err)
	}
}

func TestIntegrationHandlers(t *testing.T) {
	if kubeconfigPath == "" {
		t.Skip("Skipping integration tests: no kubeconfig available")
	}

	k8sClient, err := k8s.NewClient(kubeconfigPath)
	if err != nil {
		t.Fatalf("Failed to create K8s client: %v", err)
	}

	// Verify connectivity
	_, err = k8sClient.List(context.Background(), "namespaces", "")
	if err != nil {
		t.Fatalf("Failed to list namespaces (connectivity check): %v", err)
	}

	t.Run("HandleCreateAndGet", func(t *testing.T) {
		ns := setupTestNamespace(t, k8sClient)
		defer cleanupTestNamespace(t, k8sClient, ns)

		mockEpoch := &MockEpochValidator{
			ValidateFunc: func(ctx context.Context, requestEpoch int64) (bool, error) {
				return true, nil
			},
		}
		h := NewHandler(k8sClient, mockEpoch)

		// Create Pod
		podName := "test-pod"
		body := CreateRequest{
			Epoch: 1,
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]interface{}{
					"name": podName,
					"labels": map[string]interface{}{
						"app": "test",
					},
				},
				"spec": map[string]interface{}{
					"containers": []map[string]interface{}{
						{
							"name":  "nginx",
							"image": "nginx:latest",
						},
					},
				},
			},
		}
		bodyBytes, _ := json.Marshal(body)

		req := httptest.NewRequest("POST", "/api/v1/pods/"+ns, bytes.NewReader(bodyBytes))
		req.SetPathValue("resource", "pods")
		req.SetPathValue("namespace", ns)
		w := httptest.NewRecorder()

		h.HandleCreate(w, req)

		if w.Code != http.StatusCreated {
			t.Errorf("HandleCreate failed: expected status %d, got %d. Body: %s", http.StatusCreated, w.Code, w.Body.String())
		}

		// Get Pod
		req = httptest.NewRequest("GET", "/api/v1/pods/"+ns+"/"+podName, nil)
		req.SetPathValue("resource", "pods")
		req.SetPathValue("namespace", ns)
		req.SetPathValue("name", podName)
		w = httptest.NewRecorder()

		h.HandleGet(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("HandleGet failed: expected status %d, got %d. Body: %s", http.StatusOK, w.Code, w.Body.String())
		}

		var result unstructured.Unstructured
		if err := json.Unmarshal(w.Body.Bytes(), &result.Object); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}
		if result.GetName() != podName {
			t.Errorf("Expected pod name %s, got %s", podName, result.GetName())
		}
	})

	t.Run("HandleList", func(t *testing.T) {
		ns := setupTestNamespace(t, k8sClient)
		defer cleanupTestNamespace(t, k8sClient, ns)

		mockEpoch := &MockEpochValidator{}
		h := NewHandler(k8sClient, mockEpoch)

		// Create a configmap directly to list
		cmName := "test-cm"
		cm := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name": cmName,
				},
				"data": map[string]interface{}{
					"key": "value",
				},
			},
		}
		_, err := k8sClient.Create(context.Background(), "configmaps", ns, cm)
		if err != nil {
			t.Fatalf("Failed to create configmap: %v", err)
		}

		req := httptest.NewRequest("GET", "/api/v1/configmaps/"+ns, nil)
		req.SetPathValue("resource", "configmaps")
		req.SetPathValue("namespace", ns)
		w := httptest.NewRecorder()

		h.HandleList(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("HandleList failed: expected status %d, got %d", http.StatusOK, w.Code)
		}

		var list unstructured.UnstructuredList
		if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
			t.Fatalf("Failed to unmarshal list response: %v", err)
		}

		found := false
		for _, item := range list.Items {
			if item.GetName() == cmName {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Created configmap not found in list")
		}
	})

	t.Run("HandleUpdate", func(t *testing.T) {
		ns := setupTestNamespace(t, k8sClient)
		defer cleanupTestNamespace(t, k8sClient, ns)

		mockEpoch := &MockEpochValidator{}
		h := NewHandler(k8sClient, mockEpoch)

		// Create initial configmap
		cmName := "update-test-cm"
		cm := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name": cmName,
				},
				"data": map[string]interface{}{
					"key": "initial",
				},
			},
		}
		_, err := k8sClient.Create(context.Background(), "configmaps", ns, cm)
		if err != nil {
			t.Fatalf("Failed to create initial configmap: %v", err)
		}

		// Update it
		updateBody := UpdateRequest{
			Epoch: 1,
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name": cmName,
				},
				"data": map[string]interface{}{
					"key": "updated",
				},
			},
		}
		bodyBytes, _ := json.Marshal(updateBody)

		req := httptest.NewRequest("PUT", "/api/v1/configmaps/"+ns+"/"+cmName, bytes.NewReader(bodyBytes))
		req.SetPathValue("resource", "configmaps")
		req.SetPathValue("namespace", ns)
		req.SetPathValue("name", cmName)
		w := httptest.NewRecorder()

		h.HandleUpdate(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("HandleUpdate failed: expected status %d, got %d. Body: %s", http.StatusOK, w.Code, w.Body.String())
		}

		// Verify update
		updated, err := k8sClient.Get(context.Background(), "configmaps", ns, cmName)
		if err != nil {
			t.Fatalf("Failed to get updated configmap: %v", err)
		}
		data, _, _ := unstructured.NestedStringMap(updated.Object, "data")
		if data["key"] != "updated" {
			t.Errorf("Expected data key to be 'updated', got '%s'", data["key"])
		}
	})

	t.Run("HandlePatch", func(t *testing.T) {
		ns := setupTestNamespace(t, k8sClient)
		defer cleanupTestNamespace(t, k8sClient, ns)

		mockEpoch := &MockEpochValidator{}
		h := NewHandler(k8sClient, mockEpoch)

		// Create initial configmap
		cmName := "patch-test-cm"
		cm := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name": cmName,
					"labels": map[string]interface{}{
						"original": "true",
					},
				},
				"data": map[string]interface{}{
					"key": "initial",
				},
			},
		}
		_, err := k8sClient.Create(context.Background(), "configmaps", ns, cm)
		if err != nil {
			t.Fatalf("Failed to create initial configmap: %v", err)
		}

		// Patch it
		patchBody := map[string]interface{}{
			"epoch": 1,
			"patch": map[string]interface{}{
				"metadata": map[string]interface{}{
					"labels": map[string]interface{}{
						"patched": "true",
					},
				},
				"data": map[string]interface{}{
					"key": "patched",
				},
			},
		}
		bodyBytes, _ := json.Marshal(patchBody)

		req := httptest.NewRequest("PATCH", "/api/v1/configmaps/"+ns+"/"+cmName, bytes.NewReader(bodyBytes))
		req.SetPathValue("resource", "configmaps")
		req.SetPathValue("namespace", ns)
		req.SetPathValue("name", cmName)
		w := httptest.NewRecorder()

		h.HandlePatch(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("HandlePatch failed: expected status %d, got %d. Body: %s", http.StatusOK, w.Code, w.Body.String())
		}

		// Verify update
		updated, err := k8sClient.Get(context.Background(), "configmaps", ns, cmName)
		if err != nil {
			t.Fatalf("Failed to get updated configmap: %v", err)
		}

		// Verify data change
		data, _, _ := unstructured.NestedStringMap(updated.Object, "data")
		if data["key"] != "patched" {
			t.Errorf("Expected data key to be 'patched', got '%s'", data["key"])
		}

		// Verify label addition (merge patch behavior)
		labels, _, _ := unstructured.NestedStringMap(updated.Object, "metadata", "labels")
		if labels["original"] != "true" {
			t.Errorf("Expected original label to remain")
		}
		if labels["patched"] != "true" {
			t.Errorf("Expected new label to be added")
		}
	})

	t.Run("HandleDelete", func(t *testing.T) {
		ns := setupTestNamespace(t, k8sClient)
		defer cleanupTestNamespace(t, k8sClient, ns)

		mockEpoch := &MockEpochValidator{}
		h := NewHandler(k8sClient, mockEpoch)

		cmName := "delete-test-cm"
		cm := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name": cmName,
				},
			},
		}
		_, err := k8sClient.Create(context.Background(), "configmaps", ns, cm)
		if err != nil {
			t.Fatalf("Failed to create configmap: %v", err)
		}

		deleteBody := DeleteRequest{
			Epoch: 1,
		}
		bodyBytes, _ := json.Marshal(deleteBody)

		req := httptest.NewRequest("DELETE", "/api/v1/configmaps/"+ns+"/"+cmName, bytes.NewReader(bodyBytes))
		req.SetPathValue("resource", "configmaps")
		req.SetPathValue("namespace", ns)
		req.SetPathValue("name", cmName)
		w := httptest.NewRecorder()

		h.HandleDelete(w, req)
		if w.Code != http.StatusNoContent {
			t.Errorf("HandleDelete failed: expected status %d, got %d", http.StatusNoContent, w.Code)
		}

		// Verify deletion
		_, err = k8sClient.Get(context.Background(), "configmaps", ns, cmName)
		if err == nil {
			t.Error("Expected error getting deleted configmap, got nil")
		}
	})

	t.Run("HandleExecDeployment", func(t *testing.T) {
		ns := setupTestNamespace(t, k8sClient)
		defer cleanupTestNamespace(t, k8sClient, ns)

		mockEpoch := &MockEpochValidator{}
		h := NewHandler(k8sClient, mockEpoch)

		// Create a deployment
		deployName := "exec-test-deploy"
		deploy := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name": deployName,
				},
				"spec": map[string]interface{}{
					"replicas": int64(1),
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "exec-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "exec-test",
							},
						},
						"spec": map[string]interface{}{
							"containers": []interface{}{
								map[string]interface{}{
									"name":  "busybox",
									"image": "busybox:latest",
									"command": []interface{}{
										"sh",
										"-c",
										"while true; do sleep 3600; done",
									},
								},
							},
						},
					},
				},
			},
		}
		_, err := k8sClient.Create(context.Background(), "deployments", ns, deploy)
		if err != nil {
			t.Fatalf("Failed to create deployment: %v", err)
		}

		// Wait for pod to be running (with timeout)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		var podReady bool
		for !podReady {
			select {
			case <-ctx.Done():
				t.Fatalf("Timeout waiting for pod to be ready")
			default:
				pods, err := k8sClient.List(context.Background(), "pods", ns)
				if err == nil && len(pods.Items) > 0 {
					for _, pod := range pods.Items {
						phase, _, _ := unstructured.NestedString(pod.Object, "status", "phase")
						if phase == "Running" {
							podReady = true
							break
						}
					}
				}
				if !podReady {
					time.Sleep(2 * time.Second)
				}
			}
		}

		// Execute command on deployment
		execBody := ExecDeploymentRequest{
			Epoch:   1,
			Command: []string{"echo", "hello"},
		}
		bodyBytes, _ := json.Marshal(execBody)

		req := httptest.NewRequest("POST", "/api/v1/exec/deployments/"+ns+"/"+deployName, bytes.NewReader(bodyBytes))
		req.SetPathValue("namespace", ns)
		req.SetPathValue("name", deployName)
		w := httptest.NewRecorder()

		h.HandleExecDeployment(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("HandleExecDeployment failed: expected status %d, got %d. Body: %s", http.StatusOK, w.Code, w.Body.String())
		}

		var response ExecDeploymentResponse
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}

		if response.DeploymentName != deployName {
			t.Errorf("Expected deployment name %s, got %s", deployName, response.DeploymentName)
		}
		if response.Namespace != ns {
			t.Errorf("Expected namespace %s, got %s", ns, response.Namespace)
		}
		if len(response.Results) == 0 {
			t.Error("Expected at least one pod result")
		}

		// Verify we got output from the command
		for _, result := range response.Results {
			if result.Error != "" {
				t.Errorf("Pod %s had error: %s", result.PodName, result.Error)
			}
			if result.Result == nil {
				t.Errorf("Pod %s had nil result", result.PodName)
			} else if result.Result.Stdout != "hello\n" {
				t.Errorf("Expected stdout 'hello\\n', got '%s'", result.Result.Stdout)
			}
		}
	})

	t.Run("HandleExecDeployment_FailingCommand", func(t *testing.T) {
		ns := setupTestNamespace(t, k8sClient)
		defer cleanupTestNamespace(t, k8sClient, ns)

		mockEpoch := &MockEpochValidator{}
		h := NewHandler(k8sClient, mockEpoch)

		// Create a deployment
		deployName := "exec-fail-deploy"
		deploy := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name": deployName,
				},
				"spec": map[string]interface{}{
					"replicas": int64(1),
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "exec-fail-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "exec-fail-test",
							},
						},
						"spec": map[string]interface{}{
							"containers": []interface{}{
								map[string]interface{}{
									"name":  "busybox",
									"image": "busybox:latest",
									"command": []interface{}{
										"sh",
										"-c",
										"while true; do sleep 3600; done",
									},
								},
							},
						},
					},
				},
			},
		}
		_, err := k8sClient.Create(context.Background(), "deployments", ns, deploy)
		if err != nil {
			t.Fatalf("Failed to create deployment: %v", err)
		}

		// Wait for pod to be running (with timeout)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		var podReady bool
		for !podReady {
			select {
			case <-ctx.Done():
				t.Fatalf("Timeout waiting for pod to be ready")
			default:
				pods, err := k8sClient.List(context.Background(), "pods", ns)
				if err == nil && len(pods.Items) > 0 {
					for _, pod := range pods.Items {
						phase, _, _ := unstructured.NestedString(pod.Object, "status", "phase")
						if phase == "Running" {
							podReady = true
							break
						}
					}
				}
				if !podReady {
					time.Sleep(2 * time.Second)
				}
			}
		}

		// Execute a failing command (non-existent command)
		execBody := ExecDeploymentRequest{
			Epoch:   1,
			Command: []string{"nonexistent-command-xyz"},
		}
		bodyBytes, _ := json.Marshal(execBody)

		req := httptest.NewRequest("POST", "/api/v1/exec/deployments/"+ns+"/"+deployName, bytes.NewReader(bodyBytes))
		req.SetPathValue("namespace", ns)
		req.SetPathValue("name", deployName)
		w := httptest.NewRecorder()

		h.HandleExecDeployment(w, req)

		// The request should still succeed (HTTP 200) but contain error in results
		if w.Code != http.StatusOK {
			t.Errorf("HandleExecDeployment failed: expected status %d, got %d. Body: %s", http.StatusOK, w.Code, w.Body.String())
		}

		var response ExecDeploymentResponse
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}

		if len(response.Results) == 0 {
			t.Fatal("Expected at least one pod result")
		}

		// Verify the command failed with an error
		for _, result := range response.Results {
			if result.Error == "" {
				t.Errorf("Pod %s expected error for failing command, got none", result.PodName)
			}
			// The error should indicate command failure
			t.Logf("Pod %s error (expected): %s", result.PodName, result.Error)
		}
	})

	t.Run("HandleExecDeployment_InvalidBody", func(t *testing.T) {
		ns := setupTestNamespace(t, k8sClient)
		defer cleanupTestNamespace(t, k8sClient, ns)

		mockEpoch := &MockEpochValidator{}
		h := NewHandler(k8sClient, mockEpoch)

		req := httptest.NewRequest("POST", "/api/v1/exec/deployments/"+ns+"/test-deploy", bytes.NewReader([]byte("invalid json")))
		req.SetPathValue("namespace", ns)
		req.SetPathValue("name", "test-deploy")
		w := httptest.NewRecorder()

		h.HandleExecDeployment(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("HandleExecDeployment_MissingCommand", func(t *testing.T) {
		ns := setupTestNamespace(t, k8sClient)
		defer cleanupTestNamespace(t, k8sClient, ns)

		mockEpoch := &MockEpochValidator{}
		h := NewHandler(k8sClient, mockEpoch)

		execBody := ExecDeploymentRequest{
			Epoch:   1,
			Command: []string{},
		}
		bodyBytes, _ := json.Marshal(execBody)

		req := httptest.NewRequest("POST", "/api/v1/exec/deployments/"+ns+"/test-deploy", bytes.NewReader(bodyBytes))
		req.SetPathValue("namespace", ns)
		req.SetPathValue("name", "test-deploy")
		w := httptest.NewRecorder()

		h.HandleExecDeployment(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, w.Code)
		}

		var errResp ErrorResponse
		json.Unmarshal(w.Body.Bytes(), &errResp)
		if errResp.Error != "command is required" {
			t.Errorf("Expected error 'command is required', got '%s'", errResp.Error)
		}
	})

	t.Run("HandleExecDeployment_StaleEpoch", func(t *testing.T) {
		ns := setupTestNamespace(t, k8sClient)
		defer cleanupTestNamespace(t, k8sClient, ns)

		mockEpoch := &MockEpochValidator{
			ValidateFunc: func(ctx context.Context, requestEpoch int64) (bool, error) {
				return false, nil
			},
		}
		h := NewHandler(k8sClient, mockEpoch)

		execBody := ExecDeploymentRequest{
			Epoch:   1,
			Command: []string{"echo", "test"},
		}
		bodyBytes, _ := json.Marshal(execBody)

		req := httptest.NewRequest("POST", "/api/v1/exec/deployments/"+ns+"/test-deploy", bytes.NewReader(bodyBytes))
		req.SetPathValue("namespace", ns)
		req.SetPathValue("name", "test-deploy")
		w := httptest.NewRecorder()

		h.HandleExecDeployment(w, req)

		if w.Code != http.StatusConflict {
			t.Errorf("Expected status %d, got %d", http.StatusConflict, w.Code)
		}
	})

	t.Run("HandleExecDeployment_DeploymentNotFound", func(t *testing.T) {
		ns := setupTestNamespace(t, k8sClient)
		defer cleanupTestNamespace(t, k8sClient, ns)

		mockEpoch := &MockEpochValidator{}
		h := NewHandler(k8sClient, mockEpoch)

		execBody := ExecDeploymentRequest{
			Epoch:   1,
			Command: []string{"echo", "test"},
		}
		bodyBytes, _ := json.Marshal(execBody)

		req := httptest.NewRequest("POST", "/api/v1/exec/deployments/"+ns+"/nonexistent-deploy", bytes.NewReader(bodyBytes))
		req.SetPathValue("namespace", ns)
		req.SetPathValue("name", "nonexistent-deploy")
		w := httptest.NewRecorder()

		h.HandleExecDeployment(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Errorf("Expected status %d, got %d", http.StatusInternalServerError, w.Code)
		}
	})
}
