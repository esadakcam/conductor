package k8s

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/esadakcam/conductor/internal/logger"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/kind/pkg/cluster"
)

var (
	testKubeconfigPath string
	testClusterName    string
	testSetupOnce      sync.Once
	testProvider       *cluster.Provider
)

func TestMain(m *testing.M) {
	if os.Getenv("SKIP_INTEGRATION") != "" {
		os.Exit(m.Run())
	}

	// Run tests
	code := m.Run()

	// Cleanup
	if testClusterName != "" && testProvider != nil {
		logger.Infof("Deleting k8s client test cluster %s...", testClusterName)
		testProvider.Delete(testClusterName, "")
	}
	if testKubeconfigPath != "" {
		os.Remove(testKubeconfigPath)
	}

	os.Exit(code)
}

// setupTestCluster creates a dedicated kind cluster for k8s client tests
func setupTestCluster(t *testing.T) {
	t.Helper()

	if os.Getenv("SKIP_INTEGRATION") != "" {
		t.Skip("Skipping integration tests: SKIP_INTEGRATION is set")
	}

	var setupErr error
	testSetupOnce.Do(func() {
		// Create a dedicated cluster for k8s client tests
		testClusterName = fmt.Sprintf("k8s-client-test-cluster-%d", time.Now().Unix())
		testProvider = cluster.NewProvider()

		// Check if cluster already exists and delete it if it does
		existingClusters, _ := testProvider.List()
		for _, name := range existingClusters {
			if name == testClusterName {
				logger.Infof("Cleaning up existing cluster %s...", testClusterName)
				testProvider.Delete(testClusterName, "")
			}
		}

		logger.Infof("Creating kind cluster %s for k8s client tests...", testClusterName)
		err := testProvider.Create(
			testClusterName,
			cluster.CreateWithWaitForReady(time.Minute*5),
		)
		if err != nil {
			setupErr = fmt.Errorf("failed to create kind cluster: %w", err)
			return
		}

		// Export kubeconfig
		kubeconfig, err := testProvider.KubeConfig(testClusterName, false)
		if err != nil {
			setupErr = fmt.Errorf("failed to get kubeconfig: %w", err)
			testProvider.Delete(testClusterName, "")
			return
		}

		// Write kubeconfig to a temp file
		tmpFile, err := os.CreateTemp("", "k8s-client-kind-kubeconfig-*")
		if err != nil {
			setupErr = fmt.Errorf("failed to create temp kubeconfig file: %w", err)
			testProvider.Delete(testClusterName, "")
			return
		}

		if _, err := tmpFile.WriteString(kubeconfig); err != nil {
			setupErr = fmt.Errorf("failed to write kubeconfig: %w", err)
			tmpFile.Close()
			testProvider.Delete(testClusterName, "")
			return
		}
		tmpFile.Close()
		testKubeconfigPath = tmpFile.Name()
	})

	if setupErr != nil {
		t.Fatalf("Failed to set up k8s client test cluster: %v", setupErr)
	}

	if testKubeconfigPath == "" {
		t.Fatal("Failed to set up k8s client test cluster: kubeconfig path is empty")
	}
}

// verifyClusterConnectivity ensures we're connected to a real Kubernetes cluster
func verifyClusterConnectivity(t *testing.T, client *Client) {
	t.Helper()
	ctx := context.Background()
	_, err := client.List(ctx, "namespaces", "")
	if err != nil {
		t.Fatalf("Failed to verify cluster connectivity (list namespaces): %v", err)
	}
}

func setupTestNamespace(t *testing.T, client *Client) string {
	t.Helper()
	ns := fmt.Sprintf("test-ns-%d", time.Now().UnixNano())
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

func cleanupTestNamespace(t *testing.T, client *Client, ns string) {
	err := client.Delete(context.Background(), "namespaces", "", ns)
	if err != nil {
		t.Logf("Failed to cleanup test namespace %s: %v", ns, err)
	}
}

func TestClient_Get(t *testing.T) {
	setupTestCluster(t)

	client, err := NewClient(testKubeconfigPath)
	if err != nil {
		t.Fatalf("Failed to create K8s client: %v", err)
	}

	// Verify we're connected to a real cluster
	verifyClusterConnectivity(t, client)

	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	// Create a ConfigMap to retrieve
	cmName := fmt.Sprintf("test-cm-%d", time.Now().UnixNano())
	cmObj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]interface{}{
				"name":      cmName,
				"namespace": ns,
			},
			"data": map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
			},
		},
	}

	_, err = client.Create(ctx, "configmaps", ns, cmObj)
	if err != nil {
		t.Fatalf("Failed to create ConfigMap: %v", err)
	}

	t.Run("Get existing resource", func(t *testing.T) {
		got, err := client.Get(ctx, "configmaps", ns, cmName)
		if err != nil {
			t.Fatalf("Get failed: %v", err)
		}

		if got.GetName() != cmName {
			t.Errorf("Expected name %s, got %s", cmName, got.GetName())
		}

		if got.GetNamespace() != ns {
			t.Errorf("Expected namespace %s, got %s", ns, got.GetNamespace())
		}

		data, found, err := unstructured.NestedMap(got.Object, "data")
		if err != nil || !found {
			t.Fatalf("Failed to get data field: %v", err)
		}

		if data["key1"] != "value1" {
			t.Errorf("Expected data.key1 to be 'value1', got %v", data["key1"])
		}
	})

	t.Run("Get non-existent resource", func(t *testing.T) {
		_, err := client.Get(ctx, "configmaps", ns, "non-existent")
		if err == nil {
			t.Error("Expected error when getting non-existent resource")
		}
	})

	t.Run("Get with invalid resource type", func(t *testing.T) {
		_, err := client.Get(ctx, "invalid-resource", ns, cmName)
		if err == nil {
			t.Error("Expected error when using invalid resource type")
		}
	})

	// Cleanup
	client.Delete(ctx, "configmaps", ns, cmName)
}

func TestClient_List(t *testing.T) {
	setupTestCluster(t)

	client, err := NewClient(testKubeconfigPath)
	if err != nil {
		t.Fatalf("Failed to create K8s client: %v", err)
	}

	// Verify we're connected to a real cluster
	verifyClusterConnectivity(t, client)

	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	// Create multiple ConfigMaps
	cmCount := 3
	cmNames := make([]string, cmCount)
	for i := 0; i < cmCount; i++ {
		cmName := fmt.Sprintf("test-cm-%d-%d", time.Now().UnixNano(), i)
		cmNames[i] = cmName
		cmObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      cmName,
					"namespace": ns,
				},
				"data": map[string]interface{}{
					"index": fmt.Sprintf("%d", i),
				},
			},
		}

		_, err := client.Create(ctx, "configmaps", ns, cmObj)
		if err != nil {
			t.Fatalf("Failed to create ConfigMap %s: %v", cmName, err)
		}
		defer client.Delete(ctx, "configmaps", ns, cmName)
	}

	t.Run("List resources in namespace", func(t *testing.T) {
		list, err := client.List(ctx, "configmaps", ns)
		if err != nil {
			t.Fatalf("List failed: %v", err)
		}

		if len(list.Items) < cmCount {
			t.Errorf("Expected at least %d ConfigMaps, got %d", cmCount, len(list.Items))
		}

		// Verify our ConfigMaps are in the list
		found := make(map[string]bool)
		for _, item := range list.Items {
			if item.GetNamespace() == ns {
				found[item.GetName()] = true
			}
		}

		for _, name := range cmNames {
			if !found[name] {
				t.Errorf("ConfigMap %s not found in list", name)
			}
		}
	})

	t.Run("List all namespaces", func(t *testing.T) {
		list, err := client.List(ctx, "namespaces", "")
		if err != nil {
			t.Fatalf("List all namespaces failed: %v", err)
		}

		if len(list.Items) == 0 {
			t.Error("Expected at least one namespace")
		}

		// Verify our test namespace is in the list
		found := false
		for _, item := range list.Items {
			if item.GetName() == ns {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Test namespace %s not found in list", ns)
		}
	})

	t.Run("List with invalid resource type", func(t *testing.T) {
		_, err := client.List(ctx, "invalid-resource", ns)
		if err == nil {
			t.Error("Expected error when using invalid resource type")
		}
	})
}

func TestClient_Create(t *testing.T) {
	setupTestCluster(t)

	client, err := NewClient(testKubeconfigPath)
	if err != nil {
		t.Fatalf("Failed to create K8s client: %v", err)
	}

	// Verify we're connected to a real cluster
	verifyClusterConnectivity(t, client)

	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	t.Run("Create ConfigMap", func(t *testing.T) {
		cmName := fmt.Sprintf("test-cm-%d", time.Now().UnixNano())
		cmObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      cmName,
					"namespace": ns,
				},
				"data": map[string]interface{}{
					"key": "value",
				},
			},
		}

		created, err := client.Create(ctx, "configmaps", ns, cmObj)
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		if created.GetName() != cmName {
			t.Errorf("Expected name %s, got %s", cmName, created.GetName())
		}

		// Verify it was actually created
		got, err := client.Get(ctx, "configmaps", ns, cmName)
		if err != nil {
			t.Fatalf("Failed to get created ConfigMap: %v", err)
		}

		if got.GetName() != cmName {
			t.Errorf("Created ConfigMap name mismatch: expected %s, got %s", cmName, got.GetName())
		}

		// Cleanup
		client.Delete(ctx, "configmaps", ns, cmName)
	})

	t.Run("Create Pod", func(t *testing.T) {
		podName := fmt.Sprintf("test-pod-%d", time.Now().UnixNano())
		podObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Pod",
				"metadata": map[string]interface{}{
					"name":      podName,
					"namespace": ns,
				},
				"spec": map[string]interface{}{
					"containers": []map[string]interface{}{
						{
							"name":  "test-container",
							"image": "nginx:latest",
						},
					},
				},
			},
		}

		created, err := client.Create(ctx, "pods", ns, podObj)
		if err != nil {
			t.Fatalf("Create Pod failed: %v", err)
		}

		if created.GetName() != podName {
			t.Errorf("Expected name %s, got %s", podName, created.GetName())
		}

		// Cleanup
		client.Delete(ctx, "pods", ns, podName)
	})

	t.Run("Create Service", func(t *testing.T) {
		svcName := fmt.Sprintf("test-svc-%d", time.Now().UnixNano())
		svcObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Service",
				"metadata": map[string]interface{}{
					"name":      svcName,
					"namespace": ns,
				},
				"spec": map[string]interface{}{
					"ports": []map[string]interface{}{
						{
							"port":       80,
							"targetPort": 8080,
						},
					},
					"selector": map[string]interface{}{
						"app": "test",
					},
				},
			},
		}

		created, err := client.Create(ctx, "services", ns, svcObj)
		if err != nil {
			t.Fatalf("Create Service failed: %v", err)
		}

		if created.GetName() != svcName {
			t.Errorf("Expected name %s, got %s", svcName, created.GetName())
		}

		// Cleanup
		client.Delete(ctx, "services", ns, svcName)
	})

	t.Run("Create with invalid resource type", func(t *testing.T) {
		obj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name": "test",
				},
			},
		}

		_, err := client.Create(ctx, "invalid-resource", ns, obj)
		if err == nil {
			t.Error("Expected error when using invalid resource type")
		}
	})
}

func TestClient_Update(t *testing.T) {
	setupTestCluster(t)

	client, err := NewClient(testKubeconfigPath)
	if err != nil {
		t.Fatalf("Failed to create K8s client: %v", err)
	}

	// Verify we're connected to a real cluster
	verifyClusterConnectivity(t, client)

	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	// Create a ConfigMap first
	cmName := fmt.Sprintf("test-cm-%d", time.Now().UnixNano())
	cmObj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]interface{}{
				"name":      cmName,
				"namespace": ns,
			},
			"data": map[string]interface{}{
				"key1": "value1",
			},
		},
	}

	created, err := client.Create(ctx, "configmaps", ns, cmObj)
	if err != nil {
		t.Fatalf("Failed to create ConfigMap: %v", err)
	}
	defer client.Delete(ctx, "configmaps", ns, cmName)

	t.Run("Update existing resource", func(t *testing.T) {
		// Update the data
		created.Object["data"] = map[string]interface{}{
			"key1": "updated-value1",
			"key2": "value2",
		}

		updated, err := client.Update(ctx, "configmaps", ns, created)
		if err != nil {
			t.Fatalf("Update failed: %v", err)
		}

		data, found, err := unstructured.NestedMap(updated.Object, "data")
		if err != nil || !found {
			t.Fatalf("Failed to get data field: %v", err)
		}

		if data["key1"] != "updated-value1" {
			t.Errorf("Expected data.key1 to be 'updated-value1', got %v", data["key1"])
		}

		if data["key2"] != "value2" {
			t.Errorf("Expected data.key2 to be 'value2', got %v", data["key2"])
		}

		// Verify the update persisted
		got, err := client.Get(ctx, "configmaps", ns, cmName)
		if err != nil {
			t.Fatalf("Failed to get updated ConfigMap: %v", err)
		}

		gotData, found, err := unstructured.NestedMap(got.Object, "data")
		if err != nil || !found {
			t.Fatalf("Failed to get data field: %v", err)
		}

		if gotData["key1"] != "updated-value1" {
			t.Errorf("Update did not persist: expected data.key1 to be 'updated-value1', got %v", gotData["key1"])
		}
	})

	t.Run("Update non-existent resource", func(t *testing.T) {
		nonExistent := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      "non-existent",
					"namespace": ns,
				},
			},
		}

		_, err := client.Update(ctx, "configmaps", ns, nonExistent)
		if err == nil {
			t.Error("Expected error when updating non-existent resource")
		}
	})
}

func TestClient_Patch(t *testing.T) {
	setupTestCluster(t)

	client, err := NewClient(testKubeconfigPath)
	if err != nil {
		t.Fatalf("Failed to create K8s client: %v", err)
	}

	// Verify we're connected to a real cluster
	verifyClusterConnectivity(t, client)

	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	// Create a ConfigMap first
	cmName := fmt.Sprintf("test-cm-%d", time.Now().UnixNano())
	cmObj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]interface{}{
				"name":      cmName,
				"namespace": ns,
			},
			"data": map[string]interface{}{
				"key1": "value1",
			},
		},
	}

	_, err = client.Create(ctx, "configmaps", ns, cmObj)
	if err != nil {
		t.Fatalf("Failed to create ConfigMap: %v", err)
	}
	defer client.Delete(ctx, "configmaps", ns, cmName)

	t.Run("Patch with JSON merge patch", func(t *testing.T) {
		// JSON merge patch to add a new key
		patch := []byte(`{"data":{"key2":"value2"}}`)

		patched, err := client.Patch(ctx, "configmaps", ns, cmName, types.MergePatchType, patch)
		if err != nil {
			t.Fatalf("Patch failed: %v", err)
		}

		data, found, err := unstructured.NestedMap(patched.Object, "data")
		if err != nil || !found {
			t.Fatalf("Failed to get data field: %v", err)
		}

		if data["key1"] != "value1" {
			t.Errorf("Expected data.key1 to remain 'value1', got %v", data["key1"])
		}

		if data["key2"] != "value2" {
			t.Errorf("Expected data.key2 to be 'value2', got %v", data["key2"])
		}

		// Verify the patch persisted
		got, err := client.Get(ctx, "configmaps", ns, cmName)
		if err != nil {
			t.Fatalf("Failed to get patched ConfigMap: %v", err)
		}

		gotData, found, err := unstructured.NestedMap(got.Object, "data")
		if err != nil || !found {
			t.Fatalf("Failed to get data field: %v", err)
		}

		if gotData["key2"] != "value2" {
			t.Errorf("Patch did not persist: expected data.key2 to be 'value2', got %v", gotData["key2"])
		}
	})

	t.Run("Patch non-existent resource", func(t *testing.T) {
		patch := []byte(`{"data":{"key":"value"}}`)
		_, err := client.Patch(ctx, "configmaps", ns, "non-existent", types.MergePatchType, patch)
		if err == nil {
			t.Error("Expected error when patching non-existent resource")
		}
	})
}

func TestClient_Delete(t *testing.T) {
	setupTestCluster(t)

	client, err := NewClient(testKubeconfigPath)
	if err != nil {
		t.Fatalf("Failed to create K8s client: %v", err)
	}

	// Verify we're connected to a real cluster
	verifyClusterConnectivity(t, client)

	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	t.Run("Delete existing resource", func(t *testing.T) {
		cmName := fmt.Sprintf("test-cm-%d", time.Now().UnixNano())
		cmObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      cmName,
					"namespace": ns,
				},
				"data": map[string]interface{}{
					"key": "value",
				},
			},
		}

		_, err := client.Create(ctx, "configmaps", ns, cmObj)
		if err != nil {
			t.Fatalf("Failed to create ConfigMap: %v", err)
		}

		// Verify it exists
		_, err = client.Get(ctx, "configmaps", ns, cmName)
		if err != nil {
			t.Fatalf("Failed to get ConfigMap before delete: %v", err)
		}

		// Delete it
		err = client.Delete(ctx, "configmaps", ns, cmName)
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}

		// Verify it's gone
		_, err = client.Get(ctx, "configmaps", ns, cmName)
		if err == nil {
			t.Error("Expected error when getting deleted ConfigMap")
		}
	})

	t.Run("Delete non-existent resource", func(t *testing.T) {
		err := client.Delete(ctx, "configmaps", ns, "non-existent")
		if err == nil {
			t.Error("Expected error when deleting non-existent resource")
		}
	})

	t.Run("Delete with invalid resource type", func(t *testing.T) {
		err := client.Delete(ctx, "invalid-resource", ns, "test")
		if err == nil {
			t.Error("Expected error when using invalid resource type")
		}
	})
}

func TestClient_GetGVR(t *testing.T) {
	setupTestCluster(t)

	client, err := NewClient(testKubeconfigPath)
	if err != nil {
		t.Fatalf("Failed to create K8s client: %v", err)
	}

	// Verify we're connected to a real cluster
	verifyClusterConnectivity(t, client)

	tests := []struct {
		name     string
		resource string
		wantErr  bool
	}{
		{"pods", "pods", false},
		{"services", "services", false},
		{"configmaps", "configmaps", false},
		{"secrets", "secrets", false},
		{"deployments", "deployments", false},
		{"namespaces", "namespaces", false},
		{"invalid", "invalid-resource", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test getGVR indirectly through Get
			ctx := context.Background()
			_, err := client.Get(ctx, tt.resource, "default", "test")
			if tt.wantErr {
				if err == nil {
					t.Error("Expected error for invalid resource type")
				}
			} else {
				// We expect an error because the resource doesn't exist,
				// but not because of invalid resource type
				if err != nil {
					// Check if error is about unsupported resource
					if err.Error() == fmt.Sprintf("unsupported resource: %s", tt.resource) {
						t.Errorf("Unexpected error for valid resource type: %v", err)
					}
					// Other errors (like resource not found) are expected and fine
				}
			}
		})
	}
}

func TestClient_NewClient(t *testing.T) {
	setupTestCluster(t)

	t.Run("Create client with kubeconfig path", func(t *testing.T) {
		client, err := NewClient(testKubeconfigPath)
		if err != nil {
			t.Fatalf("Failed to create K8s client: %v", err)
		}

		if client == nil {
			t.Error("Expected non-nil client")
		}

		// Verify connectivity
		ctx := context.Background()
		_, err = client.List(ctx, "namespaces", "")
		if err != nil {
			t.Fatalf("Failed to verify client connectivity: %v", err)
		}
	})

	t.Run("Create client with empty kubeconfig path", func(t *testing.T) {
		// This should try in-cluster config first, then fallback to kubeconfig
		// Since we're not in a cluster, it should use the default kubeconfig
		// But we can't reliably test this without mocking, so we'll skip
		// or test that it fails gracefully
		client, err := NewClient("")
		if err != nil {
			// This is expected if not in-cluster and no default kubeconfig
			// We'll just verify it doesn't panic
			return
		}

		if client != nil {
			// If it succeeded, verify connectivity
			ctx := context.Background()
			_, err = client.List(ctx, "namespaces", "")
			if err != nil {
				t.Logf("Client created but connectivity failed (expected in some environments): %v", err)
			}
		}
	})
}

func TestClient_Deployment(t *testing.T) {
	setupTestCluster(t)

	client, err := NewClient(testKubeconfigPath)
	if err != nil {
		t.Fatalf("Failed to create K8s client: %v", err)
	}

	// Verify we're connected to a real cluster
	verifyClusterConnectivity(t, client)

	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	t.Run("Create and manage Deployment", func(t *testing.T) {
		deployName := fmt.Sprintf("test-deploy-%d", time.Now().UnixNano())
		deployObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      deployName,
					"namespace": ns,
				},
				"spec": map[string]interface{}{
					"replicas": 1,
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
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
				},
			},
		}

		created, err := client.Create(ctx, "deployments", ns, deployObj)
		if err != nil {
			t.Fatalf("Create Deployment failed: %v", err)
		}

		if created.GetName() != deployName {
			t.Errorf("Expected name %s, got %s", deployName, created.GetName())
		}

		// Get the deployment
		got, err := client.Get(ctx, "deployments", ns, deployName)
		if err != nil {
			t.Fatalf("Failed to get Deployment: %v", err)
		}

		if got.GetName() != deployName {
			t.Errorf("Get returned wrong name: expected %s, got %s", deployName, got.GetName())
		}

		// List deployments
		list, err := client.List(ctx, "deployments", ns)
		if err != nil {
			t.Fatalf("List deployments failed: %v", err)
		}

		found := false
		for _, item := range list.Items {
			if item.GetName() == deployName && item.GetNamespace() == ns {
				found = true
				break
			}
		}

		if !found {
			t.Error("Deployment not found in list")
		}

		// Cleanup
		err = client.Delete(ctx, "deployments", ns, deployName)
		if err != nil {
			t.Fatalf("Failed to delete Deployment: %v", err)
		}
	})
}

func TestClient_Secret(t *testing.T) {
	setupTestCluster(t)

	client, err := NewClient(testKubeconfigPath)
	if err != nil {
		t.Fatalf("Failed to create K8s client: %v", err)
	}

	// Verify we're connected to a real cluster
	verifyClusterConnectivity(t, client)

	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	t.Run("Create and manage Secret", func(t *testing.T) {
		secretName := fmt.Sprintf("test-secret-%d", time.Now().UnixNano())
		secretObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Secret",
				"metadata": map[string]interface{}{
					"name":      secretName,
					"namespace": ns,
				},
				"type": "Opaque",
				"data": map[string]interface{}{
					"username": "dXNlcm5hbWU=", // base64 encoded "username"
					"password": "cGFzc3dvcmQ=", // base64 encoded "password"
				},
			},
		}

		created, err := client.Create(ctx, "secrets", ns, secretObj)
		if err != nil {
			t.Fatalf("Create Secret failed: %v", err)
		}

		if created.GetName() != secretName {
			t.Errorf("Expected name %s, got %s", secretName, created.GetName())
		}

		// Get the secret
		got, err := client.Get(ctx, "secrets", ns, secretName)
		if err != nil {
			t.Fatalf("Failed to get Secret: %v", err)
		}

		if got.GetName() != secretName {
			t.Errorf("Get returned wrong name: expected %s, got %s", secretName, got.GetName())
		}

		// Cleanup
		err = client.Delete(ctx, "secrets", ns, secretName)
		if err != nil {
			t.Fatalf("Failed to delete Secret: %v", err)
		}
	})
}

// waitForPodRunning waits for a pod to be in the Running phase
func waitForPodRunning(t *testing.T, client *Client, ns, podName string, timeout time.Duration) error {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		pod, err := client.Get(ctx, "pods", ns, podName)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		phase, found, err := unstructured.NestedString(pod.Object, "status", "phase")
		if err != nil || !found {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if phase == "Running" {
			// Also check that containers are ready
			containerStatuses, found, _ := unstructured.NestedSlice(pod.Object, "status", "containerStatuses")
			if found && len(containerStatuses) > 0 {
				status := containerStatuses[0].(map[string]interface{})
				ready, _, _ := unstructured.NestedBool(status, "ready")
				if ready {
					return nil
				}
			}
		}

		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for pod %s to be running", podName)
}

func TestClient_Exec(t *testing.T) {
	setupTestCluster(t)

	client, err := NewClient(testKubeconfigPath)
	if err != nil {
		t.Fatalf("Failed to create K8s client: %v", err)
	}

	// Verify we're connected to a real cluster
	verifyClusterConnectivity(t, client)

	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	// Create a pod for exec tests - using busybox with sleep to keep it running
	podName := fmt.Sprintf("test-exec-pod-%d", time.Now().UnixNano())
	podObj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]interface{}{
				"name":      podName,
				"namespace": ns,
			},
			"spec": map[string]interface{}{
				"containers": []map[string]interface{}{
					{
						"name":    "busybox",
						"image":   "busybox:latest",
						"command": []interface{}{"sleep", "3600"},
					},
				},
			},
		},
	}

	_, err = client.Create(ctx, "pods", ns, podObj)
	if err != nil {
		t.Fatalf("Failed to create test pod: %v", err)
	}
	defer client.Delete(ctx, "pods", ns, podName)

	// Wait for the pod to be running
	err = waitForPodRunning(t, client, ns, podName, 2*time.Minute)
	if err != nil {
		t.Fatalf("Pod did not become ready: %v", err)
	}

	t.Run("Exec simple command", func(t *testing.T) {
		result, err := client.Exec(ctx, ns, podName, "busybox", []string{"echo", "hello"})
		if err != nil {
			t.Fatalf("Exec failed: %v", err)
		}

		expected := "hello\n"
		if result.Stdout != expected {
			t.Errorf("Expected stdout %q, got %q", expected, result.Stdout)
		}

		if result.Stderr != "" {
			t.Errorf("Expected empty stderr, got %q", result.Stderr)
		}
	})

	t.Run("Exec command with arguments", func(t *testing.T) {
		result, err := client.Exec(ctx, ns, podName, "busybox", []string{"ls", "-la", "/"})
		if err != nil {
			t.Fatalf("Exec failed: %v", err)
		}

		if result.Stdout == "" {
			t.Error("Expected non-empty stdout for ls command")
		}

		// Check for common root directory entries
		if !strings.Contains(result.Stdout, "bin") {
			t.Error("Expected stdout to contain 'bin' directory")
		}
	})

	t.Run("Exec command with empty container name", func(t *testing.T) {
		// Empty container name should use the first container
		result, err := client.Exec(ctx, ns, podName, "", []string{"echo", "test"})
		if err != nil {
			t.Fatalf("Exec with empty container failed: %v", err)
		}

		expected := "test\n"
		if result.Stdout != expected {
			t.Errorf("Expected stdout %q, got %q", expected, result.Stdout)
		}
	})

	t.Run("Exec command that writes to stderr", func(t *testing.T) {
		result, err := client.Exec(ctx, ns, podName, "busybox", []string{"sh", "-c", "echo error >&2"})
		if err != nil {
			t.Fatalf("Exec failed: %v", err)
		}

		expected := "error\n"
		if result.Stderr != expected {
			t.Errorf("Expected stderr %q, got %q", expected, result.Stderr)
		}
	})

	t.Run("Exec command that fails", func(t *testing.T) {
		result, err := client.Exec(ctx, ns, podName, "busybox", []string{"ls", "/nonexistent-directory"})
		if err == nil {
			t.Error("Expected error for command that fails")
		}

		// Result should still be returned with stderr
		if result == nil {
			t.Fatal("Expected result to be returned even on error")
		}

		if result.Stderr == "" {
			t.Error("Expected stderr to contain error message")
		}
	})

	t.Run("Exec command with non-zero exit code", func(t *testing.T) {
		// Use sh -c "exit 42" to explicitly return non-zero exit code
		result, err := client.Exec(ctx, ns, podName, "busybox", []string{"sh", "-c", "echo 'output before exit' && exit 42"})
		if err == nil {
			t.Error("Expected error for non-zero exit code")
		}

		// Result should still be returned with stdout captured before exit
		if result == nil {
			t.Fatal("Expected result to be returned even on error")
		}

		if !strings.Contains(result.Stdout, "output before exit") {
			t.Errorf("Expected stdout to contain 'output before exit', got %q", result.Stdout)
		}
	})

	t.Run("Exec on non-existent pod", func(t *testing.T) {
		_, err := client.Exec(ctx, ns, "non-existent-pod", "", []string{"echo", "hello"})
		if err == nil {
			t.Error("Expected error for non-existent pod")
		}
	})

	t.Run("Exec on non-existent container", func(t *testing.T) {
		_, err := client.Exec(ctx, ns, podName, "non-existent-container", []string{"echo", "hello"})
		if err == nil {
			t.Error("Expected error for non-existent container")
		}
	})
}

// waitForDeploymentReady waits for a deployment to have all replicas ready
func waitForDeploymentReady(t *testing.T, client *Client, ns, deploymentName string, timeout time.Duration) error {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		deployment, err := client.Get(ctx, "deployments", ns, deploymentName)
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		readyReplicas, _, _ := unstructured.NestedInt64(deployment.Object, "status", "readyReplicas")
		replicas, _, _ := unstructured.NestedInt64(deployment.Object, "spec", "replicas")

		if readyReplicas == replicas && replicas > 0 {
			return nil
		}

		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for deployment %s to be ready", deploymentName)
}

func TestClient_ExecDeployment(t *testing.T) {
	setupTestCluster(t)

	client, err := NewClient(testKubeconfigPath)
	if err != nil {
		t.Fatalf("Failed to create K8s client: %v", err)
	}

	// Verify we're connected to a real cluster
	verifyClusterConnectivity(t, client)

	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	// Create a deployment with 2 replicas for testing
	deployName := fmt.Sprintf("test-exec-deploy-%d", time.Now().UnixNano())
	deployObj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]interface{}{
				"name":      deployName,
				"namespace": ns,
			},
			"spec": map[string]interface{}{
				"replicas": int64(2),
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
						"containers": []map[string]interface{}{
							{
								"name":    "busybox",
								"image":   "busybox:latest",
								"command": []interface{}{"sleep", "3600"},
							},
						},
					},
				},
			},
		},
	}

	_, err = client.Create(ctx, "deployments", ns, deployObj)
	if err != nil {
		t.Fatalf("Failed to create deployment: %v", err)
	}
	defer client.Delete(ctx, "deployments", ns, deployName)

	// Wait for deployment to be ready
	err = waitForDeploymentReady(t, client, ns, deployName, 3*time.Minute)
	if err != nil {
		t.Fatalf("Deployment did not become ready: %v", err)
	}

	t.Run("ExecDeployment simple command", func(t *testing.T) {
		results, err := client.ExecDeployment(ctx, ns, deployName, "busybox", []string{"echo", "hello"})
		if err != nil {
			t.Fatalf("ExecDeployment failed: %v", err)
		}

		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}

		for _, r := range results {
			if r.Error != nil {
				t.Errorf("Pod %s exec failed: %v", r.PodName, r.Error)
				continue
			}

			expected := "hello\n"
			if r.Result.Stdout != expected {
				t.Errorf("Pod %s: expected stdout %q, got %q", r.PodName, expected, r.Result.Stdout)
			}
		}
	})

	t.Run("ExecDeployment with empty container name", func(t *testing.T) {
		results, err := client.ExecDeployment(ctx, ns, deployName, "", []string{"echo", "test"})
		if err != nil {
			t.Fatalf("ExecDeployment failed: %v", err)
		}

		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}

		for _, r := range results {
			if r.Error != nil {
				t.Errorf("Pod %s exec failed: %v", r.PodName, r.Error)
				continue
			}

			expected := "test\n"
			if r.Result.Stdout != expected {
				t.Errorf("Pod %s: expected stdout %q, got %q", r.PodName, expected, r.Result.Stdout)
			}
		}
	})

	t.Run("ExecDeployment returns results for all pods", func(t *testing.T) {
		results, err := client.ExecDeployment(ctx, ns, deployName, "busybox", []string{"hostname"})
		if err != nil {
			t.Fatalf("ExecDeployment failed: %v", err)
		}

		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}

		// Collect all pod names and hostnames
		podNames := make(map[string]bool)
		hostnames := make(map[string]bool)
		for _, r := range results {
			podNames[r.PodName] = true
			if r.Error == nil && r.Result != nil {
				hostnames[strings.TrimSpace(r.Result.Stdout)] = true
			}
		}

		// Each pod should have a unique name
		if len(podNames) != 2 {
			t.Errorf("Expected 2 unique pod names, got %d", len(podNames))
		}

		// Each pod should report a different hostname (which is the pod name)
		if len(hostnames) != 2 {
			t.Errorf("Expected 2 unique hostnames, got %d", len(hostnames))
		}
	})

	t.Run("ExecDeployment on non-existent deployment", func(t *testing.T) {
		_, err := client.ExecDeployment(ctx, ns, "non-existent-deployment", "", []string{"echo", "hello"})
		if err == nil {
			t.Error("Expected error for non-existent deployment")
		}
	})

	t.Run("ExecDeployment on non-existent container", func(t *testing.T) {
		results, err := client.ExecDeployment(ctx, ns, deployName, "non-existent-container", []string{"echo", "hello"})
		if err != nil {
			t.Fatalf("ExecDeployment should not return error at deployment level: %v", err)
		}

		// All pod results should have errors
		for _, r := range results {
			if r.Error == nil {
				t.Errorf("Pod %s: expected error for non-existent container", r.PodName)
			}
		}
	})

	t.Run("ExecDeployment command that fails", func(t *testing.T) {
		results, err := client.ExecDeployment(ctx, ns, deployName, "busybox", []string{"ls", "/nonexistent-directory"})
		if err != nil {
			t.Fatalf("ExecDeployment should not return error at deployment level: %v", err)
		}

		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}

		// All pod results should have errors since the command fails
		for _, r := range results {
			if r.Error == nil {
				t.Errorf("Pod %s: expected error for failing command", r.PodName)
			}

			// Result should still be returned with stderr
			if r.Result == nil {
				t.Errorf("Pod %s: expected result to be returned even on error", r.PodName)
				continue
			}

			if r.Result.Stderr == "" {
				t.Errorf("Pod %s: expected stderr to contain error message", r.PodName)
			}
		}
	})

	t.Run("ExecDeployment command with non-zero exit code", func(t *testing.T) {
		// Use sh -c "exit 1" to explicitly return non-zero exit code
		results, err := client.ExecDeployment(ctx, ns, deployName, "busybox", []string{"sh", "-c", "echo 'before exit' && exit 1"})
		if err != nil {
			t.Fatalf("ExecDeployment should not return error at deployment level: %v", err)
		}

		for _, r := range results {
			if r.Error == nil {
				t.Errorf("Pod %s: expected error for non-zero exit code", r.PodName)
			}

			// Should still capture stdout before the exit
			if r.Result == nil {
				t.Errorf("Pod %s: expected result to be returned even on error", r.PodName)
				continue
			}

			if !strings.Contains(r.Result.Stdout, "before exit") {
				t.Errorf("Pod %s: expected stdout to contain 'before exit', got %q", r.PodName, r.Result.Stdout)
			}
		}
	})
}

func TestClient_Apply(t *testing.T) {
	setupTestCluster(t)

	client, err := NewClient(testKubeconfigPath)
	if err != nil {
		t.Fatalf("Failed to create K8s client: %v", err)
	}

	// Verify we're connected to a real cluster
	verifyClusterConnectivity(t, client)

	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	t.Run("Apply creates new ConfigMap", func(t *testing.T) {
		cmName := fmt.Sprintf("test-apply-cm-%d", time.Now().UnixNano())
		cmObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      cmName,
					"namespace": ns,
				},
				"data": map[string]interface{}{
					"key1": "value1",
				},
			},
		}

		applied, err := client.Apply(ctx, "configmaps", ns, cmObj, "test-manager", false)
		if err != nil {
			t.Fatalf("Apply failed: %v", err)
		}

		if applied.GetName() != cmName {
			t.Errorf("Expected name %s, got %s", cmName, applied.GetName())
		}

		// Verify the ConfigMap was created
		got, err := client.Get(ctx, "configmaps", ns, cmName)
		if err != nil {
			t.Fatalf("Failed to get applied ConfigMap: %v", err)
		}

		data, found, err := unstructured.NestedMap(got.Object, "data")
		if err != nil || !found {
			t.Fatalf("Failed to get data field: %v", err)
		}

		if data["key1"] != "value1" {
			t.Errorf("Expected data.key1 to be 'value1', got %v", data["key1"])
		}

		// Cleanup
		client.Delete(ctx, "configmaps", ns, cmName)
	})

	t.Run("Apply updates existing ConfigMap", func(t *testing.T) {
		cmName := fmt.Sprintf("test-apply-update-%d", time.Now().UnixNano())
		cmObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      cmName,
					"namespace": ns,
				},
				"data": map[string]interface{}{
					"key1": "initial-value",
				},
			},
		}

		// First apply
		_, err := client.Apply(ctx, "configmaps", ns, cmObj, "test-manager", false)
		if err != nil {
			t.Fatalf("Initial apply failed: %v", err)
		}

		// Update and apply again
		cmObj.Object["data"] = map[string]interface{}{
			"key1": "updated-value",
			"key2": "new-value",
		}

		applied, err := client.Apply(ctx, "configmaps", ns, cmObj, "test-manager", false)
		if err != nil {
			t.Fatalf("Update apply failed: %v", err)
		}

		data, found, err := unstructured.NestedMap(applied.Object, "data")
		if err != nil || !found {
			t.Fatalf("Failed to get data field: %v", err)
		}

		if data["key1"] != "updated-value" {
			t.Errorf("Expected data.key1 to be 'updated-value', got %v", data["key1"])
		}

		if data["key2"] != "new-value" {
			t.Errorf("Expected data.key2 to be 'new-value', got %v", data["key2"])
		}

		// Cleanup
		client.Delete(ctx, "configmaps", ns, cmName)
	})

	t.Run("Apply Deployment with server-side apply", func(t *testing.T) {
		deployName := fmt.Sprintf("test-apply-deploy-%d", time.Now().UnixNano())
		deployObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      deployName,
					"namespace": ns,
				},
				"spec": map[string]interface{}{
					"replicas": int64(1),
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "ssa-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "ssa-test",
							},
						},
						"spec": map[string]interface{}{
							"containers": []interface{}{
								map[string]interface{}{
									"name":  "busybox",
									"image": "busybox:latest",
									"command": []interface{}{
										"sleep",
										"3600",
									},
								},
							},
						},
					},
				},
			},
		}

		applied, err := client.Apply(ctx, "deployments", ns, deployObj, "test-manager", false)
		if err != nil {
			t.Fatalf("Apply deployment failed: %v", err)
		}

		if applied.GetName() != deployName {
			t.Errorf("Expected name %s, got %s", deployName, applied.GetName())
		}

		// Verify it was created
		got, err := client.Get(ctx, "deployments", ns, deployName)
		if err != nil {
			t.Fatalf("Failed to get applied Deployment: %v", err)
		}

		replicas, _, _ := unstructured.NestedInt64(got.Object, "spec", "replicas")
		if replicas != 1 {
			t.Errorf("Expected replicas to be 1, got %d", replicas)
		}

		// Cleanup
		client.Delete(ctx, "deployments", ns, deployName)
	})

	t.Run("Apply with force resolves conflicts", func(t *testing.T) {
		cmName := fmt.Sprintf("test-apply-force-%d", time.Now().UnixNano())
		cmObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      cmName,
					"namespace": ns,
				},
				"data": map[string]interface{}{
					"key1": "manager1-value",
				},
			},
		}

		// Apply with manager1
		_, err := client.Apply(ctx, "configmaps", ns, cmObj, "manager1", false)
		if err != nil {
			t.Fatalf("Initial apply with manager1 failed: %v", err)
		}

		// Apply same field with manager2 without force - should conflict
		cmObj.Object["data"] = map[string]interface{}{
			"key1": "manager2-value",
		}

		_, err = client.Apply(ctx, "configmaps", ns, cmObj, "manager2", false)
		if err == nil {
			t.Log("No conflict occurred (may depend on K8s version)")
		}

		// Apply with force should succeed
		applied, err := client.Apply(ctx, "configmaps", ns, cmObj, "manager2", true)
		if err != nil {
			t.Fatalf("Apply with force failed: %v", err)
		}

		data, found, err := unstructured.NestedMap(applied.Object, "data")
		if err != nil || !found {
			t.Fatalf("Failed to get data field: %v", err)
		}

		if data["key1"] != "manager2-value" {
			t.Errorf("Expected data.key1 to be 'manager2-value', got %v", data["key1"])
		}

		// Cleanup
		client.Delete(ctx, "configmaps", ns, cmName)
	})

	t.Run("Apply without apiVersion fails", func(t *testing.T) {
		cmObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"kind": "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      "test-no-apiversion",
					"namespace": ns,
				},
			},
		}

		_, err := client.Apply(ctx, "configmaps", ns, cmObj, "test-manager", false)
		if err == nil {
			t.Error("Expected error when apiVersion is not set")
		}

		if !strings.Contains(err.Error(), "apiVersion must be set") {
			t.Errorf("Expected error about apiVersion, got: %v", err)
		}
	})

	t.Run("Apply without kind fails", func(t *testing.T) {
		cmObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"metadata": map[string]interface{}{
					"name":      "test-no-kind",
					"namespace": ns,
				},
			},
		}

		_, err := client.Apply(ctx, "configmaps", ns, cmObj, "test-manager", false)
		if err == nil {
			t.Error("Expected error when kind is not set")
		}

		if !strings.Contains(err.Error(), "kind must be set") {
			t.Errorf("Expected error about kind, got: %v", err)
		}
	})

	t.Run("Apply without name fails", func(t *testing.T) {
		cmObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"namespace": ns,
				},
			},
		}

		_, err := client.Apply(ctx, "configmaps", ns, cmObj, "test-manager", false)
		if err == nil {
			t.Error("Expected error when name is not set")
		}

		if !strings.Contains(err.Error(), "name must be set") {
			t.Errorf("Expected error about name, got: %v", err)
		}
	})

	t.Run("Apply with invalid resource type fails", func(t *testing.T) {
		cmObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      "test-invalid-resource",
					"namespace": ns,
				},
			},
		}

		_, err := client.Apply(ctx, "invalid-resource", ns, cmObj, "test-manager", false)
		if err == nil {
			t.Error("Expected error for invalid resource type")
		}
	})

	t.Run("Apply Secret", func(t *testing.T) {
		secretName := fmt.Sprintf("test-apply-secret-%d", time.Now().UnixNano())
		secretObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Secret",
				"metadata": map[string]interface{}{
					"name":      secretName,
					"namespace": ns,
				},
				"type": "Opaque",
				"data": map[string]interface{}{
					"username": "dXNlcm5hbWU=", // base64 "username"
					"password": "cGFzc3dvcmQ=", // base64 "password"
				},
			},
		}

		applied, err := client.Apply(ctx, "secrets", ns, secretObj, "test-manager", false)
		if err != nil {
			t.Fatalf("Apply Secret failed: %v", err)
		}

		if applied.GetName() != secretName {
			t.Errorf("Expected name %s, got %s", secretName, applied.GetName())
		}

		// Cleanup
		client.Delete(ctx, "secrets", ns, secretName)
	})

	t.Run("Apply Service", func(t *testing.T) {
		svcName := fmt.Sprintf("test-apply-svc-%d", time.Now().UnixNano())
		svcObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Service",
				"metadata": map[string]interface{}{
					"name":      svcName,
					"namespace": ns,
				},
				"spec": map[string]interface{}{
					"ports": []interface{}{
						map[string]interface{}{
							"port":       int64(80),
							"targetPort": int64(8080),
						},
					},
					"selector": map[string]interface{}{
						"app": "test",
					},
				},
			},
		}

		applied, err := client.Apply(ctx, "services", ns, svcObj, "test-manager", false)
		if err != nil {
			t.Fatalf("Apply Service failed: %v", err)
		}

		if applied.GetName() != svcName {
			t.Errorf("Expected name %s, got %s", svcName, applied.GetName())
		}

		// Cleanup
		client.Delete(ctx, "services", ns, svcName)
	})

	t.Run("Apply idempotency", func(t *testing.T) {
		cmName := fmt.Sprintf("test-apply-idempotent-%d", time.Now().UnixNano())
		cmObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      cmName,
					"namespace": ns,
				},
				"data": map[string]interface{}{
					"key1": "value1",
				},
			},
		}

		// Apply multiple times with the same content
		for i := 0; i < 3; i++ {
			applied, err := client.Apply(ctx, "configmaps", ns, cmObj, "test-manager", false)
			if err != nil {
				t.Fatalf("Apply iteration %d failed: %v", i, err)
			}

			if applied.GetName() != cmName {
				t.Errorf("Iteration %d: Expected name %s, got %s", i, cmName, applied.GetName())
			}
		}

		// Verify final state
		got, err := client.Get(ctx, "configmaps", ns, cmName)
		if err != nil {
			t.Fatalf("Failed to get ConfigMap after multiple applies: %v", err)
		}

		data, found, err := unstructured.NestedMap(got.Object, "data")
		if err != nil || !found {
			t.Fatalf("Failed to get data field: %v", err)
		}

		if data["key1"] != "value1" {
			t.Errorf("Expected data.key1 to be 'value1', got %v", data["key1"])
		}

		// Cleanup
		client.Delete(ctx, "configmaps", ns, cmName)
	})
}

func TestClient_WaitForDeploymentRollout(t *testing.T) {
	setupTestCluster(t)

	client, err := NewClient(testKubeconfigPath)
	if err != nil {
		t.Fatalf("Failed to create K8s client: %v", err)
	}

	// Verify we're connected to a real cluster
	verifyClusterConnectivity(t, client)

	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	t.Run("WaitForDeploymentRollout on ready deployment", func(t *testing.T) {
		deployName := fmt.Sprintf("test-wait-ready-%d", time.Now().UnixNano())
		deployObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      deployName,
					"namespace": ns,
				},
				"spec": map[string]interface{}{
					"replicas": int64(1),
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "wait-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "wait-test",
							},
						},
						"spec": map[string]interface{}{
							"containers": []map[string]interface{}{
								{
									"name":    "busybox",
									"image":   "busybox:latest",
									"command": []interface{}{"sleep", "3600"},
								},
							},
						},
					},
				},
			},
		}

		_, err := client.Create(ctx, "deployments", ns, deployObj)
		if err != nil {
			t.Fatalf("Failed to create deployment: %v", err)
		}
		defer client.Delete(ctx, "deployments", ns, deployName)

		// Wait for deployment rollout
		err = client.WaitForDeploymentRollout(ctx, ns, deployName, 3*time.Minute)
		if err != nil {
			t.Fatalf("WaitForDeploymentRollout failed: %v", err)
		}

		// Verify deployment is actually ready
		deployment, err := client.Get(ctx, "deployments", ns, deployName)
		if err != nil {
			t.Fatalf("Failed to get deployment: %v", err)
		}

		replicas, _, _ := unstructured.NestedInt64(deployment.Object, "spec", "replicas")
		readyReplicas, _, _ := unstructured.NestedInt64(deployment.Object, "status", "readyReplicas")
		availableReplicas, _, _ := unstructured.NestedInt64(deployment.Object, "status", "availableReplicas")

		if readyReplicas != replicas {
			t.Errorf("Expected readyReplicas=%d, got %d", replicas, readyReplicas)
		}
		if availableReplicas != replicas {
			t.Errorf("Expected availableReplicas=%d, got %d", replicas, availableReplicas)
		}
	})

	t.Run("WaitForDeploymentRollout with multiple replicas", func(t *testing.T) {
		deployName := fmt.Sprintf("test-wait-multi-%d", time.Now().UnixNano())
		deployObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      deployName,
					"namespace": ns,
				},
				"spec": map[string]interface{}{
					"replicas": int64(3),
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "wait-multi-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "wait-multi-test",
							},
						},
						"spec": map[string]interface{}{
							"containers": []map[string]interface{}{
								{
									"name":    "busybox",
									"image":   "busybox:latest",
									"command": []interface{}{"sleep", "3600"},
								},
							},
						},
					},
				},
			},
		}

		_, err := client.Create(ctx, "deployments", ns, deployObj)
		if err != nil {
			t.Fatalf("Failed to create deployment: %v", err)
		}
		defer client.Delete(ctx, "deployments", ns, deployName)

		// Wait for deployment rollout
		err = client.WaitForDeploymentRollout(ctx, ns, deployName, 3*time.Minute)
		if err != nil {
			t.Fatalf("WaitForDeploymentRollout failed: %v", err)
		}

		// Verify all replicas are ready
		deployment, err := client.Get(ctx, "deployments", ns, deployName)
		if err != nil {
			t.Fatalf("Failed to get deployment: %v", err)
		}

		readyReplicas, _, _ := unstructured.NestedInt64(deployment.Object, "status", "readyReplicas")
		if readyReplicas != 3 {
			t.Errorf("Expected 3 readyReplicas, got %d", readyReplicas)
		}
	})

	t.Run("WaitForDeploymentRollout after restart", func(t *testing.T) {
		deployName := fmt.Sprintf("test-wait-restart-%d", time.Now().UnixNano())
		deployObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      deployName,
					"namespace": ns,
				},
				"spec": map[string]interface{}{
					"replicas": int64(1),
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "wait-restart-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "wait-restart-test",
							},
						},
						"spec": map[string]interface{}{
							"containers": []map[string]interface{}{
								{
									"name":    "busybox",
									"image":   "busybox:latest",
									"command": []interface{}{"sleep", "3600"},
								},
							},
						},
					},
				},
			},
		}

		_, err := client.Create(ctx, "deployments", ns, deployObj)
		if err != nil {
			t.Fatalf("Failed to create deployment: %v", err)
		}
		defer client.Delete(ctx, "deployments", ns, deployName)

		// First wait for initial deployment
		err = client.WaitForDeploymentRollout(ctx, ns, deployName, 3*time.Minute)
		if err != nil {
			t.Fatalf("Initial WaitForDeploymentRollout failed: %v", err)
		}

		// Trigger a restart by patching the annotation (like kubectl rollout restart)
		restartPatch := []byte(fmt.Sprintf(`{"spec":{"template":{"metadata":{"annotations":{"kubectl.kubernetes.io/restartedAt":"%s"}}}}}`, time.Now().Format(time.RFC3339)))
		_, err = client.Patch(ctx, "deployments", ns, deployName, types.MergePatchType, restartPatch)
		if err != nil {
			t.Fatalf("Failed to patch deployment for restart: %v", err)
		}

		// Wait for the restart rollout to complete
		err = client.WaitForDeploymentRollout(ctx, ns, deployName, 3*time.Minute)
		if err != nil {
			t.Fatalf("WaitForDeploymentRollout after restart failed: %v", err)
		}

		// Verify deployment is ready
		deployment, err := client.Get(ctx, "deployments", ns, deployName)
		if err != nil {
			t.Fatalf("Failed to get deployment: %v", err)
		}

		readyReplicas, _, _ := unstructured.NestedInt64(deployment.Object, "status", "readyReplicas")
		if readyReplicas != 1 {
			t.Errorf("Expected 1 readyReplica after restart, got %d", readyReplicas)
		}

		// Verify the annotation was applied
		annotations, _, _ := unstructured.NestedStringMap(deployment.Object, "spec", "template", "metadata", "annotations")
		if annotations["kubectl.kubernetes.io/restartedAt"] == "" {
			t.Error("Expected restartedAt annotation to be set")
		}
	})

	t.Run("WaitForDeploymentRollout timeout", func(t *testing.T) {
		deployName := fmt.Sprintf("test-wait-timeout-%d", time.Now().UnixNano())
		deployObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      deployName,
					"namespace": ns,
				},
				"spec": map[string]interface{}{
					"replicas": int64(1),
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "wait-timeout-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "wait-timeout-test",
							},
						},
						"spec": map[string]interface{}{
							"containers": []map[string]interface{}{
								{
									"name":    "busybox",
									"image":   "invalid-image-that-does-not-exist:v999",
									"command": []interface{}{"sleep", "3600"},
								},
							},
						},
					},
				},
			},
		}

		_, err := client.Create(ctx, "deployments", ns, deployObj)
		if err != nil {
			t.Fatalf("Failed to create deployment: %v", err)
		}
		defer client.Delete(ctx, "deployments", ns, deployName)

		// This should timeout because the image doesn't exist
		err = client.WaitForDeploymentRollout(ctx, ns, deployName, 5*time.Second)
		if err == nil {
			t.Error("Expected timeout error")
		}

		if !strings.Contains(err.Error(), "timed out") {
			t.Errorf("Expected timeout error, got: %v", err)
		}
	})

	t.Run("WaitForDeploymentRollout non-existent deployment", func(t *testing.T) {
		err := client.WaitForDeploymentRollout(ctx, ns, "non-existent-deployment", 5*time.Second)
		if err == nil {
			t.Error("Expected error for non-existent deployment")
		}
	})

	t.Run("WaitForDeploymentRollout after scale up", func(t *testing.T) {
		deployName := fmt.Sprintf("test-wait-scale-%d", time.Now().UnixNano())
		deployObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      deployName,
					"namespace": ns,
				},
				"spec": map[string]interface{}{
					"replicas": int64(1),
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "wait-scale-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "wait-scale-test",
							},
						},
						"spec": map[string]interface{}{
							"containers": []map[string]interface{}{
								{
									"name":    "busybox",
									"image":   "busybox:latest",
									"command": []interface{}{"sleep", "3600"},
								},
							},
						},
					},
				},
			},
		}

		_, err := client.Create(ctx, "deployments", ns, deployObj)
		if err != nil {
			t.Fatalf("Failed to create deployment: %v", err)
		}
		defer client.Delete(ctx, "deployments", ns, deployName)

		// Wait for initial deployment
		err = client.WaitForDeploymentRollout(ctx, ns, deployName, 3*time.Minute)
		if err != nil {
			t.Fatalf("Initial WaitForDeploymentRollout failed: %v", err)
		}

		// Scale up to 2 replicas
		scalePatch := []byte(`{"spec":{"replicas":2}}`)
		_, err = client.Patch(ctx, "deployments", ns, deployName, types.MergePatchType, scalePatch)
		if err != nil {
			t.Fatalf("Failed to scale deployment: %v", err)
		}

		// Wait for scale up to complete
		err = client.WaitForDeploymentRollout(ctx, ns, deployName, 3*time.Minute)
		if err != nil {
			t.Fatalf("WaitForDeploymentRollout after scale up failed: %v", err)
		}

		// Verify we have 2 replicas ready
		deployment, err := client.Get(ctx, "deployments", ns, deployName)
		if err != nil {
			t.Fatalf("Failed to get deployment: %v", err)
		}

		readyReplicas, _, _ := unstructured.NestedInt64(deployment.Object, "status", "readyReplicas")
		if readyReplicas != 2 {
			t.Errorf("Expected 2 readyReplicas after scale up, got %d", readyReplicas)
		}
	})
}
