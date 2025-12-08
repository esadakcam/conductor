package k8s

import (
	"context"
	"fmt"
	"os"
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
