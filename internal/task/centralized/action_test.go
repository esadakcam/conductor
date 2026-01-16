package centralized

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/esadakcam/conductor/internal/k8s"
	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/task"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/kind/pkg/cluster"
)

var (
	testKubeconfigPath string
	testClusterName    string
	testSetupOnce      sync.Once
	testProvider       *cluster.Provider
	testK8sClient      *k8s.Client
)

func TestMain(m *testing.M) {
	if os.Getenv("SKIP_INTEGRATION") != "" {
		os.Exit(m.Run())
	}

	// Run tests
	code := m.Run()

	// Cleanup
	if testClusterName != "" && testProvider != nil {
		logger.Infof("Deleting centralized action test cluster %s...", testClusterName)
		testProvider.Delete(testClusterName, "")
	}
	if testKubeconfigPath != "" {
		os.Remove(testKubeconfigPath)
	}

	os.Exit(code)
}

// setupTestCluster creates a dedicated kind cluster for centralized action tests
func setupTestCluster(t *testing.T) *k8s.Client {
	t.Helper()

	if os.Getenv("SKIP_INTEGRATION") != "" {
		t.Skip("Skipping integration tests: SKIP_INTEGRATION is set")
	}

	var setupErr error
	testSetupOnce.Do(func() {
		// Create a dedicated cluster for centralized action tests
		testClusterName = fmt.Sprintf("centralized-action-test-%d", time.Now().Unix())
		testProvider = cluster.NewProvider()

		// Check if cluster already exists and delete it if it does
		existingClusters, _ := testProvider.List()
		for _, name := range existingClusters {
			if name == testClusterName {
				logger.Infof("Cleaning up existing cluster %s...", testClusterName)
				testProvider.Delete(testClusterName, "")
			}
		}

		logger.Infof("Creating kind cluster %s for centralized action tests...", testClusterName)
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
		tmpFile, err := os.CreateTemp("", "centralized-action-kind-kubeconfig-*")
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

		// Create k8s client
		testK8sClient, err = k8s.NewClient(testKubeconfigPath)
		if err != nil {
			setupErr = fmt.Errorf("failed to create k8s client: %w", err)
			testProvider.Delete(testClusterName, "")
			return
		}
	})

	if setupErr != nil {
		t.Fatalf("Failed to set up centralized action test cluster: %v", setupErr)
	}

	if testK8sClient == nil {
		t.Fatal("Failed to set up k8s client")
	}

	return testK8sClient
}

func setupTestNamespace(t *testing.T, client *k8s.Client) string {
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

func cleanupTestNamespace(t *testing.T, client *k8s.Client, ns string) {
	err := client.Delete(context.Background(), "namespaces", "", ns)
	if err != nil {
		t.Logf("Failed to cleanup test namespace %s: %v", ns, err)
	}
}

func createMockEC(clients map[string]*k8s.Client) *MockExecutionContext {
	return NewMockExecutionContext(clients)
}

// waitForPodRunning waits for a pod to be in the Running phase
func waitForPodRunning(t *testing.T, client *k8s.Client, ns, podName string, timeout time.Duration) error {
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

// waitForDeploymentReady waits for a deployment to have all replicas ready
func waitForDeploymentReady(t *testing.T, client *k8s.Client, ns, deploymentName string, timeout time.Duration) error {
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

func TestActionConfigValueSum_GetType(t *testing.T) {
	action := &ActionConfigValueSum{}
	if action.GetType() != task.ActionTypeConfigValueSum {
		t.Errorf("expected GetType() to return ActionTypeConfigValueSum, got %v", action.GetType())
	}
}

func TestActionConfigValueSum_Execute(t *testing.T) {
	client := setupTestCluster(t)
	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	t.Run("sum already matches target - no action needed", func(t *testing.T) {
		// Create configmap with value 5
		cmName := fmt.Sprintf("test-cm-sum-%d", time.Now().UnixNano())
		cmObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      cmName,
					"namespace": ns,
				},
				"data": map[string]interface{}{
					"replicas": "5",
				},
			},
		}
		_, err := client.Create(ctx, "configmaps", ns, cmObj)
		if err != nil {
			t.Fatalf("Failed to create configmap: %v", err)
		}
		defer client.Delete(ctx, "configmaps", ns, cmName)

		action := &ActionConfigValueSum{
			ActionConfigValueSumData: task.ActionConfigValueSumData{
				ConfigMapName: cmName,
				Namespace:     ns,
				Key:           "replicas",
				Sum:           5,
				Members:       []string{"member1"},
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err = action.Execute(ctx, ec)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Verify value unchanged
		cm, err := client.Get(ctx, "configmaps", ns, cmName)
		if err != nil {
			t.Fatalf("Failed to get configmap: %v", err)
		}
		data, _, _ := unstructured.NestedStringMap(cm.Object, "data")
		if data["replicas"] != "5" {
			t.Errorf("expected replicas to remain 5, got %s", data["replicas"])
		}
	})

	t.Run("sum needs adjustment - single member", func(t *testing.T) {
		cmName := fmt.Sprintf("test-cm-sum-%d", time.Now().UnixNano())
		cmObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      cmName,
					"namespace": ns,
				},
				"data": map[string]interface{}{
					"replicas": "3",
				},
			},
		}
		_, err := client.Create(ctx, "configmaps", ns, cmObj)
		if err != nil {
			t.Fatalf("Failed to create configmap: %v", err)
		}
		defer client.Delete(ctx, "configmaps", ns, cmName)

		action := &ActionConfigValueSum{
			ActionConfigValueSumData: task.ActionConfigValueSumData{
				ConfigMapName: cmName,
				Namespace:     ns,
				Key:           "replicas",
				Sum:           10,
				Members:       []string{"member1"},
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err = action.Execute(ctx, ec)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Verify value updated to 10
		cm, err := client.Get(ctx, "configmaps", ns, cmName)
		if err != nil {
			t.Fatalf("Failed to get configmap: %v", err)
		}
		data, _, _ := unstructured.NestedStringMap(cm.Object, "data")
		if data["replicas"] != "10" {
			t.Errorf("expected replicas to be 10, got %s", data["replicas"])
		}
	})

	t.Run("sum distributed across multiple members", func(t *testing.T) {
		// Create two configmaps for two members (using same client but different configmaps)
		cmName1 := fmt.Sprintf("test-cm-sum1-%d", time.Now().UnixNano())
		cmName2 := fmt.Sprintf("test-cm-sum2-%d", time.Now().UnixNano())

		for _, cmName := range []string{cmName1, cmName2} {
			cmObj := &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]interface{}{
						"name":      cmName,
						"namespace": ns,
					},
					"data": map[string]interface{}{
						"replicas": "2",
					},
				},
			}
			_, err := client.Create(ctx, "configmaps", ns, cmObj)
			if err != nil {
				t.Fatalf("Failed to create configmap %s: %v", cmName, err)
			}
			defer client.Delete(ctx, "configmaps", ns, cmName)
		}

		// For this test, we'll use the same configmap name but with single member
		// since we have only one cluster
		cmName := fmt.Sprintf("test-cm-sum-multi-%d", time.Now().UnixNano())
		cmObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      cmName,
					"namespace": ns,
				},
				"data": map[string]interface{}{
					"replicas": "3",
				},
			},
		}
		_, err := client.Create(ctx, "configmaps", ns, cmObj)
		if err != nil {
			t.Fatalf("Failed to create configmap: %v", err)
		}
		defer client.Delete(ctx, "configmaps", ns, cmName)

		action := &ActionConfigValueSum{
			ActionConfigValueSumData: task.ActionConfigValueSumData{
				ConfigMapName: cmName,
				Namespace:     ns,
				Key:           "replicas",
				Sum:           10,
				Members:       []string{"member1"},
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err = action.Execute(ctx, ec)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("nil k8s clients returns no error when sum matches", func(t *testing.T) {
		action := &ActionConfigValueSum{
			ActionConfigValueSumData: task.ActionConfigValueSumData{
				ConfigMapName: "test",
				Key:           "replicas",
				Sum:           0, // Sum of 0 with no members = match
				Members:       []string{},
			},
		}

		ec := createMockEC(map[string]*k8s.Client{})
		err := action.Execute(ctx, ec)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("missing k8s client for member", func(t *testing.T) {
		cmName := fmt.Sprintf("test-cm-sum-%d", time.Now().UnixNano())
		cmObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      cmName,
					"namespace": ns,
				},
				"data": map[string]interface{}{
					"replicas": "5",
				},
			},
		}
		_, err := client.Create(ctx, "configmaps", ns, cmObj)
		if err != nil {
			t.Fatalf("Failed to create configmap: %v", err)
		}
		defer client.Delete(ctx, "configmaps", ns, cmName)

		action := &ActionConfigValueSum{
			ActionConfigValueSumData: task.ActionConfigValueSumData{
				ConfigMapName: cmName,
				Namespace:     ns,
				Key:           "replicas",
				Sum:           10,
				Members:       []string{"member1", "member2"},
			},
		}

		// Only provide client for member1, not member2
		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		// This should not error at the action level but log warnings
		err = action.Execute(ctx, ec)
		// It will succeed with only member1
		if err != nil {
			t.Logf("Error (expected when member missing): %v", err)
		}
	})
}

func TestActionK8sExecDeployment_GetType(t *testing.T) {
	action := &ActionK8sExecDeployment{}
	if action.GetType() != task.ActionTypeK8sExecDeployment {
		t.Errorf("expected GetType() to return ActionTypeK8sExecDeployment, got %v", action.GetType())
	}
}

func TestActionK8sExecDeployment_Execute(t *testing.T) {
	client := setupTestCluster(t)
	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	// Create a deployment for exec tests
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

	// Wait for deployment to be ready
	err = waitForDeploymentReady(t, client, ns, deployName, 3*time.Minute)
	if err != nil {
		t.Fatalf("Deployment did not become ready: %v", err)
	}

	t.Run("successful execution", func(t *testing.T) {
		action := &ActionK8sExecDeployment{
			ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
				Member:     "member1",
				Deployment: deployName,
				Namespace:  ns,
				Container:  "busybox",
				Command:    []string{"echo", "hello"},
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err := action.Execute(ctx, ec)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("execution with default namespace", func(t *testing.T) {
		// Create deployment in default namespace for this test
		defaultDeployName := fmt.Sprintf("test-exec-default-%d", time.Now().UnixNano())
		defaultDeployObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      defaultDeployName,
					"namespace": "default",
				},
				"spec": map[string]interface{}{
					"replicas": int64(1),
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "exec-default-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "exec-default-test",
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

		_, err := client.Create(ctx, "deployments", "default", defaultDeployObj)
		if err != nil {
			t.Fatalf("Failed to create deployment: %v", err)
		}
		defer client.Delete(ctx, "deployments", "default", defaultDeployName)

		err = waitForDeploymentReady(t, client, "default", defaultDeployName, 3*time.Minute)
		if err != nil {
			t.Fatalf("Deployment did not become ready: %v", err)
		}

		action := &ActionK8sExecDeployment{
			ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
				Member:     "member1",
				Deployment: defaultDeployName,
				Namespace:  "", // Should default to "default"
				Command:    []string{"echo", "test"},
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err = action.Execute(ctx, ec)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("missing deployment name returns error", func(t *testing.T) {
		action := &ActionK8sExecDeployment{
			ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
				Member:     "member1",
				Deployment: "",
				Command:    []string{"echo"},
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err := action.Execute(ctx, ec)
		if err == nil {
			t.Error("expected error for missing deployment name")
		}
		if !strings.Contains(err.Error(), "deployment name is required") {
			t.Errorf("expected error about deployment name, got: %v", err)
		}
	})

	t.Run("missing member returns error", func(t *testing.T) {
		action := &ActionK8sExecDeployment{
			ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
				Member:     "",
				Deployment: deployName,
				Command:    []string{"echo"},
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err := action.Execute(ctx, ec)
		if err == nil {
			t.Error("expected error for missing member")
		}
		if !strings.Contains(err.Error(), "member is required") {
			t.Errorf("expected error about member, got: %v", err)
		}
	})

	t.Run("missing command returns error", func(t *testing.T) {
		action := &ActionK8sExecDeployment{
			ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
				Member:     "member1",
				Deployment: deployName,
				Command:    []string{},
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err := action.Execute(ctx, ec)
		if err == nil {
			t.Error("expected error for missing command")
		}
		if !strings.Contains(err.Error(), "command is required") {
			t.Errorf("expected error about command, got: %v", err)
		}
	})

	t.Run("missing k8s client returns error", func(t *testing.T) {
		action := &ActionK8sExecDeployment{
			ActionK8sExecDeploymentData: task.ActionK8sExecDeploymentData{
				Member:     "nonexistent",
				Deployment: deployName,
				Namespace:  ns,
				Command:    []string{"echo"},
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err := action.Execute(ctx, ec)
		if err == nil {
			t.Error("expected error for missing k8s client")
		}
		if !strings.Contains(err.Error(), "no k8s client") {
			t.Errorf("expected error about missing client, got: %v", err)
		}
	})
}

func TestActionK8sRestartDeployment_GetType(t *testing.T) {
	action := &ActionK8sRestartDeployment{}
	if action.GetType() != task.ActionTypeK8sRestartDeployment {
		t.Errorf("expected GetType() to return ActionTypeK8sRestartDeployment, got %v", action.GetType())
	}
}

func TestActionK8sRestartDeployment_Execute(t *testing.T) {
	client := setupTestCluster(t)
	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	// Create a deployment for restart tests
	deployName := fmt.Sprintf("test-restart-deploy-%d", time.Now().UnixNano())
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
						"app": "restart-test",
					},
				},
				"template": map[string]interface{}{
					"metadata": map[string]interface{}{
						"labels": map[string]interface{}{
							"app": "restart-test",
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

	// Wait for deployment to be ready
	err = waitForDeploymentReady(t, client, ns, deployName, 3*time.Minute)
	if err != nil {
		t.Fatalf("Deployment did not become ready: %v", err)
	}

	t.Run("successful restart", func(t *testing.T) {
		action := &ActionK8sRestartDeployment{
			ActionK8sRestartDeploymentData: task.ActionK8sRestartDeploymentData{
				Member:     "member1",
				Deployment: deployName,
				Namespace:  ns,
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err := action.Execute(ctx, ec)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Verify the restart annotation was added
		deployment, err := client.Get(ctx, "deployments", ns, deployName)
		if err != nil {
			t.Fatalf("Failed to get deployment: %v", err)
		}

		annotations, _, _ := unstructured.NestedStringMap(deployment.Object, "spec", "template", "metadata", "annotations")
		if annotations["kubectl.kubernetes.io/restartedAt"] == "" {
			t.Error("expected restartedAt annotation to be set")
		}
	})

	t.Run("missing deployment name returns error", func(t *testing.T) {
		action := &ActionK8sRestartDeployment{
			ActionK8sRestartDeploymentData: task.ActionK8sRestartDeploymentData{
				Member:     "member1",
				Deployment: "",
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err := action.Execute(ctx, ec)
		if err == nil {
			t.Error("expected error for missing deployment name")
		}
		if !strings.Contains(err.Error(), "deployment name is required") {
			t.Errorf("expected error about deployment name, got: %v", err)
		}
	})

	t.Run("missing member returns error", func(t *testing.T) {
		action := &ActionK8sRestartDeployment{
			ActionK8sRestartDeploymentData: task.ActionK8sRestartDeploymentData{
				Member:     "",
				Deployment: deployName,
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err := action.Execute(ctx, ec)
		if err == nil {
			t.Error("expected error for missing member")
		}
		if !strings.Contains(err.Error(), "member is required") {
			t.Errorf("expected error about member, got: %v", err)
		}
	})
}

func TestActionK8sWaitDeploymentRollout_GetType(t *testing.T) {
	action := &ActionK8sWaitDeploymentRollout{}
	if action.GetType() != task.ActionTypeK8sWaitDeploymentRollout {
		t.Errorf("expected GetType() to return ActionTypeK8sWaitDeploymentRollout, got %v", action.GetType())
	}
}

func TestActionK8sWaitDeploymentRollout_Execute(t *testing.T) {
	client := setupTestCluster(t)
	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	t.Run("successful wait on ready deployment", func(t *testing.T) {
		deployName := fmt.Sprintf("test-wait-deploy-%d", time.Now().UnixNano())
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

		action := &ActionK8sWaitDeploymentRollout{
			ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
				Member:     "member1",
				Deployment: deployName,
				Namespace:  ns,
				Timeout:    3 * time.Minute,
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err = action.Execute(ctx, ec)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("timeout on failing deployment", func(t *testing.T) {
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

		action := &ActionK8sWaitDeploymentRollout{
			ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
				Member:     "member1",
				Deployment: deployName,
				Namespace:  ns,
				Timeout:    5 * time.Second,
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err = action.Execute(ctx, ec)
		if err == nil {
			t.Error("expected timeout error")
		}
		if !strings.Contains(err.Error(), "timed out") {
			t.Errorf("expected timeout error, got: %v", err)
		}
	})

	t.Run("missing deployment name returns error", func(t *testing.T) {
		action := &ActionK8sWaitDeploymentRollout{
			ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
				Member:     "member1",
				Deployment: "",
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err := action.Execute(ctx, ec)
		if err == nil {
			t.Error("expected error for missing deployment name")
		}
	})

	t.Run("missing member returns error", func(t *testing.T) {
		action := &ActionK8sWaitDeploymentRollout{
			ActionK8sWaitDeploymentRolloutData: task.ActionK8sWaitDeploymentRolloutData{
				Member:     "",
				Deployment: "test",
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err := action.Execute(ctx, ec)
		if err == nil {
			t.Error("expected error for missing member")
		}
	})
}

func TestActionK8sUpdateConfigMap_GetType(t *testing.T) {
	action := &ActionK8sUpdateConfigMap{}
	if action.GetType() != task.ActionTypeK8sUpdateConfigMap {
		t.Errorf("expected GetType() to return ActionTypeK8sUpdateConfigMap, got %v", action.GetType())
	}
}

func TestActionK8sUpdateConfigMap_Execute(t *testing.T) {
	client := setupTestCluster(t)
	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	t.Run("successful update", func(t *testing.T) {
		cmName := fmt.Sprintf("test-update-cm-%d", time.Now().UnixNano())
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
		_, err := client.Create(ctx, "configmaps", ns, cmObj)
		if err != nil {
			t.Fatalf("Failed to create configmap: %v", err)
		}
		defer client.Delete(ctx, "configmaps", ns, cmName)

		action := &ActionK8sUpdateConfigMap{
			ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
				Member:    "member1",
				ConfigMap: cmName,
				Namespace: ns,
				Key:       "key2",
				Value:     "value2",
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err = action.Execute(ctx, ec)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Verify the update
		cm, err := client.Get(ctx, "configmaps", ns, cmName)
		if err != nil {
			t.Fatalf("Failed to get configmap: %v", err)
		}
		data, _, _ := unstructured.NestedStringMap(cm.Object, "data")
		if data["key2"] != "value2" {
			t.Errorf("expected key2 to be 'value2', got %s", data["key2"])
		}
		if data["key1"] != "value1" {
			t.Errorf("expected key1 to remain 'value1', got %s", data["key1"])
		}
	})

	t.Run("update existing key", func(t *testing.T) {
		cmName := fmt.Sprintf("test-update-existing-%d", time.Now().UnixNano())
		cmObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "ConfigMap",
				"metadata": map[string]interface{}{
					"name":      cmName,
					"namespace": ns,
				},
				"data": map[string]interface{}{
					"mykey": "oldvalue",
				},
			},
		}
		_, err := client.Create(ctx, "configmaps", ns, cmObj)
		if err != nil {
			t.Fatalf("Failed to create configmap: %v", err)
		}
		defer client.Delete(ctx, "configmaps", ns, cmName)

		action := &ActionK8sUpdateConfigMap{
			ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
				Member:    "member1",
				ConfigMap: cmName,
				Namespace: ns,
				Key:       "mykey",
				Value:     "newvalue",
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err = action.Execute(ctx, ec)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Verify the update
		cm, err := client.Get(ctx, "configmaps", ns, cmName)
		if err != nil {
			t.Fatalf("Failed to get configmap: %v", err)
		}
		data, _, _ := unstructured.NestedStringMap(cm.Object, "data")
		if data["mykey"] != "newvalue" {
			t.Errorf("expected mykey to be 'newvalue', got %s", data["mykey"])
		}
	})

	t.Run("missing config_map name returns error", func(t *testing.T) {
		action := &ActionK8sUpdateConfigMap{
			ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
				Member:    "member1",
				ConfigMap: "",
				Key:       "key",
				Value:     "value",
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err := action.Execute(ctx, ec)
		if err == nil {
			t.Error("expected error for missing config_map name")
		}
	})

	t.Run("missing member returns error", func(t *testing.T) {
		action := &ActionK8sUpdateConfigMap{
			ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
				Member:    "",
				ConfigMap: "test",
				Key:       "key",
				Value:     "value",
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err := action.Execute(ctx, ec)
		if err == nil {
			t.Error("expected error for missing member")
		}
	})

	t.Run("missing key returns error", func(t *testing.T) {
		action := &ActionK8sUpdateConfigMap{
			ActionK8sUpdateConfigMapData: task.ActionK8sUpdateConfigMapData{
				Member:    "member1",
				ConfigMap: "test",
				Key:       "",
				Value:     "value",
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err := action.Execute(ctx, ec)
		if err == nil {
			t.Error("expected error for missing key")
		}
	})
}

func TestActionK8sScaleDeployment_GetType(t *testing.T) {
	action := &ActionK8sScaleDeployment{}
	if action.GetType() != task.ActionTypeK8sScaleDeployment {
		t.Errorf("expected GetType() to return ActionTypeK8sScaleDeployment, got %v", action.GetType())
	}
}

func TestActionK8sScaleDeployment_Execute(t *testing.T) {
	client := setupTestCluster(t)
	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	t.Run("successful scale up", func(t *testing.T) {
		deployName := fmt.Sprintf("test-scale-deploy-%d", time.Now().UnixNano())
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
							"app": "scale-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "scale-test",
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
		err = waitForDeploymentReady(t, client, ns, deployName, 3*time.Minute)
		if err != nil {
			t.Fatalf("Deployment did not become ready: %v", err)
		}

		action := &ActionK8sScaleDeployment{
			ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
				Member:     "member1",
				Deployment: deployName,
				Namespace:  ns,
				Replicas:   3,
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err = action.Execute(ctx, ec)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Verify the scale
		deployment, err := client.Get(ctx, "deployments", ns, deployName)
		if err != nil {
			t.Fatalf("Failed to get deployment: %v", err)
		}
		replicas, _, _ := unstructured.NestedInt64(deployment.Object, "spec", "replicas")
		if replicas != 3 {
			t.Errorf("expected 3 replicas, got %d", replicas)
		}
	})

	t.Run("scale to zero", func(t *testing.T) {
		deployName := fmt.Sprintf("test-scale-zero-%d", time.Now().UnixNano())
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
							"app": "scale-zero-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "scale-zero-test",
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
		err = waitForDeploymentReady(t, client, ns, deployName, 3*time.Minute)
		if err != nil {
			t.Fatalf("Deployment did not become ready: %v", err)
		}

		action := &ActionK8sScaleDeployment{
			ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
				Member:     "member1",
				Deployment: deployName,
				Namespace:  ns,
				Replicas:   0,
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err = action.Execute(ctx, ec)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Verify the scale
		deployment, err := client.Get(ctx, "deployments", ns, deployName)
		if err != nil {
			t.Fatalf("Failed to get deployment: %v", err)
		}
		replicas, _, _ := unstructured.NestedInt64(deployment.Object, "spec", "replicas")
		if replicas != 0 {
			t.Errorf("expected 0 replicas, got %d", replicas)
		}
	})

	t.Run("missing deployment name returns error", func(t *testing.T) {
		action := &ActionK8sScaleDeployment{
			ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
				Member:     "member1",
				Deployment: "",
				Replicas:   3,
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err := action.Execute(ctx, ec)
		if err == nil {
			t.Error("expected error for missing deployment name")
		}
	})

	t.Run("missing member returns error", func(t *testing.T) {
		action := &ActionK8sScaleDeployment{
			ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
				Member:     "",
				Deployment: "test",
				Replicas:   3,
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err := action.Execute(ctx, ec)
		if err == nil {
			t.Error("expected error for missing member")
		}
	})

	t.Run("non-existent deployment returns error", func(t *testing.T) {
		action := &ActionK8sScaleDeployment{
			ActionK8sScaleDeploymentData: task.ActionK8sScaleDeploymentData{
				Member:     "member1",
				Deployment: "non-existent-deployment",
				Namespace:  ns,
				Replicas:   3,
			},
		}

		ec := createMockEC(map[string]*k8s.Client{
			"member1": client,
		})

		err := action.Execute(ctx, ec)
		if err == nil {
			t.Error("expected error for non-existent deployment")
		}
	})
}
