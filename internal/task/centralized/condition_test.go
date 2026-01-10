package centralized

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/esadakcam/conductor/internal/k8s"
	"github.com/esadakcam/conductor/internal/task"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestConditionK8sDeploymentReady_GetType(t *testing.T) {
	condition := &ConditionK8sDeploymentReady{}
	if condition.GetType() != task.ConditionTypeK8sDeploymentReady {
		t.Errorf("expected type %v, got %v", task.ConditionTypeK8sDeploymentReady, condition.GetType())
	}
}

func TestConditionK8sDeploymentReady_Evaluate(t *testing.T) {
	client := setupTestCluster(t)
	ctx := context.Background()
	ns := setupTestNamespace(t, client)
	defer cleanupTestNamespace(t, client, ns)

	t.Run("deployment is ready with expected replicas", func(t *testing.T) {
		deployName := fmt.Sprintf("test-ready-deploy-%d", time.Now().UnixNano())
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
							"app": "ready-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "ready-test",
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

		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: deployName,
				Namespace:  ns,
				Replicas:   2,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		result, err := condition.Evaluate(ctx, payload)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !result {
			t.Errorf("expected deployment to be ready, got false")
		}
	})

	t.Run("deployment is ready but with different replica count", func(t *testing.T) {
		deployName := fmt.Sprintf("test-diff-replicas-%d", time.Now().UnixNano())
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
							"app": "diff-replicas-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "diff-replicas-test",
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

		// Expect 3 replicas but deployment has 1
		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: deployName,
				Namespace:  ns,
				Replicas:   3,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		result, err := condition.Evaluate(ctx, payload)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result {
			t.Errorf("expected deployment to NOT be ready (wrong replica count), got true")
		}
	})

	t.Run("deployment is not ready (pending pods)", func(t *testing.T) {
		deployName := fmt.Sprintf("test-pending-deploy-%d", time.Now().UnixNano())
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
							"app": "pending-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "pending-test",
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

		// Give Kubernetes a moment to start the deployment
		time.Sleep(2 * time.Second)

		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: deployName,
				Namespace:  ns,
				Replicas:   1,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		result, err := condition.Evaluate(ctx, payload)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result {
			t.Errorf("expected deployment to NOT be ready (pending pods), got true")
		}
	})

	t.Run("deployment with default namespace", func(t *testing.T) {
		deployName := fmt.Sprintf("test-default-ns-%d", time.Now().UnixNano())
		deployObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      deployName,
					"namespace": "default",
				},
				"spec": map[string]interface{}{
					"replicas": int64(1),
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "default-ns-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "default-ns-test",
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

		_, err := client.Create(ctx, "deployments", "default", deployObj)
		if err != nil {
			t.Fatalf("Failed to create deployment: %v", err)
		}
		defer client.Delete(ctx, "deployments", "default", deployName)

		// Wait for deployment to be ready
		err = waitForDeploymentReady(t, client, "default", deployName, 3*time.Minute)
		if err != nil {
			t.Fatalf("Deployment did not become ready: %v", err)
		}

		// Test with empty namespace (should default to "default")
		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: deployName,
				Namespace:  "", // Should default to "default"
				Replicas:   1,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		result, err := condition.Evaluate(ctx, payload)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !result {
			t.Errorf("expected deployment to be ready, got false")
		}
	})

	t.Run("missing member returns error", func(t *testing.T) {
		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "",
				Deployment: "test-deployment",
				Namespace:  ns,
				Replicas:   1,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		_, err := condition.Evaluate(ctx, payload)
		if err == nil {
			t.Error("expected error for missing member")
		}
	})

	t.Run("missing deployment name returns error", func(t *testing.T) {
		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: "",
				Namespace:  ns,
				Replicas:   1,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		_, err := condition.Evaluate(ctx, payload)
		if err == nil {
			t.Error("expected error for missing deployment name")
		}
	})

	t.Run("missing k8s client for member returns error", func(t *testing.T) {
		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "nonexistent",
				Deployment: "test-deployment",
				Namespace:  ns,
				Replicas:   1,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		_, err := condition.Evaluate(ctx, payload)
		if err == nil {
			t.Error("expected error for missing k8s client")
		}
	})

	t.Run("non-existent deployment returns error", func(t *testing.T) {
		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: "non-existent-deployment",
				Namespace:  ns,
				Replicas:   1,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		_, err := condition.Evaluate(ctx, payload)
		if err == nil {
			t.Error("expected error for non-existent deployment")
		}
	})

	t.Run("invalid payload format returns error", func(t *testing.T) {
		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: "test-deployment",
				Namespace:  ns,
				Replicas:   1,
			},
		}

		_, err := condition.Evaluate(ctx, "invalid payload")
		if err == nil {
			t.Error("expected error for invalid payload format")
		}
	})

	t.Run("missing k8sClients in payload returns error", func(t *testing.T) {
		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: "test-deployment",
				Namespace:  ns,
				Replicas:   1,
			},
		}

		payload := map[string]any{
			"other": "data",
		}

		_, err := condition.Evaluate(ctx, payload)
		if err == nil {
			t.Error("expected error for missing k8sClients in payload")
		}
	})

	t.Run("deployment with zero replicas expected", func(t *testing.T) {
		deployName := fmt.Sprintf("test-zero-replicas-%d", time.Now().UnixNano())
		deployObj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "apps/v1",
				"kind":       "Deployment",
				"metadata": map[string]interface{}{
					"name":      deployName,
					"namespace": ns,
				},
				"spec": map[string]interface{}{
					"replicas": int64(0),
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{
							"app": "zero-replicas-test",
						},
					},
					"template": map[string]interface{}{
						"metadata": map[string]interface{}{
							"labels": map[string]interface{}{
								"app": "zero-replicas-test",
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

		// Wait a moment for the deployment to be processed
		time.Sleep(2 * time.Second)

		condition := &ConditionK8sDeploymentReady{
			ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
				Member:     "member1",
				Deployment: deployName,
				Namespace:  ns,
				Replicas:   0,
			},
		}

		payload := createK8sClientsPayload(map[string]*k8s.Client{
			"member1": client,
		})

		result, err := condition.Evaluate(ctx, payload)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !result {
			t.Errorf("expected deployment with 0 replicas to be ready, got false")
		}
	})
}
