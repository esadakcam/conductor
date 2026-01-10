package distributed

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/esadakcam/conductor/internal/task"
)

func TestConditionK8sDeploymentReady_Evaluate(t *testing.T) {
	// Sample deployment responses
	readyDeployment := `{
		"metadata": {
			"name": "test-deployment",
			"namespace": "default",
			"generation": 1
		},
		"spec": {
			"replicas": 3
		},
		"status": {
			"observedGeneration": 1,
			"replicas": 3,
			"updatedReplicas": 3,
			"readyReplicas": 3,
			"availableReplicas": 3
		}
	}`

	rollingDeployment := `{
		"metadata": {
			"name": "test-deployment",
			"namespace": "default",
			"generation": 2
		},
		"spec": {
			"replicas": 3
		},
		"status": {
			"observedGeneration": 2,
			"replicas": 4,
			"updatedReplicas": 2,
			"readyReplicas": 3,
			"availableReplicas": 3
		}
	}`

	pendingDeployment := `{
		"metadata": {
			"name": "test-deployment",
			"namespace": "default",
			"generation": 1
		},
		"spec": {
			"replicas": 3
		},
		"status": {
			"observedGeneration": 1,
			"replicas": 3,
			"updatedReplicas": 3,
			"readyReplicas": 2,
			"availableReplicas": 2
		}
	}`

	staleGeneration := `{
		"metadata": {
			"name": "test-deployment",
			"namespace": "default",
			"generation": 3
		},
		"spec": {
			"replicas": 3
		},
		"status": {
			"observedGeneration": 2,
			"replicas": 3,
			"updatedReplicas": 3,
			"readyReplicas": 3,
			"availableReplicas": 3
		}
	}`

	wrongReplicaCount := `{
		"metadata": {
			"name": "test-deployment",
			"namespace": "default",
			"generation": 1
		},
		"spec": {
			"replicas": 5
		},
		"status": {
			"observedGeneration": 1,
			"replicas": 5,
			"updatedReplicas": 5,
			"readyReplicas": 5,
			"availableReplicas": 5
		}
	}`

	noStatusDeployment := `{
		"metadata": {
			"name": "test-deployment",
			"namespace": "default",
			"generation": 1
		},
		"spec": {
			"replicas": 3
		}
	}`

	tests := []struct {
		name           string
		condition      *ConditionK8sDeploymentReady
		serverHandler  http.HandlerFunc
		expectedResult bool
		expectedError  bool
	}{
		{
			name: "deployment is ready with expected replicas",
			condition: &ConditionK8sDeploymentReady{
				ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
					Member:     "",
					Deployment: "test-deployment",
					Namespace:  "default",
					Replicas:   3,
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(readyDeployment))
			},
			expectedResult: true,
			expectedError:  false,
		},
		{
			name: "deployment is rolling out",
			condition: &ConditionK8sDeploymentReady{
				ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
					Member:     "",
					Deployment: "test-deployment",
					Namespace:  "default",
					Replicas:   3,
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(rollingDeployment))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "deployment has pending pods",
			condition: &ConditionK8sDeploymentReady{
				ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
					Member:     "",
					Deployment: "test-deployment",
					Namespace:  "default",
					Replicas:   3,
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(pendingDeployment))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "deployment has stale observed generation",
			condition: &ConditionK8sDeploymentReady{
				ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
					Member:     "",
					Deployment: "test-deployment",
					Namespace:  "default",
					Replicas:   3,
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(staleGeneration))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "deployment has wrong replica count",
			condition: &ConditionK8sDeploymentReady{
				ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
					Member:     "",
					Deployment: "test-deployment",
					Namespace:  "default",
					Replicas:   3,
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(wrongReplicaCount))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "deployment has no status yet",
			condition: &ConditionK8sDeploymentReady{
				ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
					Member:     "",
					Deployment: "test-deployment",
					Namespace:  "default",
					Replicas:   3,
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(noStatusDeployment))
			},
			expectedResult: false,
			expectedError:  false,
		},
		{
			name: "empty member returns error",
			condition: &ConditionK8sDeploymentReady{
				ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
					Member:     "",
					Deployment: "test-deployment",
					Namespace:  "default",
					Replicas:   3,
				},
			},
			serverHandler:  nil,
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "empty deployment returns error",
			condition: &ConditionK8sDeploymentReady{
				ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
					Member:     "will-be-replaced",
					Deployment: "",
					Namespace:  "default",
					Replicas:   3,
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(readyDeployment))
			},
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "server returns error status",
			condition: &ConditionK8sDeploymentReady{
				ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
					Member:     "",
					Deployment: "test-deployment",
					Namespace:  "default",
					Replicas:   3,
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				w.Write([]byte(`{"error": "deployment not found"}`))
			},
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "server returns invalid JSON",
			condition: &ConditionK8sDeploymentReady{
				ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
					Member:     "",
					Deployment: "test-deployment",
					Namespace:  "default",
					Replicas:   3,
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("not valid json"))
			},
			expectedResult: false,
			expectedError:  true,
		},
		{
			name: "default namespace is used when not specified",
			condition: &ConditionK8sDeploymentReady{
				ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
					Member:     "",
					Deployment: "test-deployment",
					Namespace:  "",
					Replicas:   3,
				},
			},
			serverHandler: func(w http.ResponseWriter, r *http.Request) {
				// Verify the URL contains default namespace
				if r.URL.Path != "/api/v1/deployments/default/test-deployment" {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(readyDeployment))
			},
			expectedResult: true,
			expectedError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set up test server if handler is provided
			if tt.serverHandler != nil {
				server := httptest.NewServer(tt.serverHandler)
				defer server.Close()
				if tt.condition.Member == "" || tt.condition.Member == "will-be-replaced" {
					tt.condition.Member = server.URL
				}
			}

			result, err := tt.condition.Evaluate(context.Background(), nil)

			if tt.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if result != tt.expectedResult {
					t.Errorf("expected result %v, got %v", tt.expectedResult, result)
				}
			}
		})
	}
}

func TestConditionK8sDeploymentReady_Evaluate_InvalidEndpoint(t *testing.T) {
	condition := &ConditionK8sDeploymentReady{
		ConditionK8sDeploymentReadyData: task.ConditionK8sDeploymentReadyData{
			Member:     "http://invalid-endpoint-that-does-not-exist-12345.local",
			Deployment: "test-deployment",
			Namespace:  "default",
			Replicas:   3,
		},
	}

	result, err := condition.Evaluate(context.Background(), nil)

	if err == nil {
		t.Errorf("expected error for invalid endpoint, got none")
	}
	if result {
		t.Errorf("expected false result for invalid endpoint, got true")
	}
}

func TestConditionK8sDeploymentReady_GetType(t *testing.T) {
	condition := &ConditionK8sDeploymentReady{}
	if condition.GetType() != task.ConditionTypeK8sDeploymentReady {
		t.Errorf("expected type %v, got %v", task.ConditionTypeK8sDeploymentReady, condition.GetType())
	}
}
