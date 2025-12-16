package k8s

import (
	"testing"
)

func int32Ptr(i int32) *int32 { return &i }

func TestIsDeploymentReady(t *testing.T) {
	tests := []struct {
		name             string
		deployment       map[string]interface{}
		expectedReplicas *int32
		expectedResult   bool
		expectedError    bool
	}{
		{
			name: "deployment is ready",
			deployment: map[string]interface{}{
				"metadata": map[string]interface{}{
					"generation": int64(1),
				},
				"spec": map[string]interface{}{
					"replicas": int64(3),
				},
				"status": map[string]interface{}{
					"observedGeneration": int64(1),
					"replicas":           int64(3),
					"updatedReplicas":    int64(3),
					"readyReplicas":      int64(3),
					"availableReplicas":  int64(3),
				},
			},
			expectedReplicas: int32Ptr(3),
			expectedResult:   true,
			expectedError:    false,
		},
		{
			name: "deployment is not ready - different replica count",
			deployment: map[string]interface{}{
				"metadata": map[string]interface{}{
					"generation": int64(1),
				},
				"spec": map[string]interface{}{
					"replicas": int64(5),
				},
				"status": map[string]interface{}{
					"observedGeneration": int64(1),
					"replicas":           int64(5),
					"updatedReplicas":    int64(5),
					"readyReplicas":      int64(5),
					"availableReplicas":  int64(5),
				},
			},
			expectedReplicas: int32Ptr(3),
			expectedResult:   false,
			expectedError:    false,
		},
		{
			name: "deployment is not ready - some pods not available",
			deployment: map[string]interface{}{
				"metadata": map[string]interface{}{
					"generation": int64(1),
				},
				"spec": map[string]interface{}{
					"replicas": int64(3),
				},
				"status": map[string]interface{}{
					"observedGeneration": int64(1),
					"replicas":           int64(3),
					"updatedReplicas":    int64(3),
					"readyReplicas":      int64(3),
					"availableReplicas":  int64(2),
				},
			},
			expectedReplicas: int32Ptr(3),
			expectedResult:   false,
			expectedError:    false,
		},
		{
			name: "missing metadata returns error",
			deployment: map[string]interface{}{
				"spec": map[string]interface{}{
					"replicas": int64(3),
				},
			},
			expectedReplicas: int32Ptr(3),
			expectedResult:   false,
			expectedError:    true,
		},
		{
			name: "missing spec returns error (generation check passes but spec check might fail if strict)",
			// Wait, IsDeploymentReady checks metadata.generation first.
			deployment: map[string]interface{}{
				"metadata": map[string]interface{}{
					"generation": int64(1),
				},
			},
			expectedReplicas: int32Ptr(3),
			expectedResult:   false,
			// spec.replicas defaults to 1. If expectedReplicas is 3, it returns false, nil.
			expectedError: false,
		},
		{
			name: "default replicas (1) when not specified",
			deployment: map[string]interface{}{
				"metadata": map[string]interface{}{
					"generation": int64(1),
				},
				"spec": map[string]interface{}{},
				"status": map[string]interface{}{
					"observedGeneration": int64(1),
					"replicas":           int64(1),
					"updatedReplicas":    int64(1),
					"readyReplicas":      int64(1),
					"availableReplicas":  int64(1),
				},
			},
			expectedReplicas: int32Ptr(1),
			expectedResult:   true,
			expectedError:    false,
		},
		{
			name: "nil expected replicas - check rollout only",
			deployment: map[string]interface{}{
				"metadata": map[string]interface{}{
					"generation": int64(1),
				},
				"spec": map[string]interface{}{
					"replicas": int64(5),
				},
				"status": map[string]interface{}{
					"observedGeneration": int64(1),
					"replicas":           int64(5),
					"updatedReplicas":    int64(5),
					"readyReplicas":      int64(5),
					"availableReplicas":  int64(5),
				},
			},
			expectedReplicas: nil,
			expectedResult:   true,
			expectedError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := IsDeploymentReady(tt.deployment, tt.expectedReplicas)

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
