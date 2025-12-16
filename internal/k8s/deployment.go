package k8s

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// IsDeploymentReady checks if a deployment is ready.
// If expectedReplicas is not nil, it also checks if the deployment's spec.replicas matches the expected value.
// A deployment is ready when:
// - observedGeneration >= generation
// - updatedReplicas == spec.replicas
// - replicas == updatedReplicas
// - readyReplicas == updatedReplicas
// - availableReplicas == updatedReplicas
func IsDeploymentReady(obj map[string]interface{}, expectedReplicas *int32) (bool, error) {
	// Get generation and observedGeneration
	generation, found, err := nestedInt64(obj, "metadata", "generation")
	if err != nil || !found {
		return false, fmt.Errorf("failed to get metadata.generation")
	}

	observedGeneration, _, _ := nestedInt64(obj, "status", "observedGeneration")
	if observedGeneration < generation {
		return false, nil
	}

	// Get spec.replicas (defaults to 1 if not set)
	specReplicas, found, _ := nestedInt64(obj, "spec", "replicas")
	if !found {
		specReplicas = 1
	}

	// Check if spec.replicas matches expected replicas (if provided)
	if expectedReplicas != nil {
		if int32(specReplicas) != *expectedReplicas {
			return false, nil
		}
	}

	// Get status fields
	updatedReplicas, _, _ := nestedInt64(obj, "status", "updatedReplicas")
	replicas, _, _ := nestedInt64(obj, "status", "replicas")
	readyReplicas, _, _ := nestedInt64(obj, "status", "readyReplicas")
	availableReplicas, _, _ := nestedInt64(obj, "status", "availableReplicas")

	// Check if rollout is complete
	if updatedReplicas == specReplicas &&
		replicas == updatedReplicas &&
		readyReplicas == updatedReplicas &&
		availableReplicas == updatedReplicas {
		return true, nil
	}

	return false, nil
}

func nestedInt64(obj map[string]interface{}, fields ...string) (int64, bool, error) {
	val, found, err := unstructured.NestedFieldNoCopy(obj, fields...)
	if !found || err != nil {
		return 0, found, err
	}
	switch v := val.(type) {
	case int64:
		return v, true, nil
	case int:
		return int64(v), true, nil
	case float64:
		return int64(v), true, nil
	default:
		return 0, false, fmt.Errorf("val is not int64, int or float64: %T", val)
	}
}
