package centralized

import (
	"fmt"

	"github.com/esadakcam/conductor/internal/k8s"
)

// getK8sClients extracts the k8s client map from the payload.
// This utility function is shared between actions and conditions that need
// to interact with Kubernetes clusters in centralized mode.
//
// The payload is expected to be a map[string]any containing a "k8sClients" key
// with a value of type map[string]*k8s.Client, where keys are cluster identifiers
// (typically the API server host) and values are the corresponding k8s clients.
//
// Returns an error if the payload format is invalid or k8sClients is missing.
func getK8sClients(payload any) (map[string]*k8s.Client, error) {
	data, ok := payload.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid payload format")
	}
	k8sClients, ok := data["k8sClients"].(map[string]*k8s.Client)
	if !ok {
		return nil, fmt.Errorf("invalid or missing k8sClients in payload")
	}
	return k8sClients, nil
}
