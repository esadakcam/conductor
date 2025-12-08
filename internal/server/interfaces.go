package server

import (
	"context"

	"github.com/esadakcam/conductor/internal/k8s"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

// KubernetesClient defines the interface for Kubernetes operations
type KubernetesClient interface {
	Get(ctx context.Context, resource, namespace, name string) (*unstructured.Unstructured, error)
	List(ctx context.Context, resource, namespace string) (*unstructured.UnstructuredList, error)
	Create(ctx context.Context, resource, namespace string, obj *unstructured.Unstructured) (*unstructured.Unstructured, error)
	Update(ctx context.Context, resource, namespace string, obj *unstructured.Unstructured) (*unstructured.Unstructured, error)
	Patch(ctx context.Context, resource, namespace, name string, pt types.PatchType, data []byte) (*unstructured.Unstructured, error)
	Delete(ctx context.Context, resource, namespace, name string) error
	ExecDeployment(ctx context.Context, namespace, deploymentName, container string, command []string) ([]k8s.PodExecResult, error)
}

// EpochChecker defines the interface for epoch validation
type EpochChecker interface {
	Validate(ctx context.Context, requestEpoch int64) (bool, error)
}
