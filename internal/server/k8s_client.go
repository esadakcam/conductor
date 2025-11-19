package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

// K8sClient wraps the Kubernetes dynamic client
type K8sClient struct {
	client dynamic.Interface
}

// NewK8sClient creates a new K8sClient
// It accepts an optional kubeconfig path or config object.
// If no arguments are provided, it tries to load from default locations.
func NewK8sClient(kubeconfigPath string) (*K8sClient, error) {
	var config *rest.Config
	var err error

	if kubeconfigPath != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load kubernetes config from %s: %w", kubeconfigPath, err)
		}
	} else {
		config, err = rest.InClusterConfig()
		if err != nil {
			// Fallback to kubeconfig
			var kubeconfig string
			if home := homedir.HomeDir(); home != "" {
				kubeconfig = filepath.Join(home, ".kube", "config")
			} else {
				kubeconfig = os.Getenv("KUBECONFIG")
			}

			// If KUBECONFIG env var is set, use it instead of default home path
			if os.Getenv("KUBECONFIG") != "" {
				kubeconfig = os.Getenv("KUBECONFIG")
			}

			config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
			if err != nil {
				return nil, fmt.Errorf("failed to load kubernetes config: %w", err)
			}
		}
	}

	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create dynamic client: %w", err)
	}

	return &K8sClient{
		client: client,
	}, nil
}

func (c *K8sClient) getGVR(resource string) (schema.GroupVersionResource, error) {
	switch resource {
	case "pods":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}, nil
	case "services":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}, nil
	case "configmaps":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}, nil
	case "secrets":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}, nil
	case "deployments":
		return schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}, nil
	case "namespaces":
		return schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}, nil
	default:
		return schema.GroupVersionResource{}, fmt.Errorf("unsupported resource: %s", resource)
	}
}

// Get retrieves an object
func (c *K8sClient) Get(ctx context.Context, resource, namespace, name string) (*unstructured.Unstructured, error) {
	gvr, err := c.getGVR(resource)
	if err != nil {
		return nil, err
	}

	return c.client.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
}

// List retrieves objects in a namespace (or all namespaces if namespace is empty)
func (c *K8sClient) List(ctx context.Context, resource, namespace string) (*unstructured.UnstructuredList, error) {
	gvr, err := c.getGVR(resource)
	if err != nil {
		return nil, err
	}

	if namespace == "" {
		return c.client.Resource(gvr).List(ctx, metav1.ListOptions{})
	}
	return c.client.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{})
}

// Create creates an object
func (c *K8sClient) Create(ctx context.Context, resource, namespace string, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	gvr, err := c.getGVR(resource)
	if err != nil {
		return nil, err
	}

	return c.client.Resource(gvr).Namespace(namespace).Create(ctx, obj, metav1.CreateOptions{})
}

// Update updates an object
func (c *K8sClient) Update(ctx context.Context, resource, namespace string, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	gvr, err := c.getGVR(resource)
	if err != nil {
		return nil, err
	}

	return c.client.Resource(gvr).Namespace(namespace).Update(ctx, obj, metav1.UpdateOptions{})
}

// Patch patches an object
func (c *K8sClient) Patch(ctx context.Context, resource, namespace, name string, pt types.PatchType, data []byte) (*unstructured.Unstructured, error) {
	gvr, err := c.getGVR(resource)
	if err != nil {
		return nil, err
	}

	return c.client.Resource(gvr).Namespace(namespace).Patch(ctx, name, pt, data, metav1.PatchOptions{})
}

// Delete deletes an object
func (c *K8sClient) Delete(ctx context.Context, resource, namespace, name string) error {
	gvr, err := c.getGVR(resource)
	if err != nil {
		return err
	}

	return c.client.Resource(gvr).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}
