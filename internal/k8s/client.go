package k8s

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/util/homedir"
)

// ExecResult contains the output from executing a command in a pod
type ExecResult struct {
	Stdout string
	Stderr string
}

// PodExecResult contains the result of executing a command in a specific pod
type PodExecResult struct {
	PodName string
	Result  *ExecResult
	Error   error
}

// Client wraps the Kubernetes dynamic client
type Client struct {
	client    dynamic.Interface
	clientset *kubernetes.Clientset
	config    *rest.Config
}

// NewClient creates a new Client
// It accepts an optional kubeconfig path or config object.
// If no arguments are provided, it tries to load from default locations.
func NewClient(kubeconfigPath string) (*Client, error) {
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

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	return &Client{
		client:    client,
		clientset: clientset,
		config:    config,
	}, nil
}

func (c *Client) getGVR(resource string) (schema.GroupVersionResource, error) {
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
func (c *Client) Get(ctx context.Context, resource, namespace, name string) (*unstructured.Unstructured, error) {
	gvr, err := c.getGVR(resource)
	if err != nil {
		return nil, err
	}

	return c.client.Resource(gvr).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
}

// List retrieves objects in a namespace (or all namespaces if namespace is empty)
func (c *Client) List(ctx context.Context, resource, namespace string) (*unstructured.UnstructuredList, error) {
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
func (c *Client) Create(ctx context.Context, resource, namespace string, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	gvr, err := c.getGVR(resource)
	if err != nil {
		return nil, err
	}

	return c.client.Resource(gvr).Namespace(namespace).Create(ctx, obj, metav1.CreateOptions{})
}

// Update updates an object
func (c *Client) Update(ctx context.Context, resource, namespace string, obj *unstructured.Unstructured) (*unstructured.Unstructured, error) {
	gvr, err := c.getGVR(resource)
	if err != nil {
		return nil, err
	}

	return c.client.Resource(gvr).Namespace(namespace).Update(ctx, obj, metav1.UpdateOptions{})
}

// Patch patches an object
func (c *Client) Patch(ctx context.Context, resource, namespace, name string, pt types.PatchType, data []byte) (*unstructured.Unstructured, error) {
	gvr, err := c.getGVR(resource)
	if err != nil {
		return nil, err
	}

	return c.client.Resource(gvr).Namespace(namespace).Patch(ctx, name, pt, data, metav1.PatchOptions{})
}

// Delete deletes an object
func (c *Client) Delete(ctx context.Context, resource, namespace, name string) error {
	gvr, err := c.getGVR(resource)
	if err != nil {
		return err
	}

	return c.client.Resource(gvr).Namespace(namespace).Delete(ctx, name, metav1.DeleteOptions{})
}

// Exec executes a command in a container within a pod and returns the output.
// If container is empty, it uses the first container in the pod.
func (c *Client) Exec(ctx context.Context, namespace, podName, container string, command []string) (*ExecResult, error) {
	req := c.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdin:     false,
			Stdout:    true,
			Stderr:    true,
			TTY:       false,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(c.config, "POST", req.URL())
	if err != nil {
		return nil, fmt.Errorf("failed to create executor: %w", err)
	}

	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &stdout,
		Stderr: &stderr,
	})
	if err != nil {
		return &ExecResult{
			Stdout: stdout.String(),
			Stderr: stderr.String(),
		}, fmt.Errorf("exec failed: %w", err)
	}

	return &ExecResult{
		Stdout: stdout.String(),
		Stderr: stderr.String(),
	}, nil
}

// ExecDeployment executes a command on all running pods of a deployment.
// If container is empty, it uses the first container in each pod.
// Returns results for each pod, including any errors encountered.
func (c *Client) ExecDeployment(ctx context.Context, namespace, deploymentName, container string, command []string) ([]PodExecResult, error) {
	// Get the deployment to find its selector
	deployment, err := c.Get(ctx, "deployments", namespace, deploymentName)
	if err != nil {
		return nil, fmt.Errorf("failed to get deployment %s: %w", deploymentName, err)
	}

	// Extract the selector from the deployment spec
	selector, found, err := unstructured.NestedMap(deployment.Object, "spec", "selector", "matchLabels")
	if err != nil || !found {
		return nil, fmt.Errorf("failed to get selector from deployment %s: %w", deploymentName, err)
	}

	// Build label selector string
	var labelSelector string
	for k, v := range selector {
		if labelSelector != "" {
			labelSelector += ","
		}
		labelSelector += fmt.Sprintf("%s=%s", k, v)
	}

	// List pods matching the selector
	pods, err := c.clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods for deployment %s: %w", deploymentName, err)
	}

	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no pods found for deployment %s", deploymentName)
	}

	// Execute command on each running pod
	var results []PodExecResult
	for _, pod := range pods.Items {
		// Skip pods that are not running
		if pod.Status.Phase != corev1.PodRunning {
			results = append(results, PodExecResult{
				PodName: pod.Name,
				Error:   fmt.Errorf("pod is not running (phase: %s)", pod.Status.Phase),
			})
			continue
		}

		result, err := c.Exec(ctx, namespace, pod.Name, container, command)
		results = append(results, PodExecResult{
			PodName: pod.Name,
			Result:  result,
			Error:   err,
		})
	}

	return results, nil
}
