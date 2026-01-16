package centralized

import (
	"context"
	"fmt"

	"github.com/esadakcam/conductor/internal/k8s"
	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/task"
)

func (c *ConditionK8sDeploymentReady) Evaluate(ctx context.Context, ec task.ExecutionContext) (bool, error) {
	if c.Member == "" {
		err := fmt.Errorf("member is required")
		logger.Error("ConditionK8sDeploymentReady: member is required")
		return false, err
	}

	if c.Deployment == "" {
		err := fmt.Errorf("deployment is required")
		logger.Error("ConditionK8sDeploymentReady: deployment is required")
		return false, err
	}

	k8sClients := ec.GetK8sClients()

	client, ok := k8sClients[c.Member]
	if !ok {
		err := fmt.Errorf("no k8s client for member %s", c.Member)
		logger.Errorf("ConditionK8sDeploymentReady: %v", err)
		return false, err
	}

	namespace := c.Namespace
	if namespace == "" {
		namespace = "default"
	}

	deployment, err := client.Get(ctx, "deployments", namespace, c.Deployment)
	if err != nil {
		logger.Errorf("ConditionK8sDeploymentReady: failed to get deployment %s/%s: %v", namespace, c.Deployment, err)
		return false, fmt.Errorf("failed to get deployment %s/%s: %w", namespace, c.Deployment, err)
	}

	return k8s.IsDeploymentReady(deployment.Object, &c.Replicas)
}

func (c *ConditionK8sDeploymentReady) GetType() task.ConditionType {
	return task.ConditionTypeK8sDeploymentReady
}
