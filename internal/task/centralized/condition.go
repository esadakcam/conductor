package centralized

import (
	"context"

	"github.com/esadakcam/conductor/internal/task"
)

func (c *ConditionEndpointSuccess) Evaluate(ctx context.Context) (bool, error) {
	panic("not implemented")
}

func (c *ConditionEndpointSuccess) GetType() task.ConditionType {
	return task.ConditionTypeEndpointSuccess
}

func (c *ConditionEndpointValue) Evaluate(ctx context.Context) (bool, error) {
	panic("not implemented")
}

func (c *ConditionEndpointValue) GetType() task.ConditionType {
	return task.ConditionTypeEndpointValue
}

func (c *ConditionPrometheusMetric) Evaluate(ctx context.Context) (bool, error) {
	panic("not implemented")
}

func (c *ConditionPrometheusMetric) GetType() task.ConditionType {
	return task.ConditionTypePrometheusMetric
}

func (c *ConditionK8sDeploymentReady) Evaluate(ctx context.Context) (bool, error) {
	panic("not implemented")
}

func (c *ConditionK8sDeploymentReady) GetType() task.ConditionType {
	return task.ConditionTypeK8sDeploymentReady
}

func (c *ConditionAlwaysTrue) Evaluate(ctx context.Context) (bool, error) {
	panic("not implemented")
}

func (c *ConditionAlwaysTrue) GetType() task.ConditionType {
	return task.ConditionTypeAlwaysTrue
}
