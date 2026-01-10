package distributed

import (
	"github.com/esadakcam/conductor/internal/task"
	"github.com/esadakcam/conductor/internal/task/common"
)

// Condition types wrapping base types
type ConditionEndpointSuccess = common.ConditionEndpointSuccess

type ConditionAlwaysTrue = common.ConditionAlwaysTrue

type ConditionEndpointValue = common.ConditionEndpointValue

type ConditionPrometheusMetric = common.ConditionPrometheusMetric

type ConditionK8sDeploymentReady struct {
	task.ConditionK8sDeploymentReadyData
}

// Action types wrapping base types
type ActionEndpoint = common.ActionEndpoint

type ActionEcho = common.ActionEcho

type ActionDelay = common.ActionDelay

type ActionConfigValueSum struct {
	task.ActionConfigValueSumData
}

type ActionK8sExecDeployment struct {
	task.ActionK8sExecDeploymentData
}

type ActionK8sRestartDeployment struct {
	task.ActionK8sRestartDeploymentData
}

type ActionK8sWaitDeploymentRollout struct {
	task.ActionK8sWaitDeploymentRolloutData
}

type ActionK8sUpdateConfigMap struct {
	task.ActionK8sUpdateConfigMapData
}

type ActionK8sScaleDeployment struct {
	task.ActionK8sScaleDeploymentData
}

// Factory implements task.TypeFactory for distributed mode
type Factory struct{}

func (f Factory) NewConditionEndpointSuccess() task.Condition  { return &ConditionEndpointSuccess{} }
func (f Factory) NewConditionEndpointValue() task.Condition    { return &ConditionEndpointValue{} }
func (f Factory) NewConditionPrometheusMetric() task.Condition { return &ConditionPrometheusMetric{} }
func (f Factory) NewConditionAlwaysTrue() task.Condition       { return &ConditionAlwaysTrue{} }
func (f Factory) NewConditionK8sDeploymentReady() task.Condition {
	return &ConditionK8sDeploymentReady{}
}
func (f Factory) NewActionEndpoint() task.Action             { return &ActionEndpoint{} }
func (f Factory) NewActionEcho() task.Action                 { return &ActionEcho{} }
func (f Factory) NewActionDelay() task.Action                { return &ActionDelay{} }
func (f Factory) NewActionConfigValueSum() task.Action       { return &ActionConfigValueSum{} }
func (f Factory) NewActionK8sExecDeployment() task.Action    { return &ActionK8sExecDeployment{} }
func (f Factory) NewActionK8sRestartDeployment() task.Action { return &ActionK8sRestartDeployment{} }
func (f Factory) NewActionK8sWaitDeploymentRollout() task.Action {
	return &ActionK8sWaitDeploymentRollout{}
}
func (f Factory) NewActionK8sUpdateConfigMap() task.Action { return &ActionK8sUpdateConfigMap{} }
func (f Factory) NewActionK8sScaleDeployment() task.Action { return &ActionK8sScaleDeployment{} }

// GetFactory returns the type factory for distributed mode
func GetFactory() task.TypeFactory {
	return Factory{}
}
