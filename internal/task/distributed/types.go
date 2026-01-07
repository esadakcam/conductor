package distributed

import (
	"github.com/esadakcam/conductor/internal/task"
)

type Task struct {
	Name string           `yaml:"name"`
	When []task.Condition `yaml:"when"`
	Then []task.Action    `yaml:"then"`
}

func (t *Task) GetName() string {
	return t.Name
}

func (t *Task) GetConditions() []task.Condition {
	return t.When
}

func (t *Task) GetActions() []task.Action {
	return t.Then
}

type ConditionEndpointSuccess struct {
	task.ConditionEndpointSuccess
}

type ConditionAlwaysTrue struct {
	task.ConditionAlwaysTrue
}

type ConditionEndpointValue struct {
	task.ConditionEndpointValue
}

type ConditionPrometheusMetric struct {
	task.ConditionPrometheusMetric
}

type ConditionK8sDeploymentReady struct {
	task.ConditionK8sDeploymentReady
}

type ActionEndpoint struct {
	task.ActionEndpoint
}

type ActionEcho struct {
	task.ActionEcho
}

type ActionDelay struct {
	task.ActionDelay
}

type ActionConfigValueSum struct {
	task.ActionConfigValueSum
}

type ActionK8sExecDeployment struct {
	task.ActionK8sExecDeployment
}

type ActionK8sRestartDeployment struct {
	task.ActionK8sRestartDeployment
}

type ActionK8sWaitDeploymentRollout struct {
	task.ActionK8sWaitDeploymentRollout
}

type ActionK8sUpdateConfigMap struct {
	task.ActionK8sUpdateConfigMap
}

type ActionK8sScaleDeployment struct {
	task.ActionK8sScaleDeployment
}
