package centralized

import (
	"github.com/esadakcam/conductor/internal/task"
)

// UnmarshalYAML implementations for condition types

func (c *ConditionK8sDeploymentReady) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return task.UnmarshalConditionK8sDeploymentReady(unmarshal, &c.ConditionK8sDeploymentReadyData)
}

// UnmarshalYAML implementations for action types

func (a *ActionConfigValueSum) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return task.UnmarshalActionConfigValueSum(unmarshal, &a.ActionConfigValueSumData)
}

func (a *ActionK8sExecDeployment) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return task.UnmarshalActionK8sExecDeployment(unmarshal, &a.ActionK8sExecDeploymentData)
}

func (a *ActionK8sRestartDeployment) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return task.UnmarshalActionK8sRestartDeployment(unmarshal, &a.ActionK8sRestartDeploymentData)
}

func (a *ActionK8sWaitDeploymentRollout) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return task.UnmarshalActionK8sWaitDeploymentRollout(unmarshal, &a.ActionK8sWaitDeploymentRolloutData)
}

func (a *ActionK8sUpdateConfigMap) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return task.UnmarshalActionK8sUpdateConfigMap(unmarshal, &a.ActionK8sUpdateConfigMapData)
}

func (a *ActionK8sScaleDeployment) UnmarshalYAML(unmarshal func(interface{}) error) error {
	return task.UnmarshalActionK8sScaleDeployment(unmarshal, &a.ActionK8sScaleDeploymentData)
}
