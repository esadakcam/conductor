package centralized

import (
	"context"

	"github.com/esadakcam/conductor/internal/task"
)

func (a *ActionEndpoint) Execute(ctx context.Context, payload any) error {
	panic("not implemented")
}

func (a *ActionEndpoint) GetType() task.ActionType {
	return task.ActionTypeEndpoint
}

func (a *ActionConfigValueSum) Execute(ctx context.Context, payload any) error {
	panic("not implemented")
}

func (a *ActionConfigValueSum) GetType() task.ActionType {
	return task.ActionTypeConfigValueSum
}

func (a *ActionEcho) Execute(ctx context.Context, payload any) error {
	panic("not implemented")
}

func (a *ActionEcho) GetType() task.ActionType {
	return task.ActionTypeEcho
}

func (a *ActionDelay) Execute(ctx context.Context, payload any) error {
	panic("not implemented")
}

func (a *ActionDelay) GetType() task.ActionType {
	return task.ActionTypeDelay
}

func (a *ActionK8sExecDeployment) Execute(ctx context.Context, payload any) error {
	panic("not implemented")
}

func (a *ActionK8sExecDeployment) GetType() task.ActionType {
	return task.ActionTypeK8sExecDeployment
}

func (a *ActionK8sRestartDeployment) Execute(ctx context.Context, payload any) error {
	panic("not implemented")
}

func (a *ActionK8sRestartDeployment) GetType() task.ActionType {
	return task.ActionTypeK8sRestartDeployment
}

func (a *ActionK8sWaitDeploymentRollout) Execute(ctx context.Context, payload any) error {
	panic("not implemented")
}

func (a *ActionK8sWaitDeploymentRollout) GetType() task.ActionType {
	return task.ActionTypeK8sWaitDeploymentRollout
}

func (a *ActionK8sUpdateConfigMap) Execute(ctx context.Context, payload any) error {
	panic("not implemented")
}

func (a *ActionK8sUpdateConfigMap) GetType() task.ActionType {
	return task.ActionTypeK8sUpdateConfigMap
}

func (a *ActionK8sScaleDeployment) Execute(ctx context.Context, payload any) error {
	panic("not implemented")
}

func (a *ActionK8sScaleDeployment) GetType() task.ActionType {
	return task.ActionTypeK8sScaleDeployment
}
