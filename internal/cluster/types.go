package cluster

import (
	"context"

	"github.com/esadakcam/conductor/internal/task"
)

type Outbox interface {
	IsTaskExecuting(ctx context.Context, taskName string) bool
	ExecuteTask(task task.TaskInterface) error
	// GetPayload returns the payload to be passed to condition evaluation.
	// This allows conditions to access execution environment resources like k8s clients.
	GetPayload() any
}
