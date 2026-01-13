package cluster

import (
	"context"

	"github.com/esadakcam/conductor/internal/task"
)

// Outbox manages task execution state
type Outbox interface {
	// IsTaskExecuting returns true if the task is currently being executed
	IsTaskExecuting(ctx context.Context, taskName string) bool

	// ExecuteTask executes the task's actions
	ExecuteTask(t task.TaskInterface) error

	// GetExecutionContext returns the execution context for condition evaluation
	GetExecutionContext() task.ExecutionContext
}
