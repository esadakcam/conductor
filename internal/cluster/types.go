package cluster

import (
	"context"

	"github.com/esadakcam/conductor/internal/task"
)

type Outbox interface {
	IsTaskExecuting(ctx context.Context, taskName string) bool
	ExecuteTask(task task.Task) error
}
