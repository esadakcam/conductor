package cluster

import (
	"context"
	"sync"

	"github.com/esadakcam/conductor/internal/executor"
	"github.com/esadakcam/conductor/internal/k8s"
	"github.com/esadakcam/conductor/internal/task"
)

// CentralizedOutbox manages task execution for centralized mode
type CentralizedOutbox struct {
	ctx            context.Context
	mu             sync.Mutex
	executingTasks map[string]bool
	execCtx        *executor.CentralizedContext
}

// NewCentralizedOutbox creates a new outbox for centralized mode
func NewCentralizedOutbox(ctx context.Context, k8sClients map[string]*k8s.Client) *CentralizedOutbox {
	return &CentralizedOutbox{
		ctx:            ctx,
		executingTasks: make(map[string]bool),
		execCtx:        executor.NewCentralizedContext(k8sClients),
	}
}

func (o *CentralizedOutbox) IsTaskExecuting(ctx context.Context, taskName string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.executingTasks[taskName]
}

func (o *CentralizedOutbox) ExecuteTask(t task.TaskInterface) error {
	o.mu.Lock()
	if o.executingTasks[t.GetName()] {
		o.mu.Unlock()
		return nil
	}
	o.executingTasks[t.GetName()] = true
	o.mu.Unlock()

	for _, action := range t.GetActions() {
		if err := action.Execute(o.ctx, o.execCtx); err != nil {
			o.mu.Lock()
			o.executingTasks[t.GetName()] = false
			o.mu.Unlock()
			return err
		}
	}

	o.mu.Lock()
	o.executingTasks[t.GetName()] = false
	o.mu.Unlock()
	return nil
}

func (o *CentralizedOutbox) GetExecutionContext() task.ExecutionContext {
	return o.execCtx
}
