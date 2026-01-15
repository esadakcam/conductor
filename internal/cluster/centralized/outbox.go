package centralized

import (
	"context"
	"sync"

	"github.com/esadakcam/conductor/internal/task"
)

type Outbox struct {
	ctx              context.Context
	mu               sync.Mutex
	executingTasks   map[string]bool
	executionContext *ExecutionContext
}

func NewOutbox(ctx context.Context, executionContext *ExecutionContext) *Outbox {
	return &Outbox{
		ctx:              ctx,
		mu:               sync.Mutex{},
		executingTasks:   make(map[string]bool),
		executionContext: executionContext,
	}
}

func (o *Outbox) IsTaskExecuting(ctx context.Context, taskName string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.executingTasks[taskName]
}

func (o *Outbox) ExecuteTask(t task.TaskInterface) error {
	o.mu.Lock()
	if o.executingTasks[t.GetName()] {
		o.mu.Unlock()
		return nil
	}
	o.executingTasks[t.GetName()] = true
	o.mu.Unlock()

	for _, action := range t.GetActions() {
		if err := action.Execute(o.ctx, o.executionContext); err != nil {
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

// GetExecutionContext returns the execution context for condition evaluation.
func (o *Outbox) GetExecutionContext() task.ExecutionContext {
	return o.executionContext
}
