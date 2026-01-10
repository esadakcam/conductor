package centralized

import (
	"context"
	"sync"

	"github.com/esadakcam/conductor/internal/k8s"
	"github.com/esadakcam/conductor/internal/task"
)

type Outbox struct {
	ctx            context.Context
	mu             sync.Mutex
	executingTasks map[string]bool
	k8sClients     map[string]*k8s.Client
}

func NewOutbox(ctx context.Context, k8sClients map[string]*k8s.Client) *Outbox {
	return &Outbox{
		ctx:            ctx,
		mu:             sync.Mutex{},
		executingTasks: make(map[string]bool),
		k8sClients:     k8sClients,
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

	payload := o.GetPayload()
	for _, action := range t.GetActions() {
		if err := action.Execute(o.ctx, payload); err != nil {
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

// GetPayload returns the payload containing k8s clients for condition evaluation.
func (o *Outbox) GetPayload() any {
	return map[string]any{
		"k8sClients": o.k8sClients,
	}
}
