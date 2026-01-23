package centralized

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/esadakcam/conductor/internal/k8s"
	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/task"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const outboxKey = "/conductor/centralized/outbox"

const (
	maxRetries     = 3
	initialBackoff = 1 * time.Second
	maxBackoff     = 30 * time.Second
)

type OutboxItem struct {
	ID        uuid.UUID `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	TaskStep  int64     `json:"task_step"`
}

type Outbox struct {
	ctx            context.Context
	client         *clientv3.Client
	k8sClients     map[string]*k8s.Client
	mu             sync.Mutex
	executingTasks map[string]bool
}

func NewOutbox(ctx context.Context, client *clientv3.Client, k8sClients map[string]*k8s.Client, tasks []task.TaskInterface) *Outbox {
	o := &Outbox{
		ctx:            ctx,
		client:         client,
		k8sClients:     k8sClients,
		executingTasks: make(map[string]bool),
	}
	o.init(tasks)
	return o
}

func (o *Outbox) IsTaskExecuting(ctx context.Context, taskName string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if val, ok := o.executingTasks[taskName]; ok {
		return val
	}
	resp, err := o.client.Get(ctx, fmt.Sprintf("%s/%s", outboxKey, taskName))
	if err != nil {
		return false
	}
	if len(resp.Kvs) == 0 {
		return false
	}
	o.executingTasks[taskName] = true
	return true
}

func (o *Outbox) ExecuteTask(toExecute task.TaskInterface) error {
	o.mu.Lock()
	if o.executingTasks[toExecute.GetName()] {
		o.mu.Unlock()
		return nil
	}

	// Prepare the outbox item while holding the lock
	item := OutboxItem{
		ID:        uuid.New(),
		CreatedAt: time.Now(),
		TaskStep:  0,
	}
	jsonData, err := json.Marshal(item)
	if err != nil {
		o.mu.Unlock()
		logger.Errorf("failed to marshal item: %v", err)
		return err
	}

	// Store the task in etcd
	_, err = o.client.Put(o.ctx, fmt.Sprintf("%s/%s", outboxKey, toExecute.GetName()), string(jsonData))
	if err != nil {
		o.mu.Unlock()
		logger.Errorf("failed to add task to outbox: %v", err)
		return err
	}

	// Only mark as executing after successful commit
	o.executingTasks[toExecute.GetName()] = true
	o.mu.Unlock()

	return o.fulfillTaskFromOutbox(toExecute)
}

// GetExecutionContext returns an execution context for condition evaluation.
// For condition evaluation, no idempotency key is needed.
func (o *Outbox) GetExecutionContext() task.ExecutionContext {
	return NewExecutionContext(o.k8sClients, "")
}

func executeWithRetry(ctx context.Context, operation string, fn func() error) error {
	var lastErr error
	backoff := initialBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			logger.Infof("Retrying %s (attempt %d/%d) after %v", operation, attempt, maxRetries, backoff)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		logger.Errorf("failed %s (attempt %d/%d): %v", operation, attempt+1, maxRetries+1, lastErr)
	}

	logger.Errorf("%s failed after %d attempts: %v", operation, maxRetries+1, lastErr)
	return lastErr
}

func (o *Outbox) init(tasks []task.TaskInterface) {
	logger.Infof("Initializing centralized outbox with %d tasks", len(tasks))
	for _, t := range tasks {
		go func(t task.TaskInterface) {
			existing, err := o.client.Get(o.ctx, fmt.Sprintf("%s/%s", outboxKey, t.GetName()))
			if err != nil {
				logger.Errorf("failed to get existing task: %v", err)
				return
			}
			if len(existing.Kvs) == 0 {
				return
			}
			logger.Infof("Fulfilling task %s from outbox (init)", t.GetName())
			o.fulfillTaskFromOutbox(t)
		}(t)
	}
}

func (o *Outbox) fulfillTaskFromOutbox(t task.TaskInterface) error {
	existing, err := o.client.Get(o.ctx, fmt.Sprintf("%s/%s", outboxKey, t.GetName()))
	if err != nil {
		logger.Errorf("failed to get existing task: %v", err)
		return err
	}
	if len(existing.Kvs) == 0 {
		return nil
	}
	o.mu.Lock()
	o.executingTasks[t.GetName()] = true
	o.mu.Unlock()

	var item OutboxItem
	err = json.Unmarshal(existing.Kvs[0].Value, &item)
	if err != nil {
		logger.Errorf("failed to unmarshal existing task: %v", err)
		return err
	}

	for i := item.TaskStep; i < int64(len(t.GetActions())); i++ {
		// Create execution context with idempotency key for this action
		ec := NewExecutionContext(o.k8sClients, item.ID.String())

		operation := fmt.Sprintf("task %s step %d", t.GetName(), i)
		// Actions are idempotent via K8s labels, so we can safely retry
		err = executeWithRetry(o.ctx, operation, func() error {
			return t.GetActions()[i].Execute(o.ctx, ec)
		})
		if err != nil {
			return err
		}

		// Generate new idempotency key for next step
		newItem := OutboxItem{
			ID:        uuid.New(),
			CreatedAt: time.Now(),
			TaskStep:  i + 1,
		}
		jsonData, err := json.Marshal(newItem)
		if err != nil {
			logger.Errorf("failed to marshal item: %v", err)
			return err
		}

		if i < int64(len(t.GetActions()))-1 {
			// Not the last step: update the outbox entry with new step and new idempotency key
			_, err = o.client.Put(o.ctx, fmt.Sprintf("%s/%s", outboxKey, t.GetName()), string(jsonData))
		} else {
			// Last step: remove from outbox
			_, err = o.client.Delete(o.ctx, fmt.Sprintf("%s/%s", outboxKey, t.GetName()))
		}
		if err != nil {
			logger.Errorf("failed to update outbox: %v", err)
			return err
		}
		item = newItem
	}

	o.mu.Lock()
	o.executingTasks[t.GetName()] = false
	o.mu.Unlock()
	return nil
}
