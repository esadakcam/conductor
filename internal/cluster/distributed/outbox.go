package distributed

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/task"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const outboxKey = "/conductor/outbox"

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
	epoch          int64
	epochKey       string
	client         *clientv3.Client
	mu             sync.Mutex
	executingTasks map[string]bool
}

func NewOutbox(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client, tasks []task.TaskInterface) *Outbox {
	o := &Outbox{
		ctx:            ctx,
		epoch:          epoch,
		epochKey:       epochKey,
		client:         client,
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
	then := []clientv3.Op{
		clientv3.OpPut(fmt.Sprintf("%s/%s", outboxKey, toExecute.GetName()), string(jsonData)),
	}
	cmp := clientv3.Compare(clientv3.Value(o.epochKey), "=", fmt.Sprintf("%d", o.epoch))

	// Hold lock through transaction commit to prevent TOCTOU race
	_, err = o.client.Txn(o.ctx).If(cmp).Then(then...).Commit()
	if err != nil {
		o.mu.Unlock()
		logger.Errorf("failed to add task to outbox: %v", err)
		return err
	}

	// Only mark as executing after successful commit
	o.executingTasks[toExecute.GetName()] = true
	o.mu.Unlock()

	o.fulfillTaskFromOutbox(toExecute)
	return nil
}

// GetPayload returns nil for distributed mode as conditions use HTTP-based
// communication instead of direct k8s clients.
func (o *Outbox) GetPayload() any {
	return nil
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
	logger.Infof("Initializing outbox with %d tasks", len(tasks))
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
		payload := map[string]any{
			"idempotencyId": item.ID.String(),
			"epoch":         o.epoch,
		}

		operation := fmt.Sprintf("task %s step %d", t.GetName(), i)
		// Since members are idempotent, we can safely retry the operation.
		err = executeWithRetry(o.ctx, operation, func() error {
			return t.GetActions()[i].Execute(o.ctx, payload)
		})
		if err != nil {
			return err
		}
		newItem := OutboxItem{
			ID:        uuid.New(),
			CreatedAt: time.Now(),
			TaskStep:  i + 1,
		}
		json, err := json.Marshal(newItem)
		if err != nil {
			logger.Errorf("failed to marshal item: %v", err)
			return err
		}
		cmp := clientv3.Compare(clientv3.Value(o.epochKey), "=", fmt.Sprintf("%d", o.epoch))
		var then []clientv3.Op
		if i < int64(len(t.GetActions()))-1 {
			// Not the last step: update the outbox entry with new step
			then = []clientv3.Op{
				clientv3.OpPut(fmt.Sprintf("%s/%s", outboxKey, t.GetName()), string(json)),
			}
		} else {
			// Last step: remove from outbox
			then = []clientv3.Op{
				clientv3.OpDelete(fmt.Sprintf("%s/%s", outboxKey, t.GetName())),
			}
		}
		_, err = o.client.Txn(o.ctx).If(cmp).Then(then...).Commit()
		if err != nil {
			logger.Errorf("failed to commit transaction: %v", err)
			return err
		}
		item = newItem
	}
	o.mu.Lock()
	o.executingTasks[t.GetName()] = false
	o.mu.Unlock()
	return nil
}
