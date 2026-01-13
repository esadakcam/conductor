package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/esadakcam/conductor/internal/executor"
	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/task"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const outboxKey = "/conductor/outbox"

// OutboxItem represents a task being executed in the outbox
type OutboxItem struct {
	ID        uuid.UUID `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	TaskStep  int64     `json:"task_step"`
}

// DistributedOutbox manages task execution for distributed mode
type DistributedOutbox struct {
	ctx            context.Context
	epoch          int64
	epochKey       string
	client         *clientv3.Client
	mu             sync.Mutex
	executingTasks map[string]bool
}

// NewDistributedOutbox creates a new outbox for distributed mode
func NewDistributedOutbox(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client, tasks []task.TaskInterface) *DistributedOutbox {
	o := &DistributedOutbox{
		ctx:            ctx,
		epoch:          epoch,
		epochKey:       epochKey,
		client:         client,
		executingTasks: make(map[string]bool),
	}
	o.init(tasks)
	return o
}

func (o *DistributedOutbox) IsTaskExecuting(ctx context.Context, taskName string) bool {
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

func (o *DistributedOutbox) ExecuteTask(toExecute task.TaskInterface) error {
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

	return o.fulfillTaskFromOutbox(toExecute)
}

func (o *DistributedOutbox) GetExecutionContext() task.ExecutionContext {
	// For distributed mode, we return nil since conditions don't need
	// the full execution context - they use HTTP-based communication
	return executor.NewDistributedContext("", o.epoch)
}

func (o *DistributedOutbox) init(tasks []task.TaskInterface) {
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

func (o *DistributedOutbox) fulfillTaskFromOutbox(t task.TaskInterface) error {
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
		execCtx := executor.NewDistributedContext(item.ID.String(), o.epoch)
		err := t.GetActions()[i].Execute(o.ctx, execCtx)
		if err != nil {
			logger.Errorf("failed to execute task step %d: %v", i, err)
			return err
		}

		newItem := OutboxItem{
			ID:        uuid.New(),
			CreatedAt: time.Now(),
			TaskStep:  i + 1,
		}
		jsonBytes, err := json.Marshal(newItem)
		if err != nil {
			logger.Errorf("failed to marshal item: %v", err)
			return err
		}

		cmp := clientv3.Compare(clientv3.Value(o.epochKey), "=", fmt.Sprintf("%d", o.epoch))
		var then []clientv3.Op
		if i < int64(len(t.GetActions()))-1 {
			// Not the last step: update the outbox entry with new step
			then = []clientv3.Op{
				clientv3.OpPut(fmt.Sprintf("%s/%s", outboxKey, t.GetName()), string(jsonBytes)),
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
