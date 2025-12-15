package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/task"
)

const watchInterval = 15 * time.Second

func Conduct(ctx context.Context, tasks []task.Task, outbox *Outbox) {
	var wg sync.WaitGroup
	for _, t := range tasks {
		wg.Add(1)
		go func(t task.Task) {
			defer wg.Done()
			watch(ctx, t, outbox)
		}(t)
	}
	wg.Wait()
}

func watch(ctx context.Context, task task.Task, outbox *Outbox) {
	logger.Infof("Watching task %s", task.Name)
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(watchInterval):
			if outbox.IsTaskExecuting(ctx, task.Name) {
				logger.Infof("Task %s is already executing, skipping", task.Name)
				continue
			}

			// Evaluate all conditions (AND logic)
			allConditionsMet := true
			for i, condition := range task.When {
				result, err := condition.Evaluate(ctx)
				if err != nil {
					logger.Errorf("Error evaluating condition %d for task %s: %v", i, task.Name, err)
					allConditionsMet = false
					break
				}
				if !result {
					allConditionsMet = false
					break
				}
			}

			if allConditionsMet {
				logger.Infof("All conditions met, executing actions for task %s", task.Name)
				err := outbox.ExecuteTask(task)
				if err != nil {
					logger.Errorf("Error adding task %s to outbox: %v", task.Name, err)
					continue
				}
				logger.Infof("Task %s executed", task.Name)
				continue
			}
		}
	}
}
