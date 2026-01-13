// Package cluster provides the core orchestration logic for conductor.
package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/task"
)

const watchInterval = 15 * time.Second

// Conduct starts watching all tasks and executes them when conditions are met
func Conduct(ctx context.Context, tasks []task.TaskInterface, outbox Outbox) {
	var wg sync.WaitGroup
	for _, t := range tasks {
		wg.Add(1)
		go func(t task.TaskInterface) {
			defer wg.Done()
			watch(ctx, t, outbox)
		}(t)
	}
	wg.Wait()
}

func watch(ctx context.Context, t task.TaskInterface, outbox Outbox) {
	logger.Infof("Watching task %s", t.GetName())
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(watchInterval):
			if outbox.IsTaskExecuting(ctx, t.GetName()) {
				logger.Infof("Task %s is already executing, skipping", t.GetName())
				continue
			}

			// Get execution context from outbox for condition evaluation
			execCtx := outbox.GetExecutionContext()

			// Evaluate all conditions (AND logic)
			allConditionsMet := true
			for i, condition := range t.GetConditions() {
				result, err := condition.Evaluate(ctx, execCtx)
				if err != nil {
					logger.Errorf("Error evaluating condition %d for task %s: %v", i, t.GetName(), err)
					allConditionsMet = false
					break
				}
				if !result {
					allConditionsMet = false
					break
				}
			}

			if allConditionsMet {
				logger.Infof("All conditions met, executing actions for task %s", t.GetName())
				if err := outbox.ExecuteTask(t); err != nil {
					logger.Errorf("Error executing task %s: %v", t.GetName(), err)
					continue
				}
				logger.Infof("Task %s executed", t.GetName())
			}
		}
	}
}
