package cluster

import (
	"context"
	"sync"
	"time"

	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/task"
	"github.com/google/uuid"
)

const watchInterval = 1 * time.Second

func Conduct(ctx context.Context, tasks []task.Task, epoch int64) {
	var wg sync.WaitGroup
	for _, t := range tasks {
		wg.Add(1)
		go func(t task.Task) {
			defer wg.Done()
			watch(ctx, t, epoch)
		}(t)
	}
	wg.Wait()
}

func watch(ctx context.Context, task task.Task, epoch int64) {
	logger.Infof("Watching task %s", task.When.GetType())
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(watchInterval):
			result, err := task.When.Evaluate(ctx)
			if err != nil {
				logger.Errorf("Error evaluating condition: %v", err)
				continue
			}
			if result {
				logger.Infof("Condition met, executing action for task %s", task.When.GetType())
				err := task.Then.Execute(ctx, epoch, uuid.New().String())
				if err != nil {
					logger.Errorf("Error executing action for task %s: %v", task.When.GetType(), err)
					continue
				}
				logger.Infof("Action executed successfully for task %s", task.When.GetType())
				continue
			}
		}
	}

}
