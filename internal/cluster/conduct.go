package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/esadakcam/conductor/internal/task"
)

const WATCH_INTERVAL = 1 * time.Second

func Conduct(ctx context.Context, tasks []task.Task, epoch int64) {
	var wg sync.WaitGroup
	for _, t := range tasks {
		wg.Go(func() {
			watch(ctx, t, epoch)
		})
	}
	wg.Wait()
}

func watch(ctx context.Context, task task.Task, epoch int64) {
	fmt.Printf("Watching task %s\n", task.When.GetType())
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(WATCH_INTERVAL):
			result, err := task.When.Evaluate(ctx)
			if err != nil {
				fmt.Printf("Error evaluating condition: %v\n", err)
				continue
			}
			if result {
				fmt.Printf("Condition met, executing action for task %s\n", task.When.GetType())
				err := task.Then.Execute(ctx, epoch)
				if err != nil {
					fmt.Printf("Error executing action for task %s: %v\n", task.When.GetType(), err)
					continue
				}
				fmt.Printf("Action executed successfully for task %s\n", task.When.GetType())
				continue
			}
		}
	}

}
