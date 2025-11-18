package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/esadakcam/conductor/internal/cluster"
	"github.com/esadakcam/conductor/internal/task"
	"github.com/esadakcam/conductor/internal/utils"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nShutting down...")
		cancel()
	}()

	configPath := "/Users/esad/Projects/conductor/config/config.example.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	config, err := task.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Read etcd endpoints from config file
	etcdEndpoints, err := utils.GetEtcdEndpoints(configPath)
	if err != nil {
		log.Fatalf("Failed to get etcd endpoints: %v", err)
	}

	// Read name from config file
	name, err := utils.GetName(configPath)
	if err != nil {
		log.Fatalf("Failed to get name: %v", err)
	}

	// Initialize leader election
	elector, etcdClient, err := cluster.NewLeaderElector(cluster.Config{
		EtcdEndpoints: etcdEndpoints,
		Name:          name,
		LeaderFn: func(ctx context.Context, epoch int64) error {
			log.Printf("Acquired leadership! Epoch: %d", epoch)
			log.Printf("Executing %d tasks...", len(config.Tasks))

			// Execute tasks as the leader
			for i, t := range config.Tasks {
				log.Printf("Task %d: Evaluating condition...", i+1)
				result, err := t.When.Evaluate()
				if err != nil {
					log.Printf("Task %d: Error evaluating condition: %v", i+1, err)
					continue
				}

				if result {
					log.Printf("Task %d: Condition met! Executing action...", i+1)
					if err := t.Then.Execute(); err != nil {
						log.Printf("Task %d: Error executing action: %v", i+1, err)
					} else {
						log.Printf("Task %d: Action executed successfully", i+1)
					}
				} else {
					log.Printf("Task %d: Condition not met, skipping action", i+1)
				}
			}

			// Keep running as leader (this will be cancelled when leadership is lost)
			<-ctx.Done()
			log.Printf("Leadership lost or cancelled")
			return ctx.Err()
		},
	})
	if err != nil {
		log.Fatalf("Failed to initialize leader election: %v", err)
	}
	defer etcdClient.Close()

	log.Printf("Participating in leader election (ID: %s)...", name)
	if err := elector.Run(ctx); err != nil {
		if err != context.Canceled {
			log.Fatalf("Leader election error: %v", err)
		}
		log.Println("Leader election stopped")
	}
}
