package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"gopkg.in/yaml.v3"

	"github.com/esadakcam/conductor/internal/cluster"
	"github.com/esadakcam/conductor/internal/task"
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
	etcdEndpoints, err := getEtcdEndpoints(configPath)
	if err != nil {
		log.Fatalf("Failed to get etcd endpoints: %v", err)
	}

	// Create etcd client
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create etcd client: %v", err)
	}
	defer etcdClient.Close()

	// Generate a unique leader ID
	hostname, _ := os.Hostname()
	leaderID := fmt.Sprintf("%s-%d", hostname, os.Getpid())

	// Create leader elector
	elector, err := cluster.NewLeaderElector(cluster.Config{
		Client:   etcdClient,
		ID:       leaderID,
		Prefix:   "/conductor/leader",
		EpochKey: "/conductor/epoch",
		LeaseTTL: 10,
		LeaderFn: func(ctx context.Context, epoch int64) error {
			log.Printf("🎯 Acquired leadership! Epoch: %d", epoch)
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
		Backoff: time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create leader elector: %v", err)
	}

	log.Printf("Participating in leader election (ID: %s)...", leaderID)
	if err := elector.Run(ctx); err != nil {
		if err != context.Canceled {
			log.Fatalf("Leader election error: %v", err)
		}
		log.Println("Leader election stopped")
	}
}

// getEtcdEndpoints reads etcd endpoints from the config file
func getEtcdEndpoints(configPath string) ([]string, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config struct {
		DB []string `yaml:"db"`
	}
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	if len(config.DB) == 0 {
		// Default to localhost endpoints if not specified
		return []string{"http://localhost:2379", "http://localhost:2479", "http://localhost:2579"}, nil
	}

	return config.DB, nil
}
