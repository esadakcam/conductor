package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/esadakcam/conductor/internal/cluster"
	"github.com/esadakcam/conductor/internal/server"
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

	// Read server config
	serverConfig, err := utils.GetServerConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to get server config: %v", err)
	}

	// Initialize leader election
	elector, etcdClient, err := cluster.NewLeaderElector(cluster.Config{
		EtcdEndpoints: etcdEndpoints,
		Name:          name,
		LeaderFn: func(ctx context.Context, epoch int64) error {
			cluster.Conduct(ctx, config.Tasks, epoch)
			fmt.Println("Tasks are conducted")
			<-ctx.Done() // Wait for leadership to be lost
			return nil
		},
	})
	if err != nil {
		log.Fatalf("Failed to initialize leader election: %v", err)
	}
	defer etcdClient.Close()

	// Initialize Kubernetes client
	k8sClient, err := server.NewK8sClient("")
	if err != nil {
		log.Fatalf("Failed to initialize Kubernetes client: %v", err)
	}

	// Initialize Epoch Validator
	epochValidator := server.NewEpochValidator(etcdClient, "")

	// Initialize HTTP Server
	srvHandler := server.NewHandler(k8sClient, epochValidator)
	srv := server.NewServer(server.Config{Port: serverConfig.Port}, srvHandler)

	// Start server in background
	go func() {
		log.Printf("Starting HTTP server on port %d", serverConfig.Port)
		if err := srv.Run(ctx); err != nil {
			log.Printf("HTTP server error: %v", err)
			cancel()
		}
	}()

	log.Printf("Participating in leader election (ID: %s)...", name)
	if err := elector.Run(ctx); err != nil {
		if err != context.Canceled {
			log.Fatalf("Leader election error: %v", err)
		}
		log.Println("Leader election stopped")
	}
}
