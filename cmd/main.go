package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/esadakcam/conductor/internal/cluster"
	"github.com/esadakcam/conductor/internal/k8s"
	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/server"
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
		logger.Info("\nShutting down...")
		cancel()
	}()

	configPath := os.Getenv("CONDUCTOR_CONFIG")
	if configPath == "" {
		configPath = "config/config.example.yaml"
	}
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	config, err := utils.LoadConfig(configPath)
	if err != nil {
		logger.Fatalf("Failed to load config: %v", err)
	}

	// Initialize leader election
	elector, etcdClient, err := cluster.NewLeaderElector(cluster.Config{
		EtcdEndpoints: config.EtcdEndpoints,
		Name:          config.Name,
		LeaderFn: func(ctx context.Context, epoch int64) error {
			cluster.Conduct(ctx, config.Tasks, epoch)
			logger.Info("Tasks are conducted")
			<-ctx.Done() // Wait for leadership to be lost
			return nil
		},
	})
	if err != nil {
		logger.Fatalf("Failed to initialize leader election: %v", err)
	}
	defer etcdClient.Close()

	// Initialize Kubernetes client
	k8sClient, err := k8s.NewClient("")
	if err != nil {
		logger.Fatalf("Failed to initialize Kubernetes client: %v", err)
	}

	// Initialize Epoch Validator
	epochValidator := server.NewEpochValidator(etcdClient, "")

	// Initialize HTTP Server
	srvHandler := server.NewHandler(k8sClient, epochValidator)
	srv := server.NewServer(server.Config{Port: config.Server.Port}, srvHandler)

	// Start server in background
	go func() {
		logger.Infof("Starting HTTP server on port %d", config.Server.Port)
		if err := srv.Run(ctx); err != nil {
			logger.Errorf("HTTP server error: %v", err)
			cancel()
		}
	}()

	logger.Infof("Participating in leader election (ID: %s)...", config.Name)
	if err := elector.Run(ctx); err != nil {
		if err != context.Canceled {
			logger.Fatalf("Leader election error: %v", err)
		}
		logger.Info("Leader election stopped")
	}
}
