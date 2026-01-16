package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/esadakcam/conductor/internal/cluster"
	centralizedCluster "github.com/esadakcam/conductor/internal/cluster/centralized"
	distributedCluster "github.com/esadakcam/conductor/internal/cluster/distributed"
	"github.com/esadakcam/conductor/internal/k8s"
	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/server"
	"github.com/esadakcam/conductor/internal/utils"
	clientv3 "go.etcd.io/etcd/client/v3"
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

	mode := os.Args[1]

	switch mode {
	case "distributed":
		initDistributedMode(ctx, cancel)
	case "centralized":
		initCentralizedMode(ctx, cancel)
	default:
		logger.Fatalf("Unknown mode: %s", mode)
	}

}

func initCentralizedMode(ctx context.Context, cancel context.CancelFunc) {
	configPath := os.Args[2]
	if configPath == "" {
		logger.Fatal("Config path is required")
	}
	config, err := utils.LoadCentralizedConfig(configPath)
	if err != nil {
		logger.Fatalf("Failed to load config: %v", err)
	}

	k8sClients := make(map[string]*k8s.Client)
	for _, kubeconfigLocation := range config.KubeconfigLocations {
		client, err := k8s.NewClient(kubeconfigLocation)
		if err != nil {
			logger.Fatalf("Failed to initialize Kubernetes client: %v", err)
		}
		clientHost := client.Config.Host
		k8sClients[clientHost] = client
	}

	tasks := config.Tasks

	executionContext := centralizedCluster.NewExecutionContext(k8sClients)
	outbox := centralizedCluster.NewOutbox(ctx, executionContext)
	cluster.Conduct(ctx, tasks, outbox)
}

func initDistributedMode(ctx context.Context, cancel context.CancelFunc) {
	configPath := os.Args[2]
	if configPath == "" {
		logger.Fatal("Config path is required")
	}
	config, err := utils.LoadDistributedConfig(configPath)
	if err != nil {
		logger.Fatalf("Failed to load config: %v", err)
	}

	if err != nil {
		logger.Fatalf("Failed to load distributed tasks: %v", err)
	}

	tasks := config.Tasks

	// Initialize leader election
	elector, etcdClient, err := distributedCluster.NewLeaderElector(distributedCluster.Config{
		EtcdEndpoints: config.EtcdEndpoints,
		Name:          config.Name,
		LeaderFn: func(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client) error {
			outbox := distributedCluster.NewOutbox(ctx, epoch, epochKey, client, tasks)
			cluster.Conduct(ctx, tasks, outbox)
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
	epochValidator := server.NewEpochValidator(etcdClient)
	idempotencyValidator := server.NewIdempotencyValidator(etcdClient, config.Name)

	// Initialize HTTP Server
	srvHandler := server.NewHandler(k8sClient, epochValidator, idempotencyValidator, etcdClient, config.Name)
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
