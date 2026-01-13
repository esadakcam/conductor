package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/esadakcam/conductor/internal/cluster"
	"github.com/esadakcam/conductor/internal/config"
	"github.com/esadakcam/conductor/internal/k8s"
	"github.com/esadakcam/conductor/internal/logger"
	"github.com/esadakcam/conductor/internal/server"
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

	if len(os.Args) < 2 {
		logger.Fatal("Usage: conductor <mode> <config-path>")
	}

	mode := os.Args[1]

	switch mode {
	case "distributed":
		runDistributedMode(ctx, cancel)
	case "centralized":
		runCentralizedMode(ctx)
	default:
		logger.Fatalf("Unknown mode: %s", mode)
	}
}

func runCentralizedMode(ctx context.Context) {
	if len(os.Args) < 3 {
		logger.Fatal("Config path is required")
	}
	configPath := os.Args[2]

	cfg, err := config.LoadCentralized(configPath)
	if err != nil {
		logger.Fatalf("Failed to load config: %v", err)
	}

	// Initialize K8s clients for all kubeconfig locations
	k8sClients := make(map[string]*k8s.Client)
	for _, kubeconfigLocation := range cfg.KubeconfigLocations {
		client, err := k8s.NewClient(kubeconfigLocation)
		if err != nil {
			logger.Fatalf("Failed to initialize Kubernetes client: %v", err)
		}
		clientHost := client.Config.Host
		k8sClients[clientHost] = client
	}

	outbox := cluster.NewCentralizedOutbox(ctx, k8sClients)
	cluster.Conduct(ctx, cfg.Tasks, outbox)
}

func runDistributedMode(ctx context.Context, cancel context.CancelFunc) {
	if len(os.Args) < 3 {
		logger.Fatal("Config path is required")
	}
	configPath := os.Args[2]

	cfg, err := config.LoadDistributed(configPath)
	if err != nil {
		logger.Fatalf("Failed to load config: %v", err)
	}

	// Initialize leader election
	elector, etcdClient, err := cluster.NewLeaderElector(cluster.LeaderConfig{
		EtcdEndpoints: cfg.EtcdEndpoints,
		Name:          cfg.Name,
		LeaderFn: func(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client) error {
			outbox := cluster.NewDistributedOutbox(ctx, epoch, epochKey, client, cfg.Tasks)
			cluster.Conduct(ctx, cfg.Tasks, outbox)
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

	// Initialize validators
	epochValidator := server.NewEpochValidator(etcdClient)
	idempotencyValidator := server.NewIdempotencyValidator(etcdClient, cfg.Name)

	// Initialize HTTP Server
	srvHandler := server.NewHandler(k8sClient, epochValidator, idempotencyValidator, etcdClient, cfg.Name)
	srv := server.NewServer(server.Config{Port: cfg.Server.Port}, srvHandler)

	// Start server in background
	go func() {
		logger.Infof("Starting HTTP server on port %d", cfg.Server.Port)
		if err := srv.Run(ctx); err != nil {
			logger.Errorf("HTTP server error: %v", err)
			cancel()
		}
	}()

	logger.Infof("Participating in leader election (ID: %s)...", cfg.Name)
	if err := elector.Run(ctx); err != nil {
		if err != context.Canceled {
			logger.Fatalf("Leader election error: %v", err)
		}
		logger.Info("Leader election stopped")
	}
}
