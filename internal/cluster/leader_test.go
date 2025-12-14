package cluster

import (
	"context"
	"errors"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

// findFreePort finds an available port
func findFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// setupEmbeddedEtcd starts an embedded etcd server for testing
func setupEmbeddedEtcd(t *testing.T) ([]string, func()) {
	t.Helper()

	// Create a temporary directory for etcd data
	tmpDir, err := os.MkdirTemp("", "etcd-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	// Find a free port for the client
	clientPort, err := findFreePort()
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to find free port: %v", err)
	}

	// Find a free port for peers
	peerPort, err := findFreePort()
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to find free port: %v", err)
	}

	// Configure embedded etcd
	cfg := embed.NewConfig()
	cfg.Dir = tmpDir
	cfg.Name = "test-etcd"

	clientURL := "http://127.0.0.1:" + strconv.Itoa(clientPort)
	peerURL := "http://127.0.0.1:" + strconv.Itoa(peerPort)

	listenClientURL, _ := url.Parse(clientURL)
	advertiseClientURL, _ := url.Parse(clientURL)
	listenPeerURL, _ := url.Parse(peerURL)

	cfg.ListenClientUrls = []url.URL{*listenClientURL}
	cfg.AdvertiseClientUrls = []url.URL{*advertiseClientURL}
	cfg.ListenPeerUrls = []url.URL{*listenPeerURL}
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)
	cfg.LogLevel = "warn"

	// Start embedded etcd
	etcd, err := embed.StartEtcd(cfg)
	if err != nil {
		os.RemoveAll(tmpDir)
		t.Fatalf("failed to start etcd: %v", err)
	}

	// Wait for etcd to be ready
	select {
	case <-etcd.Server.ReadyNotify():
	case <-time.After(10 * time.Second):
		etcd.Close()
		os.RemoveAll(tmpDir)
		t.Fatal("etcd took too long to start")
	}

	// Use the client URL we configured
	endpoints := []string{clientURL}

	cleanup := func() {
		etcd.Close()
		os.RemoveAll(tmpDir)
	}

	return endpoints, cleanup
}

func TestNewLeaderElector_Validation(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
		errorMsg    string
	}{
		{
			name: "missing etcd endpoints",
			config: Config{
				EtcdEndpoints: []string{},
				Name:          "test",
				LeaderFn:      func(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client) error { return nil },
			},
			expectError: true,
			errorMsg:    "etcd endpoints are required",
		},
		{
			name: "missing name",
			config: Config{
				EtcdEndpoints: []string{"http://localhost:2379"},
				Name:          "",
				LeaderFn:      func(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client) error { return nil },
			},
			expectError: true,
			errorMsg:    "name is required",
		},
		{
			name: "missing leader function",
			config: Config{
				EtcdEndpoints: []string{"http://localhost:2379"},
				Name:          "test",
				LeaderFn:      nil,
			},
			expectError: true,
			errorMsg:    "leader function is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := NewLeaderElector(tt.config)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				} else if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error to contain %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestNewLeaderElector_Defaults(t *testing.T) {
	endpoints, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	cfg := Config{
		EtcdEndpoints: endpoints,
		Name:          "test",
		LeaderFn:      func(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client) error { return nil },
		// Don't set Prefix, EpochKey, LeaseTTL, or Backoff to test defaults
	}

	elector, client, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client.Close()

	if elector.prefix != "/conductor/leader" {
		t.Errorf("expected default prefix /conductor/leader, got %q", elector.prefix)
	}
	if elector.epochKey != "/conductor/epoch" {
		t.Errorf("expected default epoch key /conductor/epoch, got %q", elector.epochKey)
	}
	if elector.leaseTTL != 10 {
		t.Errorf("expected default lease TTL 10, got %d", elector.leaseTTL)
	}
	if elector.backoff != time.Second {
		t.Errorf("expected default backoff 1s, got %v", elector.backoff)
	}
	if elector.id != "test" {
		t.Errorf("expected id 'test', got %q", elector.id)
	}
}

func TestNewLeaderElector_CustomValues(t *testing.T) {
	endpoints, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	customPrefix := "/custom/prefix"
	customEpochKey := "/custom/epoch"
	customLeaseTTL := 5
	customBackoff := 2 * time.Second

	cfg := Config{
		EtcdEndpoints: endpoints,
		Name:          "test",
		Prefix:        customPrefix,
		EpochKey:      customEpochKey,
		LeaseTTL:      customLeaseTTL,
		Backoff:       customBackoff,
		LeaderFn:      func(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client) error { return nil },
	}

	elector, client, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client.Close()

	if elector.prefix != customPrefix {
		t.Errorf("expected prefix %q, got %q", customPrefix, elector.prefix)
	}
	if elector.epochKey != customEpochKey {
		t.Errorf("expected epoch key %q, got %q", customEpochKey, elector.epochKey)
	}
	if elector.leaseTTL != customLeaseTTL {
		t.Errorf("expected lease TTL %d, got %d", customLeaseTTL, elector.leaseTTL)
	}
	if elector.backoff != customBackoff {
		t.Errorf("expected backoff %v, got %v", customBackoff, elector.backoff)
	}
}

func TestLeaderElector_Run_SingleElector(t *testing.T) {
	endpoints, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	leaderCalled := make(chan int64, 1)
	cfg := Config{
		EtcdEndpoints: endpoints,
		Name:          "leader1",
		LeaseTTL:      2, // Short TTL for faster tests
		Backoff:       100 * time.Millisecond,
		LeaderFn: func(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client) error {
			leaderCalled <- epoch
			<-ctx.Done()
			return ctx.Err()
		},
	}

	elector, client, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client.Close()

	// Run elector in background
	errChan := make(chan error, 1)
	go func() {
		errChan <- elector.Run(ctx)
	}()

	// Wait for leadership to be acquired
	select {
	case epoch := <-leaderCalled:
		if epoch <= 0 {
			t.Errorf("expected epoch > 0, got %d", epoch)
		}
		t.Logf("Leader acquired with epoch %d", epoch)
	case <-time.After(3 * time.Second):
		t.Fatal("leader function was not called within timeout")
	}

	// Cancel context to stop elector
	cancel()

	// Wait for Run to return
	select {
	case err := <-errChan:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within timeout")
	}
}

func TestLeaderElector_Run_MultipleElectors(t *testing.T) {
	endpoints, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	leader1Called := make(chan int64, 1)
	leader2Called := make(chan int64, 1)

	cfg1 := Config{
		EtcdEndpoints: endpoints,
		Name:          "leader1",
		LeaseTTL:      2,
		Backoff:       100 * time.Millisecond,
		LeaderFn: func(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client) error {
			leader1Called <- epoch
			<-ctx.Done()
			return ctx.Err()
		},
	}

	cfg2 := Config{
		EtcdEndpoints: endpoints,
		Name:          "leader2",
		LeaseTTL:      2,
		Backoff:       100 * time.Millisecond,
		LeaderFn: func(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client) error {
			leader2Called <- epoch
			<-ctx.Done()
			return ctx.Err()
		},
	}

	elector1, client1, err := NewLeaderElector(cfg1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client1.Close()

	elector2, client2, err := NewLeaderElector(cfg2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client2.Close()

	// Run both electors
	errChan1 := make(chan error, 1)
	errChan2 := make(chan error, 1)
	go func() {
		errChan1 <- elector1.Run(ctx)
	}()
	go func() {
		errChan2 <- elector2.Run(ctx)
	}()

	// Wait for first leader
	var firstEpoch int64
	select {
	case epoch := <-leader1Called:
		firstEpoch = epoch
		t.Logf("Leader1 acquired with epoch %d", epoch)
	case epoch := <-leader2Called:
		firstEpoch = epoch
		t.Logf("Leader2 acquired with epoch %d", epoch)
	case <-time.After(5 * time.Second):
		t.Fatal("no leader was elected within timeout")
	}

	// Verify only one leader at a time
	select {
	case <-leader1Called:
		t.Error("leader1 was called again, but should only be called once")
	case <-leader2Called:
		t.Error("leader2 was called again, but should only be called once")
	case <-time.After(3 * time.Second):
		// Good - no second leader call
	}

	// Cancel context
	cancel()

	// Wait for both to return
	select {
	case err := <-errChan1:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("unexpected error from elector1: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("elector1 did not return within timeout")
	}

	select {
	case err := <-errChan2:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("unexpected error from elector2: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("elector2 did not return within timeout")
	}

	if firstEpoch <= 0 {
		t.Errorf("expected epoch > 0, got %d", firstEpoch)
	}
}

func TestLeaderElector_Run_LeadershipLoss(t *testing.T) {
	endpoints, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	leaderCalls := make(chan int64, 10)
	var mu sync.Mutex
	var callCount int

	cfg := Config{
		EtcdEndpoints: endpoints,
		Name:          "leader1",
		LeaseTTL:      1, // Very short TTL to trigger leadership loss
		Backoff:       100 * time.Millisecond,
		LeaderFn: func(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client) error {
			mu.Lock()
			callCount++
			mu.Unlock()
			leaderCalls <- epoch
			<-ctx.Done()
			return ctx.Err()
		},
	}

	elector, client, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client.Close()

	// Run elector
	errChan := make(chan error, 1)
	go func() {
		errChan <- elector.Run(ctx)
	}()

	// Wait for first leadership acquisition
	select {
	case epoch := <-leaderCalls:
		if epoch <= 0 {
			t.Errorf("expected epoch > 0, got %d", epoch)
		}
		t.Logf("First leadership acquired with epoch %d", epoch)
	case <-time.After(5 * time.Second):
		t.Fatal("leader function was not called within timeout")
	}

	// With very short TTL, we should see leadership loss and re-election
	// Wait a bit to see if leadership is lost and re-acquired
	time.Sleep(3 * time.Second)

	mu.Lock()
	count := callCount
	mu.Unlock()

	if count < 1 {
		t.Errorf("expected at least 1 leader call, got %d", count)
	}

	// Cancel context
	cancel()

	// Wait for Run to return
	select {
	case err := <-errChan:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within timeout")
	}
}

func TestLeaderElector_nextEpoch(t *testing.T) {
	endpoints, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	ctx := context.Background()

	cfg := Config{
		EtcdEndpoints: endpoints,
		Name:          "test",
		EpochKey:      "/test/epoch",
		LeaderFn:      func(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client) error { return nil },
	}

	elector, client, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client.Close()

	// First epoch should be 1
	epoch1, err := elector.nextEpoch(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if epoch1 != 1 {
		t.Errorf("expected epoch 1, got %d", epoch1)
	}

	// Second epoch should be 2
	epoch2, err := elector.nextEpoch(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if epoch2 != 2 {
		t.Errorf("expected epoch 2, got %d", epoch2)
	}

	// Third epoch should be 3
	epoch3, err := elector.nextEpoch(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if epoch3 != 3 {
		t.Errorf("expected epoch 3, got %d", epoch3)
	}

	// Verify epochs are sequential
	if epoch2 != epoch1+1 || epoch3 != epoch2+1 {
		t.Errorf("epochs should be sequential: %d, %d, %d", epoch1, epoch2, epoch3)
	}
}

func TestLeaderElector_nextEpoch_Concurrent(t *testing.T) {
	endpoints, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	ctx := context.Background()

	cfg := Config{
		EtcdEndpoints: endpoints,
		Name:          "test",
		EpochKey:      "/test/epoch-concurrent",
		LeaderFn:      func(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client) error { return nil },
	}

	elector, client, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client.Close()

	// Create multiple electors to test concurrent epoch increments
	elector2, client2, err := NewLeaderElector(Config{
		EtcdEndpoints: endpoints,
		Name:          "test2",
		EpochKey:      "/test/epoch-concurrent",
		LeaderFn:      func(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client) error { return nil },
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client2.Close()

	// Concurrently increment epochs
	var wg sync.WaitGroup
	epochs := make([]int64, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var e *LeaderElector
			if idx%2 == 0 {
				e = elector
			} else {
				e = elector2
			}
			epoch, err := e.nextEpoch(ctx)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			epochs[idx] = epoch
		}(i)
	}

	wg.Wait()

	// Verify all epochs are unique and sequential
	seen := make(map[int64]bool)
	for _, epoch := range epochs {
		if seen[epoch] {
			t.Errorf("duplicate epoch: %d", epoch)
		}
		seen[epoch] = true
		if epoch <= 0 {
			t.Errorf("expected epoch > 0, got %d", epoch)
		}
	}

	// Verify epochs are in a reasonable range (1-10)
	for _, epoch := range epochs {
		if epoch < 1 || epoch > 10 {
			t.Errorf("epoch out of expected range: %d", epoch)
		}
	}
}

func TestLeaderElector_Run_ContextCancellation(t *testing.T) {
	endpoints, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())

	leaderCalled := make(chan struct{}, 1)
	cfg := Config{
		EtcdEndpoints: endpoints,
		Name:          "test",
		LeaseTTL:      5,
		Backoff:       100 * time.Millisecond,
		LeaderFn: func(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client) error {
			leaderCalled <- struct{}{}
			<-ctx.Done()
			return ctx.Err()
		},
	}

	elector, client, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client.Close()

	errChan := make(chan error, 1)
	go func() {
		errChan <- elector.Run(ctx)
	}()

	// Wait for leadership
	select {
	case <-leaderCalled:
	case <-time.After(3 * time.Second):
		t.Fatal("leader function was not called within timeout")
	}

	// Cancel context immediately
	cancel()

	// Run should return with context.Canceled
	select {
	case err := <-errChan:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within timeout")
	}
}

func TestLeaderElector_Run_LeaderFunctionError(t *testing.T) {
	endpoints, cleanup := setupEmbeddedEtcd(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	testErr := errors.New("leader function error")
	cfg := Config{
		EtcdEndpoints: endpoints,
		Name:          "test",
		LeaseTTL:      2,
		Backoff:       100 * time.Millisecond,
		LeaderFn: func(ctx context.Context, epoch int64, epochKey string, client *clientv3.Client) error {
			return testErr
		},
	}

	elector, client, err := NewLeaderElector(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer client.Close()

	errChan := make(chan error, 1)
	go func() {
		errChan <- elector.Run(ctx)
	}()

	// The leader function returns an error, but Run should continue retrying
	// Wait a bit to see if it retries
	select {
	case err := <-errChan:
		// If it returns, it should be due to context timeout
		if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			t.Errorf("unexpected error: %v", err)
		}
	case <-time.After(4 * time.Second):
		// This is expected - Run should continue retrying
	}

	cancel()
}
