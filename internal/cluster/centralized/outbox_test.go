package centralized

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/esadakcam/conductor/internal/k8s"
	"github.com/esadakcam/conductor/internal/task"
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
	tmpDir, err := os.MkdirTemp("", "etcd-centralized-test-*")
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
	cfg.Name = "test-etcd-centralized"

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

func setupOutboxTest(t *testing.T) (*clientv3.Client, func()) {
	t.Helper()

	endpoints, cleanup := setupEmbeddedEtcd(t)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		cleanup()
		t.Fatalf("failed to create etcd client: %v", err)
	}

	return client, func() {
		client.Close()
		cleanup()
	}
}

// mockTask implements task.TaskInterface for testing
type mockTask struct {
	name string
	when []task.Condition
	then []task.Action
}

func (m *mockTask) GetName() string {
	return m.name
}

func (m *mockTask) GetConditions() []task.Condition {
	return m.when
}

func (m *mockTask) GetActions() []task.Action {
	return m.then
}

// mockAction implements task.Action for testing
type mockAction struct {
	executeCalled *atomic.Int32
	executeErr    error
	executeFn     func(ctx context.Context, ec task.ExecutionContext) error
}

func newMockAction() *mockAction {
	return &mockAction{
		executeCalled: &atomic.Int32{},
	}
}

func (m *mockAction) Execute(ctx context.Context, ec task.ExecutionContext) error {
	m.executeCalled.Add(1)
	if m.executeFn != nil {
		return m.executeFn(ctx, ec)
	}
	return m.executeErr
}

func (m *mockAction) GetType() task.ActionType {
	return task.ActionTypeEcho
}

func TestNewOutbox(t *testing.T) {
	client, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()
	k8sClients := make(map[string]*k8s.Client)

	tasks := []task.TaskInterface{
		&mockTask{name: "task1", then: []task.Action{newMockAction()}},
		&mockTask{name: "task2", then: []task.Action{newMockAction()}},
	}

	outbox := NewOutbox(ctx, client, k8sClients, tasks)

	if outbox == nil {
		t.Fatal("expected outbox to not be nil")
	}
	if outbox.client != client {
		t.Error("expected client to match")
	}
	if outbox.executingTasks == nil {
		t.Error("expected executingTasks to be initialized")
	}
	if outbox.k8sClients == nil {
		t.Error("expected k8sClients to be initialized")
	}
}

func TestOutbox_IsTaskExecuting_NotExecuting(t *testing.T) {
	client, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	outbox := NewOutbox(ctx, client, nil, nil)

	// Task that doesn't exist should not be executing
	if outbox.IsTaskExecuting(ctx, "nonexistent-task") {
		t.Error("expected task to not be executing")
	}
}

func TestOutbox_IsTaskExecuting_Executing(t *testing.T) {
	client, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	outbox := NewOutbox(ctx, client, nil, nil)

	// Manually add a task to the outbox key
	item := OutboxItem{
		CreatedAt: time.Now(),
		TaskStep:  0,
	}
	jsonData, _ := json.Marshal(item)
	_, err := client.Put(ctx, fmt.Sprintf("%s/%s", outboxKey, "test-task"), string(jsonData))
	if err != nil {
		t.Fatalf("failed to put task in outbox: %v", err)
	}

	// Task should be executing
	if !outbox.IsTaskExecuting(ctx, "test-task") {
		t.Error("expected task to be executing")
	}

	// Should also be cached
	outbox.mu.Lock()
	val := outbox.executingTasks["test-task"]
	outbox.mu.Unlock()
	if !val {
		t.Error("expected task to be cached as executing")
	}
}

func TestOutbox_IsTaskExecuting_CachedValue(t *testing.T) {
	client, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	outbox := NewOutbox(ctx, client, nil, nil)

	// Manually set cached value
	outbox.mu.Lock()
	outbox.executingTasks["cached-task"] = true
	outbox.mu.Unlock()

	// Should return cached value without querying etcd
	if !outbox.IsTaskExecuting(ctx, "cached-task") {
		t.Error("expected task to be executing from cache")
	}
}

func TestOutbox_ExecuteTask_NewTask(t *testing.T) {
	client, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	action := newMockAction()
	testTask := &mockTask{
		name: "new-task",
		then: []task.Action{action},
	}

	outbox := NewOutbox(ctx, client, nil, nil)

	err := outbox.ExecuteTask(testTask)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Give some time for the task to execute
	time.Sleep(100 * time.Millisecond)

	// Verify action was executed
	if action.executeCalled.Load() != 1 {
		t.Errorf("expected action to be executed once, got %d", action.executeCalled.Load())
	}

	// Task should no longer be in outbox after completion
	resp, err := client.Get(ctx, fmt.Sprintf("%s/%s", outboxKey, "new-task"))
	if err != nil {
		t.Fatalf("failed to get task from outbox: %v", err)
	}
	if len(resp.Kvs) != 0 {
		t.Error("expected task to be removed from outbox after completion")
	}
}

func TestOutbox_ExecuteTask_AlreadyExecuting(t *testing.T) {
	client, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	action := newMockAction()
	testTask := &mockTask{
		name: "already-executing-task",
		then: []task.Action{action},
	}

	outbox := NewOutbox(ctx, client, nil, nil)

	// Mark task as already executing
	outbox.mu.Lock()
	outbox.executingTasks["already-executing-task"] = true
	outbox.mu.Unlock()

	err := outbox.ExecuteTask(testTask)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Action should not be executed
	if action.executeCalled.Load() != 0 {
		t.Errorf("expected action to not be executed, got %d", action.executeCalled.Load())
	}
}

func TestOutbox_ExecuteTask_MultipleSteps(t *testing.T) {
	client, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	action1 := newMockAction()
	action2 := newMockAction()
	action3 := newMockAction()

	testTask := &mockTask{
		name: "multi-step-task",
		then: []task.Action{action1, action2, action3},
	}

	outbox := NewOutbox(ctx, client, nil, nil)

	err := outbox.ExecuteTask(testTask)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Give some time for the task to execute
	time.Sleep(200 * time.Millisecond)

	// All actions should be executed
	if action1.executeCalled.Load() != 1 {
		t.Errorf("expected action1 to be executed once, got %d", action1.executeCalled.Load())
	}
	if action2.executeCalled.Load() != 1 {
		t.Errorf("expected action2 to be executed once, got %d", action2.executeCalled.Load())
	}
	if action3.executeCalled.Load() != 1 {
		t.Errorf("expected action3 to be executed once, got %d", action3.executeCalled.Load())
	}

	// Task should be removed from outbox after completion
	resp, err := client.Get(ctx, fmt.Sprintf("%s/%s", outboxKey, "multi-step-task"))
	if err != nil {
		t.Fatalf("failed to get task from outbox: %v", err)
	}
	if len(resp.Kvs) != 0 {
		t.Error("expected task to be removed from outbox after completion")
	}

	// Task should no longer be marked as executing
	outbox.mu.Lock()
	val := outbox.executingTasks["multi-step-task"]
	outbox.mu.Unlock()
	if val {
		t.Error("expected task to no longer be marked as executing")
	}
}

func TestOutbox_ExecuteTask_StepError_WithRetry(t *testing.T) {
	client, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	action1 := newMockAction()
	action2 := newMockAction()
	action2.executeErr = fmt.Errorf("step 2 error")
	action3 := newMockAction()

	testTask := &mockTask{
		name: "error-task",
		then: []task.Action{action1, action2, action3},
	}

	outbox := NewOutbox(ctx, client, nil, nil)

	// ExecuteTask blocks and returns error after all retries fail
	err := outbox.ExecuteTask(testTask)
	if err == nil {
		t.Fatal("expected error after retries exhausted")
	}

	// Action1 should be executed once
	if action1.executeCalled.Load() != 1 {
		t.Errorf("expected action1 to be executed once, got %d", action1.executeCalled.Load())
	}
	// Action2 should be attempted 4 times (1 initial + 3 retries with maxRetries=3)
	if action2.executeCalled.Load() != 4 {
		t.Errorf("expected action2 to be executed 4 times (with retries), got %d", action2.executeCalled.Load())
	}
	// Action3 should not be executed since action2 always fails
	if action3.executeCalled.Load() != 0 {
		t.Errorf("expected action3 to not be executed, got %d", action3.executeCalled.Load())
	}
}

func TestOutbox_Init_WithExistingTasks(t *testing.T) {
	client, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	// Pre-add a task to the outbox
	item := OutboxItem{
		CreatedAt: time.Now(),
		TaskStep:  0,
	}
	jsonData, _ := json.Marshal(item)
	_, err := client.Put(ctx, fmt.Sprintf("%s/%s", outboxKey, "existing-task"), string(jsonData))
	if err != nil {
		t.Fatalf("failed to put task in outbox: %v", err)
	}

	action := newMockAction()
	tasks := []task.TaskInterface{
		&mockTask{name: "existing-task", then: []task.Action{action}},
	}

	// NewOutbox should pick up the existing task and execute it
	_ = NewOutbox(ctx, client, nil, tasks)

	// Give some time for the task to execute
	time.Sleep(200 * time.Millisecond)

	// Action should be executed
	if action.executeCalled.Load() != 1 {
		t.Errorf("expected action to be executed once, got %d", action.executeCalled.Load())
	}

	// Task should be removed from outbox after completion
	resp, err := client.Get(ctx, fmt.Sprintf("%s/%s", outboxKey, "existing-task"))
	if err != nil {
		t.Fatalf("failed to get task from outbox: %v", err)
	}
	if len(resp.Kvs) != 0 {
		t.Error("expected task to be removed from outbox after completion")
	}
}

func TestOutbox_Init_ResumeFromStep(t *testing.T) {
	client, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	// Pre-add a task to the outbox at step 1 (already completed step 0)
	item := OutboxItem{
		CreatedAt: time.Now(),
		TaskStep:  1,
	}
	jsonData, _ := json.Marshal(item)
	_, err := client.Put(ctx, fmt.Sprintf("%s/%s", outboxKey, "resume-task"), string(jsonData))
	if err != nil {
		t.Fatalf("failed to put task in outbox: %v", err)
	}

	action0 := newMockAction()
	action1 := newMockAction()
	action2 := newMockAction()

	tasks := []task.TaskInterface{
		&mockTask{name: "resume-task", then: []task.Action{action0, action1, action2}},
	}

	// NewOutbox should resume from step 1
	_ = NewOutbox(ctx, client, nil, tasks)

	// Give some time for the task to execute
	time.Sleep(200 * time.Millisecond)

	// Action0 should NOT be executed (already done), action1 and action2 should be
	if action0.executeCalled.Load() != 0 {
		t.Errorf("expected action0 to not be executed, got %d", action0.executeCalled.Load())
	}
	if action1.executeCalled.Load() != 1 {
		t.Errorf("expected action1 to be executed once, got %d", action1.executeCalled.Load())
	}
	if action2.executeCalled.Load() != 1 {
		t.Errorf("expected action2 to be executed once, got %d", action2.executeCalled.Load())
	}
}

func TestOutbox_ExecuteTask_ConcurrentSameTask(t *testing.T) {
	client, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	var executionCount atomic.Int32
	action := newMockAction()
	action.executeFn = func(ctx context.Context, ec task.ExecutionContext) error {
		executionCount.Add(1)
		time.Sleep(50 * time.Millisecond) // Simulate some work
		return nil
	}

	testTask := &mockTask{
		name: "concurrent-task",
		then: []task.Action{action},
	}

	outbox := NewOutbox(ctx, client, nil, nil)

	// Try to add the same task concurrently
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			outbox.ExecuteTask(testTask)
		}()
	}

	wg.Wait()

	// Give some time for all tasks to complete
	time.Sleep(300 * time.Millisecond)

	// Action should only be executed once due to idempotency
	if executionCount.Load() != 1 {
		t.Errorf("expected action to be executed once, got %d", executionCount.Load())
	}
}

func TestOutbox_IdempotencyKey_Uniqueness(t *testing.T) {
	client, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	var idempotencyIds []string
	var mu sync.Mutex

	action := newMockAction()
	action.executeFn = func(ctx context.Context, ec task.ExecutionContext) error {
		mu.Lock()
		idempotencyIds = append(idempotencyIds, ec.GetIdempotencyKey())
		mu.Unlock()
		return nil
	}

	testTask := &mockTask{
		name: "idempotency-task",
		then: []task.Action{action},
	}

	outbox := NewOutbox(ctx, client, nil, nil)
	outbox.ExecuteTask(testTask)

	time.Sleep(100 * time.Millisecond)

	// Reset and add same task again
	outbox.mu.Lock()
	outbox.executingTasks["idempotency-task"] = false
	outbox.mu.Unlock()

	outbox.ExecuteTask(testTask)

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(idempotencyIds) < 2 {
		t.Fatalf("expected at least 2 idempotency IDs, got %d", len(idempotencyIds))
	}

	// Each execution should have a unique ID
	if idempotencyIds[0] == idempotencyIds[1] {
		t.Error("expected different idempotency IDs for different executions")
	}

	// Each ID should be a valid UUID
	for _, id := range idempotencyIds {
		if len(id) != 36 { // UUID format: 8-4-4-4-12
			t.Errorf("expected valid UUID format, got %q", id)
		}
	}
}

func TestOutbox_IdempotencyKey_PassedToActions(t *testing.T) {
	client, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	var receivedKey string
	action := newMockAction()
	action.executeFn = func(ctx context.Context, ec task.ExecutionContext) error {
		receivedKey = ec.GetIdempotencyKey()
		return nil
	}

	testTask := &mockTask{
		name: "key-test-task",
		then: []task.Action{action},
	}

	outbox := NewOutbox(ctx, client, nil, nil)
	outbox.ExecuteTask(testTask)

	time.Sleep(100 * time.Millisecond)

	// Verify idempotency key was passed to action
	if receivedKey == "" {
		t.Error("expected idempotency key to be passed to action")
	}

	// Verify it's a valid UUID
	if len(receivedKey) != 36 {
		t.Errorf("expected valid UUID format, got %q", receivedKey)
	}
}

func TestOutbox_EmptyTask(t *testing.T) {
	client, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	// Task with no actions
	testTask := &mockTask{
		name: "empty-task",
		then: []task.Action{},
	}

	outbox := NewOutbox(ctx, client, nil, nil)

	err := outbox.ExecuteTask(testTask)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Give some time
	time.Sleep(100 * time.Millisecond)

	// Task should still be added and then immediately completed
	outbox.mu.Lock()
	val := outbox.executingTasks["empty-task"]
	outbox.mu.Unlock()
	if val {
		t.Error("expected empty task to be completed quickly")
	}
}

func TestOutbox_GetExecutionContext(t *testing.T) {
	client, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()
	k8sClients := make(map[string]*k8s.Client)

	outbox := NewOutbox(ctx, client, k8sClients, nil)

	ec := outbox.GetExecutionContext()

	// GetExecutionContext for conditions should return empty idempotency key
	if ec.GetIdempotencyKey() != "" {
		t.Error("expected empty idempotency key for condition evaluation context")
	}

	// k8s clients should be available
	if ec.GetK8sClients() == nil {
		t.Error("expected k8s clients to be available")
	}
}

func TestExecuteWithRetry_Success(t *testing.T) {
	ctx := context.Background()

	called := 0
	err := executeWithRetry(ctx, "test-op", func() error {
		called++
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if called != 1 {
		t.Errorf("expected function to be called once, got %d", called)
	}
}

func TestExecuteWithRetry_EventualSuccess(t *testing.T) {
	ctx := context.Background()

	called := 0
	err := executeWithRetry(ctx, "test-op", func() error {
		called++
		if called < 3 {
			return fmt.Errorf("temporary error")
		}
		return nil
	})

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if called != 3 {
		t.Errorf("expected function to be called 3 times, got %d", called)
	}
}

func TestExecuteWithRetry_MaxRetries(t *testing.T) {
	ctx := context.Background()

	called := 0
	err := executeWithRetry(ctx, "test-op", func() error {
		called++
		return fmt.Errorf("persistent error")
	})

	if err == nil {
		t.Error("expected error after max retries")
	}
	// maxRetries = 3, so total calls = 1 initial + 3 retries = 4
	if called != 4 {
		t.Errorf("expected function to be called 4 times (1 + 3 retries), got %d", called)
	}
}

func TestExecuteWithRetry_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	called := 0
	errChan := make(chan error, 1)

	go func() {
		errChan <- executeWithRetry(ctx, "test-op", func() error {
			called++
			return fmt.Errorf("error")
		})
	}()

	// Let it start
	time.Sleep(50 * time.Millisecond)

	// Cancel during backoff
	cancel()

	select {
	case err := <-errChan:
		if err != context.Canceled {
			// Either context.Canceled or the last error
			t.Logf("got error: %v (called %d times)", err, called)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("executeWithRetry did not return within timeout")
	}
}
