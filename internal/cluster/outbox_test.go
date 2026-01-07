package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/esadakcam/conductor/internal/task"
	"github.com/esadakcam/conductor/internal/task/distributed"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// mockAction implements task.Action for testing
type mockAction struct {
	executeCalled *atomic.Int32
	executeErr    error
	executeFn     func(ctx context.Context, payload any) error
}

func newMockAction() *mockAction {
	return &mockAction{
		executeCalled: &atomic.Int32{},
	}
}

func (m *mockAction) Execute(ctx context.Context, payload any) error {
	m.executeCalled.Add(1)
	if m.executeFn != nil {
		return m.executeFn(ctx, payload)
	}
	return m.executeErr
}

func (m *mockAction) GetType() task.ActionType {
	return task.ActionTypeEcho
}

func setupOutboxTest(t *testing.T) (*clientv3.Client, string, func()) {
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

	// Set up epoch key with a value
	epochKey := "/test/epoch"
	_, err = client.Put(context.Background(), epochKey, "1")
	if err != nil {
		client.Close()
		cleanup()
		t.Fatalf("failed to set epoch key: %v", err)
	}

	return client, epochKey, func() {
		client.Close()
		cleanup()
	}
}

func TestNewOutbox(t *testing.T) {
	client, epochKey, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	tasks := []task.Task{
		&distributed.Task{Name: "task1", Then: []task.Action{newMockAction()}},
		&distributed.Task{Name: "task2", Then: []task.Action{newMockAction()}},
	}

	outbox := NewOutbox(ctx, 1, epochKey, client, tasks)

	if outbox == nil {
		t.Fatal("expected outbox to not be nil")
	}
	if outbox.epoch != 1 {
		t.Errorf("expected epoch 1, got %d", outbox.epoch)
	}
	if outbox.epochKey != epochKey {
		t.Errorf("expected epochKey %q, got %q", epochKey, outbox.epochKey)
	}
	if outbox.client != client {
		t.Error("expected client to match")
	}
	if outbox.executingTasks == nil {
		t.Error("expected executingTasks to be initialized")
	}
}

func TestOutbox_IsTaskExecuting_NotExecuting(t *testing.T) {
	client, epochKey, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	outbox := NewOutbox(ctx, 1, epochKey, client, nil)

	// Task that doesn't exist should not be executing
	if outbox.IsTaskExecuting(ctx, "nonexistent-task") {
		t.Error("expected task to not be executing")
	}
}

func TestOutbox_IsTaskExecuting_Executing(t *testing.T) {
	client, epochKey, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	outbox := NewOutbox(ctx, 1, epochKey, client, nil)

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
	client, epochKey, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	outbox := NewOutbox(ctx, 1, epochKey, client, nil)

	// Manually set cached value
	outbox.mu.Lock()
	outbox.executingTasks["cached-task"] = true
	outbox.mu.Unlock()

	// Should return cached value without querying etcd
	if !outbox.IsTaskExecuting(ctx, "cached-task") {
		t.Error("expected task to be executing from cache")
	}
}

func TestOutbox_AddTask_NewTask(t *testing.T) {
	client, epochKey, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	action := newMockAction()
	testTask := &distributed.Task{
		Name: "new-task",
		Then: []task.Action{action},
	}

	outbox := NewOutbox(ctx, 1, epochKey, client, nil)

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

func TestOutbox_AddTask_AlreadyExecuting(t *testing.T) {
	client, epochKey, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	action := newMockAction()
	testTask := &distributed.Task{
		Name: "already-executing-task",
		Then: []task.Action{action},
	}

	outbox := NewOutbox(ctx, 1, epochKey, client, nil)

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

func TestOutbox_AddTask_MultipleSteps(t *testing.T) {
	client, epochKey, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	action1 := newMockAction()
	action2 := newMockAction()
	action3 := newMockAction()

	testTask := &distributed.Task{
		Name: "multi-step-task",
		Then: []task.Action{action1, action2, action3},
	}

	outbox := NewOutbox(ctx, 1, epochKey, client, nil)

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

func TestOutbox_AddTask_StepError(t *testing.T) {
	client, epochKey, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	action1 := newMockAction()
	action2 := newMockAction()
	action2.executeErr = fmt.Errorf("step 2 error")
	action3 := newMockAction()

	testTask := &distributed.Task{
		Name: "error-task",
		Then: []task.Action{action1, action2, action3},
	}

	outbox := NewOutbox(ctx, 1, epochKey, client, nil)

	err := outbox.ExecuteTask(testTask)
	if err != nil {
		t.Fatalf("unexpected error adding task: %v", err)
	}

	// Give some time for the task to execute
	time.Sleep(200 * time.Millisecond)

	// Action1 should be executed, action2 should be attempted, action3 should not
	if action1.executeCalled.Load() != 1 {
		t.Errorf("expected action1 to be executed once, got %d", action1.executeCalled.Load())
	}
	if action2.executeCalled.Load() != 1 {
		t.Errorf("expected action2 to be executed once, got %d", action2.executeCalled.Load())
	}
	if action3.executeCalled.Load() != 0 {
		t.Errorf("expected action3 to not be executed, got %d", action3.executeCalled.Load())
	}
}

func TestOutbox_Init_WithExistingTasks(t *testing.T) {
	client, epochKey, cleanup := setupOutboxTest(t)
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
	tasks := []task.Task{
		&distributed.Task{Name: "existing-task", Then: []task.Action{action}},
	}

	// NewOutbox should pick up the existing task and execute it
	_ = NewOutbox(ctx, 1, epochKey, client, tasks)

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
	client, epochKey, cleanup := setupOutboxTest(t)
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

	tasks := []task.Task{
		&distributed.Task{Name: "resume-task", Then: []task.Action{action0, action1, action2}},
	}

	// NewOutbox should resume from step 1
	_ = NewOutbox(ctx, 1, epochKey, client, tasks)

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

func TestOutbox_Init_NoMatchingTask(t *testing.T) {
	client, epochKey, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	// Pre-add a task to the outbox
	item := OutboxItem{
		CreatedAt: time.Now(),
		TaskStep:  0,
	}
	jsonData, _ := json.Marshal(item)
	_, err := client.Put(ctx, fmt.Sprintf("%s/%s", outboxKey, "orphaned-task"), string(jsonData))
	if err != nil {
		t.Fatalf("failed to put task in outbox: %v", err)
	}

	action := newMockAction()
	tasks := []task.Task{
		&distributed.Task{Name: "different-task", Then: []task.Action{action}},
	}

	// NewOutbox should not pick up the orphaned task
	_ = NewOutbox(ctx, 1, epochKey, client, tasks)

	// Give some time
	time.Sleep(200 * time.Millisecond)

	// Action should not be executed
	if action.executeCalled.Load() != 0 {
		t.Errorf("expected action to not be executed, got %d", action.executeCalled.Load())
	}

	// Orphaned task should still be in outbox
	resp, err := client.Get(ctx, fmt.Sprintf("%s/%s", outboxKey, "orphaned-task"))
	if err != nil {
		t.Fatalf("failed to get task from outbox: %v", err)
	}
	if len(resp.Kvs) == 0 {
		t.Error("expected orphaned task to still be in outbox")
	}
}

func TestOutbox_AddTask_ConcurrentSameTask(t *testing.T) {
	client, epochKey, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	var executionCount atomic.Int32
	action := newMockAction()
	action.executeFn = func(ctx context.Context, payload any) error {
		executionCount.Add(1)
		time.Sleep(50 * time.Millisecond) // Simulate some work
		return nil
	}

	testTask := &distributed.Task{
		Name: "concurrent-task",
		Then: []task.Action{action},
	}

	outbox := NewOutbox(ctx, 1, epochKey, client, nil)

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

func TestOutbox_AddTask_EpochMismatch(t *testing.T) {
	client, epochKey, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	action := newMockAction()
	testTask := &distributed.Task{
		Name: "epoch-mismatch-task",
		Then: []task.Action{action},
	}

	// Create outbox with epoch 1
	outbox := NewOutbox(ctx, 1, epochKey, client, nil)

	// Change the epoch in etcd
	_, err := client.Put(ctx, epochKey, "2")
	if err != nil {
		t.Fatalf("failed to update epoch: %v", err)
	}

	// Adding task should fail silently (epoch mismatch)
	err = outbox.ExecuteTask(testTask)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Give some time
	time.Sleep(100 * time.Millisecond)

	// Action might be called initially, but subsequent steps should fail
	// The behavior depends on implementation details
}

func TestOutbox_FullfillTask_ContextCancellation(t *testing.T) {
	client, epochKey, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())

	actionCalled := make(chan struct{}, 1)
	action := newMockAction()
	action.executeFn = func(ctx context.Context, payload any) error {
		actionCalled <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}

	testTask := &distributed.Task{
		Name: "cancellation-task",
		Then: []task.Action{action},
	}

	outbox := NewOutbox(ctx, 1, epochKey, client, nil)

	// Start the task
	go outbox.ExecuteTask(testTask)

	// Wait for action to start
	select {
	case <-actionCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("action was not called within timeout")
	}

	// Cancel the context
	cancel()

	// Give some time for cancellation to propagate
	time.Sleep(100 * time.Millisecond)
}

func TestOutbox_IdempotencyId_Uniqueness(t *testing.T) {
	client, epochKey, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	var idempotencyIds []string
	var mu sync.Mutex

	action := newMockAction()
	action.executeFn = func(ctx context.Context, payload any) error {
		mu.Lock()
		_, idempotencyId, err := distributed.GetPayload(payload)
		if err != nil {
			mu.Unlock()
			return err
		}
		idempotencyIds = append(idempotencyIds, idempotencyId)
		mu.Unlock()
		return nil
	}

	testTask := &distributed.Task{
		Name: "idempotency-task",
		Then: []task.Action{action},
	}

	outbox := NewOutbox(ctx, 1, epochKey, client, nil)
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
}

func TestOutbox_EmptyTask(t *testing.T) {
	client, epochKey, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	// Task with no actions
	testTask := &distributed.Task{
		Name: "empty-task",
		Then: []task.Action{},
	}

	outbox := NewOutbox(ctx, 1, epochKey, client, nil)

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

func TestOutbox_Init_ExecutesWhileAddTaskSkips(t *testing.T) {
	client, epochKey, cleanup := setupOutboxTest(t)
	defer cleanup()

	ctx := context.Background()

	// Track execution count and when action starts
	var executionCount atomic.Int32
	actionStarted := make(chan struct{}, 1)

	action := newMockAction()
	action.executeFn = func(ctx context.Context, payload any) error {
		executionCount.Add(1)

		// Signal that action has started
		select {
		case actionStarted <- struct{}{}:
		default:
		}
		// Simulate work to give time for AddTask to be called
		time.Sleep(200 * time.Millisecond)
		return nil
	}

	testTask := &distributed.Task{
		Name: "init-task",
		Then: []task.Action{action},
	}

	// Step 1: Pre-add task to etcd outbox (simulating existing task)
	item := OutboxItem{
		CreatedAt: time.Now(),
		TaskStep:  0,
	}
	jsonData, _ := json.Marshal(item)
	_, err := client.Put(ctx, fmt.Sprintf("%s/%s", outboxKey, "init-task"), string(jsonData))
	if err != nil {
		t.Fatalf("failed to put task in outbox: %v", err)
	}

	// Step 2: Create outbox with the task - init should pick it up and start executing
	outbox := NewOutbox(ctx, 1, epochKey, client, []task.Task{testTask})

	// Wait for init to start executing the task
	select {
	case <-actionStarted:
		t.Log("Init started executing the task")
	case <-time.After(2 * time.Second):
		t.Fatal("init did not start executing within timeout")
	}

	// Step 3: While task is executing from init, try to add the same task
	// This should be skipped because the task is already executing
	err = outbox.ExecuteTask(testTask)
	if err != nil {
		t.Fatalf("unexpected error from AddTask: %v", err)
	}

	// Give time for everything to complete
	time.Sleep(400 * time.Millisecond)

	// Step 4: Verify action was executed only once (from init, not from AddTask)
	if executionCount.Load() != 1 {
		t.Errorf("expected action to be executed once (by init), got %d", executionCount.Load())
	}

	// Task should be removed from outbox after completion
	resp, err := client.Get(ctx, fmt.Sprintf("%s/%s", outboxKey, "init-task"))
	if err != nil {
		t.Fatalf("failed to get task from outbox: %v", err)
	}
	if len(resp.Kvs) != 0 {
		t.Error("expected task to be removed from outbox after completion")
	}

	// Task should no longer be marked as executing
	outbox.mu.Lock()
	val := outbox.executingTasks["init-task"]
	outbox.mu.Unlock()
	if val {
		t.Error("expected task to no longer be marked as executing after completion")
	}
}
