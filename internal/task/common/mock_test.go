package common

import "github.com/esadakcam/conductor/internal/k8s"

// MockExecutionContext is a test implementation of task.ExecutionContext
type MockExecutionContext struct {
	epoch          int64
	idempotencyKey string
}

func NewMockExecutionContext(epoch int64, idempotencyKey string) *MockExecutionContext {
	return &MockExecutionContext{
		epoch:          epoch,
		idempotencyKey: idempotencyKey,
	}
}

func (m *MockExecutionContext) GetEpoch() int64 {
	return m.epoch
}

func (m *MockExecutionContext) GetIdempotencyKey() string {
	return m.idempotencyKey
}

func (m *MockExecutionContext) GetK8sClients() map[string]*k8s.Client {
	return nil
}
