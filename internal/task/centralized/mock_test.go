package centralized

import "github.com/esadakcam/conductor/internal/k8s"

// MockExecutionContext is a test implementation of task.ExecutionContext
type MockExecutionContext struct {
	k8sClients     map[string]*k8s.Client
	idempotencyKey string
}

func NewMockExecutionContext(k8sClients map[string]*k8s.Client) *MockExecutionContext {
	return &MockExecutionContext{
		k8sClients:     k8sClients,
		idempotencyKey: "",
	}
}

func NewMockExecutionContextWithIdempotency(k8sClients map[string]*k8s.Client, idempotencyKey string) *MockExecutionContext {
	return &MockExecutionContext{
		k8sClients:     k8sClients,
		idempotencyKey: idempotencyKey,
	}
}

func (m *MockExecutionContext) GetEpoch() int64 {
	return 0
}

func (m *MockExecutionContext) GetIdempotencyKey() string {
	return m.idempotencyKey
}

func (m *MockExecutionContext) GetK8sClients() map[string]*k8s.Client {
	return m.k8sClients
}
