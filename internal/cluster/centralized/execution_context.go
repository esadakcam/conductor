package centralized

import "github.com/esadakcam/conductor/internal/k8s"

type ExecutionContext struct {
	k8sClients     map[string]*k8s.Client
	idempotencyKey string
}

func (e *ExecutionContext) GetEpoch() int64 {
	return 0
}

func (e *ExecutionContext) GetIdempotencyKey() string {
	return e.idempotencyKey
}

func (e *ExecutionContext) GetK8sClients() map[string]*k8s.Client {
	return e.k8sClients
}

func NewExecutionContext(k8sClients map[string]*k8s.Client, idempotencyKey string) *ExecutionContext {
	return &ExecutionContext{
		k8sClients:     k8sClients,
		idempotencyKey: idempotencyKey,
	}
}
