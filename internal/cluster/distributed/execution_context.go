package distributed

import "github.com/esadakcam/conductor/internal/k8s"

type ExecutionContext struct {
	epoch          int64
	idempotencyKey string
}

func (e *ExecutionContext) GetEpoch() int64 {
	return e.epoch
}

func (e *ExecutionContext) GetIdempotencyKey() string {
	return e.idempotencyKey
}

func (e *ExecutionContext) GetK8sClients() map[string]*k8s.Client {
	return nil
}

func NewExecutionContext(epoch int64, idempotencyKey string) *ExecutionContext {
	return &ExecutionContext{
		epoch:          epoch,
		idempotencyKey: idempotencyKey,
	}
}
