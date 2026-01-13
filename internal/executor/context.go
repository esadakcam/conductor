// Package executor provides execution context implementations for different modes.
package executor

import (
	"github.com/esadakcam/conductor/internal/k8s"
	"github.com/esadakcam/conductor/internal/task"
)

// CentralizedContext provides execution context for centralized mode
type CentralizedContext struct {
	K8sClients map[string]*k8s.Client
}

func NewCentralizedContext(k8sClients map[string]*k8s.Client) *CentralizedContext {
	return &CentralizedContext{
		K8sClients: k8sClients,
	}
}

func (c *CentralizedContext) GetMode() task.Mode {
	return task.ModeCentralized
}

func (c *CentralizedContext) GetIdempotencyID() string {
	return ""
}

func (c *CentralizedContext) GetEpoch() int64 {
	return 0
}

// GetK8sClient returns the k8s client for the specified member
func (c *CentralizedContext) GetK8sClient(member string) (*k8s.Client, bool) {
	client, ok := c.K8sClients[member]
	return client, ok
}

// DistributedContext provides execution context for distributed mode
type DistributedContext struct {
	IdempotencyID string
	Epoch         int64
}

func NewDistributedContext(idempotencyID string, epoch int64) *DistributedContext {
	return &DistributedContext{
		IdempotencyID: idempotencyID,
		Epoch:         epoch,
	}
}

func (c *DistributedContext) GetMode() task.Mode {
	return task.ModeDistributed
}

func (c *DistributedContext) GetIdempotencyID() string {
	return c.IdempotencyID
}

func (c *DistributedContext) GetEpoch() int64 {
	return c.Epoch
}
