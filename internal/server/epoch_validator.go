package server

import (
	"context"
	"fmt"
	"strconv"

	"github.com/esadakcam/conductor/internal/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// EpochValidator validates if the incoming epoch is valid
type EpochValidator struct {
	client   *clientv3.Client
	epochKey string
}

// NewEpochValidator creates a new EpochValidator
func NewEpochValidator(client *clientv3.Client, epochKey string) *EpochValidator {
	if epochKey == "" {
		epochKey = "/conductor/epoch"
	}
	return &EpochValidator{
		client:   client,
		epochKey: epochKey,
	}
}

// Validate checks if the request epoch is valid (>= current epoch in etcd)
func (v *EpochValidator) Validate(ctx context.Context, requestEpoch int64) (bool, error) {
	resp, err := v.client.Get(ctx, v.epochKey)
	if err != nil {
		logger.Errorf("EpochValidator: failed to read epoch from etcd: %v", err)
		return false, fmt.Errorf("failed to read epoch from etcd: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return false, nil
	}

	currentKV := resp.Kvs[0]
	currentEpoch, err := strconv.ParseInt(string(currentKV.Value), 10, 64)
	if err != nil {
		logger.Errorf("EpochValidator: failed to parse current epoch value %q: %v", string(currentKV.Value), err)
		return false, fmt.Errorf("failed to parse current epoch value %q: %w", string(currentKV.Value), err)
	}

	return requestEpoch == currentEpoch, nil
}
