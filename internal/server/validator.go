package server

import (
	"context"
	"fmt"
	"strconv"

	"github.com/esadakcam/conductor/internal/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const DefaultEpochKey = "/conductor/epoch"
const DefaultIdempotencyKeyPrefix = "/conductor/idempotency"

// EpochValidator validates if the incoming epoch is valid
type EpochValidator struct {
	client   *clientv3.Client
	epochKey string
}

type IdempotencyValidator struct {
	client               *clientv3.Client
	idempotencyKeyPrefix string
	name                 string
}

func NewIdempotencyValidator(client *clientv3.Client, name string) *IdempotencyValidator {
	return &IdempotencyValidator{
		client:               client,
		idempotencyKeyPrefix: DefaultIdempotencyKeyPrefix,
		name:                 name,
	}
}

// NewEpochValidator creates a new EpochValidator
func NewEpochValidator(client *clientv3.Client) *EpochValidator {
	return &EpochValidator{
		client:   client,
		epochKey: DefaultEpochKey,
	}
}

// Validate checks if the request epoch is valid (>= current epoch in etcd)
func (v *EpochValidator) Validate(ctx context.Context, toValidate any) (bool, error) {
	requestEpoch, ok := toValidate.(int64)
	if !ok {
		return false, fmt.Errorf("toValidate is not a int64")
	}
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

func (v *IdempotencyValidator) Validate(ctx context.Context, toValidate any) (bool, error) {
	idempotencyId, ok := toValidate.(string)
	if !ok {
		return true, fmt.Errorf("toValidate is not a string")
	}
	resp, err := v.client.Get(ctx, fmt.Sprintf("%s/%s/%s", v.idempotencyKeyPrefix, v.name, idempotencyId))
	if err != nil {
		logger.Errorf("IdempotencyValidator: failed to read idempotency key from etcd: %v", err)
		return true, fmt.Errorf("failed to read idempotency key from etcd: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return true, nil
	}

	return false, nil
}
