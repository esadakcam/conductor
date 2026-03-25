package server

import (
	"context"
	"fmt"
	"strconv"

	"github.com/esadakcam/conductor/internal/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	DefaultEpochKey             = "/conductor/epoch"
	DefaultIdempotencyKeyPrefix = "/conductor/idempotency"
)

// EpochValidator validates if the incoming epoch is valid
type EpochValidator struct {
	client   *clientv3.Client
	epochKey string
}

// EtcdIdempotencyGuard implements IdempotencyGuard using an etcd
// transaction so that Reserve is atomic: only one caller wins when
// concurrent requests carry the same idempotency id.
type EtcdIdempotencyGuard struct {
	client               *clientv3.Client
	idempotencyKeyPrefix string
	name                 string
}

func NewEtcdIdempotencyGuard(client *clientv3.Client, name string) *EtcdIdempotencyGuard {
	return &EtcdIdempotencyGuard{
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

// Reserve atomically creates the idempotency key only if it does not
// already exist (etcd Txn with CreateRevision == 0).  Returns true when
// the key was successfully reserved for this caller, false when it was
// already present (i.e. a previous request with the same id succeeded).
func (g *EtcdIdempotencyGuard) Reserve(ctx context.Context, id string) (bool, error) {
	key := fmt.Sprintf("%s/%s/%s", g.idempotencyKeyPrefix, g.name, id)
	txnResp, err := g.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, "reserved")).
		Commit()
	if err != nil {
		logger.Errorf("EtcdIdempotencyGuard: txn failed for key %s: %v", key, err)
		return false, fmt.Errorf("failed to reserve idempotency key: %w", err)
	}
	return txnResp.Succeeded, nil
}

// Release removes a previously reserved key so that a retry with the
// same id can proceed.  Called when the action fails after reservation.
func (g *EtcdIdempotencyGuard) Release(ctx context.Context, id string) error {
	key := fmt.Sprintf("%s/%s/%s", g.idempotencyKeyPrefix, g.name, id)
	_, err := g.client.Delete(ctx, key)
	if err != nil {
		logger.Errorf("EtcdIdempotencyGuard: failed to release key %s: %v", key, err)
		return fmt.Errorf("failed to release idempotency key: %w", err)
	}
	return nil
}
