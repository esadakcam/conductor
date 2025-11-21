package cluster

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/esadakcam/conductor/internal/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// LeaderFunc is invoked once leadership is acquired.
// The provided epoch is a monotonically increasing value updated via etcd.
// The function should block until the leadership context is cancelled.
type LeaderFunc func(ctx context.Context, epoch int64) error

// LeaderElector coordinates leader election against etcd using the Campaign API.
// Leadership is bound to a lease so that failovers are handled automatically when
// the lease expires or is revoked.
type LeaderElector struct {
	client   *clientv3.Client
	id       string
	prefix   string
	epochKey string
	leaseTTL int
	leaderFn LeaderFunc
	// backoff defines how long to wait before retrying when errors occur.
	backoff time.Duration
}

// Config defines the inputs required to build a LeaderElector.
type Config struct {
	// EtcdEndpoints are the etcd endpoints. Required.
	EtcdEndpoints []string
	// Name is the unique identifier for this elector. Required.
	Name string
	// Prefix is the etcd key prefix for the election. Defaults to "/conductor/leader" if empty.
	Prefix string
	// EpochKey is the etcd key for storing the epoch. Defaults to "/conductor/epoch" if empty.
	EpochKey string
	// LeaseTTL is the TTL for the lease in seconds. Defaults to 10 if <= 0.
	LeaseTTL int
	// LeaderFn is the function to execute when leadership is acquired. Required.
	LeaderFn LeaderFunc
	// Backoff is the duration to wait before retrying on errors. Defaults to 1 second if 0.
	Backoff time.Duration
}

// NewLeaderElector validates the config, creates an etcd client, and returns a ready to use LeaderElector.
// Returns the LeaderElector and the etcd client (which should be closed by the caller).
func NewLeaderElector(cfg Config) (*LeaderElector, *clientv3.Client, error) {
	if len(cfg.EtcdEndpoints) == 0 {
		err := errors.New("etcd endpoints are required")
		logger.Error("NewLeaderElector: etcd endpoints are required")
		return nil, nil, err
	}

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   cfg.EtcdEndpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Errorf("NewLeaderElector: failed to create etcd client: %v", err)
		return nil, nil, fmt.Errorf("failed to create etcd client: %w", err)
	}

	if cfg.Name == "" {
		client.Close()
		err := errors.New("name is required")
		logger.Error("NewLeaderElector: name is required")
		return nil, nil, err
	}

	if cfg.LeaderFn == nil {
		client.Close()
		err := errors.New("leader function is required")
		logger.Error("NewLeaderElector: leader function is required")
		return nil, nil, err
	}

	prefix := cfg.Prefix
	if prefix == "" {
		prefix = "/conductor/leader"
	}
	epochKey := cfg.EpochKey
	if epochKey == "" {
		epochKey = "/conductor/epoch"
	}
	leaseTTL := cfg.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = 10
	}
	backoff := cfg.Backoff
	if backoff == 0 {
		backoff = time.Second
	}

	elector := &LeaderElector{
		client:   client,
		id:       cfg.Name,
		prefix:   prefix,
		epochKey: epochKey,
		leaseTTL: leaseTTL,
		leaderFn: cfg.LeaderFn,
		backoff:  backoff,
	}

	return elector, client, nil
}

// Run blocks while participating in election until the context is cancelled.
func (e *LeaderElector) Run(ctx context.Context) error {
	for {
		session, err := concurrency.NewSession(
			e.client,
			concurrency.WithTTL(e.leaseTTL),
			concurrency.WithContext(ctx),
		)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return ctx.Err()
			}
			logger.Errorf("LeaderElector: failed to create session: %v", err)
			time.Sleep(e.backoff)
			continue
		}

		election := concurrency.NewElection(session, e.prefix)
		if err := election.Campaign(ctx, e.id); err != nil {
			session.Close()
			if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return ctx.Err()
			}
			logger.Errorf("LeaderElector: failed to campaign for leadership: %v", err)
			time.Sleep(e.backoff)
			continue
		}

		epoch, err := e.nextEpoch(ctx)
		if err != nil {
			_ = election.Resign(context.Background())
			session.Close()
			if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return ctx.Err()
			}
			logger.Errorf("LeaderElector: failed to get next epoch: %v", err)
			time.Sleep(e.backoff)
			continue
		}

		logger.Infof("Leadership acquired for epoch: %d", epoch)
		leaderCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = e.leaderFn(leaderCtx, epoch)
		}()

		var reason error
		select {
		case <-session.Done():
			reason = errors.New("leadership lost")
			logger.Warn("LeaderElector: leadership lost")
		case <-ctx.Done():
			reason = ctx.Err()
		}

		cancel()
		// The leader function should watch leaderCtx. Done channel is waited leader function to exit.
		<-done

		resignCtx, resignCancel := context.WithTimeout(context.Background(), 2*time.Second)
		if err := election.Resign(resignCtx); err != nil {
			logger.Errorf("LeaderElector: failed to resign leadership: %v", err)
		}
		resignCancel()
		session.Close()

		if errors.Is(reason, context.Canceled) || errors.Is(reason, context.DeadlineExceeded) {
			return reason
		}

		time.Sleep(e.backoff)
	}
}

func (e *LeaderElector) nextEpoch(ctx context.Context) (int64, error) {
	for {
		currEpochResp, err := e.client.Get(ctx, e.epochKey)
		if err != nil {
			logger.Errorf("LeaderElector: failed to get current epoch from etcd: %v", err)
			return 0, err
		}

		var (
			expectedRevision int64
			next             int64
		)

		if len(currEpochResp.Kvs) == 0 {
			expectedRevision = 0
			next = 1
		} else {
			currentKV := currEpochResp.Kvs[0]
			current, err := strconv.ParseInt(string(currentKV.Value), 10, 64)
			if err != nil {
				logger.Errorf("LeaderElector: failed to parse epoch value %q: %v", string(currentKV.Value), err)
				return 0, fmt.Errorf("invalid epoch value %q: %w", string(currentKV.Value), err)
			}
			expectedRevision = currentKV.ModRevision
			next = current + 1
		}

		cmp := clientv3.Compare(clientv3.ModRevision(e.epochKey), "=", expectedRevision)
		txnResp, err := e.client.Txn(ctx).
			If(cmp).
			Then(clientv3.OpPut(e.epochKey, fmt.Sprintf("%d", next))).
			Commit()
		if err != nil {
			logger.Errorf("LeaderElector: failed to commit epoch transaction: %v", err)
			return 0, err
		}

		if txnResp.Succeeded {
			return next, nil
		}
	}
}
