package cluster

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

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
	Client   *clientv3.Client
	ID       string
	Prefix   string
	EpochKey string
	LeaseTTL int
	LeaderFn LeaderFunc
	Backoff  time.Duration
}

// NewLeaderElector validates the config and returns a ready to use LeaderElector.
func NewLeaderElector(cfg Config) (*LeaderElector, error) {
	if cfg.Client == nil {
		return nil, errors.New("client is required")
	}
	if cfg.ID == "" {
		return nil, errors.New("id is required")
	}
	if cfg.Prefix == "" {
		return nil, errors.New("prefix is required")
	}
	if cfg.EpochKey == "" {
		return nil, errors.New("epoch key is required")
	}
	if cfg.LeaseTTL <= 0 {
		return nil, errors.New("lease ttl must be greater than zero")
	}
	if cfg.LeaderFn == nil {
		return nil, errors.New("leader function is required")
	}

	backoff := cfg.Backoff
	if backoff == 0 {
		backoff = time.Second
	}

	return &LeaderElector{
		client:   cfg.Client,
		id:       cfg.ID,
		prefix:   cfg.Prefix,
		epochKey: cfg.EpochKey,
		leaseTTL: cfg.LeaseTTL,
		leaderFn: cfg.LeaderFn,
		backoff:  backoff,
	}, nil
}

// Run blocks while participating in election until the context is cancelled.
func (e *LeaderElector) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		session, err := concurrency.NewSession(
			e.client,
			concurrency.WithTTL(e.leaseTTL),
			concurrency.WithContext(ctx),
		)
		if err != nil {
			if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return ctx.Err()
			}
			time.Sleep(e.backoff)
			continue
		}

		election := concurrency.NewElection(session, e.prefix)
		if err := election.Campaign(ctx, e.id); err != nil {
			session.Close()
			if errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return ctx.Err()
			}
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
			time.Sleep(e.backoff)
			continue
		}

		leaderCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = e.leaderFn(leaderCtx, epoch)
		}()

		var reason error
		select {
		case <-session.Done():
			//TODO: Cancel leader function
			reason = errors.New("leadership lost")
		case <-ctx.Done():
			reason = ctx.Err()
		}

		cancel()
		//TODO: Cancel leader function
		<-done

		resignCtx, resignCancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = election.Resign(resignCtx)
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
		getResp, err := e.client.Get(ctx, e.epochKey)
		if err != nil {
			return 0, err
		}

		var (
			expectedRevision int64
			next             int64
		)

		if len(getResp.Kvs) == 0 {
			expectedRevision = 0
			next = 1
		} else {
			currentKV := getResp.Kvs[0]
			current, err := strconv.ParseInt(string(currentKV.Value), 10, 64)
			if err != nil {
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
			return 0, err
		}

		if txnResp.Succeeded {
			return next, nil
		}
	}
}
