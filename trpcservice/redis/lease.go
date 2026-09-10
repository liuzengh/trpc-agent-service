package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	goredis "github.com/redis/go-redis/v9"
)

const (
	sessionLeasePrefix    = "trpc-agent-service:session:"
	leaseRetryInterval    = 100 * time.Millisecond
	leaseReleaseTimeout   = 5 * time.Second
	leaseRenewDivisor     = 3
	leaseRenewSuccessCode = 1
)

var renewLeaseScript = goredis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`)

var releaseLeaseScript = goredis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

// SessionLocker serializes runner execution for one partition through Redis.
type SessionLocker struct {
	client *goredis.Client
	ttl    time.Duration
}

// NewSessionLocker creates a Redis-backed worker Session locker.
func NewSessionLocker(client *Client, ttl time.Duration) (*SessionLocker, error) {
	if client == nil || client.client == nil {
		return nil, errors.New("redis client is required")
	}
	if ttl <= 0 {
		return nil, errors.New("redis session lease duration must be positive")
	}
	return &SessionLocker{client: client.client, ttl: ttl}, nil
}

// Lock waits for one partition's lease. Losing lease renewal cancels Context
// with worker.ErrSessionLeaseLost as its cause.
func (l *SessionLocker) Lock(ctx context.Context, partitionKey string) (worker.SessionLock, error) {
	if l == nil || l.client == nil {
		return nil, errors.New("redis session locker is not initialized")
	}
	if partitionKey == "" {
		return nil, errors.New("session partition key is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	key := sessionLeasePrefix + partitionKey
	token := uuid.NewString()
	for {
		result, err := l.client.SetArgs(ctx, key, token, goredis.SetArgs{
			Mode: "NX",
			TTL:  l.ttl,
		}).Result()
		if err != nil && !errors.Is(err, goredis.Nil) {
			return nil, fmt.Errorf("acquire redis session lease: %w", err)
		}
		if result == "OK" {
			break
		}
		timer := time.NewTimer(leaseRetryInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, fmt.Errorf("wait for redis session lease: %w", ctx.Err())
		case <-timer.C:
		}
	}
	runCtx, cancel := context.WithCancelCause(ctx)
	lease := &sessionLease{
		client: l.client,
		key:    key,
		token:  token,
		ttl:    l.ttl,
		runCtx: runCtx,
		cancel: cancel,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go lease.renew()
	return lease, nil
}

type sessionLease struct {
	client *goredis.Client
	key    string
	token  string
	ttl    time.Duration
	runCtx context.Context
	cancel context.CancelCauseFunc
	stop   chan struct{}
	done   chan struct{}

	once sync.Once
	err  error
}

func (l *sessionLease) Context() context.Context { return l.runCtx }

func (l *sessionLease) Release() error {
	l.once.Do(func() {
		close(l.stop)
		<-l.done
		l.cancel(nil)
		ctx, cancel := context.WithTimeout(context.Background(), leaseReleaseTimeout)
		defer cancel()
		if _, err := releaseLeaseScript.Run(ctx, l.client, []string{l.key}, l.token).Result(); err != nil {
			l.err = fmt.Errorf("release redis session lease: %w", err)
		}
	})
	return l.err
}

func (l *sessionLease) renew() {
	defer close(l.done)
	interval := l.ttl / leaseRenewDivisor
	if interval <= 0 {
		interval = time.Nanosecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), l.ttl/leaseRenewDivisor)
			result, err := renewLeaseScript.Run(ctx, l.client, []string{l.key}, l.token, l.ttl.Milliseconds()).Int()
			cancel()
			if err != nil || result != leaseRenewSuccessCode {
				l.cancel(worker.ErrSessionLeaseLost)
				return
			}
		}
	}
}

var _ worker.SessionLocker = (*SessionLocker)(nil)
