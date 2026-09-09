package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const lockPrefix = "lock:"

var (
	renewLockScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("PEXPIRE", KEYS[1], ARGV[2])
		end
		return 0
	`)
	releaseLockScript = redis.NewScript(`
		if redis.call("GET", KEYS[1]) == ARGV[1] then
			return redis.call("DEL", KEYS[1])
		end
		return 0
	`)
)

// RedisSessionLockConfig controls lease and renewal timing.
type RedisSessionLockConfig struct {
	TTL           time.Duration
	RenewInterval time.Duration
	MaxHold       time.Duration
}

// RedisSessionLock serializes one Runner execution per session.
type RedisSessionLock struct {
	client redis.Cmdable
	config RedisSessionLockConfig
}

// NewRedisSessionLock constructs a non-blocking Redis session lock.
func NewRedisSessionLock(
	client redis.Cmdable,
	config RedisSessionLockConfig,
) (*RedisSessionLock, error) {
	if client == nil {
		return nil, errors.New("session lock Redis client is required")
	}
	if config.TTL <= 0 {
		return nil, errors.New("session lock TTL must be positive")
	}
	if config.RenewInterval == 0 {
		config.RenewInterval = config.TTL / 3
	}
	if config.MaxHold == 0 {
		config.MaxHold = 10 * time.Minute
	}
	if config.RenewInterval <= 0 || config.RenewInterval >= config.TTL {
		return nil, errors.New("renew interval must be positive and shorter than lock TTL")
	}
	if config.MaxHold < config.TTL {
		return nil, errors.New("maximum hold duration must be at least the lock TTL")
	}
	return &RedisSessionLock{client: client, config: config}, nil
}

// Acquire attempts once and returns ErrHeld instead of waiting.
func (l *RedisSessionLock) Acquire(ctx context.Context, sessionID string) (Lease, error) {
	if sessionID == "" {
		return nil, errors.New("session ID is required")
	}
	token, err := newOwnerToken()
	if err != nil {
		return nil, err
	}
	key := lockPrefix + sessionID
	acquired, err := l.client.SetNX(ctx, key, token, l.config.TTL).Result()
	if err != nil {
		return nil, fmt.Errorf("acquire session lock: %w", err)
	}
	if !acquired {
		return nil, ErrHeld
	}
	lease := &redisLease{
		client: l.client,
		key:    key,
		token:  token,
		config: l.config,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
		lostCh: make(chan error, 1),
	}
	go lease.renew(ctx)
	return lease, nil
}

type redisLease struct {
	client redis.Cmdable
	key    string
	token  string
	config RedisSessionLockConfig

	stop        chan struct{}
	done        chan struct{}
	lostCh      chan error
	releaseOnce sync.Once

	mu         sync.Mutex
	lost       error
	releaseErr error
}

func (l *redisLease) renew(ctx context.Context) {
	defer close(l.done)
	ticker := time.NewTicker(l.config.RenewInterval)
	defer ticker.Stop()
	maxHold := time.NewTimer(l.config.MaxHold)
	defer maxHold.Stop()

	for {
		select {
		case <-l.stop:
			return
		case <-ctx.Done():
			return
		case <-maxHold.C:
			l.setLost(fmt.Errorf("%w: maximum hold duration exceeded", ErrLeaseLost))
			return
		case <-ticker.C:
			result, err := renewLockScript.Run(
				ctx,
				l.client,
				[]string{l.key},
				l.token,
				l.config.TTL.Milliseconds(),
			).Int64()
			if err != nil {
				l.setLost(fmt.Errorf("renew session lock: %w", err))
				return
			}
			if result != 1 {
				l.setLost(ErrLeaseLost)
				return
			}
		}
	}
}

// Release stops renewal and deletes the lock only if its fencing token still
// matches. It is safe to call more than once.
func (l *redisLease) Release(ctx context.Context) error {
	l.releaseOnce.Do(func() {
		close(l.stop)
		<-l.done
		result, err := releaseLockScript.Run(ctx, l.client, []string{l.key}, l.token).Int64()
		if err != nil {
			l.releaseErr = fmt.Errorf("release session lock: %w", err)
			return
		}
		if result != 1 {
			l.releaseErr = ErrLeaseLost
			return
		}
		if lost := l.lostError(); lost != nil {
			l.releaseErr = lost
		}
	})
	return l.releaseErr
}

func (l *redisLease) setLost(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lost == nil {
		l.lost = err
		l.lostCh <- err
		close(l.lostCh)
	}
}

// Lost exposes lease loss so holders can abort work instead of writing
// concurrently with a new lock owner.
func (l *redisLease) Lost() <-chan error {
	return l.lostCh
}

func (l *redisLease) lostError() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lost
}
