package coordination

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	redis "github.com/redis/go-redis/v9"
)

var acquireLeaseScript = redis.NewScript(`
if redis.call("exists", KEYS[1]) == 0 then
  local token = redis.call("incr", KEYS[2])
  redis.call("psetex", KEYS[1], ARGV[2], ARGV[1])
  return token
end
return 0
`)

var renewLeaseScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0
`)

var releaseLeaseScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
end
return 0
`)

// RedisOptions contains the already validated settings for Redis leases.
type RedisOptions struct {
	URL           string
	KeyPrefix     string
	LeaseTTL      time.Duration
	RenewInterval time.Duration
	RetryInterval time.Duration
}

// RedisCoordinator owns a Redis client and coordinates Session turns across
// service processes.
type RedisCoordinator struct {
	client        *redis.Client
	keyPrefix     string
	leaseTTL      time.Duration
	renewInterval time.Duration
	retryInterval time.Duration
	closeCtx      context.Context
	closeCancel   context.CancelCauseFunc
	closeOnce     sync.Once
	closeErr      error
}

// NewRedisCoordinator creates a coordinator without probing the connection.
func NewRedisCoordinator(opts RedisOptions) (*RedisCoordinator, error) {
	redisOpts, err := redis.ParseURL(opts.URL)
	if err != nil {
		return nil, fmt.Errorf("parse coordinator Redis URL: %w", err)
	}
	if opts.LeaseTTL <= 0 {
		return nil, fmt.Errorf("coordinator lease TTL must be positive")
	}
	if opts.RenewInterval <= 0 || opts.RenewInterval > opts.LeaseTTL/2 {
		return nil, fmt.Errorf("coordinator renew interval must be positive and at most half of lease TTL")
	}
	if opts.RetryInterval <= 0 {
		return nil, fmt.Errorf("coordinator retry interval must be positive")
	}
	prefix := strings.Trim(strings.TrimSpace(opts.KeyPrefix), ":")
	if prefix == "" {
		return nil, fmt.Errorf("coordinator Redis key prefix is required")
	}
	closeCtx, closeCancel := context.WithCancelCause(context.Background())
	return &RedisCoordinator{
		client:        redis.NewClient(redisOpts),
		keyPrefix:     prefix,
		leaseTTL:      opts.LeaseTTL,
		renewInterval: opts.RenewInterval,
		retryInterval: opts.RetryInterval,
		closeCtx:      closeCtx,
		closeCancel:   closeCancel,
	}, nil
}

// Acquire waits for an atomic Redis lease and starts its renewal loop.
func (c *RedisCoordinator) Acquire(ctx context.Context, key Key) (Lease, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	owner, err := randomOwnerID()
	if err != nil {
		return nil, err
	}
	lockKey, fenceKey := c.redisKeys(key)

	for {
		if err := ctx.Err(); err != nil {
			return nil, context.Cause(ctx)
		}
		if cause := context.Cause(c.closeCtx); cause != nil {
			return nil, cause
		}

		token, err := acquireLeaseScript.Run(
			ctx,
			c.client,
			[]string{lockKey, fenceKey},
			owner,
			c.leaseTTL.Milliseconds(),
		).Int64()
		if err != nil {
			return nil, fmt.Errorf("acquire Redis session lease: %w", err)
		}
		if token > 0 {
			return c.newLease(ctx, lockKey, owner, token), nil
		}

		timer := time.NewTimer(c.retryInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, context.Cause(ctx)
		case <-c.closeCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, coordinatorCause(c.closeCtx)
		}
	}
}

func (c *RedisCoordinator) newLease(
	ctx context.Context,
	lockKey string,
	owner string,
	token int64,
) *redisLease {
	leaseCtx, leaseCancel := context.WithCancelCause(ctx)
	lease := &redisLease{
		ctx:           leaseCtx,
		cancel:        leaseCancel,
		client:        c.client,
		lockKey:       lockKey,
		owner:         owner,
		token:         token,
		leaseTTL:      c.leaseTTL,
		renewInterval: c.renewInterval,
		renewDone:     make(chan struct{}),
	}
	lease.stopCloseCallback = context.AfterFunc(c.closeCtx, func() {
		lease.cancel(ErrCoordinatorClosed)
	})
	go lease.renewLoop()
	return lease
}

func (c *RedisCoordinator) redisKeys(key Key) (string, string) {
	digestInput := fmt.Sprintf(
		"%d:%s|%d:%s|%d:%s",
		len(key.AppName), key.AppName,
		len(key.UserID), key.UserID,
		len(key.SessionID), key.SessionID,
	)
	digest := sha256.Sum256([]byte(digestInput))
	hashTag := hex.EncodeToString(digest[:])
	base := c.keyPrefix + ":coord:session:{" + hashTag + "}"
	return base + ":lock", base + ":fence"
}

// Ready checks Redis connectivity.
func (c *RedisCoordinator) Ready(ctx context.Context) error {
	if c == nil || c.client == nil {
		return ErrCoordinatorClosed
	}
	if cause := context.Cause(c.closeCtx); cause != nil {
		return cause
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping coordinator Redis: %w", err)
	}
	return nil
}

// Close cancels active lease contexts and closes the owned Redis client.
func (c *RedisCoordinator) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		c.closeCancel(ErrCoordinatorClosed)
		if c.client != nil {
			c.closeErr = c.client.Close()
		}
	})
	return c.closeErr
}

type redisLease struct {
	ctx               context.Context
	cancel            context.CancelCauseFunc
	stopCloseCallback func() bool
	client            *redis.Client
	lockKey           string
	owner             string
	token             int64
	leaseTTL          time.Duration
	renewInterval     time.Duration
	renewDone         chan struct{}
	releaseOnce       sync.Once
	releaseErr        error
}

func (l *redisLease) Context() context.Context {
	return l.ctx
}

func (l *redisLease) FencingToken() int64 {
	return l.token
}

func (l *redisLease) renewLoop() {
	defer close(l.renewDone)
	ticker := time.NewTicker(l.renewInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			result, err := renewLeaseScript.Run(
				l.ctx,
				l.client,
				[]string{l.lockKey},
				l.owner,
				l.leaseTTL.Milliseconds(),
			).Int64()
			if err != nil {
				if l.ctx.Err() == nil {
					l.cancel(fmt.Errorf("%w: renew Redis lease: %v", ErrLeaseLost, err))
				}
				return
			}
			if result != 1 {
				l.cancel(ErrLeaseLost)
				return
			}
		case <-l.ctx.Done():
			return
		}
	}
}

func (l *redisLease) Release(ctx context.Context) error {
	if l == nil {
		return nil
	}
	l.releaseOnce.Do(func() {
		if l.stopCloseCallback != nil {
			l.stopCloseCallback()
		}
		l.cancel(nil)
		<-l.renewDone
		if ctx == nil {
			ctx = context.Background()
		}
		result, err := releaseLeaseScript.Run(
			ctx,
			l.client,
			[]string{l.lockKey},
			l.owner,
		).Int64()
		if err != nil {
			l.releaseErr = fmt.Errorf("release Redis session lease: %w", err)
			return
		}
		if result != 1 {
			l.releaseErr = ErrLeaseLost
		}
	})
	return l.releaseErr
}

func randomOwnerID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate Redis lease owner: %w", err)
	}
	return hex.EncodeToString(value[:]), nil
}

var _ Coordinator = (*RedisCoordinator)(nil)
var _ Lease = (*redisLease)(nil)
