package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/redis/go-redis/v9"
)

// Backend is the Redis coordination implementation. All state transitions are Lua-atomic.
type Backend struct {
	client             *redis.Client
	prefix             string
	claimTTL, leaseTTL time.Duration
}

func NewBackend(cfg Config) (*Backend, error) {
	c, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	client, err := NewClient(c)
	if err != nil {
		return nil, err
	}
	return &Backend{client: client, prefix: c.KeyPrefix, claimTTL: c.ClaimTTL, leaseTTL: c.SessionLeaseTTL}, nil
}
func NewBackendWithClient(client *redis.Client, cfg Config) (*Backend, error) {
	c, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, fmt.Errorf("redis: client is required")
	}
	return &Backend{client: client, prefix: c.KeyPrefix, claimTTL: c.ClaimTTL, leaseTTL: c.SessionLeaseTTL}, nil
}
func (b *Backend) Close() error {
	if b == nil || b.client == nil {
		return nil
	}
	return b.client.Close()
}
func (b *Backend) Ping(ctx context.Context) error {
	if err := b.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("%w: %w: %v", storage.ErrBackendUnavailable, ErrRedisUnavailable, err)
	}
	return nil
}
func validateContext(ctx context.Context, tc tenant.TenantContext, owner string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tc.Validate(); err != nil {
		return err
	}
	if err := keyPart("owner", owner); err != nil {
		return err
	}
	return nil
}
func mapRedisErr(err error) error {
	if err == nil {
		return nil
	}
	if err == context.Canceled || err == context.DeadlineExceeded {
		return err
	}
	return fmt.Errorf("%w: %w: %v", storage.ErrBackendUnavailable, ErrRedisUnavailable, err)
}
func resultInt(v interface{}) (int64, error) {
	n, ok := v.(int64)
	if !ok {
		return 0, fmt.Errorf("%w: invalid script result", ErrRedisUnavailable)
	}
	return n, nil
}

var _ storage.IdempotencyRepository = (*Backend)(nil)
