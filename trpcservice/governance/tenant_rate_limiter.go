package governance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrTenantRateLimited = errors.New("tenant request rate limit exceeded")

type TenantRateLimiter interface {
	Take(context.Context, string, string, int) error
}

// RedisTenantRateLimiter implements a tenant token bucket in one Lua transaction.
type RedisTenantRateLimiter struct {
	client redis.UniversalClient
	prefix string
	now    func() time.Time
}

const redisTokenBucketScript = `
local now = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local refill = tonumber(ARGV[3])
local tokens = tonumber(redis.call('HGET', KEYS[1], 'tokens'))
local updated = tonumber(redis.call('HGET', KEYS[1], 'updated_ms'))
if not tokens then tokens = capacity end
if not updated then updated = now end
tokens = math.min(capacity, tokens + math.max(0, now - updated) * refill)
if tokens < 1 then
  redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated_ms', now)
  redis.call('PEXPIRE', KEYS[1], 120000)
  return 0
end
redis.call('HSET', KEYS[1], 'tokens', tokens - 1, 'updated_ms', now)
redis.call('PEXPIRE', KEYS[1], 120000)
return 1
`

func NewRedisTenantRateLimiter(client redis.UniversalClient) (*RedisTenantRateLimiter, error) {
	if client == nil {
		return nil, fmt.Errorf("Redis tenant rate limiter client is required")
	}
	return &RedisTenantRateLimiter{client: client, prefix: "trpc:tenant-rate:", now: time.Now}, nil
}

func (l *RedisTenantRateLimiter) Take(ctx context.Context, tenantID, appCode string, capacity int) error {
	if capacity <= 0 {
		return nil
	}
	if l == nil || l.client == nil {
		return fmt.Errorf("Redis tenant rate limiter is required")
	}
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return fmt.Errorf("rate limit tenant ID is required")
	}
	appCode = strings.TrimSpace(appCode)
	if appCode == "" {
		return fmt.Errorf("rate limit app code is required")
	}
	allowed, err := l.client.Eval(ctx, redisTokenBucketScript, []string{l.prefix + tenantID + ":" + appCode}, l.now().UnixMilli(), capacity, float64(capacity)/60000).Int64()
	if err != nil {
		return fmt.Errorf("take tenant request token: %w", err)
	}
	if allowed != 1 {
		return ErrTenantRateLimited
	}
	return nil
}
