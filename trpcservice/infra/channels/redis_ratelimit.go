// redis_ratelimit.go implements the channels.RateLimiter contract on a shared
// Redis, so limits hold across gateway nodes. It uses a fixed-window counter
// (INCR + first-seen EXPIRE), which is simple, cheap and adequate for abuse
// throttling; the trade-off of a fixed window (bursts at window edges) is
// documented in docs/IM平台限制与降级策略.md.
package channels

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisRateLimiter allows up to limit requests per window for a key.
type RedisRateLimiter struct {
	client *redis.Client
	limit  int64
	window time.Duration
}

// NewRedisRateLimiter returns a fixed-window limiter on the given client.
func NewRedisRateLimiter(client *redis.Client, limit int64, window time.Duration) *RedisRateLimiter {
	return &RedisRateLimiter{client: client, limit: limit, window: window}
}

// Allow implements RateLimiter. A counter exceeding the limit denies the key;
// the window is reset by EXPIRE set on the first increment of each window.
// Redis errors are returned so the caller can decide the fail-open policy.
func (r *RedisRateLimiter) Allow(ctx context.Context, key string) (bool, error) {
	redisKey := "ratelimit:" + key
	n, err := r.client.Incr(ctx, redisKey).Result()
	if err != nil {
		return false, fmt.Errorf("ratelimit: %w", err)
	}
	if n == 1 {
		// First request of this window: arm the expiry (idempotent when a
		// concurrent first request already set it).
		if err := r.client.Expire(ctx, redisKey, r.window).Err(); err != nil {
			return false, fmt.Errorf("ratelimit: %w", err)
		}
	}
	return n <= r.limit, nil
}
