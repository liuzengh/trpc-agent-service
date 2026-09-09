package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// limiterTTL is how long an idle bucket key lives; buckets rebuild from full
// on the next request, so eviction only loses sub-burst precision.
const limiterTTL = 10 * time.Minute

// allowScript is an atomic token bucket: refill by elapsed time, then consume
// one token if available. Returns 1 when the request is allowed, 0 otherwise.
//
// The clock comes from Redis, not the caller: the bucket key is shared by every
// platform node, so a node skewed ahead of its peers would write a ts in their
// future (their next refill goes negative and denies traffic that should pass)
// and a node skewed behind would over-refill it (bypassing the limit). One
// clock for one shared bucket. TIME is non-deterministic, which is fine under
// the effect replication Redis 5+ uses by default.
var allowScript = redis.NewScript(`
local key    = KEYS[1]
local rate   = tonumber(ARGV[1])  -- tokens per second
local burst  = tonumber(ARGV[2])
local ttl_ms = tonumber(ARGV[3])

local t      = redis.call('TIME')
local now    = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local data   = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil or ts == nil then
  tokens = burst
  ts     = now
else
  tokens = math.min(burst, tokens + (now - ts) * rate / 1000.0)
end

local allowed = 0
if tokens >= 1 then
  tokens  = tokens - 1
  allowed = 1
end
redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, ttl_ms)
return allowed
`)

// Limiter is a Redis-backed token bucket shared by all platform nodes, so the
// limit holds cluster-wide rate state and stops one tenant from flooding the
// global queue. Scope convention: "tenant:{tenant_id}" for gateway admission,
// "send:{channel}:{tenant_id}" for outbound IM pacing.
type Limiter struct {
	rdb *redis.Client
}

// NewLimiter creates a Limiter on an established Redis client.
func NewLimiter(rdb *redis.Client) *Limiter {
	return &Limiter{rdb: rdb}
}

// Allow consumes one token from the scope's bucket and reports whether the
// request may proceed. rate is tokens per second, burst the bucket capacity.
func (l *Limiter) Allow(ctx context.Context, scope string, rate float64, burst int) (bool, error) {
	if rate <= 0 || burst <= 0 {
		return true, nil // non-positive config disables the limit
	}
	key := "ratelimit:" + scope
	res, err := allowScript.Run(ctx, l.rdb, []string{key},
		rate, burst, limiterTTL.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("token bucket %s: %w", key, err)
	}
	return res == 1, nil
}

// WaitAllow spins on Allow until a token is granted, maxWait elapses, or ctx
// is canceled. Reports whether a token was granted. Senders use it to pace
// outbound IM calls without dropping the message.
func (l *Limiter) WaitAllow(ctx context.Context, scope string, rate float64, burst int, maxWait time.Duration) (bool, error) {
	deadline := time.Now().Add(maxWait)
	for {
		ok, err := l.Allow(ctx, scope, rate, burst)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
		if time.Now().After(deadline) {
			return false, nil
		}
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}
