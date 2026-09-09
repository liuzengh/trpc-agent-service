package delivery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// Limiter applies a shared provider-facing rate limit before sending.
type Limiter interface {
	Wait(context.Context, gateway.OutboundMessage) error
}

// RedisEvaler is the minimal shared Redis surface required by the limiter.
type RedisEvaler interface {
	Eval(context.Context, string, []string, ...any) (any, error)
}

// RedisFixedWindowLimiter enforces one tenant-binding limit across nodes.
type RedisFixedWindowLimiter struct {
	Redis  RedisEvaler
	Prefix string
	Limit  int
	Window time.Duration
}

const fixedWindowLua = `
local count = redis.call('INCR', KEYS[1])
if count == 1 then redis.call('PEXPIRE', KEYS[1], ARGV[1]) end
if count > tonumber(ARGV[2]) then return redis.call('PTTL', KEYS[1]) end
return 0`

// Wait blocks until the shared fixed window admits one outbound message.
func (limiter *RedisFixedWindowLimiter) Wait(ctx context.Context, message gateway.OutboundMessage) error {
	if limiter == nil || limiter.Redis == nil || message.TenantID == "" || message.BindingID == "" {
		return errors.New("delivery: Redis limiter and tenant binding are required")
	}
	window, limit := limiter.window(), limiter.limit()
	for {
		result, err := limiter.Redis.Eval(ctx, fixedWindowLua, []string{limiter.key(message)}, window.Milliseconds(), limit)
		if err != nil {
			return fmt.Errorf("delivery: shared rate limit: %w", err)
		}
		delay, err := redisMilliseconds(result)
		if err != nil {
			return err
		}
		if delay <= 0 {
			return nil
		}
		timer := time.NewTimer(time.Duration(delay) * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (limiter *RedisFixedWindowLimiter) key(message gateway.OutboundMessage) string {
	prefix := limiter.Prefix
	if prefix == "" {
		prefix = "trpc-agent-service"
	}
	digest := sha256.Sum256([]byte(message.TenantID + "\x00" + message.BindingID))
	return prefix + ":delivery-rate:" + hex.EncodeToString(digest[:])
}

func (limiter *RedisFixedWindowLimiter) limit() int {
	if limiter.Limit > 0 {
		return limiter.Limit
	}
	return 10
}

func (limiter *RedisFixedWindowLimiter) window() time.Duration {
	if limiter.Window > 0 {
		return limiter.Window
	}
	return time.Second
}

func redisMilliseconds(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	case []byte:
		return strconv.ParseInt(string(typed), 10, 64)
	default:
		return 0, fmt.Errorf("delivery: unexpected Redis delay %T", value)
	}
}
