package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	goredis "github.com/redis/go-redis/v9"
)

const replyRateLimitPrefix = "trpc-agent-service:reply-rate:"

var replyRateLimitScript = goredis.NewScript(`
local limit = tonumber(ARGV[2])
local current = tonumber(redis.call('GET', KEYS[1]) or '0')
if current < limit then
  current = redis.call('INCR', KEYS[1])
  if current == 1 then
    redis.call('PEXPIRE', KEYS[1], ARGV[1])
  end
  return {1, 0}
end
local ttl = redis.call('PTTL', KEYS[1])
if ttl < 0 then
  ttl = tonumber(ARGV[1])
  redis.call('PEXPIRE', KEYS[1], ttl)
end
return {0, ttl}
`)

// ReplyRateLimiter applies a distributed fixed-window limit per binding. It
// waits before returning, so provider calls are proactively paced even when
// several workers claim replies concurrently.
type ReplyRateLimiter struct {
	client *goredis.Client
	limit  int64
	window time.Duration
}

// NewReplyRateLimiter creates a Redis-backed reply limiter.
func NewReplyRateLimiter(client *Client, limit int, window time.Duration) (*ReplyRateLimiter, error) {
	if client == nil || client.client == nil {
		return nil, errors.New("redis client is required")
	}
	if limit <= 0 {
		return nil, errors.New("reply rate limit must be positive")
	}
	if window <= 0 || window.Milliseconds() <= 0 {
		return nil, errors.New("reply rate limit window must be at least one millisecond")
	}
	return &ReplyRateLimiter{client: client.client, limit: int64(limit), window: window}, nil
}

// Allow waits until one provider operation is admitted by the shared limit.
func (l *ReplyRateLimiter) Allow(ctx context.Context, delivery worker.ReplyDelivery) error {
	if l == nil || l.client == nil {
		return errors.New("reply rate limiter is not initialized")
	}
	if err := delivery.Validate(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	key := replyRateLimitKey(delivery)
	for {
		result, err := replyRateLimitScript.Run(
			ctx,
			l.client,
			[]string{key},
			l.window.Milliseconds(),
			l.limit,
		).Int64Slice()
		if err != nil {
			return fmt.Errorf("check reply rate limit: %w", err)
		}
		if len(result) != 2 {
			return errors.New("reply rate limiter returned an invalid result")
		}
		if result[0] == 1 {
			return nil
		}
		wait := time.Duration(result[1]) * time.Millisecond
		if wait <= 0 {
			wait = time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf("wait for reply rate limit: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

func replyRateLimitKey(delivery worker.ReplyDelivery) string {
	digest := sha256.Sum256([]byte(delivery.Reply.TenantID + "\x00" +
		delivery.Reply.AppID + "\x00" + delivery.Reply.BindingID + "\x00" +
		string(delivery.Reply.Channel)))
	return replyRateLimitPrefix + hex.EncodeToString(digest[:])
}

var _ worker.ReplyRateLimiter = (*ReplyRateLimiter)(nil)
