package redis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	goredis "github.com/redis/go-redis/v9"
)

const admissionRateLimitPrefix = "trpc-agent-service:admission-rate:"

var admissionRateLimitScript = goredis.NewScript(`
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

// AdmissionRateLimiter applies one fixed-window budget per tenant and
// application. The Redis script makes the decision atomic across Gateway
// nodes and returns immediately when the budget is exhausted.
type AdmissionRateLimiter struct {
	client *goredis.Client
	limit  int64
	window time.Duration
}

// NewAdmissionRateLimiter creates a shared Redis admission limiter.
func NewAdmissionRateLimiter(client *Client, limit int, window time.Duration) (*AdmissionRateLimiter, error) {
	if client == nil || client.client == nil {
		return nil, errors.New("redis client is required")
	}
	if limit <= 0 {
		return nil, errors.New("admission rate limit must be positive")
	}
	if window <= 0 || window.Milliseconds() <= 0 {
		return nil, errors.New("admission rate limit window must be at least one millisecond")
	}
	return &AdmissionRateLimiter{client: client.client, limit: int64(limit), window: window}, nil
}

// Allow consumes one tenant/application admission token.
func (l *AdmissionRateLimiter) Allow(ctx context.Context, identity gateway.AdmissionIdentity) error {
	if l == nil || l.client == nil {
		return errors.New("admission rate limiter is not initialized")
	}
	if identity.Tenant.TenantID == "" || identity.Tenant.AppID == "" {
		return errors.New("admission rate limit identity scope is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	result, err := admissionRateLimitScript.Run(
		ctx,
		l.client,
		[]string{admissionRateLimitKey(identity)},
		l.window.Milliseconds(),
		l.limit,
	).Int64Slice()
	if err != nil {
		return fmt.Errorf("check admission rate limit: %w", err)
	}
	if len(result) != 2 {
		return errors.New("admission rate limiter returned an invalid result")
	}
	if result[0] == 1 {
		return nil
	}
	retryAfter := time.Duration(result[1]) * time.Millisecond
	if retryAfter <= 0 {
		retryAfter = time.Millisecond
	}
	return &gateway.AdmissionRateLimitError{RetryAfter: retryAfter}
}

func admissionRateLimitKey(identity gateway.AdmissionIdentity) string {
	digest := sha256.Sum256([]byte(identity.Tenant.TenantID + "\x00" + identity.Tenant.AppID))
	return admissionRateLimitPrefix + hex.EncodeToString(digest[:])
}

var _ gateway.AdmissionRateLimiter = (*AdmissionRateLimiter)(nil)
