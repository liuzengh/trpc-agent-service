package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// RedisEvaler is the minimal shared coordination surface used by run quotas.
type RedisEvaler interface {
	Eval(context.Context, string, []string, ...any) (any, error)
}

// RunPermit is an exact, expiring tenant concurrency reservation.
type RunPermit struct {
	TenantID string
	member   string
}

// RunLimiter limits active Runner executions across all Worker nodes.
type RunLimiter interface {
	TryAcquire(context.Context, string, string, string, int, time.Duration) (RunPermit, bool, error)
	Renew(context.Context, RunPermit, time.Duration) error
	Release(RunPermit)
}

// RedisRunLimiter implements an expiring distributed semaphore with a sorted
// set per tenant. Redis TIME prevents node clock skew from changing capacity.
type RedisRunLimiter struct {
	Redis  RedisEvaler
	Prefix string
}

const acquireRunPermitLua = `
local clock = redis.call('TIME')
local now = (tonumber(clock[1]) * 1000) + math.floor(tonumber(clock[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
if redis.call('ZSCORE', KEYS[1], ARGV[1]) then
  redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), ARGV[1])
  redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]) * 2)
  return 1
end
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[2]) then return 0 end
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), ARGV[1])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[3]) * 2)
return 1`

const renewRunPermitLua = `
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 0 end
local clock = redis.call('TIME')
local now = (tonumber(clock[1]) * 1000) + math.floor(tonumber(clock[2]) / 1000)
redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[1])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]) * 2)
return 1`

const releaseRunPermitLua = `return redis.call('ZREM', KEYS[1], ARGV[1])`

func (limiter *RedisRunLimiter) TryAcquire(ctx context.Context, tenantID, requestID, claimToken string, limit int, ttl time.Duration) (RunPermit, bool, error) {
	if limiter == nil || limiter.Redis == nil || tenantID == "" || requestID == "" || claimToken == "" || limit < 1 || limit > 256 || ttl <= 0 {
		return RunPermit{}, false, errors.New("worker: valid Redis tenant run quota is required")
	}
	permit := RunPermit{TenantID: tenantID, member: permitMember(tenantID, requestID, claimToken)}
	result, err := limiter.Redis.Eval(ctx, acquireRunPermitLua, []string{limiter.key(tenantID)}, permit.member, limit, ttl.Milliseconds())
	if err != nil {
		return RunPermit{}, false, fmt.Errorf("worker: acquire tenant run quota: %w", err)
	}
	value, err := redisInteger(result)
	if err != nil {
		return RunPermit{}, false, err
	}
	return permit, value == 1, nil
}

func (limiter *RedisRunLimiter) Renew(ctx context.Context, permit RunPermit, ttl time.Duration) error {
	if limiter == nil || limiter.Redis == nil || permit.TenantID == "" || permit.member == "" || ttl <= 0 {
		return errors.New("worker: valid tenant run permit is required")
	}
	result, err := limiter.Redis.Eval(ctx, renewRunPermitLua, []string{limiter.key(permit.TenantID)}, permit.member, ttl.Milliseconds())
	if err != nil {
		return fmt.Errorf("worker: renew tenant run quota: %w", err)
	}
	value, err := redisInteger(result)
	if err != nil {
		return err
	}
	if value != 1 {
		return errors.New("worker: tenant run permit expired")
	}
	return nil
}

func (limiter *RedisRunLimiter) Release(permit RunPermit) {
	if limiter == nil || limiter.Redis == nil || permit.TenantID == "" || permit.member == "" {
		return
	}
	_, _ = limiter.Redis.Eval(context.Background(), releaseRunPermitLua, []string{limiter.key(permit.TenantID)}, permit.member)
}

func (limiter *RedisRunLimiter) key(tenantID string) string {
	prefix := limiter.Prefix
	if prefix == "" {
		prefix = "trpc-agent-service"
	}
	digest := sha256.Sum256([]byte(tenantID))
	return prefix + ":tenant-runs:" + hex.EncodeToString(digest[:])
}

func permitMember(tenantID, requestID, claimToken string) string {
	digest := sha256.Sum256([]byte(tenantID + "\x00" + requestID + "\x00" + claimToken))
	return hex.EncodeToString(digest[:])
}

func redisInteger(value any) (int64, error) {
	switch typed := value.(type) {
	case int64:
		return typed, nil
	case string:
		return strconv.ParseInt(typed, 10, 64)
	case []byte:
		return strconv.ParseInt(string(typed), 10, 64)
	default:
		return 0, fmt.Errorf("worker: unexpected Redis quota result %T", value)
	}
}

var _ RunLimiter = (*RedisRunLimiter)(nil)
