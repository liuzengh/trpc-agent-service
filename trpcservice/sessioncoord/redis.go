package sessioncoord

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

// RedisEvaler is the small go-redis-compatible surface required by RedisCoordinator.
type RedisEvaler interface {
	Eval(context.Context, string, []string, ...any) (any, error)
}

// RedisCoordinator uses atomic Lua scripts and a persistent INCR counter to
// issue monotonic fencing tokens across Worker nodes.
type RedisCoordinator struct {
	Redis  RedisEvaler
	Fencer FenceAdvancer
	Prefix string
}

const acquireLua = `
local live = redis.call('GET', KEYS[1])
if live then return {0, live} end
local floor = tonumber(ARGV[3]) or 0
local current = tonumber(redis.call('GET', KEYS[2]) or '0') or 0
if current < floor then redis.call('SET', KEYS[2], floor) end
local token = redis.call('INCR', KEYS[2])
local value = ARGV[1] .. '|' .. token
redis.call('PSETEX', KEYS[1], ARGV[2], value)
return {token, value}`

const renewLua = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1`

const releaseLua = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])`

// Acquire claims the lease and advances the persistent write fence before execution.
func (coordinator *RedisCoordinator) Acquire(ctx context.Context, key gateway.SessionKey, owner string, ttl time.Duration) (Lease, error) {
	if coordinator == nil || coordinator.Redis == nil || coordinator.Fencer == nil || owner == "" || ttl <= 0 {
		return Lease{}, errors.New("sessioncoord: redis, fencer, owner, and positive ttl are required")
	}
	leaseKey, counterKey := coordinator.keys(key)
	var floor uint64
	var err error
	if reader, ok := coordinator.Fencer.(FenceReader); ok {
		floor, err = reader.CurrentFence(ctx, key)
		if err != nil {
			return Lease{}, err
		}
	}
	result, err := coordinator.Redis.Eval(ctx, acquireLua, []string{leaseKey, counterKey}, owner, ttl.Milliseconds(), floor)
	if err != nil {
		return Lease{}, err
	}
	values, ok := result.([]any)
	if !ok || len(values) != 2 {
		return Lease{}, fmt.Errorf("sessioncoord: unexpected Redis acquire result %T", result)
	}
	token, err := redisUint(values[0])
	if err != nil {
		return Lease{}, err
	}
	if token == 0 {
		return Lease{}, ErrLeaseHeld
	}
	lease := Lease{Key: key, Owner: owner, Token: token, ExpiresAt: time.Now().UTC().Add(ttl)}
	if err := coordinator.Fencer.AdvanceFence(ctx, key, token); err != nil {
		_, _ = coordinator.Redis.Eval(context.Background(), releaseLua, []string{leaseKey}, redisLeaseValue(lease))
		return Lease{}, err
	}
	return lease, nil
}

// Renew extends only the exact owner/token value.
func (coordinator *RedisCoordinator) Renew(ctx context.Context, lease Lease, ttl time.Duration) (Lease, error) {
	if ttl <= 0 {
		return Lease{}, errors.New("sessioncoord: positive ttl is required")
	}
	leaseKey, _ := coordinator.keys(lease.Key)
	result, err := coordinator.Redis.Eval(ctx, renewLua, []string{leaseKey}, redisLeaseValue(lease), ttl.Milliseconds())
	if err != nil {
		return Lease{}, err
	}
	ok, err := redisUint(result)
	if err != nil || ok != 1 {
		return Lease{}, ErrStaleFence
	}
	lease.ExpiresAt = time.Now().UTC().Add(ttl)
	return lease, nil
}

// Release removes only the exact owner/token value and never decrements the counter.
func (coordinator *RedisCoordinator) Release(lease Lease) {
	if coordinator == nil || coordinator.Redis == nil {
		return
	}
	leaseKey, _ := coordinator.keys(lease.Key)
	_, _ = coordinator.Redis.Eval(context.Background(), releaseLua, []string{leaseKey}, redisLeaseValue(lease))
}

func (coordinator *RedisCoordinator) keys(key gateway.SessionKey) (string, string) {
	prefix := coordinator.Prefix
	if prefix == "" {
		prefix = "trpc-agent-service"
	}
	digest := sha256.Sum256([]byte(key.TenantID + "\x00" + key.AppID + "\x00" + key.UserID + "\x00" + key.SessionID))
	suffix := hex.EncodeToString(digest[:])
	// Both keys must share a Redis Cluster hash tag because acquireLua reads
	// and updates them atomically.
	tag := "{" + suffix + "}"
	return prefix + ":session-lease:" + tag, prefix + ":session-fence:" + tag
}
func redisLeaseValue(lease Lease) string {
	return lease.Owner + "|" + strconv.FormatUint(lease.Token, 10)
}
func redisUint(value any) (uint64, error) {
	switch typed := value.(type) {
	case int64:
		if typed < 0 {
			return 0, errors.New("sessioncoord: negative Redis integer")
		}
		return uint64(typed), nil
	case uint64:
		return typed, nil
	case string:
		return strconv.ParseUint(typed, 10, 64)
	case []byte:
		return strconv.ParseUint(string(typed), 10, 64)
	default:
		return 0, fmt.Errorf("sessioncoord: unexpected Redis integer %T", value)
	}
}
