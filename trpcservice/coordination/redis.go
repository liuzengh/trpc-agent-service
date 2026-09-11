package coordination

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/observability"
	"github.com/redis/go-redis/v9"
)

const completedValue = "completed"

var sharedRateLimitScript = redis.NewScript(`
local server_time = redis.call("TIME")
local seconds = tonumber(server_time[1])
local window = math.floor(seconds / 60)
local key = KEYS[1] .. ":" .. window
local count = redis.call("INCR", key)
if count == 1 then
  redis.call("EXPIRE", key, 61)
end
local ttl = redis.call("TTL", key)
return {count, ttl}
`)

type tenantFreezeValue struct {
	MigrationID string    `json:"migration_id"`
	FrozenAt    time.Time `json:"frozen_at"`
}

var compareDelete = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

var compareRenew = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

var compareComplete = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  redis.call("SET", KEYS[1], ARGV[2], "PX", ARGV[3])
  if redis.call("EXISTS", KEYS[2]) == 1 then
    redis.call("PEXPIRE", KEYS[2], ARGV[3])
  end
  return 1
end
return 0
`)

var compareSaveResult = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  redis.call("SET", KEYS[2], ARGV[2], "PX", ARGV[3])
  return 1
end
return 0
`)

type Redis struct {
	client *redis.Client
	prefix string
}

var _ RateLimiter = (*Redis)(nil)

func NewRedis(rawURL, prefix string) (*Redis, error) {
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, errors.New("invalid coordination redis URL")
	}
	client := redis.NewClient(options)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, errors.New("coordination redis is unavailable")
	}
	return &Redis{client: client, prefix: prefix}, nil
}

func (r *Redis) Ping(ctx context.Context) error {
	if r == nil || r.client == nil {
		return errors.New("coordination redis unavailable")
	}
	return r.client.Ping(ctx).Err()
}

// Allow performs the rate check and increment in one Redis Lua invocation.
// Redis server time, rather than a worker's local clock, defines the window so
// two service nodes cannot drift into separate buckets.
func (r *Redis) Allow(ctx context.Context, tenantID string, limit int) (allowed bool, retryAfter time.Duration, err error) {
	ctx, finish := observability.StartStorage(ctx, "rate_limit.allow", "redis", tenantID, "")
	defer func() { finish(err) }()
	if tenantID == "" {
		return false, 0, errors.New("coordination: empty rate limiter tenant")
	}
	if limit <= 0 {
		return false, time.Minute, nil
	}
	value, err := sharedRateLimitScript.Run(ctx, r.client, []string{r.key("rate", tenantID)}, limit).Result()
	if err != nil {
		return false, 0, fmt.Errorf("%w: redis rate check", ErrRateLimiterUnavailable)
	}
	values, ok := value.([]interface{})
	if !ok || len(values) != 2 {
		return false, 0, fmt.Errorf("%w: invalid redis rate result", ErrRateLimiterUnavailable)
	}
	count, ok := redisInt64(values[0])
	if !ok {
		return false, 0, fmt.Errorf("%w: invalid redis rate count", ErrRateLimiterUnavailable)
	}
	ttl, ok := redisInt64(values[1])
	if !ok {
		return false, 0, fmt.Errorf("%w: invalid redis rate ttl", ErrRateLimiterUnavailable)
	}
	if count > int64(limit) {
		if ttl < 1 {
			ttl = 60
		}
		return false, time.Duration(ttl) * time.Second, nil
	}
	return true, 0, nil
}

func redisInt64(value any) (int64, bool) {
	switch typed := value.(type) {
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case string:
		var parsed int64
		if _, err := fmt.Sscan(typed, &parsed); err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func (r *Redis) Claim(ctx context.Context, key string, ttl time.Duration) (ClaimLease, error) {
	redisKey := r.key("dedup", key)
	owner := newOwnerToken()
	processingValue := "processing:" + owner
	ok, err := r.client.SetNX(ctx, redisKey, processingValue, ttl).Result()
	if err != nil {
		return ClaimLease{}, fmt.Errorf("claim message: %w", err)
	}
	if ok {
		return ClaimLease{State: Claimed, Token: owner}, nil
	}
	value, err := r.client.Get(ctx, redisKey).Result()
	if err != nil {
		return ClaimLease{}, fmt.Errorf("read message claim: %w", err)
	}
	if value == completedValue {
		return ClaimLease{State: AlreadyCompleted}, nil
	}
	return ClaimLease{State: AlreadyProcessing}, nil
}

func (r *Redis) Complete(ctx context.Context, key, ownerToken string, ttl time.Duration) error {
	result, err := compareComplete.Run(
		ctx,
		r.client,
		[]string{r.key("dedup", key), r.key("result", key)},
		"processing:"+ownerToken,
		completedValue,
		ttl.Milliseconds(),
	).Int64()
	if err != nil {
		return fmt.Errorf("complete message claim: %w", err)
	}
	if result != 1 {
		return ErrClaimOwnershipLost
	}
	return nil
}

func (r *Redis) ReleaseClaim(ctx context.Context, key, ownerToken string) error {
	_, err := compareDelete.Run(ctx, r.client, []string{r.key("dedup", key)}, "processing:"+ownerToken).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("release message claim: %w", err)
	}
	return nil
}

func (r *Redis) SaveResult(ctx context.Context, key, ownerToken string, value any, ttl time.Duration) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("encode pending result: %w", err)
	}
	result, err := compareSaveResult.Run(
		ctx,
		r.client,
		[]string{r.key("dedup", key), r.key("result", key)},
		"processing:"+ownerToken,
		encoded,
		ttl.Milliseconds(),
	).Int64()
	if err != nil {
		return fmt.Errorf("save pending result: %w", err)
	}
	if result != 1 {
		return ErrClaimOwnershipLost
	}
	return nil
}

func (r *Redis) LoadResult(ctx context.Context, key string, value any) (bool, error) {
	encoded, err := r.client.Get(ctx, r.key("result", key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load pending result: %w", err)
	}
	if err := json.Unmarshal(encoded, value); err != nil {
		return false, fmt.Errorf("decode pending result: %w", err)
	}
	return true, nil
}

func (r *Redis) DeleteResult(ctx context.Context, key string) error {
	if err := r.client.Del(ctx, r.key("result", key)).Err(); err != nil {
		return fmt.Errorf("delete pending result: %w", err)
	}
	return nil
}

// FreezeTenant installs a persistent distributed write fence. A different
// migration cannot steal it; retrying the same migration is idempotent.
func (r *Redis) FreezeTenant(ctx context.Context, tenantID, appName, migrationID string) (time.Time, error) {
	if tenantID == "" || appName == "" || migrationID == "" {
		return time.Time{}, errors.New("coordination: tenant freeze identity is required")
	}
	key := r.tenantFreezeKey(tenantID, appName)
	currentID, frozenAt, frozen, err := r.TenantFreeze(ctx, tenantID, appName)
	if err != nil {
		return time.Time{}, err
	}
	if frozen {
		if currentID != migrationID {
			return time.Time{}, errors.New("coordination: tenant is frozen by another migration")
		}
		return frozenAt, nil
	}
	value := tenantFreezeValue{MigrationID: migrationID, FrozenAt: time.Now().UTC()}
	encoded, err := json.Marshal(value)
	if err != nil {
		return time.Time{}, errors.New("coordination: encode tenant freeze")
	}
	ok, err := r.client.SetNX(ctx, key, encoded, 0).Result()
	if err != nil {
		return time.Time{}, errors.New("coordination: install tenant freeze")
	}
	if !ok {
		currentID, frozenAt, frozen, err = r.TenantFreeze(ctx, tenantID, appName)
		if err != nil || !frozen || currentID != migrationID {
			return time.Time{}, errors.New("coordination: tenant freeze raced with another migration")
		}
		return frozenAt, nil
	}
	return value.FrozenAt, nil
}

func (r *Redis) TenantFreeze(ctx context.Context, tenantID, appName string) (string, time.Time, bool, error) {
	if tenantID == "" || appName == "" {
		return "", time.Time{}, false, errors.New("coordination: tenant freeze identity is required")
	}
	encoded, err := r.client.Get(ctx, r.tenantFreezeKey(tenantID, appName)).Bytes()
	if errors.Is(err, redis.Nil) {
		return "", time.Time{}, false, nil
	}
	if err != nil {
		return "", time.Time{}, false, errors.New("coordination: read tenant freeze")
	}
	var value tenantFreezeValue
	if err := json.Unmarshal(encoded, &value); err != nil || value.MigrationID == "" || value.FrozenAt.IsZero() {
		return "", time.Time{}, false, errors.New("coordination: corrupt tenant freeze")
	}
	return value.MigrationID, value.FrozenAt, true, nil
}

// UnfreezeTenant removes only the calling migration's fence.
func (r *Redis) UnfreezeTenant(ctx context.Context, tenantID, appName, migrationID string) error {
	currentID, _, frozen, err := r.TenantFreeze(ctx, tenantID, appName)
	if err != nil || !frozen {
		return err
	}
	if currentID != migrationID {
		return errors.New("coordination: tenant freeze ownership lost")
	}
	encoded, err := r.client.Get(ctx, r.tenantFreezeKey(tenantID, appName)).Result()
	if err != nil {
		return errors.New("coordination: read tenant freeze for release")
	}
	if _, err := compareDelete.Run(ctx, r.client, []string{r.tenantFreezeKey(tenantID, appName)}, encoded).Result(); err != nil {
		return errors.New("coordination: release tenant freeze")
	}
	return nil
}

func (r *Redis) Lock(ctx context.Context, key string, ttl time.Duration) (*LockLease, error) {
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	redisKey := r.key("lock", key)
	token := newOwnerToken()
	for {
		ok, err := r.client.SetNX(ctx, redisKey, token, ttl).Result()
		if err != nil {
			return nil, fmt.Errorf("acquire redis lock: %w", err)
		}
		if ok {
			break
		}
		jitter := time.Duration(30+rand.Intn(40)) * time.Millisecond //nolint:gosec
		timer := time.NewTimer(jitter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("acquire redis lock: %w", ctx.Err())
		case <-timer.C:
		}
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	lost := make(chan error, 1)
	go func() {
		defer close(done)
		renewEvery := ttl / 3
		if renewEvery <= 0 {
			renewEvery = time.Nanosecond
		}
		ticker := time.NewTicker(renewEvery)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				renewCtx, cancel := context.WithTimeout(context.Background(), ttl/4)
				renewed, err := compareRenew.Run(renewCtx, r.client, []string{redisKey}, token, ttl.Milliseconds()).Int64()
				cancel()
				if err != nil {
					lost <- fmt.Errorf("%w: renew redis lock: %v", ErrLockOwnershipLost, err)
					return
				}
				if renewed != 1 {
					lost <- ErrLockOwnershipLost
					return
				}
			}
		}
	}()
	var once sync.Once
	release := func() {
		once.Do(func() {
			close(stop)
			<-done
			releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, _ = compareDelete.Run(releaseCtx, r.client, []string{redisKey}, token).Result()
		})
	}
	return &LockLease{Release: release, Lost: lost}, nil
}

func (r *Redis) key(kind, key string) string {
	return r.prefix + ":" + kind + ":" + key
}

func (r *Redis) tenantFreezeKey(tenantID, appName string) string {
	return r.key("tenant-freeze", tenantID+":"+appName)
}

func (r *Redis) Close() error { return r.client.Close() }
