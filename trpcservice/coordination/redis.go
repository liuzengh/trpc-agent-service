// Package coordination provides the Redis-backed coordination primitives that
// make callback handling safe across multiple gateway processes.
package coordination

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Coordinator deduplicates IM deliveries and serializes one session across
// replicas. The interface makes the memory-mode fallback explicit in Gateway.
type Coordinator interface {
	Claim(context.Context, string, time.Duration) (bool, error)
	Lock(context.Context, string, time.Duration) (func(), error)
}

// StateStore persists small channel checkpoints such as a WeChat KF cursor.
// Values are intentionally opaque to keep protocol details out of Redis keys.
type StateStore interface {
	Get(context.Context, string) (string, error)
	Set(context.Context, string, string) error
	Delete(context.Context, string) error
}

// Redis is a small namespace over an existing Redis deployment. It keeps
// coordination keys separate from framework session keys while allowing both
// to share one server and lifecycle.
type Redis struct {
	client *redis.Client
	prefix string
}

// NewRedis opens a Redis coordinator. The first command is deliberately left
// to the caller's normal request/startup flow; New does not create state.
func NewRedis(url, prefix string) (*Redis, error) {
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	return &Redis{client: redis.NewClient(opts), prefix: strings.TrimSuffix(prefix, ":") + ":coord:"}, nil
}

// Close releases the underlying client.
func (r *Redis) Close() error { return r.client.Close() }

// Claim atomically records an IM delivery id. false means another replica has
// already accepted it inside ttl; callers should ACK but not run the agent.
func (r *Redis) Claim(ctx context.Context, id string, ttl time.Duration) (bool, error) {
	if id == "" {
		return true, nil
	}
	return r.client.SetNX(ctx, r.prefix+"dedup:"+id, "1", ttl).Result()
}

func (r *Redis) Get(ctx context.Context, key string) (string, error) {
	v, err := r.client.Get(ctx, r.prefix+"state:"+key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return v, err
}

func (r *Redis) Set(ctx context.Context, key, value string) error {
	return r.client.Set(ctx, r.prefix+"state:"+key, value, 0).Err()
}

func (r *Redis) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, r.prefix+"state:"+key).Err()
}

// Lock waits for a lease on key. The token-checked Lua release prevents an
// expired holder from deleting a newer holder's lease. A bounded request
// context is required by Gateway, so retrying cannot outlive the message.
func (r *Redis) Lock(ctx context.Context, key string, ttl time.Duration) (func(), error) {
	token, err := randomToken()
	if err != nil {
		return nil, err
	}
	lockKey := r.prefix + "lock:" + key
	for {
		ok, err := r.client.SetNX(ctx, lockKey, token, ttl).Result()
		if err != nil {
			return nil, err
		}
		if ok {
			return func() {
				const release = `if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) end return 0`
				_, _ = r.client.Eval(context.Background(), release, []string{lockKey}, token).Result()
			}, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func randomToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("random lock token: %w", err)
	}
	return hex.EncodeToString(b), nil
}
