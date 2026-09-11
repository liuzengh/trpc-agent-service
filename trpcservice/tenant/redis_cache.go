package tenant

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisCache shares immutable configuration snapshots across gateway and worker
// nodes. Every cache key is already tenant/version scoped by CachedRepository.
type RedisCache struct{ client redis.UniversalClient }

func NewRedisCache(client redis.UniversalClient) (*RedisCache, error) {
	if client == nil {
		return nil, fmt.Errorf("Redis client is required")
	}
	return &RedisCache{client: client}, nil
}

func (c *RedisCache) Get(ctx context.Context, key string) (Snapshot, bool, error) {
	raw, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return Snapshot{}, false, nil
	}
	if err != nil {
		return Snapshot{}, false, fmt.Errorf("read tenant configuration cache: %w", err)
	}
	var snapshot Snapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return Snapshot{}, false, fmt.Errorf("decode tenant configuration cache: %w", err)
	}
	cloned, err := cloneSnapshot(snapshot)
	if err != nil {
		return Snapshot{}, false, err
	}
	return cloned, true, nil
}

func (c *RedisCache) Set(ctx context.Context, key string, snapshot Snapshot, ttl time.Duration) error {
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode tenant configuration cache: %w", err)
	}
	if err := c.client.Set(ctx, key, encoded, ttl).Err(); err != nil {
		return fmt.Errorf("write tenant configuration cache: %w", err)
	}
	return nil
}

func (c *RedisCache) Delete(ctx context.Context, key string) error {
	if err := c.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("delete tenant configuration cache: %w", err)
	}
	return nil
}

var _ Cache = (*RedisCache)(nil)
