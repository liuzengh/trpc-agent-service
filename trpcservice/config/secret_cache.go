package config

import (
	"context"
	"sync"
	"time"
)

// CachedResolver wraps a SecretResolver with a short-TTL in-process cache.
// Secret values must never enter logs. A backend failure serves the last
// known value; rotation uses a new ref, so staleness within the TTL is safe.
type CachedResolver struct {
	inner SecretResolver
	ttl   time.Duration

	mu    sync.Mutex
	cache map[string]cachedSecret
}

type cachedSecret struct {
	value   string
	freshAt time.Time
}

// NewCachedResolver wraps inner; ttl <= 0 defaults to one minute.
func NewCachedResolver(inner SecretResolver, ttl time.Duration) *CachedResolver {
	if ttl <= 0 {
		ttl = time.Minute
	}
	return &CachedResolver{inner: inner, ttl: ttl, cache: make(map[string]cachedSecret)}
}

// Resolve implements SecretResolver. The plaintext never enters logs; cache
// entries are keyed by ref only.
func (c *CachedResolver) Resolve(ctx context.Context, ref string) (string, error) {
	c.mu.Lock()
	entry, ok := c.cache[ref]
	c.mu.Unlock()
	if ok && time.Since(entry.freshAt) < c.ttl {
		return entry.value, nil
	}
	v, err := c.inner.Resolve(ctx, ref)
	if err != nil {
		if ok {
			// Backend down: serve the stale value.
			return entry.value, nil
		}
		return "", err
	}
	c.mu.Lock()
	c.cache[ref] = cachedSecret{value: v, freshAt: time.Now()}
	c.mu.Unlock()
	return v, nil
}
