// Package credential manages WeCom access tokens with shared caching, early
// refresh, concurrency control, and expired-token recovery.
package credential

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// TokenCache is the cache seam: Memory (local/tests) and Redis (shared nodes).
type TokenCache interface {
	Get(context.Context, string) (string, bool)
	Set(context.Context, string, string, time.Duration)
}

// MemoryTokenCache is the in-process implementation (tests/single node).
type MemoryTokenCache struct {
	mu      sync.Mutex
	entries map[string]memoryToken
	now     func() time.Time
}

type memoryToken struct {
	value     string
	expiresAt time.Time
}

// NewMemoryTokenCache constructs an isolated local cache.
func NewMemoryTokenCache() *MemoryTokenCache {
	return &MemoryTokenCache{entries: make(map[string]memoryToken), now: time.Now}
}

// Get implements TokenCache.
func (c *MemoryTokenCache) Get(ctx context.Context, key string) (string, bool) {
	if ctx.Err() != nil {
		return "", false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, exists := c.entries[key]
	if !exists {
		return "", false
	}
	if !entry.expiresAt.After(c.now()) {
		delete(c.entries, key)
		return "", false
	}
	return entry.value, true
}

// Set implements TokenCache.
func (c *MemoryTokenCache) Set(ctx context.Context, key string, value string, ttl time.Duration) {
	if ctx.Err() != nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = memoryToken{value: value, expiresAt: c.now().Add(ttl)}
}

// RedisTokenCache is the multi-node shared implementation.
type RedisTokenCache struct {
	client redis.UniversalClient
}

// NewRedisTokenCache constructs the shared cache over the injected client.
func NewRedisTokenCache(client redis.UniversalClient) (*RedisTokenCache, error) {
	if client == nil {
		return nil, errors.New("token cache Redis client is required")
	}
	return &RedisTokenCache{client: client}, nil
}

// Get implements TokenCache.
func (c *RedisTokenCache) Get(ctx context.Context, key string) (string, bool) {
	value, err := c.client.Get(ctx, key).Result()
	if err != nil || value == "" {
		return "", false
	}
	return value, true
}

// Set implements TokenCache.
func (c *RedisTokenCache) Set(ctx context.Context, key string, value string, ttl time.Duration) {
	_ = c.client.Set(ctx, key, value, ttl).Err()
}

// IsExpiredAccessTokenError reports whether a provider errcode means the
// access_token is invalid (40014) or expired (42001).
func IsExpiredAccessTokenError(errcode int64) bool {
	return errcode == 40014 || errcode == 42001
}

// Manager fetches and caches one provider access_token with the mature
// governance pattern: cache-first, mutex + double-check on miss, TTL of
// expires_in-1500s (refresh ~25min before expiry), and explicit invalidation
// so callers can retry after expired-token errors.
type Manager struct {
	corpID   string
	secret   string
	cacheKey string
	baseURL  string
	client   *http.Client
	cache    TokenCache
	mu       sync.Mutex
}

// NewManager constructs a token manager for the given provider credentials.
func NewManager(corpID, corpSecret, baseURL string, client *http.Client, cache TokenCache) *Manager {
	return &Manager{
		corpID: corpID, secret: corpSecret, cacheKey: accessTokenCacheKey(corpID, corpSecret), baseURL: strings.TrimRight(baseURL, "/"),
		client: client, cache: cache,
	}
}

func accessTokenCacheKey(corpID, secret string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(corpID) + "\x00" + secret))
	return fmt.Sprintf("wecom:access_token:%x", sum[:12])
}

// Token returns a valid token, fetching and caching on first use.
func (m *Manager) Token(ctx context.Context) (string, error) {
	if cached, ok := m.cache.Get(ctx, m.cacheKey); ok && cached != "" {
		return cached, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if cached, ok := m.cache.Get(ctx, m.cacheKey); ok && cached != "" {
		return cached, nil
	}
	accessToken, ttl, err := m.fetch(ctx)
	if err != nil {
		return "", err
	}
	m.cache.Set(ctx, m.cacheKey, accessToken, ttl)
	return accessToken, nil
}

// Invalidate drops the cached token after an expired-token error.
func (m *Manager) Invalidate(ctx context.Context) {
	m.cache.Set(ctx, m.cacheKey, "", time.Nanosecond)
}

func (m *Manager) fetch(ctx context.Context) (string, time.Duration, error) {
	endpoint := m.baseURL + "/cgi-bin/gettoken?" + url.Values{
		"corpid":     {m.corpID},
		"corpsecret": {m.secret},
	}.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", 0, fmt.Errorf("build gettoken request: %w", err)
	}
	response, err := m.client.Do(request)
	if err != nil {
		return "", 0, fmt.Errorf("call gettoken: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("gettoken HTTP %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return "", 0, fmt.Errorf("read gettoken response: %w", err)
	}
	var payload struct {
		Errcode     int    `json:"errcode"`
		Errmsg      string `json:"errmsg"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", 0, fmt.Errorf("decode gettoken response: %w", err)
	}
	if payload.Errcode != 0 {
		return "", 0, fmt.Errorf("gettoken error %d: %s", payload.Errcode, payload.Errmsg)
	}
	if payload.AccessToken == "" {
		return "", 0, errors.New("gettoken returned empty access_token")
	}
	ttl := time.Duration(payload.ExpiresIn-1500) * time.Second
	if ttl <= 0 {
		ttl = time.Minute
	}
	return payload.AccessToken, ttl, nil
}
