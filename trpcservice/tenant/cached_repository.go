package tenant

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

// Cache stores immutable tenant configuration snapshots. A production adapter
// should use a shared cache such as Redis; an unavailable cache is reported to
// callers instead of silently changing consistency semantics.
type Cache interface {
	Get(ctx context.Context, key string) (Snapshot, bool, error)
	Set(ctx context.Context, key string, snapshot Snapshot, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}

// CachedRepository decorates a control-plane repository with versioned snapshot
// caching. It never mutates a snapshot after publication.
type CachedRepository struct {
	source Repository
	cache  Cache
	ttl    time.Duration
}

type cacheInvalidationOutboxSource interface {
	UsesConfigCacheInvalidationOutbox() bool
}

// NewCachedRepository constructs a repository that fails explicitly when its
// cache cannot uphold the required read or invalidation operation.
func NewCachedRepository(source Repository, cache Cache, ttl time.Duration) (*CachedRepository, error) {
	if source == nil {
		return nil, fmt.Errorf("tenant configuration source repository is required")
	}
	if cache == nil {
		return nil, fmt.Errorf("tenant configuration cache is required")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("tenant configuration cache TTL must be positive")
	}
	return &CachedRepository{source: source, cache: cache, ttl: ttl}, nil
}

// GetActive returns the latest active configuration for one tenant application.
func (r *CachedRepository) GetActive(ctx context.Context, tenantID, appCode string) (Snapshot, error) {
	key := activeCacheKey(tenantID, appCode)
	if snapshot, found, err := r.cache.Get(ctx, key); err != nil {
		return Snapshot{}, fmt.Errorf("get active tenant configuration cache: %w", err)
	} else if found {
		return snapshot, nil
	}

	snapshot, err := r.source.GetActive(ctx, tenantID, appCode)
	if err != nil {
		return Snapshot{}, err
	}
	if err := r.cache.Set(ctx, key, snapshot, r.ttl); err != nil {
		return Snapshot{}, fmt.Errorf("set active tenant configuration cache: %w", err)
	}
	return snapshot, nil
}

func (r *CachedRepository) GetCandidate(ctx context.Context, tenantID, appCode string) (Snapshot, error) {
	snapshot, err := r.source.GetCandidate(ctx, tenantID, appCode)
	if err != nil {
		return Snapshot{}, err
	}
	key := versionCacheKey(tenantID, appCode, snapshot.Config.ConfigVersion)
	if err := r.cache.Set(ctx, key, snapshot, r.ttl); err != nil {
		return Snapshot{}, fmt.Errorf("cache candidate tenant configuration: %w", err)
	}
	return snapshot, nil
}

// GetVersion returns one immutable configuration version. Its cache key includes
// the version, so publishing a newer version cannot make this entry stale.
func (r *CachedRepository) GetVersion(ctx context.Context, tenantID, appCode string, version uint64) (Snapshot, error) {
	key := versionCacheKey(tenantID, appCode, version)
	if snapshot, found, err := r.cache.Get(ctx, key); err != nil {
		return Snapshot{}, fmt.Errorf("get versioned tenant configuration cache: %w", err)
	} else if found {
		return snapshot, nil
	}

	snapshot, err := r.source.GetVersion(ctx, tenantID, appCode, version)
	if err != nil {
		return Snapshot{}, err
	}
	if err := r.cache.Set(ctx, key, snapshot, r.ttl); err != nil {
		return Snapshot{}, fmt.Errorf("set versioned tenant configuration cache: %w", err)
	}
	return snapshot, nil
}

func (r *CachedRepository) ListVersions(ctx context.Context, tenantID, appCode string, limit int) ([]Snapshot, error) {
	return r.source.ListVersions(ctx, tenantID, appCode, limit)
}

func (r *CachedRepository) Stage(ctx context.Context, tenantConfig config.TenantConfig) (Snapshot, error) {
	staged, err := r.source.Stage(ctx, tenantConfig)
	if err != nil {
		return Snapshot{}, err
	}
	if err := r.cache.Set(ctx, versionCacheKey(tenantConfig.TenantID, tenantConfig.AppCode, tenantConfig.ConfigVersion), staged, r.ttl); err != nil {
		return Snapshot{}, fmt.Errorf("cache staged tenant configuration: %w", err)
	}
	return staged, nil
}

func (r *CachedRepository) GetRollout(ctx context.Context, tenantID, appCode string) (RolloutPolicy, error) {
	return r.source.GetRollout(ctx, tenantID, appCode)
}

func (r *CachedRepository) SetRollout(ctx context.Context, tenantID, appCode string, update RolloutUpdate) (RolloutPolicy, error) {
	return r.source.SetRollout(ctx, tenantID, appCode, update)
}

func (r *CachedRepository) StopRollout(ctx context.Context, tenantID, appCode string, expectedGeneration uint64) error {
	return r.source.StopRollout(ctx, tenantID, appCode, expectedGeneration)
}

func (r *CachedRepository) DiscardCandidate(ctx context.Context, tenantID, appCode string, expectedVersion uint64) error {
	return r.source.DiscardCandidate(ctx, tenantID, appCode, expectedVersion)
}

func (r *CachedRepository) PromoteCandidate(ctx context.Context, tenantID, appCode string, expectedVersion, expectedGeneration uint64) (Snapshot, error) {
	previous, err := r.source.GetActive(ctx, tenantID, appCode)
	if err != nil {
		return Snapshot{}, err
	}
	promoted, err := r.source.PromoteCandidate(ctx, tenantID, appCode, expectedVersion, expectedGeneration)
	if err != nil {
		return Snapshot{}, err
	}
	if source, durable := r.source.(cacheInvalidationOutboxSource); durable && source.UsesConfigCacheInvalidationOutbox() {
		return promoted, nil
	}
	keys := []string{activeCacheKey(tenantID, appCode)}
	keys = append(keys, bindingCacheKeys(previous.Config)...)
	keys = append(keys, bindingCacheKeys(promoted.Config)...)
	for _, key := range uniqueKeys(keys) {
		if err := r.cache.Delete(ctx, key); err != nil {
			return Snapshot{}, fmt.Errorf("invalidate promoted tenant configuration cache: %w", err)
		}
	}
	return promoted, nil
}

// ResolveBinding returns the currently active configuration for an external IM
// binding. Publishing invalidates both the old and new active binding keys.
func (r *CachedRepository) ResolveBinding(ctx context.Context, channel channels.Channel, bindingID string) (Snapshot, error) {
	key := bindingCacheKey(channel, bindingID)
	if snapshot, found, err := r.cache.Get(ctx, key); err != nil {
		return Snapshot{}, fmt.Errorf("get channel binding cache: %w", err)
	} else if found {
		return snapshot, nil
	}

	snapshot, err := r.source.ResolveBinding(ctx, channel, bindingID)
	if err != nil {
		return Snapshot{}, err
	}
	if err := r.cache.Set(ctx, key, snapshot, r.ttl); err != nil {
		return Snapshot{}, fmt.Errorf("set channel binding cache: %w", err)
	}
	return snapshot, nil
}

// Publish activates a newer configuration and invalidates every cache key whose
// meaning can change. T10 will persist invalidation retries through the Outbox.
func (r *CachedRepository) Publish(ctx context.Context, tenantConfig config.TenantConfig) (Snapshot, error) {
	previous, previousErr := r.source.GetActive(ctx, tenantConfig.TenantID, tenantConfig.AppCode)
	if previousErr != nil && !errors.Is(previousErr, ErrNotFound) {
		return Snapshot{}, previousErr
	}

	published, err := r.source.Publish(ctx, tenantConfig)
	if err != nil {
		return Snapshot{}, err
	}
	if source, durable := r.source.(cacheInvalidationOutboxSource); durable && source.UsesConfigCacheInvalidationOutbox() {
		// PostgresRepository committed cache invalidation in the same
		// transaction. Redis is updated asynchronously so a transient cache
		// outage cannot make this already-committed publication look failed.
		return published, nil
	}

	keys := []string{activeCacheKey(tenantConfig.TenantID, tenantConfig.AppCode)}
	keys = append(keys, bindingCacheKeys(published.Config)...)
	if previousErr == nil {
		keys = append(keys, bindingCacheKeys(previous.Config)...)
	}
	for _, key := range uniqueKeys(keys) {
		if err := r.cache.Delete(ctx, key); err != nil {
			return Snapshot{}, fmt.Errorf("invalidate tenant configuration cache: %w", err)
		}
	}
	return published, nil
}

func activeCacheKey(tenantID, appCode string) string {
	return "tenant-config:active:" + encodedKey(tenantID, appCode)
}

func versionCacheKey(tenantID, appCode string, version uint64) string {
	return fmt.Sprintf("tenant-config:version:%s:%d", encodedKey(tenantID, appCode), version)
}

func bindingCacheKey(channel channels.Channel, bindingID string) string {
	return "tenant-config:binding:" + encodedKey(string(channel), bindingID)
}

func bindingCacheKeys(tenantConfig config.TenantConfig) []string {
	keys := make([]string, 0, len(tenantConfig.Channels))
	for _, binding := range tenantConfig.Channels {
		keys = append(keys, bindingCacheKey(channels.Channel(binding.Type), binding.BindingID))
	}
	return keys
}

func encodedKey(parts ...string) string {
	var builder strings.Builder
	for _, part := range parts {
		fmt.Fprintf(&builder, "%d:%s", len(part), part)
	}
	return builder.String()
}

func uniqueKeys(keys []string) []string {
	unique := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, key)
	}
	return unique
}

// MemoryCache is a process-local cache for deterministic tests and local
// development. It is injected per caller and is not a durable production cache.
type MemoryCache struct {
	mu      sync.Mutex
	entries map[string]memoryCacheEntry
	now     func() time.Time
}

type memoryCacheEntry struct {
	snapshot  Snapshot
	expiresAt time.Time
}

// NewMemoryCache constructs an empty per-instance cache.
func NewMemoryCache() *MemoryCache {
	return &MemoryCache{
		entries: make(map[string]memoryCacheEntry),
		now:     time.Now,
	}
}

// Get retrieves a non-expired immutable snapshot.
func (c *MemoryCache) Get(ctx context.Context, key string) (Snapshot, bool, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, found := c.entries[key]
	if !found {
		return Snapshot{}, false, nil
	}
	if !entry.expiresAt.After(c.now()) {
		delete(c.entries, key)
		return Snapshot{}, false, nil
	}
	snapshot, err := cloneSnapshot(entry.snapshot)
	if err != nil {
		return Snapshot{}, false, err
	}
	return snapshot, true, nil
}

// Set stores a copy of an immutable snapshot with a bounded lifetime.
func (c *MemoryCache) Set(ctx context.Context, key string, snapshot Snapshot, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ttl <= 0 {
		return fmt.Errorf("cache TTL must be positive")
	}
	cloned, err := cloneSnapshot(snapshot)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = memoryCacheEntry{snapshot: cloned, expiresAt: c.now().Add(ttl)}
	return nil
}

// Delete removes a cache entry. Repeated deletes are safe.
func (c *MemoryCache) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
	return nil
}

// ListApplications delegates list reads to the durable source. Cross-tenant
// authorization is applied by the console layer, so this view is not cached.
func (r *CachedRepository) ListApplications(ctx context.Context, tenantID string) ([]Snapshot, error) {
	return r.source.ListApplications(ctx, tenantID)
}
