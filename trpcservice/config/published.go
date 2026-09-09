package config

import (
	"bytes"
	"context"
	"errors"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const defaultVersionCacheLimit = 256

// PublishedCache loads published tenant configurations from the control-plane
// store. Versions are immutable, so parsed files are cached forever; the
// tenant head is re-read on every Current call so a publish takes effect
// without a restart.
type PublishedCache struct {
	store      repository.Store
	maxEntries int
	validator  func(*File) error
	mu         sync.Mutex
	versions   map[publishedKey]*File
	order      []publishedKey
}

// SetValidator installs a read-time gate for immutable records that may have
// been published before the current production policy existed. It must be
// called during bootstrap, before the cache is shared with request handlers.
func (cache *PublishedCache) SetValidator(validator func(*File) error) {
	if cache == nil {
		return
	}
	cache.mu.Lock()
	cache.validator = validator
	cache.versions = make(map[publishedKey]*File)
	cache.order = nil
	cache.mu.Unlock()
}

type publishedKey struct {
	tenantID string
	version  tenant.ConfigVersion
}

// NewPublishedCache creates a cache over the control-plane store.
func NewPublishedCache(store repository.Store) (*PublishedCache, error) {
	return NewPublishedCacheWithLimit(store, defaultVersionCacheLimit)
}

// NewPublishedCacheWithLimit creates a published cache with an explicit
// maximum number of immutable version files kept in memory. A cache miss simply
// reloads the version from the control-plane store, so eviction cannot change
// correctness and bounds memory as tenants publish indefinitely.
func NewPublishedCacheWithLimit(store repository.Store, maxEntries int) (*PublishedCache, error) {
	if store == nil {
		return nil, errors.New("config: nil control-plane store")
	}
	if maxEntries <= 0 {
		return nil, errors.New("config: published cache limit must be positive")
	}
	return &PublishedCache{store: store, maxEntries: maxEntries, versions: make(map[publishedKey]*File)}, nil
}

// Version returns the immutable published file pinned at one version.
func (cache *PublishedCache) Version(ctx context.Context, tenantID string, version tenant.ConfigVersion) (*File, error) {
	key := publishedKey{tenantID: tenantID, version: version}
	cache.mu.Lock()
	if file, ok := cache.versions[key]; ok {
		cache.mu.Unlock()
		return file, nil
	}
	cache.mu.Unlock()
	record, err := cache.store.GetConfigVersion(ctx, tenantID, version)
	if err != nil {
		return nil, err
	}
	file, err := Load(bytes.NewReader(record.Payload))
	if err != nil {
		return nil, err
	}
	cache.mu.Lock()
	validator := cache.validator
	cache.mu.Unlock()
	if validator != nil {
		if err := validator(file); err != nil {
			return nil, errors.New("config: published version violates the active deployment policy")
		}
	}
	cache.mu.Lock()
	if existing, ok := cache.versions[key]; ok {
		cache.mu.Unlock()
		return existing, nil
	}
	if len(cache.versions) >= cache.maxEntries {
		oldest := cache.order[0]
		cache.order = cache.order[1:]
		delete(cache.versions, oldest)
	}
	cache.versions[key] = file
	cache.order = append(cache.order, key)
	cache.mu.Unlock()
	return file, nil
}

// Current returns the tenant's published head. The head lookup is never
// cached; only the resolved immutable version is.
func (cache *PublishedCache) Current(ctx context.Context, tenantID string) (*File, error) {
	record, err := cache.store.GetCurrentConfig(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	return cache.Version(ctx, tenantID, record.Version)
}
