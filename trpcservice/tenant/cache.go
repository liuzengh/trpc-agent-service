package tenant

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type cacheEntry struct {
	snapshot Snapshot
	expires  time.Time
}

// SnapshotCache is the Worker data-plane cache. It never dereferences secrets.
type SnapshotCache struct {
	store ConfigStore
	ttl   time.Duration
	now   func() time.Time

	mu      sync.RWMutex
	entries map[string]cacheEntry
}

// NewConfigCache constructs a TTL cache around the authoritative store.
func NewConfigCache(store ConfigStore, ttl time.Duration) (*SnapshotCache, error) {
	if store == nil {
		return nil, errors.New("config store is required")
	}
	if ttl <= 0 {
		return nil, errors.New("config cache TTL must be positive")
	}
	return &SnapshotCache{
		store:   store,
		ttl:     ttl,
		now:     time.Now,
		entries: make(map[string]cacheEntry),
	}, nil
}

// ResolveBinding returns a snapshot from cache or loads a consistent view from
// the control-plane store. Returned maps and slices are defensive copies.
func (c *SnapshotCache) ResolveBinding(ctx context.Context, channel, routeKey string) (Snapshot, error) {
	key := cacheKey(channel, routeKey)
	now := c.now()

	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()
	if ok && now.Before(entry.expires) {
		return cloneSnapshot(entry.snapshot), nil
	}

	binding, err := c.store.GetBindingByRoute(ctx, channel, routeKey)
	if err != nil {
		return Snapshot{}, err
	}
	if !binding.IsActive {
		return Snapshot{}, fmt.Errorf("binding %q: %w", binding.ID, ErrInactive)
	}
	tenantValue, err := c.store.GetTenant(ctx, binding.TenantID)
	if err != nil {
		return Snapshot{}, err
	}
	if !tenantValue.IsActive {
		return Snapshot{}, fmt.Errorf("tenant %q: %w", tenantValue.ID, ErrInactive)
	}
	app, err := c.store.GetCurrentApp(ctx, binding.AppID)
	if err != nil {
		return Snapshot{}, err
	}
	if app.TenantID != tenantValue.ID || binding.TenantID != tenantValue.ID || app.ID != binding.AppID {
		return Snapshot{}, fmt.Errorf(
			"binding %q tenant/app ownership mismatch", binding.ID,
		)
	}

	snapshot := Snapshot{Tenant: tenantValue, App: app, Binding: binding}
	c.mu.Lock()
	c.entries[key] = cacheEntry{snapshot: cloneSnapshot(snapshot), expires: now.Add(c.ttl)}
	c.mu.Unlock()
	return cloneSnapshot(snapshot), nil
}

// Invalidate evicts one route after a control-plane update.
func (c *SnapshotCache) Invalidate(channel, routeKey string) {
	c.mu.Lock()
	delete(c.entries, cacheKey(channel, routeKey))
	c.mu.Unlock()
}

func cacheKey(channel, routeKey string) string {
	return channel + "\x00" + routeKey
}

func cloneSnapshot(source Snapshot) Snapshot {
	result := source
	result.Tenant.Policy.RedactPatterns = append([]string(nil), source.Tenant.Policy.RedactPatterns...)
	result.App.Tools = append([]string(nil), source.App.Tools...)
	if source.Binding.Config != nil {
		result.Binding.Config = make(map[string]string, len(source.Binding.Config))
		for key, value := range source.Binding.Config {
			result.Binding.Config[key] = value
		}
	}
	return result
}
