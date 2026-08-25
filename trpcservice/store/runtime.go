package store

import (
	"context"
	"sync"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
)

type RuntimeProfileResolver interface {
	Resolve(context.Context, string) (tenant.RuntimeProfile, error)
	Run(context.Context) error
}

type cachedRuntime struct {
	profile   tenant.RuntimeProfile
	expiresAt time.Time
}

// CachedRuntimeResolver combines PostgreSQL LISTEN/NOTIFY invalidation with a
// bounded polling fallback. Every cache entry expires even if a notification
// is lost or the listener connection is interrupted.
type CachedRuntimeResolver struct {
	source Repository
	ttl    time.Duration
	ready  chan struct{}
	once   sync.Once

	mu    sync.RWMutex
	cache map[string]cachedRuntime
}

func NewCachedRuntimeResolver(source Repository, ttl time.Duration) *CachedRuntimeResolver {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &CachedRuntimeResolver{
		source: source, ttl: ttl, ready: make(chan struct{}), cache: make(map[string]cachedRuntime),
	}
}

func (r *CachedRuntimeResolver) Resolve(ctx context.Context, tenantID string) (tenant.RuntimeProfile, error) {
	now := time.Now()
	r.mu.RLock()
	cached, ok := r.cache[tenantID]
	r.mu.RUnlock()
	if ok && now.Before(cached.expiresAt) {
		return cached.profile, nil
	}
	profile, err := r.source.ResolveRuntimeProfile(ctx, tenantID)
	if err != nil {
		return tenant.RuntimeProfile{}, err
	}
	r.mu.Lock()
	r.cache[tenantID] = cachedRuntime{profile: profile, expiresAt: now.Add(r.ttl)}
	r.mu.Unlock()
	return profile, nil
}

func (r *CachedRuntimeResolver) Invalidate(tenantID string) {
	r.mu.Lock()
	delete(r.cache, tenantID)
	r.mu.Unlock()
}

func (r *CachedRuntimeResolver) Run(ctx context.Context) error {
	changes, err := r.source.WatchRuntimeChanges(ctx)
	r.once.Do(func() { close(r.ready) })
	if err != nil {
		changes = nil
	}
	ticker := time.NewTicker(r.ttl)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case change, ok := <-changes:
			if !ok {
				changes = nil
				continue
			}
			r.Invalidate(change.TenantID)
		case <-ticker.C:
			r.dropExpired(time.Now())
		}
	}
}

func (r *CachedRuntimeResolver) waitReady(ctx context.Context) error {
	select {
	case <-r.ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *CachedRuntimeResolver) dropExpired(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for tenantID, item := range r.cache {
		if !now.Before(item.expiresAt) {
			delete(r.cache, tenantID)
		}
	}
}
