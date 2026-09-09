package tenant

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// InvalidationChannel is the Redis pub/sub channel carrying config
// invalidation notifications: the Admin API publishes after tenant/app/binding
// writes and publish/rollback; Resolvers drop their cache on receipt so
// changes take effect in seconds. The cache TTL remains the fallback when a
// notification is lost.
const InvalidationChannel = "tenant:invalidate"

// PublishInvalidation notifies all Resolver instances to drop their cache.
func PublishInvalidation(ctx context.Context, rdb *redis.Client) error {
	if err := rdb.Publish(ctx, InvalidationChannel, "1").Err(); err != nil {
		return fmt.Errorf("publish invalidation: %w", err)
	}
	return nil
}

// DefaultCacheTTL bounds how long a snapshot is served before reloading.
// Pub/sub invalidation (PublishInvalidation) drops the cache on Admin API
// writes; this TTL is the fallback refresh when a notification is lost.
const DefaultCacheTTL = 30 * time.Second

// DefaultReloadBackoff is how long a failed reload postpones the next attempt.
// A failed reload leaves the snapshot stale by definition, so without a
// backoff every single request retries it — under r.mu's write lock, which
// serializes the whole request path behind one failing four-table load.
const DefaultReloadBackoff = 5 * time.Second

var (
	// ErrUnknownBinding means no channel_binding row serves the webhook path.
	ErrUnknownBinding = errors.New("unknown channel binding")
	// ErrUnknownApp means no agent_app row matches the requested app ID.
	ErrUnknownApp = errors.New("unknown agent app")
	// ErrInactive means the binding, its tenant or its app is disabled.
	ErrInactive = errors.New("tenant route inactive")
)

// Route is the resolution of one webhook path: which tenant and agent app a
// callback belongs to.
type Route struct {
	Tenant  Tenant
	App     AgentApp
	Binding ChannelBinding
}

// Resolver answers webhook_path → Route lookups from a cached snapshot of
// the tenant tables, refreshed every TTL. A failed reload keeps serving the
// previous snapshot (stale beats down) and waits ReloadBackoff before trying
// again; only a failed first load errors.
type Resolver struct {
	store Store
	ttl   time.Duration

	// ReloadBackoff is how long a failed reload postpones the next attempt
	// (DefaultReloadBackoff unless set). It is a field so an operator who
	// tolerates more staleness during an outage can widen it.
	ReloadBackoff time.Duration

	mu           sync.RWMutex
	tenants      map[string]Tenant
	apps         map[string]AgentApp
	bindings     map[string]ChannelBinding // by webhook_path (gateway routing)
	bindingsByID map[string]ChannelBinding // by binding id (callback dispatch)
	migs         map[string]Migration      // by tenant_id + ":" + resource
	loadedAt     time.Time
	retryAt      time.Time // zero unless the last reload failed
	loaded       bool
}

// NewResolver creates a Resolver with the default cache TTL.
func NewResolver(store Store) *Resolver {
	return NewResolverWithTTL(store, DefaultCacheTTL)
}

// NewResolverWithTTL creates a Resolver with an explicit cache TTL.
func NewResolverWithTTL(store Store, ttl time.Duration) *Resolver {
	return &Resolver{store: store, ttl: ttl, ReloadBackoff: DefaultReloadBackoff}
}

// Resolve maps a webhook path to its tenant route, refreshing the cache when
// stale. Unknown or inactive routes are rejected: the callback gets an error
// reply and the IM may redeliver, but the message never consumes queue space.
func (r *Resolver) Resolve(ctx context.Context, webhookPath string) (Route, error) {
	if err := r.refresh(ctx); err != nil {
		return Route{}, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.bindings[webhookPath]
	if !ok {
		return Route{}, fmt.Errorf("%w: %s", ErrUnknownBinding, webhookPath)
	}
	return r.resolveBinding(b)
}

// resolveBinding applies the serving checks shared by Resolve and
// RoutesByChannel to one snapshot binding: binding active + tenant exists and
// active + app exists and published. Callers must hold r.mu.
func (r *Resolver) resolveBinding(b ChannelBinding) (Route, error) {
	if b.Status != StatusActive {
		return Route{}, fmt.Errorf("%w: binding %s status %q", ErrInactive, b.ID, b.Status)
	}
	t, ok := r.tenants[b.TenantID]
	if !ok {
		return Route{}, fmt.Errorf("%w: binding %s references missing tenant %s",
			ErrUnknownBinding, b.ID, b.TenantID)
	}
	if t.Status != StatusActive {
		return Route{}, fmt.Errorf("%w: tenant %s status %q", ErrInactive, t.ID, t.Status)
	}
	app, ok := r.apps[b.AppID]
	if !ok {
		return Route{}, fmt.Errorf("%w: binding %s references missing app %s",
			ErrUnknownBinding, b.ID, b.AppID)
	}
	// Only the published version serves traffic; drafts exist for gray
	// release staging and must not receive callbacks.
	if app.Status != "published" {
		return Route{}, fmt.Errorf("%w: app %s status %q", ErrInactive, app.ID, app.Status)
	}
	return Route{Tenant: t, App: app, Binding: b}, nil
}

// RoutesByChannel returns the currently servable routes for one channel type
// (binding active + tenant active + app published), with the same TTL and
// pub-sub invalidation semantics as Resolve. A binding whose checks fail is
// logged at warn and skipped — one broken binding must not drag down the
// other connections of the same channel. The only error is a failed first
// snapshot load.
func (r *Resolver) RoutesByChannel(ctx context.Context, channel string) ([]Route, error) {
	if err := r.refresh(ctx); err != nil {
		return nil, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	var routes []Route
	for _, b := range r.bindingsByID {
		if b.Channel != channel {
			continue
		}
		route, err := r.resolveBinding(b)
		if err != nil {
			plog.Warnf("resolver: skipping %s binding %s (%s): %v", channel, b.ID, b.WebhookPath, err)
			continue
		}
		routes = append(routes, route)
	}
	return routes, nil
}

// Invalidate drops the cached snapshot; the next Resolve reloads.
func (r *Resolver) Invalidate() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.loaded = false
}

// AppByID resolves an app and its owning tenant from the cached snapshot.
// The Worker's per-app assembly uses it to load the app config and tenant
// policies for the app stamped on the message; the same staleness and
// invalidation semantics as Resolve apply.
func (r *Resolver) AppByID(ctx context.Context, appID string) (AgentApp, Tenant, error) {
	if err := r.refresh(ctx); err != nil {
		return AgentApp{}, Tenant{}, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	app, ok := r.apps[appID]
	if !ok {
		return AgentApp{}, Tenant{}, fmt.Errorf("%w: %s", ErrUnknownApp, appID)
	}
	if app.Status == StatusDisabled {
		return AgentApp{}, Tenant{}, fmt.Errorf("%w: app %s disabled", ErrInactive, app.ID)
	}
	t, ok := r.tenants[app.TenantID]
	if !ok {
		return AgentApp{}, Tenant{}, fmt.Errorf("%w: app %s references missing tenant %s",
			ErrUnknownApp, app.ID, app.TenantID)
	}
	if t.Status != StatusActive {
		return AgentApp{}, Tenant{}, fmt.Errorf("%w: tenant %s status %q", ErrInactive, t.ID, t.Status)
	}
	return app, t, nil
}

// TenantByID looks up one tenant from the cached snapshot, for policy
// lookups off the routing path (guardrail policies, send pacing overrides).
func (r *Resolver) TenantByID(ctx context.Context, tenantID string) (Tenant, error) {
	if err := r.refresh(ctx); err != nil {
		return Tenant{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tenants[tenantID]
	if !ok {
		return Tenant{}, fmt.Errorf("%w: tenant %s", ErrInactive, tenantID)
	}
	return t, nil
}

// WatchInvalidations subscribes to the invalidation channel until ctx is
// canceled; each notification drops the cache so a publish/rollback takes
// effect within seconds instead of at TTL expiry.
func (r *Resolver) WatchInvalidations(ctx context.Context, rdb *redis.Client) {
	go func() {
		sub := rdb.Subscribe(ctx, InvalidationChannel)
		defer func() { _ = sub.Close() }()
		ch := sub.Channel()
		for {
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					return
				}
				r.Invalidate()
			}
		}
	}()
}

// stale reports whether the snapshot has to be reloaded. A failed reload
// postpones the next attempt until retryAt. Callers must hold r.mu.
func (r *Resolver) stale(now time.Time) bool {
	if !r.loaded {
		return true
	}
	if now.Before(r.retryAt) {
		return false
	}
	return now.Sub(r.loadedAt) >= r.ttl
}

// refresh reloads the snapshot when the cache is stale. Concurrent refreshes
// are serialized by the write lock; the double-check after acquiring it
// keeps a burst of callbacks from reloading more than once.
func (r *Resolver) refresh(ctx context.Context) error {
	r.mu.RLock()
	stale := r.stale(time.Now())
	r.mu.RUnlock()
	if !stale {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.stale(time.Now()) {
		return nil
	}
	d, err := r.store.LoadAll(ctx)
	if err != nil {
		if r.loaded {
			r.retryAt = time.Now().Add(r.ReloadBackoff)
			plog.Warnf("tenant reload failed, serving snapshot from %s (next attempt in %s): %v",
				r.loadedAt.Format(time.RFC3339), r.ReloadBackoff, err)
			return nil
		}
		return fmt.Errorf("load tenants: %w", err)
	}

	tenants := make(map[string]Tenant, len(d.Tenants))
	for _, t := range d.Tenants {
		tenants[t.ID] = t
	}
	apps := make(map[string]AgentApp, len(d.Apps))
	for _, a := range d.Apps {
		apps[a.ID] = a
	}
	bindings := make(map[string]ChannelBinding, len(d.Bindings))
	bindingsByID := make(map[string]ChannelBinding, len(d.Bindings))
	for _, b := range d.Bindings {
		bindings[b.WebhookPath] = b
		bindingsByID[b.ID] = b
	}
	migs := make(map[string]Migration, len(d.Migrations))
	for _, m := range d.Migrations {
		migs[m.TenantID+":"+m.Resource] = m
	}
	r.tenants, r.apps, r.bindings, r.bindingsByID, r.migs = tenants, apps, bindings, bindingsByID, migs
	r.loadedAt = time.Now()
	r.retryAt = time.Time{}
	r.loaded = true
	return nil
}

// ActiveMigration returns the in-flight migration for (tenant, resource), or
// nil. The worker's session-service routing fans writes out to both backends
// while one is active.
func (r *Resolver) ActiveMigration(tenantID, resource string) *Migration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m, ok := r.migs[tenantID+":"+resource]
	if !ok {
		return nil
	}
	cp := m
	return &cp
}

// BindingByID looks up one channel binding by its ID from the cached
// snapshot.
func (r *Resolver) BindingByID(ctx context.Context, id string) (ChannelBinding, error) {
	if err := r.refresh(ctx); err != nil {
		return ChannelBinding{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.bindingsByID[id]
	if !ok {
		return ChannelBinding{}, fmt.Errorf("%w: binding %s", ErrUnknownBinding, id)
	}
	return b, nil
}
