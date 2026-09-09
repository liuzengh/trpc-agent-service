package tenant_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
)

// fakeStore implements tenant.Store with a load counter for cache assertions.
type fakeStore struct {
	mu    sync.Mutex
	data  tenant.Data
	err   error
	loads int
}

func (f *fakeStore) LoadAll(context.Context) (tenant.Data, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads++
	return f.data, f.err
}

func (f *fakeStore) loadCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loads
}

func testData() tenant.Data {
	return tenant.Data{
		Tenants: []tenant.Tenant{
			{ID: "t1", Name: "demo", Status: tenant.StatusActive},
			{ID: "t2", Name: "off", Status: tenant.StatusDisabled},
		},
		Apps: []tenant.AgentApp{
			{ID: "a1", TenantID: "t1", Name: "assistant", Version: 1, Status: "published"},
			{ID: "a2", TenantID: "t2", Name: "assistant", Version: 1, Status: "published"},
			{ID: "a3", TenantID: "t1", Name: "staging", Version: 2, Status: "draft"},
		},
		Bindings: []tenant.ChannelBinding{
			{ID: "b1", TenantID: "t1", Channel: "mock", AppID: "a1",
				WebhookPath: "/mock/callback", Status: tenant.StatusActive},
			{ID: "b2", TenantID: "t2", Channel: "mock", AppID: "a2",
				WebhookPath: "/off/callback", Status: tenant.StatusActive},
			// wecomws bindings: b3 servable; b4 disabled tenant; b5 draft app;
			// b6 disabled binding.
			{ID: "b3", TenantID: "t1", Channel: "wecomws", AppID: "a1",
				WebhookPath: "/wecomws/bot1", Status: tenant.StatusActive},
			{ID: "b4", TenantID: "t2", Channel: "wecomws", AppID: "a2",
				WebhookPath: "/wecomws/bot2", Status: tenant.StatusActive},
			{ID: "b5", TenantID: "t1", Channel: "wecomws", AppID: "a3",
				WebhookPath: "/wecomws/bot3", Status: tenant.StatusActive},
			{ID: "b6", TenantID: "t1", Channel: "wecomws", AppID: "a1",
				WebhookPath: "/wecomws/bot4", Status: tenant.StatusDisabled},
		},
	}
}

func TestResolveKnownPath(t *testing.T) {
	r := tenant.NewResolver(&fakeStore{data: testData()})
	route, err := r.Resolve(context.Background(), "/mock/callback")
	if err != nil {
		t.Fatal(err)
	}
	if route.Tenant.ID != "t1" || route.App.ID != "a1" || route.Binding.ID != "b1" {
		t.Fatalf("unexpected route: %+v", route)
	}
}

func TestResolveUnknownPath(t *testing.T) {
	r := tenant.NewResolver(&fakeStore{data: testData()})
	_, err := r.Resolve(context.Background(), "/nope")
	if !errors.Is(err, tenant.ErrUnknownBinding) {
		t.Fatalf("want ErrUnknownBinding, got %v", err)
	}
}

func TestResolveDisabledTenant(t *testing.T) {
	r := tenant.NewResolver(&fakeStore{data: testData()})
	_, err := r.Resolve(context.Background(), "/off/callback")
	if !errors.Is(err, tenant.ErrInactive) {
		t.Fatalf("want ErrInactive, got %v", err)
	}
}

func TestAppByIDKnown(t *testing.T) {
	r := tenant.NewResolver(&fakeStore{data: testData()})
	app, tn, err := r.AppByID(context.Background(), "a1")
	if err != nil {
		t.Fatal(err)
	}
	if app.ID != "a1" || tn.ID != "t1" {
		t.Fatalf("unexpected app/tenant: %+v / %+v", app, tn)
	}
}

func TestAppByIDUnknown(t *testing.T) {
	r := tenant.NewResolver(&fakeStore{data: testData()})
	_, _, err := r.AppByID(context.Background(), "nope")
	if !errors.Is(err, tenant.ErrUnknownApp) {
		t.Fatalf("want ErrUnknownApp, got %v", err)
	}
}

func TestAppByIDDisabledTenant(t *testing.T) {
	r := tenant.NewResolver(&fakeStore{data: testData()})
	_, _, err := r.AppByID(context.Background(), "a2")
	if !errors.Is(err, tenant.ErrInactive) {
		t.Fatalf("want ErrInactive for a disabled tenant's app, got %v", err)
	}
}

// RoutesByChannel enumerates the servable bindings of one channel: only
// binding-active + tenant-active + app-published rows come back, broken rows
// are skipped (not fatal), and the snapshot load is shared with Resolve.
func TestRoutesByChannel(t *testing.T) {
	store := &fakeStore{data: testData()}
	r := tenant.NewResolver(store)
	ctx := context.Background()

	routes, err := r.RoutesByChannel(ctx, "wecomws")
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 1 {
		t.Fatalf("want 1 servable wecomws route (disabled tenant/draft app/disabled binding skipped), got %d: %+v",
			len(routes), routes)
	}
	if routes[0].Binding.ID != "b3" || routes[0].Tenant.ID != "t1" || routes[0].App.ID != "a1" {
		t.Fatalf("unexpected wecomws route: %+v", routes[0])
	}

	// The other channels still enumerate with the same snapshot semantics.
	mock, err := r.RoutesByChannel(ctx, "mock")
	if err != nil {
		t.Fatal(err)
	}
	if len(mock) != 1 || mock[0].Binding.ID != "b1" {
		t.Fatalf("want only the active-tenant mock binding, got %+v", mock)
	}
	none, err := r.RoutesByChannel(ctx, "wxkf")
	if err != nil || len(none) != 0 {
		t.Fatalf("channel without bindings: empty slice, nil error, got %+v / %v", none, err)
	}

	// One snapshot serves Resolve and every channel enumeration.
	if n := store.loadCount(); n != 1 {
		t.Fatalf("want 1 load across the enumerations, got %d", n)
	}

	// A failing first load is the only error: a broken store must not be
	// silently reported as "no routes".
	_, err = tenant.NewResolver(&fakeStore{err: errors.New("db down")}).RoutesByChannel(ctx, "wecomws")
	if err == nil {
		t.Fatal("failed first load must error, not return an empty route set")
	}
}

func TestCacheTTL(t *testing.T) {
	store := &fakeStore{data: testData()}
	r := tenant.NewResolverWithTTL(store, 50*time.Millisecond)
	ctx := context.Background()

	if _, err := r.Resolve(ctx, "/mock/callback"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Resolve(ctx, "/mock/callback"); err != nil {
		t.Fatal(err)
	}
	if n := store.loadCount(); n != 1 {
		t.Fatalf("want 1 load within TTL, got %d", n)
	}

	time.Sleep(60 * time.Millisecond)
	if _, err := r.Resolve(ctx, "/mock/callback"); err != nil {
		t.Fatal(err)
	}
	if n := store.loadCount(); n != 2 {
		t.Fatalf("want reload after TTL, got %d loads", n)
	}
}

func TestReloadFailureServesStale(t *testing.T) {
	store := &fakeStore{data: testData()}
	r := tenant.NewResolverWithTTL(store, 50*time.Millisecond)
	ctx := context.Background()

	if _, err := r.Resolve(ctx, "/mock/callback"); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.err = errors.New("pg down")
	store.mu.Unlock()

	time.Sleep(60 * time.Millisecond)
	route, err := r.Resolve(ctx, "/mock/callback")
	if err != nil {
		t.Fatalf("stale snapshot should keep serving, got %v", err)
	}
	if route.Tenant.ID != "t1" {
		t.Fatalf("unexpected route: %+v", route)
	}
}

// A failed reload must not be retried by every request that follows. The
// snapshot is already past its TTL when LoadAll fails, so without a backoff
// each callback re-runs the four-table load under the write lock — a PG
// outage then throttles the whole request path to one failing query at a
// time, and the log line repeats per request.
func TestReloadFailureBacksOff(t *testing.T) {
	store := &fakeStore{data: testData()}
	r := tenant.NewResolverWithTTL(store, 20*time.Millisecond)
	r.ReloadBackoff = 150 * time.Millisecond
	ctx := context.Background()

	if _, err := r.Resolve(ctx, "/mock/callback"); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.err = errors.New("pg down")
	store.mu.Unlock()

	// Past the TTL: one attempt, which fails and is absorbed into the stale
	// snapshot. The four requests behind it must not retry.
	time.Sleep(25 * time.Millisecond)
	for i := 0; i < 5; i++ {
		route, err := r.Resolve(ctx, "/mock/callback")
		if err != nil {
			t.Fatalf("stale snapshot should keep serving, got %v", err)
		}
		if route.Tenant.ID != "t1" {
			t.Fatalf("unexpected route: %+v", route)
		}
	}
	if n := store.loadCount(); n != 2 {
		t.Fatalf("want exactly one reload attempt during the backoff, got %d loads", n)
	}

	// The backoff expires, so the resolver tries again — still failing, still
	// serving the snapshot.
	time.Sleep(160 * time.Millisecond)
	if _, err := r.Resolve(ctx, "/mock/callback"); err != nil {
		t.Fatal(err)
	}
	if n := store.loadCount(); n != 3 {
		t.Fatalf("want a retry once the backoff expires, got %d loads", n)
	}

	// Recovery clears the backoff and the reload resumes.
	store.mu.Lock()
	store.err = nil
	store.mu.Unlock()
	time.Sleep(160 * time.Millisecond)
	if _, err := r.Resolve(ctx, "/mock/callback"); err != nil {
		t.Fatal(err)
	}
	if n := store.loadCount(); n != 4 {
		t.Fatalf("want the reload to resume after recovery, got %d loads", n)
	}
}

// The backoff is defaulted by the constructors, not left at zero (a zero
// backoff is the retry storm the field exists to prevent).
func TestReloadBackoffDefault(t *testing.T) {
	r := tenant.NewResolver(&fakeStore{data: testData()})
	if r.ReloadBackoff != tenant.DefaultReloadBackoff {
		t.Fatalf("want the default reload backoff %s, got %s",
			tenant.DefaultReloadBackoff, r.ReloadBackoff)
	}
}

func TestFirstLoadFailure(t *testing.T) {
	store := &fakeStore{err: errors.New("pg down")}
	r := tenant.NewResolver(store)
	if _, err := r.Resolve(context.Background(), "/mock/callback"); err == nil {
		t.Fatal("want error when the first load fails")
	}
}

// A pub/sub invalidation drops the cache before TTL expiry; skips when Redis
// is unreachable.
func TestWatchInvalidations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb := testenv.Redis(t)
	defer func() { _ = rdb.Close() }()

	store := &fakeStore{data: testData()}
	r := tenant.NewResolverWithTTL(store, time.Hour) // TTL far away: only invalidation can refresh
	r.WatchInvalidations(ctx, rdb)

	if _, err := r.Resolve(ctx, "/mock/callback"); err != nil {
		t.Fatal(err)
	}
	if n := store.loadCount(); n != 1 {
		t.Fatalf("want 1 load, got %d", n)
	}

	// Pub/sub is fire-and-forget: the watcher may attach a beat after the
	// first publish, and a receiver count ≥1 does not mean OUR subscriber got
	// the message — the dev Redis is shared with whatever else subscribes to
	// this channel (e.g. a running service). Keep publishing while polling
	// for the reload; the TTL above makes invalidation the only way the
	// reload can happen, so extra publishes are harmless no-ops.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := rdb.Publish(ctx, tenant.InvalidationChannel, "1").Result(); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Resolve(ctx, "/mock/callback"); err != nil {
			t.Fatal(err)
		}
		if store.loadCount() >= 2 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("invalidation did not trigger a reload within 3s")
}

// TenantByID serves policy lookups off the routing path.
func TestTenantByID(t *testing.T) {
	r := tenant.NewResolver(&fakeStore{data: testData()})
	ctx := context.Background()

	tn, err := r.TenantByID(ctx, "t1")
	if err != nil {
		t.Fatal(err)
	}
	if tn.ID != "t1" || tn.Name != "demo" {
		t.Fatalf("unexpected tenant: %+v", tn)
	}
	// A tenant missing from the snapshot is reported as inactive.
	if _, err := r.TenantByID(ctx, "nope"); !errors.Is(err, tenant.ErrInactive) {
		t.Fatalf("want ErrInactive for an unknown tenant, got %v", err)
	}
}

// ActiveMigration returns the in-flight migration for (tenant, resource) as a
// copy — mutating it must not corrupt the cached snapshot.
func TestActiveMigration(t *testing.T) {
	data := testData()
	data.Migrations = []tenant.Migration{
		{ID: "m1", TenantID: "t1", Resource: "session",
			FromBackend: "redis", ToBackend: "postgres", Phase: tenant.PhaseDualWrite},
	}
	r := tenant.NewResolver(&fakeStore{data: data})
	ctx := context.Background()

	if _, err := r.Resolve(ctx, "/mock/callback"); err != nil {
		t.Fatal(err) // force the snapshot load
	}
	m := r.ActiveMigration("t1", "session")
	if m == nil || m.ID != "m1" || m.Phase != tenant.PhaseDualWrite {
		t.Fatalf("unexpected migration: %+v", m)
	}
	m.Phase = "corrupted"
	if again := r.ActiveMigration("t1", "session"); again == nil || again.Phase != tenant.PhaseDualWrite {
		t.Fatalf("the cache must hand out copies, got %+v", again)
	}
	// Other tenants and other resources have no in-flight migration.
	if r.ActiveMigration("t2", "session") != nil || r.ActiveMigration("t1", "knowledge") != nil {
		t.Fatal("unexpected migration for another tenant/resource")
	}
}

// BindingByID resolves a binding row by ID for the callback dispatcher.
func TestBindingByID(t *testing.T) {
	r := tenant.NewResolver(&fakeStore{data: testData()})
	ctx := context.Background()

	b, err := r.BindingByID(ctx, "b1")
	if err != nil {
		t.Fatal(err)
	}
	if b.ID != "b1" || b.TenantID != "t1" || b.AppID != "a1" {
		t.Fatalf("unexpected binding: %+v", b)
	}
	if _, err := r.BindingByID(ctx, "nope"); !errors.Is(err, tenant.ErrUnknownBinding) {
		t.Fatalf("want ErrUnknownBinding, got %v", err)
	}
}

// A binding referencing a tenant or app missing from the snapshot is rejected.
func TestResolveBindingDanglingReferences(t *testing.T) {
	data := tenant.Data{
		Tenants: []tenant.Tenant{{ID: "t1", Status: tenant.StatusActive}},
		Apps:    []tenant.AgentApp{{ID: "a1", TenantID: "t1", Status: "published"}},
		Bindings: []tenant.ChannelBinding{
			{ID: "b-mt", TenantID: "t-missing", Channel: "mock", AppID: "a1",
				WebhookPath: "/mt", Status: tenant.StatusActive},
			{ID: "b-ma", TenantID: "t1", Channel: "mock", AppID: "a-missing",
				WebhookPath: "/ma", Status: tenant.StatusActive},
		},
	}
	r := tenant.NewResolver(&fakeStore{data: data})
	ctx := context.Background()

	_, err := r.Resolve(ctx, "/mt")
	if !errors.Is(err, tenant.ErrUnknownBinding) {
		t.Fatalf("missing tenant reference: want ErrUnknownBinding, got %v", err)
	}
	_, err = r.Resolve(ctx, "/ma")
	if !errors.Is(err, tenant.ErrUnknownBinding) {
		t.Fatalf("missing app reference: want ErrUnknownBinding, got %v", err)
	}
}

// AppByID rejects an app whose owning tenant is missing from the snapshot.
func TestAppByIDDanglingTenant(t *testing.T) {
	data := tenant.Data{
		Tenants: []tenant.Tenant{{ID: "t1", Status: tenant.StatusActive}},
		Apps: []tenant.AgentApp{
			{ID: "a-orphan", TenantID: "t-missing", Status: "published"},
			// A disabled app is inactive even though the tenant exists.
			{ID: "a-off", TenantID: "t1", Status: tenant.StatusDisabled},
		},
	}
	r := tenant.NewResolver(&fakeStore{data: data})
	ctx := context.Background()

	if _, _, err := r.AppByID(ctx, "a-orphan"); !errors.Is(err, tenant.ErrUnknownApp) {
		t.Fatalf("missing tenant reference: want ErrUnknownApp, got %v", err)
	}
	if _, _, err := r.AppByID(ctx, "a-off"); !errors.Is(err, tenant.ErrInactive) {
		t.Fatalf("disabled app: want ErrInactive, got %v", err)
	}
}

// PublishInvalidation notifies subscribers; an unreachable Redis surfaces the
// failure instead of silently skipping the notification.
func TestPublishInvalidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	rdb := testenv.Redis(t)
	if err := tenant.PublishInvalidation(ctx, rdb); err != nil {
		t.Fatal(err)
	}

	bad := redis.NewClient(&redis.Options{Addr: "localhost:1"})
	defer func() { _ = bad.Close() }()
	if err := tenant.PublishInvalidation(ctx, bad); err == nil {
		t.Fatal("publish against an unreachable Redis must fail")
	}
}

// A failed first load is an error on every lookup entry point, not just
// Resolve.
func TestFirstLoadFailureAllEntryPoints(t *testing.T) {
	r := tenant.NewResolver(&fakeStore{err: errors.New("pg down")})
	ctx := context.Background()

	if _, _, err := r.AppByID(ctx, "a1"); err == nil {
		t.Fatal("AppByID must fail when the first load fails")
	}
	if _, err := r.TenantByID(ctx, "t1"); err == nil {
		t.Fatal("TenantByID must fail when the first load fails")
	}
	if _, err := r.BindingByID(ctx, "b1"); err == nil {
		t.Fatal("BindingByID must fail when the first load fails")
	}
}

// A burst of concurrent first refreshes loads the snapshot exactly once: the
// write-lock double-check keeps the losers from reloading.
func TestRefreshConcurrentSingleLoad(t *testing.T) {
	gate := make(chan struct{})
	store := &gatedStore{data: testData(), gate: gate}
	r := tenant.NewResolver(store)
	ctx := context.Background()

	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			route, err := r.Resolve(ctx, "/mock/callback")
			if err == nil && route.Binding.ID != "b1" {
				err = errors.New("unexpected route")
			}
			errs <- err
		}()
	}
	// Both goroutines are past the read-lock freshness check and parked on
	// the write lock by the time the store is allowed to answer.
	time.Sleep(100 * time.Millisecond)
	close(gate)

	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if n := store.loadCount(); n != 1 {
		t.Fatalf("a concurrent burst must load once, got %d loads", n)
	}
}

// gatedStore is a fakeStore whose LoadAll blocks until gate is closed.
type gatedStore struct {
	fakeStore
	gate chan struct{}
}

func (f *gatedStore) LoadAll(ctx context.Context) (tenant.Data, error) {
	<-f.gate
	return f.fakeStore.LoadAll(ctx)
}
