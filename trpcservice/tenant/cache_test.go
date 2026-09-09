package tenant

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSnapshotCacheCachesAndDefensivelyCopies(t *testing.T) {
	store := newFakeConfigStore()
	cache, err := NewConfigCache(store, 30*time.Second)
	if err != nil {
		t.Fatalf("NewConfigCache() error = %v", err)
	}
	now := time.Unix(1_000, 0)
	cache.now = func() time.Time { return now }

	first, err := cache.ResolveBinding(context.Background(), "feishu", "cli-test-app")
	if err != nil {
		t.Fatalf("ResolveBinding() error = %v", err)
	}
	first.Binding.Config["app_secret"] = "mutated"
	first.App.Tools[0] = "mutated"

	second, err := cache.ResolveBinding(context.Background(), "feishu", "cli-test-app")
	if err != nil {
		t.Fatalf("ResolveBinding() cached error = %v", err)
	}
	if got := second.Binding.Config["app_secret"]; got != "env:FEISHU_APP_SECRET_TENANT_A" {
		t.Fatalf("cached binding config was mutated: %q", got)
	}
	if got := second.App.Tools[0]; got != "search" {
		t.Fatalf("cached app tools were mutated: %q", got)
	}
	if calls := store.totalCalls(); calls != 3 {
		t.Fatalf("store calls before expiry = %d, want 3", calls)
	}

	now = now.Add(31 * time.Second)
	if _, err := cache.ResolveBinding(context.Background(), "feishu", "cli-test-app"); err != nil {
		t.Fatalf("ResolveBinding() after expiry error = %v", err)
	}
	if calls := store.totalCalls(); calls != 6 {
		t.Fatalf("store calls after expiry = %d, want 6", calls)
	}
}

func TestSnapshotCacheRejectsInactiveBinding(t *testing.T) {
	store := newFakeConfigStore()
	store.binding.IsActive = false
	cache, err := NewConfigCache(store, time.Minute)
	if err != nil {
		t.Fatalf("NewConfigCache() error = %v", err)
	}
	_, err = cache.ResolveBinding(context.Background(), "feishu", "cli-test-app")
	if !errors.Is(err, ErrInactive) {
		t.Fatalf("ResolveBinding() error = %v, want ErrInactive", err)
	}
}

type fakeConfigStore struct {
	mu      sync.Mutex
	calls   int
	tenant  Tenant
	app     AgentApp
	binding ChannelBinding
}

func newFakeConfigStore() *fakeConfigStore {
	app := validApp()
	app.Version = 1
	app.IsCurrent = true
	return &fakeConfigStore{
		tenant:  validTenant(),
		app:     app,
		binding: validBinding(),
	}
}

func (f *fakeConfigStore) count() {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
}

func (f *fakeConfigStore) totalCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeConfigStore) UpsertTenant(context.Context, Tenant) error { return nil }
func (f *fakeConfigStore) ListTenants(context.Context) ([]Tenant, error) {
	return []Tenant{f.tenant}, nil
}
func (f *fakeConfigStore) DeactivateTenant(context.Context, string) error { return nil }
func (f *fakeConfigStore) UpsertApp(context.Context, AgentApp) (int, error) {
	return 1, nil
}
func (f *fakeConfigStore) ListApps(context.Context, string) ([]AgentApp, error) {
	return []AgentApp{f.app}, nil
}
func (f *fakeConfigStore) BindAppVersion(context.Context, string, int) error { return nil }
func (f *fakeConfigStore) UpsertBinding(context.Context, ChannelBinding) error {
	return nil
}
func (f *fakeConfigStore) ListBindings(context.Context, string) ([]ChannelBinding, error) {
	return []ChannelBinding{f.binding}, nil
}

func (f *fakeConfigStore) GetTenant(context.Context, string) (Tenant, error) {
	f.count()
	return f.tenant, nil
}

func (f *fakeConfigStore) GetCurrentApp(context.Context, string) (AgentApp, error) {
	f.count()
	return f.app, nil
}

func (f *fakeConfigStore) GetBindingByRoute(context.Context, string, string) (ChannelBinding, error) {
	f.count()
	return f.binding, nil
}
