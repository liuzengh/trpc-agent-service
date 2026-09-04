package storage

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tenant"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func newRouterFixture(t *testing.T) (*Router, *tenant.Manager) {
	t.Helper()
	tenants := tenant.NewManager()
	r := NewRouter(tenants,
		SessionConfig{Backend: BackendInMemory},
		MemoryConfig{Backend: BackendInMemory},
	)
	return r, tenants
}

func TestRouterDefaultsWhenNoSelection(t *testing.T) {
	ctx := context.Background()
	r, _ := newRouterFixture(t)

	if _, err := r.Sessions(ctx, "t-no-select"); err != nil {
		t.Fatalf("Sessions on unconfigured tenant: %v", err)
	}
	if _, err := r.Memories(ctx, "t-no-select"); err != nil {
		t.Fatalf("Memories on unconfigured tenant: %v", err)
	}
}

func TestRouterHonorsTenantSelection(t *testing.T) {
	ctx := context.Background()
	r, tenants := newRouterFixture(t)

	// t1 selects inmemory, the default is inmemory too but the selection is
	// explicit; t2 selects nothing. Both resolve to usable stores.
	if err := tenants.Create(ctx, &tenant.Tenant{
		ID:          "t1",
		Name:        "t1",
		Status:      tenant.StatusActive,
		DataBackend: map[string]string{tenant.DomainSession: string(BackendInMemory)},
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := r.Sessions(ctx, "t1"); err != nil {
		t.Fatalf("Sessions(t1): %v", err)
	}
	if _, err := r.Sessions(ctx, "t2"); err != nil {
		t.Fatalf("Sessions(t2): %v", err)
	}
}

func TestRouterCachesPerBackend(t *testing.T) {
	ctx := context.Background()
	r, tenants := newRouterFixture(t)

	if err := tenants.Create(ctx, &tenant.Tenant{
		ID: "t1", Name: "t1", Status: tenant.StatusActive,
		DataBackend: map[string]string{tenant.DomainSession: string(BackendInMemory)},
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	_ = tenants.Create(ctx, &tenant.Tenant{ID: "t2", Name: "t2", Status: tenant.StatusActive})

	s1, err := r.Sessions(ctx, "t1")
	if err != nil {
		t.Fatalf("Sessions(t1): %v", err)
	}
	s2, err := r.Sessions(ctx, "t2") // same default backend
	if err != nil {
		t.Fatalf("Sessions(t2): %v", err)
	}
	if s1 != s2 {
		t.Error("tenants on the same backend should share one store instance")
	}
}

func TestRouterUnknownBackendFails(t *testing.T) {
	ctx := context.Background()
	r, tenants := newRouterFixture(t)

	if err := tenants.Create(ctx, &tenant.Tenant{
		ID: "t1", Name: "t1", Status: tenant.StatusActive,
		DataBackend: map[string]string{tenant.DomainSession: "bogus"},
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if _, err := r.Sessions(ctx, "t1"); err == nil {
		t.Error("unknown backend should fail")
	}
}

func TestRouterSessionIsolation(t *testing.T) {
	ctx := context.Background()
	r, _ := newRouterFixture(t)

	sa, err := r.Sessions(ctx, "tenant-a")
	if err != nil {
		t.Fatalf("Sessions(a): %v", err)
	}
	sb, err := r.Sessions(ctx, "tenant-b")
	if err != nil {
		t.Fatalf("Sessions(b): %v", err)
	}

	if _, err := sa.Create(ctx, "tenant-a", "u1", "s1", session.StateMap{"k": []byte("a")}); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := sb.Get(ctx, "tenant-b", "u1", "s1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got != nil {
		t.Error("tenant-b must not resolve tenant-a's session")
	}
}
