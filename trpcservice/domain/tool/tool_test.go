package tool

import (
	"context"
	"errors"
	"testing"
)

func TestRegistryGrantRevoke(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	mustRegister(t, r, Definition{ID: "t1", Name: "echo", RiskLevel: RiskLow})
	mustRegister(t, r, Definition{ID: "t2", Name: "danger", RiskLevel: RiskHigh})

	mustGrant(t, r, "agent-1", "t1")
	mustGrant(t, r, "agent-1", "t2")

	if !mustIsAllowed(t, r, "agent-1", "t1") {
		t.Error("agent-1 should be allowed t1")
	}
	if mustIsAllowed(t, r, "agent-2", "t1") {
		t.Error("agent-2 should NOT be allowed t1")
	}

	if err := r.Revoke(ctx, "agent-1", "t2"); err != nil {
		t.Fatal(err)
	}
	if mustIsAllowed(t, r, "agent-1", "t2") {
		t.Error("t2 should be revoked for agent-1")
	}

	got, err := r.Allowed(ctx, "agent-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "t1" {
		t.Errorf("Allowed(agent-1) = %v, want [t1]", got)
	}
}

func TestRegistryGet(t *testing.T) {
	r := NewRegistry()
	mustRegister(t, r, Definition{ID: "t1", Name: "echo", RiskLevel: RiskLow})

	d, err := r.Get(context.Background(), "t1")
	if err != nil {
		t.Fatal("t1 should be registered:", err)
	}
	if d.Name != "echo" {
		t.Errorf("name = %q, want echo", d.Name)
	}
	if _, err := r.Get(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(missing) err = %v, want ErrNotFound", err)
	}
}

func TestRegistryListTenantFilter(t *testing.T) {
	r := NewRegistry()
	ctx := context.Background()
	// Empty scope is treated as global; explicit global must behave alike.
	mustRegister(t, r, Definition{ID: "g1", Name: "echo", RiskLevel: RiskLow})
	mustRegister(t, r, Definition{ID: "g2", Name: "search", Scope: ScopeGlobal, RiskLevel: RiskLow})
	mustRegister(t, r, Definition{ID: "ta1", Name: "crm-query", Scope: ScopeTenant, TenantID: "tenant-a", RiskLevel: RiskMedium})
	mustRegister(t, r, Definition{ID: "tb1", Name: "wiki-search", Scope: ScopeTenant, TenantID: "tenant-b", RiskLevel: RiskLow})

	visible := func(tenantID string) map[string]bool {
		t.Helper()
		list, err := r.List(ctx, tenantID)
		if err != nil {
			t.Fatal(err)
		}
		set := make(map[string]bool, len(list))
		for _, d := range list {
			set[d.ID] = true
		}
		return set
	}

	if got := visible(""); len(got) != 4 {
		t.Errorf("List(\"\") = %v, want all 4 tools", got)
	}
	got := visible("tenant-a")
	if len(got) != 3 || !got["g1"] || !got["g2"] || !got["ta1"] {
		t.Errorf("List(tenant-a) = %v, want g1,g2,ta1", got)
	}
	got = visible("tenant-b")
	if len(got) != 3 || !got["g1"] || !got["g2"] || !got["tb1"] {
		t.Errorf("List(tenant-b) = %v, want g1,g2,tb1", got)
	}
}

func TestEchoToolDeclaration(t *testing.T) {
	tk := EchoTool()
	d := tk.Declaration()
	if d == nil {
		t.Fatal("declaration should not be nil")
	}
	if d.Name != "echo" {
		t.Errorf("name = %q, want echo", d.Name)
	}
}

// mustRegister registers a tool, failing the test on error.
func mustRegister(t *testing.T, r *Registry, d Definition) {
	t.Helper()
	if err := r.Register(context.Background(), d); err != nil {
		t.Fatal(err)
	}
}

// mustGrant grants a tool to an agent in tenant t1, failing the test on error.
func mustGrant(t *testing.T, r *Registry, agentID, toolID string) {
	t.Helper()
	if err := r.Grant(context.Background(), "t1", agentID, toolID); err != nil {
		t.Fatal(err)
	}
}

// mustIsAllowed checks permission, failing the test on store errors.
func mustIsAllowed(t *testing.T, r *Registry, agentID, toolID string) bool {
	t.Helper()
	ok, err := r.IsAllowed(context.Background(), agentID, toolID)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

// TestGrantIsScopedToItsTenant covers the second lock on the same door: the
// API already refuses to create a cross-tenant grant, and this is the check the
// worker performs at run time, so a grant row that crossed a tenant boundary
// (hand-edited database, a future writer, a bug) still cannot authorise a tool
// call for the wrong tenant.
func TestGrantIsScopedToItsTenant(t *testing.T) {
	ctx := context.Background()
	r := NewRegistry()
	mustRegister(t, r, Definition{ID: "echo", Name: "echo", RiskLevel: RiskLow})

	// A grant recorded for acme.
	if err := r.Grant(ctx, "acme", "agent-1", "echo"); err != nil {
		t.Fatal(err)
	}
	allowed, err := r.IsAllowedForTenant(ctx, "acme", "agent-1", "echo")
	if err != nil || !allowed {
		t.Fatalf("owning tenant: allowed=%v err=%v, want true", allowed, err)
	}
	allowed, err = r.IsAllowedForTenant(ctx, "globex", "agent-1", "echo")
	if err != nil {
		t.Fatal(err)
	}
	if allowed {
		t.Error("another tenant must not inherit a grant it does not own")
	}
	// The plain check stays tenant-agnostic (it answers "is there a grant").
	if !mustIsAllowed(t, r, "agent-1", "echo") {
		t.Error("IsAllowed must still report the existence of the grant")
	}

	// A legacy grant (no tenant recorded) keeps working for every tenant, so an
	// upgrade does not silently revoke tools.
	if err := r.Grant(ctx, "", "agent-legacy", "echo"); err != nil {
		t.Fatal(err)
	}
	for _, tenant := range []string{"acme", "globex"} {
		allowed, err := r.IsAllowedForTenant(ctx, tenant, "agent-legacy", "echo")
		if err != nil || !allowed {
			t.Errorf("legacy grant for %s: allowed=%v err=%v, want true", tenant, allowed, err)
		}
	}

	// Re-granting under another tenant moves the ownership, and revoking
	// removes it entirely.
	if err := r.Grant(ctx, "globex", "agent-1", "echo"); err != nil {
		t.Fatal(err)
	}
	if allowed, _ := r.IsAllowedForTenant(ctx, "acme", "agent-1", "echo"); allowed {
		t.Error("re-granting under globex must move ownership away from acme")
	}
	if err := r.Revoke(ctx, "agent-1", "echo"); err != nil {
		t.Fatal(err)
	}
	if allowed, _ := r.IsAllowedForTenant(ctx, "globex", "agent-1", "echo"); allowed {
		t.Error("revoke must clear the grant for every tenant")
	}
}
