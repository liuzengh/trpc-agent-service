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

// mustGrant grants a tool to an agent, failing the test on error.
func mustGrant(t *testing.T, r *Registry, agentID, toolID string) {
	t.Helper()
	if err := r.Grant(context.Background(), agentID, toolID); err != nil {
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
