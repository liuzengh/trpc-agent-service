package tenant

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConfigurationSnapshotIsolatedFromContextAndSource(t *testing.T) {
	appID := "app-original"
	input := validCreate("snapshot")
	input.DefaultAgentAppID = &appID
	tenant, err := NewTenant(input)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := NewConfigurationSnapshot(tenant)
	if err != nil {
		t.Fatal(err)
	}
	tenant.DisplayName = "changed after snapshot"
	ctx := WithConfigurationSnapshot(context.Background(), snapshot)
	fromContext, ok := ConfigurationSnapshotFromContext(ctx)
	fromTenant := fromContext.Tenant()
	if !ok || fromTenant.DisplayName != "Example" {
		t.Fatalf("unexpected context snapshot: %+v", fromContext)
	}
	if fromTenant.DefaultAgentAppID == nil || *fromTenant.DefaultAgentAppID != "app-original" {
		t.Fatalf("snapshot lost pointer configuration: %+v", fromTenant.DefaultAgentAppID)
	}
	fromTenant.DisplayName = "caller mutation"
	*fromTenant.DefaultAgentAppID = "caller-app-mutation"
	again, _ := ConfigurationSnapshotFromContext(ctx)
	againTenant := again.Tenant()
	if againTenant.DisplayName != "Example" {
		t.Fatal("context exposed mutable snapshot")
	}
	if againTenant.DefaultAgentAppID == nil || *againTenant.DefaultAgentAppID != "app-original" {
		t.Fatal("context exposed mutable pointer configuration")
	}
	if againTenant.Version != snapshot.Tenant().Version {
		t.Fatalf("snapshot version changed: got %d want %d", againTenant.Version, snapshot.Tenant().Version)
	}
}

func TestWithConfigurationSnapshotRejectsUninitializedSnapshot(t *testing.T) {
	tenant, err := NewTenant(validCreate("snapshot-reject-uninitialized"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := NewConfigurationSnapshot(tenant)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithConfigurationSnapshot(context.Background(), snapshot)
	ctx = WithConfigurationSnapshot(ctx, ConfigurationSnapshot{})
	if snapshot, ok := ConfigurationSnapshotFromContext(ctx); ok {
		t.Fatalf("uninitialized snapshot entered context: %+v", snapshot)
	}
}

func TestConfigurationSnapshotRejectsMissingOrInvalidTenant(t *testing.T) {
	if _, err := NewConfigurationSnapshot(nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil tenant: expected ErrInvalid, got %v", err)
	}
	tests := []struct {
		name, key string
		mutate    func(*Tenant)
	}{
		{name: "invalid tenant id", key: "snapshot-invalid-id", mutate: func(tenant *Tenant) { tenant.TenantID = "not-a-tenant-id" }},
		{name: "zero version", key: "snapshot-zero-version", mutate: func(tenant *Tenant) { tenant.Version = 0 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tenant, err := NewTenant(validCreate(test.key))
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(tenant)
			if _, err := NewConfigurationSnapshot(tenant); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected ErrInvalid, got %v", err)
			}
		})
	}
}

func TestConfigurationSnapshotRequiresActiveTenant(t *testing.T) {
	for _, status := range []Status{StatusSuspended, StatusDisabled} {
		t.Run(string(status), func(t *testing.T) {
			tenant, err := NewTenant(validCreate("snapshot-" + string(status)))
			if err != nil {
				t.Fatal(err)
			}
			tenant.Status = status
			if _, err := NewConfigurationSnapshot(tenant); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected non-active tenant to be rejected, got %v", err)
			}
		})
	}
}

func TestConfigurationSnapshotFromContextRequiresSnapshot(t *testing.T) {
	snapshot, ok := ConfigurationSnapshotFromContext(context.Background())
	if ok || snapshot.Tenant().TenantID != "" {
		t.Fatalf("expected no snapshot, got %+v, ok=%t", snapshot, ok)
	}
	if cloneTenant(nil) != nil {
		t.Fatal("nil tenant clone must remain nil")
	}
}

func TestRunnerIdentityUsesUnambiguousNamespace(t *testing.T) {
	tenantID := "t_01J1K9ZQTVE4PAWF1TSB2WMHNP"
	first, err := NewRunnerIdentity(tenantID, "12", "3:45")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRunnerIdentity(tenantID, "1", "23:45")
	if err != nil {
		t.Fatal(err)
	}
	if first.UserID == second.UserID || first.SessionID == second.SessionID {
		t.Fatalf("ambiguous namespace: %+v %+v", first, second)
	}
}

func TestRunnerIdentityRejectsInvalidInputs(t *testing.T) {
	validID := "t_01J1K9ZQTVE4PAWF1TSB2WMHNP"
	tests := []struct {
		name, tenantID, userID, sessionID string
	}{
		{name: "invalid tenant", tenantID: "invalid", userID: "user", sessionID: "session"},
		{name: "empty user", tenantID: validID, userID: "", sessionID: "session"},
		{name: "empty session", tenantID: validID, userID: "user", sessionID: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewRunnerIdentity(test.tenantID, test.userID, test.sessionID); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected ErrInvalid, got %v", err)
			}
		})
	}
}

func TestConfigurationSnapshotRejectsInvalidExternalTenant(t *testing.T) {
	tenant, err := NewTenant(validCreate("snapshot-invalid"))
	if err != nil {
		t.Fatal(err)
	}
	tenant.TenantKey = "Not-Normalized"
	if _, err := NewConfigurationSnapshot(tenant); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected invalid snapshot, got %v", err)
	}
}

func TestConfigurationSnapshotRequiresCompleteRootState(t *testing.T) {
	tenant, err := NewTenant(validCreate("snapshot-state"))
	if err != nil {
		t.Fatal(err)
	}
	tenant.CreatedAt = time.Time{}
	if err := tenant.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected incomplete tenant rejection, got %v", err)
	}
}
