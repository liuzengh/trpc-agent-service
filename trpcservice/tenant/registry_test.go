package tenant

import (
	"errors"
	"testing"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
)

func TestRollbackRejectsBindingOwnedByAnotherTenant(t *testing.T) {
	initial := &config.Config{
		Coordination: config.CoordinationConfig{Backend: "inmemory"},
		Tenants: []config.TenantConfig{
			tenantConfig("tenant-a", "v1", "shared-binding"),
			tenantConfig("tenant-b", "v1", "tenant-b-binding"),
		},
	}
	registry, err := NewRegistry(initial)
	if err != nil {
		t.Fatal(err)
	}
	next := &config.Config{
		Coordination: config.CoordinationConfig{Backend: "inmemory"},
		Tenants: []config.TenantConfig{
			tenantConfig("tenant-a", "v2", "tenant-a-binding"),
			tenantConfig("tenant-b", "v2", "shared-binding"),
		},
	}
	if err := registry.Apply(next); err != nil {
		t.Fatal(err)
	}

	if _, err := registry.Rollback("tenant-a"); !errors.Is(err, ErrBindingConflict) {
		t.Fatalf("rollback error = %v, want binding conflict", err)
	}
	active, err := registry.Tenant("tenant-a")
	if err != nil || active.Version != "v2" {
		t.Fatalf("failed rollback changed active tenant: %+v, %v", active, err)
	}
	owner, err := registry.ResolveBinding("telegram", "shared-binding")
	if err != nil || owner.Tenant.TenantID != "tenant-b" {
		t.Fatalf("binding owner changed after rejected rollback: %+v, %v", owner, err)
	}
	// A failed attempt must not consume the rollback revision.
	if _, err := registry.Rollback("tenant-a"); !errors.Is(err, ErrBindingConflict) {
		t.Fatalf("rollback history was mutated after rejection: %v", err)
	}
}

func TestApplyRejectsChangedConfigAtSameVersion(t *testing.T) {
	initialTenant := tenantConfig("tenant-a", "v1", "binding-a")
	registry, err := NewRegistry(&config.Config{
		Coordination: config.CoordinationConfig{Backend: "inmemory"},
		Tenants:      []config.TenantConfig{initialTenant},
	})
	if err != nil {
		t.Fatal(err)
	}
	changed := initialTenant
	changed.App.Instruction = "changed without a new revision"
	err = registry.Apply(&config.Config{
		Coordination: config.CoordinationConfig{Backend: "inmemory"},
		Tenants:      []config.TenantConfig{changed},
	})
	if !errors.Is(err, ErrVersionImmutable) {
		t.Fatalf("same-version update error = %v", err)
	}
	active, err := registry.Tenant("tenant-a")
	if err != nil || active.App.Instruction != initialTenant.App.Instruction {
		t.Fatalf("rejected update changed active snapshot: %+v, %v", active, err)
	}
}

func TestResolveBindingForTenantFencesOutboxOwner(t *testing.T) {
	registry, err := NewRegistry(&config.Config{
		Coordination: config.CoordinationConfig{Backend: "inmemory"},
		Tenants:      []config.TenantConfig{tenantConfig("tenant-b", "v1", "reassigned-binding")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.ResolveBindingForTenant("tenant-a", "telegram", "reassigned-binding"); !errors.Is(err, ErrBindingTenantMismatch) {
		t.Fatalf("cross-tenant resolve = %v, want ErrBindingTenantMismatch", err)
	}
	binding, err := registry.ResolveBindingForTenant("tenant-b", "telegram", "reassigned-binding")
	if err != nil || binding.Tenant.TenantID != "tenant-b" {
		t.Fatalf("owner resolve = %+v, %v", binding, err)
	}
}

func TestApplyRejectsChangedPreviouslySeenVersion(t *testing.T) {
	v1 := tenantConfig("tenant-a", "v1", "binding-a")
	registry, err := NewRegistry(&config.Config{
		Coordination: config.CoordinationConfig{Backend: "inmemory"},
		Tenants:      []config.TenantConfig{v1},
	})
	if err != nil {
		t.Fatal(err)
	}
	v2 := v1
	v2.Version = "v2"
	v2.App.Instruction = "revision two"
	if err := registry.Apply(&config.Config{
		Coordination: config.CoordinationConfig{Backend: "inmemory"},
		Tenants:      []config.TenantConfig{v2},
	}); err != nil {
		t.Fatal(err)
	}

	changedV1 := v1
	changedV1.App.Instruction = "different content reusing v1"
	err = registry.Apply(&config.Config{
		Coordination: config.CoordinationConfig{Backend: "inmemory"},
		Tenants:      []config.TenantConfig{changedV1},
	})
	if !errors.Is(err, ErrVersionImmutable) {
		t.Fatalf("previously seen revision reuse error = %v", err)
	}
	active, err := registry.Tenant("tenant-a")
	if err != nil || active.Version != "v2" || active.App.Instruction != v2.App.Instruction {
		t.Fatalf("rejected historical revision reuse changed active snapshot: %+v, %v", active, err)
	}
}

func tenantConfig(id, version, bindingID string) config.TenantConfig {
	return config.TenantConfig{
		TenantID: id,
		Version:  version,
		Enabled:  true,
		App: config.AppConfig{
			Name:      "assistant",
			AgentName: "chat-agent",
		},
		Model: config.ModelConfig{Provider: "mock", Name: "mock"},
		Channels: []config.ChannelConfig{{
			Type:             "telegram",
			BindingID:        bindingID,
			Enabled:          true,
			TokenEnv:         "TELEGRAM_TOKEN",
			SigningSecretEnv: "TELEGRAM_SECRET",
		}},
		Data: config.DataConfig{
			Session:   config.BackendConfig{Type: "inmemory"},
			Memory:    config.BackendConfig{Type: "inmemory"},
			Summary:   config.BackendConfig{Type: "inmemory"},
			Artifact:  config.BackendConfig{Type: "inmemory"},
			Knowledge: config.BackendConfig{Type: "disabled"},
			AuditLog:  config.BackendConfig{Type: "stdout"},
		},
		Audit:  config.AuditPolicy{Enabled: true, Sink: "stdout"},
		Budget: config.BudgetPolicy{RequestsPerMinute: 10, MaxInputChars: 1000},
	}
}
