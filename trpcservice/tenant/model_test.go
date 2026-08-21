package tenant

import (
	"context"
	"errors"
	"testing"
)

func validTenant() Tenant {
	return Tenant{ID: "tenant-a", Name: "Tenant A", Status: StatusActive, ConfigVersion: 1, DefaultAgentID: "agent-a", Backend: BackendPolicy{Session: "redis", Memory: "postgres", Vector: "qdrant", Object: "s3"}}
}
func validAgent() AgentApp {
	return AgentApp{TenantID: "tenant-a", ID: "agent-a", Name: "Agent A", Version: 1, Status: "released", ModelConfigRef: "secret://model/a"}
}
func validBinding() ChannelBinding {
	return ChannelBinding{TenantID: "tenant-a", ID: "binding-a", Channel: "telegram", ExternalAppID: "bot-1", SecretRef: "secret://telegram/a", Enabled: true}
}

func TestDomainValidation(t *testing.T) {
	if err := validTenant().Validate(); err != nil {
		t.Fatal(err)
	}
	bad := validTenant()
	bad.ConfigVersion = 0
	if err := bad.Validate(); err == nil {
		t.Fatal("expected invalid config version")
	}
	bad = validTenant()
	bad.Status = ""
	if err := bad.Validate(); err == nil {
		t.Fatal("expected explicit tenant status")
	}
	bad = validTenant()
	bad.Backend.Object = ""
	if err := bad.Validate(); err == nil {
		t.Fatal("expected complete backend policy")
	}
	if err := validAgent().Validate(); err != nil {
		t.Fatal(err)
	}
	if err := validBinding().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestResolverUsesVerifiedBindingMapping(t *testing.T) {
	r := NewMemoryRegistry()
	if err := r.PutTenant(validTenant()); err != nil {
		t.Fatal(err)
	}
	if err := r.PutAgent(validAgent()); err != nil {
		t.Fatal(err)
	}
	if err := r.PutBinding(validBinding()); err != nil {
		t.Fatal(err)
	}
	tc, err := (RegistryResolver{Registry: r}).Resolve(context.Background(), ResolveRequest{Channel: "telegram", ExternalAppID: "bot-1", ExternalUser: "user-1", RequestID: "req-1", MessageID: "msg-1", TraceID: "trace-1"})
	if err != nil {
		t.Fatal(err)
	}
	if tc.TenantID != "tenant-a" || tc.AgentAppID != "agent-a" || tc.BindingID != "binding-a" {
		t.Fatalf("unexpected tenant context: %+v", tc)
	}
	if _, err = (RegistryResolver{Registry: r}).Resolve(context.Background(), ResolveRequest{Channel: "telegram", ExternalAppID: "tenant-a", RequestID: "r", MessageID: "m", TraceID: "t"}); !errors.Is(err, ErrBindingNotFound) {
		t.Fatalf("request-provided tenant must not resolve a binding: %v", err)
	}
}

func TestRequireTenantRejectsMissingAndCrossTenantContext(t *testing.T) {
	if _, err := RequireTenant(context.Background(), "tenant-a"); err == nil {
		t.Fatal("expected missing context error")
	}
	tc := TenantContext{TenantID: "tenant-a", AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web", RequestID: "req", MessageID: "msg", TraceID: "trace", ConfigVersion: 1, BackendPolicy: validTenant().Backend}
	ctx := WithContext(context.Background(), tc)
	if _, err := RequireTenant(ctx, "tenant-b"); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("expected tenant mismatch, got %v", err)
	}
	if got, err := RequireTenant(ctx, "tenant-a"); err != nil || got.TenantID != "tenant-a" {
		t.Fatalf("expected authorized tenant: %v %+v", err, got)
	}
}

func TestResolverRejectsInactiveTenantAndMismatchedAgent(t *testing.T) {
	r := NewMemoryRegistry()
	tenant := validTenant()
	tenant.Status = StatusSuspended
	if err := r.PutTenant(tenant); err != nil {
		t.Fatal(err)
	}
	if err := r.PutAgent(validAgent()); err != nil {
		t.Fatal(err)
	}
	if err := r.PutBinding(validBinding()); err != nil {
		t.Fatal(err)
	}
	_, err := (RegistryResolver{Registry: r}).Resolve(context.Background(), ResolveRequest{Channel: "telegram", ExternalAppID: "bot-1", RequestID: "r", MessageID: "m", TraceID: "t"})
	if !errors.Is(err, ErrTenantInactive) {
		t.Fatalf("expected inactive tenant, got %v", err)
	}
}

func TestResolverFailurePaths(t *testing.T) {
	req := ResolveRequest{Channel: "telegram", ExternalAppID: "bot-1", RequestID: "r", MessageID: "m", TraceID: "t"}
	if _, err := (RegistryResolver{}).Resolve(context.Background(), req); err == nil {
		t.Fatal("expected missing registry error")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (RegistryResolver{Registry: NewMemoryRegistry()}).Resolve(ctx, req); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled context, got %v", err)
	}

	binding := validBinding()
	binding.Enabled = false
	registry := fixedRegistry{tenant: validTenant(), agent: validAgent(), binding: binding}
	if _, err := (RegistryResolver{Registry: registry}).Resolve(context.Background(), req); !errors.Is(err, ErrBindingNotFound) {
		t.Fatalf("expected disabled binding rejection, got %v", err)
	}

	binding = validBinding()
	binding.ExternalAppID = "other-bot"
	registry.binding = binding
	if _, err := (RegistryResolver{Registry: registry}).Resolve(context.Background(), req); !errors.Is(err, ErrBindingNotFound) {
		t.Fatalf("expected mismatched binding rejection, got %v", err)
	}

	binding = validBinding()
	registry.binding = binding
	registry.agent.Status = ""
	if _, err := (RegistryResolver{Registry: registry}).Resolve(context.Background(), req); err == nil {
		t.Fatal("expected invalid agent rejection")
	}
}

func TestMemoryRegistryRejectsCrossTenantBindingConflict(t *testing.T) {
	r := NewMemoryRegistry()
	first := validBinding()
	if err := r.PutBinding(first); err != nil {
		t.Fatal(err)
	}
	second := first
	second.ID = "binding-b"
	second.TenantID = "tenant-b"
	if err := r.PutBinding(second); !errors.Is(err, ErrBindingConflict) {
		t.Fatalf("expected cross-tenant binding conflict, got %v", err)
	}
}

func TestTenantContextCopiesPermissions(t *testing.T) {
	permissions := []string{"tool.read"}
	tc := TenantContext{TenantID: "tenant-a", AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web", RequestID: "req", MessageID: "msg", TraceID: "trace", ConfigVersion: 1, BackendPolicy: validTenant().Backend, Permissions: permissions}
	ctx := WithContext(context.Background(), tc)
	permissions[0] = "tool.write"
	stored, ok := FromContext(ctx)
	if !ok || !stored.HasPermission("tool.read") || stored.HasPermission("tool.write") {
		t.Fatalf("context permissions changed through source slice: %+v", stored.Permissions)
	}
	stored.Permissions[0] = "tool.write"
	storedAgain, _ := FromContext(ctx)
	if !storedAgain.HasPermission("tool.read") {
		t.Fatal("context permissions changed through returned slice")
	}
}

type fixedRegistry struct {
	tenant  Tenant
	agent   AgentApp
	binding ChannelBinding
}

func (r fixedRegistry) Tenant(context.Context, string) (Tenant, error) { return r.tenant, nil }
func (r fixedRegistry) Agent(context.Context, string, string) (AgentApp, error) {
	return r.agent, nil
}
func (r fixedRegistry) Binding(context.Context, string, string) (ChannelBinding, error) {
	return r.binding, nil
}
