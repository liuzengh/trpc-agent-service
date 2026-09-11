package tenant

import (
	"testing"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/configcontrol"
)

func TestStableCanaryBucketAcrossRegistries(t *testing.T) {
	v1 := controlTestTenant("tenant-a", "v1", "one")
	v2 := controlTestTenant("tenant-a", "v2", "two")
	base := &config.Config{Coordination: config.CoordinationConfig{Backend: "inmemory"}, Tenants: []config.TenantConfig{v1}}
	first, err := NewRegistry(base)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRegistry(base)
	if err != nil {
		t.Fatal(err)
	}
	state := configcontrol.TenantState{TenantID: "tenant-a", ActiveRevision: "v1", CanaryRevision: "v2", RolloutPercent: 50, Generation: 2}
	if err := first.PublishControlState(state, v1, &v2); err != nil {
		t.Fatal(err)
	}
	if err := second.PublishControlState(state, v1, &v2); err != nil {
		t.Fatal(err)
	}
	for _, session := range []string{"s1", "s2", "s3", "same-session"} {
		left, leftGeneration, err := first.ResolveForSession("tenant-a", session)
		if err != nil {
			t.Fatal(err)
		}
		right, rightGeneration, err := second.ResolveForSession("tenant-a", session)
		if err != nil {
			t.Fatal(err)
		}
		if left.Version != right.Version || leftGeneration != rightGeneration {
			t.Fatalf("session %q routed differently: %s/%d vs %s/%d", session, left.Version, leftGeneration, right.Version, rightGeneration)
		}
	}
}

func controlTestTenant(id, version, instruction string) config.TenantConfig {
	return config.TenantConfig{
		TenantID: id, Version: version, Enabled: true,
		App:      config.AppConfig{Name: "assistant", AgentName: "chat-agent", Instruction: instruction},
		Model:    config.ModelConfig{Provider: "mock", Name: "mock"},
		Channels: []config.ChannelConfig{{Type: "telegram", BindingID: "telegram-main", Enabled: true, TokenEnv: "TG_TOKEN", SigningSecretEnv: "TG_SECRET"}},
		Data: config.DataConfig{
			Session: config.BackendConfig{Type: "inmemory"}, Memory: config.BackendConfig{Type: "inmemory"},
			Summary: config.BackendConfig{Type: "inmemory"}, Artifact: config.BackendConfig{Type: "inmemory"},
			Knowledge: config.BackendConfig{Type: "disabled"}, AuditLog: config.BackendConfig{Type: "stdout"},
		},
		Audit:  config.AuditPolicy{Enabled: true, Sink: "stdout"},
		Budget: config.BudgetPolicy{RequestsPerMinute: 10, MaxInputChars: 1000, MonthlyCostUSD: 10},
	}
}
