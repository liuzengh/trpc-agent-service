package tenant

import (
	"errors"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/configcontrol"
	"testing"
)

func TestTransportPreparationGuardsApplyPublishAndRollback(t *testing.T) {
	cfg := &config.Config{Coordination: config.CoordinationConfig{Backend: "inmemory"}, Tenants: []config.TenantConfig{tenantConfig("tenant-a", "v1", "binding-a")}}
	r, err := NewRegistry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	fail := false
	calls := 0
	last := ""
	hookErr := errors.New("transport unavailable")
	if err := r.SetBeforePublish(func(values []config.TenantConfig) error {
		calls++
		last = values[0].Version
		if fail {
			return hookErr
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || last != "v1" {
		t.Fatal("initial snapshot not reconciled")
	}
	cfg.Tenants[0].Version = "v2"
	fail = true
	if err := r.Apply(cfg); !errors.Is(err, hookErr) {
		t.Fatalf("apply: %v", err)
	}
	if current, _ := r.Tenant("tenant-a"); current.Version != "v1" {
		t.Fatal("failed preparation published")
	}
	fail = false
	if err := r.Apply(cfg); err != nil {
		t.Fatal(err)
	}
	fail = true
	if _, err := r.Rollback("tenant-a"); !errors.Is(err, hookErr) {
		t.Fatalf("rollback: %v", err)
	}
	if current, _ := r.Tenant("tenant-a"); current.Version != "v2" {
		t.Fatal("failed rollback changed snapshot")
	}
	fail = false
	if _, err := r.Rollback("tenant-a"); err != nil {
		t.Fatal(err)
	}
	target := tenantConfig("tenant-a", "v3", "binding-a")
	state := configcontrol.TenantState{TenantID: "tenant-a", ActiveRevision: "v3", Generation: 3}
	fail = true
	if err := r.PublishControlState(state, target, nil); !errors.Is(err, hookErr) {
		t.Fatalf("control publish: %v", err)
	}
	if current, _ := r.Tenant("tenant-a"); current.Version != "v1" {
		t.Fatal("failed control publish changed snapshot")
	}
	fail = false
	if err := r.PublishControlState(state, target, nil); err != nil {
		t.Fatal(err)
	}
	if current, _ := r.Tenant("tenant-a"); current.Version != "v3" || last != "v3" {
		t.Fatal("prepared control snapshot not published")
	}
}
func TestCanaryRejectsDifferentBotTransport(t *testing.T) {
	active := tenantConfig("tenant-a", "v1", "binding-a")
	active.Channels = []config.ChannelConfig{{Type: "wecom-aibot", BindingID: "bot", Enabled: true, BotIDEnv: "BOT_ID", BotSecretEnv: "BOT_SECRET"}}
	r := &Registry{current: snapshot{tenants: map[string]config.TenantConfig{}, bindings: map[string]Binding{}}, history: map[string][]config.TenantConfig{}, revisions: make(map[string]map[string][32]byte)}
	canary := *cloneTenant(&active)
	canary.Version = "v2"
	canary.Channels[0].BotSecretEnv = "NEW_SECRET"
	state := configcontrol.TenantState{TenantID: "tenant-a", ActiveRevision: "v1", CanaryRevision: "v2", Generation: 1, RolloutPercent: 10}
	if err := r.PublishControlState(state, active, &canary); err == nil {
		t.Fatal("canary changed shared transport")
	}
	canary.Channels[0].BotSecretEnv = "BOT_SECRET"
	canary.App.Instruction = "new model instruction"
	if err := r.PublishControlState(state, active, &canary); err != nil {
		t.Fatal(err)
	}
}
