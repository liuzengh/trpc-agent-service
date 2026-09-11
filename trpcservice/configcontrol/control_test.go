package configcontrol

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
)

func TestMemoryRevisionImmutableAndCAS(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	v1 := controlTenant("tenant-a", "v1", "hello")
	if err := store.Bootstrap(ctx, []config.TenantConfig{v1}, "test", "bootstrap"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutRevision(ctx, RevisionInput{TenantID: v1.TenantID, Revision: v1.Version, Tenant: v1}); err != nil {
		t.Fatalf("same immutable revision should be idempotent: %v", err)
	}
	changed := v1
	changed.App.Instruction = "changed"
	if _, err := store.PutRevision(ctx, RevisionInput{TenantID: v1.TenantID, Revision: v1.Version, Tenant: changed}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("changed same revision error = %v, want conflict", err)
	}
	v2 := v1
	v2.Version = "v2"
	v2.App.Instruction = "revision two"
	v3 := v1
	v3.Version = "v3"
	v3.App.Instruction = "revision three"
	if _, err := store.PutRevision(ctx, RevisionInput{TenantID: v2.TenantID, Revision: v2.Version, Tenant: v2}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutRevision(ctx, RevisionInput{TenantID: v3.TenantID, Revision: v3.Version, Tenant: v3}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetRevision(ctx, v1.TenantID, v1.Version); err != nil {
		t.Fatalf("historical revision disappeared: %v", err)
	}
	if err := store.Heartbeat(ctx, NodeHeartbeat{NodeID: "node-a", BootID: "boot-a", Ready: true}); err != nil {
		t.Fatal(err)
	}
	if err := store.Heartbeat(ctx, NodeHeartbeat{NodeID: "node-b", BootID: "boot-b", Ready: true}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	successes := 0
	var mu sync.Mutex
	for _, revision := range []string{"v2", "v3"} {
		wg.Add(1)
		go func(revision string) {
			defer wg.Done()
			_, err := store.CreateRelease(ctx, CreateReleaseRequest{
				TenantID: "tenant-a", Kind: ReleaseFull, TargetRevision: revision,
				ExpectedGeneration: 1,
			})
			if err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}(revision)
	}
	wg.Wait()
	if successes != 1 {
		t.Fatalf("concurrent CAS releases succeeded = %d, want 1", successes)
	}
}

func TestMemoryRevisionDoesNotAliasCallerOrReader(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	tenant := controlTenant("tenant-copy", "v1", "original")
	tenant.Tools.Allow = []string{"calculator"}
	tenant.Skills.Allow = []string{"greeting"}
	tenant.Channels[0].AllowedUsers = []string{"alice"}
	if _, err := store.PutRevision(ctx, RevisionInput{TenantID: tenant.TenantID, Revision: tenant.Version, Tenant: tenant}); err != nil {
		t.Fatal(err)
	}
	tenant.App.Instruction = "caller mutation"
	tenant.Tools.Allow[0] = "mutated"
	tenant.Skills.Allow[0] = "mutated"
	tenant.Channels[0].AllowedUsers[0] = "mutated"
	got, err := store.GetRevision(ctx, "tenant-copy", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Tenant.Skills.Allow[0] != "greeting" || got.Tenant.App.Instruction != "original" || got.Tenant.Tools.Allow[0] != "calculator" || got.Tenant.Channels[0].AllowedUsers[0] != "alice" {
		t.Fatalf("stored revision aliased caller: %+v", got.Tenant)
	}
	got.Tenant.Tools.Allow[0] = "reader mutation"
	got.Tenant.Skills.Allow[0] = "reader mutation"
	got.Tenant.Channels[0].AllowedUsers[0] = "reader mutation"
	again, err := store.GetRevision(ctx, "tenant-copy", "v1")
	if err != nil {
		t.Fatal(err)
	}
	if again.Tenant.Skills.Allow[0] != "greeting" || again.Tenant.Tools.Allow[0] != "calculator" || again.Tenant.Channels[0].AllowedUsers[0] != "alice" {
		t.Fatalf("stored revision aliased reader: %+v", again.Tenant)
	}
}

func TestMemoryReleaseRequiresAllPreparedAndApplied(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	v1 := controlTenant("tenant-a", "v1", "one")
	v2 := controlTenant("tenant-a", "v2", "two")
	if err := store.Bootstrap(ctx, []config.TenantConfig{v1}, "test", "bootstrap"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutRevision(ctx, RevisionInput{TenantID: "tenant-a", Revision: "v2", Tenant: v2}); err != nil {
		t.Fatal(err)
	}
	nodes := []NodeIdentity{{NodeID: "node-a", BootID: "boot-a"}, {NodeID: "node-b", BootID: "boot-b"}}
	for _, node := range nodes {
		if err := store.Heartbeat(ctx, NodeHeartbeat{NodeID: node.NodeID, BootID: node.BootID, Ready: true}); err != nil {
			t.Fatal(err)
		}
	}
	release, err := store.CreateRelease(ctx, CreateReleaseRequest{TenantID: "tenant-a", Kind: ReleaseRollback, TargetRevision: "v2", ExpectedGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AckPrepared(ctx, release.ReleaseID, nodes[0], "v2", 1, nil); err != nil {
		t.Fatal(err)
	}
	pending, err := store.ActivateIfReady(ctx, release.ReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	if pending.Status != ReleasePreparing {
		t.Fatalf("release activated before all prepared: %s", pending.Status)
	}
	if err := store.AckPrepared(ctx, release.ReleaseID, nodes[1], "v2", 1, nil); err != nil {
		t.Fatal(err)
	}
	active, err := store.ActivateIfReady(ctx, release.ReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	if active.Status != ReleaseActivePending || active.ResultingGeneration != 2 {
		t.Fatalf("activation = %+v", active)
	}
	if err := store.AckApplied(ctx, release.ReleaseID, nodes[0], "v2", 2, nil); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.GetRelease(ctx, release.ReleaseID); got.Status != ReleaseActivePending {
		t.Fatalf("release verified with one applied node: %s", got.Status)
	}
	if err := store.AckApplied(ctx, release.ReleaseID, nodes[1], "v2", 2, nil); err != nil {
		t.Fatal(err)
	}
	verified, err := store.GetRelease(ctx, release.ReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	if verified.Status != ReleaseVerified {
		t.Fatalf("release status = %s, want verified", verified.Status)
	}
	state, err := store.GetState(ctx, "tenant-a")
	if err != nil || state.ActiveRevision != "v2" || state.Generation != 2 {
		t.Fatalf("state after verified release = %+v, %v", state, err)
	}
	// Duplicate ACKs are harmless and cannot create a second generation.
	if err := store.AckApplied(ctx, release.ReleaseID, nodes[1], "v2", 2, nil); err != nil {
		t.Fatalf("duplicate applied ACK: %v", err)
	}
}

func TestMemoryCanaryRequiresExplicitPromotion(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	v1 := controlTenant("tenant-canary", "v1", "one")
	v2 := controlTenant("tenant-canary", "v2", "two")
	if err := store.Bootstrap(ctx, []config.TenantConfig{v1}, "test", "bootstrap"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutRevision(ctx, RevisionInput{TenantID: v2.TenantID, Revision: v2.Version, Tenant: v2}); err != nil {
		t.Fatal(err)
	}
	node := NodeIdentity{NodeID: "node-a", BootID: "boot-a"}
	if err := store.Heartbeat(ctx, NodeHeartbeat{NodeID: node.NodeID, BootID: node.BootID, Ready: true}); err != nil {
		t.Fatal(err)
	}
	canary, err := store.CreateRelease(ctx, CreateReleaseRequest{TenantID: v1.TenantID, Kind: ReleaseCanary, TargetRevision: v2.Version, TargetRolloutPercent: 25, ExpectedGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AckPrepared(ctx, canary.ReleaseID, node, v2.Version, 1, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ActivateIfReady(ctx, canary.ReleaseID); err != nil {
		t.Fatal(err)
	}
	if err := store.AckApplied(ctx, canary.ReleaseID, node, v2.Version, 2, nil); err != nil {
		t.Fatal(err)
	}
	state, err := store.GetState(ctx, v1.TenantID)
	if err != nil || state.ActiveRevision != v1.Version || state.CanaryRevision != v2.Version || state.RolloutPercent != 25 {
		t.Fatalf("canary changed active state: %+v, %v", state, err)
	}
	promote, err := store.CreateRelease(ctx, CreateReleaseRequest{TenantID: v1.TenantID, Kind: ReleasePromote, TargetRevision: v2.Version, ExpectedGeneration: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AckPrepared(ctx, promote.ReleaseID, node, v2.Version, 2, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ActivateIfReady(ctx, promote.ReleaseID); err != nil {
		t.Fatal(err)
	}
	if err := store.AckApplied(ctx, promote.ReleaseID, node, v2.Version, 3, nil); err != nil {
		t.Fatal(err)
	}
	state, err = store.GetState(ctx, v1.TenantID)
	if err != nil || state.ActiveRevision != v2.Version || state.CanaryRevision != "" {
		t.Fatalf("promoted state = %+v, %v", state, err)
	}
}

func TestControllerRefreshesPersistentState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := NewMemoryStore()
	v1 := controlTenant("tenant-a", "v1", "one")
	if err := store.Bootstrap(ctx, []config.TenantConfig{v1}, "test", "bootstrap"); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	loaded := ""
	controller, err := NewController(ControllerOptions{
		Store: store, NodeID: "node-a", RefreshInterval: 10 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond,
		Apply: func(_ TenantState, active config.TenantConfig, _ *config.TenantConfig) error {
			mu.Lock()
			loaded = active.Version
			mu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer controller.Close()
	if !controller.Ready() {
		t.Fatal("controller did not become ready")
	}
	mu.Lock()
	got := loaded
	mu.Unlock()
	if got != "v1" {
		t.Fatalf("initial revision = %q", got)
	}
}

func TestControllerFailsUnacknowledgedReleaseAfterTimeout(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	v1 := controlTenant("tenant-timeout", "v1", "one")
	v2 := controlTenant("tenant-timeout", "v2", "two")
	if err := store.Bootstrap(ctx, []config.TenantConfig{v1}, "test", "bootstrap"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutRevision(ctx, RevisionInput{TenantID: v2.TenantID, Revision: v2.Version, Tenant: v2}); err != nil {
		t.Fatal(err)
	}
	if err := store.Heartbeat(ctx, NodeHeartbeat{NodeID: "offline-node", BootID: "boot-1", Ready: true}); err != nil {
		t.Fatal(err)
	}
	release, err := store.CreateRelease(ctx, CreateReleaseRequest{TenantID: v1.TenantID, Kind: ReleaseFull, TargetRevision: v2.Version, ExpectedGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := NewController(ControllerOptions{
		Store: store, NodeID: "live-node", BootID: "boot-live", AckTimeout: time.Millisecond,
		Apply: func(TenantState, config.TenantConfig, *config.TenantConfig) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	if err := controller.RefreshNow(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetRelease(ctx, release.ReleaseID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != ReleaseFailed {
		t.Fatalf("timed out release status = %s, want failed", got.Status)
	}
}

func controlTenant(id, version, instruction string) config.TenantConfig {
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
