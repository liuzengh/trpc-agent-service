package inmemory

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	appmemory "github.com/liuzengh/trpc-agent-service/trpcservice/agentapp/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/inmemory"
)

func metadata(reason string) tenant.ChangeMetadata {
	return tenant.ChangeMetadata{ActorType: "admin", ActorID: "operator", ReasonCode: reason, CorrelationID: "correlation", TraceID: "trace"}
}
func appMetadata() agentapp.ChangeMetadata {
	return agentapp.ChangeMetadata{ActorType: "admin", ActorID: "operator", Reason: "setup", CorrelationID: "correlation", TraceID: "trace"}
}

func setup(t *testing.T) (context.Context, *tenantmemory.Repository, *appmemory.Repository, *Repository) {
	t.Helper()
	ctx := context.Background()
	tenants := tenantmemory.New()
	if _, err := tenants.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: "tenant-a", TenantKey: "tenant-a", DisplayName: "Tenant A"}, ChangeMetadata: metadata("create")}); err != nil {
		t.Fatal(err)
	}
	apps := appmemory.New()
	app, err := apps.Create(ctx, agentapp.CreateInput{App: agentapp.AgentApp{TenantID: "tenant-a", AgentAppID: "app", AgentAppKey: "assistant", DisplayName: "Assistant"}, ChangeMetadata: appMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	revision, err := apps.CreateDraft(ctx, agentapp.CreateDraftInput{TenantID: "tenant-a", AgentAppID: "app", ExpectedAppVersion: app.Version, Revision: agentapp.Revision{AgentKind: "llm", Instruction: "help", ModelProfileID: "mock", ModelProfileVersion: 1}, ChangeMetadata: appMetadata()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = apps.Publish(ctx, agentapp.PublishInput{TenantID: "tenant-a", AgentAppID: "app", Revision: revision.Revision, ExpectedAppVersion: 2, ExpectedDraftVersion: 1, ChangeMetadata: appMetadata()}); err != nil {
		t.Fatal(err)
	}
	return ctx, tenants, apps, New(tenants, apps)
}

func payload(policy int64) config.ConfigV1 {
	return config.ConfigV1{SchemaVersion: 1, DefaultAgentAppID: "app", PolicyVersion: policy, ChannelBindings: []config.ChannelBinding{{BindingID: "fake-account", Channel: "fake", ExternalAccountID: "account", AgentAppID: "app", SecretRef: secrets.SecretRef{Ref: "secret://fake", Version: 1}}}}
}

func TestPublishAndCopyForwardRollback(t *testing.T) {
	ctx, tenants, _, configs := setup(t)
	first, err := configs.Publish(ctx, config.PublishInput{TenantID: "tenant-a", ExpectedTenantVersion: 1, Payload: payload(1), Metadata: metadata("publish")})
	if err != nil {
		t.Fatal(err)
	}
	if first.Snapshot.ConfigVersion != 1 || first.Tenant.Version != 2 || first.Tenant.ActiveConfigVersion != 1 {
		t.Fatalf("first=%#v", first)
	}
	binding, err := configs.ResolveExecutionBinding(ctx, tenant.Context{TenantID: "tenant-a", TenantVersion: 2, AgentAppID: "app", SubjectID: "user", Channel: "fake", TrustedSource: "channel_binding:fake-account"})
	if err != nil {
		t.Fatal(err)
	}
	if binding.ConfigVersion != 1 || binding.PolicyVersion != 1 {
		t.Fatalf("binding=%#v", binding)
	}
	second, err := configs.Publish(ctx, config.PublishInput{TenantID: "tenant-a", ExpectedTenantVersion: 2, Payload: payload(2), Metadata: metadata("publish-2")})
	if err != nil {
		t.Fatal(err)
	}
	rolled, err := configs.Rollback(ctx, config.RollbackInput{TenantID: "tenant-a", ExpectedTenantVersion: second.Tenant.Version, TargetVersion: first.Snapshot.ConfigVersion, Metadata: metadata("rollback")})
	if err != nil {
		t.Fatal(err)
	}
	if rolled.Snapshot.ConfigVersion <= second.Snapshot.ConfigVersion || rolled.Snapshot.Payload.PolicyVersion != 1 {
		t.Fatalf("rollback=%#v", rolled)
	}
	current, err := tenants.Get(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if current.ActiveConfigVersion != rolled.Snapshot.ConfigVersion {
		t.Fatalf("tenant=%#v", current)
	}
}

func TestConcurrentPublishCASHasOneWinner(t *testing.T) {
	ctx, _, _, configs := setup(t)
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := int64(1); i <= 2; i++ {
		wg.Add(1)
		go func(policy int64) {
			defer wg.Done()
			_, err := configs.Publish(ctx, config.PublishInput{TenantID: "tenant-a", ExpectedTenantVersion: 1, Payload: payload(policy), Metadata: metadata("race")})
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	success, conflict := 0, 0
	for err := range results {
		switch {
		case err == nil:
			success++
		case errors.Is(err, tenant.ErrVersionConflict):
			conflict++
		default:
			t.Fatalf("unexpected: %v", err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("success=%d conflict=%d", success, conflict)
	}
}

func TestBindingRequiresExactTrustedBinding(t *testing.T) {
	ctx, _, _, configs := setup(t)
	published, err := configs.Publish(ctx, config.PublishInput{TenantID: "tenant-a", ExpectedTenantVersion: 1, Payload: payload(1), Metadata: metadata("publish")})
	if err != nil {
		t.Fatal(err)
	}
	_, err = configs.ResolveExecutionBinding(ctx, tenant.Context{TenantID: "tenant-a", TenantVersion: published.Tenant.Version, AgentAppID: "app", SubjectID: "user", Channel: "fake", TrustedSource: "channel_binding:forged"})
	if !errors.Is(err, config.ErrTenantScope) {
		t.Fatalf("got %v", err)
	}
}

func TestReleaseSelectsStableTenantCandidateAndPinnedWorkSurvivesRollback(t *testing.T) {
	ctx, tenants, _, configs := setup(t)
	baseline, err := configs.Publish(ctx, config.PublishInput{TenantID: "tenant-a", ExpectedTenantVersion: 1, Payload: payload(1), Metadata: metadata("baseline")})
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := configs.Stage(ctx, config.StageInput{TenantID: "tenant-a", ExpectedTenantVersion: baseline.Tenant.Version, Payload: payload(2), Metadata: metadata("candidate")})
	if err != nil {
		t.Fatal(err)
	}
	current, err := tenants.Get(ctx, "tenant-a")
	if err != nil || current.ActiveConfigVersion != baseline.Snapshot.ConfigVersion || current.Version != baseline.Tenant.Version {
		t.Fatalf("staging changed tenant=%#v err=%v", current, err)
	}
	release, err := configs.CreateRelease(ctx, config.ReleaseCreateInput{ReleaseID: "release-a", Percentage: 0, Salt: "0123456789abcdef", Metadata: metadata("release"), Targets: []config.ReleaseTarget{{
		TenantID: "tenant-a", BaselineConfigVersion: baseline.Snapshot.ConfigVersion, CandidateConfigVersion: candidate.ConfigVersion,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	selected, err := configs.SelectEffective(ctx, "tenant-a", baseline.Snapshot.ConfigVersion)
	if err != nil || selected.ConfigVersion != baseline.Snapshot.ConfigVersion {
		t.Fatalf("zero rollout selected=%#v err=%v", selected, err)
	}
	release, err = configs.UpdateRelease(ctx, config.ReleaseUpdateInput{ReleaseID: release.ReleaseID, ExpectedVersion: release.Version, Percentage: 100, Metadata: metadata("rollout")})
	if err != nil {
		t.Fatal(err)
	}
	selected, err = configs.SelectEffective(ctx, "tenant-a", baseline.Snapshot.ConfigVersion)
	if err != nil || selected.ConfigVersion != candidate.ConfigVersion || selected.Payload.PolicyVersion != 2 {
		t.Fatalf("full rollout selected=%#v err=%v", selected, err)
	}
	tc := tenant.Context{TenantID: "tenant-a", TenantVersion: baseline.Tenant.Version, AgentAppID: "app", SubjectID: "user", Channel: "fake", TrustedSource: "channel_binding:fake-account"}
	pinned, err := configs.ResolveExecutionBindingAt(ctx, tc, candidate.ConfigVersion)
	if err != nil || pinned.ConfigVersion != candidate.ConfigVersion {
		t.Fatalf("pinned=%#v err=%v", pinned, err)
	}
	if _, err = configs.RollbackRelease(ctx, config.ReleaseRollbackInput{ReleaseID: release.ReleaseID, ExpectedVersion: release.Version, Metadata: metadata("rollback-release")}); err != nil {
		t.Fatal(err)
	}
	selected, err = configs.SelectEffective(ctx, "tenant-a", baseline.Snapshot.ConfigVersion)
	if err != nil || selected.ConfigVersion != baseline.Snapshot.ConfigVersion {
		t.Fatalf("rollback selected=%#v err=%v", selected, err)
	}
	pinned, err = configs.ResolveExecutionBindingAt(ctx, tc, candidate.ConfigVersion)
	if err != nil || pinned.ConfigVersion != candidate.ConfigVersion {
		t.Fatalf("rollback changed pinned work=%#v err=%v", pinned, err)
	}
}
