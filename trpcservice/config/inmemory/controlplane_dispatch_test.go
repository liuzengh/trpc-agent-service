package inmemory

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	gatewaymemory "github.com/liuzengh/trpc-agent-service/trpcservice/gateway/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	profilememory "github.com/liuzengh/trpc-agent-service/trpcservice/profile/inmemory"
	sessionmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker/mockmodel"
)

func TestPublishedControlPlaneDrivesLocalDispatcher(t *testing.T) {
	ctx, _, apps, configs := setup(t)
	published, err := configs.Publish(ctx, config.PublishInput{TenantID: "tenant-a", ExpectedTenantVersion: 1, Payload: payload(7), Metadata: metadata("publish")})
	if err != nil {
		t.Fatal(err)
	}
	app, err := apps.Get(ctx, "tenant-a", "app")
	if err != nil {
		t.Fatal(err)
	}
	revision, err := apps.GetRevision(ctx, "tenant-a", "app", app.CurrentRevision)
	if err != nil {
		t.Fatal(err)
	}
	key := profile.ExecutionProfileKey{TenantID: "tenant-a", TenantVersion: published.Tenant.Version, AgentAppID: "app", AgentAppVersion: app.Version, AgentAppRevision: revision.Revision, ContentDigest: revision.ContentDigest, ConfigVersion: published.Snapshot.ConfigVersion, PolicyVersion: published.Snapshot.Payload.PolicyVersion}
	profiles := profilememory.NewResolver(profile.ExecutionProfileSnapshot{Key: key, TenantVersion: published.Tenant.Version, AgentAppVersion: app.Version, ContentDigest: revision.ContentDigest, AgentKind: "llm", Instruction: revision.Instruction, ModelProfileRef: profile.VersionedRef{ID: "mock", Version: 1}})
	tasks := gatewaymemory.NewTaskStore()
	sessions := sessionmemory.New()
	model := mockmodel.New()
	dispatcher := gateway.LocalDispatcher{Tasks: tasks, Bindings: configs, Executor: worker.DeterministicTestExecutor{Tasks: tasks, Profiles: profiles, Sessions: sessions, Model: model}}
	tc := tenant.Context{TenantID: "tenant-a", TenantVersion: published.Tenant.Version, AgentAppID: "app", SubjectID: "user", Channel: "fake", TrustedSource: "channel_binding:fake-account"}
	handle, err := dispatcher.Dispatch(context.Background(), gateway.DispatchRequest{Tenant: tc, RequestID: "request", SessionID: "session", UserID: "user", PayloadRef: "payload://request"})
	if err != nil {
		t.Fatal(err)
	}
	if handle.Status != "succeeded" || model.Calls("tenant-a", "request") != 1 {
		t.Fatalf("handle=%#v calls=%d", handle, model.Calls("tenant-a", "request"))
	}
}

func TestConfigPublishAndRollbackOnlyAffectNewRequests(t *testing.T) {
	ctx, _, apps, configs := setup(t)
	first, err := configs.Publish(ctx, config.PublishInput{TenantID: "tenant-a", ExpectedTenantVersion: 1, Payload: payload(7), Metadata: metadata("first")})
	if err != nil {
		t.Fatal(err)
	}
	tasks := gatewaymemory.NewTaskStore()
	dispatcher := gateway.BrokerDispatcher{Tasks: tasks, Bindings: configs}
	contextFor := func(version int64) tenant.Context {
		return tenant.Context{TenantID: "tenant-a", TenantVersion: version, AgentAppID: "app", SubjectID: "user", Channel: "fake", TrustedSource: "channel_binding:fake-account"}
	}
	if _, err := dispatcher.Dispatch(ctx, gateway.DispatchRequest{Tenant: contextFor(first.Tenant.Version), RequestID: "request-old", SessionID: "session-old", UserID: "user", PayloadRef: "payload://old"}); err != nil {
		t.Fatal(err)
	}
	oldExecution, err := tasks.GetExecution(ctx, gateway.ExecutionKey{TenantID: "tenant-a", RequestID: "request-old"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := configs.Publish(ctx, config.PublishInput{TenantID: "tenant-a", ExpectedTenantVersion: first.Tenant.Version, Payload: payload(8), Metadata: metadata("second")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.Dispatch(ctx, gateway.DispatchRequest{Tenant: contextFor(second.Tenant.Version), RequestID: "request-new", SessionID: "session-new", UserID: "user", PayloadRef: "payload://new"}); err != nil {
		t.Fatal(err)
	}
	newExecution, err := tasks.GetExecution(ctx, gateway.ExecutionKey{TenantID: "tenant-a", RequestID: "request-new"})
	if err != nil {
		t.Fatal(err)
	}
	rolled, err := configs.Rollback(ctx, config.RollbackInput{TenantID: "tenant-a", ExpectedTenantVersion: second.Tenant.Version, TargetVersion: first.Snapshot.ConfigVersion, Metadata: metadata("rollback")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.Dispatch(ctx, gateway.DispatchRequest{Tenant: contextFor(rolled.Tenant.Version), RequestID: "request-rolled", SessionID: "session-rolled", UserID: "user", PayloadRef: "payload://rolled"}); err != nil {
		t.Fatal(err)
	}
	rolledExecution, err := tasks.GetExecution(ctx, gateway.ExecutionKey{TenantID: "tenant-a", RequestID: "request-rolled"})
	if err != nil {
		t.Fatal(err)
	}
	if oldExecution.Envelope.ConfigVersion != first.Snapshot.ConfigVersion || oldExecution.Envelope.PolicyVersion != 7 ||
		newExecution.Envelope.ConfigVersion != second.Snapshot.ConfigVersion || newExecution.Envelope.PolicyVersion != 8 ||
		rolledExecution.Envelope.ConfigVersion != rolled.Snapshot.ConfigVersion || rolledExecution.Envelope.PolicyVersion != 7 ||
		rolledExecution.Envelope.ConfigVersion <= newExecution.Envelope.ConfigVersion {
		t.Fatalf("old=%#v new=%#v rolled=%#v", oldExecution.Envelope, newExecution.Envelope, rolledExecution.Envelope)
	}
	app, err := apps.Get(ctx, "tenant-a", "app")
	if err != nil {
		t.Fatal(err)
	}
	revision, err := apps.GetRevision(ctx, "tenant-a", "app", app.CurrentRevision)
	if err != nil {
		t.Fatal(err)
	}
	oldKey := profile.ExecutionProfileKey{TenantID: "tenant-a", TenantVersion: oldExecution.Envelope.TenantVersion, AgentAppID: "app", AgentAppVersion: oldExecution.Envelope.AgentAppVersion, AgentAppRevision: revision.Revision, ContentDigest: revision.ContentDigest, ConfigVersion: oldExecution.Envelope.ConfigVersion, PolicyVersion: oldExecution.Envelope.PolicyVersion}
	profiles := profilememory.NewResolver(profile.ExecutionProfileSnapshot{Key: oldKey, TenantVersion: oldExecution.Envelope.TenantVersion, AgentAppVersion: oldExecution.Envelope.AgentAppVersion, ContentDigest: revision.ContentDigest})
	model := mockmodel.New()
	executor := worker.DeterministicTestExecutor{Tasks: tasks, Profiles: profiles, Sessions: sessionmemory.New(), Model: model}
	if err := executor.Execute(ctx, oldExecution.Envelope); err != nil || model.Calls("tenant-a", "request-old") != 1 {
		t.Fatalf("old execution err=%v calls=%d", err, model.Calls("tenant-a", "request-old"))
	}
}
