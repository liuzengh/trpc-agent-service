package inmemory

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
)

func appMeta() agentapp.ChangeMetadata {
	return agentapp.ChangeMetadata{ActorType: "admin", ActorID: "operator", Reason: "test", CorrelationID: "correlation", TraceID: "trace"}
}

func TestRepositoryPublishImmutableAndTenantScoped(t *testing.T) {
	ctx := context.Background()
	r := New()
	app, err := r.Create(ctx, agentapp.CreateInput{App: agentapp.AgentApp{TenantID: "tenant-a", AgentAppID: "app", AgentAppKey: "assistant", DisplayName: "Assistant"}, ChangeMetadata: appMeta()})
	if err != nil {
		t.Fatal(err)
	}
	rev, err := r.CreateDraft(ctx, agentapp.CreateDraftInput{TenantID: "tenant-a", AgentAppID: "app", ExpectedAppVersion: app.Version, Revision: agentapp.Revision{AgentKind: "llm", Instruction: "help", ModelProfileID: "model", ModelProfileVersion: 1}, ChangeMetadata: appMeta()})
	if err != nil {
		t.Fatal(err)
	}
	published, err := r.Publish(ctx, agentapp.PublishInput{TenantID: "tenant-a", AgentAppID: "app", Revision: rev.Revision, ExpectedAppVersion: 2, ExpectedDraftVersion: rev.DraftVersion, ChangeMetadata: appMeta()})
	if err != nil {
		t.Fatal(err)
	}
	published.Revision.GenerationConfig = map[string]any{"mutated": true}
	loaded, err := r.GetRevision(ctx, "tenant-a", "app", rev.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ContentDigest == "" || loaded.GenerationConfig["mutated"] != nil {
		t.Fatal("published revision was not immutable/defensively copied")
	}
	_, err = r.GetRevision(ctx, "tenant-b", "app", rev.Revision)
	if !errors.Is(err, agentapp.ErrNotFound) {
		t.Fatalf("cross tenant read: %v", err)
	}
}

func TestRepositoryKeyUniquenessIsTenantScoped(t *testing.T) {
	ctx := context.Background()
	r := New()
	base := agentapp.AgentApp{AgentAppKey: "assistant", DisplayName: "Assistant"}
	for _, app := range []agentapp.AgentApp{
		{TenantID: "tenant-a", AgentAppID: "app-1", AgentAppKey: base.AgentAppKey, DisplayName: base.DisplayName},
		{TenantID: "tenant-b", AgentAppID: "app-1", AgentAppKey: base.AgentAppKey, DisplayName: base.DisplayName},
	} {
		if _, err := r.Create(ctx, agentapp.CreateInput{App: app, ChangeMetadata: appMeta()}); err != nil {
			t.Fatal(err)
		}
	}
	_, err := r.Create(ctx, agentapp.CreateInput{App: agentapp.AgentApp{TenantID: "tenant-a", AgentAppID: "app-2", AgentAppKey: base.AgentAppKey, DisplayName: base.DisplayName}, ChangeMetadata: appMeta()})
	if !errors.Is(err, agentapp.ErrVersionConflict) {
		t.Fatalf("same-tenant duplicate key: %v", err)
	}
}
