package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

// Fixtures explicitly authorize their storage scope, just like trusted jobs.
func storageTestContext(app ...string) context.Context {
	scope := runtimecontext.TutorialScope().StorageScope
	if len(app) > 0 {
		scope = app[0]
	}
	return runtimecontext.WithStorageScope(context.Background(), scope)
}

func TestStorageRoutersRejectForeignAndMissingContext(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.BackendBindings = append(data.BackendBindings, controlplane.BackendBinding{ID: "knowledge", TenantID: "tutorial-tenant", AppID: "tutorial-app", ResourceType: "knowledge", BackendType: "inmemory", Config: json.RawMessage(`{"dimensions":16}`), MigrationState: "active", Version: 1})
	revision := data.Revisions[0]
	revision.KnowledgeConfig = json.RawMessage(`{"enabled":true,"embedding":{"provider":"hash","dimensions":16}}`)
	repo := controlplane.NewMemoryRepository(data)
	defer func() { _ = repo.Close() }()
	sessions, _ := NewSessionRouter(repo, secret.StaticStore{}, inmemory.NewSessionService(), nil)
	defer func() { _ = sessions.Close() }()
	memories, _ := NewMemoryRouter(repo, secret.StaticStore{})
	defer func() { _ = memories.Close() }()
	artifacts, _ := NewArtifactRouter(repo, secret.StaticStore{})
	defer func() { _ = artifacts.Close() }()
	knowledges, _ := NewKnowledgeRouter(repo, secret.StaticStore{})
	defer func() { _ = knowledges.Close() }()
	scope := runtimecontext.TutorialScope()
	ctx := storageTestContext()
	key := session.Key{AppName: scope.StorageScope, UserID: "user", SessionID: "session"}
	sess, err := sessions.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	user := memory.UserKey{AppName: key.AppName, UserID: key.UserID}
	if err := memories.AddMemory(ctx, user, "private memory", nil); err != nil {
		t.Fatal(err)
	}
	info := artifact.SessionInfo{AppName: key.AppName, UserID: key.UserID, SessionID: key.SessionID}
	if _, err := artifacts.SaveArtifact(ctx, info, "file", &artifact.Artifact{Data: []byte("private")}); err != nil {
		t.Fatal(err)
	}
	kb, _, err := knowledges.KnowledgeForRevision(ctx, scope, revision)
	if err != nil {
		t.Fatal(err)
	}
	foreign := "t/foreign/a/app"
	inv := &agentcore.Invocation{RunOptions: agentcore.RunOptions{AppName: foreign}, Session: session.NewSession(foreign, "user", "session")}
	for name, untrusted := range map[string]context.Context{
		"missing": context.Background(), "foreign": storageTestContext(foreign),
		"invocation":          agentcore.NewInvocationContext(context.Background(), inv),
		"invocation_conflict": agentcore.NewInvocationContext(ctx, inv),
	} {
		t.Run(name, func(t *testing.T) {
			checks := map[string]func() error{
				"session read":     func() error { _, e := sessions.GetSession(untrusted, key); return e },
				"session write":    func() error { return sessions.AppendEvent(untrusted, sess, &event.Event{ID: "forbidden"}) },
				"memory read":      func() error { _, e := memories.ReadMemories(untrusted, user, 10); return e },
				"memory write":     func() error { return memories.AddMemory(untrusted, user, "forbidden", nil) },
				"artifact read":    func() error { _, e := artifacts.LoadArtifact(untrusted, info, "file", nil); return e },
				"artifact write":   func() error { return artifacts.DeleteArtifact(untrusted, info, "file") },
				"knowledge search": func() error { _, e := kb.Search(untrusted, &knowledge.SearchRequest{Query: "private"}); return e },
				"knowledge write": func() error {
					_, e := knowledges.UpsertDocument(untrusted, scope, revision, KnowledgeDocument{ID: "forbidden", Content: "private"})
					return e
				},
			}
			for op, check := range checks {
				t.Run(op, func(t *testing.T) {
					if e := check(); !errors.Is(e, runtimecontext.ErrStorageScope) {
						t.Fatalf("err=%v", e)
					}
				})
			}
		})
	}
}
