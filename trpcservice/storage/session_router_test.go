package storage

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestSessionRouterUsesStartupBinding(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	startup := inmemory.NewSessionService()
	router, err := NewSessionRouter(repository, secret.StaticStore{}, startup, nil)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	key := session.Key{
		AppName: "t/tutorial-tenant/a/tutorial-app", UserID: "alice", SessionID: "session",
	}
	if _, err := router.CreateSession(storageTestContext(), key, session.StateMap{"name": []byte("Alice")}); err != nil {
		t.Fatalf("create: %v", err)
	}
	stored, err := router.GetSession(storageTestContext(), key)
	if err != nil || string(stored.State["name"]) != "Alice" {
		t.Fatalf("stored=%+v err=%v", stored, err)
	}
	if err := router.Ready(storageTestContext()); err != nil {
		t.Fatalf("ready: %v", err)
	}
}

func TestSessionRouterRedisBinding(t *testing.T) {
	server := miniredis.RunT(t)
	data := controlplane.BootstrapData{BackendBindings: []controlplane.BackendBinding{{
		ID: "redis-session", TenantID: "tenant-a", AppID: "app-a",
		ResourceType: "session", BackendType: "redis", MigrationState: "active", Version: 1,
		Config: json.RawMessage(`{"key_prefix":"router-test"}`), SecretRef: "secret://redis",
	}}}
	repository := controlplane.NewMemoryRepository(data)
	router, err := NewSessionRouter(
		repository,
		secret.StaticStore{"secret://redis": "redis://" + server.Addr() + "/0"},
		inmemory.NewSessionService(), nil,
	)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	key := session.Key{AppName: "t/tenant-a/a/app-a", UserID: "alice", SessionID: "redis"}
	if _, err := router.CreateSession(storageTestContext("t/tenant-a/a/app-a"), key, nil); err != nil {
		t.Fatalf("create Redis session: %v", err)
	}
	if _, err := router.GetSession(storageTestContext("t/tenant-a/a/app-a"), key); err != nil {
		t.Fatalf("get Redis session: %v", err)
	}
}

func TestSessionRouterMigrationDualWritesEvents(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	repository := controlplane.NewMemoryRepository(data)
	now := time.Now().UTC()
	target := controlplane.BackendBinding{
		ID: "session-target", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "session", BackendType: "inmemory", Config: json.RawMessage(`{}`),
		MigrationState: "migration_target", Version: 1,
	}
	_ = repository.CreateBackendBinding(storageTestContext(), target)
	_ = repository.CreateBackendMigration(storageTestContext(), controlplane.BackendMigration{
		ID: "session-migration", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "session", SourceBindingID: "tutorial-session-backend",
		TargetBindingID: target.ID, State: controlplane.MigrationDualWrite,
		Checkpoint: json.RawMessage(`{}`), Verification: json.RawMessage(`{}`),
		Version: 1, CreatedAt: now, UpdatedAt: now,
	})
	router, _ := NewSessionRouter(
		repository, secret.StaticStore{}, inmemory.NewSessionService(), nil,
	)
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	key := session.Key{
		AppName: "t/tutorial-tenant/a/tutorial-app", UserID: "alice", SessionID: "dual",
	}
	sess, err := router.CreateSession(storageTestContext(), key, nil)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	item := &event.Event{
		ID: "event-1", Timestamp: time.Now(),
		Response: &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage("hello")}}},
	}
	if err := router.AppendEvent(storageTestContext(), sess, item); err != nil {
		t.Fatalf("append: %v", err)
	}
	targetService, err := router.cachedService(storageTestContext(), target)
	if err != nil {
		t.Fatalf("target: %v", err)
	}
	targetSession, err := targetService.GetSession(storageTestContext(), key)
	if err != nil || len(targetSession.Events) != 1 ||
		targetSession.Events[0].Choices[0].Message.Content != "hello" {
		t.Fatalf("target session=%+v err=%v", targetSession, err)
	}
}

func TestSessionRouterBackfillsExistingSession(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	repository := controlplane.NewMemoryRepository(data)
	now := time.Now().UTC()
	target := controlplane.BackendBinding{
		ID: "session-backfill-target", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "session", BackendType: "inmemory", Config: json.RawMessage(`{}`),
		MigrationState: "migration_target", Version: 1,
	}
	_ = repository.CreateBackendBinding(storageTestContext(), target)
	migration := controlplane.BackendMigration{
		ID: "session-backfill", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "session", SourceBindingID: "tutorial-session-backend",
		TargetBindingID: target.ID, State: controlplane.MigrationBackfill,
		Checkpoint: json.RawMessage(`{}`), Verification: json.RawMessage(`{}`),
		Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	_ = repository.CreateBackendMigration(storageTestContext(), migration)
	startup := inmemory.NewSessionService()
	router, _ := NewSessionRouter(repository, secret.StaticStore{}, startup, nil)
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	key := session.Key{
		AppName: "t/tutorial-tenant/a/tutorial-app", UserID: "alice", SessionID: "existing",
	}
	sourceSession, _ := startup.CreateSession(storageTestContext(), key, session.StateMap{
		"name": []byte("Alice"),
	})
	for _, content := range []string{"first", "second"} {
		_ = startup.AppendEvent(storageTestContext(), sourceSession, &event.Event{
			Timestamp: time.Now(),
			Response:  &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage(content)}}},
		})
	}
	verification, err := router.BackfillSession(
		storageTestContext(), "tutorial-tenant", migration.ID,
		SessionMigrationItem{UserID: "alice", SessionID: "existing"},
	)
	if err != nil || !verification.Passed || verification.SourceEvents != 2 ||
		verification.TargetEvents != 2 || !verification.StateMatched {
		t.Fatalf("verification=%+v err=%v", verification, err)
	}
}
