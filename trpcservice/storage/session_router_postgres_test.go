package storage

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestSessionRouterPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	data := controlplane.BootstrapData{BackendBindings: []controlplane.BackendBinding{{
		ID: "postgres-session", TenantID: "tenant-pg", AppID: "app-pg",
		ResourceType: "session", BackendType: "postgres", MigrationState: "active", Version: 1,
		Config:    json.RawMessage(`{"table_prefix":"tenant_session_integration"}`),
		SecretRef: "secret://postgres",
	}}}
	repository := controlplane.NewMemoryRepository(data)
	router, err := NewSessionRouter(
		repository, secret.StaticStore{"secret://postgres": dsn},
		inmemory.NewSessionService(), &countingSummary{},
	)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	key := session.Key{AppName: "t/tenant-pg/a/app-pg", UserID: "alice", SessionID: "postgres"}
	_ = router.DeleteSession(storageTestContext("t/tenant-pg/a/app-pg"), key)
	if _, err := router.CreateSession(storageTestContext("t/tenant-pg/a/app-pg"), key, session.StateMap{
		"backend": []byte("postgres"),
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	stored, err := router.GetSession(storageTestContext("t/tenant-pg/a/app-pg"), key)
	if err != nil || string(stored.State["backend"]) != "postgres" {
		t.Fatalf("stored=%+v err=%v", stored, err)
	}
	ctx := storageTestContext(key.AppName)
	if err := router.AppendEvent(ctx, stored, &event.Event{ID: "summary-event", Timestamp: time.Now(), Response: &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage("persist this")}}}}); err != nil {
		t.Fatal(err)
	}
	if err := router.CreateSessionSummary(ctx, stored, "", true); err != nil {
		t.Fatal(err)
	}
	if err := router.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewSessionRouter(repository, secret.StaticStore{"secret://postgres": dsn}, inmemory.NewSessionService(), &countingSummary{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	persisted, err := reopened.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if text, ok := reopened.GetSessionSummaryText(ctx, persisted); !ok || text != "summary-call-1" || !summaryCoversEvents(persisted, "") {
		t.Fatal("summary and exact event boundary did not survive reopening")
	}
}
