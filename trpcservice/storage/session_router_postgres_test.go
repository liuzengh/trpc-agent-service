package storage

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
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
		inmemory.NewSessionService(), nil,
	)
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	key := session.Key{AppName: "t/tenant-pg/a/app-pg", UserID: "alice", SessionID: "postgres"}
	_ = router.DeleteSession(context.Background(), key)
	if _, err := router.CreateSession(context.Background(), key, session.StateMap{
		"backend": []byte("postgres"),
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	stored, err := router.GetSession(context.Background(), key)
	if err != nil || string(stored.State["backend"]) != "postgres" {
		t.Fatalf("stored=%+v err=%v", stored, err)
	}
}
