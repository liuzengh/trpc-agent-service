package storage

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"trpc.group/trpc-go/trpc-agent-go/memory"
)

func TestMemoryRouterPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	data := controlplane.BootstrapData{
		BackendBindings: []controlplane.BackendBinding{{
			ID: "postgres-memory", TenantID: "tenant-pg", AppID: "app-pg",
			ResourceType: "memory", BackendType: "postgres",
			Config:    json.RawMessage(`{"table_name":"memory_router_integration"}`),
			SecretRef: "secret://postgres", MigrationState: "active", Version: 1,
		}},
	}
	repository := controlplane.NewMemoryRepository(data)
	router, err := NewMemoryRouter(repository, secret.StaticStore{"secret://postgres": dsn})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	key := memory.UserKey{AppName: "t/tenant-pg/a/app-pg", UserID: "integration-user"}
	if err := router.ClearMemories(context.Background(), key); err != nil {
		t.Fatalf("clear old memories: %v", err)
	}
	if err := router.AddMemory(context.Background(), key, "postgres memory", []string{"test"}); err != nil {
		t.Fatalf("add memory: %v", err)
	}
	entries, err := router.ReadMemories(context.Background(), key, 10)
	if err != nil || len(entries) != 1 || entries[0].Memory.Memory != "postgres memory" {
		t.Fatalf("entries=%+v err=%v", entries, err)
	}
}
