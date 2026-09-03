package storage

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"trpc.group/trpc-go/trpc-agent-go/memory"
)

func TestMemoryRouterUsesTenantScopedAppName(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	repository := controlplane.NewMemoryRepository(data)
	router, err := NewMemoryRouter(repository, secret.StaticStore{})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	key := memory.UserKey{AppName: "t/tutorial-tenant/a/tutorial-app", UserID: "alice"}
	if err := router.AddMemory(context.Background(), key, "likes tea", []string{"preference"}); err != nil {
		t.Fatalf("add memory: %v", err)
	}
	entries, err := router.ReadMemories(context.Background(), key, 10)
	if err != nil || len(entries) != 1 || entries[0].Memory.Memory != "likes tea" {
		t.Fatalf("entries=%+v err=%v", entries, err)
	}
	if _, err := router.ReadMemories(context.Background(), memory.UserKey{
		AppName: "tutorial-app", UserID: "alice",
	}, 10); err == nil || !strings.Contains(err.Error(), "storage scope") {
		t.Fatalf("forged scope error=%v", err)
	}
}

func TestMemoryRouterRedisBackend(t *testing.T) {
	server := miniredis.RunT(t)
	data := controlplane.BootstrapData{
		BackendBindings: []controlplane.BackendBinding{{
			ID: "redis-memory", TenantID: "tenant-a", AppID: "app-a",
			ResourceType: "memory", BackendType: "redis",
			Config:    json.RawMessage(`{"key_prefix":"memory-router-test"}`),
			SecretRef: "secret://redis", MigrationState: "active", Version: 1,
		}},
	}
	repository := controlplane.NewMemoryRepository(data)
	router, err := NewMemoryRouter(repository, secret.StaticStore{
		"secret://redis": "redis://" + server.Addr() + "/0",
	})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	key := memory.UserKey{AppName: "t/tenant-a/a/app-a", UserID: "alice"}
	if err := router.AddMemory(context.Background(), key, "redis memory", nil); err != nil {
		t.Fatalf("add memory: %v", err)
	}
	entries, err := router.ReadMemories(context.Background(), key, 10)
	if err != nil || len(entries) != 1 || entries[0].Memory.Memory != "redis memory" {
		t.Fatalf("entries=%+v err=%v", entries, err)
	}
}

func TestResolveBackendBindingPrefersAppOverride(t *testing.T) {
	now := time.Now()
	data := controlplane.BootstrapData{
		Tenants: []controlplane.Tenant{{ID: "tenant-a"}},
		BackendBindings: []controlplane.BackendBinding{
			{ID: "default", TenantID: "tenant-a", ResourceType: "memory", BackendType: "inmemory", Config: json.RawMessage(`{}`), MigrationState: "active", Version: 1, CreatedAt: now},
			{ID: "app", TenantID: "tenant-a", AppID: "app-a", ResourceType: "memory", BackendType: "inmemory", Config: json.RawMessage(`{}`), MigrationState: "active", Version: 1, CreatedAt: now},
		},
	}
	repository := controlplane.NewMemoryRepository(data)
	binding, err := resolveBackendBinding(context.Background(), repository, "tenant-a", "app-a", "memory")
	if err != nil || binding.ID != "app" {
		t.Fatalf("binding=%+v err=%v", binding, err)
	}
}
