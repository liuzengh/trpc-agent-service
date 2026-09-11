package storage

import (
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
	if err := router.AddMemory(storageTestContext(), key, "likes tea", []string{"preference"}); err != nil {
		t.Fatalf("add memory: %v", err)
	}
	entries, err := router.ReadMemories(storageTestContext(), key, 10)
	if err != nil || len(entries) != 1 || entries[0].Memory.Memory != "likes tea" {
		t.Fatalf("entries=%+v err=%v", entries, err)
	}
	if _, err := router.ReadMemories(storageTestContext(), memory.UserKey{
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
	if err := router.AddMemory(storageTestContext("t/tenant-a/a/app-a"), key, "redis memory", nil); err != nil {
		t.Fatalf("add memory: %v", err)
	}
	entries, err := router.ReadMemories(storageTestContext("t/tenant-a/a/app-a"), key, 10)
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
	binding, err := resolveBackendBinding(storageTestContext(), repository, "tenant-a", "app-a", "memory")
	if err != nil || binding.ID != "app" {
		t.Fatalf("binding=%+v err=%v", binding, err)
	}
}

func TestMemoryRouterMigrationDualWriteAndCutover(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	repository := controlplane.NewMemoryRepository(data)
	now := time.Now().UTC()
	target := controlplane.BackendBinding{
		ID: "tutorial-memory-target", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "memory", BackendType: "inmemory", Config: json.RawMessage(`{}`),
		MigrationState: "migration_target", Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateBackendBinding(storageTestContext(), target); err != nil {
		t.Fatalf("create target: %v", err)
	}
	migration := controlplane.BackendMigration{
		ID: "memory-migration", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "memory", SourceBindingID: "tutorial-memory-backend",
		TargetBindingID: target.ID, State: controlplane.MigrationDualWrite,
		Checkpoint: json.RawMessage(`{}`), Verification: json.RawMessage(`{}`),
		Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := repository.CreateBackendMigration(storageTestContext(), migration); err != nil {
		t.Fatalf("create migration: %v", err)
	}
	router, _ := NewMemoryRouter(repository, secret.StaticStore{})
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	key := memory.UserKey{AppName: "t/tutorial-tenant/a/tutorial-app", UserID: "alice"}
	if err := router.AddMemory(storageTestContext(), key, "written to both", nil); err != nil {
		t.Fatalf("dual write: %v", err)
	}
	targetService, err := router.cachedService(storageTestContext(), target)
	if err != nil {
		t.Fatalf("target service: %v", err)
	}
	targetEntries, err := targetService.ReadMemories(storageTestContext(), key, 10)
	if err != nil || len(targetEntries) != 1 {
		t.Fatalf("target entries=%+v err=%v", targetEntries, err)
	}
	if verified, err := router.VerifyUser(storageTestContext(), "tutorial-tenant", migration.ID, "alice"); err != nil || !verified.Passed {
		t.Fatalf("verify=%+v err=%v", verified, err)
	}
	if _, err := repository.TransitionBackendMigration(
		storageTestContext(), "tutorial-tenant", migration.ID,
		controlplane.MigrationCutover, 1, nil, nil,
	); err != nil {
		t.Fatalf("cutover: %v", err)
	}
	if err := router.AddMemory(storageTestContext(), key, "target primary", nil); err != nil {
		t.Fatalf("cutover write: %v", err)
	}
	entries, err := router.ReadMemories(storageTestContext(), key, 10)
	if err != nil || len(entries) != 2 {
		t.Fatalf("cutover entries=%+v err=%v", entries, err)
	}
}

func TestMemoryRouterRecordsRepairBacklogOnSecondaryFailure(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	repository := controlplane.NewMemoryRepository(data)
	now := time.Now().UTC()
	target := controlplane.BackendBinding{
		ID: "limited-target", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "memory", BackendType: "inmemory",
		Config: json.RawMessage(`{"memory_limit":1}`), MigrationState: "migration_target", Version: 1,
	}
	_ = repository.CreateBackendBinding(storageTestContext(), target)
	migration := controlplane.BackendMigration{
		ID: "repair-migration", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "memory", SourceBindingID: "tutorial-memory-backend",
		TargetBindingID: target.ID, State: controlplane.MigrationDualWrite,
		Checkpoint: json.RawMessage(`{}`), Verification: json.RawMessage(`{}`),
		Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	_ = repository.CreateBackendMigration(storageTestContext(), migration)
	router, _ := NewMemoryRouter(repository, secret.StaticStore{})
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	key := memory.UserKey{AppName: "t/tutorial-tenant/a/tutorial-app", UserID: "alice"}
	if err := router.AddMemory(storageTestContext(), key, "first", nil); err != nil {
		t.Fatalf("first add: %v", err)
	}
	if err := router.AddMemory(storageTestContext(), key, "second", nil); err == nil {
		t.Fatal("expected secondary limit error")
	}
	stored, err := repository.GetBackendMigration(
		storageTestContext(), "tutorial-tenant", migration.ID,
	)
	if err != nil || stored.RepairBacklog != 1 {
		t.Fatalf("migration=%+v err=%v", stored, err)
	}
}

func TestMemoryRouterBackfillsAndVerifiesUser(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	repository := controlplane.NewMemoryRepository(data)
	now := time.Now().UTC()
	target := controlplane.BackendBinding{
		ID: "backfill-target", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "memory", BackendType: "inmemory", Config: json.RawMessage(`{}`),
		MigrationState: "migration_target", Version: 1,
	}
	_ = repository.CreateBackendBinding(storageTestContext(), target)
	migration := controlplane.BackendMigration{
		ID: "backfill-migration", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "memory", SourceBindingID: "tutorial-memory-backend",
		TargetBindingID: target.ID, State: controlplane.MigrationBackfill,
		Checkpoint: json.RawMessage(`{}`), Verification: json.RawMessage(`{}`),
		Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	_ = repository.CreateBackendMigration(storageTestContext(), migration)
	router, _ := NewMemoryRouter(repository, secret.StaticStore{})
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	sourceBinding, _ := repository.GetBackendBinding(
		storageTestContext(), "tutorial-tenant", "tutorial-memory-backend",
	)
	source, _ := router.cachedService(storageTestContext(), sourceBinding)
	key := memory.UserKey{AppName: "t/tutorial-tenant/a/tutorial-app", UserID: "alice"}
	if err := source.AddMemory(storageTestContext(), key, "source-only", nil); err != nil {
		t.Fatalf("source add: %v", err)
	}
	verification, err := router.BackfillUser(
		storageTestContext(), "tutorial-tenant", migration.ID, "alice",
	)
	if err != nil || !verification.Passed || verification.SourceCount != 1 ||
		verification.TargetCount != 1 {
		t.Fatalf("verification=%+v err=%v", verification, err)
	}
}
