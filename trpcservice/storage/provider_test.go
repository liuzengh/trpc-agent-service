package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestBackendProviderSelectsCachesAndIsolatesProfiles(t *testing.T) {
	server := miniredis.RunT(t)
	repository, err := tenant.NewPresetRepository(providerCatalog())
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewBackendProvider(repository, config.NewStaticCredentialResolver(map[string]string{
		"env:MODEL_KEY": "model-key", "env:REDIS_URL": "redis://" + server.Addr() + "/0",
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()

	memoryProfile, _ := repository.GetStorageProfile(context.Background(), "tenant-memory", "memory-v1")
	memoryBackend, err := provider.BackendFor(context.Background(), memoryProfile)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := memoryBackend.(*InMemoryBackend); !ok {
		t.Fatalf("memory profile returned %T", memoryBackend)
	}
	again, err := provider.BackendFor(context.Background(), memoryProfile)
	if err != nil || again != memoryBackend {
		t.Fatalf("memory backend was not cached: (%T, %v)", again, err)
	}

	redisProfile, _ := repository.GetStorageProfile(context.Background(), "tenant-redis", "redis-v1")
	// Only the tenant/profile identity is accepted from callers. The immutable
	// repository remains authoritative for kind, credentials, and prefix.
	redisProfile.Kind = tenant.StorageKindInMemory
	redisProfile.CredentialRef = ""
	redisProfile.KeyPrefix = ""
	redisBackend, err := provider.BackendFor(context.Background(), redisProfile)
	if err != nil {
		t.Fatal(err)
	}
	concrete, ok := redisBackend.(*RedisBackend)
	if !ok {
		t.Fatalf("redis profile returned %T", redisBackend)
	}
	wantPrefix := "phase2:tenant:tenant-redis:profile:redis-v1:official-v1"
	if concrete.Prefix() != wantPrefix {
		t.Fatalf("Redis prefix = %q, want %q", concrete.Prefix(), wantPrefix)
	}
}

func TestBackendProviderStrictReadinessFailureAndRecovery(t *testing.T) {
	server := miniredis.RunT(t)
	repository, err := tenant.NewPresetRepository(providerCatalog())
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewBackendProvider(repository, config.NewStaticCredentialResolver(map[string]string{
		"env:MODEL_KEY": "model-key", "env:REDIS_URL": "redis://" + server.Addr() + "/0",
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	if err := provider.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}

	server.Close()
	if err := provider.Ready(context.Background()); err == nil {
		t.Fatal("strict readiness succeeded while Redis was unavailable")
	}
	memoryProfile, _ := repository.GetStorageProfile(context.Background(), "tenant-memory", "memory-v1")
	if backend, err := provider.BackendFor(context.Background(), memoryProfile); err != nil {
		t.Fatalf("healthy tenant backend failed: %v", err)
	} else if _, ok := backend.(*InMemoryBackend); !ok {
		t.Fatalf("failed Redis profile fell back to %T", backend)
	}
	if err := server.Restart(); err != nil {
		t.Fatal(err)
	}
	if err := provider.Ready(context.Background()); err != nil {
		t.Fatalf("readiness did not recover: %v", err)
	}
}

func TestBackendProviderPreservesLegacyPrefixAndCloses(t *testing.T) {
	server := miniredis.RunT(t)
	catalog := providerCatalog()
	catalog.StorageProfiles = catalog.StorageProfiles[:1]
	catalog.StorageProfiles[0].KeyPrefix = "legacy-prefix"
	catalog.StorageProfiles[0].LegacyPrefix = true
	catalog.AgentApps = catalog.AgentApps[:1]
	catalog.ConfigVersions = catalog.ConfigVersions[:1]
	catalog.ChannelBindings = catalog.ChannelBindings[:1]
	catalog.Tenants = catalog.Tenants[:1]
	repository, err := tenant.NewPresetRepository(catalog)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewBackendProvider(repository, config.NewStaticCredentialResolver(map[string]string{
		"env:MODEL_KEY": "model-key", "env:REDIS_URL": "redis://" + server.Addr() + "/0",
	}))
	if err != nil {
		t.Fatal(err)
	}
	profile, _ := repository.GetStorageProfile(context.Background(), "tenant-redis", "redis-v1")
	backend, err := provider.BackendFor(context.Background(), profile)
	if err != nil {
		t.Fatal(err)
	}
	if got := backend.(*RedisBackend).Prefix(); got != "legacy-prefix:official-v1" {
		t.Fatalf("legacy prefix = %q", got)
	}
	if err := provider.Close(); err != nil {
		t.Fatal(err)
	}
	if err := provider.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.BackendFor(context.Background(), profile); !errors.Is(err, ErrProviderClosed) {
		t.Fatalf("BackendFor after Close error = %v", err)
	}
}

func TestStrongBackendProviderRejectsInMemoryAndCrossRedis(t *testing.T) {
	first := miniredis.RunT(t)
	second := miniredis.RunT(t)
	repository, err := tenant.NewPresetRepository(providerCatalog())
	if err != nil {
		t.Fatal(err)
	}
	credentials := config.NewStaticCredentialResolver(map[string]string{
		"env:MODEL_KEY": "model-key", "env:REDIS_URL": "redis://" + first.Addr() + "/0",
	})
	provider, err := NewBackendProvider(repository, credentials, "strong", "phase4", "redis://"+second.Addr()+"/0")
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	memoryProfile, _ := repository.GetStorageProfile(context.Background(), "tenant-memory", "memory-v1")
	if _, err := provider.BackendFor(context.Background(), memoryProfile); err == nil || !strings.Contains(err.Error(), "rejects inmemory") {
		t.Fatalf("strong InMemory error=%v", err)
	}
	redisProfile, _ := repository.GetStorageProfile(context.Background(), "tenant-redis", "redis-v1")
	if _, err := provider.BackendFor(context.Background(), redisProfile); err == nil || !strings.Contains(err.Error(), "same URL and DB") {
		t.Fatalf("cross Redis error=%v", err)
	}
}

func TestBackendProviderReadinessIsolatesUnavailableSQLProfile(t *testing.T) {
	catalog := providerCatalog()
	catalog.Tenants[1].ID = "tenant-mysql"
	catalog.StorageProfiles[1] = tenant.StorageProfile{TenantID: "tenant-mysql", ID: "mysql-v1", Kind: tenant.StorageKindMySQL, CredentialRef: "env:MYSQL_DSN", TablePrefix: "tenant_mysql"}
	catalog.AgentApps[1].TenantID = "tenant-mysql"
	catalog.ConfigVersions[1].TenantID = "tenant-mysql"
	catalog.ConfigVersions[1].StorageProfileID = "mysql-v1"
	catalog.ChannelBindings[1].TenantID = "tenant-mysql"
	catalog.ChannelBindings[1].ID = "mysql-binding"
	catalog.ChannelBindings[1].ExternalAccountID = "mysql-binding"
	catalog.Tenants = catalog.Tenants[1:]
	catalog.StorageProfiles = catalog.StorageProfiles[1:]
	catalog.AgentApps = catalog.AgentApps[1:]
	catalog.ConfigVersions = catalog.ConfigVersions[1:]
	catalog.ChannelBindings = catalog.ChannelBindings[1:]
	repository, err := tenant.NewPresetRepository(catalog)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewBackendProvider(repository, config.NewStaticCredentialResolver(map[string]string{
		"env:MODEL_KEY": "model-key",
		"env:MYSQL_DSN": "user:password@tcp(127.0.0.1:1)/missing?parseTime=true&charset=utf8mb4&loc=UTC",
	}), "strong", "phase55", "redis://127.0.0.1:1/0")
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	if err := provider.Ready(context.Background()); err != nil {
		t.Fatalf("global readiness was lowered by SQL profile: %v", err)
	}
	mysqlProfile, _ := repository.GetStorageProfile(context.Background(), "tenant-mysql", "mysql-v1")
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if _, err := provider.BackendFor(ctx, mysqlProfile); err == nil {
		t.Fatal("unavailable SQL profile unexpectedly initialized")
	}
	if err := provider.Ready(context.Background()); err != nil {
		t.Fatalf("global readiness stayed degraded after SQL profile error: %v", err)
	}
}

func providerCatalog() tenant.Catalog {
	model := tenant.ModelConfig{
		Name: "model", BaseURL: "https://example.test", CredentialRef: "env:MODEL_KEY",
		RequestTimeout: time.Second, MaxOutputTokens: 128,
	}
	return tenant.Catalog{
		Tenants: []tenant.Tenant{{ID: "tenant-redis", Enabled: true}, {ID: "tenant-memory", Enabled: true}},
		StorageProfiles: []tenant.StorageProfile{
			{TenantID: "tenant-redis", ID: "redis-v1", Kind: tenant.StorageKindRedis, CredentialRef: "env:REDIS_URL", KeyPrefix: "phase2"},
			{TenantID: "tenant-memory", ID: "memory-v1", Kind: tenant.StorageKindInMemory},
		},
		AgentApps: []tenant.AgentApp{
			{TenantID: "tenant-redis", ID: "assistant", Enabled: true, ActiveConfigVersion: "v1"},
			{TenantID: "tenant-memory", ID: "assistant", Enabled: true, ActiveConfigVersion: "v1"},
		},
		ConfigVersions: []tenant.ConfigVersion{
			{TenantID: "tenant-redis", AgentAppID: "assistant", Version: "v1", StorageProfileID: "redis-v1", Instruction: "redis", Model: model},
			{TenantID: "tenant-memory", AgentAppID: "assistant", Version: "v1", StorageProfileID: "memory-v1", Instruction: "memory", Model: model},
		},
		ChannelBindings: []tenant.ChannelBinding{
			{ID: "redis-binding", Channel: "demo", ExternalAccountID: "redis-binding", TenantID: "tenant-redis", AgentAppID: "assistant", Enabled: true},
			{ID: "memory-binding", Channel: "demo", ExternalAccountID: "memory-binding", TenantID: "tenant-memory", AgentAppID: "assistant", Enabled: true},
		},
	}
}
