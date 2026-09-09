package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	frameworkmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	frameworkrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestRunnerRegistryUsesExactVersionAndDrainsOldRunner(t *testing.T) {
	catalog := registryCatalog()
	repository, err := tenant.NewPresetRepository(catalog)
	if err != nil {
		t.Fatal(err)
	}
	credentials := config.NewStaticCredentialResolver(map[string]string{"env:MODEL_KEY": "model-key"})
	backends, err := storage.NewBackendProvider(repository, credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer backends.Close()
	created := make(map[CacheKey]*fakeRunner)
	var createdMu sync.Mutex
	registry, err := NewRunnerRegistry(
		repository, backends, credentials, DefaultCacheConfig(),
		func(_ context.Context, key CacheKey, version tenant.ConfigVersion, apiKey string, _ session.Service, _ frameworkmemory.Service) (frameworkrunner.Runner, error) {
			if version.Version != key.ConfigVersion || apiKey != "model-key" {
				t.Fatalf("factory received version=%q key=%#v apiKey=%q", version.Version, key, apiKey)
			}
			runner := &fakeRunner{}
			createdMu.Lock()
			created[key] = runner
			createdMu.Unlock()
			return runner, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()

	keyV1 := CacheKey{TenantID: "tenant-a", AgentAppID: "assistant", ConfigVersion: "v1"}
	keyV2 := CacheKey{TenantID: "tenant-a", AgentAppID: "assistant", ConfigVersion: "v2"}
	leaseV1, err := registry.Acquire(context.Background(), keyV1)
	if err != nil {
		t.Fatal(err)
	}
	leaseV2, err := registry.Acquire(context.Background(), keyV2)
	if err != nil {
		t.Fatal(err)
	}
	if leaseV1.Runner == leaseV2.Runner {
		t.Fatal("different config versions shared a Runner")
	}
	leaseV2.Release()

	drainDone := make(chan error, 1)
	go func() { drainDone <- registry.Drain(context.Background(), keyV1) }()
	select {
	case err := <-drainDone:
		t.Fatalf("Drain returned while v1 lease was active: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	leaseV1.Release()
	if err := <-drainDone; err != nil {
		t.Fatal(err)
	}
	createdMu.Lock()
	v1Runner := created[keyV1]
	createdMu.Unlock()
	if got := v1Runner.closeCount.Load(); got != 1 {
		t.Fatalf("v1 runner closed %d times, want 1", got)
	}
}

func TestRunnerRegistryFailsClosedForMissingCredential(t *testing.T) {
	repository, err := tenant.NewPresetRepository(registryCatalog())
	if err != nil {
		t.Fatal(err)
	}
	credentials := config.NewStaticCredentialResolver(nil)
	backends, err := storage.NewBackendProvider(repository, credentials)
	if err != nil {
		t.Fatal(err)
	}
	defer backends.Close()
	registry, err := NewRunnerRegistry(
		repository, backends, credentials, DefaultCacheConfig(),
		func(context.Context, CacheKey, tenant.ConfigVersion, string, session.Service, frameworkmemory.Service) (frameworkrunner.Runner, error) {
			return &fakeRunner{}, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	_, err = registry.Acquire(context.Background(), CacheKey{TenantID: "tenant-a", AgentAppID: "assistant", ConfigVersion: "v1"})
	if !errors.Is(err, ErrRegistryConfiguration) {
		t.Fatalf("Acquire() error = %v, want ErrRegistryConfiguration", err)
	}
}

func TestRunnerRegistryRefreshesRunnerWhenBackendGenerationChanges(t *testing.T) {
	var created []*fakeRunner
	cache, err := NewRunnerCache(DefaultCacheConfig(), func(context.Context, CacheKey) (frameworkrunner.Runner, error) {
		runner := &fakeRunner{}
		created = append(created, runner)
		return runner, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := &RunnerRegistry{cache: cache, generations: make(map[CacheKey]uint64)}
	defer registry.Close()
	key := CacheKey{TenantID: "tenant-a", AgentAppID: "assistant", ConfigVersion: "v1"}

	first, err := registry.acquireGeneration(context.Background(), key, 1)
	if err != nil {
		t.Fatal(err)
	}
	firstRunner := first.Runner
	first.Release()
	second, err := registry.acquireGeneration(context.Background(), key, 1)
	if err != nil {
		t.Fatal(err)
	}
	if second.Runner != firstRunner || len(created) != 1 {
		t.Fatalf("same generation created a new runner: runners=%d", len(created))
	}
	second.Release()

	third, err := registry.acquireGeneration(context.Background(), key, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Release()
	if third.Runner == firstRunner || len(created) != 2 {
		t.Fatalf("new generation did not replace runner: runners=%d", len(created))
	}
	if got := created[0].closeCount.Load(); got != 1 {
		t.Fatalf("stale runner close count=%d, want 1", got)
	}
}

func registryCatalog() tenant.Catalog {
	model := tenant.ModelConfig{
		Name: "model", BaseURL: "https://example.test", CredentialRef: "env:MODEL_KEY",
		RequestTimeout: time.Second, MaxOutputTokens: 128,
	}
	return tenant.Catalog{
		Tenants:         []tenant.Tenant{{ID: "tenant-a", Enabled: true}},
		StorageProfiles: []tenant.StorageProfile{{TenantID: "tenant-a", ID: "memory-v1", Kind: tenant.StorageKindInMemory}},
		AgentApps:       []tenant.AgentApp{{TenantID: "tenant-a", ID: "assistant", Enabled: true, ActiveConfigVersion: "v1"}},
		ConfigVersions: []tenant.ConfigVersion{
			{TenantID: "tenant-a", AgentAppID: "assistant", Version: "v1", StorageProfileID: "memory-v1", Instruction: "v1", Model: model},
			{TenantID: "tenant-a", AgentAppID: "assistant", Version: "v2", StorageProfileID: "memory-v1", Instruction: "v2", Model: model},
		},
		ChannelBindings: []tenant.ChannelBinding{{
			ID: "binding-a", Channel: "demo", ExternalAccountID: "binding-a",
			TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true,
		}},
	}
}
