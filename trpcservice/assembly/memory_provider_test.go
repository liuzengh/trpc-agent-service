package assembly

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/internal/testutil"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	"trpc.group/trpc-go/trpc-agent-go/memory"
)

func TestManagedMemoryProviderInMemoryReaderSharesRuntimeService(t *testing.T) {
	resolver, err := credential.NewEnvironmentSecretResolver(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	profiles := testBackendProfileResolver{"memory-test": {Driver: MemoryDriverInMemory}}
	provider, err := NewManagedMemoryProvider("postgres://unused", resolver, profiles, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	configuration := config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Storage: config.StoragePolicy{Memory: config.BackendProfileRef{ProfileID: "memory-test"}},
	}
	reader, err := provider.MemoryReader(context.Background(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	userKey := memory.UserKey{AppName: configuration.AppName(), UserID: "user-1"}
	before, err := reader.ReadMemories(context.Background(), userKey, 10)
	if err != nil || len(before) != 0 {
		t.Fatalf("reader before runtime = %+v, %v", before, err)
	}
	backend, err := provider.MemoryBackend(context.Background(), configuration, testutil.NewFakeModel("memory"))
	if err != nil {
		t.Fatal(err)
	}
	if err := backend.Service.AddMemory(context.Background(), userKey, "prefers concise replies", []string{"preference"}); err != nil {
		t.Fatal(err)
	}
	after, err := reader.ReadMemories(context.Background(), userKey, 10)
	if err != nil || len(after) != 1 || after[0].Memory == nil || after[0].Memory.Memory != "prefers concise replies" {
		t.Fatalf("reader after runtime = %+v, %v", after, err)
	}
}

func TestManagedMemoryProviderReferenceCountsRuntimeBackends(t *testing.T) {
	resolver, err := credential.NewEnvironmentSecretResolver(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	profiles := testBackendProfileResolver{"memory-test": {Driver: MemoryDriverInMemory}}
	provider, err := NewManagedMemoryProvider("postgres://unused", resolver, profiles, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	configuration := config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Storage: config.StoragePolicy{Memory: config.BackendProfileRef{ProfileID: "memory-test"}},
	}
	first, err := provider.MemoryBackend(context.Background(), configuration, testutil.NewFakeModel("memory-v1"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := provider.MemoryBackend(context.Background(), configuration, testutil.NewFakeModel("memory-v1"))
	if err != nil {
		t.Fatal(err)
	}
	if first.Service != second.Service {
		t.Fatal("same immutable configuration did not share its Memory service")
	}

	_, _, physicalKey, err := provider.resolve(context.Background(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	key := memoryRuntimeKey(physicalKey, configuration.ConfigVersion)
	provider.mu.Lock()
	refs := provider.runtime[key].refs
	provider.mu.Unlock()
	if refs != 2 {
		t.Fatalf("Memory backend refs = %d, want 2", refs)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	refs = provider.runtime[key].refs
	provider.mu.Unlock()
	if refs != 1 {
		t.Fatalf("Memory backend refs after first release = %d, want 1", refs)
	}
	if err := second.Service.AddMemory(context.Background(), memory.UserKey{AppName: configuration.AppName(), UserID: "user-1"}, "still open", nil); err != nil {
		t.Fatalf("shared Memory service was closed while still referenced: %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
	provider.mu.Lock()
	_, exists := provider.runtime[key]
	provider.mu.Unlock()
	if exists {
		t.Fatal("Memory backend remained cached after its final release")
	}
}

func TestManagedMemoryProviderSeparatesExtractorServiceByConfigVersion(t *testing.T) {
	resolver, err := credential.NewEnvironmentSecretResolver(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	profiles := testBackendProfileResolver{"memory-test": {Driver: MemoryDriverInMemory}}
	provider, err := NewManagedMemoryProvider("postgres://unused", resolver, profiles, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	configuration := config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Storage: config.StoragePolicy{Memory: config.BackendProfileRef{ProfileID: "memory-test"}},
	}
	first, err := provider.MemoryBackend(context.Background(), configuration, testutil.NewFakeModel("memory-v1"))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	configuration.ConfigVersion = 2
	second, err := provider.MemoryBackend(context.Background(), configuration, testutil.NewFakeModel("memory-v2"))
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if first.Service == second.Service {
		t.Fatal("different configuration versions reused a Memory service with a stale extractor model")
	}
}
