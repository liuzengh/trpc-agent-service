package assembly

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/internal/testutil"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type recordingAutoMemoryService struct {
	memory.Service
	calls int
}

func (s *recordingAutoMemoryService) EnqueueAutoMemoryJob(context.Context, *session.Session) error {
	s.calls++
	return nil
}

type recordingSessionIngestor struct{ calls int }

func (i *recordingSessionIngestor) IngestSession(context.Context, *session.Session, ...session.IngestOption) error {
	i.calls++
	return nil
}

func TestMemoryBackendSkipsAutomaticGroupMemory(t *testing.T) {
	t.Parallel()
	base := memoryinmemory.NewMemoryService()
	t.Cleanup(func() { _ = base.Close() })
	recording := &recordingAutoMemoryService{Service: base}
	guarded := memoryServiceBackend(recording).Service

	group := session.NewSession("tenant-a/support", "group:telegram:support-bot:group-1", "session-group")
	if err := guarded.EnqueueAutoMemoryJob(context.Background(), group); err != nil {
		t.Fatal(err)
	}
	if recording.calls != 0 {
		t.Fatalf("group auto-memory calls = %d, want 0", recording.calls)
	}
	personal := session.NewSession("tenant-a/support", "platform-user-1", "session-personal")
	if err := guarded.EnqueueAutoMemoryJob(context.Background(), personal); err != nil {
		t.Fatal(err)
	}
	if recording.calls != 1 {
		t.Fatalf("personal auto-memory calls = %d, want 1", recording.calls)
	}

	ingestor := &recordingSessionIngestor{}
	guardedIngestor := memoryIngestorBackend(ingestor, nil).Ingestor
	if err := guardedIngestor.IngestSession(context.Background(), group); err != nil {
		t.Fatal(err)
	}
	if ingestor.calls != 0 {
		t.Fatalf("group external-memory ingestion calls = %d, want 0", ingestor.calls)
	}
	if err := guardedIngestor.IngestSession(context.Background(), personal); err != nil {
		t.Fatal(err)
	}
	if ingestor.calls != 1 {
		t.Fatalf("personal external-memory ingestion calls = %d, want 1", ingestor.calls)
	}
}

func TestManagedMemoryProviderRedisRoundTrip(t *testing.T) {
	redisServer := miniredis.RunT(t)
	resolver, err := credential.NewEnvironmentSecretResolver(func(name string) string {
		if name == "MEMORY_REDIS_URL" {
			return "redis://" + redisServer.Addr()
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	profiles := testBackendProfileResolver{"memory-redis": {Driver: MemoryDriverRedis, ConnectionRef: "env:MEMORY_REDIS_URL"}}
	provider, err := NewManagedMemoryProvider("postgres://unused", resolver, profiles, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	configuration := config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Storage: config.StoragePolicy{Memory: config.BackendProfileRef{ProfileID: "memory-redis"}},
	}
	backend, err := provider.MemoryBackend(context.Background(), configuration, testutil.NewFakeModel("memory"))
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Release()
	userKey := memory.UserKey{AppName: configuration.AppName(), UserID: "user-1"}
	if err := backend.Service.AddMemory(context.Background(), userKey, "prefers concise replies", []string{"preference"}); err != nil {
		t.Fatal(err)
	}
	reader, err := provider.MemoryReader(context.Background(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := reader.ReadMemories(context.Background(), userKey, 10)
	if err != nil || len(entries) != 1 || entries[0].Memory == nil || entries[0].Memory.Memory != "prefers concise replies" {
		t.Fatalf("Redis Memory round trip = %+v, %v", entries, err)
	}
}

func TestManagedMemoryProviderDoesNotReopenAfterClose(t *testing.T) {
	resolver, err := credential.NewEnvironmentSecretResolver(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	profiles := testBackendProfileResolver{"memory-test": {Driver: MemoryDriverInMemory}}
	provider, err := NewManagedMemoryProvider("postgres://unused", resolver, profiles, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Close(); err != nil {
		t.Fatal(err)
	}
	if err := provider.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
	configuration := config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", ConfigVersion: 1,
		Storage: config.StoragePolicy{Memory: config.BackendProfileRef{ProfileID: "memory-test"}},
	}
	if _, err := provider.MemoryReader(context.Background(), configuration); !errors.Is(err, ErrManagedProviderClosed) {
		t.Fatalf("MemoryReader() after Close() = %v, want ErrManagedProviderClosed", err)
	}
	if _, err := provider.MemoryBackend(context.Background(), configuration, testutil.NewFakeModel("memory")); !errors.Is(err, ErrManagedProviderClosed) {
		t.Fatalf("MemoryBackend() after Close() = %v, want ErrManagedProviderClosed", err)
	}
}

func TestMemoryBackendConnectionConfiguration(t *testing.T) {
	t.Parallel()

	chroma, err := parseChromaMemoryConnection(`{"base_url":" https://chroma.example.test/ ","api_key":" key ","tenant":" tenant-a ","database":" db-a "}`)
	if err != nil {
		t.Fatal(err)
	}
	if chroma.BaseURL != "https://chroma.example.test/" || chroma.APIKey != "key" || chroma.Tenant != "tenant-a" || chroma.Database != "db-a" {
		t.Fatalf("ChromaDB config = %+v", chroma)
	}
	if _, err := parseChromaMemoryConnection(`{"api_key":"key"}`); err == nil {
		t.Fatal("ChromaDB config without base_url was accepted")
	}

	tencent, err := parseTencentDBMemoryConnection(`{"gateway_url":" https://memory.example.test ","api_key":" key "}`)
	if err != nil {
		t.Fatal(err)
	}
	if tencent.GatewayURL != "https://memory.example.test" || tencent.APIKey != "key" {
		t.Fatalf("TencentDB config = %+v", tencent)
	}
	if _, err := parseTencentDBMemoryConnection(`{"gateway_url":""}`); err == nil {
		t.Fatal("TencentDB config without gateway_url was accepted")
	}
}

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
	service, err := provider.MemoryService(context.Background(), configuration)
	if err != nil {
		t.Fatal(err)
	}
	if service != backend.Service {
		t.Fatal("MemoryService() did not resolve the active in-memory Runtime service")
	}
	if err := service.AddMemory(context.Background(), userKey, "prefers concise replies", []string{"preference"}); err != nil {
		t.Fatal(err)
	}
	after, err := reader.ReadMemories(context.Background(), userKey, 10)
	if err != nil || len(after) != 1 || after[0].Memory == nil || after[0].Memory.Memory != "prefers concise replies" {
		t.Fatalf("reader after runtime = %+v, %v", after, err)
	}
	matched, err := reader.SearchMemories(context.Background(), userKey, "concise")
	if err != nil || len(matched) != 1 || matched[0].Memory == nil || matched[0].Memory.Memory != "prefers concise replies" {
		t.Fatalf("SearchMemories() = %+v, %v", matched, err)
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
