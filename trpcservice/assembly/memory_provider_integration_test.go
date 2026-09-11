package assembly

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/testutil"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	"trpc.group/trpc-go/trpc-agent-go/event"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestPostgresMemoryProviderUsesOfficialMemoryService(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	resolver, err := credential.NewEnvironmentSecretResolver(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	profiles := testBackendProfileResolver{"memory-postgres": {Driver: MemoryDriverPostgres}}
	provider, err := NewManagedMemoryProvider(dsn, resolver, profiles, nil)
	if err != nil {
		t.Fatalf("NewManagedMemoryProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	backend, err := provider.MemoryBackend(context.Background(), config.TenantConfig{
		TenantID: "integration", AppCode: "memory", Status: config.AgentActive, ConfigVersion: 1,
		Storage: config.StoragePolicy{Memory: config.BackendProfileRef{ProfileID: "memory-postgres"}},
	}, testutil.NewFakeModel("memory extractor"))
	if err != nil {
		t.Fatalf("MemoryBackend() error = %v", err)
	}
	service := backend.Service
	t.Cleanup(func() { _ = backend.Release() })

	userKey := agentmemory.UserKey{
		AppName: "integration/memory",
		UserID:  fmt.Sprintf("memory-%d", time.Now().UnixNano()),
	}
	if err := service.AddMemory(
		context.Background(), userKey, "用户偏好简洁回答", []string{"preference"},
		agentmemory.WithMetadata(&agentmemory.Metadata{Kind: agentmemory.KindFact}),
	); err != nil {
		t.Fatalf("AddMemory() error = %v", err)
	}
	t.Cleanup(func() { _ = service.ClearMemories(context.Background(), userKey) })

	entries, err := service.ReadMemories(context.Background(), userKey, 10)
	if err != nil {
		t.Fatalf("ReadMemories() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Memory == nil || entries[0].Memory.Memory != "用户偏好简洁回答" {
		t.Fatalf("ReadMemories() = %+v", entries)
	}
	if entries[0].Memory.Kind != agentmemory.KindFact {
		t.Fatalf("memory kind = %q, want %q", entries[0].Memory.Kind, agentmemory.KindFact)
	}

	eventTime := time.Date(2024, 5, 7, 0, 0, 0, 0, time.UTC)
	if err := service.AddMemory(
		context.Background(), userKey, "On 2024-05-07, user hiked Mt. Fuji with Alice", []string{"trip"},
		agentmemory.WithMetadata(&agentmemory.Metadata{
			Kind: agentmemory.KindEpisode, EventTime: &eventTime, Location: "Mt. Fuji", Participants: []string{"Alice"},
		}),
	); err != nil {
		t.Fatalf("AddMemory() episode error = %v", err)
	}
	found, err := service.SearchMemories(
		context.Background(), userKey, "Fuji",
		agentmemory.WithSearchOptions(agentmemory.SearchOptions{Query: "Fuji", Kind: agentmemory.KindEpisode, MaxResults: 10}),
	)
	if err != nil {
		t.Fatalf("SearchMemories() error = %v", err)
	}
	if len(found) != 1 || found[0].Memory == nil || found[0].Memory.Kind != agentmemory.KindEpisode || found[0].Memory.Location != "Mt. Fuji" {
		t.Fatalf("SearchMemories() episode = %+v", found)
	}

	names := make([]string, 0, len(service.Tools()))
	for _, memoryTool := range service.Tools() {
		if memoryTool != nil && memoryTool.Declaration() != nil {
			names = append(names, memoryTool.Declaration().Name)
		}
	}
	if !containsString(names, agentmemory.SearchToolName) || containsString(names, agentmemory.LoadToolName) {
		t.Fatalf("Tools() = %v, want %s only", names, agentmemory.SearchToolName)
	}
	if containsString(names, agentmemory.AddToolName) {
		t.Fatalf("Tools() exposed background add tool: %v", names)
	}
}

func TestPostgresMemoryProviderExtractsWithTenantModel(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	resolver, err := credential.NewEnvironmentSecretResolver(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	profiles := testBackendProfileResolver{"memory-postgres": {Driver: MemoryDriverPostgres}}
	provider, err := NewManagedMemoryProvider(dsn, resolver, profiles, nil)
	if err != nil {
		t.Fatalf("NewManagedMemoryProvider() error = %v", err)
	}
	t.Cleanup(func() { _ = provider.Close() })
	extracted := testutil.NewExtractingModel("pg-extractor", "收到", "用户偏好简洁回答")
	backend, err := provider.MemoryBackend(context.Background(), config.TenantConfig{
		TenantID: "integration", AppCode: "memory-extract", Status: config.AgentActive, ConfigVersion: 1,
		Storage: config.StoragePolicy{Memory: config.BackendProfileRef{ProfileID: "memory-postgres"}},
	}, extracted)
	if err != nil {
		t.Fatalf("MemoryBackend() error = %v", err)
	}
	service := backend.Service
	t.Cleanup(func() { _ = backend.Release() })

	userKey := agentmemory.UserKey{
		AppName: "integration/memory-extract",
		UserID:  fmt.Sprintf("extract-%d", time.Now().UnixNano()),
	}
	t.Cleanup(func() { _ = service.ClearMemories(context.Background(), userKey) })

	sess := session.NewSession(userKey.AppName, userKey.UserID, "session-1")
	sess.Events = []event.Event{
		{
			Timestamp: time.Now().Add(-3 * time.Second),
			Response:  &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage("请记住我喜欢简洁回答")}}},
		},
		{
			Timestamp: time.Now().Add(-2 * time.Second),
			Response:  &model.Response{Choices: []model.Choice{{Message: model.NewAssistantMessage("好的")}}},
		},
		{
			Timestamp: time.Now().Add(-time.Second),
			Response:  &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage("之后都尽量简短")}}},
		},
		{
			Timestamp: time.Now(),
			Response:  &model.Response{Choices: []model.Choice{{Message: model.NewAssistantMessage("明白")}}},
		},
		{
			Timestamp: time.Now().Add(time.Second),
			Response:  &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage("这个偏好之后也继续保留")}}},
		},
	}
	if err := service.EnqueueAutoMemoryJob(context.Background(), sess); err != nil {
		t.Fatalf("EnqueueAutoMemoryJob() error = %v", err)
	}
	entries := waitForMemories(t, service, userKey, 1)
	if entries[0].Memory.Memory != "用户偏好简洁回答" || entries[0].Memory.Kind != agentmemory.KindFact {
		t.Fatalf("extracted postgres memory = %+v", entries[0].Memory)
	}
	if len(extracted.ToolNames()) == 0 {
		t.Fatal("memory extractor model was not invoked")
	}

	otherBackend, err := provider.MemoryBackend(context.Background(), config.TenantConfig{
		TenantID: "other", AppCode: "memory-extract", Status: config.AgentActive, ConfigVersion: 1,
		Storage: config.StoragePolicy{Memory: config.BackendProfileRef{ProfileID: "memory-postgres"}},
	}, testutil.NewExtractingModel("other-extractor", "收到", "另一个租户的记忆"))
	if err != nil {
		t.Fatalf("Memory() other tenant error = %v", err)
	}
	other := otherBackend.Service
	t.Cleanup(func() { _ = otherBackend.Release() })
	leaked, err := other.ReadMemories(context.Background(), agentmemory.UserKey{AppName: "other/memory-extract", UserID: userKey.UserID}, 10)
	if err != nil {
		t.Fatalf("ReadMemories() other tenant error = %v", err)
	}
	if len(leaked) != 0 {
		t.Fatalf("cross-tenant postgres memory leak = %+v", leaked)
	}
}
