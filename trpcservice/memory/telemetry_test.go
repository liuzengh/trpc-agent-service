package memory_test

import (
	"context"
	"sync"
	"testing"

	servicememory "github.com/liuzengh/trpc-agent-service/trpcservice/memory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
)

// operationProvider is intentionally a provider decorator, so this test
// validates the public trpc-agent-go Service boundary instead of a backend.
type operationProvider struct {
	telemetry.Provider
	mu         sync.Mutex
	operations []telemetry.Operation
}

func (p *operationProvider) StartSpan(ctx context.Context, operation telemetry.Operation, attributes ...telemetry.Attribute) (context.Context, telemetry.Span) {
	p.mu.Lock()
	p.operations = append(p.operations, operation)
	p.mu.Unlock()
	return p.Provider.StartSpan(ctx, operation, attributes...)
}

func TestResolverTracesMemoryServiceBoundary(t *testing.T) {
	snapshot := memorySnapshot()
	backend := provider.BackendProfileSnapshot{
		TenantID: "tenant-a", ProfileID: "memory", Version: 7, Status: "active", Provider: "postgres", SchemaVersion: 1,
		Capabilities: provider.CapabilitySet{"strong_ryw": true},
	}
	traces := &operationProvider{Provider: telemetry.Noop()}
	resolver := servicememory.Resolver{
		Profiles:            backendReader{value: backend},
		PostgresConnections: map[string]string{"default": "postgres://memory"},
		BuildPostgres: func(string) (agentmemory.Service, error) {
			return memoryinmemory.NewMemoryService(), nil
		},
		Telemetry: traces,
	}
	service, err := resolver.Resolve(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	key := agentmemory.UserKey{AppName: snapshot.AppName, UserID: "user"}
	if err := service.AddMemory(context.Background(), key, "likes tea", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ReadMemories(context.Background(), key, 10); err != nil {
		t.Fatal(err)
	}
	traces.mu.Lock()
	defer traces.mu.Unlock()
	want := []telemetry.Operation{telemetry.OperationMemoryAdd, telemetry.OperationMemoryRead}
	if len(traces.operations) != len(want) {
		t.Fatalf("operations=%v want=%v", traces.operations, want)
	}
	for index, operation := range want {
		if traces.operations[index] != operation {
			t.Fatalf("operations=%v want=%v", traces.operations, want)
		}
	}
}
