package tencentdb

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworkmemory "trpc.group/trpc-go/trpc-agent-go/memory/tencentdb"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
)

func TestResolverScopesIngestorsByAuthenticatedUser(t *testing.T) {
	secrets := &recordingSecrets{value: "memory-api-key"}
	resolver, err := NewResolver(secrets, &recordingGateways{url: "https://memory.example"})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	t.Cleanup(func() { _ = resolver.Close() })

	first, err := resolver.ResolveSessionIngestor(context.Background(), testExecution("user-a"))
	if err != nil {
		t.Fatalf("resolve first ingestor: %v", err)
	}
	second, err := resolver.ResolveSessionIngestor(context.Background(), testExecution("user-b"))
	if err != nil {
		t.Fatalf("resolve second ingestor: %v", err)
	}
	firstService, ok := first.(*frameworkmemory.Service)
	if !ok {
		t.Fatalf("first ingestor type = %T", first)
	}
	secondService, ok := second.(*frameworkmemory.Service)
	if !ok {
		t.Fatalf("second ingestor type = %T", second)
	}
	if firstService != secondService {
		t.Fatal("same config version did not reuse memory service")
	}
	if secrets.calls != 1 {
		t.Fatalf("secret resolution calls = %d, want 1", secrets.calls)
	}
}

func TestResolverRejectsInvalidMemoryBackend(t *testing.T) {
	resolver, err := NewResolver(&recordingSecrets{value: "memory-api-key"}, &recordingGateways{url: "https://memory.example"})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	t.Cleanup(func() { _ = resolver.Close() })

	exec := testExecution("user-a")
	exec.Config.BackendConfig.Memory.Kind = tenant.BackendVector
	if _, err := resolver.ResolveSessionIngestor(context.Background(), exec); err == nil {
		t.Fatal("resolve ingestor with invalid backend kind succeeded")
	}
}

func TestResolverSkipsUnconfiguredMemory(t *testing.T) {
	secrets := &recordingSecrets{value: "memory-api-key"}
	resolver, err := NewResolver(secrets, &recordingGateways{url: "https://memory.example"})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	t.Cleanup(func() { _ = resolver.Close() })

	exec := testExecution("user-a")
	exec.Config.BackendConfig.Memory = tenant.BackendRef{}
	ingestor, err := resolver.ResolveSessionIngestor(context.Background(), exec)
	if err != nil || ingestor != nil {
		t.Fatalf("resolve unconfigured memory = %T, %v", ingestor, err)
	}
	if secrets.calls != 0 {
		t.Fatalf("secret resolution calls = %d, want 0", secrets.calls)
	}
}

func TestResolverSkipsSharedGroupSessions(t *testing.T) {
	resolver, err := NewResolver(&recordingSecrets{value: "memory-api-key"}, &recordingGateways{url: "https://memory.example"})
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	t.Cleanup(func() { _ = resolver.Close() })

	exec := testExecution("user-a")
	exec.Tenant.SessionPrincipalID = "group-a"
	ingestor, err := resolver.ResolveSessionIngestor(context.Background(), exec)
	if err != nil || ingestor != nil {
		t.Fatalf("resolve group session memory = %T, %v", ingestor, err)
	}
}

func TestMemorySessionKeyIncludesScopeAndUser(t *testing.T) {
	key := memorySessionKey(tenant.Scope{TenantID: "tenant-a", AppID: "app-a"})(
		frameworksession.NewSession("runner", "user-a", "session-a"),
	)
	if key != "tenant:tenant-a:app:app-a:memory:user-a:session-a" {
		t.Fatalf("memory session key = %q", key)
	}
}

func testExecution(userID string) worker.Execution {
	memory := tenant.BackendRef{
		Kind:     tenant.BackendExternal,
		Provider: providerName,
		Name:     "tenant-memory",
		SecretRef: tenant.SecretRef{
			Name:    "memory-api-key",
			Version: "v1",
		},
	}
	return worker.Execution{
		Tenant: tenant.RuntimeContext{
			TenantID:           "tenant-a",
			AppID:              "app-a",
			ConfigVersion:      "v1",
			SessionPrincipalID: userID,
			UserID:             userID,
		},
		Config: tenant.AppConfig{BackendConfig: tenant.BackendConfig{Memory: memory}},
	}
}

type recordingSecrets struct {
	value string
	calls int
}

type recordingGateways struct {
	url string
}

func (r *recordingGateways) ResolveTencentDBGateway(_ context.Context, _ string) (string, error) {
	return r.url, nil
}

func (s *recordingSecrets) ResolveSecret(
	_ context.Context,
	_ tenant.Scope,
	_ tenant.SecretRef,
) (string, error) {
	s.calls++
	return s.value, nil
}
