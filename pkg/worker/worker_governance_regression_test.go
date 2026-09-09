package worker

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmem "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type leasedTestAdapter struct {
	storage.StorageAdapter
	releases atomic.Int32
}

func (a *leasedTestAdapter) AcquireServices(context.Context, *tenant.Tenant) (session.Service, memory.Service, func(), error) {
	return sessioninmem.NewSessionService(), inmemory.NewMemoryService(), func() { a.releases.Add(1) }, nil
}

// storageOnlyAdapter intentionally omits ServiceLeaseAdapter. Embedding the
// interface lets this regression test exercise the production constructor's
// type contract without opening a backend or invoking any method.
type storageOnlyAdapter struct {
	storage.StorageAdapter
}

type countingStorageAdapter struct {
	storage.StorageAdapter
	sessionCalls int
	memoryCalls  int
}

func (a *countingStorageAdapter) SessionService(context.Context, *tenant.Tenant) (session.Service, error) {
	a.sessionCalls++
	return sessioninmem.NewSessionService(), nil
}

func (a *countingStorageAdapter) MemoryService(context.Context, *tenant.Tenant) (memory.Service, error) {
	a.memoryCalls++
	return inmemory.NewMemoryService(), nil
}

type typedNilStorageAdapter struct{ storage.StorageAdapter }

func TestNewWorker_RejectsImplicitAllowAllPolicy(t *testing.T) {
	_, err := NewWorker(&tenant.Tenant{ID: "legacy-tenant"}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "whitelist") {
		t.Fatalf("legacy implicit allow-all policy was not rejected: %v", err)
	}
}

func TestNewWorkerProductionModeRejectsMissingSharedStorage(t *testing.T) {
	testTenant := &tenant.Tenant{
		ID: "production-storage-tenant",
		Models: []tenant.ModelConfig{{
			ModelName: "gpt-4", Provider: "openai", APIKey: "test-key",
		}},
		Agents:     []tenant.AgentConfig{{Name: "support", DefaultModel: "gpt-4"}},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
		Storage:    tenant.StorageConfig{SessionBackend: "redis", MemoryBackend: "postgres"},
	}
	_, err := NewProductionWorkerWithOptionsContext(context.Background(), testTenant, nil, nil, Options{})
	if !errors.Is(err, ErrSharedStorageRequired) {
		t.Fatalf("production worker error = %v, want ErrSharedStorageRequired", err)
	}
}

func TestNewWorkerProductionModeRejectsMissingDistributedCoordination(t *testing.T) {
	testTenant := &tenant.Tenant{
		ID: "production-coordination-tenant",
		Models: []tenant.ModelConfig{{
			ModelName: "gpt-4", Provider: "openai", APIKey: "test-key",
		}},
		Agents:     []tenant.AgentConfig{{Name: "support", DefaultModel: "gpt-4"}},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
		Storage:    tenant.StorageConfig{SessionBackend: "redis", MemoryBackend: "postgres"},
	}
	adapter := storage.NewMultiTenantStorageAdapterImpl()
	t.Cleanup(func() { _ = adapter.Close() })
	_, err := NewWorkerWithOptions(testTenant, adapter, nil, Options{RequireSharedStorage: true})
	if !errors.Is(err, ErrDistributedSessionCoordinationRequired) {
		t.Fatalf("production worker error = %v, want ErrDistributedSessionCoordinationRequired", err)
	}
}

func TestNewWorkerProductionModeRejectsUnleasedStorageAdapter(t *testing.T) {
	testTenant := &tenant.Tenant{
		ID:         "production-adapter-tenant",
		Models:     []tenant.ModelConfig{{ModelName: "gpt-4", Provider: "openai", APIKey: "test-key"}},
		Agents:     []tenant.AgentConfig{{Name: "support", DefaultModel: "gpt-4"}},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
		Storage:    tenant.StorageConfig{SessionBackend: "redis", MemoryBackend: "postgres"},
	}
	adapter := &storageOnlyAdapter{}
	_, err := NewProductionWorkerWithOptionsContext(context.Background(), testTenant, adapter, nil, Options{})
	if !errors.Is(err, ErrSharedStorageRequired) {
		t.Fatalf("production worker error = %v, want ErrSharedStorageRequired", err)
	}
}

func TestNewWorkerProductionModeRejectsTypedNilStorageAdapter(t *testing.T) {
	testTenant := &tenant.Tenant{
		ID:         "typed-nil-adapter-tenant",
		Status:     tenant.TenantStatusActive,
		Models:     []tenant.ModelConfig{{ModelName: "gpt-4", Provider: "openai", APIKey: "test-key"}},
		Agents:     []tenant.AgentConfig{{Name: "support", DefaultModel: "gpt-4"}},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
		Storage:    tenant.StorageConfig{SessionBackend: "redis", MemoryBackend: "postgres"},
	}
	var adapter *typedNilStorageAdapter
	_, err := NewProductionWorkerWithOptionsContext(context.Background(), testTenant, adapter, nil, Options{})
	if !errors.Is(err, ErrSharedStorageRequired) {
		t.Fatalf("typed-nil storage adapter error=%v, want ErrSharedStorageRequired", err)
	}
}

func TestNewWorkerReleasesStorageWhenConstructionFailsAfterAcquire(t *testing.T) {
	testTenant := &tenant.Tenant{
		ID:         "failed-worker-tenant",
		Models:     []tenant.ModelConfig{{ModelName: "gpt-4", Provider: "openai", APIKey: "test-key"}},
		Agents:     []tenant.AgentConfig{{Name: "support", DefaultModel: "gpt-4", Tools: []string{"not-registered"}}},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist", Allowed: []string{"not-registered"}},
	}
	adapter := &leasedTestAdapter{}
	if _, err := NewWorkerWithOptionsContext(context.Background(), testTenant, adapter, nil, Options{}); err == nil {
		t.Fatal("worker construction unexpectedly succeeded with no tool resolver")
	}
	if got := adapter.releases.Load(); got != 1 {
		t.Fatalf("storage lease releases=%d, want 1 after failed construction", got)
	}
}

func TestProcess_BlocksContentBeforeModelInvocation(t *testing.T) {
	testTenant := &tenant.Tenant{
		ID: "content-policy-tenant",
		Models: []tenant.ModelConfig{{
			ModelName: "test-model",
			Provider:  "openai",
			APIKey:    "test-key",
		}},
		Agents: []tenant.AgentConfig{{
			Name:         "test-agent",
			DefaultModel: "test-model",
		}},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
		Governance: tenant.GovernancePolicy{ContentFilters: []tenant.ContentFilter{{
			Name:     "blocked-keyword",
			Type:     "keyword",
			Patterns: []string{"forbidden"},
			Action:   "block",
		}}},
	}
	value, err := NewWorker(testTenant, nil, nil)
	if err != nil {
		t.Fatalf("create worker: %v", err)
	}
	defer value.Close()
	if _, err := value.Process(context.Background(), &Request{
		UserID:    "user",
		SessionID: "session",
		Content:   "this contains FORBIDDEN input",
	}); err == nil || !strings.Contains(err.Error(), "content policy") {
		t.Fatalf("blocked input reached model path: %v", err)
	}
}

func TestNewWorkerAlwaysOwnsTenantAppNamespace(t *testing.T) {
	newTenant := func(id string) *tenant.Tenant {
		return &tenant.Tenant{
			ID: id,
			Models: []tenant.ModelConfig{{
				ModelName: "gpt-4", Provider: "openai", APIKey: "test-key",
			}},
			Agents:     []tenant.AgentConfig{{Name: "support", DefaultModel: "gpt-4"}},
			ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
		}
	}
	left, err := NewWorkerWithOptions(newTenant("tenant-a"), nil, nil, Options{AppName: "shared"})
	if err != nil {
		t.Fatal(err)
	}
	defer left.Close()
	right, err := NewWorkerWithOptions(newTenant("tenant-b"), nil, nil, Options{AppName: "shared"})
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()
	if left.appName == right.appName {
		t.Fatalf("two tenants received the same Runner namespace %q", left.appName)
	}

	escape, err := NewWorkerWithOptions(newTenant("tenant-a"), nil, nil, Options{AppName: "tenant-b-support"})
	if err != nil {
		t.Fatal(err)
	}
	defer escape.Close()
	if escape.appName == "tenant-b-support" || escape.appName == right.appName {
		t.Fatalf("caller-controlled app name escaped tenant scope: %q", escape.appName)
	}

	defaulted, err := NewWorker(newTenant("tenant-a"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer defaulted.Close()
	if defaulted.appName == "support" || defaulted.appName == "" {
		t.Fatalf("default agent path did not receive a tenant namespace: %q", defaulted.appName)
	}
}

func TestWorkerTakesImmutableTenantSnapshot(t *testing.T) {
	testTenant := &tenant.Tenant{
		ID:         "snapshot-tenant",
		Models:     []tenant.ModelConfig{{ModelName: "gpt-4", Provider: "openai", APIKey: "test-key"}},
		Agents:     []tenant.AgentConfig{{Name: "support", DefaultModel: "gpt-4"}},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist", Allowed: []string{"search"}},
	}
	value, err := NewWorker(testTenant, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = value.Close() })
	testTenant.ID = "attacker-tenant"
	testTenant.ToolPolicy.Allowed[0] = "dangerous"
	if value.tenant.ID != "snapshot-tenant" || value.tenant.ToolPolicy.Allowed[0] != "search" {
		t.Fatalf("worker retained mutable control-plane state: %#v", value.tenant)
	}
}

func TestWorkerRejectsCrossTenantAndDeploymentRequestScope(t *testing.T) {
	testTenant := &tenant.Tenant{
		ID:         "scope-tenant",
		Models:     []tenant.ModelConfig{{ModelName: "gpt-4", Provider: "openai", APIKey: "test-key"}},
		Agents:     []tenant.AgentConfig{{Name: "support", DefaultModel: "gpt-4"}},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
	}
	value, err := NewWorkerWithOptions(testTenant, nil, nil, Options{
		AppName: "support", VersionID: "version-a", DeploymentID: "deployment-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = value.Close() })
	cases := []Request{
		{TenantID: "other-tenant"},
		{TenantID: "scope-tenant", AgentApp: "other-app"},
		{TenantID: "scope-tenant", AgentVersion: "version-b"},
		{TenantID: "scope-tenant", DeploymentID: "deployment-b"},
	}
	for _, request := range cases {
		if err := value.validateRequestScope(&request); err == nil {
			t.Fatalf("request scope %#v was accepted", request)
		}
	}
}

func TestStrictWorkerRejectsAppSnapshotIdentityMismatch(t *testing.T) {
	// This check is exercised through the production constructor only after
	// shared storage and fencing prerequisites are satisfied. The direct
	// identity helper keeps this regression independent from Redis setup.
	if got := validateWorkerAppIdentity("support", "billing"); got == nil {
		t.Fatal("strict worker accepted an app name unrelated to its snapshot agent")
	}
}

func TestWorkerRejectsUninstalledRuntimeBeforeModelOrStorageInitialization(t *testing.T) {
	llmOnlyRegistry := &RuntimeAgentRegistry{
		factories:    map[string]RuntimeAgentFactory{tenant.AgentTypeLLM: llmRuntimeAgentFactory{}},
		capabilities: map[string]string{tenant.AgentTypeLLM: tenant.AgentTypeLLM},
	}
	tenantSnapshot := &tenant.Tenant{
		ID:     "runtime-preflight",
		Status: tenant.TenantStatusActive,
		Agents: []tenant.AgentConfig{{
			Name: "assistant", Type: tenant.AgentTypeGraph, DefaultModel: "gpt-4o-mini", MaxLLMCalls: 1,
			Runtime: &tenant.AgentRuntimeConfig{Nodes: []tenant.AgentRuntimeNode{{Name: "answer"}}, Entry: "answer"},
		}},
		Models:     []tenant.ModelConfig{{Provider: "openai", ModelName: "gpt-4o-mini"}},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
	}
	adapter := &countingStorageAdapter{}
	_, err := NewWorkerWithOptionsContext(context.Background(), tenantSnapshot, adapter, nil, Options{RuntimeFactories: llmOnlyRegistry})
	if !errors.Is(err, ErrAgentFactoryUnavailable) {
		t.Fatalf("error=%v, want ErrAgentFactoryUnavailable", err)
	}
	if adapter.sessionCalls != 0 || adapter.memoryCalls != 0 {
		t.Fatalf("storage initialized before runtime rejection: session=%d memory=%d", adapter.sessionCalls, adapter.memoryCalls)
	}
}

func TestWorkerRejectsRuntimeCapabilityMismatchBeforeModelInitialization(t *testing.T) {
	tenantSnapshot := &tenant.Tenant{
		ID:         "runtime-fingerprint-mismatch",
		Models:     []tenant.ModelConfig{{Provider: "openai", ModelName: "gpt-4o-mini", APIKey: "test-key"}},
		Agents:     []tenant.AgentConfig{{Name: "assistant", Type: tenant.AgentTypeLLM, DefaultModel: "gpt-4o-mini"}},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
	}
	_, err := NewWorkerWithOptionsContext(context.Background(), tenantSnapshot, nil, nil, Options{
		RuntimeFactories:             NewRuntimeAgentRegistry(),
		RuntimeCapabilityFingerprint: strings.Repeat("a", 64),
	})
	if !errors.Is(err, ErrRuntimeCapabilityMismatch) {
		t.Fatalf("error=%v, want ErrRuntimeCapabilityMismatch", err)
	}
}

func TestProductionWorkerRejectsLegacyCustomRuntimeWithoutCapability(t *testing.T) {
	tenantSnapshot := &tenant.Tenant{
		ID:     "runtime-capability-required",
		Status: tenant.TenantStatusActive,
		Models: []tenant.ModelConfig{{Provider: "openai", ModelName: "gpt-4o-mini", APIKey: "test-key"}},
		Agents: []tenant.AgentConfig{{
			Name: "assistant", Type: tenant.AgentTypeGraph, DefaultModel: "gpt-4o-mini", MaxLLMCalls: 1,
			Runtime: &tenant.AgentRuntimeConfig{Nodes: []tenant.AgentRuntimeNode{{Name: "answer"}}, Entry: "answer"},
		}},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
	}
	registry := NewRuntimeAgentRegistry()
	if err := registry.Register(tenant.AgentTypeGraph, RuntimeAgentFactoryFunc(func(_ context.Context, spec RuntimeAgentBuildSpec, _ RuntimeAgentDependencies) (agent.Agent, error) {
		return &factoryTestAgent{name: spec.Agent.Name}, nil
	})); err != nil {
		t.Fatal(err)
	}
	_, err := NewWorkerWithOptionsContext(context.Background(), tenantSnapshot, nil, nil, Options{
		Agent:                &tenantSnapshot.Agents[0],
		Model:                &tenantSnapshot.Models[0],
		AppName:              "assistant",
		AgentAppID:           "app-1",
		VersionID:            "version-1",
		DeploymentID:         "deployment-1",
		RuntimeFactories:     registry,
		RequireAtomicFencing: true,
	})
	if !errors.Is(err, ErrRuntimeCapabilityRequired) {
		t.Fatalf("production worker error=%v, want ErrRuntimeCapabilityRequired", err)
	}
}
