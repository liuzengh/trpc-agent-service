package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/runtimeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
)

type dataPlaneResolverFunc func(context.Context, runtimeplane.Request) (runtimeplane.Lease, error)

func (f dataPlaneResolverFunc) Acquire(ctx context.Context, request runtimeplane.Request) (runtimeplane.Lease, error) {
	return f(ctx, request)
}

type workerTestKnowledge struct{}

func (*workerTestKnowledge) Search(context.Context, *knowledge.SearchRequest) (*knowledge.SearchResult, error) {
	return &knowledge.SearchResult{}, nil
}

type workerTestArtifact struct{}

func (*workerTestArtifact) SaveArtifact(context.Context, artifact.SessionInfo, string, *artifact.Artifact) (int, error) {
	return 0, nil
}
func (*workerTestArtifact) LoadArtifact(context.Context, artifact.SessionInfo, string, *int) (*artifact.Artifact, error) {
	return nil, nil
}
func (*workerTestArtifact) ListArtifactKeys(context.Context, artifact.SessionInfo) ([]string, error) {
	return nil, nil
}
func (*workerTestArtifact) DeleteArtifact(context.Context, artifact.SessionInfo, string) error {
	return nil
}
func (*workerTestArtifact) ListVersions(context.Context, artifact.SessionInfo, string) ([]int, error) {
	return nil, nil
}

func dataPlaneTenant() *tenant.Tenant {
	return &tenant.Tenant{
		ID: "tenant-data-plane",
		Models: []tenant.ModelConfig{{
			Provider: "openai", ModelName: "gpt-4o-mini", APIKey: "test-key", MaxTokens: 512,
		}},
		Agents: []tenant.AgentConfig{{
			Name: "support", Type: tenant.AgentTypeChain, DefaultModel: "gpt-4o-mini", MaxLLMCalls: 2,
			Tools: []string{"knowledge_search"},
			Runtime: &tenant.AgentRuntimeConfig{Nodes: []tenant.AgentRuntimeNode{{
				Name: "research", MaxLLMCalls: 2, Tools: []string{"knowledge_search"},
			}}},
		}},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist", Allowed: []string{"knowledge_search"}},
		Storage: tenant.StorageConfig{
			KnowledgeBackend: "qdrant", KnowledgeProfile: "knowledge-primary",
			ArtifactBackend: "s3", ArtifactProfile: "artifact-primary",
		},
	}
}

func TestWorkerWiresTenantDataPlaneIntoRuntimeAndReleasesIt(t *testing.T) {
	kb := &workerTestKnowledge{}
	artifacts := &workerTestArtifact{}
	var released atomic.Int32
	var acquired runtimeplane.Request
	resolver := dataPlaneResolverFunc(func(_ context.Context, request runtimeplane.Request) (runtimeplane.Lease, error) {
		acquired = request
		return runtimeplane.Lease{
			Knowledge: kb, Artifact: artifacts,
			Release: func() error { released.Add(1); return nil },
		}, nil
	})
	registry := NewRuntimeAgentRegistry()
	var dependencies RuntimeAgentDependencies
	if err := registry.Register(tenant.AgentTypeChain, RuntimeAgentFactoryFunc(func(_ context.Context, spec RuntimeAgentBuildSpec, deps RuntimeAgentDependencies) (agent.Agent, error) {
		dependencies = deps
		return &factoryTestAgent{name: spec.Agent.Name}, nil
	})); err != nil {
		t.Fatal(err)
	}
	value, err := NewWorkerWithOptions(dataPlaneTenant(), nil, nil, Options{
		AgentAppID: "support-app", DataPlaneResolver: resolver, RuntimeFactories: registry,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !acquired.NeedKnowledge || !acquired.NeedArtifact || acquired.AgentAppID != "support-app" {
		t.Fatalf("acquire request=%#v", acquired)
	}
	if dependencies.Knowledge != kb || dependencies.Artifact != artifacts {
		t.Fatalf("runtime dependencies were not wired: %#v", dependencies)
	}
	if err := value.Close(); err != nil {
		t.Fatal(err)
	}
	if err := value.Close(); err != nil {
		t.Fatal(err)
	}
	if got := released.Load(); got != 1 {
		t.Fatalf("data-plane releases=%d, want 1", got)
	}
}

func TestWorkerFailsClosedWhenKnowledgeToolHasNoDataPlaneResolver(t *testing.T) {
	registry := NewRuntimeAgentRegistry()
	if err := registry.Register(tenant.AgentTypeChain, RuntimeAgentFactoryFunc(func(_ context.Context, spec RuntimeAgentBuildSpec, _ RuntimeAgentDependencies) (agent.Agent, error) {
		return &factoryTestAgent{name: spec.Agent.Name}, nil
	})); err != nil {
		t.Fatal(err)
	}
	_, err := NewWorkerWithOptions(dataPlaneTenant(), nil, nil, Options{
		AgentAppID: "support-app", RuntimeFactories: registry,
	})
	if !errors.Is(err, ErrDataPlaneRuntimeRequired) {
		t.Fatalf("error=%v, want ErrDataPlaneRuntimeRequired", err)
	}
}
