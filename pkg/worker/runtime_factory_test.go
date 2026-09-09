package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

type factoryTestAgent struct{ name string }

func (a *factoryTestAgent) Run(context.Context, *agent.Invocation) (<-chan *event.Event, error) {
	return nil, nil
}
func (a *factoryTestAgent) Tools() []tool.Tool { return nil }
func (a *factoryTestAgent) Info() agent.Info   { return agent.Info{Name: a.name} }
func (a *factoryTestAgent) SubAgents() []agent.Agent {
	return nil
}
func (a *factoryTestAgent) FindSubAgent(string) agent.Agent { return nil }

type countingRuntimeAgent struct {
	name string
	runs *atomic.Int32
}

func (a *countingRuntimeAgent) Run(context.Context, *agent.Invocation) (<-chan *event.Event, error) {
	a.runs.Add(1)
	events := make(chan *event.Event)
	close(events)
	return events, nil
}
func (a *countingRuntimeAgent) Tools() []tool.Tool              { return nil }
func (a *countingRuntimeAgent) Info() agent.Info                { return agent.Info{Name: a.name} }
func (a *countingRuntimeAgent) SubAgents() []agent.Agent        { return nil }
func (a *countingRuntimeAgent) FindSubAgent(string) agent.Agent { return nil }

type panicInfoTestAgent struct{}

type snapshotTestTool struct{ name string }

func (t snapshotTestTool) Declaration() *tool.Declaration { return &tool.Declaration{Name: t.name} }

func (panicInfoTestAgent) Run(context.Context, *agent.Invocation) (<-chan *event.Event, error) {
	return nil, nil
}
func (panicInfoTestAgent) Tools() []tool.Tool { return nil }
func (panicInfoTestAgent) Info() agent.Info   { panic("provider bug") }
func (panicInfoTestAgent) SubAgents() []agent.Agent {
	return nil
}
func (panicInfoTestAgent) FindSubAgent(string) agent.Agent { return nil }

func TestRuntimeAgentRegistryBundlesAllSupportedRuntimes(t *testing.T) {
	registry := NewRuntimeAgentRegistry()
	for _, runtimeType := range []string{
		tenant.AgentTypeLLM, tenant.AgentTypeChain, tenant.AgentTypeGraph,
		tenant.AgentTypeParallel, tenant.AgentTypeCycle,
	} {
		if err := registry.ValidateRuntimeTypeStrict(runtimeType); err != nil {
			t.Fatalf("bundled runtime %q rejected: %v", runtimeType, err)
		}
	}
}

func TestBundledCompositeRuntimeFactoriesBuildUpstreamAgents(t *testing.T) {
	tests := []struct {
		name    string
		runtime string
		config  *tenant.AgentRuntimeConfig
	}{
		{"chain", tenant.AgentTypeChain, &tenant.AgentRuntimeConfig{Nodes: []tenant.AgentRuntimeNode{{Name: "first"}, {Name: "second"}}}},
		{"parallel", tenant.AgentTypeParallel, &tenant.AgentRuntimeConfig{Nodes: []tenant.AgentRuntimeNode{{Name: "left"}, {Name: "right"}}}},
		{"cycle", tenant.AgentTypeCycle, &tenant.AgentRuntimeConfig{Nodes: []tenant.AgentRuntimeNode{{Name: "review"}}, MaxIterations: 2}},
		{"graph", tenant.AgentTypeGraph, &tenant.AgentRuntimeConfig{
			Nodes: []tenant.AgentRuntimeNode{{Name: "draft"}, {Name: "check"}},
			Entry: "draft", Edges: []tenant.AgentRuntimeEdge{{From: "draft", To: "check"}},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var leaves []RuntimeLLMAgentBuildSpec
			built, err := buildRuntimeAgent(context.Background(), RuntimeAgentBuildSpec{
				Agent: tenant.AgentConfig{
					Name: "support", Type: test.runtime, DefaultModel: "gpt", MaxLLMCalls: 2,
					SystemPrompt: "shared", Runtime: test.config,
				},
			}, RuntimeAgentDependencies{LLMAgentBuilder: RuntimeLLMAgentBuilderFunc(func(_ context.Context, leaf RuntimeLLMAgentBuildSpec) (agent.Agent, error) {
				leaves = append(leaves, leaf)
				return &factoryTestAgent{name: leaf.Name}, nil
			})}, NewRuntimeAgentRegistry())
			if err != nil {
				t.Fatalf("build bundled %s runtime: %v", test.runtime, err)
			}
			if built.Info().Name != "support" || len(built.SubAgents()) != len(test.config.Nodes) || len(leaves) != len(test.config.Nodes) {
				t.Fatalf("built runtime=%#v subagents=%d leaves=%d", built.Info(), len(built.SubAgents()), len(leaves))
			}
			for _, leaf := range leaves {
				if leaf.SystemPrompt != "shared" || leaf.MaxLLMCalls != 1 {
					t.Fatalf("leaf configuration was not preserved: %#v", leaf)
				}
			}
		})
	}
}

func TestBundledCompositeRuntimesExecuteConfiguredTopology(t *testing.T) {
	tests := []struct {
		name       string
		runtime    string
		config     *tenant.AgentRuntimeConfig
		wantCounts map[string]int32
	}{
		{"chain", tenant.AgentTypeChain, &tenant.AgentRuntimeConfig{Nodes: []tenant.AgentRuntimeNode{{Name: "first"}, {Name: "second"}}}, map[string]int32{"first": 1, "second": 1}},
		{"parallel", tenant.AgentTypeParallel, &tenant.AgentRuntimeConfig{Nodes: []tenant.AgentRuntimeNode{{Name: "left"}, {Name: "right"}}}, map[string]int32{"left": 1, "right": 1}},
		{"cycle", tenant.AgentTypeCycle, &tenant.AgentRuntimeConfig{Nodes: []tenant.AgentRuntimeNode{{Name: "review"}}, MaxIterations: 2}, map[string]int32{"review": 2}},
		{"graph", tenant.AgentTypeGraph, &tenant.AgentRuntimeConfig{
			Nodes: []tenant.AgentRuntimeNode{{Name: "draft"}, {Name: "check"}}, Entry: "draft",
			Edges: []tenant.AgentRuntimeEdge{{From: "draft", To: "check"}},
		}, map[string]int32{"draft": 1, "check": 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			counters := make(map[string]*atomic.Int32, len(test.config.Nodes))
			for _, node := range test.config.Nodes {
				counters[node.Name] = &atomic.Int32{}
			}
			built, err := buildRuntimeAgent(context.Background(), RuntimeAgentBuildSpec{
				Agent: tenant.AgentConfig{Name: "support", Type: test.runtime, DefaultModel: "gpt", MaxLLMCalls: 2, Runtime: test.config},
			}, RuntimeAgentDependencies{LLMAgentBuilder: RuntimeLLMAgentBuilderFunc(func(_ context.Context, leaf RuntimeLLMAgentBuildSpec) (agent.Agent, error) {
				return &countingRuntimeAgent{name: leaf.Name, runs: counters[leaf.Name]}, nil
			})}, NewRuntimeAgentRegistry())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			events, err := built.Run(ctx, &agent.Invocation{InvocationID: "runtime-test", AgentName: "support"})
			if err != nil {
				t.Fatal(err)
			}
			for range events {
			}
			if ctx.Err() != nil {
				t.Fatalf("runtime did not complete: %v", ctx.Err())
			}
			for name, want := range test.wantCounts {
				if got := counters[name].Load(); got != want {
					t.Fatalf("node %q runs=%d, want %d", name, got, want)
				}
			}
		})
	}
}

func TestWorkerCompositionBuildsEveryBundledCompositeRuntime(t *testing.T) {
	tests := []struct {
		runtime string
		config  *tenant.AgentRuntimeConfig
	}{
		{tenant.AgentTypeChain, &tenant.AgentRuntimeConfig{Nodes: []tenant.AgentRuntimeNode{{Name: "first"}, {Name: "second"}}}},
		{tenant.AgentTypeParallel, &tenant.AgentRuntimeConfig{Nodes: []tenant.AgentRuntimeNode{{Name: "left"}, {Name: "right"}}}},
		{tenant.AgentTypeCycle, &tenant.AgentRuntimeConfig{Nodes: []tenant.AgentRuntimeNode{{Name: "review"}}, MaxIterations: 2}},
		{tenant.AgentTypeGraph, &tenant.AgentRuntimeConfig{
			Nodes: []tenant.AgentRuntimeNode{{Name: "draft"}, {Name: "check"}}, Entry: "draft",
			Edges: []tenant.AgentRuntimeEdge{{From: "draft", To: "check"}},
		}},
	}
	for _, test := range tests {
		t.Run(test.runtime, func(t *testing.T) {
			tenantSnapshot := &tenant.Tenant{
				ID: "runtime-composition-" + test.runtime,
				Models: []tenant.ModelConfig{{
					Provider: "openai", ModelName: "gpt-4o-mini", APIKey: "test-key", MaxTokens: 128,
				}},
				Agents: []tenant.AgentConfig{{
					Name: "support", Type: test.runtime, DefaultModel: "gpt-4o-mini", MaxLLMCalls: 2, Runtime: test.config,
				}},
				ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
				Storage:    tenant.StorageConfig{SessionBackend: "inmemory", MemoryBackend: "inmemory"},
			}
			value, err := NewWorkerWithOptions(tenantSnapshot, nil, nil, Options{})
			if err != nil {
				t.Fatalf("compose %s Worker: %v", test.runtime, err)
			}
			if err := value.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRuntimeAgentRegistryPreflightAcceptsRegisteredRuntime(t *testing.T) {
	registry := NewRuntimeAgentRegistry()
	if err := registry.Register(tenant.AgentTypeGraph, RuntimeAgentFactoryFunc(func(_ context.Context, spec RuntimeAgentBuildSpec, _ RuntimeAgentDependencies) (agent.Agent, error) {
		return &factoryTestAgent{name: spec.Agent.Name}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := registry.ValidateRuntimeType(tenant.AgentTypeGraph); err != nil {
		t.Fatalf("registered graph preflight failed: %v", err)
	}
	if err := (*RuntimeAgentRegistry)(nil).ValidateRuntimeType(""); err != nil {
		t.Fatalf("nil registry must retain built-in llm default: %v", err)
	}
}

func TestRuntimeAgentRegistryFingerprintIsStableAndCapabilityAware(t *testing.T) {
	left := NewRuntimeAgentRegistry()
	right := NewRuntimeAgentRegistry()
	if left.Fingerprint() != right.Fingerprint() {
		t.Fatal("identical runtime registries produced different fingerprints")
	}
	factory := RuntimeAgentFactoryFunc(func(_ context.Context, spec RuntimeAgentBuildSpec, _ RuntimeAgentDependencies) (agent.Agent, error) {
		return &factoryTestAgent{name: spec.Agent.Name}, nil
	})
	if err := left.RegisterWithCapability(tenant.AgentTypeGraph, "graph@build-1", factory); err != nil {
		t.Fatal(err)
	}
	first := left.Fingerprint()
	if err := left.RegisterWithCapability(tenant.AgentTypeGraph, "graph@build-2", factory); err != nil {
		t.Fatal(err)
	}
	if second := left.Fingerprint(); second == first {
		t.Fatal("changing a runtime capability identity did not change the fingerprint")
	}
	for name, capability := range map[string]string{
		"empty":        "",
		"same-as-type": tenant.AgentTypeParallel,
		"newline":      "graph\nbuild",
		"tab":          "graph\tbuild",
		"nul":          string([]byte{'g', 0, 'b'}),
	} {
		t.Run(name, func(t *testing.T) {
			if err := left.RegisterWithCapability(tenant.AgentTypeParallel, capability, factory); !errors.Is(err, ErrAgentFactoryInvalid) {
				t.Fatalf("capability %q error=%v, want ErrAgentFactoryInvalid", capability, err)
			}
		})
	}
}

func TestRuntimeAgentRegistrySealPreventsImplementationReplacement(t *testing.T) {
	registry := NewRuntimeAgentRegistry()
	registry.Seal()
	factory := RuntimeAgentFactoryFunc(func(_ context.Context, spec RuntimeAgentBuildSpec, _ RuntimeAgentDependencies) (agent.Agent, error) {
		return &factoryTestAgent{name: spec.Agent.Name}, nil
	})
	if err := registry.RegisterWithCapability(tenant.AgentTypeGraph, "graph@replacement", factory); !errors.Is(err, ErrRuntimeRegistrySealed) {
		t.Fatalf("sealed registry replacement error=%v, want ErrRuntimeRegistrySealed", err)
	}
	if !registry.IsSealed() {
		t.Fatal("registry lost sealed state")
	}
}

func TestRuntimeAgentRegistryKeepsLegacyLLMOnlyFingerprintCompatible(t *testing.T) {
	registry := NewRuntimeAgentRegistry()
	legacy := legacyLLMOnlyFingerprint()
	if !registry.MatchesFingerprint(tenant.AgentTypeLLM, legacy) {
		t.Fatal("historical LLM-only version fingerprint was rejected")
	}
	if registry.MatchesFingerprint(tenant.AgentTypeGraph, legacy) {
		t.Fatal("historical LLM-only fingerprint admitted a composite runtime")
	}
}

func TestRuntimeAgentRegistryStrictValidationRequiresCustomCapability(t *testing.T) {
	registry := NewRuntimeAgentRegistry()
	factory := RuntimeAgentFactoryFunc(func(_ context.Context, spec RuntimeAgentBuildSpec, _ RuntimeAgentDependencies) (agent.Agent, error) {
		return &factoryTestAgent{name: spec.Agent.Name}, nil
	})
	if err := registry.Register(tenant.AgentTypeGraph, factory); err != nil {
		t.Fatal(err)
	}
	if err := registry.ValidateRuntimeType(tenant.AgentTypeGraph); err != nil {
		t.Fatalf("legacy registration should remain compatible: %v", err)
	}
	if err := registry.ValidateRuntimeTypeStrict(tenant.AgentTypeGraph); !errors.Is(err, ErrRuntimeCapabilityRequired) {
		t.Fatalf("strict legacy registration error=%v, want ErrRuntimeCapabilityRequired", err)
	}
	if err := registry.RegisterWithCapability(tenant.AgentTypeGraph, "graph@build-1", factory); err != nil {
		t.Fatal(err)
	}
	if err := registry.ValidateRuntimeTypeStrict(tenant.AgentTypeGraph); err != nil {
		t.Fatalf("capability registration rejected: %v", err)
	}
	if err := registry.ValidateRuntimeTypeStrict(tenant.AgentTypeLLM); err != nil {
		t.Fatalf("built-in llm strict validation failed: %v", err)
	}
}

func TestRuntimeAgentRegistryBuildsRegisteredRuntime(t *testing.T) {
	registry := NewRuntimeAgentRegistry()
	called := false
	err := registry.Register(tenant.AgentTypeGraph, RuntimeAgentFactoryFunc(func(_ context.Context, spec RuntimeAgentBuildSpec, _ RuntimeAgentDependencies) (agent.Agent, error) {
		called = true
		return &factoryTestAgent{name: spec.Agent.Name}, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	built, err := buildRuntimeAgent(context.Background(), RuntimeAgentBuildSpec{
		Agent: tenant.AgentConfig{Name: "support", Type: tenant.AgentTypeGraph},
	}, RuntimeAgentDependencies{}, registry)
	if err != nil {
		t.Fatalf("registered graph runtime failed: %v", err)
	}
	if !called || built.Info().Name != "support" {
		t.Fatalf("factory was not used correctly: called=%v agent=%q", called, built.Info().Name)
	}
}

func TestRuntimeAgentRegistryRejectsFactoryNameMismatch(t *testing.T) {
	registry := NewRuntimeAgentRegistry()
	if err := registry.Register(tenant.AgentTypeChain, RuntimeAgentFactoryFunc(func(context.Context, RuntimeAgentBuildSpec, RuntimeAgentDependencies) (agent.Agent, error) {
		return &factoryTestAgent{name: "other"}, nil
	})); err != nil {
		t.Fatal(err)
	}
	_, err := buildRuntimeAgent(context.Background(), RuntimeAgentBuildSpec{
		Agent: tenant.AgentConfig{Name: "support", Type: tenant.AgentTypeChain},
	}, RuntimeAgentDependencies{}, registry)
	if !errors.Is(err, ErrAgentFactoryInvalid) {
		t.Fatalf("name mismatch error=%v, want ErrAgentFactoryInvalid", err)
	}
}

func TestRuntimeAgentRegistryRejectsMalformedExplicitTypeWithoutFallback(t *testing.T) {
	_, err := buildRuntimeAgent(context.Background(), RuntimeAgentBuildSpec{
		Agent: tenant.AgentConfig{Name: "support", Type: "graph\n"},
	}, RuntimeAgentDependencies{}, NewRuntimeAgentRegistry())
	if !errors.Is(err, ErrAgentFactoryInvalid) {
		t.Fatalf("malformed runtime type error=%v, want ErrAgentFactoryInvalid", err)
	}
}

func TestRuntimeAgentRegistryConvertsFactoryPanicToError(t *testing.T) {
	registry := NewRuntimeAgentRegistry()
	if err := registry.Register(tenant.AgentTypeChain, RuntimeAgentFactoryFunc(func(context.Context, RuntimeAgentBuildSpec, RuntimeAgentDependencies) (agent.Agent, error) {
		panic("provider bug")
	})); err != nil {
		t.Fatal(err)
	}
	_, err := buildRuntimeAgent(context.Background(), RuntimeAgentBuildSpec{
		Agent: tenant.AgentConfig{Name: "support", Type: tenant.AgentTypeChain},
	}, RuntimeAgentDependencies{}, registry)
	if !errors.Is(err, ErrAgentFactoryInvalid) {
		t.Fatalf("factory panic error=%v, want ErrAgentFactoryInvalid", err)
	}
}

func TestRuntimeAgentRegistryConvertsInfoPanicToError(t *testing.T) {
	registry := NewRuntimeAgentRegistry()
	if err := registry.Register(tenant.AgentTypeChain, RuntimeAgentFactoryFunc(func(context.Context, RuntimeAgentBuildSpec, RuntimeAgentDependencies) (agent.Agent, error) {
		return panicInfoTestAgent{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	_, err := buildRuntimeAgent(context.Background(), RuntimeAgentBuildSpec{
		Agent: tenant.AgentConfig{Name: "support", Type: tenant.AgentTypeChain},
	}, RuntimeAgentDependencies{}, registry)
	if !errors.Is(err, ErrAgentFactoryInvalid) {
		t.Fatalf("Info panic error=%v, want ErrAgentFactoryInvalid", err)
	}
}

func TestRuntimeAgentRegistryDoesNotExposeMutableBuildSnapshot(t *testing.T) {
	originalTenant := &tenant.Tenant{
		ID:     "tenant-a",
		Agents: []tenant.AgentConfig{{Name: "support", Tools: []string{"search"}}},
	}
	spec := RuntimeAgentBuildSpec{
		Tenant: originalTenant,
		Agent: tenant.AgentConfig{
			Name: "support", Type: tenant.AgentTypeChain, Tools: []string{"search"},
			Runtime: &tenant.AgentRuntimeConfig{Nodes: []tenant.AgentRuntimeNode{{Name: "step", Tools: []string{"search"}}}},
		},
		Tools: []tool.Tool{snapshotTestTool{name: "search"}},
	}
	registry := NewRuntimeAgentRegistry()
	if err := registry.Register(tenant.AgentTypeChain, RuntimeAgentFactoryFunc(func(_ context.Context, received RuntimeAgentBuildSpec, _ RuntimeAgentDependencies) (agent.Agent, error) {
		received.Agent.Tools[0] = "mutated-agent"
		received.Agent.Runtime.Nodes[0].Name = "mutated-node"
		received.Agent.Runtime.Nodes[0].Tools[0] = "mutated-node-tool"
		received.Tenant.Agents[0].Name = "mutated-tenant"
		received.Tools[0] = nil
		return &factoryTestAgent{name: received.Agent.Name}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := buildRuntimeAgent(context.Background(), spec, RuntimeAgentDependencies{}, registry); err != nil {
		t.Fatalf("build failed: %v", err)
	}
	if spec.Agent.Tools[0] != "search" || spec.Agent.Runtime.Nodes[0].Name != "step" ||
		spec.Agent.Runtime.Nodes[0].Tools[0] != "search" || originalTenant.Agents[0].Name != "support" || spec.Tools[0] == nil {
		t.Fatalf("factory mutated caller-owned build snapshot: spec=%#v tenant=%#v", spec, originalTenant)
	}
}
