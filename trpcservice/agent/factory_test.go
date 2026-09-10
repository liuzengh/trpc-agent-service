package agent

import (
	"context"
	"errors"
	"testing"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
)

func TestDefaultAgentFactoryRegistryBuildsLLMAndChain(t *testing.T) {
	registry := DefaultAgentFactoryRegistry()
	model := agentTestModel{}

	llm, err := registry.Build(context.Background(), AgentBuildInput{
		Definition: LLMAgentFactoryInput{Name: "assistant", Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Instruction: "Answer clearly."},
		Model:      model,
	})
	if err != nil {
		t.Fatal(err)
	}
	if llm.Info().Name != "assistant" || len(llm.SubAgents()) != 0 {
		t.Fatalf("LLM Agent info = %+v, sub-agents = %d", llm.Info(), len(llm.SubAgents()))
	}

	chain, err := registry.Build(context.Background(), AgentBuildInput{
		Definition: LLMAgentFactoryInput{
			Name: "support-pipeline", Kind: appmodel.KindChain, SchemaVersion: appmodel.SchemaVersionV1,
			Description: "A sequential support pipeline", Instruction: "Run the pipeline.",
			Runtime: appmodel.DefaultRuntimePolicy(),
			Chain: &appmodel.ChainConfiguration{Steps: []appmodel.ChainStep{
				{Name: "classify", Instruction: "Classify the request."},
				{Name: "answer", Instruction: "Draft the final answer."},
			}},
		},
		Model: model,
	})
	if err != nil {
		t.Fatal(err)
	}
	if chain.Info().Name != "support-pipeline" {
		t.Fatalf("Chain info = %+v", chain.Info())
	}
	children := chain.SubAgents()
	if len(children) != 2 {
		t.Fatalf("Chain children = %d, want 2", len(children))
	}
	if children[0].Info().Name != "classify" || children[1].Info().Name != "answer" {
		t.Fatalf("Chain child names = %q, %q", children[0].Info().Name, children[1].Info().Name)
	}
	if chain.FindSubAgent("answer") == nil || chain.FindSubAgent("missing") != nil {
		t.Fatal("Chain child lookup did not preserve the published step names")
	}
}

func TestAgentFactoryRegistryRejectsDuplicateAndUnknownFactories(t *testing.T) {
	factory := func(context.Context, AgentBuildInput) (trpcagent.Agent, error) {
		return llmagent.New("custom"), nil
	}
	registry, err := NewAgentFactoryRegistry(AgentFactoryRegistration{Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Factory: factory})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(appmodel.KindLLM, appmodel.SchemaVersionV1, factory); !errors.Is(err, ErrAgentFactory) {
		t.Fatalf("duplicate registration error = %v", err)
	}
	_, err = registry.Build(context.Background(), AgentBuildInput{
		Definition: LLMAgentFactoryInput{Name: "unsupported", Kind: appmodel.KindChain, SchemaVersion: appmodel.SchemaVersionV1},
		Model:      agentTestModel{},
	})
	if !errors.Is(err, ErrAgentFactoryNotFound) {
		t.Fatalf("unknown factory error = %v", err)
	}
}

func TestAgentFactoryRegistryValidatesInputsAndConstructorResults(t *testing.T) {
	var nilRegistry *AgentFactoryRegistry
	factory := func(context.Context, AgentBuildInput) (trpcagent.Agent, error) { return llmagent.New("built"), nil }
	for _, test := range []struct {
		name string
		call func() error
	}{
		{"nil registry registration", func() error { return nilRegistry.Register(appmodel.KindLLM, 1, factory) }},
		{"empty kind", func() error { return (&AgentFactoryRegistry{}).Register("", 1, factory) }},
		{"invalid schema", func() error { return (&AgentFactoryRegistry{}).Register(appmodel.KindLLM, 0, factory) }},
		{"nil factory", func() error { return (&AgentFactoryRegistry{}).Register(appmodel.KindLLM, 1, nil) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := test.call()
			if !errors.Is(err, ErrAgentFactory) {
				t.Fatalf("error = %v", err)
			}
		})
	}
	if _, err := NewAgentFactoryRegistry(
		AgentFactoryRegistration{Kind: appmodel.KindLLM, SchemaVersion: 1, Factory: factory},
		AgentFactoryRegistration{Kind: appmodel.KindLLM, SchemaVersion: 1, Factory: factory},
	); !errors.Is(err, ErrAgentFactory) {
		t.Fatalf("duplicate constructor registration error = %v", err)
	}
	if err := (&AgentFactoryRegistry{}).Register(appmodel.KindLLM, 1, factory); err != nil {
		t.Fatal(err)
	}
	registry, err := NewAgentFactoryRegistry()
	if err != nil {
		t.Fatal(err)
	}
	valid := AgentBuildInput{Definition: LLMAgentFactoryInput{Name: "agent", Kind: appmodel.KindLLM, SchemaVersion: 1}, Model: agentTestModel{}}
	defaultRegistry := DefaultAgentFactoryRegistry()
	for _, input := range []AgentBuildInput{{}, valid} {
		if input.Definition.Name == "agent" {
			input.Model = nil
		}
		if _, err := defaultRegistry.Build(context.Background(), input); !errors.Is(err, ErrAgentFactory) {
			t.Fatalf("invalid build error = %v", err)
		}
	}
	if _, err := registry.Build(nil, valid); !errors.Is(err, ErrAgentFactory) {
		t.Fatalf("nil context error = %v", err)
	}
	constructorErr := errors.New("constructor failed")
	registry, err = NewAgentFactoryRegistry(AgentFactoryRegistration{Kind: appmodel.KindLLM, SchemaVersion: 1, Factory: func(context.Context, AgentBuildInput) (trpcagent.Agent, error) { return nil, constructorErr }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Build(context.Background(), valid); !errors.Is(err, constructorErr) {
		t.Fatalf("constructor error = %v", err)
	}
	registry, err = NewAgentFactoryRegistry(AgentFactoryRegistration{Kind: appmodel.KindLLM, SchemaVersion: 1, Factory: func(context.Context, AgentBuildInput) (trpcagent.Agent, error) { return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Build(context.Background(), valid); !errors.Is(err, ErrAgentFactory) {
		t.Fatalf("nil agent error = %v", err)
	}
}

func TestChainAgentFactoryRejectsMalformedSteps(t *testing.T) {
	registry := DefaultAgentFactoryRegistry()
	base := AgentBuildInput{Definition: LLMAgentFactoryInput{Name: "chain", Kind: appmodel.KindChain, SchemaVersion: 1}, Model: agentTestModel{}}
	for _, steps := range [][]appmodel.ChainStep{
		nil,
		{{Name: "only", Instruction: "run"}},
		{{Name: "step", Instruction: ""}, {Name: "other", Instruction: "run"}},
		{{Name: "same", Instruction: "run"}, {Name: "same", Instruction: "again"}},
	} {
		input := base
		input.Definition.Chain = &appmodel.ChainConfiguration{Steps: steps}
		if _, err := registry.Build(context.Background(), input); !errors.Is(err, ErrAgentFactory) {
			t.Fatalf("malformed chain error = %v", err)
		}
	}
}
