package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/chainagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

var (
	// ErrAgentFactory reports an invalid or unsupported Agent construction
	// request. The registry deliberately does not fall back to LLMAgent for an
	// unknown kind.
	ErrAgentFactory = errors.New("agent factory error")
	// ErrAgentFactoryNotFound reports a kind/schema pair without a registered
	// constructor.
	ErrAgentFactoryNotFound = errors.New("agent factory not registered")
)

// AgentBuildInput is the materialized dependency boundary for one Agent
// Factory call. Definition contains only published, secret-free control-plane
// data; Model and Tools are already resolved for the same tenant snapshot.
// ModelOptions are shared callback/options hooks and are applied to every LLM
// node created by a composite factory.
type AgentBuildInput struct {
	Definition   LLMAgentFactoryInput
	Model        trpcmodel.Model
	Tools        []trpctool.Tool
	ModelOptions []llmagent.Option
}

// AgentFactory builds one concrete tRPC-Agent-Go Agent from a published
// definition and its materialized runtime dependencies.
type AgentFactory func(context.Context, AgentBuildInput) (trpcagent.Agent, error)

type agentFactoryKey struct {
	kind          appmodel.Kind
	schemaVersion int
}

// AgentFactoryRegistry maps versioned domain kinds to concrete constructors.
// It is safe to share a registry between Runner builds after registration.
type AgentFactoryRegistry struct {
	mu        sync.RWMutex
	factories map[agentFactoryKey]AgentFactory
}

// NewAgentFactoryRegistry creates a registry and registers the supplied
// versioned constructors.
func NewAgentFactoryRegistry(registrations ...AgentFactoryRegistration) (*AgentFactoryRegistry, error) {
	registry := &AgentFactoryRegistry{factories: make(map[agentFactoryKey]AgentFactory, len(registrations))}
	for _, registration := range registrations {
		if err := registry.Register(registration.Kind, registration.SchemaVersion, registration.Factory); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

// AgentFactoryRegistration binds one domain kind and schema version to a
// constructor.
type AgentFactoryRegistration struct {
	Kind          appmodel.Kind
	SchemaVersion int
	Factory       AgentFactory
}

// Register adds one constructor. Duplicate registrations are rejected so a
// process cannot silently change the meaning of a published revision.
func (registry *AgentFactoryRegistry) Register(kind appmodel.Kind, schemaVersion int, factory AgentFactory) error {
	if registry == nil {
		return fmt.Errorf("%w: registry is required", ErrAgentFactory)
	}
	if kind == "" || schemaVersion < 1 || factory == nil {
		return fmt.Errorf("%w: kind, schema version, and factory are required", ErrAgentFactory)
	}
	key := agentFactoryKey{kind: kind, schemaVersion: schemaVersion}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.factories == nil {
		registry.factories = make(map[agentFactoryKey]AgentFactory)
	}
	if _, exists := registry.factories[key]; exists {
		return fmt.Errorf("%w: duplicate factory for %q schema %d", ErrAgentFactory, kind, schemaVersion)
	}
	registry.factories[key] = factory
	return nil
}

// Build resolves and invokes the constructor for one published definition.
func (registry *AgentFactoryRegistry) Build(ctx context.Context, input AgentBuildInput) (trpcagent.Agent, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrAgentFactory)
	}
	if input.Definition.Name == "" || input.Definition.Kind == "" || input.Definition.SchemaVersion < 1 {
		return nil, fmt.Errorf("%w: complete Agent definition is required", ErrAgentFactory)
	}
	if input.Model == nil {
		return nil, fmt.Errorf("%w: model is required", ErrAgentFactory)
	}
	if registry == nil {
		return nil, ErrAgentFactoryNotFound
	}
	key := agentFactoryKey{kind: input.Definition.Kind, schemaVersion: input.Definition.SchemaVersion}
	registry.mu.RLock()
	factory := registry.factories[key]
	registry.mu.RUnlock()
	if factory == nil {
		return nil, fmt.Errorf("%w: kind %q schema %d", ErrAgentFactoryNotFound, key.kind, key.schemaVersion)
	}
	built, err := factory(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("%w: build %q: %w", ErrAgentFactory, key.kind, err)
	}
	if built == nil {
		return nil, fmt.Errorf("%w: build %q returned nil Agent", ErrAgentFactory, key.kind)
	}
	return built, nil
}

// DefaultAgentFactoryRegistry returns the built-in Agent kinds supported by
// schema version 1. The registry is fresh so callers may safely extend it
// with tenant-independent platform kinds without mutating global state.
func DefaultAgentFactoryRegistry() *AgentFactoryRegistry {
	registry, _ := NewAgentFactoryRegistry(
		AgentFactoryRegistration{Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Factory: buildLLMAgent},
		AgentFactoryRegistration{Kind: appmodel.KindChain, SchemaVersion: appmodel.SchemaVersionV1, Factory: buildChainAgent},
	)
	return registry
}

func buildLLMAgent(_ context.Context, input AgentBuildInput) (trpcagent.Agent, error) {
	options := llmAgentOptions(input.Definition, input.Model, input.Tools)
	options = append(options, input.ModelOptions...)
	return llmagent.New(input.Definition.Name, options...), nil
}

func buildChainAgent(_ context.Context, input AgentBuildInput) (trpcagent.Agent, error) {
	configuration := input.Definition.Chain
	if configuration == nil || len(configuration.Steps) < 2 {
		return nil, fmt.Errorf("%w: chain requires at least two steps", ErrAgentFactory)
	}
	children := make([]trpcagent.Agent, 0, len(configuration.Steps))
	seen := make(map[string]struct{}, len(configuration.Steps))
	for index, step := range configuration.Steps {
		if step.Name == "" || step.Instruction == "" {
			return nil, fmt.Errorf("%w: chain step %d requires name and instruction", ErrAgentFactory, index)
		}
		if _, exists := seen[step.Name]; exists {
			return nil, fmt.Errorf("%w: duplicate chain step %q", ErrAgentFactory, step.Name)
		}
		seen[step.Name] = struct{}{}
		stepDefinition := input.Definition.Clone()
		stepDefinition.Name = step.Name
		stepDefinition.Instruction = step.Instruction
		stepDefinition.GlobalInstruction = step.GlobalInstruction
		stepDefinition.Chain = nil
		options := llmAgentOptions(stepDefinition, input.Model, input.Tools)
		options = append(options, input.ModelOptions...)
		children = append(children, llmagent.New(step.Name, options...))
	}
	return chainagent.New(input.Definition.Name, chainagent.WithSubAgents(children)), nil
}
