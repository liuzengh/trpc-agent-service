// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/chainagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/cycleagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/graphagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/agent/parallelagent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	trpcgraph "trpc.group/trpc-go/trpc-agent-go/graph"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

var (
	// ErrAgentFactoryUnavailable means the tenant selected a runtime type for
	// which the operator has not installed a concrete factory.
	ErrAgentFactoryUnavailable = errors.New("agent runtime factory unavailable")
	// ErrAgentFactoryInvalid means a factory returned an unusable agent or was
	// registered under an invalid runtime type.
	ErrAgentFactoryInvalid = errors.New("agent runtime factory invalid")
	// ErrRuntimeCapabilityMismatch means an immutable version was published
	// against a different installed runtime capability set than this Worker.
	ErrRuntimeCapabilityMismatch = errors.New("agent runtime capability mismatch")
	// ErrRuntimeCapabilityRequired means a non-built-in runtime was registered
	// through the legacy type-only API and cannot enter a strict production
	// composition without an operator-owned implementation/build identity.
	ErrRuntimeCapabilityRequired = errors.New("agent runtime capability identity required")
	// ErrRuntimeRegistrySealed means the process has finalized its runtime
	// composition and no implementation may be replaced in-place.
	ErrRuntimeRegistrySealed = errors.New("agent runtime registry sealed")
)

// RuntimeAgentBuildSpec is the immutable control-plane snapshot handed to a
// runtime factory. Factories must treat Tenant and Agent as read-only.
type RuntimeAgentBuildSpec struct {
	Tenant      *tenant.Tenant
	Agent       tenant.AgentConfig
	Model       tenant.ModelConfig
	ModelObject model.Model
	Tools       []tool.Tool
}

// RuntimeAgentDependencies contains platform-owned services and the options
// assembled for the built-in LLMAgent runtime. A custom factory may consume
// only the fields it supports, but it must not silently downgrade a different
// runtime type to LLMAgent.
type RuntimeAgentDependencies struct {
	MemoryService   memory.Service
	Knowledge       knowledge.Knowledge
	Artifact        artifact.Service
	Collector       *telemetry.Collector
	LLMOptions      []llmagent.Option
	LLMAgentBuilder RuntimeLLMAgentBuilder
}

// RuntimeLLMAgentBuildSpec describes one leaf in a composite runtime. ToolNames
// are already constrained to the immutable parent AgentVersion capability.
type RuntimeLLMAgentBuildSpec struct {
	Name         string
	SystemPrompt string
	MaxLLMCalls  int
	ToolNames    []string
}

// RuntimeLLMAgentBuilder constructs an LLMAgent leaf using Worker-owned model,
// storage, knowledge and tool dependencies.
type RuntimeLLMAgentBuilder interface {
	Build(context.Context, RuntimeLLMAgentBuildSpec) (agent.Agent, error)
}

// RuntimeLLMAgentBuilderFunc adapts a function to RuntimeLLMAgentBuilder.
type RuntimeLLMAgentBuilderFunc func(context.Context, RuntimeLLMAgentBuildSpec) (agent.Agent, error)

func (f RuntimeLLMAgentBuilderFunc) Build(ctx context.Context, spec RuntimeLLMAgentBuildSpec) (agent.Agent, error) {
	if f == nil {
		return nil, ErrAgentFactoryInvalid
	}
	return f(ctx, spec)
}

// RuntimeAgentFactory builds one concrete agent runtime.
type RuntimeAgentFactory interface {
	Build(context.Context, RuntimeAgentBuildSpec, RuntimeAgentDependencies) (agent.Agent, error)
}

// RuntimeAgentFactoryFunc adapts a function to RuntimeAgentFactory.
type RuntimeAgentFactoryFunc func(context.Context, RuntimeAgentBuildSpec, RuntimeAgentDependencies) (agent.Agent, error)

func (f RuntimeAgentFactoryFunc) Build(ctx context.Context, spec RuntimeAgentBuildSpec, deps RuntimeAgentDependencies) (agent.Agent, error) {
	if f == nil {
		return nil, ErrAgentFactoryInvalid
	}
	return f(ctx, spec, deps)
}

// RuntimeAgentRegistry is a concurrency-safe registry controlled by the
// operator. Registration is explicit so tenant configuration cannot construct
// arbitrary Go code from user-supplied strings.
type RuntimeAgentRegistry struct {
	mu           sync.RWMutex
	factories    map[string]RuntimeAgentFactory
	capabilities map[string]string
	sealed       bool
}

const (
	builtinChainCapability    = "builtin-chain/trpc-agent-go-v1.11.2/enterprise-v1"
	builtinGraphCapability    = "builtin-graph/trpc-agent-go-v1.11.2/enterprise-v1"
	builtinParallelCapability = "builtin-parallel/trpc-agent-go-v1.11.2/enterprise-v1"
	builtinCycleCapability    = "builtin-cycle/trpc-agent-go-v1.11.2/enterprise-v1"
)

// NewRuntimeAgentRegistry creates a registry with all upstream runtime types
// installed. Composite runtimes carry explicit build identities so Admin and
// Worker still fail closed when their implementations drift.
func NewRuntimeAgentRegistry() *RuntimeAgentRegistry {
	registry := &RuntimeAgentRegistry{
		factories:    make(map[string]RuntimeAgentFactory),
		capabilities: make(map[string]string),
	}
	// This registration is immutable in intent but remains replaceable for
	// tests and operator-provided upgrades.
	_ = registry.Register(tenant.AgentTypeLLM, llmRuntimeAgentFactory{})
	_ = registry.RegisterWithCapability(tenant.AgentTypeChain, builtinChainCapability, compositeRuntimeAgentFactory{runtimeType: tenant.AgentTypeChain})
	_ = registry.RegisterWithCapability(tenant.AgentTypeGraph, builtinGraphCapability, compositeRuntimeAgentFactory{runtimeType: tenant.AgentTypeGraph})
	_ = registry.RegisterWithCapability(tenant.AgentTypeParallel, builtinParallelCapability, compositeRuntimeAgentFactory{runtimeType: tenant.AgentTypeParallel})
	_ = registry.RegisterWithCapability(tenant.AgentTypeCycle, builtinCycleCapability, compositeRuntimeAgentFactory{runtimeType: tenant.AgentTypeCycle})
	return registry
}

// Register installs or replaces a runtime factory under a validated type.
// The compatibility form uses the runtime type as its capability identity.
// Operators shipping a custom implementation should use RegisterWithCapability
// and include an immutable implementation/build revision.
func (r *RuntimeAgentRegistry) Register(runtimeType string, factory RuntimeAgentFactory) error {
	return r.register(runtimeType, runtimeType, factory, true)
}

// RegisterWithCapability installs a runtime factory and an operator-owned,
// immutable capability identity. The identity is included in the registry
// fingerprint, allowing Admin and Worker to reject same-name implementations
// built from different revisions. It must be stable across process restarts
// and must not contain credentials or host-specific data.
func (r *RuntimeAgentRegistry) RegisterWithCapability(runtimeType, capability string, factory RuntimeAgentFactory) error {
	return r.register(runtimeType, capability, factory, false)
}

// Seal finalizes the operator-owned runtime composition. A sealed registry is
// immutable for the lifetime of the process, so an already-published version
// cannot silently execute a different factory under the same runtime type.
func (r *RuntimeAgentRegistry) Seal() {
	if r == nil {
		return
	}
	r.mu.Lock()
	r.sealed = true
	r.mu.Unlock()
}

// IsSealed reports whether registration is closed.
func (r *RuntimeAgentRegistry) IsSealed() bool {
	if r == nil {
		return true
	}
	r.mu.RLock()
	sealed := r.sealed
	r.mu.RUnlock()
	return sealed
}

func (r *RuntimeAgentRegistry) register(runtimeType, capability string, factory RuntimeAgentFactory, legacy bool) error {
	if r == nil {
		return ErrAgentFactoryInvalid
	}
	runtimeType = normalizeRuntimeType(runtimeType)
	capability = normalizeRuntimeCapability(capability)
	if runtimeType == "" || capability == "" || factory == nil || isNilInterface(factory) || (!legacy && capability == runtimeType) {
		return ErrAgentFactoryInvalid
	}
	r.mu.Lock()
	if r.sealed {
		r.mu.Unlock()
		return ErrRuntimeRegistrySealed
	}
	if r.factories == nil {
		r.factories = make(map[string]RuntimeAgentFactory)
	}
	if r.capabilities == nil {
		r.capabilities = make(map[string]string)
	}
	r.factories[runtimeType] = factory
	r.capabilities[runtimeType] = capability
	r.mu.Unlock()
	return nil
}

// Factory returns a snapshot of the registered factory for runtimeType.
func (r *RuntimeAgentRegistry) Factory(runtimeType string) (RuntimeAgentFactory, bool) {
	if r == nil {
		return nil, false
	}
	runtimeType = normalizeRuntimeType(runtimeType)
	r.mu.RLock()
	factory, ok := r.factories[runtimeType]
	r.mu.RUnlock()
	return factory, ok
}

// Fingerprint returns a stable digest of the runtime capabilities installed in
// this process. It is recorded in newly published immutable version snapshots
// so Admin and Worker composition cannot silently drift across deployments.
// The digest excludes function pointers and process-specific details. Custom
// factories must use RegisterWithCapability and bump their capability identity
// when the implementation or security-relevant build changes.
func (r *RuntimeAgentRegistry) Fingerprint() string {
	if r == nil {
		r = NewRuntimeAgentRegistry()
	}
	r.mu.RLock()
	types := make([]string, 0, len(r.factories))
	legacyCapabilities := true
	for runtimeType := range r.factories {
		capability := r.capabilities[runtimeType]
		if capability == "" {
			capability = runtimeType
		}
		if capability != runtimeType {
			legacyCapabilities = false
		}
		types = append(types, runtimeType+"\x00"+capability)
	}
	r.mu.RUnlock()
	sort.Strings(types)
	prefix := "trpc-agent-runtime/v2\x00"
	if legacyCapabilities {
		// Keep fingerprints of historical built-in-only snapshots valid during a
		// rolling upgrade. Explicit custom capabilities use the v2 encoding.
		legacyTypes := make([]string, 0, len(types))
		for _, entry := range types {
			parts := strings.SplitN(entry, "\x00", 2)
			legacyTypes = append(legacyTypes, parts[0])
		}
		digest := sha256.Sum256([]byte("trpc-agent-runtime/v1\x00" + strings.Join(legacyTypes, "\x00")))
		return hex.EncodeToString(digest[:])
	}
	digest := sha256.Sum256([]byte(prefix + strings.Join(types, "\x00")))
	return hex.EncodeToString(digest[:])
}

// ValidateRuntimeType verifies that a selected runtime is both syntactically
// valid and backed by an operator-installed factory. A nil registry carries
// the same "built-in runtimes only" meaning as buildRuntimeAgent, which keeps
// control-plane admission and execution from drifting apart.
func (r *RuntimeAgentRegistry) ValidateRuntimeType(runtimeType string) error {
	_, _, err := r.runtimeFactory(runtimeType)
	return err
}

// ValidateRuntimeTypeStrict applies the production admission contract. The
// bundled llm runtime retains its historical type-only identity, while every
// other runtime must have been registered with RegisterWithCapability.
// Compatibility constructors intentionally continue to use ValidateRuntimeType
// so older in-process callers are not broken by this stronger boundary.
func (r *RuntimeAgentRegistry) ValidateRuntimeTypeStrict(runtimeType string) error {
	selected, _, err := r.runtimeFactory(runtimeType)
	if err != nil {
		return err
	}
	if selected == tenant.AgentTypeLLM {
		return nil
	}
	if r == nil {
		return fmt.Errorf("%w: type=%s", ErrRuntimeCapabilityRequired, selected)
	}
	r.mu.RLock()
	capability := r.capabilities[selected]
	r.mu.RUnlock()
	if capability == "" || capability == selected {
		return fmt.Errorf("%w: type=%s", ErrRuntimeCapabilityRequired, selected)
	}
	return nil
}

// MatchesFingerprint accepts the current capability set and the historical
// built-in-LLM-only fingerprint for LLM versions published before composite
// runtimes were bundled. Non-LLM versions always require the exact current set.
func (r *RuntimeAgentRegistry) MatchesFingerprint(runtimeType, published string) bool {
	if published == "" {
		return true
	}
	if r == nil {
		r = NewRuntimeAgentRegistry()
	}
	if published == r.Fingerprint() {
		return true
	}
	selected, err := selectedRuntimeType(runtimeType)
	return err == nil && selected == tenant.AgentTypeLLM && published == legacyLLMOnlyFingerprint()
}

func legacyLLMOnlyFingerprint() string {
	digest := sha256.Sum256([]byte("trpc-agent-runtime/v1\x00" + tenant.AgentTypeLLM))
	return hex.EncodeToString(digest[:])
}

func normalizeRuntimeType(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" || !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
		return ""
	}
	return value
}

func normalizeRuntimeCapability(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 || !utf8.ValidString(value) {
		return ""
	}
	for _, r := range value {
		if r == '\x00' || r == '\r' || r == '\n' || r == '\t' || r < 0x20 || r == 0x7f {
			return ""
		}
	}
	return value
}

func selectedRuntimeType(value string) (string, error) {
	if value == "" {
		return "llm", nil
	}
	if !utf8.ValidString(value) || strings.ContainsAny(value, "\x00\r\n") {
		return "", fmt.Errorf("%w: runtime type is invalid", ErrAgentFactoryInvalid)
	}
	runtimeType := normalizeRuntimeType(value)
	if runtimeType == "" {
		// An explicitly supplied malformed type must not silently select the
		// default runtime. That would turn a control-plane validation failure
		// into execution with a different agent implementation.
		return "", fmt.Errorf("%w: runtime type is invalid", ErrAgentFactoryInvalid)
	}
	return runtimeType, nil
}

func (r *RuntimeAgentRegistry) runtimeFactory(configuredType string) (string, RuntimeAgentFactory, error) {
	runtimeType, err := selectedRuntimeType(configuredType)
	if err != nil {
		return "", nil, err
	}
	if r == nil {
		r = NewRuntimeAgentRegistry()
	}
	factory, ok := r.Factory(runtimeType)
	if !ok {
		return "", nil, fmt.Errorf("%w: type=%s", ErrAgentFactoryUnavailable, runtimeType)
	}
	return runtimeType, factory, nil
}

type llmRuntimeAgentFactory struct{}

func (llmRuntimeAgentFactory) Build(_ context.Context, spec RuntimeAgentBuildSpec, deps RuntimeAgentDependencies) (agent.Agent, error) {
	if spec.Agent.Name == "" || isNilInterface(spec.ModelObject) {
		return nil, fmt.Errorf("%w: llm runtime requires agent name and model", ErrAgentFactoryInvalid)
	}
	options := append([]llmagent.Option(nil), deps.LLMOptions...)
	return llmagent.New(spec.Agent.Name, options...), nil
}

type compositeRuntimeAgentFactory struct {
	runtimeType string
}

func (f compositeRuntimeAgentFactory) Build(
	ctx context.Context,
	spec RuntimeAgentBuildSpec,
	deps RuntimeAgentDependencies,
) (agent.Agent, error) {
	if f.runtimeType == "" || normalizeRuntimeType(spec.Agent.Type) != f.runtimeType {
		return nil, fmt.Errorf("%w: composite runtime type mismatch", ErrAgentFactoryInvalid)
	}
	if err := tenant.ValidateAgentRuntime(spec.Agent); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAgentFactoryInvalid, err)
	}
	children, err := buildRuntimeLeafAgents(ctx, spec, deps)
	if err != nil {
		return nil, err
	}
	runtime := spec.Agent.Runtime
	switch f.runtimeType {
	case tenant.AgentTypeChain:
		return chainagent.New(spec.Agent.Name, chainagent.WithSubAgents(children)), nil
	case tenant.AgentTypeParallel:
		return parallelagent.New(spec.Agent.Name, parallelagent.WithSubAgents(children)), nil
	case tenant.AgentTypeCycle:
		return cycleagent.New(spec.Agent.Name,
			cycleagent.WithSubAgents(children),
			cycleagent.WithMaxIterations(runtime.MaxIterations),
		), nil
	case tenant.AgentTypeGraph:
		return buildGraphRuntime(spec.Agent.Name, runtime, children)
	default:
		return nil, fmt.Errorf("%w: unsupported composite runtime %q", ErrAgentFactoryInvalid, f.runtimeType)
	}
}

func buildRuntimeLeafAgents(
	ctx context.Context,
	spec RuntimeAgentBuildSpec,
	deps RuntimeAgentDependencies,
) ([]agent.Agent, error) {
	if deps.LLMAgentBuilder == nil || isNilInterface(deps.LLMAgentBuilder) {
		return nil, fmt.Errorf("%w: composite runtime requires an LLM leaf builder", ErrAgentFactoryInvalid)
	}
	children := make([]agent.Agent, 0, len(spec.Agent.Runtime.Nodes))
	for _, node := range spec.Agent.Runtime.Nodes {
		instruction := strings.TrimSpace(spec.Agent.SystemPrompt)
		if nodePrompt := strings.TrimSpace(node.SystemPrompt); nodePrompt != "" {
			if instruction != "" {
				instruction += "\n\n"
			}
			instruction += nodePrompt
		}
		child, err := safeBuildRuntimeLeafAgent(ctx, deps.LLMAgentBuilder, RuntimeLLMAgentBuildSpec{
			Name:         node.Name,
			SystemPrompt: instruction,
			MaxLLMCalls:  node.EffectiveMaxLLMCalls(),
			ToolNames:    append([]string(nil), node.Tools...),
		})
		if err != nil {
			return nil, fmt.Errorf("build runtime node %q: %w", node.Name, err)
		}
		if isNilInterface(child) {
			return nil, fmt.Errorf("%w: runtime node %q builder returned nil", ErrAgentFactoryInvalid, node.Name)
		}
		info, err := safeAgentInfo(child)
		if err != nil || info.Name != node.Name {
			return nil, fmt.Errorf("%w: runtime node %q returned agent name %q", ErrAgentFactoryInvalid, node.Name, info.Name)
		}
		children = append(children, child)
	}
	return children, nil
}

func safeBuildRuntimeLeafAgent(
	ctx context.Context,
	builder RuntimeLLMAgentBuilder,
	spec RuntimeLLMAgentBuildSpec,
) (built agent.Agent, err error) {
	defer func() {
		if recover() != nil {
			built = nil
			err = fmt.Errorf("%w: runtime leaf builder panicked", ErrAgentFactoryInvalid)
		}
	}()
	spec.ToolNames = append([]string(nil), spec.ToolNames...)
	return builder.Build(ctx, spec)
}

func buildGraphRuntime(
	name string,
	runtime *tenant.AgentRuntimeConfig,
	children []agent.Agent,
) (agent.Agent, error) {
	builder := trpcgraph.NewStateGraph(trpcgraph.MessagesStateSchema())
	for _, child := range children {
		builder.AddAgentNode(child.Info().Name)
	}
	incoming := make(map[string][]string, len(children))
	outgoing := make(map[string]struct{}, len(children))
	for _, edge := range runtime.Edges {
		incoming[edge.To] = append(incoming[edge.To], edge.From)
		outgoing[edge.From] = struct{}{}
	}
	for target, sources := range incoming {
		if len(sources) == 1 {
			builder.AddEdge(sources[0], target)
		} else {
			builder.AddJoinEdge(sources, target)
		}
	}
	builder.SetEntryPoint(runtime.Entry)
	for _, child := range children {
		if _, hasOutgoing := outgoing[child.Info().Name]; !hasOutgoing {
			builder.SetFinishPoint(child.Info().Name)
		}
	}
	compiled, err := builder.Compile()
	if err != nil {
		return nil, fmt.Errorf("%w: compile graph runtime: %v", ErrAgentFactoryInvalid, err)
	}
	return graphagent.New(name, compiled,
		graphagent.WithDescription(fmt.Sprintf("Graph agent with %d immutable LLM nodes", len(children))),
		graphagent.WithSubAgents(children),
	)
}

func buildRuntimeAgent(
	ctx context.Context,
	spec RuntimeAgentBuildSpec,
	deps RuntimeAgentDependencies,
	registry *RuntimeAgentRegistry,
) (agent.Agent, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	runtimeType, factory, err := registry.runtimeFactory(spec.Agent.Type)
	if err != nil {
		return nil, err
	}
	// The factory is an operator-installed extension point. Convert provider
	// panics into a stable construction error so one bad runtime cannot crash a
	// worker process or bypass the queue's retry/dead-letter policy.
	built, err := safeBuildRuntimeAgent(factory, ctx, spec, deps)
	if err != nil {
		return nil, fmt.Errorf("build %s agent runtime: %w", runtimeType, err)
	}
	if isNilInterface(built) {
		return nil, fmt.Errorf("%w: type=%s factory returned nil agent", ErrAgentFactoryInvalid, runtimeType)
	}
	info, err := safeAgentInfo(built)
	if err != nil {
		return nil, fmt.Errorf("inspect %s agent runtime: %w", runtimeType, err)
	}
	if info.Name == "" || (spec.Agent.Name != "" && info.Name != spec.Agent.Name) {
		return nil, fmt.Errorf("%w: type=%s returned agent name %q, want %q", ErrAgentFactoryInvalid, runtimeType, info.Name, spec.Agent.Name)
	}
	return built, nil
}

func safeBuildRuntimeAgent(
	factory RuntimeAgentFactory,
	ctx context.Context,
	spec RuntimeAgentBuildSpec,
	deps RuntimeAgentDependencies,
) (built agent.Agent, err error) {
	safeSpec, err := cloneRuntimeBuildSpec(spec)
	if err != nil {
		return nil, err
	}
	safeDeps := deps
	safeDeps.LLMOptions = append([]llmagent.Option(nil), deps.LLMOptions...)
	defer func() {
		if recover() != nil {
			built = nil
			err = fmt.Errorf("%w: runtime factory panicked", ErrAgentFactoryInvalid)
		}
	}()
	return factory.Build(ctx, safeSpec, safeDeps)
}

func cloneRuntimeBuildSpec(spec RuntimeAgentBuildSpec) (RuntimeAgentBuildSpec, error) {
	cloned := spec
	cloned.Agent = cloneAgentConfig(spec.Agent)
	cloned.Model = cloneModelConfig(spec.Model)
	cloned.Tools = append([]tool.Tool(nil), spec.Tools...)
	if spec.Tenant != nil {
		tenantCopy, err := tenant.Clone(spec.Tenant)
		if err != nil {
			return RuntimeAgentBuildSpec{}, fmt.Errorf("%w: clone tenant runtime snapshot: %v", ErrAgentFactoryInvalid, err)
		}
		cloned.Tenant = tenantCopy
	}
	return cloned, nil
}

func safeAgentInfo(built agent.Agent) (info agent.Info, err error) {
	defer func() {
		if recover() != nil {
			info = agent.Info{}
			err = fmt.Errorf("%w: runtime agent info panicked", ErrAgentFactoryInvalid)
		}
	}()
	return built.Info(), nil
}
