// Package agent hosts tenant-specific agents built on tRPC-Agent-Go.
package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	ErrInvalidSpec        = errors.New("agent: invalid agent spec")
	ErrInvalidInput       = errors.New("agent: invalid input")
	ErrTenantMismatch     = errors.New("agent: tenant context does not match agent")
	ErrProviderFailure    = errors.New("agent: provider failure")
	ErrToolFailure        = errors.New("agent: tool failure")
	ErrFrameworkFailure   = errors.New("agent: framework failure")
	ErrDrainTimeout       = errors.New("agent: event drain timeout")
	ErrProducerIncomplete = errors.New("agent: runner producer incomplete")
)

// AgentSpec is the immutable, platform-owned configuration for one agent release.
type AgentSpec struct {
	TenantID       string
	AgentAppID     string
	Version        int64
	Name           string
	ModelProvider  string
	ModelConfigRef string
	SystemPrompt   string
	ToolPolicyRef  string
	GuardrailRef   string
	Tools          []ToolSpec
}

// ToolSpec is the immutable declaration of a tool allowed for this agent.
type ToolSpec struct {
	Name        string
	Description string
	InputSchema map[string]any
	// Version and Capability are server-owned declaration metadata. A zero
	// Version is treated as the initial version for backwards compatibility.
	Version    int64
	Capability string
}

// Message is the platform representation of a conversation message.
type Message struct {
	ID        string
	Role      string
	Content   string
	CreatedAt time.Time
	ToolID    string
	ToolName  string
	ToolCalls []ToolCall
}

// AgentInput contains one request-scoped execution input.
type AgentInput struct {
	TenantContext tenant.TenantContext
	Agent         AgentSpec
	History       []Message
	Input         Message
}

// RunnerEvent is the stable event representation exposed by the platform.
type RunnerEvent struct {
	Sequence  int64
	Type      string
	Role      string
	Content   string
	ToolName  string
	ErrorType string
	Metadata  map[string]string
}

// Usage contains provider token accounting.
type Usage struct {
	InputTokens  int64
	OutputTokens int64
}

// AgentResult is the stable result representation exposed by the platform.
type AgentResult struct {
	Text       string
	Events     []RunnerEvent
	Usage      Usage
	FinishType string
}

// ProviderRequest and ProviderResponse are framework-independent model seams.
type ProviderRequest struct {
	Messages []Message
	Tools    []ToolSpec
}

type ToolCall struct {
	ID        string
	Name      string
	Arguments string
}

type ProviderResponse struct {
	Text         string
	InputTokens  int64
	OutputTokens int64
	FinishType   string
	ToolCalls    []ToolCall
}

// Provider is the minimal model dependency required by the Runtime.
type Provider interface {
	Complete(context.Context, ProviderRequest) (ProviderResponse, error)
}

// ProviderFactory constructs a request-scoped Provider from verified tenant and
// agent configuration. Implementations must not share mutable tenant state.
type ProviderFactory interface {
	Build(context.Context, tenant.TenantContext, AgentSpec) (Provider, error)
}

// ProviderFactoryFunc adapts a function to ProviderFactory.
type ProviderFactoryFunc func(context.Context, tenant.TenantContext, AgentSpec) (Provider, error)

func (f ProviderFactoryFunc) Build(ctx context.Context, tc tenant.TenantContext, spec AgentSpec) (Provider, error) {
	if f == nil {
		return nil, errors.New("agent: provider factory function is nil")
	}
	return f(ctx, tc, spec)
}

// ToolRequest is the platform-independent input for one tool invocation.
type ToolRequest struct {
	TenantContext tenant.TenantContext
	Agent         AgentSpec
	ToolName      string
	Arguments     map[string]any
}

// ToolResult is the platform-independent result of one tool invocation.
type ToolResult struct {
	Content string
	IsError bool
}

// ToolInvoker is the request-scoped tool dependency used by the Runtime.
type ToolInvoker interface {
	Invoke(context.Context, ToolRequest) (ToolResult, error)
}

// ToolInvokerFunc adapts a function to ToolInvoker.
type ToolInvokerFunc func(context.Context, ToolRequest) (ToolResult, error)

func (f ToolInvokerFunc) Invoke(ctx context.Context, request ToolRequest) (ToolResult, error) {
	if f == nil {
		return ToolResult{}, errors.New("agent: tool invoker function is nil")
	}
	return f(ctx, request)
}

// ProviderFunc adapts a pure Go function to Provider for embedding a model
// client without exposing framework types at the platform boundary.
type ProviderFunc func(context.Context, ProviderRequest) (ProviderResponse, error)

func (f ProviderFunc) Complete(ctx context.Context, request ProviderRequest) (ProviderResponse, error) {
	if f == nil {
		return ProviderResponse{}, errors.New("agent: provider function is nil")
	}
	return f(ctx, request)
}

// RuntimeDependencies contains request-independent construction dependencies.
// Providers are always built through the request-scoped ProviderFactory.
type RuntimeDependencies struct {
	ProviderFactory ProviderFactory
	ToolInvoker     ToolInvoker
	StopTimeout     time.Duration
	DrainTimeout    time.Duration
}

// AgentFactory constructs an immutable, request-scoped runtime.
type AgentFactory interface {
	Build(context.Context, tenant.TenantContext, AgentSpec) (AgentRuntime, error)
}

// AgentRuntime executes one request without owning session or tenant state.
type AgentRuntime interface {
	Run(context.Context, AgentInput) (AgentResult, error)
}

// Factory is the concrete AgentFactory implementation.
type Factory struct {
	deps RuntimeDependencies
}

type agentRuntimeImpl struct {
	deps RuntimeDependencies
	tc   tenant.TenantContext
	spec AgentSpec
}

// NewFactory validates dependencies and creates a Runtime factory.
func NewFactory(deps RuntimeDependencies) (*Factory, error) {
	if deps.ProviderFactory == nil {
		return nil, errors.New("agent: provider factory is not configured")
	}
	if deps.DrainTimeout <= 0 {
		deps.DrainTimeout = 2 * time.Second
	}
	if deps.StopTimeout <= 0 {
		deps.StopTimeout = 500 * time.Millisecond
	}
	return &Factory{deps: deps}, nil
}

// Build validates the tenant boundary before constructing a Runtime.
func (f *Factory) Build(ctx context.Context, tc tenant.TenantContext, spec AgentSpec) (AgentRuntime, error) {
	if f == nil || f.deps.ProviderFactory == nil {
		return nil, errors.New("agent: factory is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := tc.Validate(); err != nil {
		return nil, fmt.Errorf("%w: tenant context: %v", ErrInvalidInput, err)
	}
	if err := validateSpec(spec); err != nil {
		return nil, err
	}
	if tc.TenantID != spec.TenantID || tc.AgentAppID != spec.AgentAppID || tc.ConfigVersion != spec.Version {
		return nil, fmt.Errorf("%w: tenant=%q agent=%q version=%d", ErrTenantMismatch, tc.TenantID, tc.AgentAppID, tc.ConfigVersion)
	}
	return &agentRuntimeImpl{
		deps: depsCopy(f.deps),
		tc:   cloneTenantContext(tc),
		spec: cloneSpec(spec),
	}, nil
}

func validateSpec(spec AgentSpec) error {
	if strings.TrimSpace(spec.TenantID) == "" || strings.TrimSpace(spec.AgentAppID) == "" || spec.Version < 1 || strings.TrimSpace(spec.Name) == "" {
		return fmt.Errorf("%w: tenant, agent, version, and name are required", ErrInvalidSpec)
	}
	if strings.TrimSpace(spec.ModelProvider) == "" {
		return fmt.Errorf("%w: model provider is required", ErrInvalidSpec)
	}
	return nil
}

func cloneSpec(spec AgentSpec) AgentSpec {
	spec.Tools = append([]ToolSpec(nil), spec.Tools...)
	for i := range spec.Tools {
		spec.Tools[i].InputSchema = cloneArguments(spec.Tools[i].InputSchema)
	}
	return spec
}

func cloneTenantContext(tc tenant.TenantContext) tenant.TenantContext {
	tc.Permissions = append([]string(nil), tc.Permissions...)
	return tc
}

func depsCopy(deps RuntimeDependencies) RuntimeDependencies {
	return RuntimeDependencies{
		ProviderFactory: deps.ProviderFactory,
		ToolInvoker:     deps.ToolInvoker,
		StopTimeout:     deps.StopTimeout,
		DrainTimeout:    deps.DrainTimeout,
	}
}

func cloneMessages(messages []Message) []Message {
	out := append([]Message(nil), messages...)
	for i := range out {
		out[i].ToolCalls = append([]ToolCall(nil), out[i].ToolCalls...)
	}
	return out
}

func cloneMetadata(metadata map[string]string) map[string]string {
	if metadata == nil {
		return nil
	}
	out := make(map[string]string, len(metadata))
	for key, value := range metadata {
		out[key] = value
	}
	return out
}
