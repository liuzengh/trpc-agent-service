package tool

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type AuditRecord struct {
	TenantID          string
	AgentAppID        string
	SessionID         string
	RequestID         string
	MessageID         string
	ToolName          string
	ToolVersion       int64
	PolicyVersion     int64
	Decision          DecisionCategory
	InputBytes        int
	OutputBytes       int
	InputFingerprint  string
	OutputFingerprint string
	Latency           time.Duration
}

type AuditSink interface {
	Record(context.Context, AuditRecord) error
}

type AuditSinkFunc func(context.Context, AuditRecord) error

func (f AuditSinkFunc) Record(ctx context.Context, record AuditRecord) error {
	if f == nil {
		return errors.New("nil audit sink")
	}
	return f(ctx, record)
}

type SecureInvokerConfig struct {
	Registry  *Registry
	Policy    PolicyResolver
	Guardrail Guardrail
	Budget    *BudgetManager
	Audit     AuditSink
}

type SecureInvoker struct {
	registry  *Registry
	policy    PolicyResolver
	guardrail Guardrail
	budget    *BudgetManager
	audit     AuditSink
	now       func() time.Time
}

func NewSecureInvoker(config SecureInvokerConfig) (*SecureInvoker, error) {
	if config.Registry == nil || config.Policy == nil || config.Guardrail == nil {
		return nil, safeError(CategoryPolicyUnavailable, "governance_unavailable")
	}
	return &SecureInvoker{registry: config.Registry, policy: config.Policy, guardrail: config.Guardrail, budget: config.Budget, audit: config.Audit, now: func() time.Time { return time.Now().UTC() }}, nil
}

// NewFailClosedInvoker is the production default until a server-owned Tool
// registry, durable policy source, budget source, and audit sink are wired.
// It never falls back to allow and binds no implementation.
func NewFailClosedInvoker() (*SecureInvoker, error) {
	registry, err := NewRegistry(nil)
	if err != nil {
		return nil, err
	}
	guardrail, err := NewGuardrailPipeline(GuardrailConfig{Redactor: audit.NewRedactor(audit.DefaultRedactBytes)})
	if err != nil {
		return nil, err
	}
	return NewSecureInvoker(SecureInvokerConfig{Registry: registry, Policy: FailClosedPolicyResolver{}, Guardrail: guardrail})
}

func (s *SecureInvoker) Invoke(ctx context.Context, request agent.ToolRequest) (agent.ToolResult, error) {
	if s == nil || s.registry == nil || s.policy == nil || s.guardrail == nil {
		return agent.ToolResult{}, safeError(CategoryPolicyUnavailable, "governance_unavailable")
	}
	if ctx == nil {
		return agent.ToolResult{}, safeError(CategoryInvalidContext, "invalid_context")
	}
	if err := ctx.Err(); err != nil {
		return agent.ToolResult{}, safeError(categoryForContext(err), "context")
	}
	if err := validateRequest(request); err != nil {
		return agent.ToolResult{}, err
	}
	definition, ok := s.registry.Lookup(request.ToolName)
	if !ok {
		return agent.ToolResult{}, safeError(CategoryDeny, "unknown_tool")
	}
	if !definition.Enabled || definition.Invoker == nil {
		return agent.ToolResult{}, safeError(CategoryDeny, "tool_disabled")
	}
	if !declaredTool(request.Agent, definition) {
		return agent.ToolResult{}, safeError(CategoryDeny, "tool_not_declared")
	}
	decision, resolveErr := s.policy.Resolve(ctx, request.TenantContext, request.Agent, definition)
	if resolveErr != nil {
		if err := ctx.Err(); err != nil {
			return agent.ToolResult{}, safeError(categoryForContext(err), "policy_context")
		}
		return agent.ToolResult{}, safeError(CategoryPolicyUnavailable, "policy_unavailable")
	}
	if !decision.valid() {
		return agent.ToolResult{}, safeError(CategoryUnknown, "invalid_policy_decision")
	}
	if decision.Category == CategoryAllow && decision.ToolVersion != definition.Version {
		return agent.ToolResult{}, safeError(CategoryVersionMismatch, "tool_version_mismatch")
	}
	if decision.Category != CategoryAllow {
		return agent.ToolResult{}, safeError(decision.Category, decisionCode(decision.Category))
	}
	if decision.ExpiresAt.IsZero() || !decision.ExpiresAt.After(s.now()) {
		return agent.ToolResult{}, safeError(CategoryExpiredPolicy, "expired_policy")
	}
	invocation := Invocation{TenantContext: request.TenantContext, Agent: cloneAgentSpec(request.Agent), ToolName: request.ToolName, Definition: definition, Arguments: cloneMap(request.Arguments)}
	before := s.guardrail.Before(ctx, invocation)
	if before.Category != CategoryAllow {
		return agent.ToolResult{}, safeError(before.Category, decisionCode(before.Category))
	}
	if s.budget == nil {
		return agent.ToolResult{}, safeError(CategoryPolicyUnavailable, "budget_unavailable")
	}
	if s.audit == nil {
		return agent.ToolResult{}, safeError(CategoryPolicyUnavailable, "audit_unavailable")
	}
	preAudit := s.auditRecord(invocation, decision, before, GuardrailResult{Category: CategoryAllow})
	if err := preAudit.validate(); err != nil {
		return agent.ToolResult{}, safeError(CategoryPolicyUnavailable, "audit_unavailable")
	}
	if err := s.audit.Record(ctx, preAudit); err != nil {
		return agent.ToolResult{}, safeError(CategoryPolicyUnavailable, "audit_unavailable")
	}
	reservation, err := s.budget.Reserve(ctx, request.TenantContext, before.InputBytes)
	if err != nil {
		return agent.ToolResult{}, err
	}
	started := s.now()
	result, invokeErr := definition.Invoker.Invoke(ctx, agent.ToolRequest{TenantContext: cloneTenantContext(request.TenantContext), Agent: cloneAgentSpec(request.Agent), ToolName: request.ToolName, Arguments: cloneMap(request.Arguments)})
	elapsed := s.now().Sub(started)
	if elapsed < 0 {
		elapsed = 0
	}
	after := s.guardrail.After(ctx, invocation, result)
	commitErr := reservation.Commit(len(result.Content), elapsed)
	if commitErr != nil {
		return agent.ToolResult{}, commitErr
	}
	afterAudit := s.auditRecord(invocation, decision, before, after)
	if err := afterAudit.validate(); err != nil {
		return agent.ToolResult{}, safeError(CategoryPolicyUnavailable, "audit_unavailable")
	}
	if err := s.audit.Record(ctx, afterAudit); err != nil {
		return agent.ToolResult{}, safeError(CategoryPolicyUnavailable, "audit_unavailable")
	}
	if invokeErr != nil {
		if errors.Is(invokeErr, context.Canceled) || errors.Is(invokeErr, context.DeadlineExceeded) {
			return agent.ToolResult{}, invokeErr
		}
		return agent.ToolResult{}, safeError(CategoryToolFailure, "tool_failed")
	}
	if after.Category == CategoryAllow || after.Category == CategoryRedacted {
		result.Content = after.Content
		result.IsError = false
		return result, nil
	}
	return agent.ToolResult{}, safeError(after.Category, decisionCode(after.Category))
}

func validateRequest(request agent.ToolRequest) error {
	if strings.TrimSpace(request.ToolName) == "" {
		return safeError(CategoryInvalidInput, "tool_name_required")
	}
	if err := request.TenantContext.Validate(); err != nil {
		return safeError(CategoryInvalidContext, "invalid_context")
	}
	if request.Agent.TenantID != request.TenantContext.TenantID || request.Agent.AgentAppID != request.TenantContext.AgentAppID || request.Agent.Version != request.TenantContext.ConfigVersion {
		return safeError(CategoryInvalidContext, "agent_context_mismatch")
	}
	return nil
}

func declaredTool(spec agent.AgentSpec, definition Definition) bool {
	for _, candidate := range spec.Tools {
		if candidate.Name != definition.Name {
			continue
		}
		return canonicalVersion(candidate.Version) == definition.Version && (candidate.Capability == "" || candidate.Capability == definition.Capability)
	}
	return false
}

func decisionCode(category DecisionCategory) string {
	switch category {
	case CategoryApprovalRequired:
		return "approval_deferred"
	case CategoryBudgetExceeded:
		return "budget_exceeded"
	case CategoryOversized:
		return "payload_oversized"
	case CategoryRedacted:
		return "output_redacted"
	case CategoryTimeout:
		return "timeout"
	case CategoryCancelled:
		return "cancelled"
	default:
		return strings.ReplaceAll(string(category), "-", "_")
	}
}

func (s *SecureInvoker) auditRecord(invocation Invocation, decision PolicyDecision, before, after GuardrailResult) AuditRecord {
	return AuditRecord{TenantID: invocation.TenantContext.TenantID, AgentAppID: invocation.TenantContext.AgentAppID, SessionID: invocation.TenantContext.SessionID, RequestID: invocation.TenantContext.RequestID, MessageID: invocation.TenantContext.MessageID, ToolName: invocation.Definition.Name, ToolVersion: invocation.Definition.Version, PolicyVersion: decision.PolicyVersion, Decision: after.Category, InputBytes: before.InputBytes, OutputBytes: after.OutputBytes, InputFingerprint: before.Fingerprint, OutputFingerprint: after.Fingerprint}
}

func cloneTenantContext(tc tenant.TenantContext) tenant.TenantContext {
	tc.Permissions = append([]string(nil), tc.Permissions...)
	return tc
}

func cloneAgentSpec(spec agent.AgentSpec) agent.AgentSpec {
	spec.Tools = append([]agent.ToolSpec(nil), spec.Tools...)
	for i := range spec.Tools {
		spec.Tools[i].InputSchema = cloneMap(spec.Tools[i].InputSchema)
	}
	return spec
}

var _ agent.ToolInvoker = (*SecureInvoker)(nil)
var _ AuditSink = AuditSinkFunc(nil)
