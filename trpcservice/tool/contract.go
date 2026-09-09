package tool

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// DecisionCategory is the only security outcome exposed by the policy and
// guardrail boundary. It deliberately excludes backend error text.
type DecisionCategory string

const (
	CategoryAllow             DecisionCategory = "allow"
	CategoryDeny              DecisionCategory = "deny"
	CategoryApprovalRequired  DecisionCategory = "approval_required"
	CategoryInvalidInput      DecisionCategory = "invalid_input"
	CategoryInvalidContext    DecisionCategory = "invalid_context"
	CategoryPolicyUnavailable DecisionCategory = "policy_unavailable"
	CategoryExpiredPolicy     DecisionCategory = "expired_policy"
	CategoryVersionMismatch   DecisionCategory = "version_mismatch"
	CategoryBudgetExceeded    DecisionCategory = "budget_exceeded"
	CategoryRedacted          DecisionCategory = "redacted"
	CategoryOversized         DecisionCategory = "oversized"
	CategoryTimeout           DecisionCategory = "timeout"
	CategoryCancelled         DecisionCategory = "cancelled"
	CategoryToolFailure       DecisionCategory = "tool_failure"
	CategoryUnknown           DecisionCategory = "unknown"
)

// GovernanceError never wraps a policy, database, Redis, provider, or tool
// implementation error. The code is a stable, low-cardinality safe category.
type GovernanceError struct {
	Category DecisionCategory
	Code     string
}

func (e GovernanceError) Error() string {
	if e.Code == "" {
		return "tool governance: " + string(e.Category)
	}
	return "tool governance: " + string(e.Category) + ": " + e.Code
}

func CategoryOf(err error) DecisionCategory {
	var governance GovernanceError
	if errors.As(err, &governance) {
		return governance.Category
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return CategoryTimeout
	}
	if errors.Is(err, context.Canceled) {
		return CategoryCancelled
	}
	return CategoryUnknown
}

func safeError(category DecisionCategory, code string) error {
	if category == "" {
		category = CategoryUnknown
	}
	return GovernanceError{Category: category, Code: code}
}

type Definition struct {
	Name          string
	Version       int64
	Capability    string
	Enabled       bool
	MaxInputByte  int
	MaxOutputByte int
	InputSchema   map[string]any
	AllowedHosts  []string
	Invoker       agent.ToolInvoker
}

func (d Definition) validate() error {
	if !validServerID(d.Name) || d.Version < 1 || len(d.Capability) > 64 {
		return errors.New("invalid tool definition")
	}
	if d.MaxInputByte < 0 || d.MaxOutputByte < 0 {
		return errors.New("invalid tool definition bounds")
	}
	for _, host := range d.AllowedHosts {
		if host == "" || len(host) > 255 || strings.ContainsAny(host, "\r\n/\\") {
			return errors.New("invalid tool host")
		}
	}
	return nil
}

// Registry is immutable after construction. It is the only place where a
// server-owned implementation is bound to a Tool identity.
type Registry struct {
	definitions map[string]Definition
}

func NewRegistry(definitions []Definition) (*Registry, error) {
	result := &Registry{definitions: make(map[string]Definition, len(definitions))}
	for _, definition := range definitions {
		if err := definition.validate(); err != nil {
			return nil, err
		}
		if _, exists := result.definitions[definition.Name]; exists {
			return nil, errors.New("duplicate tool definition")
		}
		definition.InputSchema = cloneMap(definition.InputSchema)
		definition.AllowedHosts = append([]string(nil), definition.AllowedHosts...)
		result.definitions[definition.Name] = definition
	}
	return result, nil
}

func (r *Registry) Lookup(name string) (Definition, bool) {
	if r == nil {
		return Definition{}, false
	}
	definition, ok := r.definitions[strings.TrimSpace(name)]
	if !ok {
		return Definition{}, false
	}
	definition.InputSchema = cloneMap(definition.InputSchema)
	definition.AllowedHosts = append([]string(nil), definition.AllowedHosts...)
	return definition, true
}

type PolicyRule struct {
	ToolVersion      int64
	Capability       string
	Enabled          bool
	Allow            bool
	ApprovalRequired bool
}

type Policy struct {
	TenantID     string
	AgentAppID   string
	AgentVersion int64
	PolicyRef    string
	Version      int64
	ExpiresAt    time.Time
	Rules        map[string]PolicyRule
}

type PolicyKey struct {
	TenantID     string
	AgentAppID   string
	AgentVersion int64
	PolicyRef    string
}

func (p Policy) key() PolicyKey {
	return PolicyKey{TenantID: p.TenantID, AgentAppID: p.AgentAppID, AgentVersion: p.AgentVersion, PolicyRef: p.PolicyRef}
}

func (p Policy) validate() error {
	if !validServerID(p.TenantID) || !validServerID(p.AgentAppID) || p.AgentVersion < 1 || p.PolicyRef == "" || len(p.PolicyRef) > 256 || p.Version < 1 || p.ExpiresAt.IsZero() || len(p.Rules) == 0 {
		return errors.New("invalid tool policy")
	}
	for name, rule := range p.Rules {
		if !validServerID(name) || rule.ToolVersion < 1 || len(rule.Capability) > 64 {
			return errors.New("invalid tool policy rule")
		}
	}
	return nil
}

type PolicyDecision struct {
	Category      DecisionCategory
	PolicyVersion int64
	ToolVersion   int64
	ExpiresAt     time.Time
}

func (d PolicyDecision) valid() bool {
	switch d.Category {
	case CategoryAllow, CategoryDeny, CategoryApprovalRequired, CategoryInvalidContext, CategoryPolicyUnavailable, CategoryExpiredPolicy, CategoryVersionMismatch, CategoryUnknown:
		return d.Category != CategoryAllow || d.PolicyVersion > 0 && d.ToolVersion > 0
	default:
		return false
	}
}

type PolicyResolver interface {
	Resolve(context.Context, tenant.TenantContext, agent.AgentSpec, Definition) (PolicyDecision, error)
}

type PolicyResolverFunc func(context.Context, tenant.TenantContext, agent.AgentSpec, Definition) (PolicyDecision, error)

func (f PolicyResolverFunc) Resolve(ctx context.Context, tc tenant.TenantContext, spec agent.AgentSpec, definition Definition) (PolicyDecision, error) {
	if f == nil {
		return PolicyDecision{Category: CategoryPolicyUnavailable}, errors.New("nil policy resolver")
	}
	return f(ctx, tc, spec, definition)
}

type FailClosedPolicyResolver struct{}

func (FailClosedPolicyResolver) Resolve(context.Context, tenant.TenantContext, agent.AgentSpec, Definition) (PolicyDecision, error) {
	return PolicyDecision{Category: CategoryPolicyUnavailable}, errors.New("policy backend unavailable")
}

// StaticPolicyResolver is a deterministic contract test resolver. It is not a
// production policy store and deliberately has no persistence or cache fallback.
type StaticPolicyResolver struct {
	policies map[PolicyKey]Policy
	now      func() time.Time
}

func NewStaticPolicyResolver(policies []Policy) (*StaticPolicyResolver, error) {
	result := &StaticPolicyResolver{policies: make(map[PolicyKey]Policy, len(policies)), now: func() time.Time { return time.Now().UTC() }}
	for _, policy := range policies {
		if err := policy.validate(); err != nil {
			return nil, err
		}
		key := policy.key()
		if _, exists := result.policies[key]; exists {
			return nil, errors.New("duplicate tool policy")
		}
		policy.Rules = cloneRules(policy.Rules)
		result.policies[key] = policy
	}
	return result, nil
}

func (r *StaticPolicyResolver) Resolve(ctx context.Context, tc tenant.TenantContext, spec agent.AgentSpec, definition Definition) (PolicyDecision, error) {
	if ctx == nil {
		return PolicyDecision{Category: CategoryInvalidContext}, nil
	}
	if err := ctx.Err(); err != nil {
		return PolicyDecision{Category: categoryForContext(err)}, err
	}
	if err := tc.Validate(); err != nil || spec.TenantID != tc.TenantID || spec.AgentAppID != tc.AgentAppID || spec.Version != tc.ConfigVersion {
		return PolicyDecision{Category: CategoryInvalidContext}, nil
	}
	if err := definition.validate(); err != nil || spec.ToolPolicyRef == "" {
		return PolicyDecision{Category: CategoryDeny}, nil
	}
	policy, ok := r.policies[PolicyKey{TenantID: tc.TenantID, AgentAppID: spec.AgentAppID, AgentVersion: spec.Version, PolicyRef: spec.ToolPolicyRef}]
	if !ok {
		return PolicyDecision{Category: CategoryDeny}, nil
	}
	if !policy.ExpiresAt.After(r.now()) {
		return PolicyDecision{Category: CategoryExpiredPolicy, PolicyVersion: policy.Version, ExpiresAt: policy.ExpiresAt}, nil
	}
	rule, ok := policy.Rules[definition.Name]
	if !ok || !rule.Enabled {
		return PolicyDecision{Category: CategoryDeny, PolicyVersion: policy.Version, ExpiresAt: policy.ExpiresAt}, nil
	}
	if rule.ToolVersion != definition.Version || (rule.Capability != "" && rule.Capability != definition.Capability) {
		return PolicyDecision{Category: CategoryVersionMismatch, PolicyVersion: policy.Version, ToolVersion: rule.ToolVersion, ExpiresAt: policy.ExpiresAt}, nil
	}
	if rule.ApprovalRequired {
		return PolicyDecision{Category: CategoryApprovalRequired, PolicyVersion: policy.Version, ToolVersion: rule.ToolVersion, ExpiresAt: policy.ExpiresAt}, nil
	}
	if !rule.Allow {
		return PolicyDecision{Category: CategoryDeny, PolicyVersion: policy.Version, ToolVersion: rule.ToolVersion, ExpiresAt: policy.ExpiresAt}, nil
	}
	return PolicyDecision{Category: CategoryAllow, PolicyVersion: policy.Version, ToolVersion: rule.ToolVersion, ExpiresAt: policy.ExpiresAt}, nil
}

func categoryForContext(err error) DecisionCategory {
	if errors.Is(err, context.DeadlineExceeded) {
		return CategoryTimeout
	}
	if errors.Is(err, context.Canceled) {
		return CategoryCancelled
	}
	return CategoryInvalidContext
}

func validServerID(value string) bool {
	if value == "" || len(value) > 256 || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}

func canonicalVersion(version int64) int64 {
	if version == 0 {
		return 1
	}
	return version
}

func cloneRules(input map[string]PolicyRule) map[string]PolicyRule {
	output := make(map[string]PolicyRule, len(input))
	for key, value := range input {
		output[key] = value
	}
	return output
}

func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	output := make(map[string]any, len(input))
	for key, value := range input {
		output[key] = cloneValue(value)
	}
	return output
}

func cloneValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneMap(typed)
	case []any:
		output := make([]any, len(typed))
		for i, item := range typed {
			output[i] = cloneValue(item)
		}
		return output
	default:
		return value
	}
}

var _ PolicyResolver = PolicyResolverFunc(nil)
var _ PolicyResolver = FailClosedPolicyResolver{}
