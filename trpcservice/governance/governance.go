// Package governance enforces tenant-scoped execution policy before side effects.
package governance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

var (
	// ErrInvalidExecutionContext indicates a missing mandatory security identity.
	ErrInvalidExecutionContext = errors.New("invalid execution context")
	// ErrToolDenied indicates a policy rejected a tool call before delegation.
	ErrToolDenied = errors.New("tool call denied")
	// ErrBudgetExceeded indicates an execution has no remaining tool-call budget.
	ErrBudgetExceeded = errors.New("tool call budget exceeded")
	// ErrBudgetUnitsExceeded indicates the shared model/tool unit budget is exhausted.
	ErrBudgetUnitsExceeded = errors.New("invocation budget units exceeded")
)

// ExecutionContext carries the immutable tenant and policy identity for all
// model and tool side effects within one invocation.
type ExecutionContext struct {
	TenantID           string
	AppCode            string
	ConfigVersion      uint64
	Role               string
	TraceID            string
	RequestID          string
	Channel            string
	BindingID          string
	ConversationID     string
	ConversationScope  string
	ExternalUserID     string
	ProgressMessageID  string
	ProviderReplyToken string
	UserID             string
	SessionID          string
	AgentName          string
	PolicyVersion      string
	ToolRoles          map[string][]string
	AllowedTools       map[string]struct{}
}

// ToolRequest is the normalized tool action evaluated by governance.
type ToolRequest struct {
	Name      string
	Arguments []byte
}

// ToolOutcome is the non-sensitive result class recorded for tool governance.
type ToolOutcome string

const (
	ToolOutcomeAllowed ToolOutcome = "allowed"
	ToolOutcomeDenied  ToolOutcome = "denied"
	ToolOutcomeFailed  ToolOutcome = "failed"
)

// ToolAuditEvent contains only routing and digest metadata; callers must never
// place raw tool arguments, tokens, or full user text in this structure.
type ToolAuditEvent struct {
	TenantID        string
	TraceID         string
	RequestID       string
	Channel         string
	UserID          string
	SessionID       string
	AgentName       string
	PolicyVersion   string
	ToolName        string
	Outcome         ToolOutcome
	LatencyMS       int64
	ErrorType       string
	ArgumentsDigest string
	OccurredAt      time.Time
}

// AuditSink accepts governance evidence. Production implementations should
// route it to the tenant-scoped audit repository.
type AuditSink interface {
	RecordToolAudit(context.Context, ToolAuditEvent) error
}

// ToolPolicy performs authorization and argument-risk checks before a tool is
// called. Policies must fail closed for unknown tenants, tools, and roles.
type ToolPolicy interface {
	Authorize(context.Context, ExecutionContext, ToolRequest) error
}

// StaticToolPolicy is an immutable allow-list policy with recursive dangerous
// argument-key rejection. Tenant-specific policy loading happens outside it.
type StaticToolPolicy struct {
	allowedTools          map[string]struct{}
	forbiddenArgumentKeys map[string]struct{}
}

// NewStaticToolPolicy constructs a fail-closed allow-list policy.
func NewStaticToolPolicy(allowedTools, forbiddenArgumentKeys []string) *StaticToolPolicy {
	allowed := make(map[string]struct{}, len(allowedTools))
	for _, name := range allowedTools {
		allowed[strings.TrimSpace(name)] = struct{}{}
	}
	forbidden := make(map[string]struct{}, len(forbiddenArgumentKeys))
	for _, name := range forbiddenArgumentKeys {
		forbidden[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
	}
	return &StaticToolPolicy{allowedTools: allowed, forbiddenArgumentKeys: forbidden}
}

// Authorize verifies identity, tool allow-list membership, valid JSON, and
// recursively rejects forbidden argument keys before any tool side effect.
func (p *StaticToolPolicy) Authorize(_ context.Context, execution ExecutionContext, request ToolRequest) error {
	if strings.TrimSpace(execution.TenantID) == "" ||
		strings.TrimSpace(execution.Role) == "" ||
		strings.TrimSpace(execution.TraceID) == "" ||
		strings.TrimSpace(execution.PolicyVersion) == "" {
		return ErrInvalidExecutionContext
	}
	if _, allowed := p.allowedTools[request.Name]; !allowed {
		if _, tenantAllowed := execution.AllowedTools[request.Name]; !tenantAllowed {
			return fmt.Errorf("%w: tool %q is not allowed", ErrToolDenied, request.Name)
		}
	}
	if roles, configured := execution.ToolRoles[request.Name]; configured && !roleAllowed(execution.Role, roles) {
		return fmt.Errorf("%w: role %q is not authorized for tool %q", ErrToolDenied, execution.Role, request.Name)
	}
	var arguments any
	if !json.Valid(request.Arguments) || json.Unmarshal(request.Arguments, &arguments) != nil {
		return fmt.Errorf("%w: tool arguments must be valid JSON", ErrToolDenied)
	}
	if containsForbiddenArgument(arguments, p.forbiddenArgumentKeys) {
		return fmt.Errorf("%w: dangerous argument key", ErrToolDenied)
	}
	return nil
}

func containsForbiddenArgument(value any, forbidden map[string]struct{}) bool {
	switch typed := value.(type) {
	case map[string]any:
		for key, nested := range typed {
			if _, found := forbidden[strings.ToLower(key)]; found {
				return true
			}
			if containsForbiddenArgument(nested, forbidden) {
				return true
			}
		}
	case []any:
		for _, nested := range typed {
			if containsForbiddenArgument(nested, forbidden) {
				return true
			}
		}
	}
	return false
}

// CallBudget is a concurrency-safe per-execution tool-call budget.
type CallBudget struct {
	mu        sync.Mutex
	remaining int
}

// UnitBudget is a concurrency-safe cost-unit budget shared by model and tools.
type UnitBudget struct {
	mu        sync.Mutex
	remaining int64
}

func NewUnitBudget(limit int64) *UnitBudget {
	return &UnitBudget{remaining: limit}
}

func (b *UnitBudget) Consume(units int64) error {
	if b == nil || units <= 0 {
		return fmt.Errorf("%w: positive units are required", ErrBudgetUnitsExceeded)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.remaining < units {
		return ErrBudgetUnitsExceeded
	}
	b.remaining -= units
	return nil
}

// NewCallBudget constructs a positive, shared call budget for one execution.
func NewCallBudget(limit int) *CallBudget {
	return &CallBudget{remaining: limit}
}

// Consume reserves one call. A denied call does not consume budget because it
// never enters the underlying tool implementation.
func (b *CallBudget) Consume() error {
	if b == nil {
		return fmt.Errorf("%w: tool budget is required", ErrBudgetExceeded)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.remaining <= 0 {
		return ErrBudgetExceeded
	}
	b.remaining--
	return nil
}

func roleAllowed(role string, allowed []string) bool {
	for _, candidate := range allowed {
		if role == candidate {
			return true
		}
	}
	return false
}

func elevatedRole(role string) bool {
	return role == "admin"
}
