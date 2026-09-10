// Package audit defines the small, durable audit contract used by the
// execution path. It intentionally contains metadata only, never payloads.
package audit

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"
)

const (
	ExecutionStarted   = "execution_started"
	ExecutionCompleted = "execution_completed"
	ExecutionFailed    = "execution_failed"

	ToolAllowed        = "tool_allowed"
	ToolDenied         = "tool_denied"
	ToolReviewRequired = "tool_review_required"
	ToolCompleted      = "tool_completed"
	ToolFailed         = "tool_failed"

	IMAccessDenied = "im_access_denied"
	BudgetRejected = "budget_rejected"

	ApprovalCreated  = "approval_created"
	ApprovalApproved = "approval_approved"
	ApprovalDenied   = "approval_denied"
	ApprovalExpired  = "approval_expired"

	AuditQueryRead         = "audit_query_read"
	ConfigActivated        = "config_activated"
	ConfigCanaryEnabled    = "config_canary_enabled"
	ConfigCanaryPaused     = "config_canary_paused"
	ConfigCanaryDisabled   = "config_canary_disabled"
	ConfigCanaryPromoted   = "config_canary_promoted"
	ConfigCanaryRolledBack = "config_canary_rolled_back"
	ChannelEnabled         = "channel_enabled"
	ChannelSuspended       = "channel_suspended"
	MigrationCreated       = "migration_created"
	MigrationStarted       = "migration_started"
	MigrationSucceeded     = "migration_succeeded"
	MigrationFailed        = "migration_failed"
	CredentialIssued       = "credential_issued"
	CredentialRevoked      = "credential_revoked"
)

// Sink is the best-effort audit persistence boundary. Implementations must
// enforce tenant and application scope in every write.
type Sink interface {
	Record(context.Context, Event) error
}

// WithControlPlaneActor carries only the stable, non-secret identity of the
// authenticated administrator through repository calls. Raw credentials must
// never be placed in this context.
func WithControlPlaneActor(ctx context.Context, actorID, actorRole string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, controlPlaneActorContextKey{}, controlPlaneActor{
		ID: actorID, Role: actorRole,
	})
}

// ControlPlaneActorFromContext returns the authenticated control-plane actor,
// if the caller crossed the admin authentication boundary.
func ControlPlaneActorFromContext(ctx context.Context) (actorID, actorRole string, ok bool) {
	if ctx == nil {
		return "", "", false
	}
	actor, ok := ctx.Value(controlPlaneActorContextKey{}).(controlPlaneActor)
	if !ok || actor.ID == "" || actor.Role == "" {
		return "", "", false
	}
	return actor.ID, actor.Role, true
}

type controlPlaneActorContextKey struct{}

type controlPlaneActor struct {
	ID   string
	Role string
}

// Event is the complete audit record. It deliberately has no raw request,
// message, tool argument, provider target, secret, or artifact fields.
type Event struct {
	TenantID          string        `json:"tenant_id"`
	AppID             string        `json:"app_id"`
	ActorID           string        `json:"actor_id,omitempty"`
	ActorRole         string        `json:"actor_role,omitempty"`
	RequestedTenantID string        `json:"requested_tenant_id,omitempty"`
	RequestedAppID    string        `json:"requested_app_id,omitempty"`
	QueryDigest       string        `json:"query_digest,omitempty"`
	ResultCount       int           `json:"result_count,omitempty"`
	Channel           string        `json:"channel"`
	UserID            string        `json:"user_id"`
	SessionID         string        `json:"session_id"`
	AgentName         string        `json:"agent_name"`
	ToolName          string        `json:"tool_name"`
	Decision          string        `json:"decision"`
	PolicyRuleID      string        `json:"policy_rule_id,omitempty"`
	PolicyReason      string        `json:"policy_reason,omitempty"`
	Latency           time.Duration `json:"latency"`
	ErrorType         string        `json:"error_type"`
	Cost              *float64      `json:"cost,omitempty"`
	InputTokens       int           `json:"input_tokens"`
	OutputTokens      int           `json:"output_tokens"`
	TotalTokens       int           `json:"total_tokens"`
	TraceID           string        `json:"trace_id"`
	RequestID         string        `json:"request_id"`
	ConfigVersion     string        `json:"config_version"`
	EventType         string        `json:"event_type"`
	CreatedAt         time.Time     `json:"created_at"`
}

// Query selects metadata-only events from one exact tenant/application scope.
// Optional filters are intentionally finite and low-cardinality at the API
// boundary; no payload search is supported.
type Query struct {
	TenantID      string
	AppID         string
	EventType     string
	ToolName      string
	TraceID       string
	Limit         int
	Offset        int
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
}

var (
	redactEmailPattern       = regexp.MustCompile(`(?i)\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b`)
	redactPhonePattern       = regexp.MustCompile(`(?:\+?86[ -]?)?1[3-9][0-9]{9}`)
	redactAuthorization      = regexp.MustCompile(`(?i)\b(?:bearer|basic)\s+[A-Z0-9._~+/=\-]+`)
	redactSecretFieldPattern = regexp.MustCompile(`(?i)(\b(?:api[_-]?key|password|passwd|secret|token|authorization|credential|dsn)\b\s*[:=]\s*)("[^"]*"|'[^']*'|[^\s,;]+)`)
	redactURLCredential      = regexp.MustCompile(`(?i)(://[^/\s:@]+:)[^@/\s]+(@)`)
	redactOpenAIKey          = regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{8,}\b`)
)

// RedactString applies the limited PII and credential rules used for audit
// metadata. It intentionally does not normalize or truncate identifiers.
func RedactString(value string) string {
	value = redactEmailPattern.ReplaceAllString(value, "[REDACTED]")
	value = redactPhonePattern.ReplaceAllString(value, "[REDACTED]")
	value = redactAuthorization.ReplaceAllString(value, "[REDACTED]")
	value = redactSecretFieldPattern.ReplaceAllString(value, `${1}[REDACTED]`)
	value = redactURLCredential.ReplaceAllString(value, `${1}[REDACTED]${2}`)
	value = redactOpenAIKey.ReplaceAllString(value, "[REDACTED]")
	return value
}

// SafePolicyReason keeps policy explanations useful without allowing a
// permission callback to persist arbitrary model/tool text.
func SafePolicyReason(value string) string {
	value = strings.TrimSpace(strings.ToLower(RedactString(value)))
	if value == "" {
		return ""
	}
	if len([]rune(value)) > 128 {
		return "policy_blocked"
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' || r == ' ' {
			continue
		}
		return "policy_blocked"
	}
	return value
}

// RedactEvent returns an audit event with user-controlled identity metadata
// redacted. Prompts and raw tool arguments are not part of Event.
func RedactEvent(e Event) Event {
	e.ActorID = RedactString(e.ActorID)
	e.RequestedTenantID = RedactString(e.RequestedTenantID)
	e.RequestedAppID = RedactString(e.RequestedAppID)
	e.UserID = RedactString(e.UserID)
	e.SessionID = RedactString(e.SessionID)
	e.AgentName = RedactString(e.AgentName)
	e.ToolName = RedactString(e.ToolName)
	e.PolicyRuleID = SafePolicyReason(e.PolicyRuleID)
	e.PolicyReason = SafePolicyReason(e.PolicyReason)
	e.ErrorType = RedactString(e.ErrorType)
	return e
}

func (q Query) Validate() error {
	if strings.TrimSpace(q.TenantID) == "" || strings.TrimSpace(q.AppID) == "" {
		return errors.New("tenant_id and app_id are required")
	}
	if q.Limit < 0 || q.Limit > 1000 {
		return errors.New("audit limit is invalid")
	}
	if q.Offset < 0 || q.Offset > 1_000_000 {
		return errors.New("audit offset is invalid")
	}
	if q.CreatedAfter != nil && q.CreatedBefore != nil && q.CreatedAfter.After(*q.CreatedBefore) {
		return errors.New("audit created_after must not be after created_before")
	}
	return nil
}

// Validate checks metadata invariants before it reaches persistence.
func (e Event) Validate() error {
	for name, value := range map[string]string{
		"tenant_id":      e.TenantID,
		"app_id":         e.AppID,
		"event_type":     e.EventType,
		"decision":       e.Decision,
		"request_id":     e.RequestID,
		"config_version": e.ConfigVersion,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if e.Latency < 0 {
		return errors.New("latency must be non-negative")
	}
	if e.InputTokens < 0 || e.OutputTokens < 0 || e.TotalTokens < 0 {
		return errors.New("token usage must be non-negative")
	}
	if e.ResultCount < 0 {
		return errors.New("result count must be non-negative")
	}
	if e.Cost != nil && (*e.Cost < 0 || math.IsNaN(*e.Cost) || math.IsInf(*e.Cost, 0)) {
		return errors.New("cost must be a finite non-negative value")
	}
	return nil
}
