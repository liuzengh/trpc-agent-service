// Package tool registers platform tools and tenant-scoped MCP / function tools.
package tool

import (
	"context"
	"errors"
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
)

var (
	// ErrDenied reports a denied tool request.
	ErrDenied = errors.New("tool denied")
	// ErrApprovalRequired reports a tool request requiring approval.
	ErrApprovalRequired = errors.New("tool approval required")
)

// Decision records the tool admission result.
type Decision string

const (
	// Allow permits tool execution.
	Allow Decision = "allow"
	// Deny rejects tool execution.
	Deny Decision = "deny"
	// ApprovalRequired requires explicit tool approval.
	ApprovalRequired Decision = "approval_required"
)

// Policy is the provider-neutral tool admission boundary. It records only the
// tool name and decision; arguments and results never enter the audit event.
type Policy struct {
	Recorder audit.Recorder
	Allowed  map[string]Decision
}

// Decide evaluates and audits a tool request.
func (p Policy) Decide(ctx context.Context, requestID, traceID, toolName string) (Decision, error) {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" || len([]rune(toolName)) > 256 {
		return "", audit.ErrInvalid
	}
	decision := Deny
	if value, ok := p.Allowed[toolName]; ok {
		decision = value
	}
	var err error
	switch decision {
	case Allow:
		err = nil
	case ApprovalRequired:
		err = ErrApprovalRequired
	default:
		decision = Deny
		err = ErrDenied
	}
	eventType := audit.EventToolDenied
	switch decision {
	case Allow:
		eventType = audit.EventToolAllowed
	case ApprovalRequired:
		eventType = audit.EventToolApprovalRequired
	}
	if auditErr := p.Recorder.Record(ctx, audit.Event{
		EventType: eventType, RequestID: requestID, TraceID: traceID,
		ToolName: toolName, Decision: audit.Decision(decision),
	}); auditErr != nil {
		return "", audit.ErrWriteFailed
	}
	return decision, err
}
