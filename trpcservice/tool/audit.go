package tool

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// AuditPhase says which side of a tool execution an event describes.
type AuditPhase string

const (
	// AuditPhaseBefore is emitted when a tool call is admitted, before the tool
	// runs.
	AuditPhaseBefore AuditPhase = "before"
	// AuditPhaseAfter is emitted when the tool has returned, whether or not it
	// succeeded.
	AuditPhaseAfter AuditPhase = "after"
)

// AuditScope is the trusted identity of the run that issued a tool call. It is
// read from the platform RunContext, never from anything the model or the
// client supplied.
type AuditScope struct {
	// RequestID ties this tool call back to the single request that caused it.
	// Without it the trail can say which session ran a tool but not which of
	// that session's requests, and a caller holding an X-Request-ID has no way
	// to reach the tool calls its own request made.
	RequestID   string
	TenantID    string
	AppID       string
	PrincipalID string
	SessionID   string
	RevisionID  string
}

// AuditEvent is one record of the tool trail.
//
// Every field here is either platform-issued identity or a fact about the call
// itself. Deliberately absent: arguments, results, error text, tool
// descriptions, and anything derived from them. The trail answers "who ran
// which tool, when, and did it work" — it is not a copy of the conversation,
// and it is written to a log that has none of the retention and access rules
// the conversation content has.
//
// Succeeded, Duration and DurationValid describe a completed execution and are
// meaningful only in AuditPhaseAfter.
type AuditEvent struct {
	Phase      AuditPhase
	Scope      AuditScope
	ScopeValid bool
	ToolName   string
	ToolCallID string
	Succeeded  bool
	Duration   time.Duration
	// DurationValid reports whether Duration was measured. A missing start is
	// reported as unmeasured rather than as a zero duration, which would read
	// as an instant call.
	DurationValid bool
}

// AuditSink receives tool audit events. It is an interface so the trail can be
// asserted in a test and redirected in a deployment, without any of it
// depending on the process-wide logger.
//
// Record must not panic and must not block; the caller runs on the tool
// execution path. A sink that panics anyway is contained, not trusted — see
// recordAudit.
type AuditSink interface {
	Record(ctx context.Context, event AuditEvent)
}

// SlogAuditSink writes events to a *slog.Logger. It is the production default.
type SlogAuditSink struct {
	logger *slog.Logger
}

// NewSlogAuditSink returns a sink writing to logger, or to slog.Default() when
// logger is nil. The logger is held rather than looked up per call, so a
// deployment can give the trail its own handler without replacing the global
// one.
func NewSlogAuditSink(logger *slog.Logger) *SlogAuditSink {
	return &SlogAuditSink{logger: logger}
}

// Record writes one event as a structured log line.
func (s *SlogAuditSink) Record(ctx context.Context, event AuditEvent) {
	logger := s.logger
	if logger == nil {
		logger = slog.Default()
	}
	attributes := []any{
		"phase", string(event.Phase),
		"tool", event.ToolName,
		"tool_call_id", event.ToolCallID,
		"scope_valid", event.ScopeValid,
		"request_id", event.Scope.RequestID,
		"tenant_id", event.Scope.TenantID,
		"app_id", event.Scope.AppID,
		"principal_id", event.Scope.PrincipalID,
		"session_id", event.Scope.SessionID,
		"revision_id", event.Scope.RevisionID,
	}
	if event.Phase == AuditPhaseAfter {
		attributes = append(attributes, "success", event.Succeeded)
		if event.DurationValid {
			attributes = append(attributes, "duration_ms", event.Duration.Milliseconds())
		}
	}
	logger.InfoContext(ctx, "tool call", attributes...)
}

// auditStartKey carries the start time from the before callback to the after
// callback.
//
// The framework threads the context returned by the before callback through the
// tool execution and into the after callback, and it does that per tool call.
// So the start time rides the one context branch that belongs to this call:
// concurrent calls in the same turn cannot collide, a call that never completes
// leaves nothing behind, and nothing has to be keyed by tool name — which would
// be wrong the moment a model calls the same tool twice in one turn.
type auditStartKey struct{}

// NewAuditCallbacks returns tool callbacks that write one before event and one
// after event per tool call.
//
// The callbacks are observers. They never return an error, never return a
// custom result, and never modify arguments, so the audit trail cannot change
// what the agent does. That is not a stylistic choice: the framework treats a
// callback error as a tool failure, and turns a callback panic into one too, so
// anything less careful here would let a broken sink take down tool execution.
//
// A nil sink falls back to slog.
func NewAuditCallbacks(sink AuditSink) *agenttool.Callbacks {
	if sink == nil {
		sink = NewSlogAuditSink(nil)
	}
	callbacks := agenttool.NewCallbacks()
	callbacks.BeforeTool = append(callbacks.BeforeTool,
		func(ctx context.Context, args *agenttool.BeforeToolArgs) (*agenttool.BeforeToolResult, error) {
			if ctx == nil || args == nil {
				return nil, nil
			}
			startedAt := time.Now()
			recordAudit(ctx, sink, auditEvent(ctx, AuditPhaseBefore, args.ToolName, args.ToolCallID))
			// Only the context is set: no CustomResult, so the tool still runs,
			// and no ModifiedArguments, so it runs on what the model sent.
			return &agenttool.BeforeToolResult{
				Context: context.WithValue(ctx, auditStartKey{}, startedAt),
			}, nil
		})
	callbacks.AfterTool = append(callbacks.AfterTool,
		func(ctx context.Context, args *agenttool.AfterToolArgs) (*agenttool.AfterToolResult, error) {
			// An empty result, never a nil one. A nil return leaves the
			// framework with no result from any callback, and it then
			// reconstructs one by promoting the tool's own output to a
			// CustomResult (tool/callbacks.go, finalizeAfterToolResult). That
			// round trip is a no-op today only because the value it promotes is
			// identical; returning an empty result means no substitution is
			// attempted at all, which is what an observer should do. It also
			// avoids the nil dereference that path takes when args is nil.
			empty := &agenttool.AfterToolResult{}
			if ctx == nil || args == nil {
				return empty, nil
			}
			event := auditEvent(ctx, AuditPhaseAfter, args.ToolName, args.ToolCallID)
			// Whether the call failed is recorded; why it failed is not. The
			// error text is tool output and can quote the arguments.
			event.Succeeded = args.Error == nil
			if startedAt, ok := ctx.Value(auditStartKey{}).(time.Time); ok {
				event.Duration = time.Since(startedAt)
				event.DurationValid = true
			}
			recordAudit(ctx, sink, event)
			return empty, nil
		})
	return callbacks
}

// auditEvent builds an event from the trusted scope on the context.
//
// A run with no RunContext is recorded with ScopeValid false rather than
// dropped or failed. The tool call itself is refused elsewhere — contextRunner
// rejects an untrusted run before the agent starts — so an unscoped event here
// means a path that reached a tool some other way, which is exactly the thing
// an audit trail should show rather than suppress.
func auditEvent(ctx context.Context, phase AuditPhase, toolName string, toolCallID string) AuditEvent {
	event := AuditEvent{Phase: phase, ToolName: toolName, ToolCallID: toolCallID}
	runContext, err := identity.RunContextFrom(ctx)
	if err != nil {
		return event
	}
	event.ScopeValid = true
	event.Scope = AuditScope{
		RequestID:   runContext.RequestID,
		TenantID:    runContext.TenantID,
		AppID:       runContext.AppID,
		PrincipalID: runContext.PrincipalID,
		SessionID:   runContext.SessionID,
		RevisionID:  runContext.RevisionID,
	}
	return event
}

// recordAudit delivers one event and contains a sink that misbehaves.
//
// The recover is deliberate. Without it, the framework's own recovery turns a
// panicking sink into a callback error, and a callback error fails the tool —
// so a logging defect would become an outage. The trail is allowed to lose a
// record; the request is not allowed to lose a tool.
//
// Only the panic value's type is reported. The sink is reached from the tool
// execution path, and a formatted panic value is the one place payload could
// re-enter a log line that is otherwise free of it.
func recordAudit(ctx context.Context, sink AuditSink, event AuditEvent) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.ErrorContext(
				ctx,
				"tool audit sink panicked",
				"tool", event.ToolName,
				"tool_call_id", event.ToolCallID,
				"panic_type", fmt.Sprintf("%T", recovered),
			)
		}
	}()
	sink.Record(ctx, event)
}
