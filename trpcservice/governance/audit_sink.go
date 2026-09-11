package governance

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// LogAuditSink emits governance evidence as structured log records. It carries
// only routing metadata and the arguments digest, never raw tool arguments or
// message content. Durable audit retention is provided by the execution state
// store; this sink exists so every governed tool path has an observable audit
// trail even before a durable sink is attached.
type LogAuditSink struct {
	logger *slog.Logger
}

type DurableToolAuditSink struct {
	recorder storage.AuditRecorder
}

func NewDurableToolAuditSink(recorder storage.AuditRecorder) (*DurableToolAuditSink, error) {
	if recorder == nil {
		return nil, errors.New("durable audit recorder is required")
	}
	return &DurableToolAuditSink{recorder: recorder}, nil
}

func (s *DurableToolAuditSink) RecordToolAudit(ctx context.Context, event ToolAuditEvent) error {
	decision := string(event.Outcome)
	return s.recorder.RecordAudit(ctx, storage.AuditEvent{
		TenantID: event.TenantID, TraceID: event.TraceID, RequestID: event.RequestID,
		Channel: event.Channel, UserID: event.UserID, SessionID: event.SessionID,
		AgentName: event.AgentName, ToolName: event.ToolName,
		Action: "tool." + event.ToolName, Result: decision, Decision: decision,
		LatencyMS: event.LatencyMS, ErrorType: event.ErrorType,
		Detail:    fmt.Sprintf("arguments_sha256=%s policy_version=%s", event.ArgumentsDigest, event.PolicyVersion),
		CreatedAt: event.OccurredAt,
	})
}

type MultiAuditSink struct {
	sinks []AuditSink
}

func NewMultiAuditSink(sinks ...AuditSink) (*MultiAuditSink, error) {
	for _, sink := range sinks {
		if sink == nil {
			return nil, errors.New("audit sink is required")
		}
	}
	return &MultiAuditSink{sinks: append([]AuditSink(nil), sinks...)}, nil
}

func (s *MultiAuditSink) RecordToolAudit(ctx context.Context, event ToolAuditEvent) error {
	var errs []error
	for _, sink := range s.sinks {
		if err := sink.RecordToolAudit(ctx, event); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// NewLogAuditSink constructs a structured audit sink. A nil logger selects the
// default logger.
func NewLogAuditSink(logger *slog.Logger) *LogAuditSink {
	if logger == nil {
		logger = slog.Default()
	}
	return &LogAuditSink{logger: logger}
}

// RecordToolAudit writes one redacted tool decision record.
func (s *LogAuditSink) RecordToolAudit(_ context.Context, event ToolAuditEvent) error {
	if s == nil || s.logger == nil {
		return nil
	}
	s.logger.Info("tool_audit",
		"tenant_id", event.TenantID,
		"trace_id", event.TraceID,
		"request_id", event.RequestID,
		"channel", event.Channel,
		"user_id", event.UserID,
		"session_id", event.SessionID,
		"agent_name", event.AgentName,
		"tool_name", event.ToolName,
		"decision", string(event.Outcome),
		"latency_ms", event.LatencyMS,
		"error_type", event.ErrorType,
		"policy_version", event.PolicyVersion,
		"arguments_digest", event.ArgumentsDigest,
	)
	return nil
}
