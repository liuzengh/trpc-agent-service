// Package audit writes the append-only governance trail (proposal doc 3.5):
// every inbound message, admission throttle, guardrail decision, model call,
// reply, and admin mutation lands here with the tenant context and trace id
// attached.
package audit

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// Event kinds written to the trail.
const (
	EventInbound        = "inbound"
	EventGuardrailBlock = "guardrail_block"
	EventThrottled      = "throttled" // rejected by the tenant concurrency quota
	EventModelCall      = "model_call"
	EventReply          = "reply"
	EventAdmin          = "admin"
	EventTool           = "tool"
)

// Decisions recorded alongside events.
const (
	DecisionAllow = "allow"
	DecisionBlock = "block"
	DecisionOK    = "ok"
	DecisionError = "error"
)

// Record is one audit line. The field set follows the proposal doc 3.5
// audit table (tenant_id / channel / user_id / session_id / agent_name /
// tool_name / decision / latency / error_type / cost / trace_id); cost is
// represented by the token counters.
type Record struct {
	Time      time.Time `json:"ts"`
	TraceID   string    `json:"trace_id,omitempty"`
	Event     string    `json:"event"`
	TenantID  string    `json:"tenant_id,omitempty"`
	Channel   string    `json:"channel,omitempty"`
	UserID    string    `json:"user_id,omitempty"`
	SessionID string    `json:"session_id,omitempty"`
	AgentName string    `json:"agent_name,omitempty"`
	ToolName  string    `json:"tool_name,omitempty"`
	Decision  string    `json:"decision,omitempty"`
	Stage     string    `json:"stage,omitempty"` // admission|input|output
	Rule      string    `json:"rule,omitempty"`  // concurrency|length|keyword
	LatencyMS int64     `json:"latency_ms,omitempty"`
	// PromptTokens/CompletionTokens carry the cost dimension of the trail.
	PromptTokens     int    `json:"prompt_tokens,omitempty"`
	CompletionTokens int    `json:"completion_tokens,omitempty"`
	ErrorType        string `json:"error_type,omitempty"`
	Detail           string `json:"detail,omitempty"`
}

// Logger appends JSONL records to the configured file and echoes them
// through the structured log.
//
// There are two distinct "no file" states, and conflating them is what broke
// log-only auditing once already:
//
//	nil *Logger      — true no-op. Valid, so callers that hand-build a
//	                   Governance{} or a Service without an auditor never need
//	                   nil checks at the call sites.
//	&Logger{w: nil}  — log-only. Records still reach slog, which is what
//	                   New("") returns.
type Logger struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer
}

// New opens the audit trail at path; an empty path yields a log-only logger.
//
// Log-only is a real deployment shape, not a degraded one: in a container the
// filesystem is ephemeral and often read-only, while stdout is picked up by the
// cluster's log pipeline and is durable. Returning nil for an empty path — what
// this used to do — silently disabled the entire governance trail, because a
// nil *Logger is a no-op by design. The promise in this comment, in Logger's,
// in admin.NewService's and in config.example.yaml all said "log-only"; only
// the code disagreed, and the test covering it asserted no more than that err
// was nil.
func New(path string) (*Logger, error) {
	if path == "" {
		return &Logger{}, nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open audit file %s: %w", path, err)
	}
	return &Logger{f: f, w: bufio.NewWriterSize(f, 64*1024)}, nil
}

// Log appends one record. Records are flushed per call: audit durability
// beats throughput at this platform's message rate.
func (l *Logger) Log(rec Record) {
	if l == nil {
		return
	}
	if rec.Time.IsZero() {
		rec.Time = time.Now()
	}
	data, err := json.Marshal(rec)
	if err != nil {
		slog.Error("audit marshal failed", "event", rec.Event, "error", err)
		return
	}
	l.mu.Lock()
	if l.w != nil {
		l.w.Write(data)
		l.w.WriteByte('\n')
		l.w.Flush()
	}
	l.mu.Unlock()

	attrs := []any{"event", rec.Event, "tenant", rec.TenantID, "decision", rec.Decision}
	if rec.TraceID != "" {
		attrs = append(attrs, "trace_id", rec.TraceID)
	}
	if rec.Stage != "" {
		attrs = append(attrs, "stage", rec.Stage, "rule", rec.Rule)
	}
	if rec.ErrorType != "" {
		attrs = append(attrs, "error_type", rec.ErrorType)
	}
	slog.Info("audit", attrs...)
}

// Close flushes and closes the underlying file, if any.
func (l *Logger) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.w == nil {
		return nil
	}
	err := l.w.Flush()
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	l.w = nil
	return err
}
