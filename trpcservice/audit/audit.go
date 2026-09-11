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
	// CostMicroCents is the model cost in microcents (1 ¥ = 1_000_000 μ¢),
	// computed at dispatch time from token counts × model per-unit pricing.
	CostMicroCents int64 `json:"cost_microcents,omitempty"`
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

	// Rotation state; maxSizeBytes == 0 keeps the historical single
	// append-only file.
	path         string
	maxSizeBytes int64
	maxBackups   int
}

// Options configures NewWithOptions.
//
// MaxSizeMB > 0 turns on size-based rotation: once the current file reaches
// the size it is renamed to path.1, older generations shift up, and a fresh
// file is opened. MaxBackups bounds how many generations are kept and
// defaults to 3 when rotation is on; 0 (or negative) with rotation on means
// the default, not "keep nothing".
//
// The trail format never changes — rotation is renames plus a reopen — so an
// operator's own logrotate remains interchangeable with it.
type Options struct {
	Path       string
	MaxSizeMB  int
	MaxBackups int
}

// New opens the audit trail at path; an empty path yields a log-only logger.
// It is the rotation-free form of NewWithOptions.
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
	return NewWithOptions(Options{Path: path})
}

// NewWithOptions opens the audit trail with optional size-based rotation.
// An empty path yields a log-only logger.
func NewWithOptions(opts Options) (*Logger, error) {
	if opts.Path == "" {
		return &Logger{}, nil
	}
	if opts.MaxSizeMB > 0 && opts.MaxBackups <= 0 {
		opts.MaxBackups = 3
	}
	f, err := os.OpenFile(opts.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open audit file %s: %w", opts.Path, err)
	}
	return &Logger{
		f:            f,
		w:            bufio.NewWriterSize(f, 64*1024),
		path:         opts.Path,
		maxSizeBytes: int64(opts.MaxSizeMB) << 20,
		maxBackups:   opts.MaxBackups,
	}, nil
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
		l.rotateIfNeededLocked()
	}
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

// rotateIfNeededLocked rolls the file over once it has reached maxSizeBytes.
// The check runs before every append while the lock is held: the audit trail
// already flushes per record (durability over throughput), and one Stat next
// to that Flush is the same order of cost. A Stat failure keeps the current
// file serving — rotation is housekeeping, not a reason to lose a record.
func (l *Logger) rotateIfNeededLocked() {
	if l.maxSizeBytes <= 0 {
		return
	}
	info, err := l.f.Stat()
	if err != nil || info.Size() < l.maxSizeBytes {
		return
	}
	l.rotateLocked()
}

// rotateLocked shifts path.(N) to path.(N+1), moves the current file to
// path.1 and reopens a fresh one; the oldest generation falls off by being
// overwritten. If the reopen fails the logger degrades to log-only and says
// so: a filesystem problem must not take the process down, and the record
// still reaches slog rather than disappearing.
func (l *Logger) rotateLocked() {
	_ = l.w.Flush()
	_ = l.f.Close()
	for i := l.maxBackups - 1; i >= 1; i-- {
		_ = os.Rename(backupPath(l.path, i), backupPath(l.path, i+1))
	}
	_ = os.Rename(l.path, backupPath(l.path, 1))
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		slog.Error("audit: rotation could not reopen the trail; continuing log-only",
			"path", l.path, "err", err)
		l.f, l.w = nil, nil
		return
	}
	l.f = f
	l.w = bufio.NewWriterSize(f, 64*1024)
}

// backupPath is the rotated generation name: audit.jsonl.1, .2, ...
func backupPath(path string, n int) string {
	return fmt.Sprintf("%s.%d", path, n)
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
