package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

// ErrWriteFailed identifies a mandatory producer fact that could not be
// persisted. Callers must surface this error instead of treating the action
// as a successful compliance decision.
var ErrWriteFailed = errors.New("audit write failed")

// Recorder turns protocol/domain outcomes into validated tenant-scoped audit
// events. A nil writer deliberately makes recording a no-op for deployments
// that have not enabled audit persistence yet.
type Recorder struct {
	writer   Writer
	tenantID string
	now      func() time.Time
}

// RecorderOption configures a recorder at construction time.
type RecorderOption func(*Recorder)

// WithRecorderClock supplies the clock used for events without a timestamp.
// A nil clock uses time.Now. Later options take precedence.
func WithRecorderClock(now func() time.Time) RecorderOption {
	return func(recorder *Recorder) { recorder.now = now }
}

// NewRecorder creates a recorder with a fixed tenant scope. A nil writer makes
// recording a no-op, as does the zero Recorder value. An enabled recorder
// rejects events outside its scope before calling the writer.
func NewRecorder(writer Writer, tenantID string, options ...RecorderOption) Recorder {
	recorder := Recorder{writer: writer, tenantID: strings.TrimSpace(tenantID)}
	for _, option := range options {
		if option != nil {
			option(&recorder)
		}
	}
	return recorder
}

// WithFixedTime returns a copy whose default event timestamp is captured now.
// A caller can reuse this copy for idempotent retries without changing the
// original recorder or exposing its clock.
func (r Recorder) WithFixedTime() Recorder {
	at := r.currentTime()
	r.now = func() time.Time { return at }
	return r
}

func (r Recorder) currentTime() time.Time {
	if r.now != nil {
		return r.now().UTC()
	}
	return time.Now().UTC()
}

// Record appends an audit event with the recorder tenant scope.
func (r Recorder) Record(ctx context.Context, event Event) error {
	if r.writer == nil {
		return nil
	}
	if ctx == nil {
		return ErrWriteFailed
	}
	if event.TenantID == "" {
		event.TenantID = r.tenantID
	}
	if r.tenantID == "" || event.TenantID != r.tenantID {
		return ErrTenantScope
	}
	if event.SchemaVersion == 0 {
		event.SchemaVersion = SchemaVersion
	}
	if event.EventID == "" {
		event.EventID = NewEventID(string(event.EventType), event.RequestID, event.TraceID, event.CorrelationID)
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = r.currentTime()
	}
	if err := event.Validate(); err != nil {
		return err
	}
	if _, err := r.writer.Append(ctx, event); err != nil {
		return errors.Join(ErrWriteFailed, err)
	}
	return nil
}

// NewEventID returns a deterministic, non-sensitive identifier suitable for
// idempotent retries. Inputs are length-delimited before hashing.
func NewEventID(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		h.Write([]byte{byte(len(part) >> 24), byte(len(part) >> 16), byte(len(part) >> 8), byte(len(part))})
		h.Write([]byte(part))
	}
	return "audit_" + hex.EncodeToString(h.Sum(nil))[:32]
}

// ToolExecuted records a successful tool execution without persisting its
// arguments, result, or any provider-specific media metadata.
func (r Recorder) ToolExecuted(ctx context.Context, requestID, traceID, toolName string) error {
	return r.Record(ctx, Event{EventType: EventToolExecuted, RequestID: requestID, TraceID: traceID, ToolName: toolName, Decision: DecisionAccepted})
}

// BudgetRejected records a budget rejection.
func (r Recorder) BudgetRejected(ctx context.Context, requestID, traceID string) error {
	return r.Record(ctx, Event{EventType: EventBudgetRejected, RequestID: requestID, TraceID: traceID, Decision: DecisionRejected, ErrorType: string(ErrorBudget)})
}

// Redacted records content redaction.
func (r Recorder) Redacted(ctx context.Context, requestID, traceID string) error {
	return r.Record(ctx, Event{EventType: EventContentRedacted, RequestID: requestID, TraceID: traceID, Decision: DecisionAccepted, ErrorType: string(ErrorRedacted)})
}

// Fallback records provider fallback.
func (r Recorder) Fallback(ctx context.Context, requestID, traceID string) error {
	return r.Record(ctx, Event{EventType: EventExecutionFallback, RequestID: requestID, TraceID: traceID, Decision: DecisionAccepted})
}

// IMAuthorization records an instant-message authorization decision.
func (r Recorder) IMAuthorization(ctx context.Context, requestID, traceID, userID, sessionID string, allowed bool) error {
	eventType, decision := EventIMAuthorizationDenied, DecisionRejected
	if allowed {
		eventType, decision = EventIMAuthorizationAllowed, DecisionAccepted
	}
	return r.Record(ctx, Event{EventType: eventType, RequestID: requestID, TraceID: traceID, UserID: userID, SessionID: sessionID, Decision: decision})
}

// IMReconciled records instant-message reconciliation.
func (r Recorder) IMReconciled(ctx context.Context, requestID, traceID, errorType string) error {
	decision := DecisionAccepted
	if errorType != "" {
		decision = DecisionRejected
	}
	return r.Record(ctx, Event{EventType: EventIMDeliveryReconciled, RequestID: requestID, TraceID: traceID, Decision: decision, ErrorType: errorType})
}
