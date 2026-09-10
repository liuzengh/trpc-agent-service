package audit

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type producerWriter struct {
	events []Event
	err    error
}

func (w *producerWriter) Append(_ context.Context, event Event) (AppendResult, error) {
	w.events = append(w.events, event)
	if w.err != nil {
		return AppendResult{}, w.err
	}
	return AppendResult{Event: event}, nil
}

func TestRecorderDerivesBoundedStableIDsAndTenantScope(t *testing.T) {
	w := &producerWriter{}
	now := time.Date(2026, 8, 26, 1, 2, 3, 0, time.UTC)
	r := NewRecorder(w, "tenant-a", WithRecorderClock(func() time.Time { return now }))
	if err := r.Record(context.Background(), Event{EventType: EventIMIngressAccepted, RequestID: strings.Repeat("r", 256), TraceID: "trace", UserID: "user", SessionID: "session", Decision: DecisionAccepted}); err != nil {
		t.Fatal(err)
	}
	if len(w.events) != 1 || w.events[0].TenantID != "tenant-a" || len(w.events[0].EventID) > 256 || w.events[0].OccurredAt != now {
		t.Fatalf("recorded event = %#v", w.events)
	}
	firstID := NewEventID("a", "b")
	if firstID != NewEventID("a", "b") || firstID == NewEventID("ab") {
		t.Fatal("event ID derivation is not stable and length-delimited")
	}
}

func TestRecorderPropagatesWriterFailure(t *testing.T) {
	w := &producerWriter{err: errors.New("storage unavailable")}
	r := NewRecorder(w, "tenant-a")
	err := r.BudgetRejected(context.Background(), "request", "trace")
	if !errors.Is(err, ErrWriteFailed) || !strings.Contains(err.Error(), "storage unavailable") {
		t.Fatalf("writer failure = %v", err)
	}
}

func TestRecorderConvenienceProducersAndNoop(t *testing.T) {
	w := &producerWriter{}
	r := NewRecorder(w, "tenant-a")
	previous, next := int64(1), int64(2)
	if err := r.Record(context.Background(), Event{EventType: EventControlPlaneChanged, ActorType: "admin", ActorID: "actor", Reason: "changed", CorrelationID: "corr", PreviousVersion: &previous, NextVersion: &next}); err != nil {
		t.Fatal(err)
	}
	if err := r.Record(context.Background(), Event{EventType: EventToolDenied, RequestID: "req", TraceID: "trace", ToolName: "tool", Decision: DecisionDeny, ErrorType: string(ErrorTool)}); err != nil {
		t.Fatal(err)
	}
	if err := r.ToolExecuted(context.Background(), "req", "trace", "tool"); err != nil {
		t.Fatal(err)
	}
	if err := r.BudgetRejected(context.Background(), "req", "trace"); err != nil {
		t.Fatal(err)
	}
	if err := r.Redacted(context.Background(), "req", "trace"); err != nil {
		t.Fatal(err)
	}
	if err := r.Record(context.Background(), Event{EventType: EventIMDeliverySent, RequestID: "req", TraceID: "trace", UserID: "user", SessionID: "session", Decision: DecisionAccepted}); err != nil {
		t.Fatal(err)
	}
	if err := r.Fallback(context.Background(), "req", "trace"); err != nil {
		t.Fatal(err)
	}
	if err := r.IMAuthorization(context.Background(), "req", "trace", "user", "session", true); err != nil {
		t.Fatal(err)
	}
	if err := r.IMAuthorization(context.Background(), "req", "trace", "user", "session", false); err != nil {
		t.Fatal(err)
	}
	if err := r.IMReconciled(context.Background(), "req", "trace", ""); err != nil {
		t.Fatal(err)
	}
	if len(w.events) != 10 {
		t.Fatalf("events = %d", len(w.events))
	}
	if err := (Recorder{}).Record(context.Background(), Event{}); err != nil {
		t.Fatal(err)
	}
	if err := r.IMReconciled(context.Background(), "req", "trace", string(ErrorUnavailable)); err != nil {
		t.Fatal(err)
	}
	var nilCtx context.Context
	if err := r.Record(nilCtx, Event{EventType: EventContentRedacted}); !errors.Is(err, ErrWriteFailed) {
		t.Fatalf("nil context err=%v", err)
	}
}

func TestRecorderRejectsCrossTenantEventsBeforeWriting(t *testing.T) {
	writer := &producerWriter{}
	recorder := NewRecorder(writer, " tenant-a ")
	for _, tenantID := range []string{"", "tenant-a", "tenant-b"} {
		err := recorder.Record(context.Background(), Event{
			TenantID: tenantID, EventType: EventContentRedacted, RequestID: "request",
		})
		if tenantID == "tenant-b" {
			if !errors.Is(err, ErrTenantScope) {
				t.Fatalf("cross-tenant event error = %v", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if len(writer.events) != 2 {
		t.Fatalf("writer received %d events", len(writer.events))
	}
	for _, event := range writer.events {
		if event.TenantID != "tenant-a" {
			t.Fatalf("recorded tenant = %q", event.TenantID)
		}
	}
	if err := NewRecorder(writer, "").Record(context.Background(), Event{TenantID: "tenant-a"}); !errors.Is(err, ErrTenantScope) {
		t.Fatalf("unscoped recorder error = %v", err)
	}
}

func TestRecorderFixedTimeCopyKeepsOriginalClock(t *testing.T) {
	writer := &producerWriter{}
	now := time.Date(2026, 9, 7, 0, 0, 0, 0, time.UTC)
	original := NewRecorder(writer, "tenant-a", WithRecorderClock(func() time.Time { return now }))
	fixed := original.WithFixedTime()
	now = now.Add(time.Hour)
	for _, recorder := range []Recorder{fixed, fixed, original} {
		if err := recorder.Redacted(context.Background(), "request", "trace"); err != nil {
			t.Fatal(err)
		}
	}
	if !writer.events[0].OccurredAt.Equal(writer.events[1].OccurredAt) || !writer.events[2].OccurredAt.Equal(now) || writer.events[0].OccurredAt.Equal(now) {
		t.Fatalf("fixed-time copy changed original clock: %+v", writer.events)
	}
}
