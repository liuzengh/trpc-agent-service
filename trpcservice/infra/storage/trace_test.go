package storage

import (
	"context"
	"errors"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// testTP / testRec are process-wide: the OTel global provider resolves its
// delegate once, so installing a provider per test would silently keep sending
// spans to the previous (shut-down) one. One provider + a reset recorder per
// test mirrors production (a single provider per process) and stays honest.
var (
	testTPOnce sync.Once
	testTP     *sdktrace.TracerProvider
	testRec    *tracetest.SpanRecorder
)

// withSpanRecorder returns the shared recorder with its span buffer cleared, so
// assertions read only the spans the decorators emitted for this test.
func withSpanRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	testTPOnce.Do(func() {
		testRec = tracetest.NewSpanRecorder()
		testTP = sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(testRec))
		otel.SetTracerProvider(testTP)
	})
	testRec.Reset()
	return testRec
}

// spanNames lists the recorded span names.
func spanNames(rec *tracetest.SpanRecorder) []string {
	spans := rec.Ended()
	out := make([]string, 0, len(spans))
	for _, s := range spans {
		out = append(out, s.Name())
	}
	return out
}

// attrOf returns the string value of one span attribute.
func attrOf(s sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range s.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}

// TestSessionOperationsAreTraced covers the difficulty-5 gap: the shared state
// layer must be visible in the trace, otherwise a turn looks like it jumps from
// agent.run straight to the next tool call with no session I/O in between.
func TestSessionOperationsAreTraced(t *testing.T) {
	rec := withSpanRecorder(t)
	svc := withTracingSessions(sessionInMemoryForTest(t), BackendInMemory)
	ctx := context.Background()
	key := session.Key{AppName: "t1", UserID: "u1", SessionID: "s1"}

	if _, err := svc.CreateSession(ctx, key, session.StateMap{}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := svc.GetSession(ctx, key); err != nil {
		t.Fatalf("get: %v", err)
	}
	sess, _ := svc.GetSession(ctx, key)
	ev := event.New("inv-1", "user")
	if err := svc.AppendEvent(ctx, sess, ev); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := svc.UpdateSessionState(ctx, key, session.StateMap{"k": []byte(`"v"`)}); err != nil {
		t.Fatalf("update state: %v", err)
	}
	if _, err := svc.ListSessions(ctx, session.UserKey{AppName: "t1", UserID: "u1"}); err != nil {
		t.Fatalf("list: %v", err)
	}
	if err := svc.DeleteSession(ctx, key); err != nil {
		t.Fatalf("delete: %v", err)
	}

	want := []string{
		"session.create", "session.get", "session.get", "session.append_event",
		"session.update_session_state", "session.list", "session.delete",
	}
	if got := spanNames(rec); len(got) != len(want) {
		t.Fatalf("spans = %v, want %v", got, want)
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("span[%d] = %q, want %q", i, got[i], want[i])
			}
		}
	}

	// Tenant + session identity must be on the span, otherwise a slow backend
	// cannot be attributed to the tenant that suffered it.
	var sawAttrs bool
	for _, s := range rec.Ended() {
		if s.Name() != "session.append_event" {
			continue
		}
		sawAttrs = true
		if got := attrOf(s, "session.app_name"); got != "t1" {
			t.Errorf("session.app_name = %q, want t1", got)
		}
		if got := attrOf(s, "event.request_id"); got != ev.RequestID {
			t.Errorf("event.request_id = %q, want %q", got, ev.RequestID)
		}
		if got := attrOf(s, "storage.backend"); got != string(BackendInMemory) {
			t.Errorf("storage.backend = %q, want %q", got, BackendInMemory)
		}
	}
	if !sawAttrs {
		t.Error("no session.append_event span recorded")
	}
}

// TestSessionSummaryOperationsAreTraced covers the summary half of the session
// service: it is the operation that explains "why did this turn get cheaper"
// and it had no observability at all.
func TestSessionSummaryOperationsAreTraced(t *testing.T) {
	rec := withSpanRecorder(t)
	svc := withTracingSessions(sessionInMemoryForTest(t), BackendInMemory)
	ctx := context.Background()
	key := session.Key{AppName: "t1", UserID: "u1", SessionID: "s1"}
	sess, err := svc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	_ = svc.EnqueueSummaryJob(ctx, sess, "", false)
	if _, ok := svc.GetSessionSummaryText(ctx, sess); ok {
		t.Error("unexpected summary on a fresh session")
	}

	names := spanNames(rec)
	var sawEnqueue, sawText bool
	for _, n := range names {
		switch n {
		case "session.enqueue_summary":
			sawEnqueue = true
		case "session.summary_text":
			sawText = true
		}
	}
	if !sawEnqueue || !sawText {
		t.Errorf("spans = %v, want both summary operations traced", names)
	}
}

// TestSessionSpanRecordsBackendErrors keeps failure visible: a failing backend
// must produce an error span (status + recorded error), not a silent fast span.
func TestSessionSpanRecordsBackendErrors(t *testing.T) {
	rec := withSpanRecorder(t)
	svc := withTracingSessions(failingSessions{err: errors.New("redis: connection refused")}, BackendRedis)
	ctx := context.Background()

	err := svc.DeleteSession(ctx, session.Key{AppName: "t1", UserID: "u1", SessionID: "s1"})
	if err == nil {
		t.Fatal("expected the backend error to surface")
	}

	end := rec.Ended()
	if len(end) != 1 {
		t.Fatalf("spans = %d, want 1", len(end))
	}
	if end[0].Status().Code.String() != "Error" {
		t.Errorf("span status = %v, want Error", end[0].Status().Code)
	}
	if len(end[0].Events()) == 0 {
		t.Error("expected the backend error to be recorded on the span")
	}
	if got := attrOf(end[0], "storage.backend"); got != string(BackendRedis) {
		t.Errorf("storage.backend = %q, want %q", got, BackendRedis)
	}
}

// TestMemoryOperationsAreTraced covers the other half of the gap: preload
// reads, semantic search and agent-driven writes.
func TestMemoryOperationsAreTraced(t *testing.T) {
	rec := withSpanRecorder(t)
	svc := withTracingMemories(memoryInMemoryForTest(t), BackendMySQL)
	ctx := context.Background()
	uk := memory.UserKey{AppName: "t1", UserID: "u1"}

	if err := svc.AddMemory(ctx, uk, "prefers americano", []string{"preference"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	entries, err := svc.ReadMemories(ctx, uk, 10)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1 (the decorator must delegate, not swallow)", len(entries))
	}
	if _, err := svc.SearchMemories(ctx, uk, "americano"); err != nil {
		t.Fatalf("search: %v", err)
	}
	if svc.Tools() == nil {
		t.Error("Tools must still return the service's tool list")
	}

	want := []string{"memory.add", "memory.read", "memory.search"}
	got := spanNames(rec)
	// Tools() is intentionally unspanned; assert the I/O ops in order.
	if len(got) != len(want) {
		t.Fatalf("spans = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("span[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	for _, s := range rec.Ended() {
		if s.Name() == "memory.read" {
			if got := attrOf(s, "memory.app_name"); got != "t1" {
				t.Errorf("memory.app_name = %q, want t1", got)
			}
			if got := attrOf(s, "storage.backend"); got != string(BackendMySQL) {
				t.Errorf("storage.backend = %q, want %q", got, BackendMySQL)
			}
		}
	}
}

// TestTracingWrappersAreNilSafe keeps the Router's optional paths safe.
func TestTracingWrappersAreNilSafe(t *testing.T) {
	if got := withTracingSessions(nil, BackendInMemory); got != nil {
		t.Error("wrapping a nil session service must stay nil")
	}
	if got := withTracingMemories(nil, BackendInMemory); got != nil {
		t.Error("wrapping a nil memory service must stay nil")
	}
}

// --- test doubles -----------------------------------------------------------

// failingSessions fails every call, standing in for a dead backend.
type failingSessions struct{ err error }

func (f failingSessions) CreateSession(context.Context, session.Key, session.StateMap, ...session.Option) (*session.Session, error) {
	return nil, f.err
}
func (f failingSessions) GetSession(context.Context, session.Key, ...session.Option) (*session.Session, error) {
	return nil, f.err
}
func (f failingSessions) ListSessions(context.Context, session.UserKey, ...session.Option) ([]*session.Session, error) {
	return nil, f.err
}
func (f failingSessions) DeleteSession(context.Context, session.Key, ...session.Option) error {
	return f.err
}
func (f failingSessions) UpdateAppState(context.Context, string, session.StateMap) error {
	return f.err
}
func (f failingSessions) DeleteAppState(context.Context, string, string) error { return f.err }
func (f failingSessions) ListAppStates(context.Context, string) (session.StateMap, error) {
	return nil, f.err
}
func (f failingSessions) UpdateUserState(context.Context, session.UserKey, session.StateMap) error {
	return f.err
}
func (f failingSessions) ListUserStates(context.Context, session.UserKey) (session.StateMap, error) {
	return nil, f.err
}
func (f failingSessions) DeleteUserState(context.Context, session.UserKey, string) error { return f.err }
func (f failingSessions) UpdateSessionState(context.Context, session.Key, session.StateMap) error {
	return f.err
}
func (f failingSessions) AppendEvent(context.Context, *session.Session, *event.Event, ...session.Option) error {
	return f.err
}
func (f failingSessions) CreateSessionSummary(context.Context, *session.Session, string, bool) error {
	return f.err
}
func (f failingSessions) EnqueueSummaryJob(context.Context, *session.Session, string, bool) error {
	return f.err
}
func (f failingSessions) GetSessionSummaryText(context.Context, *session.Session, ...session.SummaryOption) (string, bool) {
	return "", false
}
func (f failingSessions) Close() error { return nil }

// sessionInMemoryForTest returns a plain in-memory session service (the
// decorator is what is under test).
func sessionInMemoryForTest(t *testing.T) session.Service {
	t.Helper()
	s, err := NewSessions(SessionConfig{Backend: BackendInMemory})
	if err != nil {
		t.Fatalf("NewSessions: %v", err)
	}
	// Unwrap the decorator installed by NewSessions so the test controls it.
	return unwrapSessions(s)
}

// unwrapSessions peels the tracing decorator (tests wrap it themselves).
func unwrapSessions(s *Sessions) session.Service {
	if ts, ok := s.svc.(*tracingSessions); ok {
		return ts.inner
	}
	return s.svc
}

// memoryInMemoryForTest returns a plain in-memory memory service.
func memoryInMemoryForTest(t *testing.T) memory.Service {
	t.Helper()
	m, err := NewMemories(MemoryConfig{Backend: BackendInMemory})
	if err != nil {
		t.Fatalf("NewMemories: %v", err)
	}
	if tm, ok := m.svc.(*tracingMemories); ok {
		return tm.inner
	}
	return m.svc
}
