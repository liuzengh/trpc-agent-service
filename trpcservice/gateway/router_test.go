package gateway

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	agenttrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"

	platformlog "github.com/Violet2314/trpc-agent-service/trpcservice/log"
	"github.com/Violet2314/trpc-agent-service/trpcservice/storage"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
	"github.com/Violet2314/trpc-agent-service/trpcservice/worker"
)

func TestRouterHandleOutcomes(t *testing.T) {
	tests := []struct {
		name        string
		message     InboundMessage
		cacheErr    error
		claimErr    error
		appendErr   error
		markErr     error
		want        Result
		wantError   bool
		wantMark    int
		wantRelease int
		wantAppend  int
	}{
		{
			name:    "invalid event",
			message: InboundMessage{},
			want:    Result{Outcome: OutcomeDropped, DropReason: DropInvalidEvent},
		},
		{
			name:     "unbound route",
			message:  validInbound(),
			cacheErr: tenant.ErrNotFound,
			want:     Result{Outcome: OutcomeNeedsBinding, DropReason: DropUnboundBinding},
		},
		{
			name:     "inactive binding",
			message:  validInbound(),
			cacheErr: tenant.ErrInactive,
			want:     Result{Outcome: OutcomeDropped, DropReason: DropRevokedBinding},
		},
		{
			name: "group not addressed",
			message: func() InboundMessage {
				value := validInbound()
				value.ChatType = "group"
				value.GroupID = "group-1"
				value.AddressedToBot = false
				return value
			}(),
			want: Result{Outcome: OutcomeDropped, DropReason: DropNotAddressedInGroup},
		},
		{
			name:     "Redis duplicate",
			message:  validInbound(),
			claimErr: ErrDuplicate,
			want: Result{
				Outcome: OutcomeDropped, DropReason: DropDuplicate,
				SessionID: "tenant-a:webui:p2p:user-1",
			},
		},
		{
			name:       "database duplicate",
			message:    validInbound(),
			appendErr:  storage.ErrDuplicateEvent,
			want:       Result{Outcome: OutcomeDropped, DropReason: DropDuplicate, SessionID: "tenant-a:webui:p2p:user-1"},
			wantMark:   1,
			wantAppend: 1,
		},
		{
			name:        "append failure releases claim",
			message:     validInbound(),
			appendErr:   errors.New("database unavailable"),
			wantError:   true,
			wantRelease: 1,
			wantAppend:  1,
		},
		{
			name:       "mark failure after durable append",
			message:    validInbound(),
			markErr:    errors.New("Redis unavailable"),
			wantError:  true,
			wantMark:   1,
			wantAppend: 1,
		},
		{
			name:       "ingested",
			message:    validInbound(),
			want:       Result{Outcome: OutcomeIngested, SessionID: "tenant-a:webui:p2p:user-1"},
			wantMark:   1,
			wantAppend: 1,
		},
		{
			name: "oversized message ID",
			message: func() InboundMessage {
				value := validInbound()
				value.MsgID = strings.Repeat("x", maxMsgIDLength+1)
				return value
			}(),
			want: Result{Outcome: OutcomeDropped, DropReason: DropInvalidEvent},
		},
		{
			name: "oversized sender ID",
			message: func() InboundMessage {
				value := validInbound()
				value.SenderID = strings.Repeat("x", maxParticipantIDLength+1)
				return value
			}(),
			want: Result{Outcome: OutcomeDropped, DropReason: DropInvalidEvent},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			debouncer := &manualDebouncer{}
			deduper := &fakeDeduper{claimErr: test.claimErr, markErr: test.markErr}
			store := &fakeEventStore{appendErr: test.appendErr}
			router := newTestRouter(t, routerTestDeps{
				cache:     &fakeCache{snapshot: validSnapshot(), err: test.cacheErr},
				deduper:   deduper,
				debouncer: debouncer,
				store:     store,
			})

			got, err := router.Handle(context.Background(), test.message)
			if (err != nil) != test.wantError {
				t.Fatalf("Handle() error = %v, wantError = %v", err, test.wantError)
			}
			if got != test.want {
				t.Fatalf("Handle() = %#v, want %#v", got, test.want)
			}
			if deduper.markCalls != test.wantMark {
				t.Fatalf("Mark calls = %d, want %d", deduper.markCalls, test.wantMark)
			}
			if deduper.releaseCalls != test.wantRelease {
				t.Fatalf("Release calls = %d, want %d", deduper.releaseCalls, test.wantRelease)
			}
			if store.appendCalls != test.wantAppend {
				t.Fatalf("Append calls = %d, want %d", store.appendCalls, test.wantAppend)
			}
			if test.want.Outcome == OutcomeIngested && debouncer.scheduled != 1 {
				t.Fatalf("scheduled flushes = %d, want 1", debouncer.scheduled)
			}
		})
	}
}

func TestRouterDrainFlushesAndMarksEventsConsumed(t *testing.T) {
	debouncer := NewDebouncer(time.Hour)
	store := &fakeEventStore{
		pending: []storage.UserEvent{{
			ID: 7, SessionID: "tenant-a:webui:p2p:user-1", TenantID: "tenant-a",
			Channel: "webui", MsgID: "msg-1", SenderID: "user-1", Text: "hello",
		}},
	}
	executor := &fakeExecutor{events: []worker.Event{
		{Type: "text_delta", Text: "hello"},
		{Type: "done"},
	}}
	dispatcher := &fakeDispatcher{}
	router := newTestRouter(t, routerTestDeps{
		debouncer:  debouncer,
		store:      store,
		executor:   executor,
		dispatcher: dispatcher,
	})

	result, err := router.Handle(context.Background(), validInbound())
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if result.Outcome != OutcomeIngested {
		t.Fatalf("Handle() outcome = %q, want ingested", result.Outcome)
	}
	router.Drain()

	if executor.calls != 1 {
		t.Fatalf("executor calls = %d, want 1", executor.calls)
	}
	if len(store.consumedIDs) != 1 || store.consumedIDs[0] != 7 {
		t.Fatalf("consumed IDs = %v, want [7]", store.consumedIDs)
	}
	if dispatcher.events != 2 {
		t.Fatalf("dispatched events = %d, want 2", dispatcher.events)
	}
	offline, err := router.Handle(context.Background(), validInbound())
	if err != nil {
		t.Fatalf("Handle() after Drain error = %v", err)
	}
	if offline.Outcome != OutcomeAgentOffline {
		t.Fatalf("Handle() after Drain outcome = %q, want agent_offline", offline.Outcome)
	}
}

func TestRouterLockHeldReschedules(t *testing.T) {
	debouncer := &manualDebouncer{}
	lock := &fakeSessionLock{err: ErrHeld}
	router := newTestRouter(t, routerTestDeps{
		debouncer: debouncer,
		lock:      lock,
		store: &fakeEventStore{pending: []storage.UserEvent{{
			ID: 1, SessionID: "tenant-a:webui:p2p:user-1",
		}}},
	})
	if _, err := router.Handle(context.Background(), validInbound()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if debouncer.flush == nil {
		t.Fatal("flush was not scheduled")
	}
	firstFlush := debouncer.flush
	firstFlush()
	if debouncer.scheduled != 2 {
		t.Fatalf("scheduled flushes = %d, want retry schedule", debouncer.scheduled)
	}
}

func TestRouterAuditsWorkerDecisions(t *testing.T) {
	audit := &recordingAudit{}
	router := newTestRouter(t, routerTestDeps{
		debouncer: NewDebouncer(time.Hour),
		store: &fakeEventStore{pending: []storage.UserEvent{{
			ID: 1, SessionID: "tenant-a:webui:p2p:user-1",
		}}},
		executor: &fakeExecutor{events: []worker.Event{
			{Type: "text_delta", Text: "cancelled", Decision: "tool_denied"},
			{Type: "done"},
		}},
		audit: audit,
	})
	if _, err := router.Handle(context.Background(), validInbound()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	router.Drain()
	if !audit.hasDecision("tool_denied") {
		t.Fatalf("audit records = %#v, want tool_denied", audit.records)
	}
}

func TestRouterTraceContinuesAcrossAsyncFlush(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	originalTracer := agenttrace.Tracer
	agenttrace.Tracer = provider.Tracer("gateway-test")
	t.Cleanup(func() {
		agenttrace.Tracer = originalTracer
		_ = provider.Shutdown(context.Background())
	})
	audit := &recordingAudit{}
	router := newTestRouter(t, routerTestDeps{
		debouncer: NewDebouncer(time.Hour),
		store: &fakeEventStore{pending: []storage.UserEvent{{
			ID: 1, SessionID: "tenant-a:webui:p2p:user-1",
		}}},
		executor: &fakeExecutor{events: []worker.Event{{Type: "done"}}},
		audit:    audit,
	})
	if _, err := router.Handle(context.Background(), validInbound()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	router.Drain()
	spans := exporter.GetSpans()
	var handleTrace, flushTrace string
	for _, span := range spans {
		switch span.Name {
		case "gateway.handle":
			handleTrace = span.SpanContext.TraceID().String()
		case "gateway.flush":
			flushTrace = span.SpanContext.TraceID().String()
		}
	}
	if handleTrace == "" || flushTrace == "" || handleTrace != flushTrace {
		t.Fatalf("trace IDs handle=%q flush=%q spans=%#v", handleTrace, flushTrace, spans)
	}
}

func TestRouterInfrastructureErrors(t *testing.T) {
	t.Run("cache", func(t *testing.T) {
		router := newTestRouter(t, routerTestDeps{
			cache: &fakeCache{err: errors.New("cache unavailable")},
		})
		if _, err := router.Handle(context.Background(), validInbound()); err == nil {
			t.Fatal("Handle() did not return cache infrastructure error")
		}
	})

	t.Run("deduper", func(t *testing.T) {
		router := newTestRouter(t, routerTestDeps{
			deduper: &fakeDeduper{claimErr: errors.New("Redis unavailable")},
		})
		if _, err := router.Handle(context.Background(), validInbound()); err == nil {
			t.Fatal("Handle() did not return deduper infrastructure error")
		}
	})

	t.Run("append and release", func(t *testing.T) {
		router := newTestRouter(t, routerTestDeps{
			deduper: &fakeDeduper{releaseErr: errors.New("release unavailable")},
			store:   &fakeEventStore{appendErr: errors.New("database unavailable")},
		})
		if _, err := router.Handle(context.Background(), validInbound()); err == nil {
			t.Fatal("Handle() did not join append and release errors")
		}
	})
}

// TestFlushDispatchesFailureReplyWhenExecutorFails kills the R3 attack: after
// the inbound message was ACKed and marked done, an executor start failure
// must still produce a user-visible error event instead of silence.
func TestFlushDispatchesFailureReplyWhenExecutorFails(t *testing.T) {
	debouncer := &manualDebouncer{}
	dispatcher := &recordingDispatcher{}
	router := newTestRouter(t, routerTestDeps{
		debouncer:  debouncer,
		dispatcher: dispatcher,
		store: &fakeEventStore{pending: []storage.UserEvent{{
			ID: 1, SessionID: "tenant-a:webui:p2p:user-1",
		}}},
		executor: &fakeExecutor{err: errors.New("model authentication failed")},
	})
	if _, err := router.Handle(context.Background(), validInbound()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	debouncer.flush()

	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for {
		if events := dispatcher.recorded(); len(events) > 0 {
			if events[0].Type != "error" || events[0].Error == "" {
				t.Fatalf("failure reply = %#v, want error event", events[0])
			}
			return
		}
		select {
		case <-time.After(5 * time.Millisecond):
		case <-deadline.C:
			t.Fatal("executor failure produced no reply event")
		}
	}
}

// TestFlushAbortsExecutionWhenLeaseLost kills the R4 attack: losing the
// session lock mid-run must cancel the in-flight execution instead of letting
// it write concurrently with the new lock owner.
func TestFlushAbortsExecutionWhenLeaseLost(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	lock, err := NewRedisSessionLock(client, RedisSessionLockConfig{
		TTL: 500 * time.Millisecond, RenewInterval: 50 * time.Millisecond, MaxHold: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedisSessionLock() error = %v", err)
	}
	executor := newBlockingExecutor()
	debouncer := &manualDebouncer{}
	router := newTestRouter(t, routerTestDeps{
		lock:      lock,
		debouncer: debouncer,
		executor:  executor,
		store: &fakeEventStore{pending: []storage.UserEvent{{
			ID: 1, SessionID: "tenant-a:webui:p2p:user-1",
		}}},
	})
	if _, err := router.Handle(context.Background(), validInbound()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if debouncer.flush == nil {
		t.Fatal("flush was not scheduled")
	}
	go debouncer.flush()
	select {
	case <-executor.started:
	case <-time.After(2 * time.Second):
		t.Fatal("executor did not start")
	}

	// Steal the lock by replacing its value; renewal then fails, the lease
	// reports loss, and the in-flight execution must be cancelled.
	sessionID := DeriveSessionID("tenant-a", "webui", validInbound())
	server.Set("lock:"+sessionID, "new-owner")

	select {
	case <-executor.cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("execution was not cancelled after lease loss")
	}
}

// TestFlushRetriesMarkConsumedAfterTransientFailure kills the R5 attack: a
// transient MarkConsumed failure after a delivered reply must be retried so
// the finished turn is not re-executed (and re-replied) on the next message.
func TestFlushRetriesMarkConsumedAfterTransientFailure(t *testing.T) {
	debouncer := &manualDebouncer{}
	store := &flakyMarkStore{
		pending:  []storage.UserEvent{{ID: 9, SessionID: "tenant-a:webui:p2p:user-1"}},
		failures: 1,
	}
	router := newTestRouter(t, routerTestDeps{
		debouncer: debouncer,
		store:     store,
		executor:  &fakeExecutor{events: []worker.Event{{Type: "done"}}},
	})
	if _, err := router.Handle(context.Background(), validInbound()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	debouncer.flush()

	if store.markCalls != 2 {
		t.Fatalf("MarkConsumed calls = %d, want 2 (failure + retry)", store.markCalls)
	}
	if len(store.consumedIDs) != 1 || store.consumedIDs[0] != 9 {
		t.Fatalf("consumed IDs = %v, want [9]", store.consumedIDs)
	}
}

// TestAuditTruncatesOversizedIdentifiers kills the R7 attack: an oversized
// hostile message ID must still produce a durable audit row (bounded to the
// audit_log.request_id column) instead of a silently dropped insert.
func TestAuditTruncatesOversizedIdentifiers(t *testing.T) {
	audit := &recordingAudit{}
	router := newTestRouter(t, routerTestDeps{audit: audit})
	message := validInbound()
	message.MsgID = strings.Repeat("x", 500)
	message.SenderID = strings.Repeat("y", 500)

	result, err := router.Handle(context.Background(), message)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if result.Outcome != OutcomeDropped || result.DropReason != DropInvalidEvent {
		t.Fatalf("Handle() = %#v, want dropped invalid_event", result)
	}
	audit.mu.Lock()
	defer audit.mu.Unlock()
	if len(audit.records) != 1 {
		t.Fatalf("audit records = %d, want 1", len(audit.records))
	}
	record := audit.records[0]
	if record.Decision != string(DropInvalidEvent) {
		t.Fatalf("audit decision = %q, want %q", record.Decision, DropInvalidEvent)
	}
	if len(record.RequestID) > maxMsgIDLength {
		t.Fatalf("audit request_id length = %d, want <= %d", len(record.RequestID), maxMsgIDLength)
	}
	if len(record.UserID) > maxParticipantIDLength {
		t.Fatalf("audit user_id length = %d, want <= %d", len(record.UserID), maxParticipantIDLength)
	}
}

// TestDrainLetsInFlightRepliesFinish kills the R9 attack: a slow (but
// healthy) replier must deliver its full reply during Drain instead of being
// cancelled the moment shutdown starts.
func TestDrainLetsInFlightRepliesFinish(t *testing.T) {
	debouncer := NewDebouncer(time.Hour)
	store := &fakeEventStore{pending: []storage.UserEvent{{
		ID: 5, SessionID: "tenant-a:webui:p2p:user-1",
	}}}
	executor := &fakeExecutor{events: []worker.Event{
		{Type: "text_delta", Text: "slow"},
		{Type: "done"},
	}}
	dispatcher := &ctxAwareDispatcher{delay: 200 * time.Millisecond}
	router := newTestRouter(t, routerTestDeps{
		debouncer:  debouncer,
		store:      store,
		executor:   executor,
		dispatcher: dispatcher,
	})
	if _, err := router.Handle(context.Background(), validInbound()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	router.Drain()
	if got := dispatcher.delivered(); got != 2 {
		t.Fatalf("delivered events = %d, want 2 (slow reply was cut off by Drain)", got)
	}
}

// TestDrainCancelsStuckRepliers keeps shutdown bounded: a replier blocked
// past the grace period must be cancelled instead of hanging Drain forever.
func TestDrainCancelsStuckRepliers(t *testing.T) {
	original := drainReplyGrace
	drainReplyGrace = 50 * time.Millisecond
	t.Cleanup(func() { drainReplyGrace = original })

	debouncer := NewDebouncer(time.Hour)
	store := &fakeEventStore{pending: []storage.UserEvent{{
		ID: 5, SessionID: "tenant-a:webui:p2p:user-1",
	}}}
	executor := &fakeExecutor{events: []worker.Event{{Type: "done"}}}
	dispatcher := &stuckDispatcher{}
	router := newTestRouter(t, routerTestDeps{
		debouncer:  debouncer,
		store:      store,
		executor:   executor,
		dispatcher: dispatcher,
	})
	if _, err := router.Handle(context.Background(), validInbound()); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	done := make(chan struct{})
	go func() {
		router.Drain()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Drain() hung on a stuck replier")
	}
	if !dispatcher.cancelled() {
		t.Fatal("stuck replier was not cancelled after the grace period")
	}
}

func TestNewRouterRejectsMissingDependencies(t *testing.T) {
	if _, err := NewRouter(
		nil, &fakeDeduper{}, &fakeSessionLock{}, &manualDebouncer{},
		&fakeEventStore{}, &fakeExecutor{}, nil, nil,
	); err == nil {
		t.Fatal("NewRouter() accepted nil cache")
	}
}

func TestDrainReplyDispatcher(t *testing.T) {
	events := make(chan worker.Event, 1)
	events <- worker.Event{Type: "done"}
	close(events)
	if err := (DrainReplyDispatcher{}).Dispatch(
		context.Background(), tenant.Snapshot{}, "session", InboundMessage{}, events,
	); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
}

func validInbound() InboundMessage {
	return InboundMessage{
		Channel:        "webui",
		RouteKey:       "binding-a",
		MsgID:          "msg-1",
		ChatType:       "p2p",
		SenderID:       "user-1",
		AddressedToBot: true,
		Text:           "hello",
		TraceID:        "trace-1",
	}
}

func validSnapshot() tenant.Snapshot {
	return tenant.Snapshot{
		Tenant: tenant.Tenant{ID: "tenant-a", Name: "Tenant A", IsActive: true},
		App: tenant.AgentApp{
			ID: "app-a", TenantID: "tenant-a", AppName: "tenant-a-support",
			Model:    tenant.ModelConfig{Provider: "test", Model: "test-model"},
			Backends: tenant.BackendSelection{Session: "redis", Memory: "pgvector"},
		},
		Binding: tenant.ChannelBinding{
			ID: "binding-a", TenantID: "tenant-a", AppID: "app-a",
			Channel: "webui", RouteKey: "binding-a", IsActive: true,
		},
	}
}

type routerTestDeps struct {
	cache      tenant.ConfigCache
	deduper    Deduper
	lock       SessionLock
	debouncer  Debouncer
	store      storage.EventStore
	executor   worker.Executor
	dispatcher ReplyDispatcher
	audit      platformlog.AuditWriter
}

func newTestRouter(t *testing.T, deps routerTestDeps) *Router {
	t.Helper()
	if deps.cache == nil {
		deps.cache = &fakeCache{snapshot: validSnapshot()}
	}
	if deps.deduper == nil {
		deps.deduper = &fakeDeduper{}
	}
	if deps.lock == nil {
		deps.lock = &fakeSessionLock{}
	}
	if deps.debouncer == nil {
		deps.debouncer = &manualDebouncer{}
	}
	if deps.store == nil {
		deps.store = &fakeEventStore{}
	}
	if deps.executor == nil {
		deps.executor = &fakeExecutor{}
	}
	router, err := NewRouter(
		deps.cache, deps.deduper, deps.lock, deps.debouncer, deps.store,
		deps.executor, deps.dispatcher, deps.audit,
	)
	if err != nil {
		t.Fatalf("NewRouter() error = %v", err)
	}
	return router
}

type fakeCache struct {
	snapshot tenant.Snapshot
	err      error
}

func (f *fakeCache) ResolveBinding(context.Context, string, string) (tenant.Snapshot, error) {
	return f.snapshot, f.err
}
func (f *fakeCache) Invalidate(string, string) {}

type fakeDeduper struct {
	claimErr     error
	markErr      error
	releaseErr   error
	markCalls    int
	releaseCalls int
}

func (f *fakeDeduper) Claim(context.Context, string) (string, error) {
	return "owner-token", f.claimErr
}
func (f *fakeDeduper) Mark(context.Context, string, string) error {
	f.markCalls++
	return f.markErr
}
func (f *fakeDeduper) Release(context.Context, string, string) error {
	f.releaseCalls++
	return f.releaseErr
}

type fakeSessionLock struct {
	err error
}

func (f *fakeSessionLock) Acquire(context.Context, string) (Lease, error) {
	if f.err != nil {
		return nil, f.err
	}
	return fakeLease{}, nil
}

type fakeLease struct{}

func (fakeLease) Release(context.Context) error { return nil }
func (fakeLease) Lost() <-chan error            { return nil }

type manualDebouncer struct {
	scheduled int
	flush     func()
}

func (d *manualDebouncer) Schedule(_ string, flush func()) {
	d.scheduled++
	d.flush = flush
}
func (d *manualDebouncer) FlushAll() {
	if d.flush != nil {
		d.flush()
		d.flush = nil
	}
}

type fakeEventStore struct {
	appendErr   error
	pendingErr  error
	markErr     error
	appendCalls int
	pending     []storage.UserEvent
	consumedIDs []int64
}

func (f *fakeEventStore) AppendUserEvent(context.Context, storage.UserEvent) error {
	f.appendCalls++
	return f.appendErr
}
func (f *fakeEventStore) PendingUserEvents(context.Context, string, int64) ([]storage.UserEvent, error) {
	return f.pending, f.pendingErr
}
func (f *fakeEventStore) MarkConsumed(_ context.Context, ids []int64) error {
	f.consumedIDs = append([]int64(nil), ids...)
	return f.markErr
}

type fakeExecutor struct {
	mu     sync.Mutex
	events []worker.Event
	err    error
	calls  int
}

func (f *fakeExecutor) Execute(
	context.Context,
	tenant.Snapshot,
	string,
	[]storage.UserEvent,
) (<-chan worker.Event, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	result := make(chan worker.Event, len(f.events))
	for _, event := range f.events {
		result <- event
	}
	close(result)
	return result, nil
}

// blockingExecutor starts a run that only finishes when its context is
// cancelled, recording whether cancellation ever arrived.
type blockingExecutor struct {
	started   chan struct{}
	cancelled chan struct{}
}

func newBlockingExecutor() *blockingExecutor {
	return &blockingExecutor{
		started:   make(chan struct{}),
		cancelled: make(chan struct{}),
	}
}

func (e *blockingExecutor) Execute(
	ctx context.Context,
	_ tenant.Snapshot,
	_ string,
	_ []storage.UserEvent,
) (<-chan worker.Event, error) {
	close(e.started)
	output := make(chan worker.Event)
	go func() {
		<-ctx.Done()
		close(e.cancelled)
		close(output)
	}()
	return output, nil
}

type recordingDispatcher struct {
	mu     sync.Mutex
	events []worker.Event
}

func (d *recordingDispatcher) Dispatch(
	_ context.Context,
	_ tenant.Snapshot,
	_ string,
	_ InboundMessage,
	events <-chan worker.Event,
) error {
	for event := range events {
		d.mu.Lock()
		d.events = append(d.events, event)
		d.mu.Unlock()
	}
	return nil
}

func (d *recordingDispatcher) recorded() []worker.Event {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]worker.Event(nil), d.events...)
}

// ctxAwareDispatcher mimics a real channel replier: it is slow but healthy,
// and like the webui/IM repliers it drops events once its context is
// cancelled. If Drain cancels too early, the reply is silently truncated.
type ctxAwareDispatcher struct {
	delay time.Duration
	mu    sync.Mutex
	count int
}

func (d *ctxAwareDispatcher) Dispatch(
	ctx context.Context,
	_ tenant.Snapshot,
	_ string,
	_ InboundMessage,
	events <-chan worker.Event,
) error {
	time.Sleep(d.delay)
	for range events {
		select {
		case <-ctx.Done():
			return nil // remaining events are dropped, like a real replier
		default:
		}
		d.mu.Lock()
		d.count++
		d.mu.Unlock()
	}
	return nil
}

func (d *ctxAwareDispatcher) delivered() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.count
}

// stuckDispatcher blocks until its context is cancelled, like a replier
// hanging on a dead IM API.
type stuckDispatcher struct {
	mu           sync.Mutex
	wasCancelled bool
}

func (d *stuckDispatcher) Dispatch(
	ctx context.Context,
	_ tenant.Snapshot,
	_ string,
	_ InboundMessage,
	_ <-chan worker.Event,
) error {
	<-ctx.Done()
	d.mu.Lock()
	d.wasCancelled = true
	d.mu.Unlock()
	return nil
}

func (d *stuckDispatcher) cancelled() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.wasCancelled
}

// flakyMarkStore fails MarkConsumed the first N times, then succeeds.
type flakyMarkStore struct {
	pending     []storage.UserEvent
	failures    int
	markCalls   int
	consumedIDs []int64
}

func (f *flakyMarkStore) AppendUserEvent(context.Context, storage.UserEvent) error {
	return nil
}

func (f *flakyMarkStore) PendingUserEvents(
	context.Context, string, int64,
) ([]storage.UserEvent, error) {
	return f.pending, nil
}

func (f *flakyMarkStore) MarkConsumed(_ context.Context, ids []int64) error {
	f.markCalls++
	if f.failures > 0 {
		f.failures--
		return errors.New("transient mysql blip")
	}
	f.consumedIDs = append([]int64(nil), ids...)
	return nil
}

type fakeDispatcher struct {
	mu     sync.Mutex
	events int
}

func (f *fakeDispatcher) Dispatch(
	_ context.Context,
	_ tenant.Snapshot,
	_ string,
	_ InboundMessage,
	events <-chan worker.Event,
) error {
	for range events {
		f.mu.Lock()
		f.events++
		f.mu.Unlock()
	}
	return nil
}

type recordingAudit struct {
	mu      sync.Mutex
	records []platformlog.AuditRecord
}

func (r *recordingAudit) Write(_ context.Context, record platformlog.AuditRecord) {
	r.mu.Lock()
	r.records = append(r.records, record)
	r.mu.Unlock()
}

func (r *recordingAudit) hasDecision(decision string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, record := range r.records {
		if record.Decision == decision {
			return true
		}
	}
	return false
}
