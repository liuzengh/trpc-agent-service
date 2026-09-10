package outbox

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
)

func TestWorkerInternalBranchCoverage(t *testing.T) {
	if eligible(runtimestorage.ReplyOutbox{Status: runtimestorage.ReplySending}) {
		t.Fatal("sending without lease should not be eligible")
	}
	if !eligible(runtimestorage.ReplyOutbox{Status: runtimestorage.ReplySending, LeaseExpiresAt: ptrTime(time.Now().Add(-time.Second))}) {
		t.Fatal("expired sending should be eligible")
	}
	if class, retryable := classify(errors.New("provider")); class != "provider_error" || !retryable {
		t.Fatalf("default classification = %s/%v", class, retryable)
	}
	store := inmemory.New()
	worker := &Worker{store: store, tenantID: "tenant-a", owner: "worker"}
	worker.advanceEvent(context.Background(), "")
	worker.advanceEvent(context.Background(), "missing")
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event", SessionID: "session", BindingID: "binding", ExternalMessageID: "external"}); err != nil {
		t.Fatal(err)
	}
	worker.advanceEvent(context.Background(), "event")
}

func ptrTime(value time.Time) *time.Time { return &value }

type branchStore struct {
	runtimestorage.ReplyStore
	runtimestorage.MessageStore
	candidates         []runtimestorage.ReplyOutbox
	listErr            error
	event              runtimestorage.MessageEvent
	getErr             error
	transitionErr      error
	messageTransitions []runtimestorage.MessageTransition
	replyTransitions   []runtimestorage.ReplyTransition
}

type replyStoreOnly struct{ runtimestorage.ReplyStore }

type messageStoreOnly struct{ runtimestorage.MessageStore }

func seedWorkerReply(t *testing.T, store *inmemory.Store) {
	t.Helper()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-split", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event-split", SessionID: "session-split", BindingID: "binding-split", ExternalMessageID: "external-split"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-split", EventID: "event-split", SegmentIndex: 0, SegmentCount: 1, Payload: "payload"}); err != nil {
		t.Fatal(err)
	}
}

func (s *branchStore) ClaimReply(_ context.Context, _ string, _ string, _ int, _ string, _ time.Duration) (runtimestorage.ReplyOutbox, error) {
	if len(s.candidates) == 0 {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrNotFound
	}
	claimed := s.candidates[0]
	claimed.Status = runtimestorage.ReplySending
	claimed.FencingToken++
	return claimed, nil
}

func (s *branchStore) ListReplyCandidates(context.Context, string) ([]runtimestorage.ReplyOutbox, error) {
	return s.candidates, s.listErr
}

func (s *branchStore) GetMessage(context.Context, string, string) (runtimestorage.MessageEvent, error) {
	return s.event, s.getErr
}

func (s *branchStore) TransitionMessage(_ context.Context, transition runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error) {
	s.messageTransitions = append(s.messageTransitions, transition)
	return s.event, s.transitionErr
}

func (s *branchStore) TransitionReply(_ context.Context, transition runtimestorage.ReplyTransition) (runtimestorage.ReplyOutbox, error) {
	s.replyTransitions = append(s.replyTransitions, transition)
	return runtimestorage.ReplyOutbox{}, s.transitionErr
}

type branchProvider struct {
	status DeliveryStatus
	id     string
	err    error
}

func (p branchProvider) Deliver(context.Context, runtimestorage.ReplyOutbox) (string, error) {
	return p.id, p.err
}

func (p branchProvider) Reconcile(context.Context, runtimestorage.ReplyOutbox) (DeliveryStatus, string, error) {
	return p.status, p.id, p.err
}

func TestAdvanceEventBranchCoverage(t *testing.T) {
	ctx := context.Background()
	base := runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", Status: runtimestorage.ReplySent}
	cases := []struct {
		name            string
		store           *branchStore
		wantTransitions int
	}{
		{name: "list error", store: &branchStore{listErr: errors.New("list")}},
		{name: "no matching event", store: &branchStore{candidates: []runtimestorage.ReplyOutbox{{EventID: "other", Status: runtimestorage.ReplySent}}}},
		{name: "incomplete replies", store: &branchStore{candidates: []runtimestorage.ReplyOutbox{{EventID: "event", Status: runtimestorage.ReplyPending}}}},
		{name: "get error", store: &branchStore{candidates: []runtimestorage.ReplyOutbox{base}, getErr: errors.New("get")}},
		{name: "completed transition error", store: &branchStore{candidates: []runtimestorage.ReplyOutbox{base}, event: runtimestorage.MessageEvent{Status: runtimestorage.EventCompleted}, transitionErr: errors.New("transition")}, wantTransitions: 1},
		{name: "reply pending advances", store: &branchStore{candidates: []runtimestorage.ReplyOutbox{base}, event: runtimestorage.MessageEvent{Status: runtimestorage.EventReplyPending}, transitionErr: nil}, wantTransitions: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			worker := &Worker{store: tc.store, messageStore: tc.store, tenantID: "tenant-a", owner: "worker"}
			worker.advanceEvent(ctx, "event")
			if got := len(tc.store.messageTransitions); got != tc.wantTransitions {
				t.Fatalf("message transitions = %d, want %d", got, tc.wantTransitions)
			}
		})
	}
}

func TestReconcileBranchCoverage(t *testing.T) {
	ctx := context.Background()
	claimed := runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", SegmentIndex: 0, FencingToken: 7, Status: runtimestorage.ReplySending}
	cases := []struct {
		name     string
		provider branchProvider
		store    *branchStore
		want     bool
		wantTo   string
	}{
		{name: "provider error", provider: branchProvider{err: errors.New("reconcile")}, store: &branchStore{}},
		{name: "unknown", provider: branchProvider{status: DeliveryUnknown}, store: &branchStore{}},
		{name: "rejected", provider: branchProvider{status: DeliveryRejected, id: "provider"}, store: &branchStore{}, want: true, wantTo: runtimestorage.ReplyRetryable},
		{name: "accepted transition error", provider: branchProvider{status: DeliveryAccepted, id: "provider"}, store: &branchStore{transitionErr: errors.New("transition")}},
		{name: "accepted", provider: branchProvider{status: DeliveryAccepted, id: "provider"}, store: &branchStore{}, want: true, wantTo: runtimestorage.ReplySent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			worker := &Worker{store: tc.store, provider: tc.provider, tenantID: "tenant-a", owner: "worker"}
			if got := worker.reconcile(ctx, claimed); got != tc.want {
				t.Fatalf("reconcile = %v, want %v", got, tc.want)
			}
			if tc.wantTo != "" {
				if len(tc.store.replyTransitions) != 1 || tc.store.replyTransitions[0].To != tc.wantTo {
					t.Fatalf("reply transition = %+v, want %s", tc.store.replyTransitions, tc.wantTo)
				}
			}
		})
	}
}

func TestClassifyTypedDeliveryErrors(t *testing.T) {
	if got := (&DeliveryError{}).Error(); got != ErrProvider.Error() {
		t.Fatalf("delivery error text = %q", got)
	}
	if class, retryable := classify(&DeliveryError{Class: "", Retryable: false}); class != "provider_error" || !retryable {
		t.Fatalf("empty class = %s/%v", class, retryable)
	}
	if class, retryable := classify(&DeliveryError{Class: "permanent token=secret", Retryable: false}); class != "provider_error" || retryable {
		t.Fatalf("typed class = %s/%v", class, retryable)
	}
	if metricErrorClass("provider_rejected") != "error" || metricErrorClass("rate_limited") != "rate_limited" {
		t.Fatal("provider classes were not reduced to low-cardinality metric classes")
	}
}

func TestWorkerRunAndRetryDueBoundaryBranches(t *testing.T) {
	var nilWorker *Worker
	if err := nilWorker.Run(context.Background(), time.Second); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil worker run = %v", err)
	}
	worker, err := New(Config{Store: inmemory.New(), MessageStore: inmemory.New(), Provider: branchProvider{}, TenantID: "tenant-a", Owner: "worker", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if worker.retryDue(runtimestorage.ReplyOutbox{Status: runtimestorage.ReplyRetryable, UpdatedAt: time.Time{}}, time.Now()) != true {
		t.Fatal("zero updated_at should be immediately due")
	}
	if worker.retryDue(runtimestorage.ReplyOutbox{Status: runtimestorage.ReplyRetryable, Attempts: 9, UpdatedAt: time.Now().Add(-time.Hour)}, time.Now()) != true {
		t.Fatal("max backoff should eventually become due")
	}
	if !worker.retryDue(runtimestorage.ReplyOutbox{Status: runtimestorage.ReplySent}, time.Now()) {
		t.Fatal("non-retry status should not be delayed by retryDue")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := worker.Run(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("default poll interval cancellation = %v", err)
	}
}

func TestNewWorkerAcceptsSplitStorageCapabilities(t *testing.T) {
	store := inmemory.New()
	seedWorkerReply(t, store)
	worker, err := New(Config{
		Store:         replyStoreOnly{ReplyStore: store},
		MessageStore:  messageStoreOnly{MessageStore: store},
		Provider:      branchProvider{id: "provider"},
		TenantID:      "tenant-a",
		Owner:         "worker",
		LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if processed, err := worker.RunOnce(context.Background()); err != nil || processed != 1 {
		t.Fatalf("split-capability run = %d/%v", processed, err)
	}
	if _, err := New(Config{
		Store:         replyStoreOnly{ReplyStore: store},
		Provider:      branchProvider{},
		TenantID:      "tenant-a",
		Owner:         "worker",
		LeaseDuration: time.Second,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing message capability error = %v, want %v", err, ErrInvalid)
	}
}

func TestRunOnceProviderAndTransitionErrorBranches(t *testing.T) {
	ctx := context.Background()
	base := runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", Status: runtimestorage.ReplyPending}
	cases := []struct {
		name     string
		provider branchProvider
		store    *branchStore
		wantErr  bool
		wantTo   string
	}{
		{name: "success", provider: branchProvider{id: "provider"}, store: &branchStore{candidates: []runtimestorage.ReplyOutbox{base}}, wantTo: runtimestorage.ReplySent},
		{name: "retryable", provider: branchProvider{err: &DeliveryError{Class: "unavailable", Retryable: true}}, store: &branchStore{candidates: []runtimestorage.ReplyOutbox{base}}, wantTo: runtimestorage.ReplyRetryable},
		{name: "transition error", provider: branchProvider{id: "provider"}, store: &branchStore{candidates: []runtimestorage.ReplyOutbox{base}, transitionErr: errors.New("transition")}, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			worker := &Worker{store: tc.store, provider: tc.provider, tenantID: "tenant-a", owner: "worker", leaseDuration: time.Second, retry: retryPolicy{maxAttempts: 3, base: time.Millisecond, maximum: time.Second}}
			processed, err := worker.RunOnce(ctx)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected transition error")
				}
			} else if err != nil || processed != 1 {
				t.Fatalf("run = %d/%v", processed, err)
			}
			if tc.wantTo != "" && (len(tc.store.replyTransitions) != 1 || tc.store.replyTransitions[0].To != tc.wantTo) {
				t.Fatalf("reply transition = %+v, want %s", tc.store.replyTransitions, tc.wantTo)
			}
		})
	}
}

func TestWorkerRetryBackoffAndValidationBranches(t *testing.T) {
	if _, err := New(Config{Store: inmemory.New(), MessageStore: inmemory.New(), Provider: branchProvider{}, TenantID: "tenant-a", Owner: "worker", LeaseDuration: time.Second, BackoffBase: -time.Second}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative backoff = %v", err)
	}
	if _, err := New(Config{Store: inmemory.New(), MessageStore: inmemory.New(), Provider: branchProvider{}, TenantID: "tenant-a", Owner: "worker", LeaseDuration: time.Second, BackoffBase: time.Second, BackoffMax: time.Millisecond}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("inverted backoff = %v", err)
	}
	worker, err := New(Config{Store: inmemory.New(), MessageStore: inmemory.New(), Provider: branchProvider{}, TenantID: "tenant-a", Owner: "worker", LeaseDuration: time.Second, BackoffBase: time.Second, BackoffMax: 2 * time.Second, Jitter: .2})
	if err != nil {
		t.Fatal(err)
	}
	if !worker.retryDue(runtimestorage.ReplyOutbox{Status: runtimestorage.ReplyPending}, time.Now()) {
		t.Fatal("pending reply should be due")
	}
	if worker.retryDue(runtimestorage.ReplyOutbox{Status: runtimestorage.ReplyRetryable, Attempts: 1, UpdatedAt: time.Now()}, time.Now()) {
		t.Fatal("fresh retry should not be due")
	}
	old := time.Now().Add(-time.Hour)
	if !worker.retryDue(runtimestorage.ReplyOutbox{Status: runtimestorage.ReplyRetryable, Attempts: 3, UpdatedAt: old}, time.Now()) {
		t.Fatal("old retry should be due")
	}
	if err := (*Worker)(nil).Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMaterializerValidationBranches(t *testing.T) {
	if _, err := NewMaterializer(MaterializerConfig{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil materializer = %v", err)
	}
	m, err := NewMaterializer(MaterializerConfig{BatchStore: inmemory.New(), SegmentSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	for _, input := range []MaterializeInput{{}, {TenantID: "tenant-a", EventID: "event", ReplyID: "reply", Payload: " "}} {
		if _, err := m.Materialize(context.Background(), input); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid materialization %#v = %v", input, err)
		}
	}
}

type batchOnlyStore struct {
	rows []runtimestorage.ReplyOutbox
}

func (store *batchOnlyStore) EnqueueReplies(_ context.Context, rows []runtimestorage.ReplyOutbox) ([]runtimestorage.ReplyOutbox, error) {
	store.rows = append(store.rows, rows...)
	return rows, nil
}

func TestMaterializerUsesNarrowBatchCapability(t *testing.T) {
	store := &batchOnlyStore{}
	m, err := NewMaterializer(MaterializerConfig{BatchStore: store, SegmentSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	count, err := m.Materialize(context.Background(), MaterializeInput{
		TenantID: "tenant-a", EventID: "event", ReplyID: "reply", Payload: "abcd",
	})
	if err != nil || count != 2 || len(store.rows) != 2 || store.rows[0].Payload != "ab" || store.rows[1].Payload != "cd" {
		t.Fatalf("narrow materialization = count:%d rows:%+v err:%v", count, store.rows, err)
	}
}

func TestMaterializerRequiresBatchCapability(t *testing.T) {
	if _, err := NewMaterializer(MaterializerConfig{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing batch capability error = %v", err)
	}
}

func TestRedactedMaterializationErrorDropsUnknownDetails(t *testing.T) {
	if redactedMaterializationError(nil) != nil {
		t.Fatal("nil materialization error was not preserved")
	}
	for _, stable := range []error{
		context.Canceled,
		context.DeadlineExceeded,
		runtimestorage.ErrConflict,
		runtimestorage.ErrDuplicate,
		runtimestorage.ErrInvalid,
		runtimestorage.ErrNotFound,
		runtimestorage.ErrStorage,
	} {
		err := redactedMaterializationError(stable)
		if !errors.Is(err, ErrMaterialization) || !errors.Is(err, stable) {
			t.Fatalf("stable materialization error = %v, want %v", err, stable)
		}
	}
	err := redactedMaterializationError(errors.New("driver password=top-secret"))
	if !errors.Is(err, ErrMaterialization) || strings.Contains(err.Error(), "top-secret") {
		t.Fatalf("unknown materialization error was not redacted: %v", err)
	}
}

func TestMaterializerPreservesNotFoundErrorClass(t *testing.T) {
	m, err := NewMaterializer(MaterializerConfig{BatchStore: inmemory.New(), SegmentSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	_, err = m.Materialize(context.Background(), MaterializeInput{
		TenantID: "tenant-a",
		EventID:  "missing-event",
		ReplyID:  "reply-missing-event",
		Payload:  "reply",
	})
	if !errors.Is(err, ErrMaterialization) || !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing event materialization error = %v", err)
	}
}

func TestMaterializerDefaultSegmentSizeAndUnicodeSplit(t *testing.T) {
	store := inmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-default", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event-default", SessionID: "session-default", BindingID: "binding", ExternalMessageID: "external-default"}); err != nil {
		t.Fatal(err)
	}
	m, err := NewMaterializer(MaterializerConfig{BatchStore: store, SegmentSize: 0})
	if err != nil {
		t.Fatal(err)
	}
	validTraceParent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	count, err := m.Materialize(context.Background(), MaterializeInput{TenantID: "tenant-a", EventID: "event-default", ReplyID: "reply-default", RequestID: "request-default", TraceID: "trace-default", TraceParent: validTraceParent, Payload: "你好"})
	if err != nil || count != 1 {
		t.Fatalf("default materialization = %d/%v", count, err)
	}
	rows, err := store.ListReplyCandidates(context.Background(), "tenant-a")
	if err != nil || len(rows) != 1 || rows[0].Payload != "你好" {
		t.Fatalf("unicode row = %+v/%v", rows, err)
	}
	correlation, err := store.GetReplyCorrelation(context.Background(), "tenant-a", "event-default")
	if err != nil || correlation.RequestID != "request-default" || correlation.TraceID != "trace-default" || correlation.TraceParent != validTraceParent {
		t.Fatalf("materializer correlation = %+v/%v", correlation, err)
	}
	if got := splitRunes("  ", 2); got != nil {
		t.Fatalf("blank split = %#v", got)
	}
	if got := splitRunes(strings.Repeat("界", 513), defaultSegmentRunes); len(got) != 2 || len([]byte(got[0])) > 2048 {
		t.Fatalf("default segment boundary = %#v", got)
	}
}

type countingProvider struct {
	deliveries int
	status     DeliveryStatus
	err        error
}

func (p *countingProvider) Deliver(context.Context, runtimestorage.ReplyOutbox) (string, error) {
	p.deliveries++
	return "", nil
}

func (p *countingProvider) Reconcile(context.Context, runtimestorage.ReplyOutbox) (DeliveryStatus, string, error) {
	return p.status, "", p.err
}

func TestRunOnceDoesNotRedeliverUnknownSendingLease(t *testing.T) {
	provider := &countingProvider{status: DeliveryUnknown}
	store := &branchStore{candidates: []runtimestorage.ReplyOutbox{{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", Status: runtimestorage.ReplySending, LeaseExpiresAt: ptrTime(time.Now().Add(-time.Second))}}}
	telemetry := &recordingTelemetry{}
	worker, err := New(Config{Store: store, MessageStore: store, Provider: provider, TenantID: "tenant-a", Owner: "worker", LeaseDuration: time.Second, Observability: telemetry})
	if err != nil {
		t.Fatal(err)
	}
	if processed, err := worker.RunOnce(context.Background()); err != nil || processed != 1 {
		t.Fatalf("run = %d/%v", processed, err)
	}
	if provider.deliveries != 0 {
		t.Fatalf("unknown reconciliation redelivered %d times", provider.deliveries)
	}
	if countTelemetryOperations(telemetry, observability.OperationChannelSend) != 1 || countTelemetryOperations(telemetry, observability.OperationStorageOperation) < 2 || len(telemetry.metrics) < 2 {
		t.Fatalf("missing lease-recovery telemetry: operations=%d metrics=%d", len(telemetry.operations), len(telemetry.metrics))
	}
}

type telemetryRecord struct {
	requestID string
	traceID   string
	name      string
	attrs     []observability.Attribute
}

type recordingTelemetry struct {
	operations []telemetryRecord
	metrics    []telemetryRecord
}

func (p *recordingTelemetry) Tracer(string) observability.Tracer { return recordingTracer{p} }
func (p *recordingTelemetry) Meter(string) observability.Meter   { return recordingMeter{p} }
func (p *recordingTelemetry) Logger() observability.Logger       { return recordingLogger{} }
func (p *recordingTelemetry) Shutdown(context.Context) error     { return nil }

type recordingTracer struct{ provider *recordingTelemetry }

func (t recordingTracer) Start(ctx context.Context, name string, attrs ...observability.Attribute) (context.Context, observability.Span) {
	t.provider.operations = append(t.provider.operations, telemetryRecord{requestID: observability.RequestID(ctx), traceID: observability.TraceID(ctx), name: name, attrs: attrs})
	return ctx, recordingSpan{}
}

type recordingSpan struct{}

func (recordingSpan) End()                                     {}
func (recordingSpan) SetAttributes(...observability.Attribute) {}
func (recordingSpan) SetStatus(observability.Status, string)   {}
func (recordingSpan) RecordError(error)                        {}

type recordingMeter struct{ provider *recordingTelemetry }

func (m recordingMeter) Counter(name string) observability.Counter {
	return recordingCounter{m.provider, name}
}
func (m recordingMeter) Histogram(name string) observability.Histogram {
	return recordingHistogram{m.provider, name}
}
func (m recordingMeter) UpDownCounter(string) observability.UpDownCounter {
	return recordingUpDownCounter{}
}

type recordingCounter struct {
	provider *recordingTelemetry
	name     string
}

func (c recordingCounter) Add(ctx context.Context, _ int64, attrs ...observability.Attribute) {
	c.provider.metrics = append(c.provider.metrics, telemetryRecord{requestID: observability.RequestID(ctx), traceID: observability.TraceID(ctx), name: c.name, attrs: attrs})
}

type recordingHistogram struct {
	provider *recordingTelemetry
	name     string
}

func (h recordingHistogram) Record(ctx context.Context, _ float64, attrs ...observability.Attribute) {
	h.provider.metrics = append(h.provider.metrics, telemetryRecord{requestID: observability.RequestID(ctx), traceID: observability.TraceID(ctx), name: h.name, attrs: attrs})
}

type recordingUpDownCounter struct{}

func (recordingUpDownCounter) Add(context.Context, int64, ...observability.Attribute) {}

type recordingLogger struct{}

func (recordingLogger) Log(context.Context, observability.Level, string, ...observability.Attribute) {
}

type correlatedProvider struct {
	requestID string
	traceID   string
}

func (p *correlatedProvider) Deliver(ctx context.Context, _ runtimestorage.ReplyOutbox) (string, error) {
	p.requestID = observability.RequestID(ctx)
	p.traceID = observability.TraceID(ctx)
	return "", &DeliveryError{Class: "token=top-secret", Retryable: false}
}

func (*correlatedProvider) Reconcile(context.Context, runtimestorage.ReplyOutbox) (DeliveryStatus, string, error) {
	return DeliveryUnknown, "", nil
}

func TestWorkerTelemetryPropagatesCorrelationAndRedactsProviderClasses(t *testing.T) {
	ctx := observability.WithCorrelation(context.Background(), "request-1", "trace-1")
	store := inmemory.New()
	if _, err := store.CreateSession(ctx, "tenant-a", "session-1", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event-1", SessionID: "session-1", BindingID: "binding-1", ExternalMessageID: "external-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-1", EventID: "event-1", SegmentCount: 1, Payload: "payload"}); err != nil {
		t.Fatal(err)
	}
	telemetry := &recordingTelemetry{}
	provider := &correlatedProvider{}
	worker, err := New(Config{Store: store, MessageStore: store, Provider: provider, TenantID: "tenant-a", Owner: "worker-a", LeaseDuration: time.Second, MaxAttempts: 1, Observability: telemetry})
	if err != nil {
		t.Fatal(err)
	}
	if processed, err := worker.RunOnce(ctx); err != nil || processed != 1 {
		t.Fatalf("RunOnce = %d, %v", processed, err)
	}
	if provider.requestID != "request-1" || provider.traceID != "trace-1" {
		t.Fatalf("provider correlation = %q/%q", provider.requestID, provider.traceID)
	}
	if countTelemetryOperations(telemetry, observability.OperationChannelSend) != 1 || countTelemetryOperations(telemetry, observability.OperationStorageOperation) == 0 {
		t.Fatalf("operation correlation = %+v", telemetry.operations)
	}
	for _, operation := range telemetry.operations {
		if operation.requestID != "request-1" || operation.traceID != "trace-1" {
			t.Fatalf("operation lost correlation = %+v", operation)
		}
	}
	for _, record := range telemetry.metrics {
		labels := make(map[string]string, len(record.attrs))
		for _, attr := range record.attrs {
			labels[attr.Key] = attr.Value
			if strings.Contains(attr.Value, "top-secret") {
				t.Fatalf("sensitive provider class leaked into telemetry: %+v", record)
			}
		}
		if labels["error_class"] == "provider_error" {
			t.Fatalf("unbounded provider class leaked into metric labels: %+v", labels)
		}
	}
}

func TestRestoreCorrelationContextDropsAmbientSpanWhenCorrelationUnavailable(t *testing.T) {
	valid := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ambient := observability.ContextWithTraceParent(context.Background(), valid)
	value := runtimestorage.ReplyOutbox{TenantID: "tenant-a", EventID: "event-legacy"}
	ctx := restoreCorrelationContext(ambient, inmemory.New(), value)
	if got := observability.TraceParentFromContext(ctx); got != "" {
		t.Fatalf("ambient traceparent retained for legacy correlation: %q", got)
	}
}

func TestWorkerDeliveryAuditUsesPersistedCorrelation(t *testing.T) {
	store := inmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-audit-correlation", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event-audit-correlation", SessionID: "session-audit-correlation", BindingID: "binding-audit-correlation", ExternalMessageID: "external-audit-correlation"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueRepliesWithCorrelation(context.Background(), runtimestorage.ReplyCorrelation{TenantID: "tenant-a", EventID: "event-audit-correlation", RequestID: "request-audit", TraceID: "trace-audit"}, []runtimestorage.ReplyOutbox{{TenantID: "tenant-a", ReplyID: "reply-audit-correlation", EventID: "event-audit-correlation", SegmentIndex: 0, SegmentCount: 1, Payload: "payload"}}); err != nil {
		t.Fatal(err)
	}
	auditStore, err := audit.NewInMemory("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	worker, err := New(Config{Store: store, MessageStore: store, Provider: branchProvider{id: "provider"}, TenantID: "tenant-a", Owner: "worker-a", LeaseDuration: time.Second, AuditWriter: auditStore})
	if err != nil {
		t.Fatal(err)
	}
	if processed, err := worker.RunOnce(context.Background()); err != nil || processed != 1 {
		t.Fatalf("RunOnce = %d, %v", processed, err)
	}
	events, err := auditStore.List(context.Background(), audit.Query{EventTypes: []audit.EventType{audit.EventIMDeliverySent}})
	if err != nil || len(events) != 1 {
		t.Fatalf("delivery audit events = %d, %v", len(events), err)
	}
	if events[0].RequestID != "request-audit" || events[0].TraceID != "trace-audit" {
		t.Fatalf("delivery correlation = %q/%q", events[0].RequestID, events[0].TraceID)
	}
}

func TestWorkerTelemetryRecordsSuccessfulDelivery(t *testing.T) {
	store := inmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-success", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event-success", SessionID: "session-success", BindingID: "binding-success", ExternalMessageID: "external-success"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-success", EventID: "event-success", SegmentCount: 1, Payload: "payload"}); err != nil {
		t.Fatal(err)
	}
	telemetry := &recordingTelemetry{}
	worker, err := New(Config{Store: store, MessageStore: store, Provider: branchProvider{id: "provider-success"}, TenantID: "tenant-a", Owner: "worker-a", LeaseDuration: time.Second, Observability: telemetry})
	if err != nil {
		t.Fatal(err)
	}
	if processed, err := worker.RunOnce(context.Background()); err != nil || processed != 1 {
		t.Fatalf("RunOnce = %d, %v", processed, err)
	}
	if countTelemetryOperations(telemetry, observability.OperationChannelSend) != 1 || countTelemetryOperations(telemetry, observability.OperationStorageOperation) == 0 || len(telemetry.metrics) < 3 {
		t.Fatalf("missing successful delivery telemetry: operations=%d metrics=%d", len(telemetry.operations), len(telemetry.metrics))
	}
}

func countTelemetryOperations(provider *recordingTelemetry, name string) int {
	count := 0
	for _, operation := range provider.operations {
		if operation.name == name {
			count++
		}
	}
	return count
}
