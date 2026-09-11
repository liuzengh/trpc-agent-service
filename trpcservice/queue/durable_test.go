package queue

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	agentruntime "github.com/cyl6/trpc-agent-service/trpcservice/agent"
	"github.com/cyl6/trpc-agent-service/trpcservice/channels"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/coordination"
	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
	"github.com/cyl6/trpc-agent-service/trpcservice/store"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant/governance"
	"github.com/cyl6/trpc-agent-service/trpcservice/tooloperation"
	"github.com/cyl6/trpc-agent-service/trpcservice/worker"
)

// --- fakes ---------------------------------------------------------------

type fakeProcessor struct {
	mu    sync.Mutex
	runs  int
	tasks []worker.Task
	fail  error
	delay time.Duration
}

type atomicMemoryStore struct {
	*store.Memory
	txCompletes     atomic.Int32
	legacyCompletes atomic.Int32
	legacyFailures  atomic.Int32
}

func (s *atomicMemoryStore) CompleteInboxBatchTx(
	ctx context.Context,
	_ pgx.Tx,
	fence store.InboxLeaseFence,
	outboxes []store.OutboxRecord,
) error {
	s.txCompletes.Add(1)
	return s.Memory.CompleteInboxBatch(ctx, fence.InboxID, fence.Owner, outboxes, time.Now())
}

func (s *atomicMemoryStore) CompleteInboxBatch(
	ctx context.Context,
	inboxID, owner string,
	outboxes []store.OutboxRecord,
	now time.Time,
) error {
	s.legacyCompletes.Add(1)
	if s.legacyFailures.Add(-1) >= 0 {
		return errors.New("injected terminal Inbox completion failure")
	}
	return s.Memory.CompleteInboxBatch(ctx, inboxID, owner, outboxes, now)
}

type atomicFinalizingProcessor struct {
	calls   atomic.Int32
	postErr error
}

type dispositionProcessor struct {
	calls       atomic.Int32
	err         error
	disposition worker.ProcessDisposition
	retryAfter  time.Duration
}

func (p *dispositionProcessor) Process(context.Context, worker.Task) (worker.Result, error) {
	p.calls.Add(1)
	return worker.Result{}, worker.WithProcessDisposition(p.err, p.disposition, p.retryAfter)
}

func (p *atomicFinalizingProcessor) Process(ctx context.Context, task worker.Task) (worker.Result, error) {
	p.calls.Add(1)
	if task.TurnCommitParticipant == nil {
		return worker.Result{}, errors.New("atomic turn participant was not attached")
	}
	outbound := domain.OutboundMessage{
		TenantID: task.Tenant.TenantID, BindingID: task.Binding.BindingID,
		Channel: task.Binding.Type, Target: task.Message.ConversationID,
		Scope: task.Message.Scope, Text: "canonical reply",
	}
	first, second := outbound, outbound
	first.Text = "part-1"
	second.Text = "part-2"
	if err := task.TurnCommitParticipant.CompleteInTransaction(ctx, nil, worker.AtomicDeliveryPlan{
		Version: 1, Outbound: outbound, Parts: []domain.OutboundMessage{first, second},
	}); err != nil {
		return worker.Result{}, err
	}
	task.TurnCommitParticipant.MarkCommitted()
	return worker.Result{Text: outbound.Text, Outbound: &outbound}, p.postErr
}

func (f *fakeProcessor) Process(ctx context.Context, task worker.Task) (worker.Result, error) {
	f.mu.Lock()
	f.runs++
	f.tasks = append(f.tasks, task)
	fail := f.fail
	delay := f.delay
	f.mu.Unlock()
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return worker.Result{}, ctx.Err()
		case <-timer.C:
		}
	}
	if fail != nil {
		return worker.Result{}, fail
	}
	return worker.Result{Text: "reply", Outbound: &domain.OutboundMessage{Target: task.Message.ConversationID, Text: "reply"}}, nil
}

type fakeDeliverer struct {
	mu        sync.Mutex
	history   []domain.OutboundMessage
	requests  []delivery.Request
	traceIDs  []trace.TraceID
	failN     atomic.Int32 // returns safe retry while > 0, decrementing per call
	result    delivery.Result
	resultFn  func(delivery.Request) delivery.Result
	planParts []string
}

func (f *fakeDeliverer) PlanDelivery(_ config.ChannelConfig, msg domain.OutboundMessage) ([]delivery.Part, error) {
	f.mu.Lock()
	configured := append([]string(nil), f.planParts...)
	f.mu.Unlock()
	if len(configured) == 0 {
		return []delivery.Part{{Message: msg, Index: 0, Total: 1}}, nil
	}
	parts := make([]delivery.Part, 0, len(configured))
	for index, text := range configured {
		part := msg
		part.Text = text
		parts = append(parts, delivery.Part{Message: part, Index: index, Total: len(configured)})
	}
	return parts, nil
}

func (f *fakeDeliverer) DeliverOperation(ctx context.Context, _ config.ChannelConfig, request delivery.Request) delivery.Result {
	f.mu.Lock()
	f.history = append(f.history, request.Message)
	f.requests = append(f.requests, request)
	if spanContext := trace.SpanContextFromContext(ctx); spanContext.IsValid() {
		f.traceIDs = append(f.traceIDs, spanContext.TraceID())
	}
	configured := f.result
	resultFn := f.resultFn
	f.mu.Unlock()
	if f.failN.Add(-1) >= 0 {
		return delivery.Result{Outcome: delivery.RetryableNotSent, ErrorType: "provider_busy"}
	}
	if configured.Outcome != "" {
		return configured
	}
	if resultFn != nil {
		return resultFn(request)
	}
	return delivery.Result{Outcome: delivery.Confirmed, ProviderMessageID: "provider-message"}
}

type fakeResolver struct {
	binding tenant.Binding
	err     error
}

func (f fakeResolver) ResolveBindingForTenant(tenantID, _, _ string) (tenant.Binding, error) {
	if f.err != nil {
		return tenant.Binding{}, f.err
	}
	if f.binding.Tenant.TenantID != tenantID {
		return tenant.Binding{}, tenant.ErrBindingTenantMismatch
	}
	return f.binding, nil
}

// --- helpers -------------------------------------------------------------

func testTask() worker.Task {
	return worker.Task{
		Tenant: config.TenantConfig{
			TenantID: "acme", Enabled: true, App: config.AppConfig{Name: "assistant"},
		},
		Binding: config.ChannelConfig{Type: "telegram", BindingID: "b1"},
		Message: domain.InboundMessage{
			ConversationID: "c1", ExternalUserID: "u1", ExternalMessageID: "m1",
			Scope: domain.ScopeDirect, Text: "hello",
		},
	}
}

func testOptions() DurableOptions {
	return DurableOptions{
		PollInterval: 5 * time.Millisecond, LeaseTTL: time.Minute,
		BatchSize: 8, InboxMaxAttempts: 2, OutboxMaxAttempts: 3,
		RetryBase: time.Millisecond, RetryMax: 2 * time.Millisecond, WorkerCount: 1,
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func newTestDurable(t *testing.T, proc *fakeProcessor, del *fakeDeliverer, opts DurableOptions) *Durable {
	t.Helper()
	d := NewDurable(store.NewMemory(), proc, fakeResolver{binding: tenant.Binding{
		Tenant:  config.TenantConfig{TenantID: "acme"},
		Channel: config.ChannelConfig{Type: "telegram", BindingID: "b1"},
	}}, del, nil, opts)
	d.Start()
	t.Cleanup(func() { _ = d.Close() })
	return d
}

// --- tests ---------------------------------------------------------------

func TestDurableEndToEndDeliversReply(t *testing.T) {
	proc := &fakeProcessor{}
	del := &fakeDeliverer{}
	d := newTestDurable(t, proc, del, testOptions())
	ctx := context.Background()
	if err := d.Submit(ctx, testTask()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	eventually(t, "delivery", func() bool {
		del.mu.Lock()
		defer del.mu.Unlock()
		return len(del.history) == 1
	})
	if del.history[0].Text != "reply" || del.history[0].Target != "c1" {
		t.Fatalf("delivered %+v", del.history[0])
	}
	if proc.tasks[0].Deliver {
		t.Fatal("relay must run tasks with Deliver=false; delivery is owned by the sender")
	}
	eventually(t, "drained queues", func() bool {
		runnable, dead, pending, deadOut, uncertain, err := d.store.Depths(ctx)
		return err == nil && runnable == 0 && dead == 0 && pending == 0 && deadOut == 0 && uncertain == 0
	})
}

func TestDurableAtomicTurnCompletionSkipsLegacyCommitAndPostCommitRetry(t *testing.T) {
	base := store.NewMemory()
	st := &pipelineAtomicStore{
		atomicMemoryStore: &atomicMemoryStore{Memory: base},
		identity:          "postgres-binding-v1:atomic-completion",
	}
	postErr := errors.New("coordinator cleanup failed after SQL commit")
	processor := &atomicFinalizingProcessor{postErr: postErr}
	d := NewDurable(st, processor, fakeResolver{binding: tenant.Binding{
		Tenant:  config.TenantConfig{TenantID: "acme"},
		Channel: config.ChannelConfig{Type: "telegram", BindingID: "b1"},
	}}, &fakeDeliverer{}, nil, testOptions())
	t.Cleanup(func() { _ = d.Close() })
	task := testTask()
	task.Tenant.Data.Session = config.BackendConfig{Type: "sql"}
	if err := d.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if worked := d.relayOnce(context.Background()); !worked {
		t.Fatal("relay did not lease the atomic Inbox")
	}
	if got := processor.calls.Load(); got != 1 {
		t.Fatalf("processor calls = %d, want 1", got)
	}
	if tx, legacy := st.txCompletes.Load(), st.legacyCompletes.Load(); tx != 1 || legacy != 0 {
		t.Fatalf("transaction/legacy completions = %d/%d, want 1/0", tx, legacy)
	}
	runnable, deadInbox, pending, deadOutbox, uncertain, err := st.Depths(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if runnable != 0 || deadInbox != 0 || pending != 2 || deadOutbox != 0 || uncertain != 0 {
		t.Fatalf("queue depths after committed post-error = %d/%d/%d/%d/%d",
			runnable, deadInbox, pending, deadOutbox, uncertain)
	}
	if records, err := st.LeaseInbox(context.Background(), "must-not-retry", time.Now(), time.Minute, 1); err != nil || len(records) != 0 {
		t.Fatalf("committed Inbox became retryable: records=%+v err=%v", records, err)
	}
}

func TestDurableSQLTerminalPolicyRejectionRecoversCompletionFailureWithoutPoison(t *testing.T) {
	base := store.NewMemory()
	st := &pipelineAtomicStore{
		atomicMemoryStore: &atomicMemoryStore{Memory: base},
		identity:          "postgres-binding-v1:policy-rejection",
	}
	// Exercise the critical crash/retry window: the Worker has returned a
	// terminal policy result, but the first Inbox acknowledgement does not land.
	st.legacyFailures.Store(1)
	runtimes := agentruntime.NewManager()
	coordinator := coordination.NewInMemory()
	processor := worker.NewService(
		runtimes, coordinator, channels.NewRegistry(), governance.NewFilter(), nil, nil,
		worker.Options{LockTTL: time.Minute, DedupTTL: time.Hour, RunTimeout: time.Second},
	)
	t.Cleanup(func() {
		_ = runtimes.Close()
		_ = coordinator.Close()
	})
	d := NewDurable(st, processor, fakeResolver{}, &fakeDeliverer{}, nil, testOptions())
	t.Cleanup(func() { _ = d.Close() })
	task := testTask()
	task.Tenant.Data.Session = config.BackendConfig{Type: "sql"}
	task.Binding.AllowedUsers = []string{"another-user"}
	if err := d.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	if worked := d.relayOnce(context.Background()); !worked {
		t.Fatal("relay did not lease the SQL policy-denied Inbox")
	}
	// The failed acknowledgement re-arms the Inbox. Its coordinator claim must
	// have been released, so the policy decision can be evaluated and consumed
	// again instead of entering the nonexistent SQL replay path.
	time.Sleep(5 * time.Millisecond)
	if worked := d.relayOnce(context.Background()); !worked {
		t.Fatal("terminal SQL Inbox was not recoverable after acknowledgement failure")
	}
	if tx, legacy := st.txCompletes.Load(), st.legacyCompletes.Load(); tx != 0 || legacy != 2 {
		t.Fatalf("transaction/legacy completions = %d/%d, want 0/2", tx, legacy)
	}
	runnable, deadInbox, pending, deadOutbox, uncertain, err := st.Depths(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if runnable != 0 || deadInbox != 0 || pending != 0 || deadOutbox != 0 || uncertain != 0 {
		t.Fatalf("queue depths after terminal policy decision = %d/%d/%d/%d/%d",
			runnable, deadInbox, pending, deadOutbox, uncertain)
	}
	if worked := d.relayOnce(context.Background()); worked {
		t.Fatal("terminal policy decision left a retryable SQL Inbox")
	}
}

func TestDurableRawLegacySQLPolicyRejectionRecoversCompletionFailure(t *testing.T) {
	base := store.NewMemory()
	st := &pipelineAtomicStore{
		atomicMemoryStore: &atomicMemoryStore{Memory: base},
		identity:          "postgres-binding-v1:legacy-policy-rejection",
	}
	// This is a pre-v2 row, so its policy-only terminal path must remain
	// recoverable without retroactively attaching an atomic participant.
	st.legacyFailures.Store(1)
	runtimes := agentruntime.NewManager()
	coordinator := coordination.NewInMemory()
	processor := worker.NewService(
		runtimes, coordinator, channels.NewRegistry(), governance.NewFilter(), nil, nil,
		worker.Options{LockTTL: time.Minute, DedupTTL: time.Hour, RunTimeout: time.Second},
	)
	t.Cleanup(func() {
		_ = runtimes.Close()
		_ = coordinator.Close()
	})
	d := NewDurable(st, processor, fakeResolver{}, &fakeDeliverer{}, nil, testOptions())
	t.Cleanup(func() { _ = d.Close() })
	task := testTask()
	task.Tenant.Data.Session = config.BackendConfig{Type: "sql"}
	task.Binding.AllowedUsers = []string{"another-user"}
	insertRawLegacyTask(t, st, task)

	if worked := d.relayOnce(context.Background()); !worked {
		t.Fatal("relay did not lease the raw legacy policy-denied Inbox")
	}
	time.Sleep(5 * time.Millisecond)
	if worked := d.relayOnce(context.Background()); !worked {
		t.Fatal("raw legacy terminal Inbox was not recoverable after acknowledgement failure")
	}
	if tx, legacy := st.txCompletes.Load(), st.legacyCompletes.Load(); tx != 0 || legacy != 2 {
		t.Fatalf("transaction/legacy completions = %d/%d, want 0/2", tx, legacy)
	}
	runnable, deadInbox, pending, deadOutbox, uncertain, err := st.Depths(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if runnable != 0 || deadInbox != 0 || pending != 0 || deadOutbox != 0 || uncertain != 0 {
		t.Fatalf("raw legacy policy depths = %d/%d/%d/%d/%d",
			runnable, deadInbox, pending, deadOutbox, uncertain)
	}
}

func TestDurableDeadLettersCorruptDispositionWithoutRetry(t *testing.T) {
	st := store.NewMemory()
	processor := &dispositionProcessor{
		err: errors.New("corrupt persisted replay"), disposition: worker.ProcessDeadLetter,
	}
	d := NewDurable(st, processor, fakeResolver{}, &fakeDeliverer{}, nil, testOptions())
	t.Cleanup(func() { _ = d.Close() })
	if err := d.Submit(context.Background(), testTask()); err != nil {
		t.Fatal(err)
	}
	if worked := d.relayOnce(context.Background()); !worked {
		t.Fatal("relay did not lease corrupt Inbox")
	}
	if got := processor.calls.Load(); got != 1 {
		t.Fatalf("processor calls = %d, want 1", got)
	}
	_, deadInbox, pending, _, _, err := st.Depths(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if deadInbox != 1 || pending != 0 {
		t.Fatalf("corrupt disposition depths dead/pending = %d/%d, want 1/0", deadInbox, pending)
	}
	if worked := d.relayOnce(context.Background()); worked {
		t.Fatal("dead-lettered corrupt Inbox became retryable")
	}
}

func TestDurableRetryableDispositionHonorsRetryAfter(t *testing.T) {
	st := store.NewMemory()
	processor := &dispositionProcessor{
		err:         errors.New("fixed-window rate limit"),
		disposition: worker.ProcessRetryable,
		retryAfter:  75 * time.Millisecond,
	}
	d := NewDurable(st, processor, fakeResolver{}, &fakeDeliverer{}, nil, testOptions())
	t.Cleanup(func() { _ = d.Close() })
	if err := d.Submit(context.Background(), testTask()); err != nil {
		t.Fatal(err)
	}
	if worked := d.relayOnce(context.Background()); !worked {
		t.Fatal("relay did not lease retryable Inbox")
	}
	if worked := d.relayOnce(context.Background()); worked {
		t.Fatal("retryable Inbox ignored processor retry_after")
	}
	if got := processor.calls.Load(); got != 1 {
		t.Fatalf("processor calls before retry window = %d, want 1", got)
	}
}

func TestDurableBlockedDispositionDoesNotExhaustAttemptBudget(t *testing.T) {
	st := store.NewMemory()
	processor := &dispositionProcessor{
		err:         worker.ErrAtomicCommitUnavailable,
		disposition: worker.ProcessBlocked,
		retryAfter:  2 * time.Millisecond,
	}
	opts := testOptions()
	opts.InboxMaxAttempts = 1
	d := NewDurable(st, processor, fakeResolver{}, &fakeDeliverer{}, nil, opts)
	t.Cleanup(func() { _ = d.Close() })
	if err := d.Submit(context.Background(), testTask()); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(2 * processor.retryAfter)
		}
		if worked := d.relayOnce(context.Background()); !worked {
			t.Fatalf("blocked Inbox was not leased on attempt %d", attempt+1)
		}
	}
	if got := processor.calls.Load(); got != 3 {
		t.Fatalf("processor calls = %d, want 3", got)
	}
	runnable, deadInbox, pending, _, _, err := st.Depths(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if runnable != 1 || deadInbox != 0 || pending != 0 {
		t.Fatalf("blocked disposition depths runnable/dead/pending = %d/%d/%d, want 1/0/0",
			runnable, deadInbox, pending)
	}
}

func TestDurableToolOutcomeBlockUsesReconciliationMetricNotPipelineMetric(t *testing.T) {
	st := store.NewMemory()
	processor := &dispositionProcessor{
		err: tooloperation.ErrOutcomeUnknown, disposition: worker.ProcessBlocked,
		retryAfter: time.Hour,
	}
	d := NewDurable(st, processor, fakeResolver{}, &fakeDeliverer{}, nil, testOptions())
	t.Cleanup(func() { _ = d.Close() })
	if err := d.Submit(context.Background(), testTask()); err != nil {
		t.Fatal(err)
	}
	if worked := d.relayOnce(context.Background()); !worked {
		t.Fatal("relay did not park the tool-outcome Inbox")
	}

	recorder := httptest.NewRecorder()
	d.metrics.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	if !strings.Contains(body, `queue_inbox_processing_blocked_total{tenant="acme",result="tool_outcome_unknown"} 1`) {
		t.Fatalf("tool reconciliation block metric missing:\n%s", body)
	}
	if strings.Contains(body, "queue_inbox_pipeline_blocked_total") {
		t.Fatalf("tool outcome was misclassified as a pipeline rollout block:\n%s", body)
	}
}

type partitionBlockingProcessor struct {
	starts  chan string
	release map[string]chan struct{}
}

func (p *partitionBlockingProcessor) Process(ctx context.Context, task worker.Task) (worker.Result, error) {
	id := task.Message.ExternalMessageID
	select {
	case p.starts <- id:
	case <-ctx.Done():
		return worker.Result{}, ctx.Err()
	}
	select {
	case <-p.release[id]:
	case <-ctx.Done():
		return worker.Result{}, ctx.Err()
	}
	return worker.Result{Text: id, Outbound: &domain.OutboundMessage{
		Target: task.Message.ConversationID, Text: id,
	}}, nil
}

func TestDurableRunsSessionsConcurrentlyButOneSessionFIFO(t *testing.T) {
	st := store.NewMemory()
	processor := &partitionBlockingProcessor{
		starts: make(chan string, 3),
		release: map[string]chan struct{}{
			"m1": make(chan struct{}),
			"m2": make(chan struct{}),
			"m3": make(chan struct{}),
		},
	}
	del := &fakeDeliverer{}
	opts := testOptions()
	opts.WorkerCount = 2
	opts.BatchSize = 2
	opts.PollInterval = 2 * time.Millisecond
	d := NewDurable(st, processor, fakeResolver{binding: tenant.Binding{
		Tenant:  config.TenantConfig{TenantID: "acme"},
		Channel: config.ChannelConfig{Type: "telegram", BindingID: "b1"},
	}}, del, nil, opts)

	first := testTask()
	second := testTask()
	second.Message.ExternalMessageID = "m2"
	other := testTask()
	other.Message.ExternalMessageID = "m3"
	other.Message.ExternalUserID = "u2"
	other.Message.ConversationID = "c2"
	for _, task := range []worker.Task{first, second, other} {
		if err := d.Submit(context.Background(), task); err != nil {
			t.Fatalf("submit %s: %v", task.Message.ExternalMessageID, err)
		}
	}
	d.Start()
	t.Cleanup(func() { _ = d.Close() })

	started := map[string]bool{}
	for len(started) < 2 {
		select {
		case id := <-processor.starts:
			started[id] = true
		case <-time.After(time.Second):
			t.Fatalf("initial starts = %v, want m1 and m3", started)
		}
	}
	if !started["m1"] || !started["m3"] || started["m2"] {
		t.Fatalf("initial starts = %v, want m1 and cross-session m3", started)
	}

	// Free the cross-session worker. It must remain idle because m1 is still
	// the active head for m2's partition.
	close(processor.release["m3"])
	select {
	case id := <-processor.starts:
		t.Fatalf("same-session successor %s started before m1 completed", id)
	case <-time.After(75 * time.Millisecond):
	}

	close(processor.release["m1"])
	select {
	case id := <-processor.starts:
		if id != "m2" {
			t.Fatalf("next same-session start = %s, want m2", id)
		}
	case <-time.After(time.Second):
		t.Fatal("m2 did not start after m1 completed")
	}
	close(processor.release["m2"])
	eventually(t, "all partitioned replies delivered", func() bool {
		del.mu.Lock()
		defer del.mu.Unlock()
		return len(del.history) == 3
	})
}

func TestDurableReadinessTracksLifecycleAndStore(t *testing.T) {
	st := store.NewMemory()
	d := NewDurable(st, &fakeProcessor{}, fakeResolver{binding: tenant.Binding{
		Tenant:  config.TenantConfig{TenantID: "acme"},
		Channel: config.ChannelConfig{Type: "telegram", BindingID: "b1"},
	}}, &fakeDeliverer{}, nil, testOptions())
	if err := d.Ready(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("readiness before start = %v, want ErrClosed", err)
	}
	d.Start()
	if err := d.Ready(context.Background()); err != nil {
		t.Fatalf("active readiness: %v", err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := d.Ready(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("readiness after close = %v, want ErrClosed", err)
	}
}

func TestDurableOutboxContinuesInboundTrace(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
		_ = provider.Shutdown(context.Background())
	})

	rootCtx, rootSpan := otel.Tracer("durable-test").Start(context.Background(), "callback")
	wantTraceID := rootSpan.SpanContext().TraceID()
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(rootCtx, carrier)
	rootSpan.End()

	proc := &fakeProcessor{}
	del := &fakeDeliverer{}
	d := newTestDurable(t, proc, del, testOptions())
	task := testTask()
	task.TraceCarrier = map[string]string(carrier)
	if err := d.Submit(context.Background(), task); err != nil {
		t.Fatalf("submit: %v", err)
	}
	eventually(t, "traced delivery", func() bool {
		del.mu.Lock()
		defer del.mu.Unlock()
		return len(del.traceIDs) == 1
	})
	del.mu.Lock()
	gotTraceID := del.traceIDs[0]
	del.mu.Unlock()
	if gotTraceID != wantTraceID {
		t.Fatalf("outbox trace = %s, want inbound trace %s", gotTraceID, wantTraceID)
	}
}

func TestDurableSubmitDeduplicatesPlatformRedelivery(t *testing.T) {
	proc := &fakeProcessor{}
	del := &fakeDeliverer{}
	d := newTestDurable(t, proc, del, testOptions())
	ctx := context.Background()
	if err := d.Submit(ctx, testTask()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	// The platform redelivers the same message before ACK; Submit reports
	// success so the gateway ACKs without double processing.
	if err := d.Submit(ctx, testTask()); err != nil {
		t.Fatalf("redelivery submit: %v", err)
	}
	eventually(t, "single delivery", func() bool {
		del.mu.Lock()
		defer del.mu.Unlock()
		return len(del.history) == 1
	})
	proc.mu.Lock()
	defer proc.mu.Unlock()
	if proc.runs != 1 {
		t.Fatalf("processor ran %d times, want 1", proc.runs)
	}
}

func TestDurableOutboxRetriesDeliveryThenSucceeds(t *testing.T) {
	proc := &fakeProcessor{}
	del := &fakeDeliverer{}
	del.failN.Store(2) // first two attempts fail
	d := newTestDurable(t, proc, del, testOptions())
	if err := d.Submit(context.Background(), testTask()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	eventually(t, "delivery after retries", func() bool {
		del.mu.Lock()
		defer del.mu.Unlock()
		return len(del.history) >= 3
	})
	ctx := context.Background()
	eventually(t, "sent state", func() bool {
		runnable, deadIn, pending, deadOut, uncertain, err := d.store.Depths(ctx)
		return err == nil && runnable == 0 && deadIn == 0 && pending == 0 && deadOut == 0 && uncertain == 0
	})
}

func TestDurableOutboxDeadLettersAfterMaxAttempts(t *testing.T) {
	proc := &fakeProcessor{}
	del := &fakeDeliverer{}
	del.failN.Store(1 << 30) // always fail
	opts := testOptions()
	opts.OutboxMaxAttempts = 3
	d := newTestDurable(t, proc, del, opts)
	if err := d.Submit(context.Background(), testTask()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	ctx := context.Background()
	eventually(t, "outbox dead letter", func() bool {
		_, deadIn, _, deadOut, _, err := d.store.Depths(ctx)
		return err == nil && deadIn == 0 && deadOut == 1
	})
	del.mu.Lock()
	attempts := len(del.history)
	del.mu.Unlock()
	if attempts != 3 {
		t.Fatalf("delivery attempts = %d, want 3", attempts)
	}
}

func TestDurablePlansOneOutboxPerPartWithStableOperationKeys(t *testing.T) {
	proc := &fakeProcessor{}
	del := &fakeDeliverer{planParts: []string{"part-1", "part-2", "part-3"}}
	d := newTestDurable(t, proc, del, testOptions())
	if err := d.Submit(context.Background(), testTask()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	eventually(t, "all delivery parts", func() bool {
		del.mu.Lock()
		defer del.mu.Unlock()
		return len(del.requests) == 3
	})
	del.mu.Lock()
	defer del.mu.Unlock()
	for index, request := range del.requests {
		if request.Message.Text != "part-"+strconv.Itoa(index+1) {
			t.Fatalf("part %d text = %q", index, request.Message.Text)
		}
		if request.OperationKey == "" || request.AttemptNo != 1 {
			t.Fatalf("part %d request = %+v", index, request)
		}
		if index > 0 && request.OperationKey == del.requests[index-1].OperationKey {
			t.Fatalf("parts %d and %d reused operation key", index-1, index)
		}
	}
}

func TestDurableUnknownParksWithoutAutomaticRetryAndManualRetryKeepsOperationKey(t *testing.T) {
	proc := &fakeProcessor{}
	del := &fakeDeliverer{result: delivery.Result{Outcome: delivery.Unknown, ErrorType: "network_unknown"}}
	d := newTestDurable(t, proc, del, testOptions())
	if err := d.Submit(context.Background(), testTask()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	eventually(t, "unknown operation", func() bool {
		_, _, pending, _, uncertain, err := d.store.Depths(context.Background())
		return err == nil && pending == 0 && uncertain == 1
	})
	time.Sleep(4 * testOptions().PollInterval)
	del.mu.Lock()
	if len(del.requests) != 1 {
		t.Fatalf("unknown operation attempts = %d, want 1", len(del.requests))
	}
	firstKey := del.requests[0].OperationKey
	del.result = delivery.Result{Outcome: delivery.Confirmed, ProviderMessageID: "confirmed-after-review"}
	del.mu.Unlock()
	operations, err := d.ListUncertainOutbox(context.Background(), 10)
	if err != nil || len(operations) != 1 {
		t.Fatalf("list uncertain = %+v, %v", operations, err)
	}
	if err := d.ResolveOutbox(context.Background(), ResolveOutboxRequest{
		ResolutionID: uuid.NewString(), OutboxID: operations[0].OutboxID,
		ExpectedVersion: operations[0].StateVersion, ExpectedAttempt: operations[0].AttemptNo,
		Action: store.ResolveRetry, Reason: "operator accepted duplicate risk", Actor: "test",
	}); err != nil {
		t.Fatalf("resolve retry: %v", err)
	}
	eventually(t, "explicit retry delivered", func() bool {
		del.mu.Lock()
		defer del.mu.Unlock()
		return len(del.requests) == 2
	})
	del.mu.Lock()
	defer del.mu.Unlock()
	if del.requests[1].OperationKey != firstKey || del.requests[1].AttemptNo != 2 {
		t.Fatalf("manual retry identity = %+v, first key %q", del.requests[1], firstKey)
	}
}

func TestDurableUnknownPartBlocksLaterPartsInSameLane(t *testing.T) {
	proc := &fakeProcessor{}
	del := &fakeDeliverer{planParts: []string{"part-1", "part-2", "part-3"}}
	del.resultFn = func(request delivery.Request) delivery.Result {
		if request.Message.Text == "part-2" {
			return delivery.Result{Outcome: delivery.Unknown, ErrorType: "response_lost"}
		}
		return delivery.Result{Outcome: delivery.Confirmed}
	}
	d := newTestDurable(t, proc, del, testOptions())
	if err := d.Submit(context.Background(), testTask()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	eventually(t, "second part unknown", func() bool {
		del.mu.Lock()
		defer del.mu.Unlock()
		return len(del.requests) == 2
	})
	time.Sleep(10 * testOptions().PollInterval)
	del.mu.Lock()
	defer del.mu.Unlock()
	if len(del.requests) != 2 || del.requests[0].Message.Text != "part-1" || del.requests[1].Message.Text != "part-2" {
		t.Fatalf("requests after unknown middle part = %+v", del.requests)
	}
}

func TestDurableInboxDeadLettersPersistentProcessingFailure(t *testing.T) {
	proc := &fakeProcessor{fail: errors.New("provider down")}
	del := &fakeDeliverer{}
	opts := testOptions()
	opts.InboxMaxAttempts = 2
	d := newTestDurable(t, proc, del, opts)
	if err := d.Submit(context.Background(), testTask()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	ctx := context.Background()
	eventually(t, "inbox dead letter", func() bool {
		runnable, deadIn, _, _, _, err := d.store.Depths(ctx)
		return err == nil && runnable == 0 && deadIn == 1
	})
	del.mu.Lock()
	delivered := len(del.history)
	del.mu.Unlock()
	if delivered != 0 {
		t.Fatalf("dead-lettered message must not deliver, got %d deliveries", delivered)
	}
}

func TestDurableReclaimsExpiredLeaseAndFinishes(t *testing.T) {
	// Simulate a crashed worker: a row is leased directly in the store and
	// never completed. The reclaim loop must hand it back for processing.
	st := store.NewMemory()
	proc := &fakeProcessor{}
	del := &fakeDeliverer{}
	d := NewDurable(st, proc, fakeResolver{binding: tenant.Binding{
		Tenant:  config.TenantConfig{TenantID: "acme"},
		Channel: config.ChannelConfig{Type: "telegram", BindingID: "b1"},
	}}, del, nil, testOptions())
	if err := st.InsertInbox(context.Background(), &store.InboxRecord{
		InboxID: "orphan", TenantID: "acme", ChannelType: "telegram", BindingID: "b1",
		DedupKey: "orphan-key", Payload: mustJSON(t, testTask()),
	}, time.Now()); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// The crashed worker leases with a TTL that is already in the past.
	if _, err := st.LeaseInbox(context.Background(), "ghost", time.Now(), -time.Second, 10); err != nil {
		t.Fatalf("ghost lease: %v", err)
	}
	d.Start()
	t.Cleanup(func() { _ = d.Close() })
	eventually(t, "orphan delivered", func() bool {
		del.mu.Lock()
		defer del.mu.Unlock()
		return len(del.history) == 1
	})
}

func TestDurableRenewsLeaseForSlowProcessor(t *testing.T) {
	proc := &fakeProcessor{delay: 120 * time.Millisecond}
	del := &fakeDeliverer{}
	opts := testOptions()
	opts.LeaseTTL = 30 * time.Millisecond
	opts.PollInterval = 2 * time.Millisecond
	d := newTestDurable(t, proc, del, opts)
	if err := d.Submit(context.Background(), testTask()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	eventually(t, "slow delivery", func() bool {
		del.mu.Lock()
		defer del.mu.Unlock()
		return len(del.history) == 1
	})
	proc.mu.Lock()
	runs := proc.runs
	proc.mu.Unlock()
	if runs != 1 {
		t.Fatalf("slow processor ran %d times after lease reclaim, want 1", runs)
	}
}

func TestDurableDoesNotDeliverWithReassignedTenantBinding(t *testing.T) {
	st := store.NewMemory()
	proc := &fakeProcessor{}
	del := &fakeDeliverer{}
	opts := testOptions()
	opts.OutboxMaxAttempts = 1
	d := NewDurable(st, proc, fakeResolver{binding: tenant.Binding{
		Tenant:  config.TenantConfig{TenantID: "tenant-b"},
		Channel: config.ChannelConfig{Type: "telegram", BindingID: "b1"},
	}}, del, nil, opts)
	d.Start()
	t.Cleanup(func() { _ = d.Close() })
	if err := d.Submit(context.Background(), testTask()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	eventually(t, "tenant-mismatched outbox dead letter", func() bool {
		_, _, _, deadOut, _, err := st.Depths(context.Background())
		return err == nil && deadOut == 1
	})
	del.mu.Lock()
	defer del.mu.Unlock()
	if len(del.history) != 0 {
		t.Fatalf("cross-tenant binding delivered %d messages", len(del.history))
	}
}

type failCompleteStore struct {
	store.Store
	failures atomic.Int32
}

func (s *failCompleteStore) CompleteInbox(ctx context.Context, inboxID, owner string, outbox *store.OutboxRecord, now time.Time) error {
	return s.Store.CompleteInbox(ctx, inboxID, owner, outbox, now)
}

func (s *failCompleteStore) CompleteInboxBatch(ctx context.Context, inboxID, owner string, outboxes []store.OutboxRecord, now time.Time) error {
	if s.failures.Add(-1) >= 0 {
		return errors.New("injected inbox completion failure")
	}
	return s.Store.CompleteInboxBatch(ctx, inboxID, owner, outboxes, now)
}

type replayingProcessor struct {
	mu        sync.Mutex
	calls     int
	agentRuns int
	results   map[string]worker.Result
}

func (p *replayingProcessor) Process(_ context.Context, task worker.Task) (worker.Result, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	key := task.Message.ExternalMessageID
	if result, ok := p.results[key]; ok {
		return result, nil
	}
	p.agentRuns++
	result := worker.Result{Text: "reply", Outbound: &domain.OutboundMessage{
		Target: task.Message.ConversationID, Text: "reply",
	}}
	if p.results == nil {
		p.results = make(map[string]worker.Result)
	}
	p.results[key] = result
	return result, nil
}

func TestDurableRetriesFailedInboxCommitFromReplayableResult(t *testing.T) {
	baseStore := store.NewMemory()
	st := &failCompleteStore{Store: baseStore}
	st.failures.Store(1)
	proc := &replayingProcessor{}
	del := &fakeDeliverer{}
	d := NewDurable(st, proc, fakeResolver{binding: tenant.Binding{
		Tenant:  config.TenantConfig{TenantID: "acme"},
		Channel: config.ChannelConfig{Type: "telegram", BindingID: "b1"},
	}}, del, nil, testOptions())
	d.Start()
	t.Cleanup(func() { _ = d.Close() })
	if err := d.Submit(context.Background(), testTask()); err != nil {
		t.Fatalf("submit: %v", err)
	}
	eventually(t, "delivery after inbox commit retry", func() bool {
		del.mu.Lock()
		defer del.mu.Unlock()
		return len(del.history) == 1
	})
	proc.mu.Lock()
	defer proc.mu.Unlock()
	if proc.calls < 2 || proc.agentRuns != 1 {
		t.Fatalf("process calls=%d agent runs=%d, want replay without a second Agent run", proc.calls, proc.agentRuns)
	}
}

var errInjectedLedgerCommitResult = errors.New("injected ambiguous ledger commit result")

type deliveryLedgerFaultStore struct {
	store.Store
	markCommittedThenError   bool
	finishBeforeCommitError  bool
	finishCommittedThenError bool
	renewOutboxError         bool
}

func (s *deliveryLedgerFaultStore) MarkOutboxDispatched(
	ctx context.Context,
	outboxID, owner string,
	attemptNo int,
	now time.Time,
) error {
	err := s.Store.MarkOutboxDispatched(ctx, outboxID, owner, attemptNo, now)
	if err == nil && s.markCommittedThenError {
		return errInjectedLedgerCommitResult
	}
	return err
}

func (s *deliveryLedgerFaultStore) FinishOutboxAttempt(
	ctx context.Context,
	outboxID, owner string,
	attemptNo int,
	result delivery.Result,
	now, retryAt time.Time,
	exhausted bool,
) error {
	if s.finishBeforeCommitError {
		return errInjectedLedgerCommitResult
	}
	err := s.Store.FinishOutboxAttempt(ctx, outboxID, owner, attemptNo, result, now, retryAt, exhausted)
	if err == nil && s.finishCommittedThenError {
		return errInjectedLedgerCommitResult
	}
	return err
}

func (s *deliveryLedgerFaultStore) RenewOutboxLease(
	ctx context.Context,
	outboxID, owner string,
	now time.Time,
	leaseTTL time.Duration,
) error {
	if s.renewOutboxError {
		return store.ErrLeaseLost
	}
	return s.Store.RenewOutboxLease(ctx, outboxID, owner, now, leaseTTL)
}

func prepareLeasedDeliveryOperation(
	t *testing.T,
	st store.Store,
	owner string,
	leaseTTL time.Duration,
) store.OutboxRecord {
	t.Helper()
	ctx := context.Background()
	now := time.Now()
	suffix := uuid.NewString()
	inbox := &store.InboxRecord{
		InboxID: suffix, TenantID: "acme", ChannelType: "telegram", BindingID: "b1",
		ExternalMessageID: "external-" + suffix, DedupKey: "inbox-" + suffix,
		PartitionKey: "session-" + suffix, Payload: mustJSON(t, testTask()),
	}
	if err := st.InsertInbox(ctx, inbox, now); err != nil {
		t.Fatalf("insert fault-window inbox: %v", err)
	}
	relayOwner := "relay-" + suffix
	if records, err := st.LeaseInbox(ctx, relayOwner, now, leaseTTL, 1); err != nil || len(records) != 1 {
		t.Fatalf("lease fault-window inbox: records=%+v err=%v", records, err)
	}
	operationKey := "operation-" + suffix
	outbox := outboxRecordForTest(t, operationKey, inbox.PartitionKey)
	if err := st.CompleteInboxBatch(ctx, inbox.InboxID, relayOwner, []store.OutboxRecord{outbox}, now); err != nil {
		t.Fatalf("complete fault-window inbox: %v", err)
	}
	records, err := st.LeaseOutbox(ctx, owner, now, leaseTTL, 1)
	if err != nil || len(records) != 1 {
		t.Fatalf("lease fault-window outbox: records=%+v err=%v", records, err)
	}
	return records[0]
}

func outboxRecordForTest(t *testing.T, operationKey, partitionKey string) store.OutboxRecord {
	t.Helper()
	return store.OutboxRecord{
		OutboxID: uuid.NewString(), OperationKey: operationKey, OperationVersion: 1,
		PartIndex: 0, PartCount: 1,
		TenantID: "acme", ChannelType: "telegram", BindingID: "b1",
		DedupKey: operationKey, PartitionKey: partitionKey,
		Payload: mustJSON(t, domain.OutboundMessage{Target: "c1", Text: "reply"}),
	}
}

func newUnstartedTestDurable(st store.Store, deliverer Deliverer, opts DurableOptions) *Durable {
	return NewDurable(st, &fakeProcessor{}, fakeResolver{binding: tenant.Binding{
		Tenant:  config.TenantConfig{TenantID: "acme"},
		Channel: config.ChannelConfig{Type: "telegram", BindingID: "b1"},
	}}, deliverer, nil, opts)
}

func TestDurableDeliveryLedgerCommitFaultWindowsNeverBlindRetry(t *testing.T) {
	tests := []struct {
		name              string
		configure         func(*deliveryLedgerFaultStore)
		wantProviderCalls int
		wantUnknown       bool
	}{
		{
			name: "dispatch marker committed but acknowledgement lost",
			configure: func(st *deliveryLedgerFaultStore) {
				st.markCommittedThenError = true
			},
			wantProviderCalls: 0,
			wantUnknown:       true,
		},
		{
			name: "provider called but attempt finalization did not commit",
			configure: func(st *deliveryLedgerFaultStore) {
				st.finishBeforeCommitError = true
			},
			wantProviderCalls: 1,
			wantUnknown:       true,
		},
		{
			name: "attempt finalized but commit acknowledgement lost",
			configure: func(st *deliveryLedgerFaultStore) {
				st.finishCommittedThenError = true
			},
			wantProviderCalls: 1,
			wantUnknown:       false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base := store.NewMemory()
			faultStore := &deliveryLedgerFaultStore{Store: base}
			test.configure(faultStore)
			deliverer := &fakeDeliverer{}
			opts := testOptions()
			opts.LeaseTTL = time.Minute
			owner := "sender-" + uuid.NewString()
			record := prepareLeasedDeliveryOperation(t, faultStore, owner, opts.LeaseTTL)
			durable := newUnstartedTestDurable(faultStore, deliverer, opts)

			durable.deliverOutbox(context.Background(), &record, owner)
			deliverer.mu.Lock()
			providerCalls := len(deliverer.requests)
			deliverer.mu.Unlock()
			if providerCalls != test.wantProviderCalls {
				t.Fatalf("provider calls = %d, want %d", providerCalls, test.wantProviderCalls)
			}

			_, reclaimed, err := base.ReclaimExpired(context.Background(), time.Now().Add(2*time.Minute))
			if err != nil {
				t.Fatalf("reclaim fault-window operation: %v", err)
			}
			unknown, err := base.ListUncertainOutbox(context.Background(), 10)
			if err != nil {
				t.Fatalf("list fault-window unknown: %v", err)
			}
			if test.wantUnknown {
				if reclaimed != 1 || len(unknown) != 1 || unknown[0].OutboxID != record.OutboxID {
					t.Fatalf("reclaimed=%d unknown=%+v, want the dispatched operation parked", reclaimed, unknown)
				}
			} else {
				if reclaimed != 0 || len(unknown) != 0 {
					t.Fatalf("confirmed operation was reclaimed: reclaimed=%d unknown=%+v", reclaimed, unknown)
				}
				if records, err := base.LeaseOutbox(context.Background(), "second-sender", time.Now().Add(2*time.Minute), time.Minute, 1); err != nil || len(records) != 0 {
					t.Fatalf("finalized operation became sendable again: records=%+v err=%v", records, err)
				}
			}
		})
	}
}

type cancellationBlockingDeliverer struct {
	calls atomic.Int32
}

func (*cancellationBlockingDeliverer) PlanDelivery(_ config.ChannelConfig, msg domain.OutboundMessage) ([]delivery.Part, error) {
	return []delivery.Part{{Message: msg, Index: 0, Total: 1}}, nil
}

func (d *cancellationBlockingDeliverer) DeliverOperation(
	ctx context.Context,
	_ config.ChannelConfig,
	_ delivery.Request,
) delivery.Result {
	d.calls.Add(1)
	<-ctx.Done()
	return delivery.Result{Outcome: delivery.Unknown, ErrorType: "context_canceled"}
}

func TestDurableLeaseRenewalFailureAfterDispatchParksUnknown(t *testing.T) {
	base := store.NewMemory()
	faultStore := &deliveryLedgerFaultStore{Store: base, renewOutboxError: true}
	deliverer := &cancellationBlockingDeliverer{}
	opts := testOptions()
	opts.LeaseTTL = 15 * time.Millisecond
	owner := "sender-" + uuid.NewString()
	record := prepareLeasedDeliveryOperation(t, faultStore, owner, opts.LeaseTTL)
	durable := newUnstartedTestDurable(faultStore, deliverer, opts)

	durable.deliverOutbox(context.Background(), &record, owner)
	if calls := deliverer.calls.Load(); calls != 1 {
		t.Fatalf("provider calls = %d, want 1", calls)
	}
	_, reclaimed, err := base.ReclaimExpired(context.Background(), time.Now().Add(time.Minute))
	if err != nil || reclaimed != 1 {
		t.Fatalf("reclaim renewal-failed operation: count=%d err=%v", reclaimed, err)
	}
	unknown, err := base.ListUncertainOutbox(context.Background(), 10)
	if err != nil || len(unknown) != 1 || unknown[0].OutboxID != record.OutboxID {
		t.Fatalf("renewal-failed operation not parked unknown: records=%+v err=%v", unknown, err)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return data
}
