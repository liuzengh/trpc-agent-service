package worker

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	agentruntime "github.com/cyl6/trpc-agent-service/trpcservice/agent"
	"github.com/cyl6/trpc-agent-service/trpcservice/channels"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/coordination"
	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
	auditlog "github.com/cyl6/trpc-agent-service/trpcservice/log"
	"github.com/cyl6/trpc-agent-service/trpcservice/metrics"
	"github.com/cyl6/trpc-agent-service/trpcservice/sessionturn"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant/governance"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

const (
	strictTestAppNamespace     = "tenant-strict/assistant"
	strictTestDatabaseIdentity = "postgres-binding-v1:strict-test"
)

type strictTurnContextKey struct{}

type strictTestRunner struct {
	mu             sync.Mutex
	calls          int
	sawTurnContext bool
	run            func(context.Context) (<-chan *event.Event, error)
}

func (r *strictTestRunner) Run(
	ctx context.Context,
	_ string,
	_ string,
	_ model.Message,
	_ ...agent.RunOption,
) (<-chan *event.Event, error) {
	r.mu.Lock()
	r.calls++
	r.sawTurnContext = ctx.Value(strictTurnContextKey{}) == true
	run := r.run
	r.mu.Unlock()
	if run != nil {
		return run(ctx)
	}
	return strictCompletionEvents("local result", 3, 4), nil
}

func (*strictTestRunner) Close() error { return nil }

func (r *strictTestRunner) snapshot() (calls int, sawTurnContext bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls, r.sawTurnContext
}

type strictTestTurn struct {
	mu         sync.Mutex
	replay     []byte
	replayed   bool
	stageErr   error
	commitFn   func(context.Context, []byte) ([]byte, bool, error)
	commits    int
	aborts     int
	lastCommit []byte
}

func (t *strictTestTurn) Replay() ([]byte, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]byte(nil), t.replay...), t.replayed
}

func (t *strictTestTurn) Commit(ctx context.Context, replay []byte) ([]byte, bool, error) {
	t.mu.Lock()
	t.commits++
	t.lastCommit = append([]byte(nil), replay...)
	commitFn := t.commitFn
	t.mu.Unlock()
	if commitFn != nil {
		return commitFn(ctx, replay)
	}
	return append([]byte(nil), replay...), false, nil
}

func (t *strictTestTurn) CommitWithParticipant(
	ctx context.Context,
	replay []byte,
	participant sessionturn.CommitParticipant,
) ([]byte, bool, error) {
	canonical, replayed, err := t.Commit(ctx, replay)
	if err != nil {
		return nil, false, err
	}
	if participant != nil {
		if err := participant(ctx, nil, canonical); err != nil {
			return nil, false, err
		}
	}
	return canonical, replayed, nil
}

func (t *strictTestTurn) Abort() {
	t.mu.Lock()
	t.aborts++
	t.mu.Unlock()
}

func (t *strictTestTurn) Err() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stageErr
}

func (t *strictTestTurn) counts() (commits, aborts int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.commits, t.aborts
}

type strictTestTurnParticipant struct {
	mu       sync.Mutex
	plan     AtomicDeliveryPlan
	complete int
	marked   int
	err      error
}

func (p *strictTestTurnParticipant) CompleteInTransaction(
	_ context.Context,
	_ pgx.Tx,
	plan AtomicDeliveryPlan,
) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.complete++
	p.plan = AtomicDeliveryPlan{
		Version: plan.Version, Outbound: plan.Outbound,
		Parts: append([]domain.OutboundMessage(nil), plan.Parts...),
	}
	return p.err
}

func (p *strictTestTurnParticipant) MarkCommitted() {
	p.mu.Lock()
	p.marked++
	p.mu.Unlock()
}

func (p *strictTestTurnParticipant) snapshot() (AtomicDeliveryPlan, int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.plan, p.complete, p.marked
}

type strictTestSession struct {
	session.Service
	mu              sync.Mutex
	turn            sessionturn.Turn
	beginErr        error
	beginCalls      int
	lastKey         session.Key
	lastIdempotency string
}

func (s *strictTestSession) BeginTurn(
	ctx context.Context,
	key session.Key,
	idempotencyKey string,
) (context.Context, sessionturn.Turn, error) {
	s.mu.Lock()
	s.beginCalls++
	s.lastKey = key
	s.lastIdempotency = idempotencyKey
	turn := s.turn
	err := s.beginErr
	s.mu.Unlock()
	if err != nil {
		return ctx, nil, err
	}
	return context.WithValue(ctx, strictTurnContextKey{}, true), turn, nil
}

func (s *strictTestSession) snapshot() (calls int, key session.Key, idempotencyKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.beginCalls, s.lastKey, s.lastIdempotency
}

type strictCountingCoordinator struct {
	*coordination.InMemory
	mu     sync.Mutex
	saves  int
	onSave func()
}

func newStrictCountingCoordinator() *strictCountingCoordinator {
	return &strictCountingCoordinator{InMemory: coordination.NewInMemory()}
}

func (c *strictCountingCoordinator) SaveResult(
	ctx context.Context,
	key string,
	ownerToken string,
	value any,
	ttl time.Duration,
) error {
	c.mu.Lock()
	c.saves++
	onSave := c.onSave
	c.mu.Unlock()
	if onSave != nil {
		onSave()
	}
	return c.InMemory.SaveResult(ctx, key, ownerToken, value, ttl)
}

func (c *strictCountingCoordinator) saveCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.saves
}

type strictCaptureAdapter struct {
	mu       sync.Mutex
	delivers int
	last     domain.OutboundMessage
	onSend   func()
}

func (*strictCaptureAdapter) Name() string { return "telegram" }

func (*strictCaptureAdapter) Verify(*http.Request, []byte, config.ChannelConfig) error { return nil }

func (*strictCaptureAdapter) Parse([]byte, config.ChannelConfig) (channels.ParsedWebhook, error) {
	return channels.ParsedWebhook{}, nil
}

func (*strictCaptureAdapter) Plan(_ config.ChannelConfig, message domain.OutboundMessage) ([]delivery.Part, error) {
	return []delivery.Part{{Message: message, Index: 0, Total: 1}}, nil
}

func (a *strictCaptureAdapter) Deliver(
	_ context.Context,
	_ config.ChannelConfig,
	request delivery.Request,
) delivery.Result {
	a.mu.Lock()
	a.delivers++
	a.last = request.Message
	onSend := a.onSend
	a.mu.Unlock()
	if onSend != nil {
		onSend()
	}
	return delivery.Result{Outcome: delivery.Confirmed}
}

func (a *strictCaptureAdapter) snapshot() (int, domain.OutboundMessage) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.delivers, a.last
}

type strictAuditSink struct {
	mu      sync.Mutex
	entries []auditlog.Entry
	onWrite func()
}

func (s *strictAuditSink) Write(entry auditlog.Entry) error {
	s.mu.Lock()
	s.entries = append(s.entries, entry)
	onWrite := s.onWrite
	s.mu.Unlock()
	if onWrite != nil {
		onWrite()
	}
	return nil
}

func (s *strictAuditSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

type strictOrder struct {
	mu    sync.Mutex
	items []string
}

func (o *strictOrder) add(item string) {
	o.mu.Lock()
	o.items = append(o.items, item)
	o.mu.Unlock()
}

func (o *strictOrder) snapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.items...)
}

func strictCompletionEvents(text string, promptTokens, completionTokens int) <-chan *event.Event {
	events := make(chan *event.Event, 1)
	events <- &event.Event{InvocationID: "root", Response: &model.Response{
		Object: model.ObjectTypeRunnerCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Message: model.NewAssistantMessage(text),
		}},
		Usage: &model.Usage{PromptTokens: promptTokens, CompletionTokens: completionTokens},
	}}
	close(events)
	return events
}

func strictTask(deliver bool) Task {
	tenantConfig := testTenant("tenant-strict")
	tenantConfig.Data.Session = config.BackendConfig{Type: "sql"}
	tenantConfig.Model.InputPrice = 1
	tenantConfig.Model.OutputPrice = 2
	task := taskFor(tenantConfig, "strict-message-1", "user-1", "conversation-1", domain.ScopeDirect)
	task.Message.ReplyTarget = "target-1"
	task.Message.ThreadID = "thread-1"
	task.Deliver = deliver
	if deliver {
		task.Binding.Type = "telegram"
		task.Binding.BindingID = "strict-telegram"
		task.Message.Channel = task.Binding.Type
		task.Message.BindingID = task.Binding.BindingID
	}
	return task
}

func strictPending(task Task, text, requestID string, promptTokens, completionTokens int, cost float64) pendingResult {
	return pendingResult{
		Outbound: domain.OutboundMessage{
			TenantID:  task.Tenant.TenantID,
			BindingID: task.Binding.BindingID,
			Channel:   task.Binding.Type,
			Target:    task.Message.ReplyTarget,
			ThreadID:  task.Message.ThreadID,
			Scope:     task.Message.Scope,
			Text:      text,
		},
		RequestID:        requestID,
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
		CostUSD:          cost,
	}
}

func strictIdentity(t *testing.T, task Task) (session.Key, string, string) {
	t.Helper()
	principalID, sessionID := domain.Identity(task.Message, task.Tenant.App.Name)
	key := session.Key{AppName: strictTestAppNamespace, UserID: principalID, SessionID: sessionID}
	idempotencyKey := messageKey(task.Message)
	turnID, err := sessionturn.DeriveTurnID(key, idempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	return key, idempotencyKey, turnID
}

func newStrictTestService(
	t *testing.T,
	runner *strictTestRunner,
	turn *strictTestTurn,
	coordinator *strictCountingCoordinator,
	adapter *strictCaptureAdapter,
	auditSink auditlog.Sink,
	runTimeout time.Duration,
) (*Service, *strictTestSession, *metrics.Metrics) {
	t.Helper()
	if coordinator == nil {
		coordinator = newStrictCountingCoordinator()
	}
	baseSession := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = baseSession.Close() })
	transactional := &strictTestSession{Service: baseSession, turn: turn}
	registry := channels.NewRegistry()
	if adapter != nil {
		registry = channels.NewRegistry(adapter)
	}
	metricSet := metrics.NewMetrics()
	manager := agentruntime.NewManager()
	t.Cleanup(func() { _ = manager.Close() })
	service := NewService(
		manager,
		coordinator,
		registry,
		governance.NewFilter(),
		auditSink,
		metricSet,
		Options{LockTTL: time.Minute, DedupTTL: time.Hour, RunTimeout: runTimeout},
	)
	runtime := &agentruntime.Runtime{
		Runner:                  runner,
		AppNamespace:            strictTestAppNamespace,
		Session:                 transactional,
		TurnSession:             transactional,
		SessionDatabaseIdentity: strictTestDatabaseIdentity,
	}
	service.acquireRuntime = func(context.Context, config.TenantConfig) (*agentruntime.Runtime, func(), error) {
		return runtime, func() {}, nil
	}
	t.Cleanup(func() { _ = coordinator.Close() })
	return service, transactional, metricSet
}

func TestTurnReplayEnvelopeRoundTripAndIdentityValidation(t *testing.T) {
	task := strictTask(false)
	_, _, turnID := strictIdentity(t, task)
	want := strictPending(task, "canonical", "request-canonical", 11, 7, 0.25)
	encoded, err := encodeTurnReplay(turnID, task, want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeTurnReplay(encoded, turnID, task)
	if err != nil {
		t.Fatal(err)
	}
	if got.RequestID != want.RequestID || !reflect.DeepEqual(got.Outbound, want.Outbound) ||
		got.PromptTokens != want.PromptTokens || got.CompletionTokens != want.CompletionTokens || got.CostUSD != want.CostUSD {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}

	wrongRoute := task
	wrongRoute.Message.ReplyTarget = "another-target"
	if _, err := decodeTurnReplay(encoded, turnID, wrongRoute); err == nil || !strings.Contains(err.Error(), "outbound identity") {
		t.Fatalf("wrong outbound route error = %v", err)
	}
	if _, err := decodeTurnReplay(encoded, "another-turn", task); err == nil || !strings.Contains(err.Error(), "turn identity") {
		t.Fatalf("wrong turn error = %v", err)
	}
	wrongRevision := task
	wrongRevision.Tenant.Version = "v2"
	if _, err := decodeTurnReplay(encoded, turnID, wrongRevision); err == nil || !strings.Contains(err.Error(), "routing identity") {
		t.Fatalf("wrong config revision error = %v", err)
	}
	unknownField := append(append([]byte(nil), encoded[:len(encoded)-1]...), []byte(`,"unexpected":true}`)...)
	if _, err := decodeTurnReplay(unknownField, turnID, task); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field error = %v", err)
	}

	tooLarge := want
	tooLarge.Outbound.Text = strings.Repeat("x", maxTurnReplayBytes)
	if _, err := encodeTurnReplay(turnID, task, tooLarge); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized replay error = %v", err)
	}
}

func TestStrictTurnBeginReplaySkipsRunnerAndPersistsCanonicalResult(t *testing.T) {
	task := strictTask(false)
	want := strictPending(task, "canonical replay", "request-replay", 11, 7, 0.25)
	key, idempotencyKey, turnID := strictIdentity(t, task)
	replay, err := encodeTurnReplay(turnID, task, want)
	if err != nil {
		t.Fatal(err)
	}
	turn := &strictTestTurn{replay: replay, replayed: true}
	runner := &strictTestRunner{}
	coordinator := newStrictCountingCoordinator()
	service, transactional, _ := newStrictTestService(t, runner, turn, coordinator, nil, nil, time.Second)

	result, err := service.Process(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != want.Outbound.Text || result.RequestID != want.RequestID ||
		result.PromptTokens != want.PromptTokens || result.CompletionTokens != want.CompletionTokens ||
		!result.Duplicate || !result.CacheHit || result.Outbound == nil {
		t.Fatalf("result = %+v", result)
	}
	if calls, _ := runner.snapshot(); calls != 0 {
		t.Fatalf("Runner.Run calls = %d, want 0", calls)
	}
	if commits, aborts := turn.counts(); commits != 0 || aborts != 1 {
		t.Fatalf("turn commit/abort = %d/%d, want 0/1", commits, aborts)
	}
	if coordinator.saveCount() != 1 {
		t.Fatalf("SaveResult calls = %d, want 1", coordinator.saveCount())
	}
	beginCalls, gotKey, gotIdempotency := transactional.snapshot()
	if beginCalls != 1 || gotKey != key || gotIdempotency != idempotencyKey {
		t.Fatalf("BeginTurn = calls:%d key:%+v idempotency:%q", beginCalls, gotKey, gotIdempotency)
	}
}

func TestStrictTurnBeginErrorPreventsRunnerAndPersistence(t *testing.T) {
	task := strictTask(false)
	beginErr := errors.New("begin turn failed")
	turn := &strictTestTurn{}
	runner := &strictTestRunner{}
	coordinator := newStrictCountingCoordinator()
	service, transactional, _ := newStrictTestService(t, runner, turn, coordinator, nil, nil, time.Second)
	transactional.beginErr = beginErr

	result, err := service.Process(context.Background(), task)
	if !errors.Is(err, beginErr) {
		t.Fatalf("Process error = %v, want %v", err, beginErr)
	}
	if result.Outbound != nil || coordinator.saveCount() != 0 {
		t.Fatalf("BeginTurn error escaped strict boundary: result=%+v saves=%d", result, coordinator.saveCount())
	}
	if calls, _ := runner.snapshot(); calls != 0 {
		t.Fatalf("Runner.Run calls = %d, want 0", calls)
	}
	if commits, aborts := turn.counts(); commits != 0 || aborts != 0 {
		t.Fatalf("unbegun turn commit/abort = %d/%d, want 0/0", commits, aborts)
	}
}

func TestStrictTurnCommitFailureDoesNotPersistAuditOrDeliver(t *testing.T) {
	task := strictTask(true)
	commitErr := errors.New("commit failed")
	turn := &strictTestTurn{commitFn: func(context.Context, []byte) ([]byte, bool, error) {
		return nil, false, commitErr
	}}
	runner := &strictTestRunner{}
	coordinator := newStrictCountingCoordinator()
	adapter := &strictCaptureAdapter{}
	auditSink := &strictAuditSink{}
	service, _, _ := newStrictTestService(t, runner, turn, coordinator, adapter, auditSink, time.Second)

	result, err := service.Process(context.Background(), task)
	if !errors.Is(err, commitErr) {
		t.Fatalf("Process error = %v, want %v", err, commitErr)
	}
	if result.Outbound != nil {
		t.Fatalf("failed commit exposed outbound: %+v", result.Outbound)
	}
	if coordinator.saveCount() != 0 {
		t.Fatalf("SaveResult calls = %d, want 0", coordinator.saveCount())
	}
	if auditSink.count() != 0 {
		t.Fatalf("audit writes = %d, want 0", auditSink.count())
	}
	if delivers, _ := adapter.snapshot(); delivers != 0 {
		t.Fatalf("deliveries = %d, want 0", delivers)
	}
	if calls, sawTurnContext := runner.snapshot(); calls != 1 || !sawTurnContext {
		t.Fatalf("Runner.Run calls/context = %d/%v, want 1/true", calls, sawTurnContext)
	}
	if commits, aborts := turn.counts(); commits != 1 || aborts != 1 {
		t.Fatalf("turn commit/abort = %d/%d, want 1/1", commits, aborts)
	}
	lease, claimErr := coordinator.Claim(context.Background(), messageKey(task.Message), time.Minute)
	if claimErr != nil || lease.State != coordination.Claimed {
		t.Fatalf("failed attempt did not release claim: lease=%+v err=%v", lease, claimErr)
	}
}

func TestStrictTurnCanonicalCommitReplayWinsWithoutDuplicateAccounting(t *testing.T) {
	task := strictTask(true)
	want := strictPending(task, "canonical winner", "request-winner", 11, 7, 0.25)
	_, _, turnID := strictIdentity(t, task)
	canonical, err := encodeTurnReplay(turnID, task, want)
	if err != nil {
		t.Fatal(err)
	}
	turn := &strictTestTurn{commitFn: func(_ context.Context, replay []byte) ([]byte, bool, error) {
		local, decodeErr := decodeTurnReplay(replay, turnID, task)
		if decodeErr != nil {
			return nil, false, decodeErr
		}
		if local.Outbound.Text != "local result" || local.PromptTokens != 3 || local.CompletionTokens != 4 {
			return nil, false, errors.New("worker committed unexpected local replay")
		}
		return canonical, true, nil
	}}
	runner := &strictTestRunner{}
	coordinator := newStrictCountingCoordinator()
	adapter := &strictCaptureAdapter{}
	auditSink := &strictAuditSink{}
	service, _, metricSet := newStrictTestService(t, runner, turn, coordinator, adapter, auditSink, time.Second)

	result, err := service.Process(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != want.Outbound.Text || result.RequestID != want.RequestID ||
		result.PromptTokens != want.PromptTokens || result.CompletionTokens != want.CompletionTokens ||
		result.CostUSD != want.CostUSD || !result.Duplicate || !result.CacheHit {
		t.Fatalf("canonical result was not authoritative: %+v", result)
	}
	if delivers, outbound := adapter.snapshot(); delivers != 1 || outbound.Text != want.Outbound.Text {
		t.Fatalf("delivery = %d %+v, want canonical", delivers, outbound)
	}
	if auditSink.count() != 0 {
		t.Fatalf("canonical replay duplicated allow audit: %d", auditSink.count())
	}
	recorder := httptest.NewRecorder()
	metricSet.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if strings.Contains(recorder.Body.String(), "model_tokens_total") || strings.Contains(recorder.Body.String(), "tenant_cost_usd_total") {
		t.Fatalf("canonical replay duplicated model accounting:\n%s", recorder.Body.String())
	}
}

func TestStrictAtomicTurnPersistsCanonicalDeliveryPlan(t *testing.T) {
	task := strictTask(true)
	task.Deliver = false
	task.Pipeline = DurablePipelineMetadata{
		SchemaVersion: DurablePipelineVersion, AtomicCommitMode: AtomicCommitRequired,
		DatabaseIdentity: strictTestDatabaseIdentity,
	}
	participant := &strictTestTurnParticipant{}
	task.TurnCommitParticipant = participant
	want := strictPending(task, "canonical winner", "request-winner", 11, 7, 0.25)
	want.DeliveryPlanVersion = deliveryPlanVersion
	for _, text := range []string{"canonical part 1", "canonical part 2"} {
		part := want.Outbound
		part.Text = text
		want.DeliveryParts = append(want.DeliveryParts, part)
	}
	_, _, turnID := strictIdentity(t, task)
	canonical, err := encodeTurnReplay(turnID, task, want)
	if err != nil {
		t.Fatal(err)
	}
	turn := &strictTestTurn{commitFn: func(_ context.Context, proposed []byte) ([]byte, bool, error) {
		local, decodeErr := decodeTurnReplay(proposed, turnID, task)
		if decodeErr != nil {
			return nil, false, decodeErr
		}
		if local.Outbound.Text != "local result" || local.DeliveryPlanVersion != deliveryPlanVersion || len(local.DeliveryParts) != 1 {
			return nil, false, fmt.Errorf("unexpected local replay: %+v", local)
		}
		return canonical, true, nil
	}}
	coordinator := newStrictCountingCoordinator()
	adapter := &strictCaptureAdapter{}
	service, _, _ := newStrictTestService(t, &strictTestRunner{}, turn, coordinator, adapter, nil, time.Second)

	result, err := service.Process(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != want.Outbound.Text || !result.Duplicate || !result.CacheHit {
		t.Fatalf("result = %+v, want canonical replay", result)
	}
	plan, completes, marked := participant.snapshot()
	if completes != 1 || marked != 1 || plan.Version != deliveryPlanVersion ||
		!reflect.DeepEqual(plan.Outbound, want.Outbound) || !reflect.DeepEqual(plan.Parts, want.DeliveryParts) {
		t.Fatalf("participant plan=%+v complete=%d marked=%d", plan, completes, marked)
	}
	if delivers, _ := adapter.snapshot(); delivers != 0 {
		t.Fatalf("durable atomic path sent synchronously: %d", delivers)
	}
}

func TestStrictAtomicParticipantFailurePreventsPostCommitEffects(t *testing.T) {
	task := strictTask(true)
	task.Deliver = false
	task.Pipeline = DurablePipelineMetadata{
		SchemaVersion: DurablePipelineVersion, AtomicCommitMode: AtomicCommitRequired,
		DatabaseIdentity: strictTestDatabaseIdentity,
	}
	participantErr := errors.New("atomic Inbox completion failed")
	participant := &strictTestTurnParticipant{err: participantErr}
	task.TurnCommitParticipant = participant
	turn := &strictTestTurn{}
	coordinator := newStrictCountingCoordinator()
	adapter := &strictCaptureAdapter{}
	auditSink := &strictAuditSink{}
	service, _, _ := newStrictTestService(t, &strictTestRunner{}, turn, coordinator, adapter, auditSink, time.Second)

	result, err := service.Process(context.Background(), task)
	if !errors.Is(err, participantErr) {
		t.Fatalf("Process error = %v, want %v", err, participantErr)
	}
	if result.Outbound != nil || coordinator.saveCount() != 0 || auditSink.count() != 0 {
		t.Fatalf("participant failure escaped boundary: result=%+v saves=%d audits=%d",
			result, coordinator.saveCount(), auditSink.count())
	}
	_, completes, marked := participant.snapshot()
	if completes != 1 || marked != 0 {
		t.Fatalf("participant complete/marked = %d/%d, want 1/0", completes, marked)
	}
	if commits, aborts := turn.counts(); commits != 1 || aborts != 1 {
		t.Fatalf("turn commit/abort = %d/%d, want 1/1", commits, aborts)
	}
}

func TestStrictTurnCommitPrecedesResultAuditAndDelivery(t *testing.T) {
	task := strictTask(true)
	order := &strictOrder{}
	turn := &strictTestTurn{commitFn: func(_ context.Context, replay []byte) ([]byte, bool, error) {
		order.add("commit")
		return replay, false, nil
	}}
	runner := &strictTestRunner{run: func(context.Context) (<-chan *event.Event, error) {
		order.add("run")
		return strictCompletionEvents("local result", 3, 4), nil
	}}
	coordinator := newStrictCountingCoordinator()
	coordinator.onSave = func() { order.add("save") }
	adapter := &strictCaptureAdapter{onSend: func() { order.add("deliver") }}
	auditSink := &strictAuditSink{onWrite: func() { order.add("audit") }}
	service, _, _ := newStrictTestService(t, runner, turn, coordinator, adapter, auditSink, time.Second)

	if _, err := service.Process(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	got := order.snapshot()
	position := func(want string) int {
		for index, item := range got {
			if item == want {
				return index
			}
		}
		return -1
	}
	runAt, commitAt := position("run"), position("commit")
	if runAt < 0 || commitAt <= runAt {
		t.Fatalf("run/commit order = %v", got)
	}
	for _, sideEffect := range []string{"save", "audit", "deliver"} {
		if at := position(sideEffect); at <= commitAt {
			t.Fatalf("%s occurred before canonical commit: %v", sideEffect, got)
		}
	}
}

func TestStrictTurnRunTimeoutDoesNotCommitDetachedCompletion(t *testing.T) {
	task := strictTask(false)
	turn := &strictTestTurn{}
	runner := &strictTestRunner{run: func(ctx context.Context) (<-chan *event.Event, error) {
		events := make(chan *event.Event, 1)
		go func() {
			<-ctx.Done()
			events <- &event.Event{InvocationID: "root", Response: &model.Response{
				Object:  model.ObjectTypeRunnerCompletion,
				Done:    true,
				Choices: []model.Choice{{Message: model.NewAssistantMessage("late completion")}},
			}}
			close(events)
		}()
		return events, nil
	}}
	coordinator := newStrictCountingCoordinator()
	auditSink := &strictAuditSink{}
	service, _, _ := newStrictTestService(t, runner, turn, coordinator, nil, auditSink, 20*time.Millisecond)

	result, err := service.Process(context.Background(), task)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Process error = %v, want deadline exceeded", err)
	}
	if result.Outbound != nil || coordinator.saveCount() != 0 || auditSink.count() != 0 {
		t.Fatalf("late completion escaped strict boundary: result=%+v saves=%d audits=%d", result, coordinator.saveCount(), auditSink.count())
	}
	if commits, aborts := turn.counts(); commits != 0 || aborts != 1 {
		t.Fatalf("turn commit/abort = %d/%d, want 0/1", commits, aborts)
	}
}

func TestStrictTurnBufferErrorAfterDrainPreventsCommit(t *testing.T) {
	task := strictTask(false)
	stageErr := errors.New("append event failed")
	turn := &strictTestTurn{stageErr: stageErr}
	runner := &strictTestRunner{}
	coordinator := newStrictCountingCoordinator()
	service, _, _ := newStrictTestService(t, runner, turn, coordinator, nil, nil, time.Second)

	result, err := service.Process(context.Background(), task)
	if !errors.Is(err, stageErr) {
		t.Fatalf("Process error = %v, want %v", err, stageErr)
	}
	if result.Outbound != nil || coordinator.saveCount() != 0 {
		t.Fatalf("staging error escaped strict boundary: result=%+v saves=%d", result, coordinator.saveCount())
	}
	if commits, aborts := turn.counts(); commits != 0 || aborts != 1 {
		t.Fatalf("turn commit/abort = %d/%d, want 0/1", commits, aborts)
	}
}

func TestAlreadyCompletedMissingResultUsesStrictReplayFallback(t *testing.T) {
	task := strictTask(false)
	want := strictPending(task, "durable fallback", "request-fallback", 11, 7, 0.25)
	_, _, turnID := strictIdentity(t, task)
	replay, err := encodeTurnReplay(turnID, task, want)
	if err != nil {
		t.Fatal(err)
	}
	turn := &strictTestTurn{replay: replay, replayed: true}
	runner := &strictTestRunner{}
	coordinator := newStrictCountingCoordinator()
	claim, err := coordinator.Claim(context.Background(), messageKey(task.Message), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Complete(context.Background(), messageKey(task.Message), claim.Token, time.Hour); err != nil {
		t.Fatal(err)
	}
	service, transactional, _ := newStrictTestService(t, runner, turn, coordinator, nil, nil, time.Second)

	result, err := service.Process(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != want.Outbound.Text || result.RequestID != want.RequestID || result.Outbound == nil ||
		!result.Duplicate || !result.CacheHit {
		t.Fatalf("fallback result = %+v", result)
	}
	if calls, _ := runner.snapshot(); calls != 0 {
		t.Fatalf("Runner.Run calls = %d, want 0", calls)
	}
	if beginCalls, _, _ := transactional.snapshot(); beginCalls != 1 {
		t.Fatalf("BeginTurn calls = %d, want 1", beginCalls)
	}
}

func TestRequiredAtomicTaskNeverUsesCoordinatorOnlyCache(t *testing.T) {
	for _, claimState := range []string{"completed", "released"} {
		t.Run(claimState, func(t *testing.T) {
			task := strictTask(false)
			task.Pipeline = DurablePipelineMetadata{
				SchemaVersion: DurablePipelineVersion, AtomicCommitMode: AtomicCommitRequired,
				DatabaseIdentity: strictTestDatabaseIdentity,
			}
			participant := &strictTestTurnParticipant{}
			task.TurnCommitParticipant = participant
			canonical := strictPending(task, "canonical session reply", "request-canonical", 11, 7, 0.25)
			canonical.DeliveryPlanVersion = deliveryPlanVersion
			canonical.DeliveryParts = []domain.OutboundMessage{canonical.Outbound}
			_, _, turnID := strictIdentity(t, task)
			replay, err := encodeTurnReplay(turnID, task, canonical)
			if err != nil {
				t.Fatal(err)
			}
			turn := &strictTestTurn{replay: replay, replayed: true}
			runner := &strictTestRunner{}
			coordinator := newStrictCountingCoordinator()
			claim, err := coordinator.Claim(context.Background(), messageKey(task.Message), time.Hour)
			if err != nil {
				t.Fatal(err)
			}
			stale := strictPending(task, "stale coordinator reply", "request-stale", 1, 1, 0)
			if err := coordinator.SaveResult(context.Background(), messageKey(task.Message), claim.Token, stale, time.Hour); err != nil {
				t.Fatal(err)
			}
			if claimState == "completed" {
				if err := coordinator.Complete(context.Background(), messageKey(task.Message), claim.Token, time.Hour); err != nil {
					t.Fatal(err)
				}
			} else if err := coordinator.ReleaseClaim(context.Background(), messageKey(task.Message), claim.Token); err != nil {
				t.Fatal(err)
			}
			service, transactional, _ := newStrictTestService(t, runner, turn, coordinator, nil, nil, time.Second)

			result, err := service.Process(context.Background(), task)
			if err != nil {
				t.Fatal(err)
			}
			if result.Text != canonical.Outbound.Text || result.RequestID != canonical.RequestID {
				t.Fatalf("coordinator cache bypassed Session replay: %+v", result)
			}
			if calls, _ := runner.snapshot(); calls != 0 {
				t.Fatalf("Runner.Run calls = %d, want 0", calls)
			}
			if beginCalls, _, _ := transactional.snapshot(); beginCalls != 1 {
				t.Fatalf("BeginTurn calls = %d, want 1", beginCalls)
			}
			plan, completes, marked := participant.snapshot()
			if completes != 1 || marked != 1 || plan.Version != deliveryPlanVersion || len(plan.Parts) != 1 {
				t.Fatalf("participant plan=%+v complete=%d marked=%d", plan, completes, marked)
			}
		})
	}
}

func TestNonSQLCompletedMissingResultKeepsLegacyPath(t *testing.T) {
	task := strictTask(false)
	task.Tenant.Data.Session.Type = "inmemory"
	coordinator := newStrictCountingCoordinator()
	claim, err := coordinator.Claim(context.Background(), messageKey(task.Message), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Complete(context.Background(), messageKey(task.Message), claim.Token, time.Hour); err != nil {
		t.Fatal(err)
	}
	manager := agentruntime.NewManager()
	t.Cleanup(func() { _ = manager.Close() })
	t.Cleanup(func() { _ = coordinator.Close() })
	service := NewService(manager, coordinator, nil, nil, nil, nil, Options{})
	service.acquireRuntime = func(context.Context, config.TenantConfig) (*agentruntime.Runtime, func(), error) {
		return nil, nil, errors.New("legacy path unexpectedly acquired a runtime")
	}

	result, err := service.Process(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Duplicate || !result.CacheHit || result.Outbound != nil {
		t.Fatalf("legacy completed result = %+v", result)
	}
}

func TestPostgresStrictTurnRecoversAfterCoordinatorResultLoss(t *testing.T) {
	if os.Getenv("TEST_POSTGRES_DSN") == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	tenantConfig := testTenant("tenant-worker-pg-" + uuid.NewString())
	tenantConfig.Data.Session = config.BackendConfig{Type: "sql", DSNEnv: "TEST_POSTGRES_DSN"}
	tenantConfig.Data.Summary = config.BackendConfig{Type: "disabled"}
	tenantConfig.Privacy = config.PrivacyPolicy{Input: "redact", Output: "redact"}
	task := taskFor(tenantConfig, "pg-message-1", "user-1", "conversation-1", domain.ScopeDirect)
	task.Message.ReplyTarget = "target-1"
	task.Message.Text = "contact alice@example.com"

	manager := agentruntime.NewManager()
	coordinator := coordination.NewInMemory()
	service := NewService(
		manager,
		coordinator,
		channels.NewRegistry(),
		governance.NewFilter(),
		nil,
		metrics.NewMetrics(),
		Options{LockTTL: time.Minute, DedupTTL: time.Hour, RunTimeout: 5 * time.Second},
	)
	t.Cleanup(func() {
		principalID, sessionID := domain.Identity(task.Message, tenantConfig.App.Name)
		runtime, release, err := manager.Acquire(context.Background(), tenantConfig)
		if err == nil {
			_ = runtime.Session.DeleteSession(context.Background(), session.Key{
				AppName:   runtime.AppNamespace,
				UserID:    principalID,
				SessionID: sessionID,
			})
			release()
		}
		_ = manager.Close()
		_ = coordinator.Close()
	})

	first, err := service.Process(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(first.Text, "alice@example.com") || !strings.Contains(first.Text, "[REDACTED]") {
		t.Fatal("SQL canonical reply bypassed privacy")
	}
	if first.Outbound == nil || first.Text == "" {
		t.Fatalf("fresh SQL result = %+v", first)
	}
	if err := coordinator.DeleteResult(context.Background(), messageKey(task.Message)); err != nil {
		t.Fatal(err)
	}

	replayed, err := service.Process(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.Duplicate || !replayed.CacheHit || replayed.Outbound == nil ||
		replayed.Text != first.Text || replayed.RequestID != first.RequestID {
		t.Fatalf("PostgreSQL replay diverged: first=%+v replayed=%+v", first, replayed)
	}
}
