//
// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type scriptedRunner struct {
	events       []*event.Event
	runErr       error
	deadlineSeen time.Time
	runs         int
}

func (r *scriptedRunner) Run(
	ctx context.Context,
	_, _ string,
	_ model.Message,
	_ ...agent.RunOption,
) (<-chan *event.Event, error) {
	r.runs++
	r.deadlineSeen, _ = ctx.Deadline()
	if r.runErr != nil {
		return nil, r.runErr
	}
	ch := make(chan *event.Event, len(r.events))
	for _, value := range r.events {
		ch <- value
	}
	close(ch)
	return ch, nil
}

func (*scriptedRunner) Close() error { return nil }

type recordingBudgetController struct {
	reservation governance.TokenReservation
	checkErr    error
	reserveErr  error
	settleErr   error
	releaseErr  error
	dispatchErr error
	settled     []int64
	released    int
	dispatched  int
	slot        governance.SessionSlot
}

func (b *recordingBudgetController) CheckBudget(context.Context) error { return b.checkErr }
func (b *recordingBudgetController) AcquireSessionSlot(context.Context) (governance.SessionSlot, error) {
	return b.slot, nil
}
func (b *recordingBudgetController) ReserveTokenBudget(context.Context) (governance.TokenReservation, error) {
	return b.reservation, b.reserveErr
}

type recordingSessionSlot struct {
	done chan struct{}
	once sync.Once
	mu   sync.Mutex
	err  error
}

func newRecordingSessionSlot() *recordingSessionSlot {
	return &recordingSessionSlot{done: make(chan struct{})}
}

func (s *recordingSessionSlot) Done() <-chan struct{} { return s.done }

func (s *recordingSessionSlot) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *recordingSessionSlot) Release(context.Context) error {
	s.once.Do(func() { close(s.done) })
	return s.Err()
}

func (s *recordingSessionSlot) lose(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
	s.once.Do(func() { close(s.done) })
}

type cancellationRunner struct {
	started chan struct{}
}

func (r *cancellationRunner) Run(
	ctx context.Context,
	_, _ string,
	_ model.Message,
	_ ...agent.RunOption,
) (<-chan *event.Event, error) {
	ch := make(chan *event.Event)
	close(r.started)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

func (*cancellationRunner) Close() error { return nil }
func (b *recordingBudgetController) DispatchTokenBudget(context.Context, governance.TokenReservation) error {
	b.dispatched++
	return b.dispatchErr
}
func (b *recordingBudgetController) SettleTokenBudget(_ context.Context, _ governance.TokenReservation, actual int64) error {
	b.settled = append(b.settled, actual)
	return b.settleErr
}
func (b *recordingBudgetController) ReleaseTokenBudget(context.Context, governance.TokenReservation) error {
	b.released++
	return b.releaseErr
}

func newBudgetProcessWorker(
	t *testing.T,
	r *scriptedRunner,
	budget *recordingBudgetController,
) *Worker {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return &Worker{
		tenant:        &tenant.Tenant{ID: "budget-worker", ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"}},
		runner:        r,
		budgetTracker: budget,
		sessionLocks:  storage.NewSessionLockManager(rdb),
		appName:       "support",
		agentName:     "support",
		modelName:     "model",
	}
}

func newLiveBudgetProcessWorker(t *testing.T, r *scriptedRunner) *Worker {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(mr.Close)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	tenantConfig := &tenant.Tenant{
		ID:         "live-budget-worker",
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist"},
		Budget: tenant.BudgetConfig{
			MaxTokensPerDay:     1_000,
			MaxTokensPerRequest: 100,
		},
	}
	return &Worker{
		tenant:        tenantConfig,
		runner:        r,
		budgetTracker: governance.NewBudgetTracker(rdb, tenantConfig),
		sessionLocks:  storage.NewSessionLockManager(rdb),
		appName:       "support",
		agentName:     "support",
		modelName:     "model",
	}
}

func budgetProcessRequest() *Request {
	return &Request{UserID: "user", SessionID: "session", ChannelType: "telegram", Content: "hello"}
}

func completedModelEvents(responseID, content string, totalTokens *int) []*event.Event {
	response := &model.Response{
		ID:      responseID,
		Choices: []model.Choice{{Delta: model.Message{Content: content}}},
	}
	if totalTokens != nil {
		response.Usage = &model.Usage{TotalTokens: *totalTokens}
	}
	return []*event.Event{
		event.NewResponseEvent("invocation", "agent", response),
		runnerCompletionEvent(),
	}
}

func tokenReservationForWorker() governance.TokenReservation {
	return governance.TokenReservation{
		ID:        "reservation",
		Reserved:  100,
		ExpiresAt: time.Now().Add(time.Minute),
	}
}

func TestProcessDoesNotUseRedisAuditExpiryAsLocalDeadline(t *testing.T) {
	tokens := 30
	r := &scriptedRunner{events: completedModelEvents("provider-response", "answer", &tokens)}
	budget := &recordingBudgetController{reservation: tokenReservationForWorker()}
	w := newBudgetProcessWorker(t, r, budget)

	response, err := w.Process(context.Background(), budgetProcessRequest())
	if err != nil || response == nil || response.Content != "answer" {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if len(budget.settled) != 1 || budget.settled[0] != 30 || budget.released != 0 || budget.dispatched != 1 {
		t.Fatalf("settled=%v released=%d dispatched=%d", budget.settled, budget.released, budget.dispatched)
	}
	if !r.deadlineSeen.IsZero() {
		t.Fatalf("legacy/custom reservation unexpectedly imposed a local deadline: %v", r.deadlineSeen)
	}
}

func TestProcessUsesLocalExecutionDeadlineFromRealBudgetReservation(t *testing.T) {
	tokens := 30
	r := &scriptedRunner{events: completedModelEvents("provider-response", "answer", &tokens)}
	w := newLiveBudgetProcessWorker(t, r)
	started := time.Now()

	response, err := w.Process(context.Background(), budgetProcessRequest())
	if err != nil || response == nil || response.Content != "answer" {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if r.deadlineSeen.IsZero() {
		t.Fatal("real Redis reservation did not impose a local execution deadline")
	}
	if !r.deadlineSeen.After(started.Add(9 * time.Minute)) {
		t.Fatalf("execution deadline=%v is not a conservative reservation window", r.deadlineSeen)
	}
}

func TestProcessChargesFullReservationWhenProviderUsageIsMissing(t *testing.T) {
	r := &scriptedRunner{events: completedModelEvents("provider-response", "answer", nil)}
	budget := &recordingBudgetController{reservation: tokenReservationForWorker()}
	w := newBudgetProcessWorker(t, r, budget)

	if _, err := w.Process(context.Background(), budgetProcessRequest()); err != nil {
		t.Fatal(err)
	}
	if len(budget.settled) != 1 || budget.settled[0] != 100 {
		t.Fatalf("settled=%v, want full reservation", budget.settled)
	}
}

func TestProcessChargesReservationWhenDispatchOutcomeIsUnknown(t *testing.T) {
	r := &scriptedRunner{runErr: errors.New("runner setup failed")}
	budget := &recordingBudgetController{reservation: tokenReservationForWorker()}
	w := newBudgetProcessWorker(t, r, budget)

	if _, err := w.Process(context.Background(), budgetProcessRequest()); err == nil {
		t.Fatal("runner setup failure was accepted")
	}
	if budget.released != 0 || len(budget.settled) != 1 || budget.settled[0] != 100 {
		t.Fatalf("settled=%v released=%d", budget.settled, budget.released)
	}
}

func TestProcessReleasesReservationWhenDispatchIsRejected(t *testing.T) {
	r := &scriptedRunner{}
	budget := &recordingBudgetController{
		reservation: tokenReservationForWorker(),
		dispatchErr: governance.ErrBudgetReservationInvalid,
	}
	w := newBudgetProcessWorker(t, r, budget)

	if _, err := w.Process(context.Background(), budgetProcessRequest()); err == nil {
		t.Fatal("dispatch rejection was accepted")
	}
	if r.runs != 0 || budget.released != 1 || len(budget.settled) != 0 {
		t.Fatalf("runs=%d settled=%v released=%d", r.runs, budget.settled, budget.released)
	}
}

func TestProcessChargesFullReservationAfterStartedStreamFails(t *testing.T) {
	r := &scriptedRunner{events: []*event.Event{
		responseEvent("partial"),
		event.NewErrorEvent("invocation", "agent", "model_error", "provider detail"),
	}}
	budget := &recordingBudgetController{reservation: tokenReservationForWorker()}
	w := newBudgetProcessWorker(t, r, budget)

	if _, err := w.Process(context.Background(), budgetProcessRequest()); err == nil {
		t.Fatal("failed stream was accepted")
	}
	if len(budget.settled) != 1 || budget.settled[0] != 100 || budget.released != 0 {
		t.Fatalf("settled=%v released=%d", budget.settled, budget.released)
	}
}

func TestProcessNeverReturnsSuccessWhenBudgetSettlementFails(t *testing.T) {
	tokens := 30
	r := &scriptedRunner{events: completedModelEvents("provider-response", "answer", &tokens)}
	budget := &recordingBudgetController{
		reservation: tokenReservationForWorker(),
		settleErr:   errors.New("budget store unavailable"),
	}
	w := newBudgetProcessWorker(t, r, budget)

	response, err := w.Process(context.Background(), budgetProcessRequest())
	if err == nil || response != nil {
		t.Fatalf("response=%+v err=%v", response, err)
	}
}

func TestProcessRejectsProviderUsageAboveReservationAfterAccounting(t *testing.T) {
	tokens := 120
	r := &scriptedRunner{events: completedModelEvents("provider-response", "answer", &tokens)}
	budget := &recordingBudgetController{
		reservation: tokenReservationForWorker(),
		settleErr:   governance.ErrBudgetReservationExceeded,
	}
	w := newBudgetProcessWorker(t, r, budget)

	response, err := w.Process(context.Background(), budgetProcessRequest())
	if !errors.Is(err, governance.ErrBudgetReservationExceeded) || response != nil {
		t.Fatalf("response=%+v err=%v", response, err)
	}
	if len(budget.settled) != 1 || budget.settled[0] != 120 {
		t.Fatalf("settled=%v, want actual provider usage", budget.settled)
	}
}

func TestProcessCancelsAndRejectsResultWhenConcurrencySlotIsLost(t *testing.T) {
	slot := newRecordingSessionSlot()
	r := &cancellationRunner{started: make(chan struct{})}
	w := newBudgetProcessWorker(t, &scriptedRunner{}, &recordingBudgetController{slot: slot})
	w.runner = r

	type processResult struct {
		response *Response
		err      error
	}
	result := make(chan processResult, 1)
	go func() {
		response, err := w.Process(context.Background(), budgetProcessRequest())
		result <- processResult{response: response, err: err}
	}()

	select {
	case <-r.started:
	case <-time.After(time.Second):
		t.Fatal("runner did not start")
	}
	slot.lose(governance.ErrSessionSlotLost)

	select {
	case got := <-result:
		if got.response != nil || !errors.Is(got.err, governance.ErrSessionSlotLost) {
			t.Fatalf("response=%+v err=%v", got.response, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not cancel after concurrency slot loss")
	}
}
