package outbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type observedTransition struct {
	op   string
	id   string
	code string
	next time.Time
	err  error
}

type observingRepository struct {
	inner storage.OutboxRepository

	mu              sync.Mutex
	transitions     []observedTransition
	transitionCalls chan observedTransition
	mutationCalls   chan string
	claimCalls      chan struct{}
	claimEntered    chan struct{}
	claimRelease    chan struct{}
	completeErr     error
	retryErr        error
	dlqErr          error
}

func (r *observingRepository) Enqueue(ctx context.Context, tc tenant.TenantContext, message storage.OutboxMessage) error {
	return r.inner.Enqueue(ctx, tc, message)
}

func (r *observingRepository) ClaimBatch(ctx context.Context, tc tenant.TenantContext, owner string, limit int) ([]storage.OutboxMessage, error) {
	if r.claimCalls != nil {
		select {
		case r.claimCalls <- struct{}{}:
		default:
		}
	}
	if r.claimEntered != nil {
		r.claimEntered <- struct{}{}
		<-r.claimRelease
	}
	return r.inner.ClaimBatch(ctx, tc, owner, limit)
}

func (r *observingRepository) MarkCompleted(ctx context.Context, tc tenant.TenantContext, owner, id string) error {
	if r.mutationCalls != nil {
		r.mutationCalls <- "complete"
	}
	if r.completeErr != nil {
		r.record(observedTransition{op: "complete", id: id, err: r.completeErr})
		return r.completeErr
	}
	err := r.inner.MarkCompleted(ctx, tc, owner, id)
	r.record(observedTransition{op: "complete", id: id, err: err})
	return err
}

func (r *observingRepository) MarkRetry(ctx context.Context, tc tenant.TenantContext, owner, id string, next time.Time, code string) error {
	if r.mutationCalls != nil {
		r.mutationCalls <- "retry"
	}
	if r.retryErr != nil {
		r.record(observedTransition{op: "retry", id: id, code: code, next: next, err: r.retryErr})
		return r.retryErr
	}
	err := r.inner.MarkRetry(ctx, tc, owner, id, next, code)
	r.record(observedTransition{op: "retry", id: id, code: code, next: next, err: err})
	return err
}

func (r *observingRepository) MoveToDLQ(ctx context.Context, tc tenant.TenantContext, owner, id, reason string) error {
	if r.mutationCalls != nil {
		r.mutationCalls <- "dead-letter"
	}
	if r.dlqErr != nil {
		r.record(observedTransition{op: "dead-letter", id: id, code: reason, err: r.dlqErr})
		return r.dlqErr
	}
	err := r.inner.MoveToDLQ(ctx, tc, owner, id, reason)
	r.record(observedTransition{op: "dead-letter", id: id, code: reason, err: err})
	return err
}

func (r *observingRepository) record(transition observedTransition) {
	r.mu.Lock()
	r.transitions = append(r.transitions, transition)
	calls := r.transitionCalls
	r.mu.Unlock()
	if calls != nil {
		calls <- transition
	}
}

func (r *observingRepository) snapshot() []observedTransition {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]observedTransition(nil), r.transitions...)
}

type scriptedSender struct {
	fn       func(context.Context, storage.OutboxMessage) SenderOutcome
	calls    chan string
	finished chan struct{}
}

func (s *scriptedSender) Send(ctx context.Context, message storage.OutboxMessage) SenderOutcome {
	if s.calls != nil {
		s.calls <- message.ID
	}
	outcome := SenderOutcome{Class: Delivered}
	if s.fn != nil {
		outcome = s.fn(ctx, message)
	}
	if s.finished != nil {
		select {
		case s.finished <- struct{}{}:
		default:
		}
	}
	return outcome
}

func testTenant(id string) tenant.TenantContext {
	return tenant.TenantContext{
		TenantID: id, AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web",
		RequestID: "request-a", MessageID: "message-a", TraceID: "trace-a", ConfigVersion: 1,
		BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "memory"},
	}
}

func testConfig(tc tenant.TenantContext, owner string) Config {
	return Config{
		OwnerID: owner, Tenants: []tenant.TenantContext{tc}, ClaimBatchSize: 2, Concurrency: 1,
		ClaimInterval: time.Millisecond, ShutdownTimeout: time.Second, LockGuard: time.Millisecond,
		RetryPolicy: RetryPolicy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: time.Minute},
	}
}

func enqueueTestMessage(t *testing.T, repository storage.OutboxRepository, tc tenant.TenantContext, id string) {
	t.Helper()
	if err := repository.Enqueue(context.Background(), tc, storage.OutboxMessage{
		TenantID: tc.TenantID, ID: id, Kind: "reply", AggregateID: "aggregate-" + id,
		Payload: []byte(`{"message":"hello"}`),
	}); err != nil {
		t.Fatal(err)
	}
}

func waitForTransition(t *testing.T, repository *observingRepository, operation string) observedTransition {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	if repository.transitionCalls == nil {
		t.Fatal("transition barrier is not configured")
	}
	for {
		select {
		case transition := <-repository.transitionCalls:
			if transition.op == operation && transition.err == nil {
				return transition
			}
		case <-deadline.C:
			t.Fatalf("did not observe successful %s transition; got %+v", operation, repository.snapshot())
		}
	}
}

func stopCleanly(t *testing.T, dispatcher *Dispatcher) {
	t.Helper()
	if err := dispatcher.Stop(context.Background()); err != nil {
		t.Fatalf("dispatcher stop: %v", err)
	}
}

func TestDispatcherRoutesSenderOutcomesToDurableMutations(t *testing.T) {
	tests := []struct {
		name      string
		outcome   SenderOutcome
		operation string
		wantCode  string
		wantStats func(Stats) uint64
	}{
		{name: "delivered", outcome: SenderOutcome{Class: Delivered}, operation: "complete", wantStats: func(s Stats) uint64 { return s.Completed }},
		{name: "retryable", outcome: SenderOutcome{Class: RetryableFailure, Code: SenderRateLimitedCode}, operation: "retry", wantCode: SenderRateLimitedCode, wantStats: func(s Stats) uint64 { return s.Retried }},
		{name: "permanent", outcome: SenderOutcome{Class: PermanentFailure, Code: SenderInvalidPayloadCode}, operation: "dead-letter", wantCode: SenderInvalidPayloadCode, wantStats: func(s Stats) uint64 { return s.DeadLettered }},
		{name: "unknown", outcome: SenderOutcome{Class: Unknown, Code: "provider body with secret"}, operation: "retry", wantCode: DeliveryOutcomeUnknownCode, wantStats: func(s Stats) uint64 { return s.Retried }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tc := testTenant("tenant-" + test.name)
			repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 4)}
			enqueueTestMessage(t, repository, tc, "outbox-"+test.name)
			sender := &scriptedSender{fn: func(context.Context, storage.OutboxMessage) SenderOutcome { return test.outcome }}
			dispatcher, err := NewDispatcher(repository, sender, testConfig(tc, "dispatcher-"+test.name))
			if err != nil {
				t.Fatal(err)
			}
			runDone := make(chan error, 1)
			go func() { runDone <- dispatcher.Run(context.Background()) }()
			transition := waitForTransition(t, repository, test.operation)
			if transition.code != test.wantCode {
				t.Fatalf("transition=%+v, want code %q", transition, test.wantCode)
			}
			stopCleanly(t, dispatcher)
			if err := <-runDone; !errors.Is(err, context.Canceled) {
				t.Fatalf("Run error=%v, want context cancellation after Stop", err)
			}
			if got := test.wantStats(dispatcher.Stats()); got != 1 {
				t.Fatalf("stats=%+v, want one %s", dispatcher.Stats(), test.operation)
			}
			for _, item := range repository.snapshot() {
				if item.err != nil {
					t.Fatalf("durable transition failed: %+v", item)
				}
			}
		})
	}
}

func TestDispatcherUnknownOutcomeIsRetryableAndNeverSuccessOrImmediateDeadLetter(t *testing.T) {
	tc := testTenant("tenant-unknown")
	repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 2)}
	enqueueTestMessage(t, repository, tc, "outbox-unknown-policy")
	sender := &scriptedSender{fn: func(context.Context, storage.OutboxMessage) SenderOutcome {
		return SenderOutcome{Class: Unknown, Code: "authorization: bearer secret"}
	}}
	config := testConfig(tc, "dispatcher-unknown-policy")
	config.RetryPolicy = RetryPolicy{MaxAttempts: 2, BaseDelay: time.Hour, MaxDelay: time.Hour}
	dispatcher, err := NewDispatcher(repository, sender, config)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(context.Background()) }()
	transition := waitForTransition(t, repository, "retry")
	if transition.code != DeliveryOutcomeUnknownCode || transition.next.Before(time.Now().UTC().Add(50*time.Minute)) {
		t.Fatalf("unknown policy transition=%+v", transition)
	}
	stopCleanly(t, dispatcher)
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}
	stats := dispatcher.Stats()
	if stats.Completed != 0 || stats.DeadLettered != 0 || stats.UnknownOutcomes != 1 {
		t.Fatalf("unknown outcome stats=%+v", stats)
	}
}

func TestDispatcherMutationFailureIsFailClosed(t *testing.T) {
	tc := testTenant("tenant-mutation-failure")
	repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 1), mutationCalls: make(chan string, 1), completeErr: errors.Join(storage.ErrBackendUnavailable, storage.ErrTransactionOutcomeUnknown)}
	enqueueTestMessage(t, repository, tc, "outbox-mutation-failure")
	dispatcher, err := NewDispatcher(repository, &scriptedSender{}, testConfig(tc, "dispatcher-mutation-failure"))
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(context.Background()) }()
	select {
	case operation := <-repository.mutationCalls:
		if operation != "complete" {
			t.Fatalf("mutation operation=%s", operation)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("durable mutation was not attempted")
	}
	stopErr := dispatcher.Stop(context.Background())
	if !errors.Is(stopErr, ErrMutationFailed) || !errors.Is(stopErr, storage.ErrBackendUnavailable) || !errors.Is(stopErr, storage.ErrTransactionOutcomeUnknown) {
		t.Fatalf("stop error=%v, want fail-closed mutation error", stopErr)
	}
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}
	stats := dispatcher.Stats()
	if stats.Completed != 0 || stats.MutationErrors != 1 || stats.LastErrorCategory != "repository unavailable or ambiguous" {
		t.Fatalf("fail-closed stats=%+v", stats)
	}
}

func TestDispatcherBatchFailureDoesNotPolluteOtherMessages(t *testing.T) {
	tc := testTenant("tenant-batch")
	repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 4)}
	enqueueTestMessage(t, repository, tc, "outbox-batch-success")
	enqueueTestMessage(t, repository, tc, "outbox-batch-dead")
	sender := &scriptedSender{fn: func(_ context.Context, message storage.OutboxMessage) SenderOutcome {
		if message.ID == "outbox-batch-dead" {
			return SenderOutcome{Class: PermanentFailure, Code: SenderRejectedCode}
		}
		return SenderOutcome{Class: Delivered}
	}}
	dispatcher, err := NewDispatcher(repository, sender, testConfig(tc, "dispatcher-batch"))
	if err != nil {
		t.Fatal(err)
	}
	configRun := make(chan error, 1)
	go func() { configRun <- dispatcher.Run(context.Background()) }()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for len(repository.snapshot()) < 2 {
		select {
		case transition := <-repository.transitionCalls:
			if transition.err != nil {
				t.Fatalf("batch transition failed: %+v", transition)
			}
		case <-deadline.C:
			t.Fatalf("batch transitions=%+v", repository.snapshot())
		}
	}
	stopCleanly(t, dispatcher)
	if err := <-configRun; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}
	transitions := repository.snapshot()
	if len(transitions) != 2 || transitions[0].err != nil || transitions[1].err != nil {
		t.Fatalf("batch transitions=%+v", transitions)
	}
	seen := map[string]string{}
	for _, transition := range transitions {
		seen[transition.id] = transition.op
	}
	if seen["outbox-batch-success"] != "complete" || seen["outbox-batch-dead"] != "dead-letter" {
		t.Fatalf("batch outcome map=%v", seen)
	}
}

func TestTwoDispatchersHaveOneActiveSenderOwner(t *testing.T) {
	tc := testTenant("tenant-competing")
	claimEntered := make(chan struct{}, 2)
	claimRelease := make(chan struct{})
	repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 2), claimEntered: claimEntered, claimRelease: claimRelease}
	enqueueTestMessage(t, repository, tc, "outbox-competing")
	calls := make(chan string, 2)
	sender := &scriptedSender{calls: calls}
	first, err := NewDispatcher(repository, sender, testConfig(tc, "dispatcher-a"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewDispatcher(repository, sender, testConfig(tc, "dispatcher-b"))
	if err != nil {
		t.Fatal(err)
	}
	firstRun, secondRun := make(chan error, 1), make(chan error, 1)
	go func() { firstRun <- first.Run(context.Background()) }()
	go func() { secondRun <- second.Run(context.Background()) }()
	for started := 0; started < 2; started++ {
		select {
		case <-claimEntered:
		case <-time.After(3 * time.Second):
			t.Fatal("both dispatchers did not reach the claim barrier")
		}
	}
	close(claimRelease)
	select {
	case <-calls:
	case <-time.After(3 * time.Second):
		t.Fatal("no dispatcher reached Sender")
	}
	transition := waitForTransition(t, repository, "complete")
	if transition.id != "outbox-competing" {
		t.Fatalf("transition=%+v", transition)
	}
	if first.Stats().SendCalls+second.Stats().SendCalls != 1 {
		t.Fatalf("dispatcher send stats first=%+v second=%+v", first.Stats(), second.Stats())
	}
	stopCleanly(t, first)
	stopCleanly(t, second)
	if err := <-firstRun; !errors.Is(err, context.Canceled) {
		t.Fatalf("first Run error=%v", err)
	}
	if err := <-secondRun; !errors.Is(err, context.Canceled) {
		t.Fatalf("second Run error=%v", err)
	}
}

func TestDispatcherStopPreventsNewSenderCalls(t *testing.T) {
	tc := testTenant("tenant-stop")
	repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 2), claimCalls: make(chan struct{}, 2)}
	enqueueTestMessage(t, repository, tc, "outbox-stop-a")
	enqueueTestMessage(t, repository, tc, "outbox-stop-b")
	started := make(chan struct{}, 1)
	sender := &scriptedSender{fn: func(ctx context.Context, _ storage.OutboxMessage) SenderOutcome {
		started <- struct{}{}
		<-ctx.Done()
		return SenderOutcome{Class: Unknown}
	}}
	config := testConfig(tc, "dispatcher-stop")
	dispatcher, err := NewDispatcher(repository, sender, config)
	if err != nil {
		t.Fatal(err)
	}
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(context.Background()) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("sender did not start")
	}
	claimsBeforeStop := dispatcher.Stats().ClaimCalls
	stopCleanly(t, dispatcher)
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}
	if dispatcher.Stats().SendCalls != 1 || dispatcher.Stats().ClaimCalls != claimsBeforeStop {
		t.Fatalf("Stop allowed claim/send after shutdown: stats=%+v beforeClaims=%d", dispatcher.Stats(), claimsBeforeStop)
	}
}

func TestDispatcherNonCooperativeSenderHonorsBoundedShutdownBoundary(t *testing.T) {
	tc := testTenant("tenant-noncooperative")
	repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 2)}
	enqueueTestMessage(t, repository, tc, "outbox-noncooperative")
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	finished := make(chan struct{}, 1)
	sender := &scriptedSender{finished: finished, fn: func(context.Context, storage.OutboxMessage) SenderOutcome {
		started <- struct{}{}
		<-release
		return SenderOutcome{Class: Unknown}
	}}
	config := testConfig(tc, "dispatcher-noncooperative")
	config.ShutdownTimeout = 20 * time.Millisecond
	dispatcher, err := NewDispatcher(repository, sender, config)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = dispatcher.Run(context.Background()) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("sender did not start")
	}
	stopStartedAt := time.Now()
	stopErr := dispatcher.Stop(context.Background())
	if elapsed := time.Since(stopStartedAt); elapsed > time.Second {
		t.Fatalf("Stop exceeded bounded test budget: %s", elapsed)
	}
	if !errors.Is(stopErr, ErrShutdownTimeout) {
		t.Fatalf("stop error=%v, want bounded shutdown timeout", stopErr)
	}
	if dispatcher.Stats().SendCalls != 1 {
		t.Fatalf("unexpected sender count=%+v", dispatcher.Stats())
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("non-cooperative sender did not eventually return")
	}
	if got := repository.snapshot(); len(got) != 0 {
		t.Fatalf("sender result after shutdown deadline caused mutation: %+v", got)
	}
}

func TestDispatcherContextCancellationStopsClaims(t *testing.T) {
	tc := testTenant("tenant-cancel")
	repository := &observingRepository{inner: storage.NewFakeRepository(), transitionCalls: make(chan observedTransition, 2), claimCalls: make(chan struct{}, 1)}
	sender := &scriptedSender{}
	dispatcher, err := NewDispatcher(repository, sender, testConfig(tc, "dispatcher-cancel"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- dispatcher.Run(ctx) }()
	select {
	case <-repository.claimCalls:
	case <-time.After(3 * time.Second):
		t.Fatal("dispatcher did not attempt initial claim")
	}
	cancel()
	if err := <-runDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error=%v", err)
	}
	enqueueTestMessage(t, repository, tc, "outbox-after-cancel")
	if dispatcher.Stats().SendCalls != 0 {
		t.Fatalf("canceled dispatcher sent a message: %+v", dispatcher.Stats())
	}
}

func TestDispatcherRejectsUnboundedConfiguration(t *testing.T) {
	tc := testTenant("tenant-config")
	base := testConfig(tc, "dispatcher-config")
	tests := []Config{
		func() Config { c := base; c.ClaimBatchSize = MaxDispatcherBatchSize + 1; return c }(),
		func() Config { c := base; c.Concurrency = MaxDispatcherConcurrency + 1; return c }(),
		func() Config { c := base; c.ClaimInterval = MaxDispatcherClaimInterval + time.Nanosecond; return c }(),
		func() Config { c := base; c.ShutdownTimeout = MaxDispatcherShutdown + time.Nanosecond; return c }(),
		func() Config { c := base; c.LockGuard = MaxDispatcherLockGuard + time.Nanosecond; return c }(),
	}
	for _, config := range tests {
		if _, err := NewDispatcher(storage.NewFakeRepository(), &scriptedSender{}, config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("config=%+v error=%v", config, err)
		}
	}
}
