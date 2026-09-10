package worker_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func TestConsumerAcknowledgesAfterExecutionCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claim := testQueueClaim(t, "request-consumer-success")
	stream := &testStream{delivery: queue.Delivery{ID: "1-0", Dispatch: queue.Dispatch{OutboxID: 1, TenantID: "tenant-a", AppID: "support", RequestID: claim.Job.RequestID()}}, cancel: cancel}
	store := &testExecutionStore{claim: claim}
	consumer, err := worker.NewConsumer(&consumerExecutor{result: worker.RunResult{RunnerCompleted: true}}, stream, store, "worker-1")
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	if store.completed != queue.CompletionSucceeded {
		t.Fatalf("completion = %q", store.completed)
	}
	if stream.acks != 1 {
		t.Fatalf("acks = %d", stream.acks)
	}
}

func TestConsumerReleasesQuotaOnlyAfterDurableTerminalEvent(t *testing.T) {
	for _, tt := range []struct {
		name    string
		persist bool
		release bool
	}{
		{name: "completion not persisted", persist: false, release: false},
		{name: "completion persisted", persist: true, release: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			claim := testQueueClaim(t, "request-consumer-quota-"+tt.name)
			stream := &testStream{delivery: queue.Delivery{
				ID:       "quota-" + tt.name,
				Dispatch: queue.Dispatch{OutboxID: 101, TenantID: "tenant-a", AppID: "support", RequestID: claim.Job.RequestID()},
			}, cancel: cancel}
			store := &testExecutionStore{claim: claim}
			consumer, err := worker.NewConsumer(&consumerExecutor{
				result: worker.RunResult{RunnerCompleted: true, TerminalEventPersisted: tt.persist},
			}, stream, store, "worker-1")
			if err != nil {
				t.Fatalf("new consumer: %v", err)
			}
			if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("run = %v", err)
			}
			if store.usageCalls != 1 || len(store.usageReleases) != 1 || store.usageReleases[0] != tt.release {
				t.Fatalf("usage calls=%d releases=%v, want one/%v", store.usageCalls, store.usageReleases, tt.release)
			}
		})
	}
}

func TestConsumerIgnoresExecutionCleanupErrorForCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claim := testQueueClaim(t, "request-consumer-cleanup")
	stream := &testStream{delivery: queue.Delivery{ID: "1-cleanup", Dispatch: queue.Dispatch{
		OutboxID: 11, TenantID: "tenant-a", AppID: "support", RequestID: claim.Job.RequestID(),
	}}, cancel: cancel}
	store := &testExecutionStore{claim: claim}
	consumer, err := worker.NewConsumer(&consumerExecutor{
		result: worker.RunResult{RunnerCompleted: true, CleanupError: errors.New("cleanup failed")},
	}, stream, store, "worker-1")
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	if store.completed != queue.CompletionSucceeded || store.retries != 0 || stream.acks != 1 {
		t.Fatalf("completion=%q retries=%d acks=%d", store.completed, store.retries, stream.acks)
	}
}

func TestConsumerRetriesAndAcknowledgesFailedRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claim := testQueueClaim(t, "request-consumer-retry")
	stream := &testStream{delivery: queue.Delivery{ID: "2-0", Dispatch: queue.Dispatch{OutboxID: 2, TenantID: "tenant-a", AppID: "support", RequestID: claim.Job.RequestID()}}, cancel: cancel}
	store := &testExecutionStore{claim: claim}
	consumer, err := worker.NewConsumer(&consumerExecutor{err: errors.New("runner failed")}, stream, store, "worker-1")
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	if store.retries != 1 || stream.acks != 1 {
		t.Fatalf("retries=%d acks=%d", store.retries, stream.acks)
	}
}

func TestConsumerReleasesLocalOwnershipWhenAckFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claim := testQueueClaim(t, "request-consumer-ack-failure")
	stream := &testStream{
		delivery: queue.Delivery{ID: "ack-failure-0", Dispatch: queue.Dispatch{
			OutboxID: 3, TenantID: "tenant-a", AppID: "support", RequestID: claim.Job.RequestID(),
		}},
		ackErr: errors.New("redis unavailable"),
		cancel: cancel,
	}
	consumer, err := worker.NewConsumer(&consumerExecutor{result: worker.RunResult{RunnerCompleted: true}}, stream, &testExecutionStore{claim: claim}, "worker-1")
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v, want context canceled", err)
	}
	if stream.releases != 1 {
		t.Fatalf("local ownership releases = %d, want 1", stream.releases)
	}
}

func TestConsumerBoundsAckRetriesWhileKeepingDeliveryPending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claim := testQueueClaim(t, "request-consumer-ack-retry-limit")
	stream := &testStream{
		delivery: queue.Delivery{ID: "ack-retry-limit-0", Dispatch: queue.Dispatch{
			OutboxID: 4, TenantID: "tenant-a", AppID: "support", RequestID: claim.Job.RequestID(),
		}},
		ackErr:         errors.New("redis unavailable"),
		ackCancelAfter: 5,
		cancel:         cancel,
	}
	consumer, err := worker.NewConsumerWithOptions(
		&consumerExecutor{result: worker.RunResult{RunnerCompleted: true}},
		stream,
		&testExecutionStore{claim: claim},
		"worker-1",
		worker.ConsumerOptions{RetryDelay: func(int) time.Duration { return time.Nanosecond }},
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v, want context canceled", err)
	}
	if stream.acks != 5 || stream.releases != 5 {
		t.Fatalf("ack attempts=%d releases=%d, want five of each", stream.acks, stream.releases)
	}
}

func TestConsumerHonorsExecutionFailureClassification(t *testing.T) {
	tests := []struct {
		name          string
		err           error
		wantCompleted queue.CompletionStatus
		wantRetries   int
	}{
		{
			name:          "permanent",
			err:           worker.NewPermanentExecutionError(errors.New("invalid config")),
			wantCompleted: queue.CompletionFailed,
		},
		{
			name:          "side effect uncertain",
			err:           worker.NewSideEffectUncertainError(errors.New("remote write may have happened")),
			wantCompleted: queue.CompletionUncertain,
			wantRetries:   0,
		},
		{
			name:        "retryable",
			err:         worker.NewRetryableExecutionError(errors.New("temporary backend failure")),
			wantRetries: 1,
		},
	}
	for index, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			claim := testQueueClaim(t, fmt.Sprintf("request-class-%d", index))
			stream := &testStream{delivery: queue.Delivery{
				ID: fmt.Sprintf("class-%d-0", index),
				Dispatch: queue.Dispatch{
					OutboxID:  int64(100 + index),
					TenantID:  "tenant-a",
					AppID:     "support",
					RequestID: claim.Job.RequestID(),
				},
			}, cancel: cancel}
			store := &testExecutionStore{claim: claim}
			consumer, err := worker.NewConsumer(&consumerExecutor{err: tt.err}, stream, store, "worker-1")
			if err != nil {
				t.Fatalf("new consumer: %v", err)
			}
			if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("run = %v", err)
			}
			if store.completed != tt.wantCompleted || store.retries != tt.wantRetries {
				t.Fatalf("completion=%q retries=%d, want %q/%d", store.completed, store.retries, tt.wantCompleted, tt.wantRetries)
			}
		})
	}
}

func TestConsumerFailsStartedRunnerWithoutAutomaticRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claim := testQueueClaim(t, "request-consumer-started-runner")
	stream := &testStream{delivery: queue.Delivery{ID: "2-1", Dispatch: queue.Dispatch{OutboxID: 21, TenantID: "tenant-a", AppID: "support", RequestID: claim.Job.RequestID()}}, cancel: cancel}
	store := &testExecutionStore{claim: claim}
	consumer, err := worker.NewConsumer(&consumerExecutor{
		result: worker.RunResult{RunnerStarted: true},
		err:    errors.New("tool failed after execution started"),
	}, stream, store, "worker-1")
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	if store.completed != queue.CompletionFailed || store.retries != 0 {
		t.Fatalf("completion=%q retries=%d, want FAILED and 0", store.completed, store.retries)
	}
	if stream.acks != 1 {
		t.Fatalf("acks = %d", stream.acks)
	}
}

func TestConsumerHonorsRetryableClassificationAfterRunnerStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claim := testQueueClaim(t, "request-consumer-started-retryable")
	stream := &testStream{delivery: queue.Delivery{
		ID: "2-1-retryable",
		Dispatch: queue.Dispatch{
			OutboxID:  211,
			TenantID:  "tenant-a",
			AppID:     "support",
			RequestID: claim.Job.RequestID(),
		},
	}, cancel: cancel}
	store := &testExecutionStore{claim: claim}
	consumer, err := worker.NewConsumer(&consumerExecutor{
		result: worker.RunResult{RunnerStarted: true},
		err:    worker.NewRetryableExecutionError(errors.New("model transport failed before side effect")),
	}, stream, store, "worker-1")
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	if store.completed != "" || store.retries != 1 {
		t.Fatalf("completion=%q retries=%d, want no completion and one retry", store.completed, store.retries)
	}
}

func TestConsumerRetriesTemporaryClaimFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claim := testQueueClaim(t, "request-consumer-claim-retry")
	stream := &testStream{delivery: queue.Delivery{ID: "2-2", Dispatch: queue.Dispatch{OutboxID: 22, TenantID: "tenant-a", AppID: "support", RequestID: claim.Job.RequestID()}}, cancel: cancel}
	store := &testExecutionStore{claim: claim, claimFailures: 1}
	consumer, err := worker.NewConsumer(&consumerExecutor{result: worker.RunResult{RunnerCompleted: true}}, stream, store, "worker-1")
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	if store.claimCalls != 2 || store.completed != queue.CompletionSucceeded || stream.acks != 1 {
		t.Fatalf("claims=%d completion=%q acks=%d", store.claimCalls, store.completed, stream.acks)
	}
}

func TestConsumerStopClaimingDrainsActiveExecution(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claim := testQueueClaim(t, "request-consumer-drain")
	stream := &testStream{delivery: queue.Delivery{ID: "2-3", Dispatch: queue.Dispatch{OutboxID: 23, TenantID: "tenant-a", AppID: "support", RequestID: claim.Job.RequestID()}}, cancel: cancel}
	store := &testExecutionStore{claim: claim}
	executor := &blockingConsumerExecutor{started: make(chan context.Context, 1), finish: make(chan struct{})}
	consumer, err := worker.NewConsumer(executor, stream, store, "worker-1")
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	runCtx := <-executor.started
	consumer.StopClaiming()
	select {
	case <-runCtx.Done():
		t.Fatal("StopClaiming canceled an already claimed execution")
	case <-time.After(20 * time.Millisecond):
	}
	close(executor.finish)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cooperative execution did not drain during shutdown")
	}
	if store.completed != queue.CompletionSucceeded || stream.acks != 1 {
		t.Fatalf("completion=%q acks=%d", store.completed, stream.acks)
	}
}

func TestConsumerDeadLettersMalformedDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &malformedStream{cancel: cancel}
	consumer, err := worker.NewConsumer(&consumerExecutor{}, stream, &testExecutionStore{}, "worker-1")
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	if stream.dead != 1 || stream.deadID != "poison-0" {
		t.Fatalf("dead=%d id=%q, want one dead-letter for poison-0", stream.dead, stream.deadID)
	}
}

func TestConsumerExtractsTraceContextFromDispatch(t *testing.T) {
	runtime := platformtelemetry.NewNoop(context.Background(), "consumer-test")
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })
	parentCtx, parentSpan := platformtelemetry.StartSpan(context.Background(), "gateway")
	defer parentSpan.End()
	wantTraceID := platformtelemetry.TraceID(parentCtx)
	carrier := platformtelemetry.Inject(parentCtx)
	if wantTraceID == "" || carrier["traceparent"] == "" {
		t.Fatal("parent trace context was not created")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claim := testQueueClaim(t, "request-consumer-trace")
	stream := &testStream{delivery: queue.Delivery{
		ID: "3-0",
		Dispatch: queue.Dispatch{
			OutboxID:    3,
			TenantID:    "tenant-a",
			AppID:       "support",
			RequestID:   claim.Job.RequestID(),
			TraceParent: carrier["traceparent"],
			TraceState:  carrier["tracestate"],
		},
	}, cancel: cancel}
	store := &testExecutionStore{claim: claim}
	executor := &consumerExecutor{result: worker.RunResult{RunnerCompleted: true}}
	consumer, err := worker.NewConsumer(executor, stream, store, "worker-1")
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	if executor.traceID != wantTraceID {
		t.Fatalf("executor trace id = %q, want %q", executor.traceID, wantTraceID)
	}
}

func TestConsumerPropagatesFinalAttemptFromClaim(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	claim := testQueueClaim(t, "request-consumer-final-attempt")
	claim.FinalAttempt = true
	stream := &testStream{
		delivery: queue.Delivery{ID: "final-1", Dispatch: queue.Dispatch{
			OutboxID: 1, TenantID: "tenant-a", AppID: "support", RequestID: claim.Job.RequestID(),
		}},
		cancel: cancel,
	}
	executor := &consumerExecutor{result: worker.RunResult{RunnerCompleted: true}}
	consumer, err := worker.NewConsumer(executor, stream, &testExecutionStore{claim: claim}, "worker-1")
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	if err := consumer.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("run = %v", err)
	}
	if !executor.finalAttempt {
		t.Fatal("executor did not receive final-attempt claim")
	}
}

type consumerExecutor struct {
	result       worker.RunResult
	err          error
	traceID      string
	finalAttempt bool
}

type blockingConsumerExecutor struct {
	started chan context.Context
	finish  chan struct{}
}

func (e *blockingConsumerExecutor) Run(ctx context.Context, _ execution.Job) (worker.RunResult, error) {
	if _, ok := worker.JobLeaseFromContext(ctx); !ok {
		return worker.RunResult{}, errors.New("lease is missing")
	}
	e.started <- ctx
	select {
	case <-e.finish:
		return worker.RunResult{RunnerStarted: true, RunnerCompleted: true}, nil
	case <-ctx.Done():
		return worker.RunResult{RunnerStarted: true}, ctx.Err()
	}
}

func (e *consumerExecutor) Run(ctx context.Context, _ execution.Job) (worker.RunResult, error) {
	if _, ok := worker.JobLeaseFromContext(ctx); !ok {
		return worker.RunResult{}, errors.New("lease is missing")
	}
	e.traceID = platformtelemetry.TraceID(ctx)
	e.finalAttempt = worker.FinalAttemptFromContext(ctx)
	return e.result, e.err
}

type testStream struct {
	mu             sync.Mutex
	delivery       queue.Delivery
	delivered      bool
	acks           int
	ackErr         error
	releases       int
	ackCancelAfter int
	cancel         context.CancelFunc
}

type malformedStream struct {
	dead     int
	deadID   string
	received bool
	cancel   context.CancelFunc
}

func (s *malformedStream) Receive(ctx context.Context, _ string, _ time.Duration) (queue.Delivery, error) {
	if !s.received {
		s.received = true
		return queue.Delivery{ID: "poison-0"}, errors.New("malformed dispatch payload")
	}
	<-ctx.Done()
	return queue.Delivery{}, ctx.Err()
}
func (s *malformedStream) Ack(context.Context, queue.Delivery) error { return nil }
func (s *malformedStream) Release(queue.Delivery)                    {}
func (s *malformedStream) Dead(_ context.Context, delivery queue.Delivery, _ error) error {
	s.dead++
	s.deadID = delivery.ID
	s.cancel()
	return nil
}

func (s *testStream) Receive(ctx context.Context, _ string, _ time.Duration) (queue.Delivery, error) {
	s.mu.Lock()
	if !s.delivered {
		s.delivered = true
		s.mu.Unlock()
		return s.delivery, nil
	}
	s.mu.Unlock()
	<-ctx.Done()
	return queue.Delivery{}, ctx.Err()
}
func (s *testStream) Ack(_ context.Context, _ queue.Delivery) error {
	s.mu.Lock()
	s.acks++
	err := s.ackErr
	shouldCancel := s.cancel != nil && (s.ackCancelAfter == 0 || s.acks >= s.ackCancelAfter)
	s.mu.Unlock()
	if shouldCancel {
		s.cancel()
	}
	return err
}
func (s *testStream) Release(queue.Delivery) {
	s.mu.Lock()
	s.releases++
	s.mu.Unlock()
}
func (s *testStream) Dead(context.Context, queue.Delivery, error) error { return nil }

type testExecutionStore struct {
	claim         queue.Claim
	claimed       bool
	claimFailures int
	claimCalls    int
	completed     queue.CompletionStatus
	retries       int
	usageCalls    int
	usageReleases []bool
}

func (s *testExecutionStore) Claim(_ context.Context, _ queue.Dispatch, _ queue.ClaimRequest) (queue.Claim, bool, error) {
	s.claimCalls++
	if s.claimFailures > 0 {
		s.claimFailures--
		return queue.Claim{}, false, errors.New("postgres temporarily unavailable")
	}
	if s.claimed {
		return queue.Claim{}, false, nil
	}
	s.claimed = true
	return s.claim, true, nil
}
func (s *testExecutionStore) Renew(_ context.Context, c queue.Claim, _ time.Duration) (queue.Lease, error) {
	return c.Lease, nil
}
func (s *testExecutionStore) Complete(_ context.Context, _ queue.Claim, status queue.CompletionStatus) error {
	s.completed = status
	return nil
}
func (s *testExecutionStore) CompleteUncertain(_ context.Context, _ queue.Claim, _ error) error {
	s.completed = queue.CompletionUncertain
	return nil
}
func (s *testExecutionStore) WaitForApproval(_ context.Context, _ queue.Claim, _ string) error {
	s.completed = "WAITING_APPROVAL"
	return nil
}
func (s *testExecutionStore) Retry(_ context.Context, _ queue.Claim, _ error) error {
	s.retries++
	return nil
}
func (s *testExecutionStore) RecordExecutionUsage(_ context.Context, _ queue.Claim, _ worker.RunResult, release bool) error {
	s.usageCalls++
	s.usageReleases = append(s.usageReleases, release)
	return nil
}
func testQueueClaim(t *testing.T, requestID string) queue.Claim {
	t.Helper()
	return queue.Claim{Job: testJob(requestID, "tenant-a", "session-consumer"), TurnSeq: 1, Lease: queue.Lease{Owner: "worker-1", Token: "token-1", Until: time.Now().Add(time.Minute)}, Attempt: 1}
}
