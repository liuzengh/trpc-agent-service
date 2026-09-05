package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type testQueue struct {
	mu                   sync.Mutex
	delivery             queue.Delivery
	ready                chan queue.Delivery
	acked                chan struct{}
	nacked               chan queue.NackOptions
	closed               chan struct{}
	closeOnce            sync.Once
	ackErrors            []error
	nackErrors           []error
	nackAlwaysError      error
	nackStarted          chan queue.NackOptions
	nackFailed           chan struct{}
	nackBlock            <-chan struct{}
	extendErrors         []error
	receiveErrors        []error
	ackCalls             int
	nackCalls            int
	extendCalls          int
	receiveCalls         int
	ackApplied           bool
	ackApplyBeforeError  bool
	nackApplied          bool
	nackApplyBeforeError bool
	closeCalls           int
	activeActions        int
	closeWhileAction     bool
	extendStarted        chan struct{}
	extendBlock          <-chan struct{}
	extendRespectContext bool
	receiveCanceled      chan struct{}
}

func newTestQueue(delivery queue.Delivery) *testQueue {
	return &testQueue{delivery: delivery, ready: make(chan queue.Delivery, 1), acked: make(chan struct{}, 1), nacked: make(chan queue.NackOptions, 1), closed: make(chan struct{})}
}
func (q *testQueue) Enqueue(_ context.Context, job queue.AgentJob) (queue.QueueReceipt, error) {
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		return queue.QueueReceipt{}, err
	}
	q.mu.Lock()
	q.delivery.Job = job
	q.delivery.Envelope = envelope
	q.mu.Unlock()
	return queue.QueueReceipt{Accepted: true, ReceiptID: "receipt-worker", JobID: job.JobID, Attempt: job.Attempt}, nil
}
func (q *testQueue) Receive(ctx context.Context, _ time.Duration) (queue.Delivery, error) {
	q.mu.Lock()
	q.receiveCalls++
	if len(q.receiveErrors) > 0 {
		err := q.receiveErrors[0]
		q.receiveErrors = q.receiveErrors[1:]
		q.mu.Unlock()
		return queue.Delivery{}, err
	}
	q.mu.Unlock()
	select {
	case delivery := <-q.ready:
		return delivery, nil
	case <-ctx.Done():
		if q.receiveCanceled != nil {
			select {
			case q.receiveCanceled <- struct{}{}:
			default:
			}
		}
		return queue.Delivery{}, ctx.Err()
	case <-q.closed:
		return queue.Delivery{}, queue.ErrQueueClosed
	}
}
func (q *testQueue) Ack(ctx context.Context, _ queue.Delivery) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	q.ackCalls++
	var err error
	if len(q.ackErrors) > 0 {
		err = q.ackErrors[0]
		q.ackErrors = q.ackErrors[1:]
	}
	if q.ackApplied {
		q.mu.Unlock()
		return nil
	}
	if err == nil || q.ackApplyBeforeError {
		q.ackApplied = true
	}
	applied := q.ackApplied
	q.mu.Unlock()
	if err != nil && !applied {
		return err
	}
	if err != nil {
		return err
	}
	select {
	case q.acked <- struct{}{}:
	default:
	}
	return nil
}
func (q *testQueue) Nack(ctx context.Context, _ queue.Delivery, options queue.NackOptions) error {
	q.mu.Lock()
	q.nackCalls++
	started := q.nackStarted
	failed := q.nackFailed
	block := q.nackBlock
	q.mu.Unlock()
	if started != nil {
		select {
		case started <- options:
		default:
		}
	}
	if block != nil {
		<-block
	}
	if err := ctx.Err(); err != nil {
		if failed != nil {
			select {
			case failed <- struct{}{}:
			default:
			}
		}
		return err
	}
	q.mu.Lock()
	var err error
	if q.nackAlwaysError != nil {
		err = q.nackAlwaysError
	} else if len(q.nackErrors) > 0 {
		err = q.nackErrors[0]
		q.nackErrors = q.nackErrors[1:]
	}
	if q.nackApplied {
		q.mu.Unlock()
		return nil
	}
	if err == nil || q.nackApplyBeforeError {
		q.nackApplied = true
	}
	q.mu.Unlock()
	if err != nil {
		if failed != nil {
			select {
			case failed <- struct{}{}:
			default:
			}
		}
		return err
	}
	select {
	case q.nacked <- options:
	default:
	}
	return nil
}
func (q *testQueue) ExtendVisibility(ctx context.Context, _ queue.Delivery, _ time.Duration) error {
	q.mu.Lock()
	q.extendCalls++
	q.activeActions++
	started := q.extendStarted
	block := q.extendBlock
	respectContext := q.extendRespectContext
	q.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	defer func() {
		q.mu.Lock()
		q.activeActions--
		q.mu.Unlock()
	}()
	if block != nil {
		if respectContext {
			select {
			case <-block:
			case <-ctx.Done():
				return ctx.Err()
			}
		} else {
			<-block
		}
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.extendErrors) == 0 {
		return nil
	}
	err := q.extendErrors[0]
	q.extendErrors = q.extendErrors[1:]
	return err
}
func (q *testQueue) Close() error {
	q.mu.Lock()
	q.closeCalls++
	if q.activeActions > 0 {
		q.closeWhileAction = true
	}
	q.mu.Unlock()
	q.closeOnce.Do(func() { close(q.closed) })
	return nil
}

func (q *testQueue) deliver() { q.ready <- q.delivery }

type testLeaseStore struct {
	mu     sync.Mutex
	lease  storage.Lease
	number uint64
}

func (s *testLeaseStore) Acquire(ctx context.Context, tc tenant.TenantContext, sessionID, ownerID string, ttl time.Duration) (storage.Lease, error) {
	if err := ctx.Err(); err != nil {
		return storage.Lease{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease.ExpiresAt.After(time.Now()) {
		return storage.Lease{}, storage.ErrConflict
	}
	s.number++
	s.lease = storage.Lease{TenantID: tc.TenantID, SessionID: sessionID, ResourceID: sessionID, OwnerID: ownerID, FenceToken: s.number, Epoch: 1, Backend: storage.BackendPostgres, ExpiresAt: time.Now().Add(ttl)}
	return s.lease, nil
}
func (s *testLeaseStore) Renew(ctx context.Context, _ tenant.TenantContext, lease storage.Lease, ttl time.Duration) (storage.Lease, error) {
	if err := ctx.Err(); err != nil {
		return storage.Lease{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease.FenceToken != lease.FenceToken {
		return storage.Lease{}, storage.ErrLeaseLost
	}
	s.lease.ExpiresAt = time.Now().Add(ttl)
	return s.lease, nil
}
func (s *testLeaseStore) Release(context.Context, tenant.TenantContext, storage.Lease) error {
	return nil
}
func (s *testLeaseStore) Validate(ctx context.Context, _ tenant.TenantContext, lease storage.Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lease.FenceToken != lease.FenceToken {
		return storage.ErrFenceRejected
	}
	return nil
}

type testRuntime struct {
	err        error
	inputCh    chan agent.AgentInput
	waitCh     <-chan struct{}
	canceledCh chan struct{}
}

func (r testRuntime) Run(ctx context.Context, input agent.AgentInput) (agent.AgentResult, error) {
	if r.inputCh != nil {
		r.inputCh <- input
	}
	if r.err != nil {
		return agent.AgentResult{}, r.err
	}
	if r.waitCh != nil {
		select {
		case <-r.waitCh:
		case <-ctx.Done():
			if r.canceledCh != nil {
				select {
				case r.canceledCh <- struct{}{}:
				default:
				}
			}
			return agent.AgentResult{}, ctx.Err()
		}
	}
	return agent.AgentResult{Text: "ok"}, nil
}

type testAgents struct {
	err        error
	inputCh    chan agent.AgentInput
	waitCh     <-chan struct{}
	canceledCh chan struct{}
}

func (a testAgents) Build(context.Context, tenant.TenantContext, agent.AgentSpec) (agent.AgentRuntime, error) {
	return testRuntime{err: a.err, inputCh: a.inputCh, waitCh: a.waitCh, canceledCh: a.canceledCh}, nil
}

type testSink struct {
	mu      sync.Mutex
	commits int
}

func (s *testSink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commits
}

func (s *testSink) Commit(ctx context.Context, commit execution.ExecutionCommit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if commit.Job.JobID == "" || commit.Job.ExecutionID == "" || commit.TenantContext.TenantID == "" || commit.TenantContext.SessionID == "" ||
		commit.Lease.OwnerID == "" || commit.Lease.Epoch == 0 || commit.Lease.FenceToken == 0 ||
		commit.Lease.TenantID != commit.TenantContext.TenantID || commit.Lease.SessionID != commit.TenantContext.SessionID ||
		commit.Lease.ResourceID != commit.TenantContext.SessionID {
		return storage.ErrFenceRejected
	}
	s.mu.Lock()
	s.commits++
	s.mu.Unlock()
	return nil
}

func workerJob(t *testing.T) queue.AgentJob {
	t.Helper()
	now := time.Now().UTC()
	tc := tenant.TenantContext{TenantID: "tenant-worker", AgentAppID: "agent-worker", BindingID: "binding-worker", Channel: "web", ExternalUser: "worker-user", ExternalChat: "worker-chat", SessionID: "session-worker", RequestID: "request-worker", MessageID: "message-worker", TraceID: "trace-worker", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "none"}}
	return queue.AgentJob{SchemaVersion: queue.SchemaVersion, JobID: "job-worker", ExecutionID: "execution-worker", Tenant: queue.TenantContextDTOFromContext(tc), Agent: queue.AgentRefDTO{TenantID: tc.TenantID, AgentAppID: tc.AgentAppID, Version: 1}, Message: queue.MessageDTO{ID: tc.MessageID, Role: "user", Content: "hello", CreatedAt: now}, Trace: queue.TraceContextDTO{TraceID: tc.TraceID, RequestID: tc.RequestID, MessageID: tc.MessageID, ExecutionID: "execution-worker"}, CreatedAt: now, Deadline: now.Add(time.Minute), Attempt: 1}
}

func newWorker(t *testing.T, runtimeErr error, q *testQueue) *Worker {
	return newConfiguredWorker(t, runtimeErr, q, nil)
}

func newBlockingWorker(t *testing.T, q *testQueue, waitCh <-chan struct{}, canceledCh chan struct{}, configure func(*Config)) (*Worker, *testSink) {
	t.Helper()
	leases := &testLeaseStore{}
	sink := &testSink{}
	executor, err := execution.NewExecutor(leases, testAgents{waitCh: waitCh, canceledCh: canceledCh}, sink)
	if err != nil {
		t.Fatal(err)
	}
	job := q.delivery.Job
	config := Config{WorkerID: "worker-blocking", Concurrency: 1, VisibilityTimeout: 30 * time.Millisecond, LeaseTTL: time.Second, CleanupTimeout: 20 * time.Millisecond, ResolveAgent: AgentResolverFunc(func(context.Context, tenant.TenantContext, queue.AgentRefDTO) (agent.AgentSpec, error) {
		return agent.AgentSpec{TenantID: job.Tenant.TenantID, AgentAppID: job.Tenant.AgentAppID, Version: job.Agent.Version, Name: "assistant", ModelProvider: "fake"}, nil
	})}
	if configure != nil {
		configure(&config)
	}
	w, err := New(q, executor, config)
	if err != nil {
		t.Fatal(err)
	}
	return w, sink
}

func newConfiguredWorker(t *testing.T, runtimeErr error, q *testQueue, configure func(*Config)) *Worker {
	t.Helper()
	leases := &testLeaseStore{}
	sink := &testSink{}
	executor, err := execution.NewExecutor(leases, testAgents{err: runtimeErr}, sink)
	if err != nil {
		t.Fatal(err)
	}
	job := q.delivery.Job
	config := Config{WorkerID: "worker-1", Concurrency: 1, VisibilityTimeout: time.Second, LeaseTTL: time.Second, CleanupTimeout: 100 * time.Millisecond, ResolveAgent: AgentResolverFunc(func(context.Context, tenant.TenantContext, queue.AgentRefDTO) (agent.AgentSpec, error) {
		return agent.AgentSpec{TenantID: job.Tenant.TenantID, AgentAppID: job.Tenant.AgentAppID, Version: job.Agent.Version, Name: "assistant", ModelProvider: "fake"}, nil
	})}
	if configure != nil {
		configure(&config)
	}
	w, err := New(q, executor, config)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestWorkerExecutesCommitsAndAcks(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-worker", Envelope: envelope, Job: job, Attempt: 1, ReceivedAt: time.Now().UTC(), VisibleUntil: time.Now().UTC().Add(time.Second)})
	w := newWorker(t, nil, q)
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	select {
	case <-q.acked:
	case <-time.After(time.Second):
		t.Fatal("worker did not ack successful execution")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerPassesHistoryFromJobToExecution(t *testing.T) {
	job := workerJob(t)
	job.History = []queue.MessageDTO{
		{ID: "history-1", Role: "user", Content: "previous", CreatedAt: time.Now().UTC(), ToolCalls: []queue.ToolCallDTO{{ID: "tool-1", Name: "lookup", Arguments: "{}"}}},
		{ID: "history-2", Role: "assistant", Content: "answer", CreatedAt: time.Now().UTC()},
	}
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-history", Envelope: envelope, Job: job, Attempt: 1, ReceivedAt: time.Now().UTC(), VisibleUntil: time.Now().UTC().Add(time.Second)})
	inputCh := make(chan agent.AgentInput, 1)
	leases := &testLeaseStore{}
	sink := &testSink{}
	executor, err := execution.NewExecutor(leases, testAgents{inputCh: inputCh}, sink)
	if err != nil {
		t.Fatal(err)
	}
	w, err := New(q, executor, Config{WorkerID: "worker-history", Concurrency: 1, VisibilityTimeout: time.Second, LeaseTTL: time.Second, CleanupTimeout: 100 * time.Millisecond, ResolveAgent: AgentResolverFunc(func(context.Context, tenant.TenantContext, queue.AgentRefDTO) (agent.AgentSpec, error) {
		return agent.AgentSpec{TenantID: job.Tenant.TenantID, AgentAppID: job.Tenant.AgentAppID, Version: job.Agent.Version, Name: "assistant", ModelProvider: "fake"}, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	select {
	case input := <-inputCh:
		if len(input.History) != 2 || input.History[0].ID != "history-1" || input.History[0].Content != "previous" || input.History[0].ToolCalls[0].Arguments != "{}" || input.History[1].ID != "history-2" {
			t.Fatalf("history mapping mismatch: count=%d first=%s second=%s", len(input.History), input.History[0].ID, input.History[1].ID)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not execute history job")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayQueueWorkerExecutionPreservesHistory(t *testing.T) {
	job := workerJob(t)
	tc, err := job.Tenant.Restore()
	if err != nil {
		t.Fatal(err)
	}
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-e2e-history", ReceivedAt: time.Now().UTC(), VisibleUntil: time.Now().UTC().Add(time.Second)})
	inputCh := make(chan agent.AgentInput, 1)
	g, err := gateway.New(q)
	if err != nil {
		t.Fatal(err)
	}
	request := gateway.GatewayRequest{
		TenantContext: tc,
		Agent:         agent.AgentSpec{TenantID: tc.TenantID, AgentAppID: tc.AgentAppID, Version: tc.ConfigVersion, Name: "assistant", ModelProvider: "fake"},
		History:       []agent.Message{{ID: "history-e2e-1", Role: "user", Content: "previous", CreatedAt: time.Now().UTC()}, {ID: "history-e2e-2", Role: "assistant", Content: "answer", CreatedAt: time.Now().UTC()}},
		Input:         agent.Message{ID: tc.MessageID, Role: "user", Content: "next", CreatedAt: time.Now().UTC()},
		Deadline:      time.Now().UTC().Add(time.Minute),
	}
	if _, err := g.Submit(context.Background(), request); err != nil {
		t.Fatalf("gateway submit: %v", err)
	}
	leases := &testLeaseStore{}
	sink := &testSink{}
	executor, err := execution.NewExecutor(leases, testAgents{inputCh: inputCh}, sink)
	if err != nil {
		t.Fatal(err)
	}
	w, err := New(q, executor, Config{WorkerID: "worker-e2e-history", Concurrency: 1, VisibilityTimeout: time.Second, LeaseTTL: time.Second, CleanupTimeout: 100 * time.Millisecond, ResolveAgent: AgentResolverFunc(func(context.Context, tenant.TenantContext, queue.AgentRefDTO) (agent.AgentSpec, error) {
		return request.Agent, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	select {
	case input := <-inputCh:
		if len(input.History) != 2 || input.History[0].ID != "history-e2e-1" || input.History[1].ID != "history-e2e-2" {
			t.Fatalf("e2e history mismatch: schema=%d queued=%d received=%d first=%s second=%s", q.delivery.Job.SchemaVersion, len(q.delivery.Job.History), len(input.History), input.History[0].ID, input.History[1].ID)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not receive gateway history")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerNackReasonIsSafeAndStable(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	sensitive := fmt.Errorf("provider response Authorization=Bearer secret Prompt=private URL=https://example.test/?token=abc body=private")
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-sensitive", Envelope: envelope, Job: job, Attempt: 1, ReceivedAt: time.Now().UTC(), VisibleUntil: time.Now().UTC().Add(time.Second)})
	w := newWorker(t, sensitive, q)
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	select {
	case options := <-q.nacked:
		if options.Reason != "execution_retryable_failure" || strings.Contains(options.Reason, "Authorization") || strings.Contains(options.Reason, "secret") || strings.Contains(options.Reason, "private") || strings.Contains(options.Reason, "token") {
			t.Fatalf("unsafe nack reason: reason=%q sensitive=%v", options.Reason, true)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not nack sensitive failure")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerNackReasonClassifiesStructuredProviderError(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	providerErr := fmt.Errorf("%w: %w", agent.ErrProviderFailure, &agent.ProviderResponseError{Type: "provider.internal", Code: "rate_limit", Message: "Authorization=secret"})
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-provider", Envelope: envelope, Job: job, Attempt: 1, ReceivedAt: time.Now().UTC(), VisibleUntil: time.Now().UTC().Add(time.Second)})
	w := newWorker(t, providerErr, q)
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	select {
	case options := <-q.nacked:
		if options.Reason != "provider_response_error" || strings.Contains(options.Reason, "Authorization") || strings.Contains(options.Reason, "secret") {
			t.Fatalf("unsafe structured reason: reason=%q sensitive=%v", options.Reason, true)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not nack provider failure")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerNacksRetryableRuntimeFailure(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-failure", Envelope: envelope, Job: job, Attempt: 1, ReceivedAt: time.Now().UTC(), VisibleUntil: time.Now().UTC().Add(time.Second)})
	w := newWorker(t, agent.ErrProducerIncomplete, q)
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	select {
	case options := <-q.nacked:
		if !options.Requeue {
			t.Fatal("retryable failure was not requeued")
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not nack failed execution")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func waitForQueueStat(t *testing.T, q *testQueue, stat func(*testQueue) bool, what string) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		q.mu.Lock()
		matched := stat(q)
		q.mu.Unlock()
		if matched {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %s: receives=%d acks=%d nacks=%d extends=%d", what, q.receiveCalls, q.ackCalls, q.nackCalls, q.extendCalls)
		case <-ticker.C:
		}
	}
}

func TestWorkerAckFailureRetriesThenAcks(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	ackErr := errors.New("ack transport unavailable")
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-ack-retry", Envelope: envelope, Job: job, Attempt: 1})
	q.ackErrors = []error{ackErr}
	w := newConfiguredWorker(t, nil, q, func(config *Config) {
		config.ActionRetryAttempts = 2
		config.ActionRetryBackoff = time.Millisecond
	})
	record := w.track(q.delivery, context.Background())
	if err := w.ack(record); err != nil {
		t.Fatalf("ack retry returned error: %v", err)
	}
	if got := record.actionState(); got != deliveryAcked {
		t.Fatalf("state=%s, want acked", got)
	}
	q.mu.Lock()
	ackCalls := q.ackCalls
	q.mu.Unlock()
	if ackCalls != 2 || !q.ackApplied {
		t.Fatalf("ack calls=%d applied=%v, want one failed call and one successful call", ackCalls, q.ackApplied)
	}
}

func TestWorkerAckUnknownUsesVisibilityRecoveryAndStaysUnresolved(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	ackErr := errors.New("ack outcome unknown")
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-ack-unknown", Envelope: envelope, Job: job, Attempt: 1})
	q.ackErrors = []error{ackErr, ackErr, ackErr}
	w := newConfiguredWorker(t, nil, q, func(config *Config) {
		config.ActionRetryAttempts = 3
		config.VisibilityRetryAttempts = 1
		config.ActionRetryBackoff = time.Millisecond
	})
	record := w.track(q.delivery, context.Background())
	err = w.ack(record)
	if !errors.Is(err, ErrDeliveryUnresolved) || !errors.Is(err, ErrDeliveryAction) || !errors.Is(err, ackErr) {
		t.Fatalf("error chain=%v", err)
	}
	if record.actionState() != deliveryUnresolved || record.isUnresolved() == false {
		t.Fatalf("state=%s, want unresolved", record.actionState())
	}
	q.mu.Lock()
	ackCalls, extendCalls := q.ackCalls, q.extendCalls
	q.mu.Unlock()
	if ackCalls != 3 || extendCalls != 1 {
		t.Fatalf("ack calls=%d extend calls=%d, want bounded recovery", ackCalls, extendCalls)
	}
	w.mu.Lock()
	_, stillTracked := w.inFlight[q.delivery.DeliveryID]
	w.mu.Unlock()
	if !stillTracked {
		t.Fatal("unresolved delivery was untracked")
	}
}

func TestWorkerAckUnknownDoesNotNackWhenVisibilityRecoveryFails(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	ackErr := errors.New("ack unknown")
	visibilityErr := errors.New("visibility unavailable")
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-ack-unresolved", Envelope: envelope, Job: job, Attempt: 1})
	q.ackErrors = []error{ackErr}
	q.extendErrors = []error{visibilityErr}
	w := newConfiguredWorker(t, nil, q, func(config *Config) {
		config.ActionRetryAttempts = 1
		config.VisibilityRetryAttempts = 1
	})
	record := w.track(q.delivery, context.Background())
	err = w.ack(record)
	if !errors.Is(err, ErrDeliveryUnresolved) || !errors.Is(err, ackErr) || !errors.Is(err, visibilityErr) || !errors.Is(err, ErrVisibilityFailure) {
		t.Fatalf("error chain=%v", err)
	}
	q.mu.Lock()
	nackCalls := q.nackCalls
	q.mu.Unlock()
	if nackCalls != 0 {
		t.Fatalf("ack unknown triggered conflicting nack calls=%d", nackCalls)
	}
}

func TestWorkerNackFailureRetriesThenNacks(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	nackErr := errors.New("nack transport unavailable")
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-nack-retry", Envelope: envelope, Job: job, Attempt: 1})
	q.nackErrors = []error{nackErr}
	w := newConfiguredWorker(t, nil, q, func(config *Config) {
		config.ActionRetryAttempts = 2
		config.ActionRetryBackoff = time.Millisecond
	})
	record := w.track(q.delivery, context.Background())
	if err := w.nack(record, true, agent.ErrProducerIncomplete); err != nil {
		t.Fatalf("nack retry returned error: %v", err)
	}
	if record.actionState() != deliveryNacked || !q.nackApplied {
		t.Fatalf("state=%s applied=%v, want nacked", record.actionState(), q.nackApplied)
	}
	q.mu.Lock()
	nackCalls := q.nackCalls
	q.mu.Unlock()
	if nackCalls != 2 {
		t.Fatalf("nack calls=%d, want 2", nackCalls)
	}
}

func TestWorkerNackFailureIsUnresolvedAfterVisibilityRecoveryFailure(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	nackErr := errors.New("nack unknown")
	visibilityErr := errors.New("visibility recovery failed")
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-nack-unresolved", Envelope: envelope, Job: job, Attempt: 1})
	q.nackErrors = []error{nackErr}
	q.extendErrors = []error{visibilityErr}
	w := newConfiguredWorker(t, nil, q, func(config *Config) {
		config.ActionRetryAttempts = 1
		config.VisibilityRetryAttempts = 1
	})
	record := w.track(q.delivery, context.Background())
	err = w.nack(record, true, agent.ErrProducerIncomplete)
	if !errors.Is(err, ErrDeliveryUnresolved) || !errors.Is(err, nackErr) || !errors.Is(err, visibilityErr) {
		t.Fatalf("error chain=%v", err)
	}
	if record.actionState() != deliveryUnresolved {
		t.Fatalf("state=%s, want unresolved", record.actionState())
	}
}

func TestWorkerAckUnknownAfterRemoteApplyDoesNotNack(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	ackErr := errors.New("ack response lost")
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-ack-applied", Envelope: envelope, Job: job, Attempt: 1})
	q.ackErrors = []error{ackErr}
	q.ackApplyBeforeError = true
	w := newConfiguredWorker(t, nil, q, func(config *Config) {
		config.ActionRetryAttempts = 2
		config.ActionRetryBackoff = time.Millisecond
	})
	record := w.track(q.delivery, context.Background())
	if err := w.ack(record); err != nil {
		t.Fatalf("remote-applied ack was not recovered: %v", err)
	}
	q.mu.Lock()
	ackCalls, nackCalls, applied := q.ackCalls, q.nackCalls, q.ackApplied
	q.mu.Unlock()
	if ackCalls != 2 || nackCalls != 0 || !applied || record.actionState() != deliveryAcked {
		t.Fatalf("ack outcome calls=%d nacks=%d applied=%v state=%s", ackCalls, nackCalls, applied, record.actionState())
	}
}

func TestWorkerDuplicateDeliveryActionGuardPreventsConflictingActions(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-action-guard", Envelope: envelope, Job: job, Attempt: 1})
	w := newConfiguredWorker(t, nil, q, nil)
	record := w.track(q.delivery, context.Background())
	if err := w.ack(record); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	firstAckCalls := q.ackCalls
	q.mu.Unlock()
	if record.actionState() != deliveryAcked || firstAckCalls != 1 {
		t.Fatalf("first ack state=%s calls=%d", record.actionState(), firstAckCalls)
	}
	if err := w.ack(record); err != nil {
		t.Fatalf("duplicate ack should be idempotent: %v", err)
	}
	if err := w.nack(record, true, agent.ErrProducerIncomplete); !errors.Is(err, ErrDeliveryAction) {
		t.Fatalf("conflicting nack error=%v, want ErrDeliveryAction", err)
	}
	q.mu.Lock()
	ackCalls, nackCalls := q.ackCalls, q.nackCalls
	q.mu.Unlock()
	if ackCalls != 1 || nackCalls != 0 {
		t.Fatalf("action guard calls: ack=%d nack=%d", ackCalls, nackCalls)
	}
}

func TestWorkerExecutionErrorChainSurvivesNackFailure(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	nackErr := errors.New("nack response lost")
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-error-chain", Envelope: envelope, Job: job, Attempt: 1})
	q.nackErrors = []error{nackErr}
	w := newConfiguredWorker(t, agent.ErrProducerIncomplete, q, func(config *Config) {
		config.ActionRetryAttempts = 1
		config.VisibilityRetryAttempts = 1
	})
	record := w.track(q.delivery, context.Background())
	err = w.process(record)
	if !errors.Is(err, agent.ErrProducerIncomplete) || !errors.Is(err, nackErr) || !errors.Is(err, ErrDeliveryUnresolved) {
		t.Fatalf("execution/nack error chain=%v", err)
	}
	if record.actionState() != deliveryUnresolved {
		t.Fatalf("state=%s, want unresolved", record.actionState())
	}
}

func TestWorkerTransientVisibilityFailureRecoversBeforeCommit(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	visibilityErr := errors.New("transient visibility error")
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-visibility-recover", Envelope: envelope, Job: job, Attempt: 1})
	q.extendErrors = []error{visibilityErr}
	allow := make(chan struct{})
	canceled := make(chan struct{}, 1)
	w, sink := newBlockingWorker(t, q, allow, canceled, func(config *Config) {
		config.VisibilityRetryAttempts = 3
		config.VisibilityRetryBackoff = time.Millisecond
		config.ActionRetryAttempts = 1
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	waitForQueueStat(t, q, func(q *testQueue) bool { return q.extendCalls >= 2 }, "visibility recovery")
	close(allow)
	select {
	case <-q.acked:
	case <-time.After(time.Second):
		t.Fatal("recovered visibility execution did not ack")
	}
	if sink.Count() != 1 {
		t.Fatalf("commit count=%d, want one successful commit", sink.Count())
	}
	select {
	case <-canceled:
		t.Fatal("transient visibility failure canceled recovered runtime")
	default:
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerPersistentVisibilityFailureCancelsAndBlocksCommitAck(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	visibilityErr := errors.New("persistent visibility error")
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-visibility-fail", Envelope: envelope, Job: job, Attempt: 1})
	q.extendErrors = []error{visibilityErr, visibilityErr, visibilityErr, visibilityErr}
	allow := make(chan struct{})
	canceled := make(chan struct{}, 1)
	w, sink := newBlockingWorker(t, q, allow, canceled, func(config *Config) {
		config.VisibilityRetryAttempts = 2
		config.VisibilityRetryBackoff = time.Millisecond
		config.ActionRetryAttempts = 1
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("runtime did not receive visibility cancellation")
	}
	select {
	case <-q.acked:
		t.Fatal("visibility failure produced Ack")
	default:
	}
	if sink.Count() != 0 {
		t.Fatalf("visibility failure committed result: commits=%d", sink.Count())
	}
	select {
	case options := <-q.nacked:
		if !options.Requeue || options.Reason != "execution_canceled" {
			t.Fatalf("visibility failure nack=%+v", options)
		}
	case <-time.After(time.Second):
		t.Fatal("visibility failure did not nack recovery delivery")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerVisibilityAndNackFailureRemainUnresolved(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	visibilityErr := errors.New("visibility unavailable")
	nackErr := errors.New("nack unavailable")
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-visibility-nack", Envelope: envelope, Job: job, Attempt: 1})
	q.extendErrors = []error{visibilityErr, visibilityErr, visibilityErr, visibilityErr}
	q.nackErrors = []error{nackErr}
	allow := make(chan struct{})
	canceled := make(chan struct{}, 1)
	w, sink := newBlockingWorker(t, q, allow, canceled, func(config *Config) {
		config.VisibilityRetryAttempts = 2
		config.VisibilityRetryBackoff = time.Millisecond
		config.ActionRetryAttempts = 1
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	waitForQueueStat(t, q, func(q *testQueue) bool { return q.nackCalls >= 1 && q.extendCalls >= 4 }, "unresolved visibility/nack recovery")
	if sink.Count() != 0 {
		t.Fatalf("unresolved visibility path committed result: commits=%d", sink.Count())
	}
	q.mu.Lock()
	acked, nacked := q.ackCalls, q.nackCalls
	q.mu.Unlock()
	if acked != 0 || nacked != 1 {
		t.Fatalf("conflicting action calls: ack=%d nack=%d", acked, nacked)
	}
	w.mu.Lock()
	_, tracked := w.inFlight[q.delivery.DeliveryID]
	w.mu.Unlock()
	if !tracked {
		t.Fatal("unresolved visibility delivery was untracked")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerVisibilityFailureHasSafeNackReason(t *testing.T) {
	if got := safeNackReason(ErrVisibilityFailure); got != "delivery_visibility_failed" {
		t.Fatalf("visibility reason=%q", got)
	}
}

func TestWorkerReceiveErrorsUseBoundedBackoffAndRecover(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-receive-recover", Envelope: envelope, Job: job, Attempt: 1})
	q.receiveErrors = []error{errors.New("receive unavailable"), errors.New("receive unavailable"), errors.New("receive unavailable")}
	w := newConfiguredWorker(t, nil, q, func(config *Config) {
		config.ReceiveBackoffInitial = 2 * time.Millisecond
		config.ReceiveBackoffMax = 4 * time.Millisecond
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	select {
	case <-q.acked:
	case <-time.After(time.Second):
		t.Fatal("worker did not recover from receive errors")
	}
	q.mu.Lock()
	receiveCalls := q.receiveCalls
	q.mu.Unlock()
	if receiveCalls < 4 {
		t.Fatalf("receive calls=%d, expected failures followed by recovery", receiveCalls)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerReceiveBackoffCancelsWithoutBusyLoop(t *testing.T) {
	q := newTestQueue(queue.Delivery{})
	q.receiveErrors = []error{errors.New("receive unavailable"), errors.New("receive unavailable"), errors.New("receive unavailable"), errors.New("receive unavailable")}
	job := workerJob(t)
	q.delivery.Job = job
	w := newConfiguredWorker(t, nil, q, func(config *Config) {
		config.ReceiveBackoffInitial = 50 * time.Millisecond
		config.ReceiveBackoffMax = 100 * time.Millisecond
	})
	parent, cancel := context.WithCancel(context.Background())
	if err := w.Start(parent); err != nil {
		t.Fatal(err)
	}
	waitForQueueStat(t, q, func(q *testQueue) bool { return q.receiveCalls >= 1 }, "first receive failure")
	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), time.Second)
	defer stopCancel()
	if err := w.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	receiveCalls := q.receiveCalls
	q.mu.Unlock()
	if receiveCalls > 2 {
		t.Fatalf("receive calls=%d, cancellation did not stop backoff promptly", receiveCalls)
	}
}

func TestWorkerReceiveQueueClosedDoesNotRetry(t *testing.T) {
	q := newTestQueue(queue.Delivery{})
	if err := q.Close(); err != nil {
		t.Fatal(err)
	}
	job := workerJob(t)
	q.delivery.Job = job
	w := newConfiguredWorker(t, nil, q, nil)
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	q.mu.Lock()
	receiveCalls := q.receiveCalls
	q.mu.Unlock()
	if receiveCalls != 1 {
		t.Fatalf("queue closed was retried: receive calls=%d", receiveCalls)
	}
}

func TestExponentialBackoffIsBounded(t *testing.T) {
	if got := exponentialBackoff(time.Millisecond, 4*time.Millisecond, 1); got != time.Millisecond {
		t.Fatalf("first backoff=%s", got)
	}
	if got := exponentialBackoff(time.Millisecond, 4*time.Millisecond, 10); got != 4*time.Millisecond {
		t.Fatalf("bounded backoff=%s", got)
	}
}

func waitForInFlight(t *testing.T, w *Worker, deliveryID string) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		w.mu.Lock()
		_, tracked := w.inFlight[deliveryID]
		w.mu.Unlock()
		if tracked {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for delivery %s to be tracked", deliveryID)
		case <-ticker.C:
		}
	}
}

func TestWorkerStopStopsReceiveBeforeQueueClose(t *testing.T) {
	q := newTestQueue(queue.Delivery{})
	q.receiveCanceled = make(chan struct{}, 1)
	job := workerJob(t)
	q.delivery.Job = job
	w := newConfiguredWorker(t, nil, q, nil)
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	stopDone := make(chan error, 1)
	go func() { stopDone <- w.Stop(context.Background()) }()
	select {
	case <-q.receiveCanceled:
	case <-time.After(time.Second):
		t.Fatal("blocked Receive was not canceled by Stop")
	}
	if err := <-stopDone; err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
	q.mu.Lock()
	receiveCalls, closeCalls, closeWhileAction := q.receiveCalls, q.closeCalls, q.closeWhileAction
	q.mu.Unlock()
	if receiveCalls != 1 || closeCalls != 1 || closeWhileAction {
		t.Fatalf("receive=%d close=%d closeWhileAction=%v", receiveCalls, closeCalls, closeWhileAction)
	}
	q.deliver()
	select {
	case <-q.acked:
		t.Fatal("delivery was acked after Stop")
	default:
	}
	q.mu.Lock()
	if q.receiveCalls != 1 {
		t.Fatalf("Receive called after Stop: %d", q.receiveCalls)
	}
	q.mu.Unlock()
}

func TestWorkerStopGracefullyDrainsRuntimeBeforeCancel(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-graceful-stop", Envelope: envelope, Job: job, Attempt: 1})
	allow := make(chan struct{})
	canceled := make(chan struct{}, 1)
	w, sink := newBlockingWorker(t, q, allow, canceled, func(config *Config) {
		config.ShutdownTimeout = time.Second
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	waitForInFlight(t, w, q.delivery.DeliveryID)
	stopDone := make(chan error, 1)
	go func() { stopDone <- w.Stop(context.Background()) }()
	select {
	case <-w.stopStarted:
	case <-time.After(time.Second):
		t.Fatal("Stop did not enter stopping state")
	}
	select {
	case <-canceled:
		t.Fatal("graceful Stop canceled Runtime before deadline")
	default:
	}
	close(allow)
	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("graceful Stop returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("graceful Stop did not drain Runtime")
	}
	if sink.Count() != 1 {
		t.Fatalf("commit count=%d, want 1", sink.Count())
	}
	q.mu.Lock()
	closeCalls, closeWhileAction := q.closeCalls, q.closeWhileAction
	q.mu.Unlock()
	if closeCalls != 1 || closeWhileAction {
		t.Fatalf("close calls=%d closeWhileAction=%v", closeCalls, closeWhileAction)
	}
}

func TestWorkerStopForcesCancellationAndReportsUnresolved(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	releaseNack := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseNack) }) }
	defer release()
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-forced-stop", Envelope: envelope, Job: job, Attempt: 1})
	q.nackAlwaysError = errors.New("controlled nack failure")
	q.nackStarted = make(chan queue.NackOptions, 4)
	q.nackFailed = make(chan struct{}, 4)
	q.nackBlock = releaseNack
	allow := make(chan struct{})
	canceled := make(chan struct{}, 1)
	w, sink := newBlockingWorker(t, q, allow, canceled, func(config *Config) {
		config.ShutdownTimeout = 30 * time.Millisecond
		config.ActionRetryAttempts = 1
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	waitForInFlight(t, w, q.delivery.DeliveryID)
	stopErr := w.Stop(context.Background())
	if !errors.Is(stopErr, ErrDrainTimeout) {
		t.Fatalf("Stop error=%v, want ErrDrainTimeout", stopErr)
	}
	if !errors.Is(stopErr, ErrDeliveryUnresolved) {
		t.Fatalf("Stop error=%v, want ErrDeliveryUnresolved", stopErr)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("forced Stop did not cancel Runtime")
	}
	select {
	case <-q.nackStarted:
	case <-time.After(time.Second):
		t.Fatal("controlled Nack did not start")
	}
	if sink.Count() != 0 {
		t.Fatalf("forced cancellation committed result: %d", sink.Count())
	}
	q.mu.Lock()
	ackCalls, closeCalls, activeActions := q.ackCalls, q.closeCalls, q.activeActions
	q.mu.Unlock()
	if ackCalls != 0 || activeActions != 0 {
		t.Fatalf("ack calls=%d active actions=%d", ackCalls, activeActions)
	}
	if closeCalls > 1 {
		t.Fatalf("queue closed more than once: %d", closeCalls)
	}
	release()
	for failure := 0; failure < 2; failure++ {
		select {
		case <-q.nackFailed:
		case <-time.After(time.Second):
			t.Fatalf("controlled Nack failure %d was not observed", failure+1)
		}
	}
	select {
	case <-q.closed:
	case <-time.After(time.Second):
		t.Fatal("background stop did not close Queue")
	}
	q.mu.Lock()
	nackCalls, closeCalls, activeActions, nackApplied := q.nackCalls, q.closeCalls, q.activeActions, q.nackApplied
	q.mu.Unlock()
	if nackCalls < 2 || nackApplied {
		t.Fatalf("nack calls=%d applied=%v, want repeated deterministic failures", nackCalls, nackApplied)
	}
	if closeCalls != 1 || activeActions != 0 {
		t.Fatalf("close calls=%d active actions=%d, want one close after recovery", closeCalls, activeActions)
	}
	w.mu.Lock()
	_, tracked := w.inFlight[q.delivery.DeliveryID]
	w.mu.Unlock()
	if !tracked {
		t.Fatal("unresolved delivery was removed from inFlight")
	}
}

func TestWorkerStopUsesCallerDeadlineBeforeConfiguredTimeout(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-caller-deadline", Envelope: envelope, Job: job, Attempt: 1})
	q.extendStarted = make(chan struct{}, 1)
	releaseExtend := make(chan struct{})
	q.extendBlock = releaseExtend
	q.extendRespectContext = false
	allow := make(chan struct{})
	canceled := make(chan struct{}, 1)
	w, _ := newBlockingWorker(t, q, allow, canceled, func(config *Config) {
		config.ShutdownTimeout = time.Second
		config.CleanupTimeout = time.Second
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	select {
	case <-q.extendStarted:
	case <-time.After(time.Second):
		t.Fatal("visibility action did not start")
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	stopErr := w.Stop(stopCtx)
	cancel()
	if !errors.Is(stopErr, ErrDrainTimeout) || !errors.Is(stopErr, ErrDeliveryUnresolved) {
		t.Fatalf("Stop error=%v, want caller deadline timeout and unresolved cleanup", stopErr)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("caller deadline did not cancel Runtime")
	}
	q.mu.Lock()
	closeCalls := q.closeCalls
	q.mu.Unlock()
	if closeCalls != 0 {
		t.Fatalf("Queue closed despite incomplete caller-deadline cleanup: %d", closeCalls)
	}
	close(releaseExtend)
	waitForQueueStat(t, q, func(q *testQueue) bool { return q.activeActions == 0 }, "caller-deadline visibility action exit")
	q.Close()
}

func TestWorkerStopDoesNotCloseWhileVisibilityActionIsInUse(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-visibility-stop", Envelope: envelope, Job: job, Attempt: 1})
	q.extendStarted = make(chan struct{}, 1)
	q.extendBlock = make(chan struct{})
	q.extendRespectContext = true
	allow := make(chan struct{})
	canceled := make(chan struct{}, 1)
	w, sink := newBlockingWorker(t, q, allow, canceled, func(config *Config) {
		config.ShutdownTimeout = 100 * time.Millisecond
		config.CleanupTimeout = time.Second
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	select {
	case <-q.extendStarted:
	case <-time.After(time.Second):
		t.Fatal("visibility action did not start")
	}
	stopErr := w.Stop(context.Background())
	if !errors.Is(stopErr, ErrDrainTimeout) {
		t.Fatalf("Stop error=%v, want forced drain timeout", stopErr)
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel Runtime")
	}
	q.mu.Lock()
	closeCalls, closeWhileAction, activeActions := q.closeCalls, q.closeWhileAction, q.activeActions
	q.mu.Unlock()
	if closeCalls > 1 || closeWhileAction {
		t.Fatalf("close=%d closeWhileAction=%v active=%d", closeCalls, closeWhileAction, activeActions)
	}
	waitForQueueStat(t, q, func(q *testQueue) bool { return q.activeActions == 0 }, "visibility action exit after Stop")
	if sink.Count() != 0 {
		t.Fatalf("visibility cancellation committed result: %d", sink.Count())
	}
}

func TestWorkerStopReportsIncompleteCleanupWhenVisibilityIgnoresCancel(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-visibility-stuck", Envelope: envelope, Job: job, Attempt: 1})
	q.extendStarted = make(chan struct{}, 1)
	releaseExtend := make(chan struct{})
	q.extendBlock = releaseExtend
	q.extendRespectContext = false
	allow := make(chan struct{})
	canceled := make(chan struct{}, 1)
	w, _ := newBlockingWorker(t, q, allow, canceled, func(config *Config) {
		config.ShutdownTimeout = 30 * time.Millisecond
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	select {
	case <-q.extendStarted:
	case <-time.After(time.Second):
		t.Fatal("stuck visibility action did not start")
	}
	stopErr := w.Stop(context.Background())
	if !errors.Is(stopErr, ErrDrainTimeout) || !errors.Is(stopErr, ErrDeliveryUnresolved) {
		t.Fatalf("Stop error=%v, want timeout and unresolved cleanup", stopErr)
	}
	q.mu.Lock()
	closeCalls, activeActions := q.closeCalls, q.activeActions
	q.mu.Unlock()
	if closeCalls != 0 || activeActions != 1 {
		t.Fatalf("incomplete cleanup close=%d active=%d", closeCalls, activeActions)
	}
	close(releaseExtend)
	waitForQueueStat(t, q, func(q *testQueue) bool { return q.activeActions == 0 }, "stuck visibility action exit")
	waitForQueueStat(t, q, func(q *testQueue) bool { return q.closeCalls == 1 }, "deferred Queue.Close")
	q.mu.Lock()
	if q.closeWhileAction {
		t.Fatal("deferred Queue.Close raced with the visibility action")
	}
	q.mu.Unlock()
}

func TestWorkerStopConcurrentCallsCloseOnceAndShareResult(t *testing.T) {
	q := newTestQueue(queue.Delivery{})
	w := newConfiguredWorker(t, nil, q, func(config *Config) {
		config.Concurrency = 2
	})
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	go func() { results <- w.Stop(context.Background()) }()
	go func() { results <- w.Stop(context.Background()) }()
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent Stop returned error: %v", err)
		}
	}
	q.mu.Lock()
	closeCalls := q.closeCalls
	q.mu.Unlock()
	if closeCalls != 1 {
		t.Fatalf("Close calls=%d, want 1", closeCalls)
	}
}

func TestWorkerOutstandingRecoveryRespectsTerminalAndUnknownStates(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	q := newTestQueue(queue.Delivery{Envelope: envelope, Job: job, Attempt: 1})
	w := newConfiguredWorker(t, nil, q, func(config *Config) {
		config.ActionRetryAttempts = 1
		config.VisibilityRetryAttempts = 1
	})
	states := []struct {
		id         string
		state      deliveryActionState
		lastAction deliveryActionState
		actionErr  error
	}{
		{id: "processing", state: deliveryProcessing},
		{id: "ack-pending", state: deliveryAckPending, lastAction: deliveryAckPending},
		{id: "nack-pending", state: deliveryNackPending, lastAction: deliveryNackPending},
		{id: "unresolved-ack", state: deliveryUnresolved, lastAction: deliveryAckPending, actionErr: errors.New("ack unknown")},
		{id: "unresolved-nack", state: deliveryUnresolved, lastAction: deliveryNackPending, actionErr: errors.New("nack unknown")},
		{id: "acked", state: deliveryAcked},
		{id: "nacked", state: deliveryNacked},
	}
	for _, item := range states {
		delivery := q.delivery
		delivery.DeliveryID = item.id
		record := w.track(delivery, context.Background())
		record.mu.Lock()
		record.state = item.state
		record.lastAction = item.lastAction
		record.actionErr = item.actionErr
		record.mu.Unlock()
	}
	if err := w.nackOutstanding(context.Background()); err == nil || !errors.Is(err, ErrDeliveryUnresolved) {
		t.Fatalf("outstanding recovery error=%v, want unresolved states", err)
	}
	q.mu.Lock()
	ackCalls, nackCalls, extendCalls := q.ackCalls, q.nackCalls, q.extendCalls
	q.mu.Unlock()
	if ackCalls != 0 || nackCalls != 2 || extendCalls != 1 {
		t.Fatalf("recovery actions ack=%d nack=%d extend=%d", ackCalls, nackCalls, extendCalls)
	}
	w.mu.Lock()
	remaining := len(w.inFlight)
	w.mu.Unlock()
	if remaining != 3 {
		t.Fatalf("remaining inFlight=%d, want ack-pending/nack-pending/unresolved-ack", remaining)
	}
	if err := w.outstandingError(); !errors.Is(err, ErrDeliveryUnresolved) || !strings.Contains(err.Error(), "ack-pending") {
		t.Fatalf("outstanding error=%v", err)
	}
}

func TestWorkerStopIsIdempotent(t *testing.T) {
	job := workerJob(t)
	envelope, _ := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-stop", Envelope: envelope, Job: job, Attempt: 1})
	w := newWorker(t, nil, q)
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	if err := w.Stop(context.Background()); err != nil {
		t.Fatalf("repeated Stop should be nil, got %v", err)
	}
}
