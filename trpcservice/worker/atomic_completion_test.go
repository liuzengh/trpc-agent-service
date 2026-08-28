package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type atomicCompletionStub struct {
	mu        sync.Mutex
	calls     int
	request   storage.AtomicCompletionRequest
	err       error
	completed chan struct{}
}

func (s *atomicCompletionStub) CommitResultAndAck(ctx context.Context, request storage.AtomicCompletionRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	s.calls++
	s.request = request
	err := s.err
	s.mu.Unlock()
	if s.completed != nil {
		select {
		case s.completed <- struct{}{}:
		default:
		}
	}
	return err
}

func (s *atomicCompletionStub) snapshot() (int, storage.AtomicCompletionRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.request
}

func TestWorkerAtomicCompletionDoesNotCallIndependentAck(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-atomic", Envelope: envelope, Job: job, Attempt: 1, ReceivedAt: time.Now().UTC(), VisibleUntil: time.Now().UTC().Add(time.Second)})
	leases := &testLeaseStore{}
	sink := &testSink{}
	executor, err := execution.NewExecutor(leases, testAgents{}, sink)
	if err != nil {
		t.Fatal(err)
	}
	completion := &atomicCompletionStub{completed: make(chan struct{}, 1)}
	config := Config{
		WorkerID: "worker-atomic", Concurrency: 1, VisibilityTimeout: time.Second, LeaseTTL: time.Second,
		CleanupTimeout: 100 * time.Millisecond,
		ResolveAgent: AgentResolverFunc(func(context.Context, tenant.TenantContext, queue.AgentRefDTO) (agent.AgentSpec, error) {
			return agent.AgentSpec{TenantID: job.Tenant.TenantID, AgentAppID: job.Agent.AgentAppID, Version: job.Agent.Version, Name: "assistant", ModelProvider: "fake"}, nil
		}),
	}
	w, err := NewWithAtomicCompletion(q, executor, config, completion)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	select {
	case <-completion.completed:
	case <-time.After(time.Second):
		t.Fatal("worker did not call atomic completion")
	}
	calls, request := completion.snapshot()
	if calls != 1 || request.Commit.JobID != job.JobID || request.Delivery.DeliveryID != q.delivery.DeliveryID ||
		request.Outbox == nil || request.Outbox.AggregateID != job.ExecutionID || request.Outbox.DedupKey == "" {
		t.Fatalf("atomic completion calls=%d request=%+v", calls, request)
	}
	q.mu.Lock()
	ackCalls := q.ackCalls
	q.mu.Unlock()
	if ackCalls != 0 {
		t.Fatalf("independent queue Ack calls=%d", ackCalls)
	}
	if sink.Count() != 0 {
		t.Fatalf("legacy execution sink commits=%d", sink.Count())
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Stop(stopCtx); err != nil && !errors.Is(err, ErrDrainTimeout) {
		t.Fatal(err)
	}
}

func TestWorkerAtomicCompletionErrorRequeuesWithoutAck(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-atomic-error", Envelope: envelope, Job: job, Attempt: 1, ReceivedAt: time.Now().UTC(), VisibleUntil: time.Now().UTC().Add(time.Second)})
	leases := &testLeaseStore{}
	sink := &testSink{}
	executor, err := execution.NewExecutor(leases, testAgents{}, sink)
	if err != nil {
		t.Fatal(err)
	}
	completion := &atomicCompletionStub{err: storage.ErrFenceRejected, completed: make(chan struct{}, 1)}
	config := Config{
		WorkerID: "worker-atomic-error", Concurrency: 1, VisibilityTimeout: time.Second, LeaseTTL: time.Second,
		CleanupTimeout: 100 * time.Millisecond,
		ResolveAgent: AgentResolverFunc(func(context.Context, tenant.TenantContext, queue.AgentRefDTO) (agent.AgentSpec, error) {
			return agent.AgentSpec{TenantID: job.Tenant.TenantID, AgentAppID: job.Agent.AgentAppID, Version: job.Agent.Version, Name: "assistant", ModelProvider: "fake"}, nil
		}),
	}
	w, err := NewWithAtomicCompletion(q, executor, config, completion)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	select {
	case <-q.nacked:
	case <-time.After(time.Second):
		t.Fatal("atomic completion failure was not requeued")
	}
	q.mu.Lock()
	ackCalls := q.ackCalls
	nackApplied := q.nackApplied
	q.mu.Unlock()
	if ackCalls != 0 || !nackApplied {
		t.Fatalf("failure actions ack_calls=%d nack_applied=%v", ackCalls, nackApplied)
	}
	if sink.Count() != 0 {
		t.Fatalf("legacy execution sink commits=%d", sink.Count())
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Stop(stopCtx); err != nil && !errors.Is(err, ErrDrainTimeout) {
		t.Fatal(err)
	}
}

func TestWorkerAtomicCompletionUnknownOutcomeStaysUnresolved(t *testing.T) {
	job := workerJob(t)
	envelope, err := queue.EncodeJobAt(job, time.Now().UTC(), queue.DefaultJobMaxAge)
	if err != nil {
		t.Fatal(err)
	}
	q := newTestQueue(queue.Delivery{DeliveryID: "delivery-atomic-unknown", Envelope: envelope, Job: job, Attempt: 1, ReceivedAt: time.Now().UTC(), VisibleUntil: time.Now().UTC().Add(time.Second)})
	leases := &testLeaseStore{}
	sink := &testSink{}
	executor, err := execution.NewExecutor(leases, testAgents{}, sink)
	if err != nil {
		t.Fatal(err)
	}
	// pi-lens-ignore: UndeclaredImportedName
	completion := &atomicCompletionStub{err: storage.ErrCompletionOutcomeUnknown, completed: make(chan struct{}, 1)}
	config := Config{
		WorkerID: "worker-atomic-unknown", Concurrency: 1, VisibilityTimeout: time.Second, LeaseTTL: time.Second,
		CleanupTimeout: 100 * time.Millisecond,
		ResolveAgent: AgentResolverFunc(func(context.Context, tenant.TenantContext, queue.AgentRefDTO) (agent.AgentSpec, error) {
			return agent.AgentSpec{TenantID: job.Tenant.TenantID, AgentAppID: job.Agent.AgentAppID, Version: job.Agent.Version, Name: "assistant", ModelProvider: "fake"}, nil
		}),
	}
	w, err := NewWithAtomicCompletion(q, executor, config, completion)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	q.deliver()
	select {
	case <-completion.completed:
	case <-time.After(time.Second):
		t.Fatal("worker did not reach unknown completion")
	}
	q.mu.Lock()
	ackCalls := q.ackCalls
	nackCalls := q.nackCalls
	q.mu.Unlock()
	if ackCalls != 0 || nackCalls != 0 {
		t.Fatalf("unknown outcome triggered conflicting action ack=%d nack=%d", ackCalls, nackCalls)
	}
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Stop(stopCtx); err != nil && !errors.Is(err, ErrDrainTimeout) {
		t.Fatal(err)
	}
}
