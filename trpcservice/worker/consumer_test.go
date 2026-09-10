package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionmemory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/inmemory"
)

type leaseStub struct {
	mu        sync.Mutex
	renewals  int
	failRenew bool
}

func (s *leaseStub) Acquire(_ context.Context, key coordination.SessionKey, worker string, ttl time.Duration) (coordination.Lease, error) {
	return coordination.Lease{Session: key, WorkerID: worker, LeaseID: "lease", Fence: 1, ExpiresAt: time.Now().Add(ttl)}, nil
}
func (s *leaseStub) Renew(context.Context, coordination.Lease, time.Duration) (coordination.Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renewals++
	if s.failRenew {
		return coordination.Lease{}, runtime.ErrLeaseLost
	}
	return coordination.Lease{Fence: 1}, nil
}
func (*leaseStub) Release(context.Context, coordination.Lease) error { return nil }
func (*leaseStub) EnsureFenceAtLeast(context.Context, coordination.SessionKey, uint64) error {
	return nil
}

type brokerStub struct{ acked bool }

type executionReaderStub struct {
	mu        sync.Mutex
	cancelled bool
}

func (s *executionReaderStub) GetExecution(_ context.Context, key gateway.ExecutionKey) (gateway.ExecutionStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	version := int64(0)
	if s.cancelled {
		version = 1
	}
	return gateway.ExecutionStatus{Envelope: runtime.ExecutionEnvelope{TenantID: key.TenantID, RequestID: key.RequestID}, CancelRequested: s.cancelled, CancelVersion: version}, nil
}

func (*brokerStub) Publish(context.Context, broker.Shard, runtime.ExecutionEnvelope) error {
	return nil
}

type recoveryBroker struct {
	delivery  broker.Delivery
	mu        sync.Mutex
	ackCalls  int
	reclaimed bool
	recovered chan struct{}
}

func (*recoveryBroker) Publish(context.Context, broker.Shard, runtime.ExecutionEnvelope) error {
	return nil
}
func (s *recoveryBroker) Consume(ctx context.Context, _ broker.ConsumerOptions, handle func(context.Context, broker.Delivery) error) error {
	if err := handle(ctx, s.delivery); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}
func (s *recoveryBroker) Ack(context.Context, broker.Delivery) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ackCalls++
	if s.ackCalls == 1 {
		return errors.New("ack connection lost")
	}
	select {
	case <-s.recovered:
	default:
		close(s.recovered)
	}
	return nil
}
func (s *recoveryBroker) Reclaim(context.Context, broker.ReclaimOptions) ([]broker.Delivery, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ackCalls == 1 && !s.reclaimed {
		s.reclaimed = true
		return []broker.Delivery{s.delivery}, nil
	}
	return nil, nil
}
func (*brokerStub) Consume(context.Context, broker.ConsumerOptions, func(context.Context, broker.Delivery) error) error {
	return nil
}
func (s *brokerStub) Ack(context.Context, broker.Delivery) error { s.acked = true; return nil }
func (*brokerStub) Reclaim(context.Context, broker.ReclaimOptions) ([]broker.Delivery, error) {
	return nil, nil
}

type leaseExecutorFunc func(context.Context, runtime.ExecutionEnvelope, uint64, func(context.Context) error) error

func (f leaseExecutorFunc) ExecuteWithLease(ctx context.Context, envelope runtime.ExecutionEnvelope, fence uint64, guard func(context.Context) error) error {
	return f(ctx, envelope, fence, guard)
}

func (leaseExecutorFunc) CancelWithLease(context.Context, runtime.ExecutionEnvelope, uint64, func(context.Context) error) error {
	return runtime.ErrCapabilityUnsupported
}

type cancellationExecutorStub struct {
	started   chan struct{}
	mu        sync.Mutex
	cancelled bool
}

func (s *cancellationExecutorStub) ExecuteWithLease(ctx context.Context, _ runtime.ExecutionEnvelope, _ uint64, _ func(context.Context) error) error {
	close(s.started)
	<-ctx.Done()
	return ctx.Err()
}

func (s *cancellationExecutorStub) CancelWithLease(ctx context.Context, _ runtime.ExecutionEnvelope, _ uint64, guard func(context.Context) error) error {
	if err := guard(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	s.cancelled = true
	s.mu.Unlock()
	return nil
}

type drainingBroker struct {
	delivery broker.Delivery
	acked    atomic.Bool
}

func (*drainingBroker) Publish(context.Context, broker.Shard, runtime.ExecutionEnvelope) error {
	return nil
}
func (s *drainingBroker) Consume(ctx context.Context, _ broker.ConsumerOptions, handle func(context.Context, broker.Delivery) error) error {
	if err := handle(ctx, s.delivery); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}
func (s *drainingBroker) Ack(context.Context, broker.Delivery) error {
	s.acked.Store(true)
	return nil
}
func (*drainingBroker) Reclaim(context.Context, broker.ReclaimOptions) ([]broker.Delivery, error) {
	return nil, nil
}

type parkerStub struct {
	result gateway.ParkResult
	calls  int
}

func (s *parkerStub) ParkInput(context.Context, gateway.ParkRequest) (gateway.ParkResult, error) {
	s.calls++
	return s.result, nil
}

func TestConsumerDurablyParksFutureInputBeforeAck(t *testing.T) {
	leases := &leaseStub{}
	messages := &brokerStub{}
	parker := &parkerStub{result: gateway.ParkResult{Disposition: gateway.ParkedInput, Attempt: 1}}
	consumer := Consumer{WorkerID: "worker", Broker: messages, Leases: leases, Sessions: sessionmemory.New(), Parker: parker, Statuses: &executionReaderStub{},
		LeaseTTL: time.Second, RenewInterval: time.Hour,
		Executor: leaseExecutorFunc(func(context.Context, runtime.ExecutionEnvelope, uint64, func(context.Context) error) error {
			return runtime.ErrInputNotReady
		})}
	delivery := broker.Delivery{ID: "message", Envelope: runtime.ExecutionEnvelope{TenantID: "tenant", AgentAppID: "app", SessionID: "session", RequestID: "request", InputSeq: 2}}
	if err := consumer.handle(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	if parker.calls != 1 || !messages.acked {
		t.Fatalf("park calls=%d acked=%t", parker.calls, messages.acked)
	}
}

func TestConsumerRenewsSlowExecutionAndValidatesBeforeCommit(t *testing.T) {
	leases := &leaseStub{}
	messages := &brokerStub{}
	consumer := Consumer{WorkerID: "worker", Broker: messages, Leases: leases, Sessions: sessionmemory.New(), Statuses: &executionReaderStub{}, LeaseTTL: 30 * time.Millisecond, RenewInterval: 5 * time.Millisecond}
	consumer.Executor = leaseExecutorFunc(func(ctx context.Context, _ runtime.ExecutionEnvelope, _ uint64, guard func(context.Context) error) error {
		timer := time.NewTimer(35 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		return guard(ctx)
	})
	delivery := broker.Delivery{ID: "message", Envelope: runtime.ExecutionEnvelope{TenantID: "tenant", AgentAppID: "app", SessionID: "session"}}
	if err := consumer.handle(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	if leases.renewals < 2 || !messages.acked {
		t.Fatalf("renewals=%d acked=%t", leases.renewals, messages.acked)
	}
}

func TestConsumerDoesNotAckAfterLeaseLoss(t *testing.T) {
	leases := &leaseStub{failRenew: true}
	messages := &brokerStub{}
	consumer := Consumer{WorkerID: "worker", Broker: messages, Leases: leases, Sessions: sessionmemory.New(), Statuses: &executionReaderStub{}, LeaseTTL: 30 * time.Millisecond, RenewInterval: time.Millisecond}
	consumer.Executor = leaseExecutorFunc(func(ctx context.Context, _ runtime.ExecutionEnvelope, _ uint64, guard func(context.Context) error) error {
		<-ctx.Done()
		return guard(context.Background())
	})
	delivery := broker.Delivery{ID: "message", Envelope: runtime.ExecutionEnvelope{TenantID: "tenant", AgentAppID: "app", SessionID: "session"}}
	err := consumer.handle(context.Background(), delivery)
	if !errors.Is(err, runtime.ErrLeaseLost) || messages.acked {
		t.Fatalf("err=%v acked=%t", err, messages.acked)
	}
}

func TestConsumerStaysAliveAndReclaimsAfterAckFailure(t *testing.T) {
	delivery := broker.Delivery{ID: "message", Envelope: runtime.ExecutionEnvelope{TenantID: "tenant", AgentAppID: "app", SessionID: "session"}}
	messages := &recoveryBroker{delivery: delivery, recovered: make(chan struct{})}
	consumer := Consumer{
		WorkerID: "worker", Shards: []broker.Shard{0}, Broker: messages, Leases: &leaseStub{}, Sessions: sessionmemory.New(),
		Statuses: &executionReaderStub{},
		LeaseTTL: time.Second, RenewInterval: time.Hour, ReclaimInterval: time.Millisecond,
		Executor: leaseExecutorFunc(func(ctx context.Context, _ runtime.ExecutionEnvelope, _ uint64, guard func(context.Context) error) error {
			return guard(ctx)
		}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	select {
	case <-messages.recovered:
		cancel()
	case <-ctx.Done():
		t.Fatal("pending delivery was not reclaimed")
	}
	<-done
	messages.mu.Lock()
	defer messages.mu.Unlock()
	if messages.ackCalls != 2 || !messages.reclaimed {
		t.Fatalf("ack calls=%d reclaimed=%t", messages.ackCalls, messages.reclaimed)
	}
}

func TestConsumerTurnsDurableCancelIntoFencedTerminal(t *testing.T) {
	statuses := &executionReaderStub{}
	executor := &cancellationExecutorStub{started: make(chan struct{})}
	messages := &brokerStub{}
	consumer := Consumer{WorkerID: "worker", Broker: messages, Leases: &leaseStub{}, Sessions: sessionmemory.New(),
		Statuses: statuses, Executor: executor, LeaseTTL: time.Second, RenewInterval: time.Hour, CancelPollInterval: time.Millisecond}
	delivery := broker.Delivery{ID: "message", Envelope: runtime.ExecutionEnvelope{TenantID: "tenant", AgentAppID: "app", SessionID: "session", RequestID: "request"}}
	done := make(chan error, 1)
	go func() { done <- consumer.handle(context.Background(), delivery) }()
	<-executor.started
	statuses.mu.Lock()
	statuses.cancelled = true
	statuses.mu.Unlock()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	executor.mu.Lock()
	cancelled := executor.cancelled
	executor.mu.Unlock()
	if !cancelled || !messages.acked {
		t.Fatalf("cancelled=%t acked=%t", cancelled, messages.acked)
	}
}

func TestConsumerDrainStopsNewConsumptionAndFinishesActiveDelivery(t *testing.T) {
	lifecycle := NewLifecycle()
	release := make(chan struct{})
	started := make(chan struct{})
	delivery := broker.Delivery{ID: "message", Envelope: runtime.ExecutionEnvelope{TenantID: "tenant", AgentAppID: "app", SessionID: "session", RequestID: "request"}}
	messages := &drainingBroker{delivery: delivery}
	consumer := Consumer{WorkerID: "worker", Shards: []broker.Shard{0}, Broker: messages, Leases: &leaseStub{}, Sessions: sessionmemory.New(),
		Statuses: &executionReaderStub{}, Lifecycle: lifecycle, DrainTimeout: time.Second, LeaseTTL: time.Second, RenewInterval: time.Hour,
		Executor: leaseExecutorFunc(func(ctx context.Context, _ runtime.ExecutionEnvelope, _ uint64, guard func(context.Context) error) error {
			close(started)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return guard(ctx)
			}
		})}
	done := make(chan error, 1)
	go func() { done <- consumer.Run(context.Background()) }()
	<-started
	if !lifecycle.Ready() {
		t.Fatalf("lifecycle=%s", lifecycle.State())
	}
	lifecycle.BeginDrain()
	select {
	case err := <-done:
		t.Fatalf("drain returned before active delivery completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !messages.acked.Load() {
		t.Fatal("active delivery was not acknowledged during graceful drain")
	}
	if lifecycle.State() != LifecycleStopped {
		t.Fatalf("lifecycle=%s", lifecycle.State())
	}
}
