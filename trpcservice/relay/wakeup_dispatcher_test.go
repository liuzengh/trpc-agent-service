package relay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type wakeupQueueStub struct {
	acked      bool
	ackCalls   int
	ackErrOnce error
}

func (*wakeupQueueStub) Consume(context.Context, WakeupConsumerOptions, func(context.Context, WakeupDelivery) error) error {
	panic("unexpected call")
}
func (s *wakeupQueueStub) AckWakeup(context.Context, WakeupDelivery) error {
	s.ackCalls++
	if s.ackErrOnce != nil {
		err := s.ackErrOnce
		s.ackErrOnce = nil
		return err
	}
	s.acked = true
	return nil
}
func (*wakeupQueueStub) ReclaimWakeups(context.Context, WakeupReclaimOptions) ([]WakeupDelivery, error) {
	return nil, nil
}

type wakeupStoreStub struct {
	candidate   gateway.WakeupCandidate
	marked      bool
	markErrOnce error
	keys        []gateway.ExecutionKey
}

func (s *wakeupStoreStub) InspectWakeup(context.Context, gateway.ExecutionKey) (gateway.WakeupCandidate, error) {
	return s.candidate, nil
}
func (s *wakeupStoreStub) MarkWoken(context.Context, gateway.ExecutionKey, int64) error {
	if s.markErrOnce != nil {
		err := s.markErrOnce
		s.markErrOnce = nil
		return err
	}
	s.marked = true
	s.candidate.Execution.Outcome = runtime.OutcomeQueued
	return nil
}
func (s *wakeupStoreStub) ListActionableParkedInputs(context.Context, time.Time, int) ([]gateway.ExecutionKey, error) {
	return append([]gateway.ExecutionKey(nil), s.keys...), nil
}

type wakeupBrokerStub struct {
	published runtime.ExecutionEnvelope
	calls     int
	errOnce   error
}

func (s *wakeupBrokerStub) Publish(_ context.Context, _ broker.Shard, envelope runtime.ExecutionEnvelope) error {
	s.published = envelope
	s.calls++
	if s.errOnce != nil {
		err := s.errOnce
		s.errOnce = nil
		return err
	}
	return nil
}

func TestWakeupDispatcherReplaysPublishAfterMarkFailure(t *testing.T) {
	envelope := runtime.ExecutionEnvelope{TenantID: "tenant", AgentAppID: "app", SessionID: "session", RequestID: "request"}
	queue := &wakeupQueueStub{}
	store := &wakeupStoreStub{candidate: gateway.WakeupCandidate{Execution: gateway.ExecutionStatus{Envelope: envelope, Outcome: runtime.OutcomePending}, Ready: true, Version: 4}, markErrOnce: runtime.ErrBackendUnavailable}
	dispatch := &wakeupBrokerStub{}
	d := WakeupDispatcher{ConsumerID: "consumer", Wakeups: queue, Store: store, Dispatch: dispatch, ShardCount: 4}
	delivery := WakeupDelivery{ID: "1-0", Event: WakeupEvent{TenantID: "tenant", AggregateID: "request"}}
	if err := d.handle(context.Background(), delivery); !errors.Is(err, runtime.ErrBackendUnavailable) || queue.acked {
		t.Fatalf("first err=%v acked=%t", err, queue.acked)
	}
	if err := d.handle(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	if dispatch.calls != 2 || !store.marked || !queue.acked {
		t.Fatalf("publish calls=%d marked=%t acked=%t", dispatch.calls, store.marked, queue.acked)
	}
}

func TestWakeupDispatcherDoesNotRepublishAfterAckFailure(t *testing.T) {
	envelope := runtime.ExecutionEnvelope{TenantID: "tenant", AgentAppID: "app", SessionID: "session", RequestID: "request"}
	queue := &wakeupQueueStub{ackErrOnce: runtime.ErrBackendUnavailable}
	store := &wakeupStoreStub{candidate: gateway.WakeupCandidate{Execution: gateway.ExecutionStatus{Envelope: envelope, Outcome: runtime.OutcomePending}, Ready: true, Version: 4}}
	dispatch := &wakeupBrokerStub{}
	d := WakeupDispatcher{ConsumerID: "consumer", Wakeups: queue, Store: store, Dispatch: dispatch, ShardCount: 4}
	delivery := WakeupDelivery{ID: "1-0", Event: WakeupEvent{TenantID: "tenant", AggregateID: "request"}}
	if err := d.handle(context.Background(), delivery); !errors.Is(err, runtime.ErrBackendUnavailable) {
		t.Fatalf("first err=%v", err)
	}
	if err := d.handle(context.Background(), delivery); err != nil {
		t.Fatal(err)
	}
	if dispatch.calls != 1 || queue.ackCalls != 2 || !queue.acked {
		t.Fatalf("publish calls=%d ack calls=%d acked=%t", dispatch.calls, queue.ackCalls, queue.acked)
	}
}

func TestParkedInputReconcilerRepairsLostWakeup(t *testing.T) {
	key := gateway.ExecutionKey{TenantID: "tenant", RequestID: "request"}
	envelope := runtime.ExecutionEnvelope{TenantID: "tenant", AgentAppID: "app", SessionID: "session", RequestID: "request"}
	store := &wakeupStoreStub{keys: []gateway.ExecutionKey{key}, candidate: gateway.WakeupCandidate{Execution: gateway.ExecutionStatus{Envelope: envelope, Outcome: runtime.OutcomePending}, Ready: true, Version: 4}}
	dispatch := &wakeupBrokerStub{}
	reconciler := ParkedInputReconciler{Store: store, Dispatch: dispatch, ShardCount: 4}
	if count, err := reconciler.RunOnce(context.Background()); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if dispatch.calls != 1 || !store.marked {
		t.Fatalf("publish calls=%d marked=%t", dispatch.calls, store.marked)
	}
}

func TestParkedInputReconcilerContinuesAfterItemFailure(t *testing.T) {
	keys := []gateway.ExecutionKey{{TenantID: "tenant", RequestID: "first"}, {TenantID: "tenant", RequestID: "second"}}
	envelope := runtime.ExecutionEnvelope{TenantID: "tenant", AgentAppID: "app", SessionID: "session", RequestID: "request"}
	store := &wakeupStoreStub{keys: keys, candidate: gateway.WakeupCandidate{Execution: gateway.ExecutionStatus{Envelope: envelope, Outcome: runtime.OutcomePending}, Ready: true, Version: 4}}
	dispatch := &wakeupBrokerStub{errOnce: runtime.ErrBackendUnavailable}
	reconciler := ParkedInputReconciler{Store: store, Dispatch: dispatch, ShardCount: 4}
	count, err := reconciler.RunOnce(context.Background())
	if !errors.Is(err, runtime.ErrBackendUnavailable) || count != 1 || dispatch.calls != 2 || !store.marked {
		t.Fatalf("count=%d calls=%d marked=%t err=%v", count, dispatch.calls, store.marked, err)
	}
}
func (*wakeupBrokerStub) Consume(context.Context, broker.ConsumerOptions, func(context.Context, broker.Delivery) error) error {
	panic("unexpected call")
}
func (*wakeupBrokerStub) Ack(context.Context, broker.Delivery) error { panic("unexpected call") }
func (*wakeupBrokerStub) Reclaim(context.Context, broker.ReclaimOptions) ([]broker.Delivery, error) {
	panic("unexpected call")
}

func TestWakeupDispatcherPublishesMarksThenAcknowledges(t *testing.T) {
	envelope := runtime.ExecutionEnvelope{TenantID: "tenant", AgentAppID: "app", SessionID: "session", RequestID: "request"}
	queue := &wakeupQueueStub{}
	store := &wakeupStoreStub{candidate: gateway.WakeupCandidate{Execution: gateway.ExecutionStatus{Envelope: envelope, Outcome: runtime.OutcomePending}, Ready: true, Version: 4}}
	dispatch := &wakeupBrokerStub{}
	d := WakeupDispatcher{ConsumerID: "consumer", Wakeups: queue, Store: store, Dispatch: dispatch, ShardCount: 4}
	if err := d.handle(context.Background(), WakeupDelivery{ID: "1-0", Event: WakeupEvent{TenantID: "tenant", AggregateID: "request"}}); err != nil {
		t.Fatal(err)
	}
	if dispatch.published.RequestID != "request" || !store.marked || !queue.acked {
		t.Fatalf("published=%#v marked=%t acked=%t", dispatch.published, store.marked, queue.acked)
	}
}
