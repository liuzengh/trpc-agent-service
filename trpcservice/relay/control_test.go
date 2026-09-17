package relay

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type controlPublisherStub struct{ events []TenantControlEvent }

func (s *controlPublisherStub) PublishTenantControl(_ context.Context, event TenantControlEvent) error {
	s.events = append(s.events, event)
	return nil
}

type executionControlPublisherStub struct{ events []ExecutionControlEvent }

func (s *executionControlPublisherStub) PublishExecutionControl(_ context.Context, event ExecutionControlEvent) error {
	s.events = append(s.events, event)
	return nil
}

func TestExecutionControlRelayPublishesCancellationHint(t *testing.T) {
	outbox := &outboxStub{records: []messaging.OutboxRecord{{TenantID: "tenant", OutboxID: "control", Kind: "execution-control", AggregateID: "request", IdempotencyKey: "cancel:1", PayloadRef: "cancel-intent://tenant/request/1", EventSeq: 1, Version: 1}}}
	publisher := &executionControlPublisherStub{}
	r := ExecutionControlRelay{Outbox: outbox, Controls: publisher, Owner: "execution-control-relay"}
	count, err := r.RunOnce(context.Background())
	if err != nil || count != 1 || len(publisher.events) != 1 || publisher.events[0].Version != 1 || !outbox.published {
		t.Fatalf("count=%d err=%v events=%#v published=%t", count, err, publisher.events, outbox.published)
	}
}

func TestTenantControlRelayPublishesVersionedEvent(t *testing.T) {
	outbox := &outboxStub{records: []messaging.OutboxRecord{{TenantID: "tenant", OutboxID: "control", Kind: "tenant-control", AggregateID: "tenant", IdempotencyKey: "tenant:4", PayloadRef: "tenant://tenant/4", EventSeq: 4, Version: 1}}}
	publisher := &controlPublisherStub{}
	r := TenantControlRelay{Outbox: outbox, Controls: publisher, Kind: "tenant-control", Owner: "control-relay"}
	count, err := r.RunOnce(context.Background())
	if err != nil || count != 1 || len(publisher.events) != 1 || publisher.events[0].Version != 4 || !outbox.published {
		t.Fatalf("count=%d err=%v events=%#v published=%t", count, err, publisher.events, outbox.published)
	}
}

type controlReaderStub struct{ version uint64 }

func (s *controlReaderStub) ReadTenantControl(_ context.Context, event TenantControlEvent) (TenantControlState, error) {
	return TenantControlState{TenantID: event.TenantID, Kind: event.Kind, AggregateID: event.AggregateID, Status: "active", Version: s.version}, nil
}

type controlSinkStub struct{ versions []uint64 }

func (s *controlSinkStub) ApplyTenantControl(_ context.Context, state TenantControlState) error {
	s.versions = append(s.versions, state.Version)
	return nil
}

func TestTenantControlConsumerConvergesMonotonically(t *testing.T) {
	reader := &controlReaderStub{}
	sink := &controlSinkStub{}
	consumer := &MonotonicTenantControlConsumer{Reader: reader, Sink: sink}
	for _, version := range []uint64{3, 2, 4} {
		reader.version = version
		event := TenantControlEvent{TenantID: "tenant", Kind: "tenant-control", AggregateID: "tenant", Version: version}
		if err := consumer.Consume(context.Background(), event); err != nil {
			t.Fatal(err)
		}
	}
	if got := consumer.Watermark("tenant", "tenant-control", "tenant"); got != 4 {
		t.Fatalf("watermark=%d", got)
	}
	if len(sink.versions) != 2 || sink.versions[0] != 3 || sink.versions[1] != 4 {
		t.Fatalf("applied=%v", sink.versions)
	}
}

type wakeupPublisherStub struct{ events []WakeupEvent }

func (s *wakeupPublisherStub) PublishWakeup(_ context.Context, event WakeupEvent) error {
	s.events = append(s.events, event)
	return nil
}

func TestWakeupRelayPublishesStableEvent(t *testing.T) {
	outbox := &outboxStub{records: []messaging.OutboxRecord{{TenantID: "tenant", OutboxID: "wakeup", Kind: "wakeup", AggregateID: "request", IdempotencyKey: "wakeup:request", PayloadRef: "execution://tenant/request", EventSeq: 8, Version: 1}}}
	publisher := &wakeupPublisherStub{}
	r := WakeupRelay{Outbox: outbox, Wakeups: publisher, Owner: "wakeup-relay"}
	count, err := r.RunOnce(context.Background())
	if err != nil || count != 1 || len(publisher.events) != 1 || publisher.events[0].IdempotencyKey != "wakeup:request" {
		t.Fatalf("count=%d err=%v events=%#v", count, err, publisher.events)
	}
}
