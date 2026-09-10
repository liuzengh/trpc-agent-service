package relay

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type outboxStub struct {
	records     []messaging.OutboxRecord
	published   bool
	retried     bool
	renewals    int
	markVersion uint64
}

func (s *outboxStub) ClaimOutbox(context.Context, string, int, string, time.Time) ([]messaging.OutboxRecord, error) {
	return s.records, nil
}
func (s *outboxStub) MarkPublished(_ context.Context, _, _ string, expectedVersion uint64) error {
	s.published = true
	s.markVersion = expectedVersion
	return nil
}
func (s *outboxStub) RenewOutboxClaim(_ context.Context, _, _ string, expectedVersion uint64, _ string, _ time.Time) (uint64, error) {
	s.renewals++
	return expectedVersion + 1, nil
}
func (s *outboxStub) MarkRetry(context.Context, string, string, uint64, time.Time) error {
	s.retried = true
	return nil
}

type relayTaskStub struct{ envelope runtime.ExecutionEnvelope }

func (s relayTaskStub) PrepareDispatch(context.Context, gateway.PrepareDispatchRequest) (gateway.PreparedDispatch, error) {
	panic("unexpected call")
}
func (s relayTaskStub) GetExecution(context.Context, gateway.ExecutionKey) (gateway.ExecutionStatus, error) {
	return gateway.ExecutionStatus{Envelope: s.envelope}, nil
}
func (relayTaskStub) RequestCancel(context.Context, gateway.CancelRequest) (gateway.CancelResult, error) {
	panic("unexpected call")
}
func (relayTaskStub) ParkInput(context.Context, gateway.ParkRequest) (gateway.ParkResult, error) {
	panic("unexpected call")
}

type relayBrokerStub struct {
	fail      bool
	published runtime.ExecutionEnvelope
	delay     time.Duration
	calls     int
}

func (s *relayBrokerStub) Publish(_ context.Context, _ broker.Shard, envelope runtime.ExecutionEnvelope) error {
	s.calls++
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	if s.fail {
		return errors.New("publish unavailable")
	}
	s.published = envelope
	return nil
}

type crashOutbox struct {
	claims int
	marks  int
}

func (s *crashOutbox) ClaimOutbox(context.Context, string, int, string, time.Time) ([]messaging.OutboxRecord, error) {
	s.claims++
	return []messaging.OutboxRecord{{TenantID: "tenant", OutboxID: "outbox", AggregateID: "request", Version: uint64(s.claims)}}, nil
}
func (*crashOutbox) RenewOutboxClaim(_ context.Context, _, _ string, expected uint64, _ string, _ time.Time) (uint64, error) {
	return expected + 1, nil
}
func (s *crashOutbox) MarkPublished(context.Context, string, string, uint64) error {
	s.marks++
	if s.marks == 1 {
		return errors.New("relay crashed before mark persisted")
	}
	return nil
}
func (*crashOutbox) MarkRetry(context.Context, string, string, uint64, time.Time) error { return nil }
func (*relayBrokerStub) Consume(context.Context, broker.ConsumerOptions, func(context.Context, broker.Delivery) error) error {
	return nil
}
func (*relayBrokerStub) Ack(context.Context, broker.Delivery) error { return nil }
func (*relayBrokerStub) Reclaim(context.Context, broker.ReclaimOptions) ([]broker.Delivery, error) {
	return nil, nil
}

func TestDispatchRelayPublishesAuthoritativeEnvelopeThenMarksOutbox(t *testing.T) {
	envelope := relayEnvelope()
	outbox := &outboxStub{records: []messaging.OutboxRecord{{TenantID: "tenant", OutboxID: "outbox", AggregateID: "request", Version: 2}}}
	messages := &relayBrokerStub{}
	relay := DispatchRelay{Outbox: outbox, Tasks: relayTaskStub{envelope: envelope}, Broker: messages, Owner: "relay", ShardCount: 4}
	count, err := relay.RunOnce(context.Background())
	if err != nil || count != 1 || !outbox.published || messages.published != envelope {
		t.Fatalf("count=%d err=%v published=%t envelope=%#v", count, err, outbox.published, messages.published)
	}
}

func TestDispatchRelaySchedulesRetryWhenPublishFails(t *testing.T) {
	envelope := relayEnvelope()
	outbox := &outboxStub{records: []messaging.OutboxRecord{{TenantID: "tenant", OutboxID: "outbox", AggregateID: "request", Version: 2}}}
	relay := DispatchRelay{Outbox: outbox, Tasks: relayTaskStub{envelope: envelope}, Broker: &relayBrokerStub{fail: true}, Owner: "relay", ShardCount: 4}
	count, err := relay.RunOnce(context.Background())
	if err == nil || count != 0 || !outbox.retried || outbox.published {
		t.Fatalf("count=%d err=%v retried=%t published=%t", count, err, outbox.retried, outbox.published)
	}
}

func TestDispatchRelayRenewsClaimDuringSlowPublish(t *testing.T) {
	envelope := relayEnvelope()
	outbox := &outboxStub{records: []messaging.OutboxRecord{{TenantID: "tenant", OutboxID: "outbox", AggregateID: "request", Version: 2}}}
	messages := &relayBrokerStub{delay: 25 * time.Millisecond}
	relay := DispatchRelay{Outbox: outbox, Tasks: relayTaskStub{envelope: envelope}, Broker: messages, Owner: "relay", ShardCount: 4, ClaimTTL: 30 * time.Millisecond, ClaimRenewInterval: 5 * time.Millisecond}
	if count, err := relay.RunOnce(context.Background()); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if outbox.renewals < 2 || outbox.markVersion <= 2 {
		t.Fatalf("renewals=%d markVersion=%d", outbox.renewals, outbox.markVersion)
	}
}

func TestDispatchRelayRepublishesAfterPublishMarkCrash(t *testing.T) {
	envelope := relayEnvelope()
	outbox := &crashOutbox{}
	messages := &relayBrokerStub{}
	relay := DispatchRelay{Outbox: outbox, Tasks: relayTaskStub{envelope: envelope}, Broker: messages, Owner: "relay", ShardCount: 4}
	if count, err := relay.RunOnce(context.Background()); err == nil || count != 0 {
		t.Fatalf("first count=%d err=%v", count, err)
	}
	if count, err := relay.RunOnce(context.Background()); err != nil || count != 1 {
		t.Fatalf("second count=%d err=%v", count, err)
	}
	if messages.calls != 2 || outbox.marks != 2 {
		t.Fatalf("publish calls=%d marks=%d", messages.calls, outbox.marks)
	}
}

func relayEnvelope() runtime.ExecutionEnvelope {
	return runtime.ExecutionEnvelope{
		SchemaVersion: 1, TenantID: "tenant", TenantVersion: 1, AgentAppID: "app", AgentAppVersion: 1,
		AgentAppRevision: 1, AgentContentDigest: "digest", ConfigVersion: 1, PolicyVersion: 1,
		RequestID: "request", SessionID: "session", UserID: "user", Channel: "fake", InputSeq: 1,
		PayloadRef: "payload://request", CreatedAt: time.Now().UTC(),
	}
}
