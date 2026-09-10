package audit_test

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	auditmemory "github.com/liuzengh/trpc-agent-service/trpcservice/audit/inmemory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type relayOutbox struct {
	messaging.OutboxStore
	record    messaging.OutboxRecord
	published int
}

func (s *relayOutbox) ClaimOutbox(context.Context, string, int, string, time.Time) ([]messaging.OutboxRecord, error) {
	if s.published != 0 {
		return nil, nil
	}
	s.record.Version++
	return []messaging.OutboxRecord{s.record}, nil
}
func (s *relayOutbox) RenewOutboxClaim(context.Context, string, string, uint64, string, time.Time) (uint64, error) {
	s.record.Version++
	return s.record.Version, nil
}
func (s *relayOutbox) MarkPublished(context.Context, string, string, uint64) error {
	s.published++
	return nil
}
func (s *relayOutbox) MarkRetry(context.Context, string, string, uint64, time.Time) error { return nil }

type fixedResolver struct{ event audit.Event }

func (r fixedResolver) ResolveAuditEvent(context.Context, messaging.OutboxRecord) (audit.Event, error) {
	return r.event, nil
}

type alertObserver struct{ count int }

func (o *alertObserver) ObserveQuarantineAlert() { o.count++ }

func TestRelayExportsThenMarksOutbox(t *testing.T) {
	record := messaging.OutboxRecord{TenantID: "tenant", OutboxID: "outbox", Kind: "audit", AggregateID: "request",
		IdempotencyKey: "governance:decision", PayloadRef: "governance://tenant/decision", CreatedAt: time.Unix(20, 0).UTC()}
	id, _ := audit.StableID(record.TenantID, record.OutboxID)
	source := &relayOutbox{record: record}
	sink := auditmemory.New()
	value := audit.Event{SchemaVersion: 1, AuditID: id, TenantID: "tenant", RequestID: "request", Action: "governance", Decision: "deny", OccurredAt: record.CreatedAt}
	worker := audit.Relay{Base: relay.Base{Outbox: source, Kind: "audit", Owner: "owner"}, Resolver: fixedResolver{event: value}, Sink: sink}
	count, err := worker.RunOnce(context.Background())
	if err != nil || count != 1 || source.published != 1 || len(sink.Events("tenant")) != 1 {
		t.Fatalf("count=%d published=%d events=%d err=%v", count, source.published, len(sink.Events("tenant")), err)
	}
}

func TestRelayObservesQuarantineOnlyAfterDurableExport(t *testing.T) {
	record := messaging.OutboxRecord{TenantID: "tenant", OutboxID: "quarantine-outbox", Kind: "audit", AggregateID: "artifact",
		IdempotencyKey: "artifact-quarantine", PayloadRef: "artifact-quarantine://tenant/upload/artifact/2", CreatedAt: time.Unix(20, 0).UTC()}
	id, _ := audit.StableID(record.TenantID, record.OutboxID)
	source := &relayOutbox{record: record}
	observer := &alertObserver{}
	value := audit.Event{SchemaVersion: 1, AuditID: id, TenantID: "tenant", Action: "artifact.quarantine", Decision: "alert",
		ErrorType: "version_mismatch", ResourceRefs: []string{record.PayloadRef}, OccurredAt: record.CreatedAt}
	worker := audit.Relay{Base: relay.Base{Outbox: source, Kind: "audit", Owner: "owner"}, Resolver: fixedResolver{event: value},
		Sink: auditmemory.New(), Alerts: observer}
	if count, err := worker.RunOnce(context.Background()); err != nil || count != 1 || observer.count != 1 || source.published != 1 {
		t.Fatalf("count=%d alerts=%d published=%d err=%v", count, observer.count, source.published, err)
	}
}
