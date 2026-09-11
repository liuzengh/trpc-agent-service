package storage

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestMemoryOutboxDeliveryDefersRetryUntilBackoffExpires(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	clock := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return clock }
	event, err := store.RecordExecution(context.Background(), ExecutionRecord{
		TenantID:        "tenant-a",
		AppCode:         "support",
		SessionKey:      "tenant-a/support/web/session-1",
		MessageID:       "request-1",
		Channel:         "web",
		BindingID:       "web-console",
		TraceID:         "trace-1",
		Action:          "agent_reply",
		Result:          "queued",
		AuditDetail:     "reply",
		OutboxType:      "channel_reply",
		OutboxPayload:   []byte(`{"channel":"web","conversation_id":"session-1","text":"reply"}`),
		OutboxRequestID: "request-1",
	})
	if err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}

	claimed, err := store.ClaimPendingOutbox(context.Background(), "tenant-a", "dispatcher-a", time.Minute, 1)
	if err != nil || len(claimed) != 1 || claimed[0].ID != event.ID {
		t.Fatalf("first ClaimPendingOutbox() = %#v, %v; want event %q", claimed, err, event.ID)
	}
	if err := store.FailOutboxDelivery(context.Background(), "tenant-a", event.ID, "dispatcher-a", errors.New("provider unavailable")); err != nil {
		t.Fatalf("FailOutboxDelivery() error = %v", err)
	}

	claimed, err = store.ClaimPendingOutbox(context.Background(), "tenant-a", "dispatcher-b", time.Minute, 1)
	if err != nil {
		t.Fatalf("immediate ClaimPendingOutbox() error = %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("immediate ClaimPendingOutbox() claimed %d events, want retry to be deferred", len(claimed))
	}

	clock = clock.Add(time.Second)
	claimed, err = store.ClaimPendingOutbox(context.Background(), "tenant-a", "dispatcher-b", time.Minute, 1)
	if err != nil || len(claimed) != 1 || claimed[0].DeliveryAttempts != 2 {
		t.Fatalf("backoff-expired ClaimPendingOutbox() = %#v, %v; want second attempt", claimed, err)
	}
}

func TestMemoryOutboxDeliveryRenewalKeepsClaimExclusive(t *testing.T) {
	t.Parallel()

	store := NewMemoryStateStore()
	clock := time.Date(2026, time.August, 29, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return clock }
	event, err := store.RecordExecution(context.Background(), ExecutionRecord{
		TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/web/session-1",
		MessageID: "request-renew", Channel: "web", BindingID: "web-console", TraceID: "trace-renew",
		Action: "agent_reply", Result: "queued", AuditDetail: "reply",
		OutboxType: "channel_reply", OutboxPayload: []byte("{\"channel\":\"web\",\"conversation_id\":\"session-1\",\"text\":\"reply\"}"),
	})
	if err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}
	claimed, err := store.ClaimPendingOutbox(context.Background(), "tenant-a", "dispatcher-a", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimPendingOutbox() = %#v, %v; want one claim", claimed, err)
	}

	clock = clock.Add(30 * time.Second)
	if err := store.RenewOutboxDelivery(context.Background(), "tenant-a", event.ID, "dispatcher-a", time.Minute); err != nil {
		t.Fatalf("RenewOutboxDelivery() error = %v", err)
	}
	clock = clock.Add(45 * time.Second)
	claimed, err = store.ClaimPendingOutbox(context.Background(), "tenant-a", "dispatcher-b", time.Minute, 1)
	if err != nil {
		t.Fatalf("ClaimPendingOutbox() after renewal error = %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("another dispatcher claimed renewed event: %#v", claimed)
	}

	clock = clock.Add(16 * time.Second)
	claimed, err = store.ClaimPendingOutbox(context.Background(), "tenant-a", "dispatcher-b", time.Minute, 1)
	if err != nil || len(claimed) != 1 || claimed[0].DeliveryOwner != "dispatcher-b" {
		t.Fatalf("ClaimPendingOutbox() after renewed lease expiry = %#v, %v; want dispatcher-b claim", claimed, err)
	}
}

func TestMemoryOutboxDeliveryLifecycleAndOwnershipBoundaries(t *testing.T) {
	t.Parallel()
	store := NewMemoryStateStore()
	clock := time.Date(2026, time.September, 11, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return clock }
	event, err := store.RecordExecution(context.Background(), ExecutionRecord{
		TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/session/1",
		MessageID: "message-1", Channel: "web", BindingID: "web-console", TraceID: "trace-1",
		Action: "agent_reply", Result: "queued", OutboxType: "channel_reply.web",
		OutboxPayload: []byte(`{"channel":"web","text":"reply"}`), OutboxRequestID: "request-1",
	})
	if err != nil {
		t.Fatal(err)
	}

	if claimed, err := store.ClaimPendingOutboxByType(context.Background(), "tenant-a", "worker-a", time.Minute, 1, "other"); err != nil || len(claimed) != 0 {
		t.Fatalf("wrong-type claim = %#v, %v", claimed, err)
	}
	claimed, err := store.ClaimPendingOutboxByType(context.Background(), "tenant-a", "worker-a", time.Minute, 1, "channel_reply.web")
	if err != nil || len(claimed) != 1 || claimed[0].ID != event.ID || claimed[0].DeliveryAttempts != 1 {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	if err := store.CompleteOutboxDelivery(context.Background(), "tenant-a", event.ID, "worker-b", "receipt"); err == nil {
		t.Fatal("CompleteOutboxDelivery() accepted a foreign owner")
	}
	if err := store.FailOutboxDelivery(context.Background(), "tenant-a", event.ID, "worker-b", nil); err == nil {
		t.Fatal("FailOutboxDelivery() accepted a foreign owner")
	}
	if err := store.RenewOutboxDelivery(context.Background(), "tenant-a", event.ID, "worker-b", time.Minute); err == nil {
		t.Fatal("RenewOutboxDelivery() accepted a foreign owner")
	}
	if err := store.CompleteOutboxDelivery(context.Background(), "tenant-a", event.ID, "worker-a", "receipt-1"); err != nil {
		t.Fatalf("CompleteOutboxDelivery() error = %v", err)
	}
	found, err := store.FindOutboxByRequestID(context.Background(), "tenant-a", "request-1")
	if err != nil || found.DeliveryReceipt != "receipt-1" || found.DeliveredAt == nil || found.DeliveryOwner != "" || found.LeaseExpiresAt != nil {
		t.Fatalf("FindOutboxByRequestID() = %#v, %v", found, err)
	}
	if _, err := store.FindOutboxByRequestID(context.Background(), "tenant-a", "missing"); !errors.Is(err, ErrOutboxEventNotFound) {
		t.Fatalf("missing request error = %v", err)
	}
	if err := store.CompleteOutboxDelivery(context.Background(), "tenant-a", event.ID, "worker-a", "receipt-2"); err == nil {
		t.Fatal("completed event accepted a second completion")
	}
}

func TestMemoryOutboxRetentionPurgesOnlyOldDeliveredEventsInBatches(t *testing.T) {
	t.Parallel()
	store := NewMemoryStateStore()
	clock := time.Date(2026, time.September, 11, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return clock }
	record := func(messageID string) OutboxEvent {
		t.Helper()
		event, err := store.RecordExecution(context.Background(), ExecutionRecord{
			TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/session/1",
			MessageID: messageID, Channel: "web", BindingID: "web-console", TraceID: "trace-" + messageID,
			Action: "agent_reply", Result: "queued", OutboxType: "channel_reply.web",
			OutboxPayload: []byte(`{"channel":"web","text":"reply"}`), OutboxRequestID: messageID,
		})
		if err != nil {
			t.Fatal(err)
		}
		return event
	}
	oldOne := record("old-1")
	oldTwo := record("old-2")
	pending := record("pending")
	claimed, err := store.ClaimPendingOutbox(context.Background(), "tenant-a", "worker", time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []OutboxEvent{oldOne, oldTwo} {
		var ownerEvent *OutboxEvent
		for index := range claimed {
			if claimed[index].ID == event.ID {
				ownerEvent = &claimed[index]
				break
			}
		}
		if ownerEvent == nil {
			t.Fatalf("event %q was not claimed", event.ID)
		}
		if err := store.CompleteOutboxDelivery(context.Background(), "tenant-a", event.ID, ownerEvent.DeliveryOwner, "receipt"); err != nil {
			t.Fatal(err)
		}
	}
	clock = clock.Add(8 * 24 * time.Hour)
	deleted, err := store.PurgeDeliveredOutboxBefore(context.Background(), "tenant-a", clock.Add(-7*24*time.Hour), 1)
	if err != nil || deleted != 1 {
		t.Fatalf("first purge = %d, %v", deleted, err)
	}
	deleted, err = store.PurgeDeliveredOutboxBefore(context.Background(), "tenant-a", clock.Add(-7*24*time.Hour), 10)
	if err != nil || deleted != 1 {
		t.Fatalf("second purge = %d, %v", deleted, err)
	}
	if _, err := store.FindOutboxByRequestID(context.Background(), "tenant-a", pending.RequestID); err != nil {
		t.Fatalf("pending event was purged: %v", err)
	}
}

func TestMemoryFindOutboxByRequestIDsFiltersDeduplicatesAndSorts(t *testing.T) {
	t.Parallel()
	store := NewMemoryStateStore()
	clock := time.Date(2026, time.September, 11, 1, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return clock }
	record := func(tenantID, messageID, requestID, text string) OutboxEvent {
		t.Helper()
		event, err := store.RecordExecution(context.Background(), ExecutionRecord{
			TenantID: tenantID, AppCode: "support", SessionKey: tenantID + "/support/session/1",
			MessageID: messageID, Channel: "web", BindingID: "web-console", TraceID: "trace-" + messageID,
			Action: "agent_reply", Result: "queued", OutboxType: "channel_reply.web",
			OutboxPayload: []byte(`{"channel":"web","text":"` + text + `"}`), OutboxRequestID: requestID,
		})
		if err != nil {
			t.Fatal(err)
		}
		clock = clock.Add(time.Second)
		return event
	}
	first := record("tenant-a", "message-1", "request-1", "first")
	second := record("tenant-a", "message-2", "request-2", "second")
	_ = record("tenant-b", "message-3", "request-1", "foreign")

	found, err := store.FindOutboxByRequestIDs(context.Background(), "tenant-a", []string{" request-2 ", "request-1", "request-2", "", "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 2 || found[0].ID != first.ID || found[1].ID != second.ID {
		t.Fatalf("FindOutboxByRequestIDs() = %#v", found)
	}
	found[0].Payload[0] = 'X'
	again, err := store.FindOutboxByRequestIDs(context.Background(), "tenant-a", []string{"request-1"})
	if err != nil || len(again) != 1 || len(again[0].Payload) == 0 || again[0].Payload[0] == 'X' {
		t.Fatalf("stored outbox mutated through batch result: %#v, %v", again, err)
	}
	if empty, err := store.FindOutboxByRequestIDs(context.Background(), "tenant-a", []string{" ", ""}); err != nil || len(empty) != 0 {
		t.Fatalf("empty batch = %#v, %v", empty, err)
	}
	if _, err := store.FindOutboxByRequestIDs(context.Background(), "", []string{"request-1"}); err == nil {
		t.Fatal("missing tenant accepted")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.FindOutboxByRequestIDs(cancelled, "tenant-a", []string{"request-1"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled batch error = %v", err)
	}
}

func TestPostgresFindOutboxByRequestIDsValidatesBeforeQuery(t *testing.T) {
	store := closedPostgresStateStore(t)
	if _, err := store.FindOutboxByRequestIDs(context.Background(), "", []string{"request-1"}); err == nil {
		t.Fatal("missing tenant accepted")
	}
	if got, err := store.FindOutboxByRequestIDs(context.Background(), "tenant-a", []string{"", "  "}); err != nil || len(got) != 0 {
		t.Fatalf("empty request IDs = %#v, %v", got, err)
	}
	if _, err := store.FindOutboxByRequestIDs(context.Background(), "tenant-a", []string{" request-1 ", "request-1", "request-2"}); err == nil || !strings.Contains(err.Error(), "find outbox events by request IDs") {
		t.Fatalf("closed database error = %v", err)
	}
}

func TestMemoryOutboxDeliveryValidatesClaimsAndContext(t *testing.T) {
	t.Parallel()
	store := NewMemoryStateStore()
	for _, test := range []struct {
		name   string
		tenant string
		owner  string
		lease  time.Duration
		limit  int
	}{
		{name: "tenant", owner: "worker", lease: time.Minute, limit: 1},
		{name: "owner", tenant: "tenant-a", lease: time.Minute, limit: 1},
		{name: "lease", tenant: "tenant-a", owner: "worker", limit: 1},
		{name: "limit", tenant: "tenant-a", owner: "worker", lease: time.Minute},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := store.ClaimPendingOutbox(context.Background(), test.tenant, test.owner, test.lease, test.limit); err == nil {
				t.Fatal("ClaimPendingOutbox() error = nil")
			}
		})
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.ClaimPendingOutbox(cancelled, "tenant-a", "worker", time.Minute, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled claim error = %v", err)
	}
	if _, err := store.FindOutboxByRequestID(cancelled, "tenant-a", "request"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled find error = %v", err)
	}
	if err := store.CompleteOutboxDelivery(cancelled, "tenant-a", "event", "worker", "receipt"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled complete error = %v", err)
	}
	if err := store.FailOutboxDelivery(cancelled, "tenant-a", "event", "worker", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled fail error = %v", err)
	}
}

func TestMemoryOutboxDeliveryFailureRecordsSafeRetryState(t *testing.T) {
	t.Parallel()
	store := NewMemoryStateStore()
	clock := time.Date(2026, time.September, 11, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return clock }
	event, err := store.RecordExecution(context.Background(), ExecutionRecord{
		TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/session/2",
		MessageID: "message-2", Channel: "telegram", BindingID: "support-bot", TraceID: "trace-2",
		Action: "agent_reply", Result: "queued", OutboxType: "channel_reply.telegram",
		OutboxPayload: []byte(`{"channel":"telegram","text":"reply"}`), OutboxRequestID: "request-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimPendingOutbox(context.Background(), "tenant-a", "worker-a", time.Minute, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	if err := store.FailOutboxDelivery(context.Background(), "tenant-a", event.ID, "worker-a", errors.New("temporary provider error")); err != nil {
		t.Fatal(err)
	}
	found, err := store.FindOutboxByRequestID(context.Background(), "tenant-a", "request-2")
	if err != nil {
		t.Fatal(err)
	}
	if found.DeliveryOwner != "" || found.LeaseExpiresAt != nil || found.LastDeliveryError != "temporary provider error" || !found.AvailableAt.After(clock) {
		t.Fatalf("failed delivery state = %#v", found)
	}
}
