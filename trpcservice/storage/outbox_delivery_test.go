package storage

import (
	"context"
	"errors"
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
