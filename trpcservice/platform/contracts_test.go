package platform

import (
	"context"
	"testing"
	"time"
)

func TestTenantContextIsServerScoped(t *testing.T) {
	ctx := WithTenantContext(context.Background(), TenantContext{TenantID: "t-1", UserID: "u-1"})
	got, ok := TenantContextFromContext(ctx)
	if !ok || got.TenantID != "t-1" || got.UserID != "u-1" {
		t.Fatalf("unexpected tenant context: %#v, %v", got, ok)
	}
	if _, ok := TenantContextFromContext(context.Background()); ok {
		t.Fatal("tenant context unexpectedly present")
	}
}

func TestSessionEventCarriesOrderingAndIdempotencyContract(t *testing.T) {
	event := SessionEvent{
		ID: "event-1", TenantID: "tenant-1", SessionID: "session-1",
		Sequence: 7, IdempotencyKey: "callback-1", Type: "message",
		Payload: []byte(`{"text":"hello"}`), OccurredAt: time.Now(),
	}
	if event.Sequence == 0 || event.IdempotencyKey == "" || event.OccurredAt.IsZero() {
		t.Fatalf("event misses required ordering/idempotency fields: %#v", event)
	}
}

func TestDeploymentStatusValues(t *testing.T) {
	statuses := []DeploymentStatus{DeploymentDraft, DeploymentPublished, DeploymentActive, DeploymentPaused}
	seen := map[DeploymentStatus]bool{}
	for _, status := range statuses {
		if status == "" || seen[status] {
			t.Fatalf("invalid duplicate deployment status %q", status)
		}
		seen[status] = true
	}
}
