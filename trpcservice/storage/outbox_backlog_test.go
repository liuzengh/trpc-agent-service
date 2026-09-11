package storage

import (
	"context"
	"testing"
	"time"
)

func TestMemoryStateStoreReportsOutboxBacklogByType(t *testing.T) {
	now := time.Date(2026, 9, 11, 3, 0, 0, 0, time.UTC)
	store := NewMemoryStateStore()
	store.now = func() time.Time { return now }
	oldest := now.Add(-45 * time.Second)
	recent := now.Add(-5 * time.Second)
	delivered := now.Add(-time.Second)
	store.outbox["web-old"] = OutboxEvent{ID: "web-old", TenantID: "tenant-a", Type: "channel_reply.web", CreatedAt: oldest}
	store.outbox["web-new"] = OutboxEvent{ID: "web-new", TenantID: "tenant-a", Type: "channel_reply.web", CreatedAt: recent}
	store.outbox["telegram"] = OutboxEvent{ID: "telegram", TenantID: "tenant-a", Type: "channel_reply.telegram", CreatedAt: oldest}
	store.outbox["delivered"] = OutboxEvent{ID: "delivered", TenantID: "tenant-a", Type: "channel_reply.web", CreatedAt: oldest, DeliveredAt: &delivered}

	backlog, err := store.OutboxBacklog(context.Background(), "tenant-a", "channel_reply.web")
	if err != nil {
		t.Fatal(err)
	}
	if backlog.Pending != 2 || !backlog.OldestCreatedAt.Equal(oldest) {
		t.Fatalf("backlog = %#v", backlog)
	}
	empty, err := store.OutboxBacklog(context.Background(), "tenant-a", "channel_reply.feishu")
	if err != nil {
		t.Fatal(err)
	}
	if empty.Pending != 0 || !empty.OldestCreatedAt.IsZero() {
		t.Fatalf("empty backlog = %#v", empty)
	}
}
