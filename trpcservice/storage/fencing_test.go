package storage

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryStateStoreSessionExecutionFenceRejectsStaleOwner(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	now := time.Date(2026, 9, 10, 1, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	firstLease, err := store.AcquireSessionExecutionLease(ctx, "tenant-1", "tenant-1/sess-1", "run-1", time.Minute)
	if err != nil || firstLease.FencingToken != 1 {
		t.Fatalf("first lease = %+v, %v; want token 1", firstLease, err)
	}
	first := fencedRecord("msg-1", "trace-1", firstLease.FencingToken)
	if _, err := store.RecordExecution(ctx, first); err != nil {
		t.Fatalf("first RecordExecution() error = %v", err)
	}
	if err := store.ReleaseSessionExecutionLease(ctx, firstLease); err != nil {
		t.Fatalf("release first lease: %v", err)
	}

	secondLease, err := store.AcquireSessionExecutionLease(ctx, "tenant-1", "tenant-1/sess-1", "run-2", time.Minute)
	if err != nil || secondLease.FencingToken != 2 {
		t.Fatalf("second lease = %+v, %v; want token 2", secondLease, err)
	}

	stale := fencedRecord("msg-stale", "trace-stale", firstLease.FencingToken)
	if _, err := store.RecordExecution(ctx, stale); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("stale RecordExecution() error = %v, want ErrStaleFencingToken", err)
	}
	current := fencedRecord("msg-2", "trace-2", secondLease.FencingToken)
	if _, err := store.RecordExecution(ctx, current); err != nil {
		t.Fatalf("current RecordExecution() error = %v", err)
	}

	session, err := store.GetSession(ctx, "tenant-1", "tenant-1/sess-1")
	if err != nil {
		t.Fatalf("GetSession() error = %v", err)
	}
	if session.Revision != 2 {
		t.Fatalf("session revision = %d, want two committed state revisions", session.Revision)
	}
}

func TestMemoryStateStoreExpiredSessionExecutionFenceCannotCommit(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStateStore()
	now := time.Date(2026, 9, 10, 1, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	lease, err := store.AcquireSessionExecutionLease(ctx, "tenant-1", "tenant-1/sess-1", "run-1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if _, err := store.RecordExecution(ctx, fencedRecord("msg-expired", "trace-expired", lease.FencingToken)); !errors.Is(err, ErrStaleFencingToken) {
		t.Fatalf("expired RecordExecution() error = %v, want ErrStaleFencingToken", err)
	}
}

func fencedRecord(messageID, traceID string, token uint64) ExecutionRecord {
	return ExecutionRecord{
		TenantID: "tenant-1", AppCode: "app-1", SessionKey: "tenant-1/sess-1",
		MessageID: messageID, Channel: "web", BindingID: "web-console", TraceID: traceID,
		Action: "chat", Result: "ok", OutboxType: "reply", OutboxPayload: []byte(`{}`),
		FencingToken: token,
	}
}
