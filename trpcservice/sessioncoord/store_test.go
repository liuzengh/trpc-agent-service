package sessioncoord

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

func sessionKey() gateway.SessionKey {
	return gateway.SessionKey{TenantID: "tenant", AppID: "app", UserID: "user", SessionID: "session"}
}

func TestFenceGuardSerializesTakeoverWithRuntimeMutation(t *testing.T) {
	store := NewMemoryWriteStore()
	key := sessionKey()
	if err := store.AdvanceFence(context.Background(), key, 1); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	mutationDone := make(chan error, 1)
	go func() {
		mutationDone <- store.WithFence(context.Background(), key, 1, func(context.Context) error {
			close(started)
			<-release
			return nil
		})
	}()
	<-started
	takeoverDone := make(chan error, 1)
	go func() { takeoverDone <- store.AdvanceFence(context.Background(), key, 2) }()
	select {
	case err := <-takeoverDone:
		t.Fatalf("takeover crossed active fenced mutation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-mutationDone; err != nil {
		t.Fatal(err)
	}
	if err := <-takeoverDone; err != nil {
		t.Fatal(err)
	}
	if err := store.WithFence(context.Background(), key, 1, func(context.Context) error { return nil }); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("old fence mutation error=%v", err)
	}
}

func TestValidateTurnRejectsLaterSequenceBeforeRunner(t *testing.T) {
	store := NewMemoryWriteStore()
	key := sessionKey()
	if err := store.ValidateTurn(context.Background(), key, 2); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("later turn error=%v", err)
	}
	if err := store.ValidateTurn(context.Background(), key, 1); err != nil {
		t.Fatal(err)
	}
}
func turn(key gateway.SessionKey, fence uint64, inbox string) TurnWrite {
	eventID := "event-" + inbox
	seq := uint64(1)
	if inbox == "two" || inbox == "stale" {
		seq = 2
	}
	return TurnWrite{Key: key, Fence: fence, InboxSeq: seq, InboxID: inbox, EventID: eventID, EventType: "assistant", Payload: "reply", StateDelta: map[string]string{"last": inbox}}
}
func outbound(key gateway.SessionKey, inbox string) gateway.OutboundMessage {
	return gateway.OutboundMessage{TenantID: key.TenantID, AppID: key.AppID, BindingID: "http", OutboxID: "out-" + inbox, DedupeKey: "reply-" + inbox, UserID: key.UserID, SessionID: key.SessionID, SourceInboxID: inbox, SourceEventID: "event-" + inbox, Text: "reply"}
}

func TestStaleOwnerCannotPartiallyWrite(t *testing.T) {
	store := NewMemoryWriteStore()
	coordinator, _ := NewCoordinator(store)
	now := time.Unix(100, 0)
	coordinator.now = func() time.Time { return now }
	key := sessionKey()
	first, err := coordinator.Acquire(context.Background(), key, "a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitTurn(context.Background(), turn(key, first.Token, "one")); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	second, err := coordinator.Acquire(context.Background(), key, "b", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitTurn(context.Background(), turn(key, first.Token, "stale")); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale commit err=%v", err)
	}
	if err := store.PublishSummary(context.Background(), key, first.Token, Summary{Version: 1, CutoffEventSeq: 1, Content: "bad"}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale summary err=%v", err)
	}
	if err := store.UpsertMemory(context.Background(), key, first.Token, Memory{MemoryID: "bad", SourceEventID: "bad", SourceEventSeq: 1, Version: 1}); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale memory err=%v", err)
	}
	if err := store.PublishOutbox(context.Background(), key, first.Token, outbound(key, "one")); !errors.Is(err, ErrStaleFence) {
		t.Fatalf("stale outbox err=%v", err)
	}
	head, events, summary, memories := store.Snapshot(key)
	if head.LastEventSeq != 1 || len(events) != 1 || summary != nil || len(memories) != 0 {
		t.Fatalf("partial stale write: %+v %v %v %v", head, events, summary, memories)
	}
	if _, err := store.CommitTurn(context.Background(), turn(key, second.Token, "two")); err != nil {
		t.Fatal(err)
	}
}

func TestAtomicIdempotencySummaryAndMemory(t *testing.T) {
	store := NewMemoryWriteStore()
	coordinator, _ := NewCoordinator(store)
	key := sessionKey()
	lease, _ := coordinator.Acquire(context.Background(), key, "worker", time.Minute)
	seq, err := store.CommitTurn(context.Background(), turn(key, lease.Token, "one"))
	if err != nil || seq != 1 {
		t.Fatalf("seq=%d err=%v", seq, err)
	}
	seq, err = store.CommitTurn(context.Background(), turn(key, lease.Token, "one"))
	if err != nil || seq != 1 {
		t.Fatalf("retry seq=%d err=%v", seq, err)
	}
	if err := store.PublishSummary(context.Background(), key, lease.Token, Summary{Version: 1, CutoffEventSeq: 1, Content: "summary"}); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishSummary(context.Background(), key, lease.Token, Summary{Version: 1, CutoffEventSeq: 1, Content: "summary"}); err != nil {
		t.Fatalf("idempotent summary: %v", err)
	}
	if err := store.PublishSummary(context.Background(), key, lease.Token, Summary{Version: 2, CutoffEventSeq: 1, Content: "stale"}); err == nil {
		t.Fatal("stale cutoff accepted")
	}
	memory := Memory{MemoryID: "memory-1", SourceEventID: "event-one", SourceEventSeq: 1, Version: 1, Status: "active", Content: "fact"}
	if err := store.UpsertMemory(context.Background(), key, lease.Token, memory); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishOutbox(context.Background(), key, lease.Token, outbound(key, "one")); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishOutbox(context.Background(), key, lease.Token, outbound(key, "one")); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertMemory(context.Background(), key, lease.Token, memory); err != nil {
		t.Fatal(err)
	}
	head, events, summary, memories := store.Snapshot(key)
	if head.StateVersion != 1 || len(events) != 1 || summary == nil || len(memories) != 1 {
		t.Fatalf("snapshot=%+v %v %v %v", head, events, summary, memories)
	}
	if _, ok := store.Outbox(key.TenantID, "reply-one"); !ok {
		t.Fatal("outbox not persisted")
	}
}

func TestInboxSequenceRejectsCrossNodeReordering(t *testing.T) {
	store := NewMemoryWriteStore()
	coordinator, _ := NewCoordinator(store)
	key := sessionKey()
	lease, _ := coordinator.Acquire(context.Background(), key, "worker-b", time.Minute)
	if _, err := store.CommitTurn(context.Background(), turn(key, lease.Token, "two")); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("out-of-order error=%v", err)
	}
	head, events, _, _ := store.Snapshot(key)
	if head.LastEventSeq != 0 || head.StateVersion != 0 || len(events) != 0 {
		t.Fatalf("out-of-order write was partial: %+v %v", head, events)
	}
	first := turn(key, lease.Token, "one")
	if _, err := store.CommitTurn(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CommitTurn(context.Background(), turn(key, lease.Token, "two")); err != nil {
		t.Fatal(err)
	}
}
