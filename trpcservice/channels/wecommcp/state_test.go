package wecommcp

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoryStateContract(t *testing.T) { testStateContract(t, NewMemoryStore()) }

func testStateContract(t *testing.T, store Store) {
	t.Helper()
	ctx := context.Background()
	key := PollKey{"tutorial-tenant", "tutorial-http-binding", "chat-fingerprint"}
	start := time.Now().UTC().Truncate(time.Second)
	cp, err := store.Checkpoint(ctx, key, "config", start)
	if err != nil || cp.Version != 1 || !cp.Through.Equal(start) {
		t.Fatalf("initial checkpoint: %v", err)
	}
	if _, err := store.Checkpoint(ctx, key, "other-config", start); !errors.Is(err, ErrStateConflict) {
		t.Fatal("changed scope silently reset checkpoint")
	}
	if err := store.Advance(ctx, key, cp, start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := store.Advance(ctx, key, cp, start.Add(2*time.Minute)); !errors.Is(err, ErrStateConflict) {
		t.Fatal("stale checkpoint writer succeeded")
	}
	newCP, _ := store.Checkpoint(ctx, key, "config", start)
	if err := store.Advance(ctx, key, newCP, start); !errors.Is(err, ErrStateConflict) {
		t.Fatal("checkpoint regressed")
	}
	if err := store.MarkSeen(ctx, key, "message"); err != nil {
		t.Fatal(err)
	}
	if seen, err := store.Seen(ctx, key, "message"); err != nil || !seen {
		t.Fatal("seen message lost")
	}
	other := key
	other.ChatHash = "other-chat"
	if seen, err := store.Seen(ctx, other, "message"); err != nil || seen {
		t.Fatal("chat scope leaked")
	}
	other = key
	other.TenantID = "other-tenant"
	if seen, err := store.Seen(ctx, other, "message"); err != nil || seen {
		t.Fatal("tenant scope leaked")
	}
	delivery := DeliveryKey{key.TenantID, key.BindingID, "outbound-part-1"}
	var winners atomic.Int32
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, owner, err := store.BeginDelivery(ctx, delivery, "input")
			if err != nil {
				t.Error(err)
			}
			if owner {
				winners.Add(1)
			}
		}()
	}
	wg.Wait()
	if winners.Load() != 1 {
		t.Fatal("multiple send owners")
	}
	if _, _, err := store.BeginDelivery(ctx, delivery, "changed-input"); !errors.Is(err, ErrStateConflict) {
		t.Fatal("conflicting send input accepted")
	}
	if err := store.FinishDelivery(ctx, delivery, "input", "sent"); err != nil {
		t.Fatal(err)
	}
	previous, owner, err := store.BeginDelivery(ctx, delivery, "input")
	if err != nil || owner || previous.Status != "sent" {
		t.Fatal("successful send became retryable")
	}
	if err := store.FinishDelivery(ctx, delivery, "input", "unknown"); !errors.Is(err, ErrStateConflict) {
		t.Fatal("terminal delivery overwritten")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.Checkpoint(cancelled, key, "config", start); err == nil {
		t.Fatal("cancelled read succeeded")
	}
}
