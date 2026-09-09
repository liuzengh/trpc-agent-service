package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestDeduper(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	msgID := fmt.Sprintf("dedup-test-%d", time.Now().UnixNano())
	key := fmt.Sprintf("dedup:mock::%s", msgID)
	t.Cleanup(func() { rdb.Del(ctx, key) })

	d := NewDeduper(rdb)

	first, err := d.Check(ctx, "mock", "", msgID)
	if err != nil {
		t.Fatal(err)
	}
	if !first {
		t.Error("first arrival should pass")
	}

	// Concurrent-style duplicates: same msg_id must be rejected.
	for i := 0; i < 3; i++ {
		first, err := d.Check(ctx, "mock", "", msgID)
		if err != nil {
			t.Fatal(err)
		}
		if first {
			t.Fatalf("duplicate %d should be rejected", i)
		}
	}

	// The key carries the TTL covering the IM redelivery window.
	ttl, err := rdb.TTL(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > DedupTTL {
		t.Errorf("ttl = %v, want (0, %v]", ttl, DedupTTL)
	}

	// A different channel namespace must not collide.
	other, err := d.Check(ctx, "wecom", "", msgID)
	if err != nil {
		t.Fatal(err)
	}
	if !other {
		t.Error("same msg_id on another channel should pass")
	}
	rdb.Del(ctx, fmt.Sprintf("dedup:wecom::%s", msgID))
}

// Forget reopens a message for redelivery. This is what keeps a failed
// delivery retryable: without it, a failure after Check (a failed enqueue)
// leaves the key behind and the IM's redelivery is dropped as a duplicate for
// the whole TTL.
func TestDeduperForget(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	msgID := fmt.Sprintf("dedup-forget-%d", time.Now().UnixNano())
	key := fmt.Sprintf("dedup:mock::%s", msgID)
	t.Cleanup(func() { rdb.Del(ctx, key) })

	d := NewDeduper(rdb)
	if first, err := d.Check(ctx, "mock", "", msgID); err != nil || !first {
		t.Fatalf("first arrival should pass, got first=%v err=%v", first, err)
	}
	if err := d.Forget(ctx, "mock", "", msgID); err != nil {
		t.Fatal(err)
	}

	first, err := d.Check(ctx, "mock", "", msgID)
	if err != nil {
		t.Fatal(err)
	}
	if !first {
		t.Fatal("message must pass again after Forget")
	}
	// Forgetting only touches its own key namespace.
	if n, err := rdb.Exists(ctx, "dedup:wecom::"+msgID).Result(); err != nil || n != 0 {
		t.Fatalf("other channel key must be untouched, n=%d err=%v", n, err)
	}
}

// The dedup key carries the binding dimension: msg_id uniqueness is
// guaranteed by the IM per corp/app only, so the same numeric ID can
// legitimately arrive on two bindings of one channel.
func TestDeduperBindingScope(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	msgID := fmt.Sprintf("dedup-scope-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		rdb.Del(ctx, dedupKey("mock", "b1", msgID))
		rdb.Del(ctx, dedupKey("mock", "b2", msgID))
	})

	d := NewDeduper(rdb)
	first, err := d.Check(ctx, "mock", "b1", msgID)
	if err != nil || !first {
		t.Fatalf("first arrival on binding b1 should pass: first=%v err=%v", first, err)
	}
	other, err := d.Check(ctx, "mock", "b2", msgID)
	if err != nil || !other {
		t.Fatalf("same msg_id on another binding must pass: first=%v err=%v", other, err)
	}
	dup, err := d.Check(ctx, "mock", "b1", msgID)
	if err != nil || dup {
		t.Fatalf("redelivery on the same binding must be dropped: dup=%v err=%v", dup, err)
	}
}

// The wxkf re-pull window after a lost cursor is three days, so that channel's
// dedup key must outlive it (four days, one day of headroom); other channels
// keep the 24h redelivery window. sent: and done: markers share the same
// per-channel TTL or the re-pulled history would be re-processed/re-answered.
func TestDeduperChannelTTL(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	suffix := fmt.Sprintf("ttl-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		rdb.Del(ctx, dedupKey("wxkf", "", suffix))
		rdb.Del(ctx, dedupKey("wecom", "", suffix))
		rdb.Del(ctx, sentKey("wxkf", "", suffix))
		rdb.Del(ctx, doneKey("wxkf", "", suffix))
	})

	d := NewDeduper(rdb)
	if _, err := d.Check(ctx, "wxkf", "", suffix); err != nil {
		t.Fatal(err)
	}
	ttl, err := rdb.TTL(ctx, dedupKey("wxkf", "", suffix)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= DedupTTL || ttl > DedupTTLWxkf {
		t.Errorf("wxkf dedup ttl = %v, want (%v, %v]", ttl, DedupTTL, DedupTTLWxkf)
	}

	if _, err := d.Check(ctx, "wecom", "", suffix); err != nil {
		t.Fatal(err)
	}
	ttl, err = rdb.TTL(ctx, dedupKey("wecom", "", suffix)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > DedupTTL {
		t.Errorf("other channels keep the 24h window, ttl = %v", ttl)
	}

	if err := NewSentMarker(rdb).MarkSent(ctx, "wxkf", "", suffix, "im-1"); err != nil {
		t.Fatal(err)
	}
	ttl, err = rdb.TTL(ctx, sentKey("wxkf", "", suffix)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= DedupTTL || ttl > DedupTTLWxkf {
		t.Errorf("wxkf sent marker ttl = %v, want (%v, %v]", ttl, DedupTTL, DedupTTLWxkf)
	}

	if err := NewProcessedMarker(rdb).MarkDone(ctx, "wxkf", "", suffix); err != nil {
		t.Fatal(err)
	}
	ttl, err = rdb.TTL(ctx, doneKey("wxkf", "", suffix)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= DedupTTL || ttl > DedupTTLWxkf {
		t.Errorf("wxkf done marker ttl = %v, want (%v, %v]", ttl, DedupTTL, DedupTTLWxkf)
	}
}
