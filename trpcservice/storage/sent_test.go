package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSentMarker(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	msgID := fmt.Sprintf("sent-test-%d", time.Now().UnixNano())
	key := sentKey("mock", "", msgID)
	t.Cleanup(func() { rdb.Del(ctx, key) })

	m := NewSentMarker(rdb)

	sent, err := m.IsSent(ctx, "mock", "", msgID)
	if err != nil {
		t.Fatal(err)
	}
	if sent {
		t.Fatal("unmarked message must not report as sent")
	}

	if err := m.MarkSent(ctx, "mock", "", msgID, "im-msg-42"); err != nil {
		t.Fatal(err)
	}

	sent, err = m.IsSent(ctx, "mock", "", msgID)
	if err != nil {
		t.Fatal(err)
	}
	if !sent {
		t.Fatal("marked message must report as sent")
	}

	ttl, err := rdb.TTL(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > DedupTTL {
		t.Errorf("ttl = %v, want (0, %v]", ttl, DedupTTL)
	}
}

// The sent key carries the binding dimension: two tenants' replies with the
// same inbound msg_id must not suppress each other.
func TestSentMarkerBindingScope(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	msgID := fmt.Sprintf("sent-scope-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		rdb.Del(ctx, sentKey("mock", "b1", msgID))
		rdb.Del(ctx, sentKey("mock", "b2", msgID))
	})

	m := NewSentMarker(rdb)
	if err := m.MarkSent(ctx, "mock", "b1", msgID, "im-1"); err != nil {
		t.Fatal(err)
	}
	sent, err := m.IsSent(ctx, "mock", "b2", msgID)
	if err != nil || sent {
		t.Fatalf("binding b2 must not see b1's marker: sent=%v err=%v", sent, err)
	}
}
