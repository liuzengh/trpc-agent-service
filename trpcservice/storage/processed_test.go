package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// The done marker gates Stream redeliveries: MarkDone must flip IsDone and
// carry the shared 24h dedup TTL.
func TestProcessedMarkerRoundTrip(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	m := NewProcessedMarker(rdb)

	channel := "wecom"
	binding := fmt.Sprintf("bind-%d", time.Now().UnixNano())
	msgID := "msg-1"
	key := doneKey(channel, binding, msgID)
	t.Cleanup(func() { rdb.Del(ctx, key) })

	// Format is part of the operator-facing key layout.
	if got := doneKey("c", "b", "m"); got != "done:c:b:m" {
		t.Fatalf("unexpected done key format: %q", got)
	}

	ok, err := m.IsDone(ctx, channel, binding, msgID)
	if err != nil || ok {
		t.Fatalf("unmarked message must not be done (ok=%v err=%v)", ok, err)
	}

	if err := m.MarkDone(ctx, channel, binding, msgID); err != nil {
		t.Fatal(err)
	}
	ok, err = m.IsDone(ctx, channel, binding, msgID)
	if err != nil || !ok {
		t.Fatalf("marked message must be done (ok=%v err=%v)", ok, err)
	}

	// The marker expires: a redelivery window later the key is gone.
	ttl, err := rdb.TTL(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > DedupTTL {
		t.Fatalf("done key TTL must be within (0, %s], got %s", DedupTTL, ttl)
	}

	// A different binding is a different message space: not marked.
	ok, err = m.IsDone(ctx, channel, binding+"-other", msgID)
	if err != nil || ok {
		t.Fatalf("other binding must stay unmarked (ok=%v err=%v)", ok, err)
	}
}
