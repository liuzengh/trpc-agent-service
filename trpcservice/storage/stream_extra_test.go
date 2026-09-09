package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Len backs the gateway's backpressure check.
func TestStreamLen(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	stream := fmt.Sprintf("test:stream:len:%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, stream) })

	s := NewStream(rdb)
	n, err := s.Len(ctx, stream)
	if err != nil || n != 0 {
		t.Fatalf("empty stream must report 0, got %d err=%v", n, err)
	}
	for i := 0; i < 3; i++ {
		if _, err := s.Add(ctx, stream, []byte(fmt.Sprintf("m%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	n, err = s.Len(ctx, stream)
	if err != nil || n != 3 {
		t.Fatalf("stream must report 3 after 3 adds, got %d err=%v", n, err)
	}
}

// Pending exposes the consumer group backlog: zeros for a missing group, the
// pending count plus oldest-idle once messages are read but un-acked.
func TestStreamPendingReportsBacklog(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	stream := fmt.Sprintf("test:stream:pending:%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, stream) })

	s := NewStream(rdb)
	const group = "workers"

	// A missing group reports zeros instead of a NOGROUP error.
	count, idle, err := s.Pending(ctx, stream, group)
	if err != nil || count != 0 || idle != 0 {
		t.Fatalf("missing group must report zeros, got count=%d idle=%s err=%v", count, idle, err)
	}

	if err := s.EnsureGroup(ctx, stream, group); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Add(ctx, stream, []byte("backlog-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Read(ctx, stream, group, "w1", 10, time.Second); err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)

	count, idle, err = s.Pending(ctx, stream, group)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("one un-acked message must be pending, got %d", count)
	}
	if idle <= 0 {
		t.Fatalf("oldest pending message must carry a positive idle, got %s", idle)
	}

	// After the Ack the backlog is drained.
	ids, err := rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: stream, Group: group, Start: "-", End: "+", Count: 10,
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 {
		t.Fatalf("expected the pending entry id, got %+v", ids)
	}
	if err := s.Ack(ctx, stream, group, ids[0].ID); err != nil {
		t.Fatal(err)
	}
	count, _, err = s.Pending(ctx, stream, group)
	if err != nil || count != 0 {
		t.Fatalf("backlog must drain after ack, got count=%d err=%v", count, err)
	}
}

// AutoClaim is the crash-takeover path: a pending message idle longer than
// minIdle is transferred to a new consumer and redelivered.
func TestStreamAutoClaimTakesOver(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	stream := fmt.Sprintf("test:stream:autoclaim:%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, stream) })

	s := NewStream(rdb)
	const group = "workers"
	if err := s.EnsureGroup(ctx, stream, group); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Add(ctx, stream, []byte(`{"take":1}`)); err != nil {
		t.Fatal(err)
	}
	// w1 reads and "crashes" without acking.
	msgs, err := s.Read(ctx, stream, group, "w1", 10, time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("w1 must receive the message: %+v err=%v", msgs, err)
	}
	time.Sleep(150 * time.Millisecond)

	// w2 claims messages idle longer than 100ms.
	taken, err := s.AutoClaim(ctx, stream, group, "w2", 100*time.Millisecond, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(taken) != 1 || string(taken[0].Payload) != `{"take":1}` {
		t.Fatalf("takeover must redeliver the pending message, got %+v", taken)
	}

	// Just-claimed messages are below min-idle again: nothing to reclaim.
	taken, err = s.AutoClaim(ctx, stream, group, "w2", time.Hour, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(taken) != 0 {
		t.Fatalf("messages below min-idle must not be reclaimed, got %+v", taken)
	}
	if err := s.Ack(ctx, stream, group, msgs[0].ID); err != nil {
		t.Fatal(err)
	}
}

// Attempts reads the poison-message counter and IncAttempts bumps it: reading
// must not count, and the counter is namespaced per (stream, group, entry) with
// the shared dedup TTL bound.
func TestStreamAttemptsCounter(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	stream := fmt.Sprintf("test:stream:attempts:%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, stream) })

	s := NewStream(rdb)
	id, err := s.Add(ctx, stream, []byte("poison"))
	if err != nil {
		t.Fatal(err)
	}

	// A message that never failed reads 0, and reading it again must not move
	// the counter — the reaper reads on every takeover.
	for i := 0; i < 3; i++ {
		if n, err := s.Attempts(ctx, stream, "workers", id); err != nil || n != 0 {
			t.Fatalf("an uncounted message must read 0, got %d err=%v", n, err)
		}
	}

	if n, err := s.IncAttempts(ctx, stream, "workers", id); err != nil || n != 1 {
		t.Fatalf("first failure must be 1, got %d err=%v", n, err)
	}
	if n, err := s.IncAttempts(ctx, stream, "workers", id); err != nil || n != 2 {
		t.Fatalf("second failure must be 2, got %d err=%v", n, err)
	}
	if n, err := s.Attempts(ctx, stream, "workers", id); err != nil || n != 2 {
		t.Fatalf("read must report the counted failures, got %d err=%v", n, err)
	}

	// Another group consuming the same stream keeps its own counter.
	if n, err := s.Attempts(ctx, stream, "senders-ws", id); err != nil || n != 0 {
		t.Fatalf("a second group must start at 0, got %d err=%v", n, err)
	}

	key := fmt.Sprintf("retry:%s:workers:%s", stream, id)
	ttl, err := rdb.TTL(ctx, key).Result()
	if err != nil {
		t.Fatal(err)
	}
	if ttl <= 0 || ttl > DedupTTL {
		t.Fatalf("retry counter must carry a TTL within (0, %s], got %s", DedupTTL, ttl)
	}
	t.Cleanup(func() { rdb.Del(ctx, key) })
}

// DeadLetter preserves the message in stream:deadletter and acks the origin
// group so the redelivery loop stops.
func TestStreamDeadLetterMovesAndAcks(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	stream := fmt.Sprintf("test:stream:deadletter:%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, stream) })

	s := NewStream(rdb)
	const group = "workers"
	if err := s.EnsureGroup(ctx, stream, group); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Add(ctx, stream, []byte(`{"order":"poison"}`)); err != nil {
		t.Fatal(err)
	}
	msgs, err := s.Read(ctx, stream, group, "w1", 10, time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("w1 must receive the message: %+v err=%v", msgs, err)
	}
	origin := msgs[0]

	if err := s.DeadLetter(ctx, stream, group, origin); err != nil {
		t.Fatal(err)
	}

	// The origin group no longer holds the message pending.
	count, _, err := s.Pending(ctx, stream, group)
	if err != nil || count != 0 {
		t.Fatalf("dead-lettered message must be acked in the origin group, count=%d err=%v", count, err)
	}

	// The deadletter entry carries the origin coordinates and the payload;
	// scan the newest entries (concurrent writers may interleave).
	entries, err := rdb.XRevRangeN(ctx, StreamDeadletter, "+", "-", 50).Result()
	if err != nil {
		t.Fatal(err)
	}
	var found map[string]interface{}
	for _, e := range entries {
		if e.Values[payloadField] == nil {
			continue
		}
		var payload map[string]interface{}
		if err := json.Unmarshal([]byte(e.Values[payloadField].(string)), &payload); err != nil {
			continue
		}
		if payload["origin_id"] == origin.ID && payload["origin_stream"] == stream {
			found = payload
			t.Cleanup(func() { rdb.XDel(ctx, StreamDeadletter, e.ID) })
			break
		}
	}
	if found == nil {
		t.Fatalf("deadletter entry for origin %s not found among %d recent entries", origin.ID, len(entries))
	}
	if found["payload"] != `{"order":"poison"}` {
		t.Fatalf("deadletter payload mismatch: %v", found["payload"])
	}
}
