package storage

import (
	"context"
	"fmt"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// testRedisAddr and testPGDSN address the services the integration tests
// need; each test skips with the variable named when its service is
// unreachable.
var (
	testRedisAddr = testenv.RedisAddr()
	testPGDSN     = testenv.PGDSN()
)

func redisOrSkip(t *testing.T) *redis.Client {
	t.Helper()
	rdb, err := NewRedis(context.Background(), testRedisAddr)
	if err != nil {
		t.Skipf("redis unavailable (%v) — set TRPC_TEST_REDIS_ADDR (default %s), skipping integration test", err, testRedisAddr)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestNewRedisBadAddr(t *testing.T) {
	// An unreachable address must fail within dialTimeout, not hang.
	if _, err := NewRedis(context.Background(), "localhost:1"); err == nil {
		t.Error("NewRedis to unreachable addr should fail")
	}
}

func TestStreamRoundTrip(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	stream := fmt.Sprintf("test:stream:%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, stream) })

	s := NewStream(rdb)
	const group = "workers"

	if err := s.EnsureGroup(ctx, stream, group); err != nil {
		t.Fatal(err)
	}
	// Consumer-group creation is idempotent: creating twice must not error.
	if err := s.EnsureGroup(ctx, stream, group); err != nil {
		t.Fatal(err)
	}

	id, err := s.Add(ctx, stream, []byte(`{"text":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Error("Add should return entry id")
	}

	msgs, err := s.Read(ctx, stream, group, "w1", 10, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || string(msgs[0].Payload) != `{"text":"hello"}` {
		t.Fatalf("unexpected messages: %+v", msgs)
	}

	if err := s.Ack(ctx, stream, group, msgs[0].ID); err != nil {
		t.Fatal(err)
	}

	// After Ack there is nothing new: the blocking read times out empty.
	msgs, err = s.Read(ctx, stream, group, "w1", 10, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Errorf("expected no messages after ack, got %+v", msgs)
	}
}

// Un-acked messages go pending and can be re-read later (the basis of crash
// takeover).
func TestStreamPendingRedeliver(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	stream := fmt.Sprintf("test:stream:%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, stream) })

	s := NewStream(rdb)
	if err := s.EnsureGroup(ctx, stream, "workers"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Add(ctx, stream, []byte("m1")); err != nil {
		t.Fatal(err)
	}

	// w1 reads but never Acks (simulating a crash).
	if _, err := s.Read(ctx, stream, "workers", "w1", 10, time.Second); err != nil {
		t.Fatal(err)
	}

	// w1 re-reads its own pending with "0"; the message must still be there.
	msgs, err := rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: "workers", Consumer: "w1", Streams: []string{stream, "0"},
	}).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || len(msgs[0].Messages) != 1 {
		t.Fatalf("pending message lost: %+v", msgs)
	}
}

func TestNewPGAndSchema(t *testing.T) {
	pool, err := NewPG(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("postgres unavailable (%v) — set TRPC_TEST_PG_DSN (default %s), skipping integration test", err, testPGDSN)
	}
	defer pool.Close()

	// All 8 platform tables must exist.
	var n int
	err = pool.QueryRow(context.Background(),
		`SELECT count(*) FROM information_schema.tables
		 WHERE table_schema='public' AND table_name = ANY($1)`,
		[]string{"tenant", "agent_app", "channel_binding", "session",
			"session_event", "memory_item", "summary", "audit_log"},
	).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	if n != 8 {
		t.Errorf("expected 8 tables, found %d", n)
	}
}
