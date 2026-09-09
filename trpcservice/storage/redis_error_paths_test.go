package storage

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// One dead Redis client exercises every error-return path of the Redis
// wrappers: connection-refused dials fail fast, and each call must return a
// wrapped error instead of panicking or reporting success.
func TestRedisWrappersFailClosedOnRedisError(t *testing.T) {
	dead := redis.NewClient(&redis.Options{
		Addr:        "localhost:1",
		DialTimeout: 500 * time.Millisecond,
		ReadTimeout: 500 * time.Millisecond,
		MaxRetries:  -1,
	})
	defer func() { _ = dead.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Run("dedup", func(t *testing.T) {
		d := NewDeduper(dead)
		if ok, err := d.Check(ctx, "c", "b", "m"); ok || err == nil {
			t.Fatalf("Check must fail: ok=%v err=%v", ok, err)
		}
		if err := d.Forget(ctx, "c", "b", "m"); err == nil {
			t.Fatal("Forget must fail")
		}
	})
	t.Run("sent", func(t *testing.T) {
		m := NewSentMarker(dead)
		if ok, err := m.IsSent(ctx, "c", "b", "m"); ok || err == nil {
			t.Fatalf("IsSent must fail: ok=%v err=%v", ok, err)
		}
		if err := m.MarkSent(ctx, "c", "b", "m", "im-1"); err == nil {
			t.Fatal("MarkSent must fail")
		}
	})
	t.Run("processed", func(t *testing.T) {
		m := NewProcessedMarker(dead)
		if ok, err := m.IsDone(ctx, "c", "b", "m"); ok || err == nil {
			t.Fatalf("IsDone must fail: ok=%v err=%v", ok, err)
		}
		if err := m.MarkDone(ctx, "c", "b", "m"); err == nil {
			t.Fatal("MarkDone must fail")
		}
	})
	t.Run("lock", func(t *testing.T) {
		l := NewLock(dead)
		if ok, err := l.TryAcquire(ctx, "a", "s", "o", time.Minute); ok || err == nil {
			t.Fatalf("TryAcquire must fail: ok=%v err=%v", ok, err)
		}
		if err := l.Release(ctx, "a", "s", "o"); err == nil {
			t.Fatal("Release must fail")
		}
		if ok, err := l.Extend(ctx, "a", "s", "o", time.Minute); ok || err == nil {
			t.Fatalf("Extend must fail: ok=%v err=%v", ok, err)
		}
	})
	t.Run("limiter", func(t *testing.T) {
		l := NewLimiter(dead)
		if ok, err := l.Allow(ctx, "s", 1, 1); ok || err == nil {
			t.Fatalf("Allow must fail: ok=%v err=%v", ok, err)
		}
		if ok, err := l.WaitAllow(ctx, "s", 1, 1, time.Second); ok || err == nil {
			t.Fatalf("WaitAllow must propagate the error: ok=%v err=%v", ok, err)
		}
	})
	t.Run("stream", func(t *testing.T) {
		s := NewStream(dead)
		if _, err := s.Add(ctx, "st", []byte("p")); err == nil {
			t.Fatal("Add must fail")
		}
		if _, err := s.Len(ctx, "st"); err == nil {
			t.Fatal("Len must fail")
		}
		if err := s.EnsureGroup(ctx, "st", "g"); err == nil {
			t.Fatal("EnsureGroup must fail")
		}
		if _, err := s.Read(ctx, "st", "g", "c", 1, time.Second); err == nil {
			t.Fatal("Read must fail")
		}
		if err := s.Ack(ctx, "st", "g", "0-0"); err == nil {
			t.Fatal("Ack must fail")
		}
		if _, err := s.AutoClaim(ctx, "st", "g", "c", time.Second, 1); err == nil {
			t.Fatal("AutoClaim must fail")
		}
		if _, err := s.Attempts(ctx, "st", "g", "0-0"); err == nil {
			t.Fatal("Attempts must fail")
		}
		if _, err := s.IncAttempts(ctx, "st", "g", "0-0"); err == nil {
			t.Fatal("IncAttempts must fail")
		}
		if err := s.DeadLetter(ctx, "st", "g", Message{ID: "0-0", Payload: []byte("p")}); err == nil {
			t.Fatal("DeadLetter must fail")
		}
	})
}

// WaitAllow must also give up on a canceled context while spinning for a
// token, returning the context error.
func TestLimiterWaitAllowHonorsContext(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	l := NewLimiter(rdb)

	scope := "waitallow-cancel-" + time.Now().Format("150405.000000000")
	t.Cleanup(func() { rdb.Del(ctx, "ratelimit:"+scope) })

	// Drain the burst of 1, then spin against an empty bucket with a context
	// that is already canceled: the wait must stop immediately.
	if ok, err := l.Allow(ctx, scope, 1, 1); err != nil || !ok {
		t.Fatalf("first token must be granted: ok=%v err=%v", ok, err)
	}
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	ok, err := l.WaitAllow(cctx, scope, 1, 1, 10*time.Second)
	if ok {
		t.Fatal("no token may be granted from an empty bucket")
	}
	if err == nil {
		t.Fatal("a canceled wait must surface the context error")
	}
}
