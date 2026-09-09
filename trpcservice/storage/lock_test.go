package storage

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestLockMutualExclusion(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	sess := fmt.Sprintf("lock-test-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, lockKey("app1", sess)) })

	l := NewLock(rdb)

	ok, err := l.TryAcquire(ctx, "app1", sess, "w1", 10*time.Second)
	if err != nil || !ok {
		t.Fatalf("first acquire should succeed: ok=%v err=%v", ok, err)
	}
	ok, err = l.TryAcquire(ctx, "app1", sess, "w2", 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("second acquire must fail while w1 holds the lock")
	}

	if err := l.Release(ctx, "app1", sess, "w1"); err != nil {
		t.Fatal(err)
	}
	ok, _ = l.TryAcquire(ctx, "app1", sess, "w2", 10*time.Second)
	if !ok {
		t.Fatal("acquire after release should succeed")
	}
}

func TestLockReleaseChecksOwner(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	sess := fmt.Sprintf("lock-test-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, lockKey("app1", sess)) })

	l := NewLock(rdb)
	if ok, _ := l.TryAcquire(ctx, "app1", sess, "w1", 10*time.Second); !ok {
		t.Fatal("acquire failed")
	}

	// A non-owner release must not delete the lock.
	if err := l.Release(ctx, "app1", sess, "intruder"); err != nil {
		t.Fatal(err)
	}
	ok, _ := l.TryAcquire(ctx, "app1", sess, "w2", 10*time.Second)
	if ok {
		t.Fatal("non-owner release must not free the lock")
	}
}

func TestLockExtend(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	sess := fmt.Sprintf("lock-test-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, lockKey("app1", sess)) })

	l := NewLock(rdb)
	if ok, _ := l.TryAcquire(ctx, "app1", sess, "w1", 500*time.Millisecond); !ok {
		t.Fatal("acquire failed")
	}

	// Extend by the owner keeps it alive past the original TTL.
	time.Sleep(300 * time.Millisecond)
	ok, err := l.Extend(ctx, "app1", sess, "w1", 800*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("owner extend should succeed: ok=%v err=%v", ok, err)
	}
	time.Sleep(400 * time.Millisecond) // total 700ms > original 500ms TTL
	ok, _ = l.TryAcquire(ctx, "app1", sess, "w2", time.Second)
	if ok {
		t.Fatal("extended lock should still be held")
	}

	// Extend by a non-owner fails.
	if ok, _ := l.Extend(ctx, "app1", sess, "intruder", time.Second); ok {
		t.Fatal("non-owner extend must fail")
	}
}

func TestLockExpiresOnCrash(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	sess := fmt.Sprintf("lock-test-%d", time.Now().UnixNano())
	t.Cleanup(func() { rdb.Del(ctx, lockKey("app1", sess)) })

	l := NewLock(rdb)
	if ok, _ := l.TryAcquire(ctx, "app1", sess, "crashed-worker", 200*time.Millisecond); !ok {
		t.Fatal("acquire failed")
	}
	// No release (crash): the TTL alone must free the lock.
	time.Sleep(300 * time.Millisecond)
	ok, _ := l.TryAcquire(ctx, "app1", sess, "w2", time.Second)
	if !ok {
		t.Fatal("lock should expire with its TTL after a crash")
	}
}

// The lock key carries the app dimension (lock:sess:{app}:{session}): two
// tenants' users can carry the same channel:user session key, and one
// tenant's long run must never queue the other's messages.
func TestLockAppScopeIsolation(t *testing.T) {
	rdb := redisOrSkip(t)
	ctx := context.Background()
	sess := fmt.Sprintf("lock-scope-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		rdb.Del(ctx, lockKey("app-a", sess))
		rdb.Del(ctx, lockKey("app-b", sess))
	})

	l := NewLock(rdb)
	okA, err := l.TryAcquire(ctx, "app-a", sess, "w1", 10*time.Second)
	if err != nil || !okA {
		t.Fatalf("app-a acquire should succeed: ok=%v err=%v", okA, err)
	}
	okB, err := l.TryAcquire(ctx, "app-b", sess, "w2", 10*time.Second)
	if err != nil || !okB {
		t.Fatalf("app-b acquire on the same session key must not be blocked by app-a: ok=%v err=%v", okB, err)
	}
}
