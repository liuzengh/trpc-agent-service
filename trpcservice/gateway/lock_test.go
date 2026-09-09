package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisSessionLockMutualExclusionAndRelease(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	lock := newTestLock(t, client)
	ctx := context.Background()

	lease, err := lock.Acquire(ctx, "tenant-a:webui:user-1")
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	if _, err := lock.Acquire(ctx, "tenant-a:webui:user-1"); !errors.Is(err, ErrHeld) {
		t.Fatalf("second Acquire() error = %v, want ErrHeld", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	second, err := lock.Acquire(ctx, "tenant-a:webui:user-1")
	if err != nil {
		t.Fatalf("Acquire() after release error = %v", err)
	}
	if err := second.Release(ctx); err != nil {
		t.Fatalf("second Release() error = %v", err)
	}
}

func TestRedisSessionLockWrongOwnerCannotDelete(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	lock := newTestLock(t, client)
	ctx := context.Background()

	leaseValue, err := lock.Acquire(ctx, "session-fenced")
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	lease := leaseValue.(*redisLease)
	server.Set(lease.key, "new-owner")

	if err := lease.Release(ctx); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Release() error = %v, want ErrLeaseLost", err)
	}
	value, err := server.Get(lease.key)
	if err != nil {
		t.Fatalf("read lock key: %v", err)
	}
	if value != "new-owner" {
		t.Fatalf("lock value = %q, want new-owner", value)
	}
}

func TestRedisSessionLockRenewsLease(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	lock, err := NewRedisSessionLock(client, RedisSessionLockConfig{
		TTL:           200 * time.Millisecond,
		RenewInterval: 20 * time.Millisecond,
		MaxHold:       time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedisSessionLock() error = %v", err)
	}
	ctx := context.Background()
	leaseValue, err := lock.Acquire(ctx, "session-renew")
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	lease := leaseValue.(*redisLease)
	server.FastForward(150 * time.Millisecond)

	deadline := time.NewTimer(500 * time.Millisecond)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for server.TTL(lease.key) <= 100*time.Millisecond {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("lease TTL was not renewed, TTL = %s", server.TTL(lease.key))
		}
	}
	server.FastForward(100 * time.Millisecond)
	if !server.Exists(lease.key) {
		t.Fatal("renewed lease expired before release")
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
}

func newTestLock(t *testing.T, client redis.Cmdable) *RedisSessionLock {
	t.Helper()
	lock, err := NewRedisSessionLock(client, RedisSessionLockConfig{
		TTL:           time.Second,
		RenewInterval: 100 * time.Millisecond,
		MaxHold:       5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedisSessionLock() error = %v", err)
	}
	return lock
}
