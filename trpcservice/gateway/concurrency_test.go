package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestConcurrencyComponentValidation(t *testing.T) {
	if _, err := NewRedisDeduper(nil, time.Second, time.Second); err == nil {
		t.Fatal("NewRedisDeduper() accepted nil client")
	}
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	if _, err := NewRedisDeduper(client, 0, time.Second); err == nil {
		t.Fatal("NewRedisDeduper() accepted zero TTL")
	}
	deduper, err := NewRedisDeduper(client, time.Second, time.Hour)
	if err != nil {
		t.Fatalf("NewRedisDeduper() error = %v", err)
	}
	if _, err := deduper.Claim(context.Background(), ""); err == nil {
		t.Fatal("Claim() accepted empty key")
	}
	if err := deduper.Mark(context.Background(), "", "token"); err == nil {
		t.Fatal("Mark() accepted empty key")
	}
	if err := deduper.Release(context.Background(), "key", ""); err == nil {
		t.Fatal("Release() accepted empty token")
	}

	if _, err := NewRedisSessionLock(nil, RedisSessionLockConfig{TTL: time.Second}); err == nil {
		t.Fatal("NewRedisSessionLock() accepted nil client")
	}
	if _, err := NewRedisSessionLock(client, RedisSessionLockConfig{}); err == nil {
		t.Fatal("NewRedisSessionLock() accepted zero TTL")
	}
	if _, err := NewRedisSessionLock(client, RedisSessionLockConfig{
		TTL: time.Second, RenewInterval: time.Second,
	}); err == nil {
		t.Fatal("NewRedisSessionLock() accepted renew interval equal to TTL")
	}
	if _, err := NewRedisSessionLock(client, RedisSessionLockConfig{
		TTL: time.Second, RenewInterval: time.Millisecond, MaxHold: time.Millisecond,
	}); err == nil {
		t.Fatal("NewRedisSessionLock() accepted max hold shorter than TTL")
	}
	lock := newTestLock(t, client)
	if _, err := lock.Acquire(context.Background(), ""); err == nil {
		t.Fatal("Acquire() accepted empty session ID")
	}
}

func TestConcurrencyComponentsReturnRedisErrors(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	deduper, err := NewRedisDeduper(client, time.Second, time.Hour)
	if err != nil {
		t.Fatalf("NewRedisDeduper() error = %v", err)
	}
	lock := newTestLock(t, client)
	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := deduper.Claim(context.Background(), "key"); err == nil {
		t.Fatal("Claim() did not return closed-client error")
	}
	if err := deduper.Mark(context.Background(), "key", "token"); err == nil {
		t.Fatal("Mark() did not return closed-client error")
	}
	if err := deduper.Release(context.Background(), "key", "token"); err == nil {
		t.Fatal("Release() did not return closed-client error")
	}
	if _, err := lock.Acquire(context.Background(), "session"); err == nil {
		t.Fatal("Acquire() did not return closed-client error")
	}
}

func TestRedisSessionLockDetectsLostLease(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	lock, err := NewRedisSessionLock(client, RedisSessionLockConfig{
		TTL:           100 * time.Millisecond,
		RenewInterval: 10 * time.Millisecond,
		MaxHold:       time.Second,
	})
	if err != nil {
		t.Fatalf("NewRedisSessionLock() error = %v", err)
	}
	leaseValue, err := lock.Acquire(context.Background(), "lost-session")
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}
	lease := leaseValue.(*redisLease)
	server.Del(lease.key)

	deadline := time.NewTimer(500 * time.Millisecond)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for lease.lostError() == nil {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("lease loss was not detected")
		}
	}
	if err := lease.Release(context.Background()); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Release() error = %v, want ErrLeaseLost", err)
	}
}
