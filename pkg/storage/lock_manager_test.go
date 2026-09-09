package storage

import (
	"context"
	"testing"
	"time"
)

func TestSessionLockManagerAcquireAndRelease(t *testing.T) {
	_, client := newLeaseRedis(t)
	manager := NewSessionLockManager(client)
	ctx := context.Background()
	lock, err := manager.AcquireLock(ctx, "session-1", 5*time.Second)
	if err != nil {
		t.Fatalf("acquire lock: %v", err)
	}
	if lock.Key != "session-1" || lock.Token == "" {
		t.Fatalf("invalid lock: %+v", lock)
	}
	if !manager.ValidateLock(ctx, lock) {
		t.Fatal("new lock is not valid")
	}
	if err := manager.ReleaseLock(ctx, lock); err != nil {
		t.Fatalf("release lock: %v", err)
	}
	if manager.ValidateLock(ctx, lock) {
		t.Fatal("released lock remained valid")
	}
}

func TestSessionLockManagerRejectsConcurrentOwner(t *testing.T) {
	_, client := newLeaseRedis(t)
	manager := NewSessionLockManager(client)
	ctx := context.Background()
	first, err := manager.AcquireLock(ctx, "session-2", 5*time.Second)
	if err != nil {
		t.Fatalf("acquire first lock: %v", err)
	}
	defer manager.ReleaseLock(ctx, first)
	second, err := manager.AcquireLock(ctx, "session-2", 5*time.Second)
	if err == nil {
		_ = manager.ReleaseLock(ctx, second)
		t.Fatal("second owner acquired a live session lock")
	}
}

func TestSessionLockOwnershipTokenChangesAfterExpiry(t *testing.T) {
	server, client := newLeaseRedis(t)
	manager := NewSessionLockManager(client)
	ctx := context.Background()
	first, err := manager.AcquireLock(ctx, "session-3", time.Second)
	if err != nil {
		t.Fatalf("acquire first lock: %v", err)
	}
	server.FastForward(2 * time.Second)
	second, err := manager.AcquireLock(ctx, "session-3", 5*time.Second)
	if err != nil {
		t.Fatalf("acquire after expiry: %v", err)
	}
	defer manager.ReleaseLock(ctx, second)
	if first.Token == second.Token {
		t.Fatal("new owner reused the previous ownership token")
	}
	if manager.ValidateLock(ctx, first) {
		t.Fatal("expired owner remained valid")
	}
}
