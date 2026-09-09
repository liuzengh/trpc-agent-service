package storage

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
)

func TestSessionLockManagerRejectsInvalidDependenciesAndKeys(t *testing.T) {
	if _, err := (*SessionLockManager)(nil).AcquireLock(context.Background(), "key", time.Second); !errors.Is(err, ErrLockManagerUnavailable) {
		t.Fatalf("nil manager error=%v, want ErrLockManagerUnavailable", err)
	}
	_, client := newLeaseRedis(t)
	manager := NewSessionLockManager(client)
	for _, tc := range []struct {
		name string
		key  string
		ttl  time.Duration
	}{
		{name: "empty key", key: "", ttl: time.Second},
		{name: "control key", key: "session\nkey", ttl: time.Second},
		{name: "oversized key", key: strings.Repeat("x", maxSessionLockKeyBytes+1), ttl: time.Second},
		{name: "zero ttl", key: "session", ttl: 0},
		{name: "sub-millisecond ttl", key: "session", ttl: time.Microsecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := manager.AcquireLock(context.Background(), tc.key, tc.ttl); !errors.Is(err, ErrInvalidLockRequest) {
				t.Fatalf("error=%v, want ErrInvalidLockRequest", err)
			}
		})
	}
}

func TestLeaseAndSessionLockNilAccessorsAreSafe(t *testing.T) {
	var lease *Lease
	if got := lease.Token(); got != "" {
		t.Fatalf("nil lease token=%q, want empty", got)
	}
	if got := lease.Done(); got != nil {
		t.Fatal("nil lease Done returned a channel")
	}
	var lock *SessionLock
	if got := lock.ExpiresAt(); !got.IsZero() {
		t.Fatalf("nil lock expiry=%s, want zero", got)
	}
}

func TestSessionLockManagerRejectsSubMillisecondExtension(t *testing.T) {
	_, client := newLeaseRedis(t)
	manager := NewSessionLockManager(client)
	lock, err := manager.AcquireLock(context.Background(), "session-extension", time.Second)
	if err != nil {
		t.Fatalf("acquire lock: %v", err)
	}
	defer manager.ReleaseLock(context.Background(), lock)
	if err := manager.ExtendLock(context.Background(), lock, time.Microsecond); !errors.Is(err, ErrInvalidLockRequest) {
		t.Fatalf("error=%v, want ErrInvalidLockRequest", err)
	}
	if !manager.ValidateLock(context.Background(), lock) {
		t.Fatal("invalid extension removed the live lock")
	}
}

func newLeaseRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("start miniredis: %v", err)
	}
	t.Cleanup(server.Close)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return server, client
}

func TestLeaseRenewsBeyondOriginalTTL(t *testing.T) {
	_, client := newLeaseRedis(t)
	manager := NewSessionLockManager(client)
	ctx := context.Background()
	ttl := 300 * time.Millisecond
	lease, err := manager.AcquireLease(ctx, "session-long", ttl)
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	defer lease.Release(ctx)
	time.Sleep(ttl * 3)
	if err := lease.Err(); err != nil {
		t.Fatalf("lease lost during a live invocation: %v", err)
	}
	if !manager.ValidateLock(ctx, lease.lock) {
		t.Fatal("lease expired despite renewal")
	}
}

func TestLeaseReleaseAllowsImmediateNextOwner(t *testing.T) {
	_, client := newLeaseRedis(t)
	manager := NewSessionLockManager(client)
	ctx := context.Background()
	lease, err := manager.AcquireLease(ctx, "session-release", time.Second)
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatalf("release first lease: %v", err)
	}
	next, err := manager.AcquireLease(ctx, "session-release", time.Second)
	if err != nil {
		t.Fatalf("next owner could not acquire: %v", err)
	}
	defer next.Release(ctx)
}

func TestLeaseSerializesConcurrentInvocationOwners(t *testing.T) {
	_, client := newLeaseRedis(t)
	manager := NewSessionLockManager(client)
	ctx := context.Background()
	const workers = 8
	var mutex sync.Mutex
	var acquired int
	var group sync.WaitGroup
	for index := 0; index < workers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			lease, err := manager.AcquireLease(ctx, "session-contended", 5*time.Second)
			if err != nil {
				return
			}
			mutex.Lock()
			acquired++
			mutex.Unlock()
			time.Sleep(400 * time.Millisecond)
			_ = lease.Release(ctx)
		}()
	}
	group.Wait()
	if acquired != 1 {
		t.Fatalf("%d workers acquired the same session concurrently, want 1", acquired)
	}
}

func TestLeaseDetectsLostRedisOwnership(t *testing.T) {
	_, client := newLeaseRedis(t)
	manager := NewSessionLockManager(client)
	ctx := context.Background()
	lease, err := manager.AcquireLease(ctx, "session-stolen", 150*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	defer lease.Release(ctx)
	if err := client.Del(ctx, "lock:session-stolen").Err(); err != nil {
		t.Fatalf("remove lease key: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && lease.Err() == nil {
		time.Sleep(20 * time.Millisecond)
	}
	if lease.Err() == nil {
		t.Fatal("renewal loop did not report lost ownership")
	}
}

func TestLeaseRenewalAndValidationAreRaceSafe(t *testing.T) {
	_, client := newLeaseRedis(t)
	manager := NewSessionLockManager(client)
	ctx := context.Background()
	lease, err := manager.AcquireLease(ctx, "session-race", 60*time.Millisecond)
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	defer lease.Release(ctx)
	var group sync.WaitGroup
	stop := time.Now().Add(500 * time.Millisecond)
	for index := 0; index < 4; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for time.Now().Before(stop) {
				manager.ValidateLock(ctx, lease.lock)
				_ = lease.lock.ExpiresAt()
				_ = lease.Err()
			}
		}()
	}
	group.Wait()
	if err := lease.Err(); err != nil {
		t.Fatalf("lease lost during concurrent validation: %v", err)
	}
}
