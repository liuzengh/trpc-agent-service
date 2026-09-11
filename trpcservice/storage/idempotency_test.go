package storage

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/redis/go-redis/v9"
)

func TestMemoryIdempotencyStoreAllowsOneConcurrentLeasePerTenantMessage(t *testing.T) {
	t.Parallel()

	store := NewMemoryIdempotencyStore()
	key, err := BuildIdempotencyKey("tenant-a", channels.Telegram, "telegram-bot-a", "update-42")
	if err != nil {
		t.Fatalf("BuildIdempotencyKey() error = %v", err)
	}

	const attempts = 16
	results := make(chan AcquireResult, attempts)
	errors := make(chan error, attempts)
	var group sync.WaitGroup
	for index := 0; index < attempts; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := store.Acquire(context.Background(), key, time.Minute)
			if err != nil {
				errors <- err
				return
			}
			results <- result
		}()
	}
	group.Wait()
	close(results)
	close(errors)

	for err := range errors {
		t.Fatalf("Acquire() error = %v", err)
	}

	var acquired []Lease
	inProgress := 0
	for result := range results {
		switch result.State {
		case LeaseAcquired:
			acquired = append(acquired, result.Lease)
		case LeaseInProgress:
			inProgress++
		default:
			t.Fatalf("acquire state = %q, want acquired or in_progress", result.State)
		}
	}
	if len(acquired) != 1 {
		t.Fatalf("acquired leases = %d, want 1", len(acquired))
	}
	if inProgress != attempts-1 {
		t.Fatalf("in-progress leases = %d, want %d", inProgress, attempts-1)
	}

	if err := store.Complete(context.Background(), acquired[0], time.Minute); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	result, err := store.Acquire(context.Background(), key, time.Minute)
	if err != nil {
		t.Fatalf("Acquire() after completion error = %v", err)
	}
	if result.State != LeaseAlreadyCompleted {
		t.Fatalf("acquire state after completion = %q, want %q", result.State, LeaseAlreadyCompleted)
	}
}

func TestBuildIdempotencyKeyValidatesAllParts(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		tenantID  string
		channel   channels.Channel
		bindingID string
		messageID string
	}{
		{name: "tenant", channel: channels.Telegram, bindingID: "support-bot", messageID: "message-1"},
		{name: "channel", tenantID: "tenant-a", channel: channels.Channel("unknown"), bindingID: "support-bot", messageID: "message-1"},
		{name: "binding", tenantID: "tenant-a", channel: channels.Telegram, messageID: "message-1"},
		{name: "message", tenantID: "tenant-a", channel: channels.Telegram, bindingID: "support-bot"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := BuildIdempotencyKey(test.tenantID, test.channel, test.bindingID, test.messageID); err == nil {
				t.Fatal("BuildIdempotencyKey() error = nil")
			}
		})
	}
}

func TestMemoryIdempotencyStoreLeaseFailureBoundaries(t *testing.T) {
	t.Parallel()
	store := NewMemoryIdempotencyStore()
	now := time.Date(2026, time.September, 11, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	if _, err := store.Acquire(context.Background(), "", time.Minute); err == nil {
		t.Fatal("Acquire() accepted blank key")
	}
	if _, err := store.Acquire(context.Background(), "message", 0); err == nil {
		t.Fatal("Acquire() accepted zero TTL")
	}
	acquired, err := store.Acquire(context.Background(), "message", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	foreign := acquired.Lease
	foreign.token = "other-token"
	if err := store.Renew(context.Background(), foreign, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("foreign Renew() error = %v", err)
	}
	if err := store.Complete(context.Background(), foreign, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("foreign Complete() error = %v", err)
	}
	if err := store.Release(context.Background(), foreign); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("foreign Release() error = %v", err)
	}
	if err := store.Renew(context.Background(), acquired.Lease, 0); err == nil {
		t.Fatal("Renew() accepted zero TTL")
	}
	if err := store.Complete(context.Background(), acquired.Lease, 0); err == nil {
		t.Fatal("Complete() accepted zero TTL")
	}

	now = now.Add(2 * time.Minute)
	if err := store.Renew(context.Background(), acquired.Lease, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired Renew() error = %v", err)
	}
	if err := store.Complete(context.Background(), acquired.Lease, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired Complete() error = %v", err)
	}
	if err := store.Release(context.Background(), acquired.Lease); err != nil {
		t.Fatalf("Release() of expired but still owned lease = %v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Acquire(cancelled, "message-2", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Acquire() error = %v", err)
	}
	if err := store.Renew(cancelled, Lease{}, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Renew() error = %v", err)
	}
	if err := store.Complete(cancelled, Lease{}, time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Complete() error = %v", err)
	}
	if err := store.Release(cancelled, Lease{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Release() error = %v", err)
	}
}

func TestRedisIdempotencyStoreLifecycleAndValidation(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	if _, err := NewRedisIdempotencyStore(nil); err == nil {
		t.Fatal("NewRedisIdempotencyStore(nil) error = nil")
	}
	store, err := NewRedisIdempotencyStore(client)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := store.Acquire(context.Background(), "", time.Minute); err == nil {
		t.Fatal("Redis Acquire() accepted blank key")
	}
	if _, err := store.Acquire(context.Background(), "message", 0); err == nil {
		t.Fatal("Redis Acquire() accepted zero TTL")
	}
	first, err := store.Acquire(context.Background(), "message", time.Minute)
	if err != nil || first.State != LeaseAcquired {
		t.Fatalf("first Acquire() = %#v, %v", first, err)
	}
	second, err := store.Acquire(context.Background(), "message", time.Minute)
	if err != nil || second.State != LeaseInProgress {
		t.Fatalf("second Acquire() = %#v, %v", second, err)
	}
	foreign := first.Lease
	foreign.token = "other-token"
	if err := store.Renew(context.Background(), foreign, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("foreign Redis Renew() error = %v", err)
	}
	if err := store.Complete(context.Background(), foreign, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("foreign Redis Complete() error = %v", err)
	}
	if err := store.Release(context.Background(), foreign); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("foreign Redis Release() error = %v", err)
	}
	if err := store.Release(context.Background(), first.Lease); err != nil {
		t.Fatalf("Redis Release() error = %v", err)
	}
	reacquired, err := store.Acquire(context.Background(), "message", time.Minute)
	if err != nil || reacquired.State != LeaseAcquired {
		t.Fatalf("Acquire() after release = %#v, %v", reacquired, err)
	}
	if err := store.Complete(context.Background(), reacquired.Lease, time.Hour); err != nil {
		t.Fatal(err)
	}
	completed, err := store.Acquire(context.Background(), "message", time.Minute)
	if err != nil || completed.State != LeaseAlreadyCompleted {
		t.Fatalf("Acquire() after complete = %#v, %v", completed, err)
	}
}

func TestMemoryIdempotencyStoreRenewKeepsLongRunOwned(t *testing.T) {
	store := NewMemoryIdempotencyStore()
	now := time.Unix(1_700_000_000, 0)
	store.now = func() time.Time { return now }

	acquired, err := store.Acquire(context.Background(), "message-1", time.Second)
	if err != nil || acquired.State != LeaseAcquired {
		t.Fatalf("Acquire() = %#v, %v", acquired, err)
	}
	now = now.Add(800 * time.Millisecond)
	if err := store.Renew(context.Background(), acquired.Lease, time.Second); err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	now = now.Add(800 * time.Millisecond)
	if err := store.Complete(context.Background(), acquired.Lease, time.Minute); err != nil {
		t.Fatalf("Complete() after renewal error = %v", err)
	}
}

func TestRedisIdempotencyStoreRenewKeepsLongRunOwned(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, err := NewRedisIdempotencyStore(client)
	if err != nil {
		t.Fatal(err)
	}

	acquired, err := store.Acquire(context.Background(), "message-1", time.Second)
	if err != nil || acquired.State != LeaseAcquired {
		t.Fatalf("Acquire() = %#v, %v", acquired, err)
	}
	server.FastForward(800 * time.Millisecond)
	if err := store.Renew(context.Background(), acquired.Lease, time.Second); err != nil {
		t.Fatalf("Renew() error = %v", err)
	}
	server.FastForward(800 * time.Millisecond)
	if err := store.Complete(context.Background(), acquired.Lease, time.Minute); err != nil {
		t.Fatalf("Complete() after renewal error = %v", err)
	}
}

func TestBuildIdempotencyKeyScopesEqualMessageIDsByTenant(t *testing.T) {
	t.Parallel()

	tenantA, err := BuildIdempotencyKey("tenant-a", channels.Telegram, "telegram-bot-a", "update-42")
	if err != nil {
		t.Fatalf("BuildIdempotencyKey() tenant A error = %v", err)
	}
	tenantB, err := BuildIdempotencyKey("tenant-b", channels.Telegram, "telegram-bot-a", "update-42")
	if err != nil {
		t.Fatalf("BuildIdempotencyKey() tenant B error = %v", err)
	}
	if tenantA == tenantB {
		t.Fatalf("tenant-scoped idempotency keys collide: %q", tenantA)
	}
}

func TestBuildIdempotencyKeyScopesEqualProviderMessageIDsByBinding(t *testing.T) {
	t.Parallel()

	botA, err := BuildIdempotencyKey("tenant-a", channels.Telegram, "telegram-bot-a", "update-42")
	if err != nil {
		t.Fatalf("BuildIdempotencyKey() bot A error = %v", err)
	}
	botB, err := BuildIdempotencyKey("tenant-a", channels.Telegram, "telegram-bot-b", "update-42")
	if err != nil {
		t.Fatalf("BuildIdempotencyKey() bot B error = %v", err)
	}
	if botA == botB {
		t.Fatalf("binding-scoped idempotency keys collide: %q", botA)
	}
}
