package storage

import (
	"context"
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
