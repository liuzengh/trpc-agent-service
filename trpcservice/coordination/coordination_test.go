package coordination

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestInMemoryClaimLifecycle(t *testing.T) {
	c := NewInMemory()
	ctx := context.Background()
	lease, err := c.Claim(ctx, "m1", time.Minute)
	if err != nil || lease.State != Claimed || lease.Token == "" {
		t.Fatalf("first claim = %+v, %v", lease, err)
	}
	second, _ := c.Claim(ctx, "m1", time.Minute)
	if second.State != AlreadyProcessing || second.Token != "" {
		t.Fatalf("second claim = %+v", second)
	}
	if err := c.Complete(ctx, "m1", lease.Token, time.Minute); err != nil {
		t.Fatal(err)
	}
	completed, _ := c.Claim(ctx, "m1", time.Minute)
	if completed.State != AlreadyCompleted {
		t.Fatalf("completed claim = %+v", completed)
	}
}

func TestInMemoryCompletedClaimRetainsReplayableResult(t *testing.T) {
	c := NewInMemory()
	ctx := context.Background()
	lease, err := c.Claim(ctx, "m-result", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"reply": "persist me"}
	if err := c.SaveResult(ctx, "m-result", lease.Token, want, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := c.Complete(ctx, "m-result", lease.Token, time.Minute); err != nil {
		t.Fatal(err)
	}
	var got map[string]string
	found, err := c.LoadResult(ctx, "m-result", &got)
	if err != nil || !found || got["reply"] != want["reply"] {
		t.Fatalf("completed result found=%v got=%v err=%v", found, got, err)
	}
}

func TestCompleteRefreshesReplayResultTTL(t *testing.T) {
	t.Run("in-memory", func(t *testing.T) {
		c := NewInMemory()
		ctx := context.Background()
		lease, err := c.Claim(ctx, "refresh", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.SaveResult(ctx, "refresh", lease.Token, map[string]string{"reply": "kept"}, time.Second); err != nil {
			t.Fatal(err)
		}
		c.mu.Lock()
		before := c.results["refresh"].expires
		c.mu.Unlock()
		if err := c.Complete(ctx, "refresh", lease.Token, time.Hour); err != nil {
			t.Fatal(err)
		}
		c.mu.Lock()
		resultExpiry := c.results["refresh"].expires
		claimExpiry := c.claims["refresh"].expires
		c.mu.Unlock()
		if !resultExpiry.Equal(claimExpiry) || !resultExpiry.After(before) {
			t.Fatalf("result expiry=%v claim expiry=%v before=%v", resultExpiry, claimExpiry, before)
		}
	})

	t.Run("redis", func(t *testing.T) {
		server := miniredis.RunT(t)
		c, err := NewRedis("redis://"+server.Addr()+"/0", "refresh-test")
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		ctx := context.Background()
		lease, err := c.Claim(ctx, "refresh", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.SaveResult(ctx, "refresh", lease.Token, map[string]string{"reply": "kept"}, time.Second); err != nil {
			t.Fatal(err)
		}
		server.FastForward(500 * time.Millisecond)
		if err := c.Complete(ctx, "refresh", lease.Token, time.Minute); err != nil {
			t.Fatal(err)
		}
		server.FastForward(2 * time.Second)
		var got map[string]string
		found, err := c.LoadResult(ctx, "refresh", &got)
		if err != nil || !found || got["reply"] != "kept" {
			t.Fatalf("refreshed result found=%v got=%v err=%v", found, got, err)
		}
	})
}

func TestInMemoryStaleOwnerCannotMutateNewClaim(t *testing.T) {
	c := NewInMemory()
	ctx := context.Background()
	old, err := c.Claim(ctx, "m1", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	current, err := c.Claim(ctx, "m1", time.Minute)
	if err != nil || current.State != Claimed {
		t.Fatalf("replacement claim = %+v, %v", current, err)
	}
	if err := c.ReleaseClaim(ctx, "m1", old.Token); err != nil {
		t.Fatal(err)
	}
	if err := c.Complete(ctx, "m1", old.Token, time.Minute); !errors.Is(err, ErrClaimOwnershipLost) {
		t.Fatalf("stale complete error = %v", err)
	}
	if err := c.SaveResult(ctx, "m1", old.Token, map[string]string{"owner": "old"}, time.Minute); !errors.Is(err, ErrClaimOwnershipLost) {
		t.Fatalf("stale result write error = %v", err)
	}
	seen, err := c.Claim(ctx, "m1", time.Minute)
	if err != nil || seen.State != AlreadyProcessing {
		t.Fatalf("stale owner changed current claim: %+v, %v", seen, err)
	}
}

func TestRedisStaleOwnerCannotCompleteOrReleaseNewClaim(t *testing.T) {
	server := miniredis.RunT(t)
	c, err := NewRedis("redis://"+server.Addr()+"/0", "test")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	old, err := c.Claim(ctx, "m1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	server.FastForward(2 * time.Second)
	current, err := c.Claim(ctx, "m1", time.Minute)
	if err != nil || current.State != Claimed {
		t.Fatalf("replacement claim = %+v, %v", current, err)
	}
	if err := c.ReleaseClaim(ctx, "m1", old.Token); err != nil {
		t.Fatal(err)
	}
	if err := c.Complete(ctx, "m1", old.Token, time.Minute); !errors.Is(err, ErrClaimOwnershipLost) {
		t.Fatalf("stale complete error = %v", err)
	}
	if err := c.SaveResult(ctx, "m1", old.Token, map[string]string{"owner": "old"}, time.Minute); !errors.Is(err, ErrClaimOwnershipLost) {
		t.Fatalf("stale result write error = %v", err)
	}
	seen, err := c.Claim(ctx, "m1", time.Minute)
	if err != nil || seen.State != AlreadyProcessing {
		t.Fatalf("stale owner changed current claim: %+v, %v", seen, err)
	}
	if err := c.Complete(ctx, "m1", current.Token, time.Minute); err != nil {
		t.Fatal(err)
	}
}

func TestNewRedisDoesNotLeakCredentialsInParseError(t *testing.T) {
	const credential = "canary-password"
	_, err := NewRedis("redis://user:"+credential+"@localhost/%zz", "test")
	if err == nil {
		t.Fatal("expected malformed redis URL to fail")
	}
	if strings.Contains(err.Error(), credential) {
		t.Fatalf("redis parse error leaked credential: %v", err)
	}
}

func TestRedisTenantFreezeIsOwnedAndPersistent(t *testing.T) {
	server := miniredis.RunT(t)
	c, err := NewRedis("redis://"+server.Addr()+"/0", "freeze-test")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	frozenAt, err := c.FreezeTenant(ctx, "tenant-a", "assistant", "migration-a")
	if err != nil || frozenAt.IsZero() {
		t.Fatalf("freeze time=%v err=%v", frozenAt, err)
	}
	id, gotAt, frozen, err := c.TenantFreeze(ctx, "tenant-a", "assistant")
	if err != nil || !frozen || id != "migration-a" || !gotAt.Equal(frozenAt) {
		t.Fatalf("freeze id=%q at=%v frozen=%v err=%v", id, gotAt, frozen, err)
	}
	if _, err := c.FreezeTenant(ctx, "tenant-a", "assistant", "migration-b"); err == nil {
		t.Fatal("another migration stole the freeze")
	}
	if err := c.UnfreezeTenant(ctx, "tenant-a", "assistant", "migration-b"); err == nil {
		t.Fatal("another migration released the freeze")
	}
	if err := c.UnfreezeTenant(ctx, "tenant-a", "assistant", "migration-a"); err != nil {
		t.Fatal(err)
	}
	_, _, frozen, err = c.TenantFreeze(ctx, "tenant-a", "assistant")
	if err != nil || frozen {
		t.Fatalf("freeze remained after release: frozen=%v err=%v", frozen, err)
	}
}

func TestRedisRateLimiterIsSharedAtomicAndTenantScoped(t *testing.T) {
	server := miniredis.RunT(t)
	first, err := NewRedis("redis://"+server.Addr()+"/0", "rate-test")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRedis("redis://"+server.Addr()+"/0", "rate-test")
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	defer first.Close()
	defer second.Close()

	ctx := context.Background()
	for i, limiter := range []RateLimiter{first, second, first} {
		allowed, _, err := limiter.Allow(ctx, "tenant-a", 3)
		if err != nil || !allowed {
			t.Fatalf("shared allowance %d = allowed=%v err=%v", i, allowed, err)
		}
	}
	allowed, retryAfter, err := first.Allow(ctx, "tenant-a", 3)
	if err != nil || allowed || retryAfter <= 0 {
		t.Fatalf("fourth allowance = allowed=%v retry=%s err=%v", allowed, retryAfter, err)
	}
	allowed, _, err = second.Allow(ctx, "tenant-b", 3)
	if err != nil || !allowed {
		t.Fatalf("different tenant was rate limited: allowed=%v err=%v", allowed, err)
	}
	server.FastForward(62 * time.Second)
	allowed, _, err = first.Allow(ctx, "tenant-a", 3)
	if err != nil || !allowed {
		t.Fatalf("next window allowance = allowed=%v err=%v", allowed, err)
	}
}

func TestRedisRateLimiterFailsClosedWhenRedisIsUnavailable(t *testing.T) {
	server := miniredis.RunT(t)
	limiter, err := NewRedis("redis://"+server.Addr()+"/0", "rate-failure")
	if err != nil {
		t.Fatal(err)
	}
	server.Close()
	defer limiter.Close()
	allowed, _, err := limiter.Allow(context.Background(), "tenant-a", 1)
	if allowed || !errors.Is(err, ErrRateLimiterUnavailable) {
		t.Fatalf("unavailable limiter = allowed=%v err=%v", allowed, err)
	}
}

func TestInMemoryRateLimiterIsAtomic(t *testing.T) {
	limiter := NewInMemory()
	const attempts = 32
	var wg sync.WaitGroup
	allowed := make(chan bool, attempts)
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, _, err := limiter.Allow(context.Background(), "tenant-a", 5)
			if err != nil {
				t.Errorf("Allow() error = %v", err)
			}
			allowed <- ok
		}()
	}
	wg.Wait()
	close(allowed)
	count := 0
	for ok := range allowed {
		if ok {
			count++
		}
	}
	if count != 5 {
		t.Fatalf("allowed %d concurrent requests, want 5", count)
	}
}

func TestInMemoryLockSerializesSameSession(t *testing.T) {
	c := NewInMemory()
	ctx := context.Background()
	var mu sync.Mutex
	active, maxActive := 0, 0
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := c.Lock(ctx, "session", time.Minute)
			if err != nil {
				t.Error(err)
				return
			}
			defer lease.Release()
			mu.Lock()
			active++
			if active > maxActive {
				maxActive = active
			}
			mu.Unlock()
			time.Sleep(time.Millisecond)
			mu.Lock()
			active--
			mu.Unlock()
		}()
	}
	wg.Wait()
	if maxActive != 1 {
		t.Fatalf("same-session critical sections overlapped: max=%d", maxActive)
	}
}
