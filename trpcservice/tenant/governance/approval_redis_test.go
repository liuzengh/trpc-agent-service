package governance

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func redisApprovalClient(t *testing.T, server *miniredis.Miniredis) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{Addr: server.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestRedisApprovalSharedAcrossIndependentInstances(t *testing.T) {
	server := miniredis.RunT(t)
	first := NewRedisApprovalStore(redisApprovalClient(t, server), "approval-test")
	second := NewRedisApprovalStore(redisApprovalClient(t, server), "approval-test")
	ctx := context.Background()
	nonce, err := first.IssueApproval(ctx, "tenant/user/app/channel/private-scope", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	keys := server.Keys()
	if len(keys) != 1 || strings.Contains(keys[0], nonce) || strings.Contains(keys[0], "private-scope") {
		t.Fatalf("unexpected stored keys: %v", keys)
	}
	value, err := server.Get(keys[0])
	if err != nil || value != approvalHash("tenant/user/app/channel/private-scope") {
		t.Fatalf("scope must be hashed, got %q, %v", value, err)
	}
	if ttl := server.TTL(keys[0]); ttl != 5*time.Minute {
		t.Fatalf("expiry = %v", ttl)
	}
	if ok, err := second.ConsumeApproval(ctx, nonce, "different-scope"); ok || err != nil {
		t.Fatalf("mismatched scope = %v, %v", ok, err)
	}
	if ok, err := second.ConsumeApproval(ctx, nonce, "tenant/user/app/channel/private-scope"); !ok || err != nil {
		t.Fatalf("cross-instance consume = %v, %v", ok, err)
	}
	if ok, err := first.ConsumeApproval(ctx, nonce, "tenant/user/app/channel/private-scope"); ok || err != nil {
		t.Fatalf("second consume = %v, %v", ok, err)
	}
}

func TestRedisApprovalConcurrentConsumeOnlyOneWins(t *testing.T) {
	server := miniredis.RunT(t)
	stores := []*RedisApprovalStore{
		NewRedisApprovalStore(redisApprovalClient(t, server), "concurrent"),
		NewRedisApprovalStore(redisApprovalClient(t, server), "concurrent"),
	}
	nonce, err := stores[0].IssueApproval(context.Background(), "scope", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var winners atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ok, err := stores[i%len(stores)].ConsumeApproval(context.Background(), nonce, "scope")
			if err != nil {
				t.Errorf("consume error: %v", err)
			}
			if ok {
				winners.Add(1)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if got := winners.Load(); got != 1 {
		t.Fatalf("concurrent consumers granted %d approvals", got)
	}
}

func TestRedisApprovalExpiryCancellationAndPrefixIsolation(t *testing.T) {
	server := miniredis.RunT(t)
	store := NewRedisApprovalStore(redisApprovalClient(t, server), "first")
	other := NewRedisApprovalStore(redisApprovalClient(t, server), "other")
	nonce, err := store.IssueApproval(context.Background(), "scope", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := other.ConsumeApproval(context.Background(), nonce, "scope"); ok || err != nil {
		t.Fatalf("other namespace consumed token = %v, %v", ok, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.IssueApproval(ctx, "other", time.Minute); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled issue = %v", err)
	}
	if ok, err := store.ConsumeApproval(ctx, nonce, "scope"); ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled consume = %v, %v", ok, err)
	}
	if !server.Exists(store.key(nonce)) {
		t.Fatal("canceled consume burned token")
	}
	server.FastForward(5 * time.Minute)
	if ok, err := store.ConsumeApproval(context.Background(), nonce, "scope"); ok || err != nil {
		t.Fatalf("expired consume = %v, %v", ok, err)
	}
}

func TestRedisApprovalCollisionIsBoundedAndCannotReplaceAuthorization(t *testing.T) {
	server := miniredis.RunT(t)
	store := NewRedisApprovalStore(redisApprovalClient(t, server), "collision")
	store.newNonce = func() (string, error) { return "nonce", nil }
	if _, err := store.IssueApproval(context.Background(), "original", time.Minute); err != nil {
		t.Fatal(err)
	}
	calls := 0
	store.newNonce = func() (string, error) { calls++; return "nonce", nil }
	if _, err := store.IssueApproval(context.Background(), "replacement", time.Hour); !errors.Is(err, ErrApprovalUnavailable) {
		t.Fatalf("collision error = %v", err)
	}
	if calls != 3 || server.TTL(store.key("nonce")) != time.Minute {
		t.Fatalf("collision attempts = %d, original ttl = %v", calls, server.TTL(store.key("nonce")))
	}
	if ok, err := store.ConsumeApproval(context.Background(), "nonce", "original"); !ok || err != nil {
		t.Fatalf("collision replaced authorization: %v, %v", ok, err)
	}
	store.newNonce = func() (string, error) { return "", errors.New("secret entropy failure") }
	if _, err := store.IssueApproval(context.Background(), "scope", time.Minute); !errors.Is(err, ErrApprovalUnavailable) {
		t.Fatalf("nonce failure = %v", err)
	}
}

func TestRedisApprovalCollisionRetriesWithFreshNonce(t *testing.T) {
	server := miniredis.RunT(t)
	store := NewRedisApprovalStore(redisApprovalClient(t, server), "retry")
	store.newNonce = func() (string, error) { return "old", nil }
	if _, err := store.IssueApproval(context.Background(), "old-scope", time.Minute); err != nil {
		t.Fatal(err)
	}
	calls := 0
	store.newNonce = func() (string, error) {
		calls++
		if calls < 3 {
			return "old", nil
		}
		return "fresh", nil
	}
	if nonce, err := store.IssueApproval(context.Background(), "new-scope", time.Minute); nonce != "fresh" || err != nil {
		t.Fatalf("retry = %q, %v", nonce, err)
	}
}

func TestRedisApprovalUnavailableAndCanceledPolicyFailClosed(t *testing.T) {
	server := miniredis.RunT(t)
	client := redisApprovalClient(t, server)
	store := NewRedisApprovalStore(client, "unavailable")
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.IssueApproval(context.Background(), "scope", time.Minute); !errors.Is(err, ErrApprovalUnavailable) {
		t.Fatalf("unavailable issue = %v", err)
	}
	if ok, err := store.ConsumeApproval(context.Background(), "nonce", "scope"); ok || !errors.Is(err, ErrApprovalUnavailable) {
		t.Fatalf("unavailable consume = %v, %v", ok, err)
	}
	policy := PermissionPolicy(config.ToolPolicy{Allow: []string{"danger"}, RequireConfirm: []string{"danger"}}, store)
	for _, nonce := range []string{"", "nonce"} {
		ctx := WithRequestContext(context.Background(), RequestContext{TenantID: "tenant", ApprovalNonce: nonce})
		decision, err := policy(ctx, &tool.PermissionRequest{ToolName: "danger"})
		if err != nil || decision.Action != tool.PermissionActionDeny || strings.Contains(decision.Reason, server.Addr()) {
			t.Fatalf("unavailable policy = %+v, %v", decision, err)
		}
	}
	live := NewRedisApprovalStore(redisApprovalClient(t, server), "live")
	policy = PermissionPolicy(config.ToolPolicy{Allow: []string{"danger"}, RequireConfirm: []string{"danger"}}, live)
	ctx, cancel := context.WithCancel(WithRequestContext(context.Background(), RequestContext{TenantID: "tenant"}))
	cancel()
	if decision, err := policy(ctx, &tool.PermissionRequest{ToolName: "danger"}); err != nil || decision.Action != tool.PermissionActionDeny {
		t.Fatalf("canceled policy = %+v, %v", decision, err)
	}
	if len(server.Keys()) != 0 {
		t.Fatalf("denied calls wrote approvals: %v", server.Keys())
	}
}

func TestRedisApprovalRejectsInvalidExpirationAndScope(t *testing.T) {
	server := miniredis.RunT(t)
	store := NewRedisApprovalStore(redisApprovalClient(t, server), "invalid")
	for _, ttl := range []time.Duration{-time.Second, 0, time.Nanosecond} {
		if _, err := store.IssueApproval(context.Background(), "scope", ttl); !errors.Is(err, ErrApprovalUnavailable) {
			t.Fatalf("ttl %v = %v", ttl, err)
		}
	}
	if _, err := store.IssueApproval(context.Background(), "", time.Minute); !errors.Is(err, ErrApprovalUnavailable) {
		t.Fatalf("empty scope = %v", err)
	}
	if len(server.Keys()) != 0 {
		t.Fatalf("invalid issue wrote keys: %v", server.Keys())
	}
}
