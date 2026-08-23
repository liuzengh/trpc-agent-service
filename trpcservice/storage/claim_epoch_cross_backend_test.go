package storage_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	redisstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type memoryEpochAuthority struct {
	mu     sync.Mutex
	epochs map[string]storage.Epoch
}

func newMemoryEpochAuthority() *memoryEpochAuthority {
	return &memoryEpochAuthority{epochs: map[string]storage.Epoch{}}
}
func (a *memoryEpochAuthority) key(tenantID, resource string) string {
	return tenantID + ":" + resource
}
func (a *memoryEpochAuthority) GetEpoch(_ context.Context, tenantID, resource string) (storage.Epoch, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	e := a.epochs[a.key(tenantID, resource)]
	if e == 0 {
		return 1, nil
	}
	return e, nil
}
func (a *memoryEpochAuthority) BumpEpoch(_ context.Context, tenantID, resource string) (storage.Epoch, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	k := a.key(tenantID, resource)
	a.epochs[k]++
	if a.epochs[k] == 1 {
		a.epochs[k] = 2
	}
	return a.epochs[k], nil
}
func (a *memoryEpochAuthority) ValidateEpoch(ctx context.Context, tenantID, resource string, epoch storage.Epoch) error {
	current, err := a.GetEpoch(ctx, tenantID, resource)
	if err != nil {
		return err
	}
	if current != epoch {
		return storage.ErrEpochRejected
	}
	return nil
}

type claimEpochFixture struct {
	name      string
	store     storage.ClaimStore
	authority storage.EpochAuthority
	bump      func(context.Context, string, string) (storage.Epoch, error)
	close     func()
}

func claimEpochContext() tenant.TenantContext {
	return tenant.TenantContext{TenantID: "p006-claim-epoch", AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web", RequestID: "claim-epoch-request", MessageID: "claim-epoch-message", TraceID: "claim-epoch-trace", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "memory"}}
}

func TestRedisClaimRejectsOldAuthorityEpoch(t *testing.T) {
	fixture := newClaimEpochRealFixture(t, true)
	defer fixture.close()
	runClaimEpochContract(t, fixture)
}

func TestClaimEpochEquivalentOutcomes(t *testing.T) {
	if os.Getenv("TEST_REDIS_URL") == "" || os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("real Redis and PostgreSQL are required; Claim epoch outcomes not verified")
	}
	memory := newMemoryEpochAuthority()
	fake := storage.NewFakeCoordinationStoreWithEpochAuthority(memory)
	fakeFixture := claimEpochFixture{name: "fake", store: fake, authority: memory, bump: memory.BumpEpoch, close: func() {}}
	runClaimEpochContract(t, fakeFixture)
	postgres := newClaimEpochRealFixture(t, false)
	defer postgres.close()
	runClaimEpochContract(t, postgres)
	redis := newClaimEpochRealFixture(t, true)
	defer redis.close()
	runClaimEpochContract(t, redis)
}

func runClaimEpochContract(t *testing.T, fixture claimEpochFixture) {
	t.Run(fixture.name, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		tc := claimEpochContext()
		key := storage.DedupKey{TenantID: tc.TenantID, Channel: tc.Channel, BindingID: tc.BindingID, ExternalMessageID: fmt.Sprintf("epoch-old-%d", time.Now().UnixNano())}
		first, err := fixture.store.Claim(ctx, tc, key, 5*time.Second, "owner-1")
		if err != nil {
			t.Fatal(err)
		}
		logClaimEpoch(t, fixture.name, "first", first, nil)
		if first.Epoch != 1 || first.OwnerID != "owner-1" || first.FenceToken == 0 {
			t.Fatalf("invalid first claim: %+v", first)
		}
		guard := storage.OperationGuard{Backend: first.Backend, Epoch: first.Epoch, OwnerID: first.OwnerID, FenceToken: first.FenceToken}
		next, err := fixture.bump(ctx, tc.TenantID, storage.ClaimEpochResource(key))
		if err != nil {
			t.Fatal(err)
		}
		if next != 2 {
			t.Fatalf("bump=%d, want 2", next)
		}
		if err := fixture.store.Complete(ctx, tc, key, first.OwnerID, "stale", guard); !errors.Is(err, storage.ErrEpochRejected) {
			t.Fatalf("old complete classification: %v", err)
		}
		newKey := key
		newKey.ExternalMessageID = fmt.Sprintf("epoch-new-%d", time.Now().UnixNano())
		fresh, err := fixture.store.Claim(ctx, tc, newKey, 5*time.Second, "owner-2")
		if err != nil {
			t.Fatal(err)
		}
		logClaimEpoch(t, fixture.name, "new-epoch", fresh, nil)
		if fresh.Epoch != 2 || fresh.OwnerID != "owner-2" || fresh.FenceToken == 0 {
			t.Fatalf("invalid new epoch claim: %+v", fresh)
		}
		freshGuard := storage.OperationGuard{Backend: fresh.Backend, Epoch: fresh.Epoch, OwnerID: fresh.OwnerID, FenceToken: fresh.FenceToken}
		if err := fixture.store.Fail(ctx, tc, newKey, fresh.OwnerID, freshGuard, true); err != nil {
			t.Fatalf("new epoch fail: %v", err)
		}
		if err := fixture.store.Fail(ctx, tc, key, first.OwnerID, guard, true); !errors.Is(err, storage.ErrEpochRejected) {
			t.Fatalf("old fail classification: %v", err)
		}
	})
}

func logClaimEpoch(t *testing.T, backend, stage string, claim storage.Claim, err error) {
	t.Logf("backend=%s stage=%s owner=%s attempt=%d token=%d epoch=%d expiry=%s error=%v", backend, stage, claim.OwnerID, claim.Attempt, claim.FenceToken, claim.Epoch, claim.ExpiresAt.UTC().Format(time.RFC3339Nano), err)
}

func newClaimEpochRealFixture(t *testing.T, redisOnly bool) claimEpochFixture {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set; Claim epoch not verified")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	cfg := pgstore.PostgresConfig{URL: url, MaxConns: 8, MinConns: 1, AllowDestructiveDown: true}
	base, err := pgstore.NewPool(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	schema := fmt.Sprintf("p006_claim_epoch_%d", time.Now().UnixNano())
	if _, err = base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		base.Close()
		cancel()
		t.Fatal(err)
	}
	cfg.SearchPath = schema
	pool, err := pgstore.NewPool(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	migrator, err := pgstore.NewMigratorWithPool(pool, cfg, os.DirFS(filepath.Join(filepath.Dir(file), "../../migrations")))
	if err != nil {
		t.Fatal(err)
	}
	if err = migrator.Up(ctx); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{`INSERT INTO tenant (tenant_id, name) VALUES ('p006-claim-epoch', 'claim epoch')`, `INSERT INTO agent_app (tenant_id, agent_app_id, name) VALUES ('p006-claim-epoch', 'agent-a', 'claim epoch')`, `INSERT INTO channel_binding (tenant_id, channel, binding_id, external_app_id) VALUES ('p006-claim-epoch', 'web', 'binding-a', 'claim-epoch')`} {
		if _, err = pool.Exec(ctx, query); err != nil {
			t.Fatal(err)
		}
	}
	authority, err := pgstore.NewCoordinationStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	closePG := func() {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
	}
	if !redisOnly {
		return claimEpochFixture{name: "postgres", store: authority, authority: authority, bump: authority.BumpEpoch, close: closePG}
	}
	redisURL := os.Getenv("TEST_REDIS_URL")
	if redisURL == "" {
		closePG()
		t.Skip("TEST_REDIS_URL is not set; Redis Claim epoch not verified")
	}
	prefix := fmt.Sprintf("p006-claim-epoch-%d-", time.Now().UnixNano())
	client, err := redisstore.NewClient(redisstore.Config{URL: redisURL, KeyPrefix: prefix})
	if err != nil {
		closePG()
		t.Fatal(err)
	}
	if err = client.Ping(ctx).Err(); err != nil {
		client.Close()
		closePG()
		t.Fatal(err)
	}
	backend, err := redisstore.NewBackendWithClient(client, redisstore.Config{URL: redisURL, KeyPrefix: prefix})
	if err != nil {
		client.Close()
		closePG()
		t.Fatal(err)
	}
	store := redisstore.NewStoreWithEpochAuthority(backend, authority)
	return claimEpochFixture{name: "redis", store: store, authority: authority, bump: authority.BumpEpoch, close: func() {
		cleanup, cc := context.WithTimeout(context.Background(), 5*time.Second)
		defer cc()
		var cursor uint64
		for {
			keys, next, e := client.Scan(cleanup, cursor, prefix+"*", 100).Result()
			if e != nil {
				break
			}
			if len(keys) > 0 {
				_ = client.Del(cleanup, keys...).Err()
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		_ = backend.Close()
		client.Close()
		closePG()
	}}
}
