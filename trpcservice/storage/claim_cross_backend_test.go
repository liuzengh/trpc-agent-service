package storage_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	redisstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type claimBackend struct {
	name  string
	store storage.ClaimStore
	close func()
}

func crossBackendContext() tenant.TenantContext {
	return tenant.TenantContext{TenantID: "p006-cross", AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web", RequestID: "request-a", MessageID: "message-a", TraceID: "trace-a", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "memory"}}
}

func TestFakeCoordinationEpoch(t *testing.T) {
	store := storage.NewFakeCoordinationStore()
	tc := crossBackendContext()
	key := storage.DedupKey{TenantID: tc.TenantID, Channel: tc.Channel, BindingID: tc.BindingID, ExternalMessageID: "fake-epoch"}
	ctx := context.Background()
	claim, err := store.Claim(ctx, tc, key, time.Second, "owner-1")
	if err != nil {
		t.Fatal(err)
	}
	good := storage.OperationGuard{Backend: claim.Backend, Epoch: claim.Epoch, OwnerID: claim.OwnerID, FenceToken: claim.FenceToken}
	if err := store.Complete(ctx, tc, key, claim.OwnerID, "ok", good); err != nil {
		t.Fatalf("valid guard rejected: %v", err)
	}
	key.ExternalMessageID = "fake-epoch-reject"
	claim, err = store.Claim(ctx, tc, key, time.Second, "owner-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(ctx, tc, key, claim.OwnerID, "bad", storage.OperationGuard{Backend: claim.Backend, Epoch: claim.Epoch + 1, OwnerID: claim.OwnerID, FenceToken: claim.FenceToken}); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("epoch=%d current=1 owner=%s token=%d: %v", claim.Epoch+1, claim.OwnerID, claim.FenceToken, err)
	}
	if err := store.Fail(ctx, tc, key, claim.OwnerID, storage.OperationGuard{Backend: claim.Backend, Epoch: claim.Epoch, OwnerID: claim.OwnerID, FenceToken: claim.FenceToken - 1}, true); !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("invalid token accepted: %v", err)
	}
	if err := store.Fail(ctx, tc, key, "wrong-owner", storage.OperationGuard{Backend: claim.Backend, Epoch: claim.Epoch, OwnerID: claim.OwnerID, FenceToken: claim.FenceToken}, true); !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("invalid owner accepted: %v", err)
	}
}

func TestClaimEquivalentOutcomes(t *testing.T) {
	backends := []claimBackend{{name: "fake", store: storage.NewFakeCoordinationStore(), close: func() {}}}
	if url := os.Getenv("TEST_REDIS_URL"); url != "" {
		prefix := fmt.Sprintf("p006-cross-%d", time.Now().UnixNano())
		client, err := redisstore.NewClient(redisstore.Config{URL: url, KeyPrefix: prefix})
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Ping(context.Background()).Err(); err != nil {
			client.Close()
			t.Fatal(err)
		}
		backend, err := redisstore.NewBackendWithClient(client, redisstore.Config{URL: url, KeyPrefix: prefix})
		if err != nil {
			client.Close()
			t.Fatal(err)
		}
		backends = append(backends, claimBackend{name: "redis", store: redisstore.NewStore(backend), close: func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var cursor uint64
			for {
				keys, next, scanErr := client.Scan(ctx, cursor, prefix+"*", 100).Result()
				if scanErr != nil {
					break
				}
				if len(keys) > 0 {
					_ = client.Del(ctx, keys...).Err()
				}
				cursor = next
				if cursor == 0 {
					break
				}
			}
			_ = backend.Close()
		}})
	} else {
		t.Log("redis skipped: TEST_REDIS_URL is not set; cross-backend result is not verified")
	}
	if url := os.Getenv("TEST_DATABASE_URL"); url != "" {
		backends = append(backends, newPostgresCrossBackend(t, url))
	} else {
		t.Log("postgres skipped: TEST_DATABASE_URL is not set; cross-backend result is not verified")
	}
	if len(backends) != 3 {
		t.Skip("cross-backend contract not verified: a real backend is unavailable")
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) { defer backend.close(); runClaimSemanticContract(t, backend.name, backend.store) })
	}
}

func runClaimSemanticContract(t *testing.T, backendName string, store storage.ClaimStore) {
	tc := crossBackendContext()
	key := storage.DedupKey{TenantID: tc.TenantID, Channel: tc.Channel, BindingID: tc.BindingID, ExternalMessageID: fmt.Sprintf("message-%d", time.Now().UnixNano())}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	first, err := store.Claim(ctx, tc, key, 2*time.Second, "owner-1")
	logClaim(t, backendName, "first", first, err)
	if err != nil || first.Status != storage.ClaimAcquired || first.OwnerID != "owner-1" || first.Attempt != 1 || first.FenceToken == 0 || first.Epoch == 0 || first.Key != key || first.Backend == "" {
		t.Fatalf("invalid first claim: %+v, %v", first, err)
	}
	second, err := store.Claim(ctx, tc, key, 2*time.Second, "owner-2")
	logClaim(t, backendName, "repeat", second, err)
	if err != nil || second.OwnerID != first.OwnerID || second.Attempt != first.Attempt || second.FenceToken != first.FenceToken || second.Epoch != first.Epoch {
		t.Fatalf("invalid repeat result: %+v, %v", second, err)
	}
	oldGuard := storage.OperationGuard{Backend: first.Backend, Epoch: first.Epoch, OwnerID: first.OwnerID, FenceToken: first.FenceToken}
	if err := store.Complete(ctx, tc, key, "owner-1", "old", oldGuard); err != nil {
		t.Fatalf("initial complete failed: %v", err)
	}
	completed, err := store.Claim(ctx, tc, key, 2*time.Second, "owner-3")
	logClaim(t, backendName, "completed", completed, err)
	if err != nil || completed.Status != storage.ClaimCompleted || completed.OwnerID != first.OwnerID {
		t.Fatalf("completed claim changed: %+v, %v", completed, err)
	}
	key = storage.DedupKey{TenantID: tc.TenantID, Channel: tc.Channel, BindingID: tc.BindingID, ExternalMessageID: fmt.Sprintf("takeover-%d", time.Now().UnixNano())}
	first, err = store.Claim(ctx, tc, key, 2*time.Second, "owner-1")
	if err != nil {
		t.Fatal(err)
	}
	second, err = store.Claim(ctx, tc, key, 2*time.Second, "owner-2")
	if err != nil || second.OwnerID != first.OwnerID {
		t.Fatalf("early takeover accepted: %+v, %v", second, err)
	}
	time.Sleep(2200 * time.Millisecond)
	second, err = store.Claim(ctx, tc, key, 2*time.Second, "owner-2")
	logClaim(t, backendName, "takeover", second, err)
	if err != nil || second.OwnerID != "owner-2" || second.Attempt <= first.Attempt || second.FenceToken <= first.FenceToken {
		t.Fatalf("invalid takeover: %+v, %v", second, err)
	}
	oldGuard = storage.OperationGuard{Backend: first.Backend, Epoch: first.Epoch, OwnerID: first.OwnerID, FenceToken: first.FenceToken}
	if err := store.Complete(ctx, tc, key, first.OwnerID, "stale", oldGuard); !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("stale owner accepted: %v", err)
	}
	if err := store.Complete(ctx, tc, key, "wrong-owner", "stale", storage.OperationGuard{Backend: second.Backend, Epoch: second.Epoch, OwnerID: second.OwnerID, FenceToken: second.FenceToken}); !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("wrong owner classification: %v", err)
	}
	if err := store.Fail(ctx, tc, key, second.OwnerID, storage.OperationGuard{Backend: second.Backend, Epoch: second.Epoch, OwnerID: second.OwnerID, FenceToken: second.FenceToken - 1}, true); !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("old token classification: %v", err)
	}
	if err := store.Complete(ctx, tc, key, second.OwnerID, "bad-epoch", storage.OperationGuard{Backend: second.Backend, Epoch: second.Epoch + 1, OwnerID: second.OwnerID, FenceToken: second.FenceToken}); !errors.Is(err, storage.ErrFenceRejected) && !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("bad epoch classification: %v", err)
	}
}

func logClaim(t *testing.T, backend, stage string, claim storage.Claim, err error) {
	t.Logf("backend=%s stage=%s owner=%s attempt=%d fence=%d epoch=%d status=%s error=%v", backend, stage, claim.OwnerID, claim.Attempt, claim.FenceToken, claim.Epoch, claim.Status, err)
}

func newPostgresCrossBackend(t *testing.T, url string) claimBackend {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	cfg := pgstore.PostgresConfig{URL: url, MaxConns: 8, MinConns: 1, AllowDestructiveDown: true}
	base, err := pgstore.NewPool(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	schema := fmt.Sprintf("p006_cross_%d", time.Now().UnixNano())
	if _, err = base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		base.Close()
		cancel()
		t.Fatal(err)
	}
	cfg.SearchPath = schema
	pool, err := pgstore.NewPool(ctx, cfg)
	if err != nil {
		base.Close()
		cancel()
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
	if _, err = pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name) VALUES ('p006-cross', 'cross test')`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO agent_app (tenant_id, agent_app_id, name) VALUES ('p006-cross', 'agent-a', 'cross test')`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO channel_binding (tenant_id, channel, binding_id, external_app_id) VALUES ('p006-cross', 'web', 'binding-a', 'cross-test')`); err != nil {
		t.Fatal(err)
	}
	store, err := pgstore.NewCoordinationStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	return claimBackend{name: "postgres", store: store, close: func() {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
	}}
}
