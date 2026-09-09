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
	"github.com/redis/go-redis/v9"
)

type leaseBackend struct {
	name  string
	store storage.LeaseStore
	close func()
}

func leaseCrossContext() tenant.TenantContext {
	return tenant.TenantContext{TenantID: "p006-lease-cross", AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web", RequestID: "request-lease-cross", MessageID: "message-lease-cross", TraceID: "trace-lease-cross", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "memory"}}
}

func TestLeaseEquivalentOutcomes(t *testing.T) {
	backends := make([]leaseBackend, 0, 2)
	if url := os.Getenv("TEST_REDIS_URL"); url != "" {
		backends = append(backends, newRedisLeaseCrossBackend(t, url))
	} else {
		t.Log("redis skipped: TEST_REDIS_URL is not set; cross-backend lease not verified")
	}
	if url := os.Getenv("TEST_DATABASE_URL"); url != "" {
		backends = append(backends, newPostgresLeaseCrossBackend(t, url))
	} else {
		t.Log("postgres skipped: TEST_DATABASE_URL is not set; cross-backend lease not verified")
	}
	if len(backends) != 2 {
		t.Skip("cross-backend lease not verified: a real backend is unavailable")
	}
	for _, backend := range backends {
		backend := backend
		t.Run(backend.name, func(t *testing.T) { defer backend.close(); runLeaseSemanticContract(t, backend.name, backend.store) })
	}
}

func runLeaseSemanticContract(t *testing.T, backendName string, store storage.LeaseStore) {
	tc := leaseCrossContext()
	resource := "session-cross"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	first, err := store.Acquire(ctx, tc, resource, "owner-1", 2*time.Second)
	logLease(t, backendName, "first", first, err)
	if err != nil || first.TenantID != tc.TenantID || first.SessionID != resource || first.ResourceID != resource || first.OwnerID != "owner-1" || first.FenceToken == 0 || first.ExpiresAt.IsZero() || first.Backend == "" || first.Epoch == 0 {
		t.Fatalf("invalid first lease: %+v, %v", first, err)
	}
	if err := store.Validate(ctx, tc, first); err != nil {
		t.Fatalf("first lease did not validate: %v", err)
	}

	repeat, err := store.Acquire(ctx, tc, resource, "owner-2", 2*time.Second)
	logLease(t, backendName, "repeat", repeat, err)
	if err == nil {
		if repeat.OwnerID != first.OwnerID || repeat.FenceToken != first.FenceToken {
			t.Fatalf("repeat created a second owner/token: first=%+v repeat=%+v", first, repeat)
		}
	} else if !errors.Is(err, storage.ErrLeaseLost) && !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("unexpected repeat error: %v", err)
	}
	if err := store.Validate(ctx, tc, first); err != nil {
		t.Fatalf("original lease lost after repeat: %v", err)
	}

	beforeRenew := first.ExpiresAt
	renewed, err := store.Renew(ctx, tc, first, 3*time.Second)
	logLease(t, backendName, "renew", renewed, err)
	if err != nil || !renewed.ExpiresAt.After(beforeRenew) || renewed.FenceToken != first.FenceToken || renewed.OwnerID != first.OwnerID {
		t.Fatalf("invalid renewal: before=%+v after=%+v error=%v", first, renewed, err)
	}
	badOwner := first
	badOwner.OwnerID = "wrong-owner"
	if _, err := store.Renew(ctx, tc, badOwner, time.Second); !errors.Is(err, storage.ErrLeaseLost) && !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("wrong owner renew classification: %v", err)
	}
	badToken := first
	badToken.FenceToken--
	if _, err := store.Renew(ctx, tc, badToken, time.Second); !errors.Is(err, storage.ErrLeaseLost) && !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("old token renew classification: %v", err)
	}
	badEpoch := first
	badEpoch.Epoch++
	if _, err := store.Renew(ctx, tc, badEpoch, time.Second); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("bad epoch renew classification: %v", err)
	}
	if err := store.Validate(ctx, tc, badOwner); !errors.Is(err, storage.ErrLeaseLost) && !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("wrong owner validate classification: %v", err)
	}
	if err := store.Validate(ctx, tc, badToken); !errors.Is(err, storage.ErrLeaseLost) && !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("old token validate classification: %v", err)
	}
	if err := store.Validate(ctx, tc, badEpoch); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("bad epoch validate classification: %v", err)
	}

	if err := store.Release(ctx, tc, renewed); err != nil {
		t.Fatalf("valid release failed: %v", err)
	}
	if err := store.Validate(ctx, tc, renewed); !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("released lease validation: %v", err)
	}
	if err := store.Release(ctx, tc, renewed); !errors.Is(err, storage.ErrLeaseLost) && !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("repeat release classification: %v", err)
	}

	resource = "session-cross"
	first, err = store.Acquire(ctx, tc, resource, "owner-1", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	early, err := store.Acquire(ctx, tc, resource, "owner-2", 2*time.Second)
	if err == nil || !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("early takeover classification: lease=%+v error=%v", early, err)
	}
	time.Sleep(2200 * time.Millisecond)
	next, err := store.Acquire(ctx, tc, resource, "owner-2", 2*time.Second)
	logLease(t, backendName, "takeover", next, err)
	if err != nil || next.OwnerID != "owner-2" || next.FenceToken <= first.FenceToken || !next.ExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("invalid takeover: first=%+v next=%+v error=%v", first, next, err)
	}
	if _, err := store.Renew(ctx, tc, first, time.Second); !errors.Is(err, storage.ErrLeaseLost) && !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("old owner renew classification: %v", err)
	}
	if err := store.Release(ctx, tc, first); !errors.Is(err, storage.ErrLeaseLost) && !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("old owner release classification: %v", err)
	}
	if err := store.Validate(ctx, tc, first); !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("old owner validate classification: %v", err)
	}
}

func logLease(t *testing.T, backend, stage string, lease storage.Lease, err error) {
	t.Logf("backend=%s stage=%s owner=%s token=%d expiry=%s epoch=%d session=%s error=%v", backend, stage, lease.OwnerID, lease.FenceToken, lease.ExpiresAt.UTC().Format(time.RFC3339Nano), lease.Epoch, lease.SessionID, err)
}

func newRedisLeaseCrossBackend(t *testing.T, url string) leaseBackend {
	t.Helper()
	opt, err := redis.ParseURL(url)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		cancel()
		t.Fatalf("redis unavailable: %v", err)
	}
	prefix := fmt.Sprintf("p006-lease-cross-%d-", time.Now().UnixNano())
	backend, err := redisstore.NewBackendWithClient(client, redisstore.Config{URL: url, KeyPrefix: prefix, SessionLeaseTTL: 3 * time.Second})
	if err != nil {
		client.Close()
		cancel()
		t.Fatal(err)
	}
	store := redisstore.NewStore(backend)
	tc := leaseCrossContext()
	resource := "session-cross"
	leaseKey, _ := redisLeaseKeyForTest(prefix, tc.TenantID, resource)
	sequenceKey, _ := redisFenceKeyForTest(prefix, tc.TenantID, resource)
	return leaseBackend{name: "redis", store: store, close: func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = client.Del(cleanupCtx, leaseKey, sequenceKey).Result()
		_ = backend.Close()
		cancel()
	}}
}

func redisLeaseKeyForTest(prefix, tenantID, resource string) (string, error) {
	return prefix + ":session-lease:" + tenantID + ":session:" + resource, nil
}
func redisFenceKeyForTest(prefix, tenantID, resource string) (string, error) {
	return prefix + ":fence:session:" + tenantID + ":" + resource, nil
}

func newPostgresLeaseCrossBackend(t *testing.T, url string) leaseBackend {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	cfg := pgstore.PostgresConfig{URL: url, MaxConns: 8, MinConns: 1, AllowDestructiveDown: true}
	base, err := pgstore.NewPool(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	schema := fmt.Sprintf("p006_lease_cross_%d", time.Now().UnixNano())
	if _, err = base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		base.Close()
		cancel()
		t.Fatal(err)
	}
	cfg.SearchPath = schema
	pool, err := pgstore.NewPool(ctx, cfg)
	if err != nil {
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
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
	tc := leaseCrossContext()
	if _, err = pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name) VALUES ('p006-lease-cross', 'lease cross test')`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO agent_app (tenant_id, agent_app_id, name) VALUES ('p006-lease-cross', 'agent-a', 'lease cross test')`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO channel_binding (tenant_id, channel, binding_id, external_app_id) VALUES ('p006-lease-cross', 'web', 'binding-a', 'lease-cross-test')`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO session (tenant_id, session_id, agent_app_id, agent_version, channel, binding_id, external_chat, external_user) VALUES ($1,'session-cross',$2,1,$3,$4,'chat-cross','user-cross')`, tc.TenantID, tc.AgentAppID, tc.Channel, tc.BindingID); err != nil {
		t.Fatal(err)
	}
	store, err := pgstore.NewCoordinationStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	return leaseBackend{name: "postgres", store: store, close: func() {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
	}}
}
