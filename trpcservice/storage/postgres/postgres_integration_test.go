package postgres

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	coordination "github.com/liuzengh/trpc-agent-service/trpcservice/storage/coordination"
	redisstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	redisclient "github.com/redis/go-redis/v9"
)

func postgresClaimContext() tenant.TenantContext {
	return tenant.TenantContext{TenantID: "p006-pg", AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web", RequestID: "request-a", MessageID: "message-a", TraceID: "trace-a", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "memory"}}
}

func postgresClaimFixture(t *testing.T, ttl time.Duration) (*CoordinationStore, *pgxpool.Pool, context.Context, tenant.TenantContext, storage.DedupKey) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set; PostgreSQL Claim not verified")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	cfg := PostgresConfig{URL: url, MaxConns: 8, MinConns: 1, AllowDestructiveDown: true}
	base, err := NewPool(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	schema := fmt.Sprintf("p006_claim_%d", time.Now().UnixNano())
	if _, err = base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		base.Close()
		cancel()
		t.Fatal(err)
	}
	cfg.SearchPath = schema
	pool, err := NewPool(ctx, cfg)
	if err != nil {
		base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	source, _ := os.DirFS(filepath.Join(mustCallerDir(t), "../../../migrations")).(fstest.MapFS)
	_ = source
	_, file, _, _ := runtime.Caller(0)
	migrator, err := NewMigratorWithPool(pool, cfg, os.DirFS(filepath.Join(filepath.Dir(file), "../../../migrations")))
	if err != nil {
		t.Fatal(err)
	}
	if err = migrator.Up(ctx); err != nil {
		t.Fatal(err)
	}
	// message_dedup references the tenant-qualified channel_binding key.
	if _, err = pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name) VALUES ('p006-pg', 'claim test')`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO agent_app (tenant_id, agent_app_id, name) VALUES ('p006-pg', 'agent-a', 'claim test')`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO channel_binding (tenant_id, channel, binding_id, external_app_id) VALUES ('p006-pg', 'web', 'binding-a', 'claim-test')`); err != nil {
		t.Fatal(err)
	}
	store, err := NewCoordinationStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	tc := postgresClaimContext()
	key := storage.DedupKey{TenantID: tc.TenantID, Channel: tc.Channel, BindingID: tc.BindingID, ExternalMessageID: fmt.Sprintf("message-%d", time.Now().UnixNano())}
	t.Cleanup(func() {
		pool.Close()
		base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
	})
	return store, pool, ctx, tc, key
}

func postgresLeaseFixture(t *testing.T) (*CoordinationStore, *pgxpool.Pool, context.Context, tenant.TenantContext, string) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set; PostgreSQL lease not verified")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	cfg := PostgresConfig{URL: url, MaxConns: 8, MinConns: 1, AllowDestructiveDown: true}
	base, err := NewPool(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	schema := fmt.Sprintf("p006_lease_%d", time.Now().UnixNano())
	if _, err = base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		base.Close()
		cancel()
		t.Fatal(err)
	}
	cfg.SearchPath = schema
	pool, err := NewPool(ctx, cfg)
	if err != nil {
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	migrator, err := NewMigratorWithPool(pool, cfg, os.DirFS(filepath.Join(filepath.Dir(file), "../../../migrations")))
	if err != nil {
		t.Fatal(err)
	}
	if err = migrator.Up(ctx); err != nil {
		t.Fatal(err)
	}
	tc := tenant.TenantContext{TenantID: "p006-lease", AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web", RequestID: "request-lease", MessageID: "message-lease", TraceID: "trace-lease", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "memory"}}
	if _, err = pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name) VALUES ('p006-lease', 'lease test')`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO agent_app (tenant_id, agent_app_id, name) VALUES ('p006-lease', 'agent-a', 'lease test')`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO channel_binding (tenant_id, channel, binding_id, external_app_id) VALUES ('p006-lease', 'web', 'binding-a', 'lease-test')`); err != nil {
		t.Fatal(err)
	}
	sessionID := fmt.Sprintf("session-%d", time.Now().UnixNano())
	if _, err = pool.Exec(ctx, `INSERT INTO session (tenant_id, session_id, agent_app_id, agent_version, channel, binding_id, external_chat, external_user) VALUES ($1,$2,$3,1,$4,$5,'chat-lease','user-lease')`, tc.TenantID, sessionID, tc.AgentAppID, tc.Channel, tc.BindingID); err != nil {
		t.Fatal(err)
	}
	store, err := NewCoordinationStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
	})
	return store, pool, ctx, tc, sessionID
}

func waitForRunnerExpiry(t *testing.T, runner *storage.LeaseRenewalRunner, initial time.Time) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if runner.Lease().ExpiresAt.After(initial) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("runner did not renew before deadline: initial=%s latest=%s", initial, runner.Lease().ExpiresAt)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPostgresLeaseRenewalRunner(t *testing.T) {
	store, _, ctx, tc, sessionID := postgresLeaseFixture(t)
	lease, err := store.Acquire(ctx, tc, sessionID, "runner-owner", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	parent, cancel := context.WithCancel(ctx)
	runner, err := storage.NewLeaseRenewalRunner(parent, store, tc, lease, time.Second, 200*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Start(); err != nil {
		t.Fatal(err)
	}
	waitForRunnerExpiry(t, runner, lease.ExpiresAt)
	latest := runner.Lease()
	if latest.OwnerID != lease.OwnerID || latest.FenceToken != lease.FenceToken || latest.Epoch != lease.Epoch {
		t.Fatalf("lease identity changed: initial=%+v latest=%+v", lease, latest)
	}
	if err := store.Validate(ctx, tc, latest); err != nil {
		t.Fatalf("renewed lease failed validation: %v", err)
	}
	if _, err := store.BumpEpoch(ctx, tc.TenantID, sessionID); err != nil {
		t.Fatal(err)
	}
	if err := runner.Wait(); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("runner error after epoch bump: %v", err)
	}
	cancel()
}

func TestLeaseRenewalRunnerEquivalentOutcomes(t *testing.T) {
	if os.Getenv("TEST_REDIS_URL") == "" || os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("TEST_REDIS_URL and TEST_DATABASE_URL are required; runner semantic test not verified")
	}
	pgStore, _, ctx, tc, sessionID := postgresLeaseFixture(t)
	redisURL := os.Getenv("TEST_REDIS_URL")
	opt, err := redisclient.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	client := redisclient.NewClient(opt)
	prefix := fmt.Sprintf("p006-runner-%d-", time.Now().UnixNano())
	backend, err := redisstore.NewBackendWithClient(client, redisstore.Config{URL: redisURL, KeyPrefix: prefix, SessionLeaseTTL: 100 * time.Millisecond})
	if err != nil {
		client.Close()
		t.Fatal(err)
	}
	redisStore := redisstore.NewStoreWithEpochAuthority(backend, pgStore)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		var cursor uint64
		for {
			keys, next, scanErr := client.Scan(cleanupCtx, cursor, prefix+"*", 100).Result()
			if scanErr != nil {
				break
			}
			if len(keys) > 0 {
				_ = client.Del(cleanupCtx, keys...).Err()
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		_ = backend.Close()
		_ = client.Close()
	})

	t.Run("redis", func(t *testing.T) {
		lease, acquireErr := redisStore.Acquire(ctx, tc, sessionID, "redis-runner", 3*time.Second)
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		parent, cancel := context.WithCancel(ctx)
		runner, runnerErr := storage.NewLeaseRenewalRunner(parent, redisStore, tc, lease, 3*time.Second, 500*time.Millisecond, nil)
		if runnerErr != nil {
			t.Fatal(runnerErr)
		}
		if err := runner.Start(); err != nil {
			t.Fatal(err)
		}
		waitForRunnerExpiry(t, runner, lease.ExpiresAt)
		if runner.Lease().OwnerID != lease.OwnerID || runner.Lease().FenceToken != lease.FenceToken || runner.Lease().Epoch != lease.Epoch {
			t.Fatalf("Redis lease identity changed: initial=%+v latest=%+v", lease, runner.Lease())
		}
		cancel()
		if err := runner.Wait(); !errors.Is(err, context.Canceled) {
			t.Fatalf("Redis cancellation error: %v", err)
		}
		if _, err := pgStore.BumpEpoch(ctx, tc.TenantID, sessionID); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("postgres", func(t *testing.T) {
		lease, acquireErr := pgStore.Acquire(ctx, tc, sessionID, "postgres-runner", time.Second)
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
		parent, cancel := context.WithCancel(ctx)
		runner, runnerErr := storage.NewLeaseRenewalRunner(parent, pgStore, tc, lease, time.Second, 200*time.Millisecond, nil)
		if runnerErr != nil {
			t.Fatal(runnerErr)
		}
		if err := runner.Start(); err != nil {
			t.Fatal(err)
		}
		waitForRunnerExpiry(t, runner, lease.ExpiresAt)
		if runner.Lease().OwnerID != lease.OwnerID || runner.Lease().FenceToken != lease.FenceToken || runner.Lease().Epoch != lease.Epoch {
			t.Fatalf("PostgreSQL lease identity changed: initial=%+v latest=%+v", lease, runner.Lease())
		}
		if _, err := pgStore.BumpEpoch(ctx, tc.TenantID, sessionID); err != nil {
			t.Fatal(err)
		}
		if err := runner.Wait(); !errors.Is(err, storage.ErrEpochRejected) {
			t.Fatalf("PostgreSQL epoch rejection error: %v", err)
		}
		cancel()
	})
}

type injectedLeaseStore struct {
	storage.LeaseStore
	fail bool
}

func (s *injectedLeaseStore) Acquire(ctx context.Context, tc tenant.TenantContext, resource, owner string, ttl time.Duration) (storage.Lease, error) {
	if s.fail {
		return storage.Lease{}, storage.ErrBackendUnavailable
	}
	return s.LeaseStore.Acquire(ctx, tc, resource, owner, ttl)
}

func TestFailoverStoreAutomaticSwitchRealBackends(t *testing.T) {
	if os.Getenv("TEST_REDIS_URL") == "" || os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("TEST_REDIS_URL and TEST_DATABASE_URL are required; real FailoverStore contract not verified")
	}
	pgStore, _, ctx, tc, sessionID := postgresLeaseFixture(t)
	redisURL := os.Getenv("TEST_REDIS_URL")
	client, err := redisstore.NewClient(redisstore.Config{URL: redisURL, KeyPrefix: fmt.Sprintf("p006-failover-%d-", time.Now().UnixNano())})
	if err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("p006-failover-%d-", time.Now().UnixNano())
	backend, err := redisstore.NewBackendWithClient(client, redisstore.Config{URL: redisURL, KeyPrefix: prefix, SessionLeaseTTL: 3 * time.Second})
	if err != nil {
		client.Close()
		t.Fatal(err)
	}
	redisStore := redisstore.NewStoreWithEpochAuthority(backend, pgStore)
	injected := &injectedLeaseStore{LeaseStore: redisStore}
	var probeErr error
	store := coordination.NewFailoverStoreWithAuthority(
		coordination.Endpoint{Name: storage.BackendRedis, Leases: injected, Probe: func(context.Context) error { return probeErr }},
		coordination.Endpoint{Name: storage.BackendPostgres, Leases: pgStore},
		pgStore, tc.TenantID, sessionID, coordination.Config{Now: time.Now},
	)
	if err := store.Initialize(ctx); err != nil {
		backend.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var cursor uint64
		for {
			keys, next, scanErr := client.Scan(cleanup, cursor, prefix+"*", 100).Result()
			if scanErr != nil {
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
		_ = client.Close()
	})
	oldLease, err := store.Acquire(ctx, tc, sessionID, "redis-owner", 3*time.Second)
	if err != nil || oldLease.Backend != storage.BackendRedis || oldLease.Epoch != 1 {
		t.Fatalf("initial lease=%+v err=%v", oldLease, err)
	}
	injected.fail = true
	if _, err := store.Acquire(ctx, tc, sessionID, "blocked-owner", 3*time.Second); !errors.Is(err, storage.ErrOperationAmbiguous) {
		t.Fatalf("injected unavailable=%v", err)
	}
	if store.State() != coordination.Quarantined {
		t.Fatalf("state=%s", store.State())
	}
	newEpoch, err := store.AutomaticSwitch(ctx)
	if err != nil || newEpoch != 2 {
		t.Fatalf("switch epoch=%d err=%v", newEpoch, err)
	}
	newLease, err := store.Acquire(ctx, tc, sessionID, "postgres-owner", time.Second)
	if err != nil || newLease.Backend != storage.BackendPostgres || newLease.Epoch != 2 {
		t.Fatalf("secondary lease=%+v err=%v", newLease, err)
	}
	if err := store.Validate(ctx, tc, newLease); err != nil {
		t.Fatalf("secondary lease validation=%v", err)
	}
	if err := store.Release(ctx, tc, oldLease); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old Redis release=%v", err)
	}
	beforeProbeEpoch := store.Epoch()
	probeErr = storage.ErrBackendUnavailable
	if err := store.Probe(ctx); err == nil || store.State() != coordination.Quarantined || store.Epoch() != beforeProbeEpoch {
		t.Fatalf("failed probe state=%s epoch=%d err=%v", store.State(), store.Epoch(), err)
	}
	probeErr = nil
	injected.fail = false
	if err := store.Probe(ctx); err != nil || store.State() != coordination.Recovering {
		t.Fatalf("successful probe state=%s err=%v", store.State(), err)
	}
	recoveredEpoch, err := store.Recover(ctx)
	if err != nil || recoveredEpoch != 3 || store.Epoch() != 3 || store.State() != coordination.ActivePrimary {
		t.Fatalf("recover epoch=%d state=%s err=%v", recoveredEpoch, store.State(), err)
	}
	if wait := time.Until(oldLease.ExpiresAt) + 100*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
	primaryLease, err := store.Acquire(ctx, tc, sessionID, "recovered-redis-owner", time.Second)
	if err != nil || primaryLease.Backend != storage.BackendRedis || primaryLease.Epoch != 3 {
		t.Fatalf("recovered lease=%+v err=%v", primaryLease, err)
	}
}

func TestPostgresLeaseAcquireRenewReleaseValidate(t *testing.T) {
	store, _, ctx, tc, sessionID := postgresLeaseFixture(t)
	lease, err := store.Acquire(ctx, tc, sessionID, "owner-1", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease.TenantID != tc.TenantID || lease.SessionID != sessionID || lease.ResourceID != sessionID || lease.OwnerID != "owner-1" || lease.FenceToken == 0 || lease.ExpiresAt.IsZero() || lease.Backend != storage.BackendPostgres || lease.Epoch == 0 {
		t.Fatalf("invalid lease: %+v", lease)
	}
	var persistedOwner string
	var persistedToken uint64
	var persistedExpiry time.Time
	if err := store.pool.QueryRow(ctx, `SELECT owner_id, fencing_token, leased_until FROM session_lease WHERE tenant_id=$1 AND session_id=$2`, tc.TenantID, sessionID).Scan(&persistedOwner, &persistedToken, &persistedExpiry); err != nil {
		t.Fatal(err)
	}
	if persistedOwner != lease.OwnerID || persistedToken != lease.FenceToken || !persistedExpiry.Equal(lease.ExpiresAt) {
		t.Fatalf("lease was not persisted: owner=%s token=%d expiry=%s lease=%+v", persistedOwner, persistedToken, persistedExpiry, lease)
	}
	if err := store.Validate(ctx, tc, lease); err != nil {
		t.Fatalf("valid lease failed validation: %v", err)
	}
	originalExpiry := lease.ExpiresAt
	renewed, err := store.Renew(ctx, tc, lease, 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !renewed.ExpiresAt.After(originalExpiry) || renewed.FenceToken != lease.FenceToken {
		t.Fatalf("invalid renewal: before=%+v after=%+v", lease, renewed)
	}
	wrongOwner := lease
	wrongOwner.OwnerID = "wrong-owner"
	if _, err := store.Renew(ctx, tc, wrongOwner, time.Second); !errors.Is(err, storage.ErrLeaseLost) && !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("wrong owner error: %v", err)
	}
	oldToken := lease
	oldToken.FenceToken--
	if _, err := store.Renew(ctx, tc, oldToken, time.Second); !errors.Is(err, storage.ErrLeaseLost) && !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("old token error: %v", err)
	}
	badEpoch := lease
	badEpoch.Epoch++
	if _, err := store.Renew(ctx, tc, badEpoch, time.Second); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("bad epoch error: %v", err)
	}
	if err := store.Release(ctx, tc, renewed); err != nil {
		t.Fatalf("valid release failed: %v", err)
	}
	if err := store.Validate(ctx, tc, renewed); !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("released validation error: %v", err)
	}
	if err := store.Release(ctx, tc, renewed); !errors.Is(err, storage.ErrFenceRejected) && !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("repeat release error: %v", err)
	}
}

func TestPostgresLeaseConcurrentAcquire(t *testing.T) {
	store, _, ctx, tc, sessionID := postgresLeaseFixture(t)
	type result struct {
		requested string
		lease     storage.Lease
		err       error
	}
	const n = 100
	results := make(chan result, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		owner := fmt.Sprintf("owner-%d", i)
		wg.Add(1)
		go func(owner string) {
			defer wg.Done()
			<-start
			lease, err := store.Acquire(ctx, tc, sessionID, owner, 2*time.Second)
			results <- result{owner, lease, err}
		}(owner)
	}
	close(start)
	wg.Wait()
	close(results)
	owners, tokens, epochs, errorsByClass := map[string]int{}, map[uint64]int{}, map[storage.Epoch]int{}, map[string]int{}
	for r := range results {
		if r.err != nil {
			errorsByClass[fmt.Sprintf("%T:%v", r.err, r.err)]++
			if !errors.Is(r.err, storage.ErrLeaseLost) && !errors.Is(r.err, storage.ErrFenceRejected) {
				t.Fatalf("owner=%s unexpected acquire error: %v", r.requested, r.err)
			}
			continue
		}
		owners[r.lease.OwnerID]++
		tokens[r.lease.FenceToken]++
		epochs[r.lease.Epoch]++
	}
	t.Logf("owners=%v tokens=%v epochs=%v errors=%v", owners, tokens, epochs, errorsByClass)
	if len(owners) != 1 || len(tokens) != 1 || len(epochs) != 1 {
		t.Fatalf("lease metadata split: owners=%v tokens=%v epochs=%v errors=%v", owners, tokens, epochs, errorsByClass)
	}
}

func TestPostgresLeaseTakeover(t *testing.T) {
	store, _, ctx, tc, sessionID := postgresLeaseFixture(t)
	first, err := store.Acquire(ctx, tc, sessionID, "owner-1", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	early, err := store.Acquire(ctx, tc, sessionID, "owner-2", time.Second)
	if !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("early takeover error=%v lease=%+v", err, early)
	}
	time.Sleep(1200 * time.Millisecond)
	next, err := store.Acquire(ctx, tc, sessionID, "owner-2", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if next.OwnerID != "owner-2" || next.FenceToken <= first.FenceToken || !next.ExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("invalid takeover first=%+v next=%+v", first, next)
	}
	var persistedOwner string
	var persistedToken uint64
	var persistedExpiry time.Time
	if err := store.pool.QueryRow(ctx, `SELECT owner_id, fencing_token, leased_until FROM session_lease WHERE tenant_id=$1 AND session_id=$2`, tc.TenantID, sessionID).Scan(&persistedOwner, &persistedToken, &persistedExpiry); err != nil {
		t.Fatal(err)
	}
	if persistedOwner != next.OwnerID || persistedToken != next.FenceToken || persistedExpiry.Sub(next.ExpiresAt) > time.Microsecond || next.ExpiresAt.Sub(persistedExpiry) > time.Microsecond {
		t.Fatalf("takeover not persisted: owner=%s token=%d expiry=%s lease=%+v", persistedOwner, persistedToken, persistedExpiry, next)
	}
	old := first
	if _, err := store.Renew(ctx, tc, old, time.Second); !errors.Is(err, storage.ErrLeaseLost) && !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("old renew error: %v", err)
	}
	if err := store.Release(ctx, tc, old); !errors.Is(err, storage.ErrFenceRejected) && !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("old release error: %v", err)
	}
	if err := store.Validate(ctx, tc, old); !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("old validate error: %v", err)
	}
}

func TestPostgresEpochAuthorityGetAndBump(t *testing.T) {
	store, _, ctx, tc, sessionID := postgresLeaseFixture(t)
	initial, err := store.GetEpoch(ctx, tc.TenantID, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if initial != 1 {
		t.Fatalf("initial epoch=%d, want 1", initial)
	}
	first, err := store.BumpEpoch(ctx, tc.TenantID, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.BumpEpoch(ctx, tc.TenantID, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if first != 2 || second != 3 {
		t.Fatalf("bumps=%d,%d, want 2,3", first, second)
	}
	got, err := store.GetEpoch(ctx, tc.TenantID, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if got != second {
		t.Fatalf("get=%d, want %d", got, second)
	}
	if err := store.ValidateEpoch(ctx, tc.TenantID, sessionID, second); err != nil {
		t.Fatalf("current epoch rejected: %v", err)
	}
	if err := store.ValidateEpoch(ctx, tc.TenantID, sessionID, first); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old epoch error: %v", err)
	}
	if err := store.ValidateEpoch(ctx, tc.TenantID, "other-resource", second); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("unregistered resource error: %v", err)
	}
}

func TestPostgresEpochAuthorityConcurrentBump(t *testing.T) {
	store, pool, ctx, tc, sessionID := postgresLeaseFixture(t)
	const n = 100
	values := make(chan storage.Epoch, n)
	errs := make(chan error, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			epoch, err := store.BumpEpoch(ctx, tc.TenantID, sessionID)
			if err != nil {
				errs <- err
				return
			}
			values <- epoch
		}()
	}
	close(start)
	wg.Wait()
	close(values)
	close(errs)
	seen := map[storage.Epoch]bool{}
	for err := range errs {
		t.Fatalf("concurrent bump error: %v", err)
	}
	for epoch := range values {
		if seen[epoch] {
			t.Fatalf("duplicate epoch %d", epoch)
		}
		seen[epoch] = true
	}
	if len(seen) != n {
		t.Fatalf("successful bumps=%d, want %d", len(seen), n)
	}
	for epoch := storage.Epoch(2); epoch <= n+1; epoch++ {
		if !seen[epoch] {
			t.Fatalf("missing epoch %d", epoch)
		}
	}
	final, err := store.GetEpoch(ctx, tc.TenantID, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if final != n+1 {
		t.Fatalf("final epoch=%d, want %d", final, n+1)
	}
	other, err := store.BumpEpoch(ctx, tc.TenantID, "other-resource")
	if err != nil {
		t.Fatal(err)
	}
	if other != 2 {
		t.Fatalf("isolated resource epoch=%d, want 2", other)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name) VALUES ('p006-lease-other', 'lease other tenant')`); err != nil {
		t.Fatal(err)
	}
	otherTenant, err := store.BumpEpoch(ctx, "p006-lease-other", sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if otherTenant != 2 {
		t.Fatalf("isolated tenant epoch=%d, want 2", otherTenant)
	}
}

func TestPostgresEpochAuthorityContextCancel(t *testing.T) {
	store, _, ctx, tc, sessionID := postgresLeaseFixture(t)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.GetEpoch(cancelled, tc.TenantID, sessionID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Get error: %v", err)
	}
	if _, err := store.BumpEpoch(cancelled, tc.TenantID, sessionID); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Bump error: %v", err)
	}
	if err := store.ValidateEpoch(cancelled, tc.TenantID, sessionID, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Validate error: %v", err)
	}
	got, err := store.GetEpoch(ctx, tc.TenantID, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Fatalf("cancelled operations changed epoch to %d", got)
	}
}

func TestPostgresLeaseRejectsOldEpochAfterBump(t *testing.T) {
	store, _, ctx, tc, sessionID := postgresLeaseFixture(t)
	oldLease, err := store.Acquire(ctx, tc, sessionID, "owner-1", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	newEpoch, err := store.BumpEpoch(ctx, tc.TenantID, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if newEpoch != oldLease.Epoch+1 {
		t.Fatalf("new epoch=%d old=%d", newEpoch, oldLease.Epoch)
	}
	if _, err := store.Renew(ctx, tc, oldLease, time.Second); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old renew error: %v", err)
	}
	if err := store.Release(ctx, tc, oldLease); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old release error: %v", err)
	}
	if err := store.Validate(ctx, tc, oldLease); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old validate error: %v", err)
	}
	newLease, err := store.Acquire(ctx, tc, sessionID, "owner-2", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if newLease.Epoch != newEpoch || newLease.OwnerID != "owner-2" || newLease.FenceToken <= oldLease.FenceToken || !newLease.ExpiresAt.After(time.Now().UTC()) {
		t.Fatalf("invalid new lease: old=%+v new=%+v", oldLease, newLease)
	}
	if err := store.Validate(ctx, tc, newLease); err != nil {
		t.Fatalf("new lease validation: %v", err)
	}
}

func TestPostgresClaimRejectsOldEpochAfterBump(t *testing.T) {
	store, pool, ctx, tc, key := postgresClaimFixture(t, 5*time.Second)
	claim, err := store.Claim(ctx, tc, key, 5*time.Second, "owner-1")
	if err != nil {
		t.Fatal(err)
	}
	guard := storage.OperationGuard{Backend: claim.Backend, Epoch: claim.Epoch, OwnerID: claim.OwnerID, FenceToken: claim.FenceToken}
	next, err := store.BumpEpoch(ctx, key.TenantID, storage.ClaimEpochResource(key))
	if err != nil {
		t.Fatal(err)
	}
	if next != claim.Epoch+1 {
		t.Fatalf("bump=%d claim epoch=%d", next, claim.Epoch)
	}
	if err := store.Complete(ctx, tc, key, claim.OwnerID, "stale", guard); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old complete error: %v", err)
	}
	var status string
	var response *string
	if err := pool.QueryRow(ctx, `SELECT status, response_ref FROM message_dedup WHERE tenant_id=$1 AND channel=$2 AND binding_id=$3 AND external_message_id=$4`, key.TenantID, key.Channel, key.BindingID, key.ExternalMessageID).Scan(&status, &response); err != nil {
		t.Fatal(err)
	}
	if status != string(storage.ClaimAcquired) || response != nil {
		t.Fatalf("stale complete changed claim: status=%s response=%v", status, response)
	}
	newClaim, err := store.Claim(ctx, tc, key, 5*time.Second, "owner-2")
	if err != nil {
		t.Fatal(err)
	}
	if newClaim.Epoch != next || newClaim.OwnerID != "owner-2" || newClaim.FenceToken <= claim.FenceToken {
		t.Fatalf("invalid new epoch claim: old=%+v new=%+v", claim, newClaim)
	}
	newGuard := storage.OperationGuard{Backend: newClaim.Backend, Epoch: newClaim.Epoch, OwnerID: newClaim.OwnerID, FenceToken: newClaim.FenceToken}
	if err := store.Fail(ctx, tc, key, newClaim.OwnerID, newGuard, true); err != nil {
		t.Fatalf("new epoch fail: %v", err)
	}
}

func TestPostgresClaimUsesCurrentAuthorityEpoch(t *testing.T) {
	store, _, ctx, tc, key := postgresClaimFixture(t, time.Second)
	if _, err := store.BumpEpoch(ctx, key.TenantID, storage.ClaimEpochResource(key)); err != nil {
		t.Fatal(err)
	}
	claim, err := store.Claim(ctx, tc, key, time.Second, "owner-1")
	if err != nil {
		t.Fatal(err)
	}
	if claim.Epoch != 2 {
		t.Fatalf("claim epoch=%d, want 2", claim.Epoch)
	}
	if err := store.Complete(ctx, tc, key, claim.OwnerID, "ok", storage.OperationGuard{Backend: claim.Backend, Epoch: claim.Epoch, OwnerID: claim.OwnerID, FenceToken: claim.FenceToken}); err != nil {
		t.Fatal(err)
	}
	if err := store.ValidateEpoch(ctx, key.TenantID, storage.ClaimEpochResource(key), claim.Epoch); err != nil {
		t.Fatal(err)
	}
}

func mustCallerDir(t *testing.T) string {
	_, file, _, ok := runtime.Caller(1)
	if !ok {
		t.Fatal("caller")
	}
	return filepath.Dir(file)
}

func TestPostgresClaimIntegration(t *testing.T) {
	s, _, ctx, tc, key := postgresClaimFixture(t, time.Second)
	c, err := s.Claim(ctx, tc, key, time.Second, "owner-1")
	if err != nil {
		t.Fatal(err)
	}
	if c.Status != storage.ClaimAcquired || c.OwnerID != "owner-1" || c.Attempt != 1 || c.FenceToken == 0 || c.Epoch == 0 || c.Backend != storage.BackendPostgres || c.Key != key {
		t.Fatalf("invalid claim: %+v", c)
	}
}
func TestPostgresClaimRepeatAcquire(t *testing.T) {
	s, _, ctx, tc, key := postgresClaimFixture(t, 2*time.Second)
	first, err := s.Claim(ctx, tc, key, 2*time.Second, "owner-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, o := range []string{"owner-2", "owner-3"} {
		c, e := s.Claim(ctx, tc, key, 2*time.Second, o)
		if e != nil {
			t.Fatal(e)
		}
		if c.OwnerID != first.OwnerID || c.Attempt != first.Attempt || c.FenceToken != first.FenceToken {
			t.Fatalf("unstable repeat: %+v", c)
		}
	}
}
func TestPostgresClaimConcurrency(t *testing.T) {
	s, _, ctx, tc, key := postgresClaimFixture(t, 2*time.Second)
	type r struct {
		c storage.Claim
		e error
	}
	out := make(chan r, 100)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		o := fmt.Sprintf("owner-%d", i)
		wg.Add(1)
		go func() { defer wg.Done(); c, e := s.Claim(ctx, tc, key, 2*time.Second, o); out <- r{c, e} }()
	}
	wg.Wait()
	close(out)
	owners := map[string]bool{}
	attempts := map[int]bool{}
	fences := map[uint64]bool{}
	for x := range out {
		if x.e != nil {
			t.Fatal(x.e)
		}
		owners[x.c.OwnerID] = true
		attempts[x.c.Attempt] = true
		fences[x.c.FenceToken] = true
	}
	t.Logf("owners=%v attempts=%v fences=%v", owners, attempts, fences)
	if len(owners) != 1 || len(attempts) != 1 || len(fences) != 1 {
		t.Fatal("split claim")
	}
}
func TestPostgresClaimTakeover(t *testing.T) {
	s, _, ctx, tc, key := postgresClaimFixture(t, time.Second)
	a, e := s.Claim(ctx, tc, key, time.Second, "owner-1")
	if e != nil {
		t.Fatal(e)
	}
	b, e := s.Claim(ctx, tc, key, time.Second, "owner-2")
	if e != nil || b.OwnerID != a.OwnerID {
		t.Fatalf("early takeover: %+v %v", b, e)
	}
	time.Sleep(1200 * time.Millisecond)
	b, e = s.Claim(ctx, tc, key, 2*time.Second, "owner-2")
	if e != nil || b.OwnerID != "owner-2" || b.Attempt <= a.Attempt || b.FenceToken <= a.FenceToken {
		t.Fatalf("takeover: %+v %v", b, e)
	}
}
func TestPostgresClaimFencing(t *testing.T) {
	s, _, ctx, tc, key := postgresClaimFixture(t, time.Second)
	c, e := s.Claim(ctx, tc, key, time.Second, "owner-1")
	if e != nil {
		t.Fatal(e)
	}
	g := storage.OperationGuard{Backend: storage.BackendPostgres, OwnerID: c.OwnerID, FenceToken: c.FenceToken, Epoch: c.Epoch}
	if e = s.Complete(ctx, tc, key, c.OwnerID, "ok", g); e != nil {
		t.Fatal(e)
	}
	if e = s.Complete(ctx, tc, key, "wrong", "bad", g); e == nil {
		t.Fatal("wrong owner accepted")
	}
}

func TestPostgreSQLMigrations(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set; PostgreSQL integration test skipped")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := PostgresConfig{URL: url, MaxConns: 2, MinConns: 1, AllowDestructiveDown: true}
	basePool, err := NewPool(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("p0_05_%d", time.Now().UnixNano())
	if _, err := basePool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		basePool.Close()
		t.Fatal(err)
	}
	cfg.SearchPath = schema
	pool, err := NewPool(ctx, cfg)
	if err != nil {
		_, _ = basePool.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		basePool.Close()
		t.Fatal(err)
	}
	defer func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = basePool.Exec(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
		basePool.Close()
	}()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate repository root")
	}
	source := os.DirFS(filepath.Join(filepath.Dir(file), "../../../migrations"))
	loaded, err := loadMigrations(source)
	if err != nil || len(loaded) < 1 {
		t.Fatalf("load current migrations: %v", err)
	}
	migrator, err := NewMigratorWithPool(pool, cfg, source)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := migrator.Down(cleanupCtx, len(loaded)); err != nil {
			t.Logf("test database cleanup failed: %v", err)
		}
	}()

	if err := migrator.Up(ctx); err != nil {
		t.Fatal(err)
	}
	current, err := migrator.Current(ctx)
	if err != nil || current.Version != loaded[len(loaded)-1].version {
		t.Fatalf("unexpected current migration: %+v, %v", current, err)
	}
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("repeat Up failed: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM schema_migration WHERE version = 1`); err != nil {
		t.Fatal(err)
	}
	if err := migrator.Up(ctx); err == nil || !errors.Is(err, ErrMigrationMissingVersion) {
		t.Fatalf("expected missing version failure, got %v", err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO schema_migration (version, name, checksum) VALUES ($1, $2, $3)`, loaded[0].version, loaded[0].name, loaded[0].checksum); err != nil {
		t.Fatal(err)
	}

	checksumSource := fstest.MapFS{
		"000001_changed.up.sql":   {Data: []byte("SELECT 1")},
		"000001_changed.down.sql": {Data: []byte("SELECT 1")},
	}
	for _, name := range []string{
		"000002_coordination.up.sql", "000002_coordination.down.sql",
		"000003_execution_result.up.sql", "000003_execution_result.down.sql",
		"000004_job_queue.up.sql", "000004_job_queue.down.sql",
		"000005_outbox_repository.up.sql", "000005_outbox_repository.down.sql",
		"000006_p1_04_binding_identity.up.sql", "000006_p1_04_binding_identity.down.sql",
		"000007_vector_projection_task.up.sql", "000007_vector_projection_task.down.sql",
		"000008_vector_rebuild_run.up.sql", "000008_vector_rebuild_run.down.sql",
		"000009_p1_08_config_publication.up.sql", "000009_p1_08_config_publication.down.sql",
		"000010_p2_01_row_level_security.up.sql", "000010_p2_01_row_level_security.down.sql",
		"000011_p2_02_restore_trigger_compat.up.sql", "000011_p2_02_restore_trigger_compat.down.sql",
	} {
		data, readErr := fs.ReadFile(source, name)
		if readErr != nil {
			t.Fatal(readErr)
		}
		checksumSource[name] = &fstest.MapFile{Data: data}
	}
	checksumMigrator, err := NewMigratorWithPool(pool, cfg, checksumSource)
	if err != nil {
		t.Fatal(err)
	}
	if err := checksumMigrator.Up(ctx); err == nil || !errors.Is(err, ErrMigrationChecksum) {
		t.Fatalf("expected checksum mismatch, got %v", err)
	}

	if _, err := pool.Exec(ctx, `INSERT INTO schema_migration (version, name, checksum) VALUES (99, 'unknown', 'unknown')`); err != nil {
		t.Fatal(err)
	}
	if err := migrator.Up(ctx); err == nil || !errors.Is(err, ErrUnknownMigrationVersion) {
		t.Fatalf("expected unknown migration version, got %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM schema_migration WHERE version = 99`); err != nil {
		t.Fatal(err)
	}

	failedSource := fstest.MapFS{}
	for _, name := range []string{
		"000001_initial.up.sql", "000001_initial.down.sql",
		"000002_coordination.up.sql", "000002_coordination.down.sql",
		"000003_execution_result.up.sql", "000003_execution_result.down.sql",
		"000004_job_queue.up.sql", "000004_job_queue.down.sql",
		"000005_outbox_repository.up.sql", "000005_outbox_repository.down.sql",
		"000006_p1_04_binding_identity.up.sql", "000006_p1_04_binding_identity.down.sql",
		"000007_vector_projection_task.up.sql", "000007_vector_projection_task.down.sql",
		"000008_vector_rebuild_run.up.sql", "000008_vector_rebuild_run.down.sql",
	} {
		data, readErr := fs.ReadFile(source, name)
		if readErr != nil {
			t.Fatal(readErr)
		}
		failedSource[name] = &fstest.MapFile{Data: data}
	}
	// Version 12 is the first unused version: the real P1-08 publication,
	// P2-01 RLS and P2-02 restore-compat migrations own versions 9, 10 and
	// 11 in the source tree.
	failedSource["000012_broken.up.sql"] = &fstest.MapFile{Data: []byte("CREATE TABLE migration_failure_probe (id integer); SELECT * FROM missing_migration_table;")}
	failedSource["000012_broken.down.sql"] = &fstest.MapFile{Data: []byte("DROP TABLE IF EXISTS migration_failure_probe;")}
	failedMigrator, err := NewMigratorWithPool(pool, cfg, failedSource)
	if err != nil {
		t.Fatal(err)
	}
	if err := failedMigrator.Up(ctx); err == nil {
		t.Fatal("expected broken migration to fail")
	}
	var probeExists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_name = 'migration_failure_probe'
	)`).Scan(&probeExists); err != nil {
		t.Fatal(err)
	}
	if probeExists {
		t.Fatal("failed migration left its probe table behind")
	}
	var failedVersionExists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM schema_migration WHERE version = 12
	)`).Scan(&failedVersionExists); err != nil {
		t.Fatal(err)
	}
	if failedVersionExists {
		t.Fatal("failed migration was recorded as applied")
	}

	var tableCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_name = ANY($1)`, []string{
		"tenant", "agent_app", "channel_binding", "user_identity", "session",
		"session_event", "message_dedup", "memory", "summary", "artifact",
		"audit_log", "outbox_message", "dead_letter", "agent_release",
		"tenant_config_version", "tenant_config_rollout", "tenant_config_operation",
	}).Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 17 {
		t.Fatalf("expected 17 business tables, got %d", tableCount)
	}

	if _, err := pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name) VALUES ('tenant-a', 'A'), ('tenant-b', 'B')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO agent_app (tenant_id, agent_app_id, name)
		VALUES ('tenant-a', 'shared-app', 'A'), ('tenant-b', 'shared-app', 'B')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO channel_binding
		(tenant_id, channel, binding_id, external_app_id)
		VALUES ('tenant-a', 'web', 'binding', 'same'), ('tenant-b', 'web', 'binding', 'same')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO session
		(tenant_id, session_id, agent_app_id, agent_version, channel, binding_id,
		external_chat, external_user)
		VALUES ('tenant-a', 'session', 'shared-app', 1, 'web', 'binding', 'chat', 'user')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO session_event
		(tenant_id, session_id, event_id, sequence, event_type)
		VALUES ('tenant-a', 'session', 'event', 1, 'user.received')`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO session_event
		(tenant_id, session_id, event_id, sequence, event_type)
		VALUES ('tenant-a', 'session', 'event', 2, 'user.received')`); err == nil {
		t.Fatal("expected duplicate event_id to fail")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO session_event
		(tenant_id, session_id, event_id, sequence, event_type)
		VALUES ('tenant-a', 'session', 'other-event', 1, 'user.received')`); err == nil {
		t.Fatal("expected duplicate sequence to fail")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO session_event
		(tenant_id, session_id, event_id, sequence, event_type)
		VALUES ('tenant-b', 'session', 'event', 1, 'user.received')`); err == nil {
		t.Fatal("expected missing tenant-b session to fail")
	}

	var exists bool
	err = pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM pg_constraint WHERE conname = 'session_event_pkey'
	)`).Scan(&exists)
	if err != nil || !exists {
		t.Fatalf("session_event primary key was not created: %v", err)
	}

	// Exercise a rollback in a savepoint-like standalone transaction without
	// changing the migration version or the durable test data.
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "CREATE TABLE migration_rollback_probe (id integer)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_name = 'migration_rollback_probe'
	)`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("rollback probe table survived rollback")
	}

	// Two concurrent callers must both complete against the same applied schema.
	// The first call acquires the dedicated connection lock; the second waits and
	// then observes the recorded checksum without re-running the DDL.
	results := make(chan error, 2)
	go func() { results <- migrator.Up(ctx) }()
	go func() { results <- migrator.Up(ctx) }()
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent Up failed: %v", err)
		}
	}

	if err := migrator.Down(ctx, len(loaded)); err != nil {
		t.Fatalf("Down failed: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_name = 'tenant'
	)`).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("Down left the initial schema behind")
	}
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("Up after Down failed: %v", err)
	}
}
