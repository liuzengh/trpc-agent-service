package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/internal/testinfra"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/coordination"
	redisstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/redis/go-redis/v9"
)

type dockerFailoverFixture struct {
	lab      *testinfra.DockerLab
	base     *pgxpool.Pool
	pool     *pgxpool.Pool
	pg       *CoordinationStore
	redis    *redisstore.Store
	backend  *redisstore.Backend
	client   *redis.Client
	failover *coordination.FailoverStore
	ctx      context.Context
	tc       tenant.TenantContext
	resource string
	prefix   string
	schema   string
}

func newDockerPool(ctx context.Context, cfg PostgresConfig) (*pgxpool.Pool, error) {
	deadline := time.Now().Add(30 * time.Second)
	var lastErr error
	for {
		pool, err := NewPool(ctx, cfg)
		if err == nil {
			return pool, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, lastErr
		}
		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !errors.Is(ctx.Err(), context.Canceled) {
				return nil, ctx.Err()
			}
			return nil, lastErr
		case <-timer.C:
		}
	}
}

func newDockerFailoverFixture(t *testing.T) *dockerFailoverFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	lab := testinfra.NewDockerLab(t)
	lab.Start(ctx)
	lab.WaitHealthy(ctx)
	baseCfg := PostgresConfig{URL: lab.PostgresURL(ctx), MaxConns: 8, MinConns: 1, AllowDestructiveDown: true}
	base, err := newDockerPool(ctx, baseCfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	schema := fmt.Sprintf("p006_fi_%d", time.Now().UnixNano())
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		base.Close()
		cancel()
		t.Fatal(err)
	}
	cfg := baseCfg
	cfg.SearchPath = schema
	pool, err := newDockerPool(ctx, cfg)
	if err != nil {
		base.Close()
		cancel()
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	migrator, err := NewMigratorWithPool(pool, cfg, os.DirFS(filepath.Join(filepath.Dir(file), "../../../migrations")))
	if err != nil {
		pool.Close()
		base.Close()
		cancel()
		t.Fatal(err)
	}
	if err := migrator.Up(ctx); err != nil {
		pool.Close()
		base.Close()
		cancel()
		t.Fatal(err)
	}
	tc := tenant.TenantContext{TenantID: "p006-fi", AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web", RequestID: "request-fi", MessageID: "message-fi", TraceID: "trace-fi", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "memory"}}
	for _, query := range []string{
		`INSERT INTO tenant (tenant_id, name) VALUES ('p006-fi', 'fault test')`,
		`INSERT INTO agent_app (tenant_id, agent_app_id, name) VALUES ('p006-fi', 'agent-a', 'fault test')`,
		`INSERT INTO channel_binding (tenant_id, channel, binding_id, external_app_id) VALUES ('p006-fi', 'web', 'binding-a', 'fault-test')`,
	} {
		if _, err := pool.Exec(ctx, query); err != nil {
			pool.Close()
			base.Close()
			cancel()
			t.Fatal(err)
		}
	}
	resource := fmt.Sprintf("session-%d", time.Now().UnixNano())
	if _, err := pool.Exec(ctx, `INSERT INTO session (tenant_id, session_id, agent_app_id, agent_version, channel, binding_id, external_chat, external_user) VALUES ($1,$2,$3,1,$4,$5,'chat-fi','user-fi')`, tc.TenantID, resource, tc.AgentAppID, tc.Channel, tc.BindingID); err != nil {
		pool.Close()
		base.Close()
		cancel()
		t.Fatal(err)
	}
	pg, err := NewCoordinationStore(pool)
	if err != nil {
		pool.Close()
		base.Close()
		cancel()
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("p006-fi-redis-%d-", time.Now().UnixNano())
	clientOpts, err := redis.ParseURL(lab.RedisURL(ctx))
	if err != nil {
		pool.Close()
		base.Close()
		cancel()
		t.Fatal(err)
	}
	client := redis.NewClient(clientOpts)
	backend, err := redisstore.NewBackendWithClient(client, redisstore.Config{URL: lab.RedisURL(ctx), KeyPrefix: prefix, SessionLeaseTTL: 3 * time.Second, ClaimTTL: 3 * time.Second})
	if err != nil {
		client.Close()
		pool.Close()
		base.Close()
		cancel()
		t.Fatal(err)
	}
	redisCoord := redisstore.NewStoreWithEpochAuthority(backend, pg)
	failover := coordination.NewFailoverStoreWithAuthority(
		coordination.Endpoint{Name: storage.BackendRedis, Claims: redisCoord, Leases: redisCoord, Probe: backend.Ping},
		coordination.Endpoint{Name: storage.BackendPostgres, Claims: pg, Leases: pg},
		pg, tc.TenantID, resource,
		coordination.Config{FailureThreshold: 1, QuarantineWindow: 0, Now: time.Now},
	)
	if err := failover.Initialize(ctx); err != nil {
		backend.Close()
		pool.Close()
		base.Close()
		cancel()
		t.Fatal(err)
	}
	f := &dockerFailoverFixture{lab: lab, base: base, pool: pool, pg: pg, redis: redisCoord, backend: backend, client: client, failover: failover, ctx: ctx, tc: tc, resource: resource, prefix: prefix, schema: schema}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
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
		pool.Close()
		_, _ = base.Exec(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
	})
	return f
}

func TestDockerFailoverFixtureUsesProxyAndDynamicPostgres(t *testing.T) {
	f := newDockerFailoverFixture(t)
	lease, err := f.failover.Acquire(f.ctx, f.tc, f.resource, "fixture-owner", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Backend != storage.BackendRedis || lease.Epoch == 0 {
		t.Fatalf("unexpected primary lease: %+v", lease)
	}
	if err := f.failover.Validate(f.ctx, f.tc, lease); err != nil {
		t.Fatal(err)
	}
	claim, err := f.failover.Claim(f.ctx, f.tc, storage.DedupKey{TenantID: f.tc.TenantID, Channel: f.tc.Channel, BindingID: f.tc.BindingID, ExternalMessageID: "fixture-message"}, time.Second, "fixture-owner")
	if err != nil {
		t.Fatal(err)
	}
	if claim.Backend != storage.BackendRedis || claim.Epoch != lease.Epoch {
		t.Fatalf("unexpected primary claim: %+v", claim)
	}
}

func TestRealFailoverStoreAfterRedisRestart(t *testing.T) {
	f := newDockerFailoverFixture(t)
	lease, err := f.failover.Acquire(f.ctx, f.tc, f.resource, "redis-owner", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	before := f.failover.Snapshot()
	var callbackCount atomic.Int32
	runnerCtx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	runner, err := storage.NewLeaseRenewalRunner(runnerCtx, f.failover, f.tc, lease, 3*time.Second, 100*time.Millisecond, func(error) { callbackCount.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Start(); err != nil {
		t.Fatal(err)
	}
	f.lab.StopRedis(f.ctx)
	failureCtx, failureCancel := context.WithTimeout(f.ctx, 7*time.Second)
	_, renewErr := f.failover.Renew(failureCtx, f.tc, lease, 3*time.Second)
	failureCancel()
	if renewErr == nil {
		t.Fatal("Renew unexpectedly succeeded after Redis stop")
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && f.failover.State() != coordination.Quarantined {
		time.Sleep(50 * time.Millisecond)
	}
	if f.failover.State() != coordination.Quarantined {
		t.Fatalf("state did not quarantine after Redis stop: %+v", f.failover.Snapshot())
	}
	if err := runner.Wait(); !errors.Is(err, storage.ErrLeaseLost) && !errors.Is(err, storage.ErrOperationAmbiguous) {
		t.Fatalf("runner error=%v", err)
	}
	if callbackCount.Load() > 1 {
		t.Fatalf("runner callback repeated: %d", callbackCount.Load())
	}
	f.lab.RestartRedis(f.ctx)
	f.lab.WaitHealthy(f.ctx)
	if err := f.failover.Probe(f.ctx); err != nil {
		t.Fatal(err)
	}
	if f.failover.State() != coordination.Recovering {
		t.Fatalf("probe state=%s", f.failover.State())
	}
	recoveredEpoch, err := f.failover.Recover(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredEpoch != before.Epoch+1 {
		t.Fatalf("epoch before=%d recovered=%d", before.Epoch, recoveredEpoch)
	}
	if err := f.failover.Validate(f.ctx, f.tc, lease); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old lease validation=%v", err)
	}
	newLease, err := f.failover.Acquire(f.ctx, f.tc, f.resource, "new-owner", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if newLease.Epoch != recoveredEpoch || newLease.Backend != storage.BackendRedis {
		t.Fatalf("new lease=%+v", newLease)
	}
}

func TestRealFailoverStoreDuringRedisNetworkPartition(t *testing.T) {
	f := newDockerFailoverFixture(t)
	lease, err := f.failover.Acquire(f.ctx, f.tc, f.resource, "redis-owner", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	before := f.failover.Snapshot()
	var callbackCount atomic.Int32
	runnerCtx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	runner, err := storage.NewLeaseRenewalRunner(runnerCtx, f.failover, f.tc, lease, 3*time.Second, 100*time.Millisecond, func(error) { callbackCount.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.Start(); err != nil {
		t.Fatal(err)
	}
	f.lab.DisconnectRedis(f.ctx)
	failureCtx, failureCancel := context.WithTimeout(f.ctx, 7*time.Second)
	_, renewErr := f.failover.Renew(failureCtx, f.tc, lease, 3*time.Second)
	failureCancel()
	if renewErr == nil {
		t.Fatal("Renew unexpectedly succeeded after Redis network disconnect")
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) && f.failover.State() != coordination.Quarantined {
		time.Sleep(50 * time.Millisecond)
	}
	if f.failover.State() != coordination.Quarantined {
		t.Fatalf("state did not quarantine: %+v", f.failover.Snapshot())
	}
	if err := f.lab.RedisManagementPing(f.ctx); err != nil {
		t.Fatalf("Redis stopped instead of partitioning: %v", err)
	}
	if err := runner.Wait(); !errors.Is(err, storage.ErrLeaseLost) && !errors.Is(err, storage.ErrOperationAmbiguous) {
		t.Fatalf("runner error=%v", err)
	}
	if callbackCount.Load() > 1 {
		t.Fatalf("runner callback repeated: %d", callbackCount.Load())
	}
	if _, err := f.failover.Acquire(f.ctx, f.tc, f.resource, "secondary-owner", time.Second); !errors.Is(err, storage.ErrBackendUnavailable) {
		t.Fatalf("quarantine acquire=%v", err)
	}
	f.lab.ReconnectRedis(f.ctx)
	f.lab.WaitHealthy(f.ctx)
	if err := f.failover.Probe(f.ctx); err != nil {
		t.Fatal(err)
	}
	if f.failover.State() != coordination.Recovering {
		t.Fatalf("probe state=%s", f.failover.State())
	}
	recoveredEpoch, err := f.failover.Recover(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredEpoch != before.Epoch+1 {
		t.Fatalf("epoch before=%d recovered=%d", before.Epoch, recoveredEpoch)
	}
	if err := f.failover.Validate(f.ctx, f.tc, lease); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old lease validation=%v", err)
	}
	newLease, err := f.failover.Acquire(f.ctx, f.tc, f.resource, "new-owner", 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if newLease.Epoch != recoveredEpoch || newLease.Backend != storage.BackendRedis {
		t.Fatalf("new lease=%+v", newLease)
	}
}
