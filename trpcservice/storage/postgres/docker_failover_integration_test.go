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

func logRedisFailoverStage(t *testing.T, stage, category string, started time.Time, err error, retries int, ownershipVerified bool) {
	t.Helper()
	if category == "" {
		category = "none"
	}
	t.Logf("redis_failover stage=%s category=%s success=%t duration_ms=%d retries=%d ownership_verified=%t", stage, category, err == nil, time.Since(started).Milliseconds(), retries, ownershipVerified)
}

func redisFailoverErrorCategory(err error) string {
	if err == nil {
		return "none"
	}
	if errors.Is(err, context.Canceled) {
		return "context_canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	if errors.Is(err, storage.ErrLeaseLost) {
		return "session_lease_lost"
	}
	if errors.Is(err, storage.ErrEpochRejected) {
		return "epoch_rejected"
	}
	if errors.Is(err, storage.ErrFenceRejected) {
		return "fence_rejected"
	}
	if errors.Is(err, storage.ErrBackendUnavailable) {
		return "redis_unavailable"
	}
	if errors.Is(err, storage.ErrOperationAmbiguous) {
		return "operation_ambiguous"
	}
	return "other"
}
func newIndependentRedisStore(t *testing.T, f *dockerFailoverFixture) (*redisstore.Store, func()) {
	endpoint := f.lab.RedisURL(f.ctx)
	opts, err := redis.ParseURL(endpoint)
	if err != nil {
		t.Fatal("new owner connect failed: category=endpoint_parse")
	}
	client := redis.NewClient(opts)
	backend, err := redisstore.NewBackendWithClient(client, redisstore.Config{URL: endpoint, KeyPrefix: f.prefix, SessionLeaseTTL: 3 * time.Second, ClaimTTL: 3 * time.Second})
	if err != nil {
		_ = client.Close()
		t.Fatal("new owner connect failed: category=client_create")
	}
	return redisstore.NewStoreWithEpochAuthority(backend, f.pg), func() { _ = backend.Close() }
}
func TestRealFailoverStoreAfterRedisRestart(t *testing.T) {
	f := newDockerFailoverFixture(t)
	ownershipVerified := f.lab.OwnershipVerified(f.ctx)
	for _, stage := range []string{"redis_container_create", "redis_network_attach", "redis_alias_resolution", "redis_start", "redis_ready_before_restart"} {
		logRedisFailoverStage(t, stage, "none", time.Now(), nil, 0, ownershipVerified)
	}
	initialStarted := time.Now()
	lease, err := f.failover.Acquire(f.ctx, f.tc, f.resource, "redis-owner", 3*time.Second)
	logRedisFailoverStage(t, "initial_owner_acquire", redisFailoverErrorCategory(err), initialStarted, err, 0, ownershipVerified)
	if err != nil {
		t.Fatal("initial owner acquire failed: category=" + redisFailoverErrorCategory(err))
	}
	initialRenewStarted := time.Now()
	lease, err = f.failover.Renew(f.ctx, f.tc, lease, 3*time.Second)
	logRedisFailoverStage(t, "initial_owner_renew", redisFailoverErrorCategory(err), initialRenewStarted, err, 0, ownershipVerified)
	if err != nil {
		t.Fatal("initial owner renew failed: category=" + redisFailoverErrorCategory(err))
	}
	before := f.failover.Snapshot()
	var callbackCount atomic.Int32
	runnerCtx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	runner, err := storage.NewLeaseRenewalRunner(runnerCtx, f.failover, f.tc, lease, 3*time.Second, 100*time.Millisecond, func(error) { callbackCount.Add(1) })
	if err != nil {
		t.Fatal("renewal runner create failed: category=invalid_runner")
	}
	if err := runner.Start(); err != nil {
		t.Fatal("renewal runner start failed: category=runner_start")
	}
	stopStarted := time.Now()
	f.lab.StopRedis(f.ctx)
	logRedisFailoverStage(t, "redis_stop", "none", stopStarted, nil, 0, ownershipVerified)
	failureStarted := time.Now()
	failureCtx, failureCancel := context.WithTimeout(f.ctx, 7*time.Second)
	_, renewErr := f.failover.Renew(failureCtx, f.tc, lease, 3*time.Second)
	failureCancel()
	logRedisFailoverStage(t, "redis_unavailable", redisFailoverErrorCategory(renewErr), failureStarted, renewErr, 0, ownershipVerified)
	if renewErr == nil {
		t.Fatal("renew unexpectedly succeeded after Redis stop: category=redis_still_available")
	}
	quarantineStarted := time.Now()
	retries := 0
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && f.failover.State() != coordination.Quarantined {
		retries++
		time.Sleep(50 * time.Millisecond)
	}
	if f.failover.State() != coordination.Quarantined {
		logRedisFailoverStage(t, "deadline", "deadline", quarantineStarted, context.DeadlineExceeded, retries, ownershipVerified)
		t.Fatal("Redis quarantine deadline exhausted: category=deadline")
	}
	logRedisFailoverStage(t, "deadline", "bounded_poll_complete", quarantineStarted, nil, retries, ownershipVerified)
	if runnerErr := runner.Wait(); runnerErr != nil && !errors.Is(runnerErr, storage.ErrLeaseLost) && !errors.Is(runnerErr, storage.ErrOperationAmbiguous) {
		t.Fatal("renewal runner failed: category=" + redisFailoverErrorCategory(runnerErr))
	}
	if callbackCount.Load() > 1 {
		t.Fatal("renewal callback repeated: category=callback_repeat")
	}
	restartStarted := time.Now()
	f.lab.RestartRedis(f.ctx)
	logRedisFailoverStage(t, "redis_restart", "none", restartStarted, nil, 0, ownershipVerified)
	readyStarted := time.Now()
	f.lab.WaitHealthy(f.ctx)
	logRedisFailoverStage(t, "redis_ready_after_restart", "none", readyStarted, nil, 0, ownershipVerified)
	probeStarted := time.Now()
	probeErr := f.failover.Probe(f.ctx)
	logRedisFailoverStage(t, "new_owner_reconnect", redisFailoverErrorCategory(probeErr), probeStarted, probeErr, 0, ownershipVerified)
	if probeErr != nil {
		t.Fatal("recovered Redis probe failed: category=" + redisFailoverErrorCategory(probeErr))
	}
	if f.failover.State() != coordination.Recovering {
		t.Fatal("probe state invalid: category=state_transition")
	}
	epochStarted := time.Now()
	recoveredEpoch, err := f.failover.Recover(f.ctx)
	logRedisFailoverStage(t, "new_owner_epoch", redisFailoverErrorCategory(err), epochStarted, err, 0, ownershipVerified)
	if err != nil {
		t.Fatal("epoch recovery failed: category=" + redisFailoverErrorCategory(err))
	}
	if recoveredEpoch != before.Epoch+1 {
		t.Fatal("epoch did not advance: category=epoch_not_advanced")
	}
	newOwnerStore, closeNewOwner := newIndependentRedisStore(t, f)
	defer closeNewOwner()
	newConnectStarted := time.Now()
	newConnectErr := newOwnerStore.Backend.Ping(f.ctx)
	logRedisFailoverStage(t, "new_owner_connect", redisFailoverErrorCategory(newConnectErr), newConnectStarted, newConnectErr, 0, ownershipVerified)
	if newConnectErr != nil {
		t.Fatal("new owner connect failed: category=redis_unavailable")
	}
	newEpochStarted := time.Now()
	observedEpoch, newEpochErr := f.pg.GetEpoch(f.ctx, f.tc.TenantID, f.resource)
	logRedisFailoverStage(t, "new_owner_epoch", redisFailoverErrorCategory(newEpochErr), newEpochStarted, newEpochErr, 0, ownershipVerified)
	if newEpochErr != nil || observedEpoch != recoveredEpoch {
		t.Fatal("new owner epoch invalid: category=epoch_not_recovered")
	}
	acquireStarted := time.Now()
	newLease, acquireErr := newOwnerStore.Acquire(f.ctx, f.tc, f.resource, "new-owner", 2*time.Second)
	logRedisFailoverStage(t, "new_owner_acquire", redisFailoverErrorCategory(acquireErr), acquireStarted, acquireErr, 0, ownershipVerified)
	if acquireErr != nil {
		t.Fatal("new owner acquire failed: category=" + redisFailoverErrorCategory(acquireErr))
	}
	if newLease.Epoch != recoveredEpoch || newLease.Backend != storage.BackendRedis || newLease.FenceToken <= lease.FenceToken {
		t.Fatal("new lease did not advance fencing identity: category=fence_not_advanced")
	}
	renewStarted := time.Now()
	newLease, renewErr = newOwnerStore.Renew(f.ctx, f.tc, newLease, 2*time.Second)
	logRedisFailoverStage(t, "new_owner_renew", redisFailoverErrorCategory(renewErr), renewStarted, renewErr, 0, ownershipVerified)
	if renewErr != nil {
		t.Fatal("new owner renew failed: category=" + redisFailoverErrorCategory(renewErr))
	}
	fencingStarted := time.Now()
	oldRenewErr := error(nil)
	_, oldRenewErr = f.failover.Renew(f.ctx, f.tc, lease, 2*time.Second)
	oldReleaseErr := f.failover.Release(f.ctx, f.tc, lease)
	oldValidateErr := f.failover.Validate(f.ctx, f.tc, lease)
	directOldRenewErr := error(nil)
	_, directOldRenewErr = f.redis.Renew(f.ctx, f.tc, lease, 2*time.Second)
	directOldReleaseErr := f.redis.Release(f.ctx, f.tc, lease)
	directOldValidateErr := f.redis.Validate(f.ctx, f.tc, lease)
	logRedisFailoverStage(t, "old_owner_fencing", "epoch_rejected", fencingStarted, nil, 0, ownershipVerified)
	if !errors.Is(oldRenewErr, storage.ErrEpochRejected) || !errors.Is(oldReleaseErr, storage.ErrEpochRejected) || !errors.Is(oldValidateErr, storage.ErrEpochRejected) || !errors.Is(directOldRenewErr, storage.ErrEpochRejected) || !errors.Is(directOldReleaseErr, storage.ErrEpochRejected) || !errors.Is(directOldValidateErr, storage.ErrEpochRejected) {
		t.Fatal("old owner fencing failed: category=epoch_fencing")
	}
	durableStarted := time.Now()
	currentEpoch, durableErr := f.pg.GetEpoch(f.ctx, f.tc.TenantID, f.resource)
	var sessionExists bool
	if durableErr == nil {
		durableErr = f.pool.QueryRow(f.ctx, "SELECT EXISTS(SELECT 1 FROM session WHERE tenant_id=$1 AND session_id=$2)", f.tc.TenantID, f.resource).Scan(&sessionExists)
	}
	logRedisFailoverStage(t, "durable_fact_check", redisFailoverErrorCategory(durableErr), durableStarted, durableErr, 0, ownershipVerified)
	if durableErr != nil || currentEpoch != recoveredEpoch || !sessionExists {
		t.Fatal("durable fact check failed: category=durable_fact_missing")
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
