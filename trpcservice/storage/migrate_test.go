package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
)

// redisSessionService builds the framework redis session service on the
// compose instance; sessions are deleted in cleanup.
func redisSessionService(t *testing.T) session.Service {
	t.Helper()
	svc, err := sessionredis.NewService(
		sessionredis.WithRedisClientURL("redis://" + migrateTestRedisAddr),
	)
	if err != nil {
		t.Skipf("redis unavailable (%v) — set TRPC_TEST_REDIS_ADDR (default %s), skipping integration test", err, migrateTestRedisAddr)
	}
	t.Cleanup(func() { _ = svc.Close() })
	return svc
}

// A redis→postgres session migration must walk dual_write → backfilling →
// observing → done, copy the sessions, and flip tenant.storage_config at the
// read switch.
func TestMigratorRedisToPostgres(t *testing.T) {
	_, pool := pgSessionService(t) // ensures the test tenant/app rows
	ctx := context.Background()

	rdb := redisOrSkipForMigrate(t)
	redisSvc := redisSessionService(t)
	pgSvc := storage.NewPGSessionService(pool)

	// One session on the redis backend with two events.
	key := testKey(t.Name())
	sess, err := redisSvc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	if err := redisSvc.AppendEvent(ctx, sess, textEvent("mig-e1", "user", "迁移前")); err != nil {
		t.Fatal(err)
	}
	if err := redisSvc.AppendEvent(ctx, sess, textEvent("mig-e2", "assistant", "好的")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = redisSvc.DeleteSession(context.Background(), key) })
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })

	// Open the migration (phase dual_write), like the Admin API would.
	var migID string
	err = pool.QueryRow(ctx,
		`INSERT INTO storage_migration (tenant_id, resource, from_backend, to_backend, phase)
		 VALUES ($1, 'session', 'redis', 'postgres', 'dual_write') RETURNING id`,
		testTenantID).Scan(&migID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM storage_migration WHERE id = $1`, migID)
		_, _ = pool.Exec(context.Background(), `UPDATE tenant SET storage_config = NULL WHERE id = $1`, testTenantID)
	})

	m := storage.NewMigrator(pool, rdb,
		map[string]session.Service{"redis": redisSvc, "postgres": pgSvc}, 50*time.Millisecond)

	// dual_write → backfilling.
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	// backfilling → copy + consistency check + read switch → observing.
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	// Observing window is 50ms; wait past it, then the next tick finishes.
	time.Sleep(80 * time.Millisecond)
	if err := m.Tick(ctx); err != nil {
		t.Fatal(err)
	}

	var phase string
	if err := pool.QueryRow(ctx,
		`SELECT phase FROM storage_migration WHERE id = $1`, migID).Scan(&phase); err != nil {
		t.Fatal(err)
	}
	if phase != tenant.PhaseDone {
		t.Fatalf("want phase done, got %s", phase)
	}

	// The read switch flipped the tenant's storage_config.
	var storageCfg string
	if err := pool.QueryRow(ctx,
		`SELECT storage_config::text FROM tenant WHERE id = $1`, testTenantID).Scan(&storageCfg); err != nil {
		t.Fatal(err)
	}
	if storageCfg != `{"session": {"type": "postgres"}}` {
		t.Fatalf("tenant storage_config not switched: %s", storageCfg)
	}

	// The session and both events exist in PG now.
	got, err := pgSvc.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got.Events) != 2 {
		t.Fatalf("want 2 migrated events, got %+v", got)
	}
	if got.Events[0].ID != "mig-e1" || got.Events[1].ID != "mig-e2" {
		t.Fatalf("events out of order: %+v", got.Events)
	}
}

// migrateTestRedisAddr is the compose redis.
var migrateTestRedisAddr = testenv.RedisAddr()

// redisOrSkipForMigrate returns the raw compose redis client (used by the
// migrator for key enumeration), skipping when unreachable.
func redisOrSkipForMigrate(t *testing.T) *redis.Client {
	t.Helper()
	rdb, err := storage.NewRedis(context.Background(), migrateTestRedisAddr)
	if err != nil {
		t.Skipf("redis unavailable (%v) — set TRPC_TEST_REDIS_ADDR (default %s), skipping integration test", err, migrateTestRedisAddr)
	}
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}
