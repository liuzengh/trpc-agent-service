//go:build integration

package integration

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/dataprojection"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionpostgres "trpc.group/trpc-go/trpc-agent-go/session/postgres"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

func openRedis(t *testing.T) *redis.Client {
	t.Helper()
	rawURL := os.Getenv("TEST_REDIS_URL")
	if rawURL == "" {
		t.Fatal("TEST_REDIS_URL is required for integration-tag tests")
	}
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		t.Fatalf("parse TEST_REDIS_URL: %v", err)
	}
	client := redis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping real Redis integration backend: %v", err)
	}
	return client
}

func TestRedisToPostgresMigrationVerticalSlice(t *testing.T) {
	db := openDatabase(t)
	ctx := context.Background()
	tenantID := "migration-integration-" + uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants(id,name,status,config) VALUES($1,'migration integration','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(ctx, `DELETE FROM data_migration_records WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(ctx, `DELETE FROM data_migrations WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(ctx, `DELETE FROM tenants WHERE id=$1`, tenantID)
	})

	redisClient := openRedis(t)
	prefix := "integration-migration-" + uuid.NewString()
	t.Cleanup(func() {
		iterator := redisClient.Scan(context.Background(), 0, prefix+"*", 100).Iterator()
		keys := make([]string, 0, 8)
		for iterator.Next(context.Background()) {
			keys = append(keys, iterator.Val())
		}
		if len(keys) > 0 {
			_ = redisClient.Del(context.Background(), keys...).Err()
		}
	})
	source := &datamigration.RedisSource{Client: redisClient, Prefix: prefix}
	index := source.IndexKey(tenantID, datamigration.DomainMemory)
	for _, item := range []struct {
		key string
		ver float64
		val string
	}{{"session:event:1", 1, `{"role":"user","content":"hello"}`}, {"session:event:2", 2, `{"role":"assistant","content":"hi"}`}} {
		if err := redisClient.ZAdd(ctx, index, redis.Z{Score: item.ver, Member: item.key}).Err(); err != nil {
			t.Fatal(err)
		}
		if err := redisClient.Set(ctx, source.VersionedValueKey(tenantID, datamigration.DomainMemory, item.key, int64(item.ver)), item.val, 0).Err(); err != nil {
			t.Fatal(err)
		}
	}

	now := time.Now().UTC()
	job, err := datamigration.NewJob("migration-"+uuid.NewString(), tenantID, datamigration.DomainMemory, "redis-a", "postgres-a", now)
	if err != nil {
		t.Fatal(err)
	}
	store := datamigration.NewPostgresStore(db)
	if err := store.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	executor := &datamigration.Executor{
		Store: store, Source: source, Target: datamigration.NewPostgresTarget(db),
		Owner: "integration-worker", LeaseTTL: time.Minute, BatchSize: 1,
		Hooks: datamigration.Hooks{
			Prepare:         func(context.Context, datamigration.Job, datamigration.LeaseFence) error { return nil },
			EnableDualWrite: func(context.Context, datamigration.Job, datamigration.LeaseFence) error { return nil },
			Validate:        func(context.Context, datamigration.Job, datamigration.LeaseFence) error { return nil },
			ShadowRead:      func(context.Context, datamigration.Job, datamigration.LeaseFence) error { return nil },
			Cutover:         func(context.Context, datamigration.Job, datamigration.LeaseFence) error { return nil },
			Complete:        func(context.Context, datamigration.Job, datamigration.LeaseFence) error { return nil },
		},
	}
	for i := 0; i < 8; i++ {
		job, err = executor.RunOnce(ctx, job.ID)
		if err != nil {
			t.Fatalf("migration step %d: %v", i, err)
		}
	}
	if job.Phase != datamigration.PhaseRollbackWindow {
		t.Fatalf("phase=%s, want rollback window", job.Phase)
	}
	if _, err := executor.Complete(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM data_migration_records WHERE tenant_id=$1 AND domain='memory'`, tenantID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("migrated record count=%d, want 2", count)
	}
}

func TestTRPCSessionRedisToPostgresMigrationVerticalSlice(t *testing.T) {
	db := openDatabase(t)
	ctx := context.Background()
	rawRedisURL := os.Getenv("TEST_REDIS_URL")
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if rawRedisURL == "" || databaseURL == "" {
		t.Fatal("TEST_REDIS_URL and TEST_DATABASE_URL are required")
	}
	tenantID := "session-migration-" + uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants(id,name,status,config) VALUES($1,'session migration','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DELETE FROM audit_logs WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM data_migration_records WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM data_migrations WHERE tenant_id=$1`, tenantID)
		_, _ = db.ExecContext(context.Background(), `DELETE FROM tenants WHERE id=$1`, tenantID)
	})

	redisClient := openRedis(t)
	sourcePrefix := "trpc-session-source-" + uuid.NewString()
	journalPrefix := "trpc-session-journal-" + uuid.NewString()
	t.Cleanup(func() {
		for _, prefix := range []string{sourcePrefix, journalPrefix} {
			iterator := redisClient.Scan(context.Background(), 0, prefix+"*", 100).Iterator()
			keys := make([]string, 0, 16)
			for iterator.Next(context.Background()) {
				keys = append(keys, iterator.Val())
			}
			if len(keys) > 0 {
				_ = redisClient.Del(context.Background(), keys...).Err()
			}
		}
	})
	sourceSessions, err := sessionredis.NewService(
		sessionredis.WithRedisClientURL(rawRedisURL),
		sessionredis.WithKeyPrefix(sourcePrefix),
		sessionredis.WithSessionEventLimit(1000),
	)
	if err != nil {
		t.Fatalf("open real Redis session service: %v", err)
	}
	t.Cleanup(func() { _ = sourceSessions.Close() })

	// Keep generated index identifiers below PostgreSQL's 63-byte limit. The
	// upstream Session module verifies the full expected index name and will
	// correctly fail startup if PostgreSQL truncated a longer identifier.
	schemaName := "sm" + strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
	if _, err := db.ExecContext(ctx, "CREATE SCHEMA "+pq.QuoteIdentifier(schemaName)); err != nil {
		t.Fatalf("create isolated PostgreSQL session schema: %v", err)
	}
	targetSessions, err := sessionpostgres.NewService(
		sessionpostgres.WithPostgresClientDSN(databaseURL),
		sessionpostgres.WithSchema(schemaName),
		sessionpostgres.WithSessionEventLimit(1000),
	)
	if err != nil {
		_, _ = db.ExecContext(ctx, "DROP SCHEMA "+pq.QuoteIdentifier(schemaName)+" CASCADE")
		t.Fatalf("open real PostgreSQL session service: %v", err)
	}
	t.Cleanup(func() {
		_ = targetSessions.Close()
		_, _ = db.ExecContext(context.Background(), "DROP SCHEMA "+pq.QuoteIdentifier(schemaName)+" CASCADE")
	})

	tenantValue := &tenant.Tenant{ID: tenantID}
	physicalApp, err := storage.TenantScopedAppName(tenantValue, "support")
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: physicalApp, UserID: "owner-1", SessionID: "session-1"}
	sourceSession, err := sourceSessions.CreateSession(ctx, key, session.StateMap{"case_status": []byte("open")})
	if err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, time.September, 4, 3, 0, 0, 0, time.UTC)
	if err := sourceSessions.AppendEvent(ctx, sourceSession, &event.Event{
		ID: "event-1", Timestamp: base,
		Response: &model.Response{Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: "open case"}}}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := sourceSessions.AppendTrackEvent(ctx, sourceSession, &session.TrackEvent{
		Track: "protocol", Payload: []byte(`{"phase":"received"}`), Timestamp: base,
	}); err != nil {
		t.Fatal(err)
	}

	journal := &datamigration.RedisSource{Client: redisClient, Prefix: journalPrefix}
	publish := func(record datamigration.Record) {
		t.Helper()
		valueKey := journal.VersionedValueKey(tenantID, datamigration.DomainSession, record.Key, record.Version)
		if err := redisClient.Set(ctx, valueKey, record.Payload, 0).Err(); err != nil {
			t.Fatal(err)
		}
		if err := redisClient.ZAdd(ctx, journal.IndexKey(tenantID, datamigration.DomainSession), redis.Z{
			Score: float64(record.Version), Member: record.Key,
		}).Err(); err != nil {
			t.Fatal(err)
		}
	}
	firstRecord, err := dataprojection.NewSessionRecord(ctx, sourceSessions, key, 1, 100)
	if err != nil {
		t.Fatalf("snapshot real Redis session: %v", err)
	}
	publish(firstRecord)

	projector, err := dataprojection.NewSessionProjector(
		func(_ context.Context, resolvedTenantID, appName string) (session.Service, error) {
			if resolvedTenantID != tenantID || appName != physicalApp {
				return nil, fmt.Errorf("unexpected session projection scope")
			}
			return targetSessions, nil
		}, 100,
	)
	if err != nil {
		t.Fatal(err)
	}
	projectionTarget, err := dataprojection.NewTarget(db, projector)
	if err != nil {
		t.Fatal(err)
	}
	migrationID := "session-backend-" + uuid.NewString()
	job, err := datamigration.NewJob(migrationID, tenantID, datamigration.DomainSession, "session-redis", "session-postgres", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	store := datamigration.NewPostgresStore(db)
	if err := store.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	var publishedCatchUp bool
	repository, err := tenant.NewSQLRepository("postgres", databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	assertTarget := func() error {
		value, err := targetSessions.GetSession(ctx, key, session.WithEventNum(100))
		if err != nil {
			return err
		}
		if value == nil || len(value.Events) != 2 || string(value.State["case_status"]) != "resolved" {
			return fmt.Errorf("target session mismatch")
		}
		tracked, err := targetSessions.GetTrackEvents(ctx, key, "protocol", session.WithEventNum(100))
		if err != nil || tracked == nil || len(tracked.Events) != 1 {
			return fmt.Errorf("target track mismatch")
		}
		return nil
	}
	executor := &datamigration.Executor{
		Store: store, Source: journal, Target: projectionTarget,
		Owner: "session-migration-worker", LeaseTTL: time.Minute, BatchSize: 1,
		Hooks: datamigration.Hooks{
			Prepare: func(context.Context, datamigration.Job, datamigration.LeaseFence) error { return nil },
			EnableDualWrite: func(hookCtx context.Context, _ datamigration.Job, _ datamigration.LeaseFence) error {
				if publishedCatchUp {
					return nil
				}
				if err := sourceSessions.AppendEvent(hookCtx, sourceSession, &event.Event{
					ID: "event-2", Timestamp: base.Add(time.Second),
					Response: &model.Response{Choices: []model.Choice{{Message: model.Message{Role: model.RoleAssistant, Content: "case resolved"}}}},
				}); err != nil {
					return err
				}
				if err := sourceSessions.UpdateSessionState(hookCtx, key, session.StateMap{"case_status": []byte("resolved")}); err != nil {
					return err
				}
				updated, err := dataprojection.NewSessionRecord(hookCtx, sourceSessions, key, 2, 100)
				if err != nil {
					return err
				}
				publish(updated)
				publishedCatchUp = true
				return nil
			},
			Validate:   func(context.Context, datamigration.Job, datamigration.LeaseFence) error { return assertTarget() },
			ShadowRead: func(context.Context, datamigration.Job, datamigration.LeaseFence) error { return assertTarget() },
			Cutover: func(hookCtx context.Context, _ datamigration.Job, _ datamigration.LeaseFence) error {
				value, err := repository.GetByID(hookCtx, tenantID)
				if err != nil {
					return err
				}
				// This library fixture owns its synthetic cutover. Production
				// changes use LiveCoordinator; ordinary tenant updates reject
				// storage relocation even after a caller has copied data.
				result, err := db.ExecContext(hookCtx, `UPDATE tenants SET
					config=jsonb_set(config,'{storage}',COALESCE(config->'storage','{}'::jsonb) ||
					jsonb_build_object('sessionBackend','postgres','sessionProfile','session-postgres')),
					config_version=config_version+1
					WHERE id=$1 AND config_version=$2`, tenantID, value.ConfigVersion)
				if err != nil {
					return err
				}
				rows, err := result.RowsAffected()
				if err != nil || rows != 1 {
					return fmt.Errorf("test cutover CAS failed: rows=%d error=%v", rows, err)
				}
				return nil
			},
			Complete: func(context.Context, datamigration.Job, datamigration.LeaseFence) error { return assertTarget() },
		},
	}
	for step := 0; step < 12 && job.Phase != datamigration.PhaseRollbackWindow; step++ {
		job, err = executor.RunOnce(ctx, migrationID)
		if err != nil {
			t.Fatalf("session migration step %d: %v", step, err)
		}
	}
	if job.Phase != datamigration.PhaseRollbackWindow {
		t.Fatalf("phase=%s, want rollback window", job.Phase)
	}
	job, err = executor.Complete(ctx, migrationID)
	if err != nil || job.Phase != datamigration.PhaseComplete {
		t.Fatalf("complete phase=%s err=%v", job.Phase, err)
	}
	if err := assertTarget(); err != nil {
		t.Fatal(err)
	}
	resolvedTenant, err := repository.GetByID(ctx, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	if resolvedTenant.ConfigVersion != 2 || resolvedTenant.Storage.SessionBackend != "postgres" ||
		resolvedTenant.Storage.SessionProfile != "session-postgres" {
		t.Fatalf("cutover tenant version/storage = %d/%#v", resolvedTenant.ConfigVersion, resolvedTenant.Storage)
	}
	var projected int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM data_migration_records
		WHERE tenant_id=$1 AND domain='session' AND projected_at IS NOT NULL`, tenantID).Scan(&projected); err != nil {
		t.Fatal(err)
	}
	if projected != 1 {
		t.Fatalf("projected session rows=%d, want 1 latest canonical record", projected)
	}
}
