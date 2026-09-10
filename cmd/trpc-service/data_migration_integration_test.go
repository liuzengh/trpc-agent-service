//go:build integration

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	artifactcos "github.com/liuzengh/trpc-agent-service/trpcservice/artifact/cos"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/session"
	redisprovider "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

var (
	dataMigrationTestDSN = flag.String(
		"data-migration-test-dsn",
		os.Getenv("TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN"),
		"dedicated PostgreSQL integration database DSN",
	)
	dataMigrationTestURL = flag.String(
		"data-migration-test-url",
		os.Getenv("TRPC_AGENT_SERVICE_REDIS_TEST_URL"),
		"Redis integration URL",
	)
)

func TestWorkerRuntimeCompletesRedisPostgresMigration(t *testing.T) {
	if *dataMigrationTestDSN == "" || *dataMigrationTestURL == "" {
		t.Fatal("TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN and TRPC_AGENT_SERVICE_REDIS_TEST_URL are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := openDataMigrationIntegrationPool(t)
	store, err := postgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres store: %v", err)
	}

	seed := time.Now().UnixNano()
	tenantID := fmt.Sprintf("migration-%x", seed)
	appID := "support"
	schema := fmt.Sprintf("mig%x", seed&0xffffff)
	t.Cleanup(func() { cleanupDataMigrationIntegration(t, pool, tenantID, appID, schema) })

	v1, v2 := dataMigrationAppConfigs(tenantID, appID, schema)
	if err := store.CreateTenant(ctx, tenant.Tenant{
		ID:     tenantID,
		Name:   "Migration Integration",
		Status: tenant.StatusActive,
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID:            tenantID,
		AppID:               appID,
		Name:                "Migration Integration",
		ActiveConfigVersion: v1.Version,
		Status:              tenant.StatusActive,
	}, v1); err != nil {
		t.Fatalf("create agent app: %v", err)
	}
	if err := store.InsertAppConfigVersion(ctx, v2); err != nil {
		t.Fatalf("insert target config: %v", err)
	}

	scope := tenant.Scope{TenantID: tenantID, AppID: appID}
	appName, err := scope.Key("runner")
	if err != nil {
		t.Fatalf("build session app name: %v", err)
	}
	key := session.Key{AppName: appName, UserID: "user-1", SessionID: "session-1"}
	var laneCount int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM platform.session_lane
WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID).Scan(&laneCount); err != nil {
		t.Fatalf("check legacy session lane absence: %v", err)
	}
	if laneCount != 0 {
		t.Fatalf("legacy migration fixture unexpectedly has %d session lanes", laneCount)
	}
	source, err := redisprovider.NewService(
		redisprovider.WithRedisClientURL(*dataMigrationTestURL),
		redisprovider.WithCompatMode(redisprovider.CompatModeTransition),
	)
	if err != nil {
		t.Fatalf("create redis session service: %v", err)
	}
	t.Cleanup(func() {
		_ = source.DeleteSession(context.Background(), key)
		_ = source.Close()
	})
	if _, err := source.CreateSession(ctx, key, session.StateMap{"topic": []byte("billing")}); err != nil {
		t.Fatalf("create source session: %v", err)
	}

	record := migration.Record{
		ID:                  fmt.Sprintf("migration-%x", seed),
		TenantID:            tenantID,
		AppID:               appID,
		SourceConfigVersion: v1.Version,
		TargetConfigVersion: v2.Version,
		Status:              migration.StatusPending,
	}
	if err := store.CreateDataMigration(ctx, record); err != nil {
		t.Fatalf("create data migration: %v", err)
	}
	if started, err := store.BeginDataMigrationControlPlane(ctx, tenantID, appID, record.ID, time.Now().Add(time.Minute)); err != nil {
		t.Fatalf("begin data migration from control plane: %v", err)
	} else if started.LeaseOwner != "" || started.Status != migration.StatusDraining {
		t.Fatalf("control-plane migration begin = %#v", started)
	}

	redisClient, err := platformredis.NewClient(ctx, *dataMigrationTestURL)
	if err != nil {
		t.Fatalf("create platform redis client: %v", err)
	}
	t.Cleanup(func() { _ = redisClient.Close() })
	stream, err := platformredis.NewStream(redisClient, fmt.Sprintf("migration-test:%x", seed), "integration", time.Second)
	if err != nil {
		t.Fatalf("create redis stream: %v", err)
	}
	getenv := migrationSecretEnvironment(
		tenantID,
		appID,
		*dataMigrationTestURL,
		*dataMigrationTestDSN,
	)
	artifacts, err := artifactcos.NewResolver(
		environmentSecretProvider{getenv: getenv},
		environmentCOSEndpointResolver{getenv: getenv},
	)
	if err != nil {
		t.Fatalf("new artifact resolver: %v", err)
	}
	t.Cleanup(func() { _ = artifacts.Close() })
	runtime, err := newWorkerRuntime(workerRuntimeDependencies{
		store:       store,
		redisClient: redisClient,
		stream:      stream,
		owner:       "worker-migration",
		getenv:      getenv,
		artifacts:   artifacts,
	})
	if err != nil {
		t.Fatalf("new worker runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.close() })
	if err := runtime.runDataMigrationPass(ctx); err != nil {
		t.Fatalf("run data migration pass: %v", err)
	}

	app, err := store.ResolveAgentApp(ctx, tenantID, appID)
	if err != nil {
		t.Fatalf("resolve app after migration: %v", err)
	}
	if app.ActiveConfigVersion != v2.Version {
		t.Fatalf("active config = %q, want %q", app.ActiveConfigVersion, v2.Version)
	}
	status := migration.Status("")
	var totalSessions, copyProgress, verifyProgress, successCount int64
	var checkpointAt *time.Time
	var failureStage string
	if err := pool.QueryRow(ctx, `
SELECT status, total_sessions, copy_progress, verify_progress,
       success_count, last_checkpoint_at, last_failure_stage
FROM platform.data_migration
WHERE migration_id = $1`, record.ID).Scan(
		&status,
		&totalSessions,
		&copyProgress,
		&verifyProgress,
		&successCount,
		&checkpointAt,
		&failureStage,
	); err != nil {
		t.Fatalf("query migration checkpoint: %v", err)
	}
	if status != migration.StatusSucceeded {
		t.Fatalf("migration status = %q, want %q", status, migration.StatusSucceeded)
	}
	if totalSessions != 1 || copyProgress != 1 || verifyProgress != 1 || successCount != 1 || checkpointAt == nil || failureStage != "" {
		t.Fatalf("migration checkpoint = total=%d copy=%d verify=%d success=%d checkpoint_at=%v failure_stage=%q", totalSessions, copyProgress, verifyProgress, successCount, checkpointAt, failureStage)
	}
	targetExec := dataMigrationExecution(t, key, v2)
	target, err := runtime.postgresSessions.ResolveSession(ctx, targetExec)
	if err != nil {
		t.Fatalf("resolve postgres target session: %v", err)
	}
	targetSession, err := target.GetSession(ctx, key)
	if err != nil {
		t.Fatalf("read migrated session: %v", err)
	}
	if targetSession == nil || string(targetSession.State["topic"]) != "billing" || len(targetSession.Events) != 0 {
		t.Fatalf("migrated session = %#v", targetSession)
	}
}

func TestWorkerRuntimeKeepsMigrationResumableWhenTargetProviderUnavailable(t *testing.T) {
	if *dataMigrationTestDSN == "" || *dataMigrationTestURL == "" {
		t.Fatal("TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN and TRPC_AGENT_SERVICE_REDIS_TEST_URL are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := openDataMigrationIntegrationPool(t)
	store, err := postgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres store: %v", err)
	}

	seed := time.Now().UnixNano()
	tenantID := fmt.Sprintf("migration-copy-failure-%x", seed)
	appID := "support"
	schema := fmt.Sprintf("copyfail%x", seed&0xffffff)
	t.Cleanup(func() { cleanupDataMigrationIntegration(t, pool, tenantID, appID, schema) })
	v1, v2 := dataMigrationAppConfigs(tenantID, appID, schema)
	if err := store.CreateTenant(ctx, tenant.Tenant{ID: tenantID, Name: "Migration Copy Failure", Status: tenant.StatusActive}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Migration Copy Failure", ActiveConfigVersion: v1.Version, Status: tenant.StatusActive,
	}, v1); err != nil {
		t.Fatalf("create agent app: %v", err)
	}
	if err := store.InsertAppConfigVersion(ctx, v2); err != nil {
		t.Fatalf("insert target config: %v", err)
	}

	key := dataMigrationSessionKey(t, tenantID, appID)
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.session_lane (tenant_id, app_id, session_principal_id, session_id)
VALUES ($1, $2, $3, $4)`, tenantID, appID, key.UserID, key.SessionID); err != nil {
		t.Fatalf("insert session lane: %v", err)
	}
	source, err := redisprovider.NewService(redisprovider.WithRedisClientURL(*dataMigrationTestURL))
	if err != nil {
		t.Fatalf("create redis session service: %v", err)
	}
	t.Cleanup(func() {
		_ = source.DeleteSession(context.Background(), key)
		_ = source.Close()
	})
	if _, err := source.CreateSession(ctx, key, session.StateMap{"topic": []byte("billing")}); err != nil {
		t.Fatalf("create source session: %v", err)
	}

	record := migration.Record{
		ID:                  fmt.Sprintf("migration-copy-failure-%x", seed),
		TenantID:            tenantID,
		AppID:               appID,
		SourceConfigVersion: v1.Version,
		TargetConfigVersion: v2.Version,
		Status:              migration.StatusPending,
	}
	if err := store.CreateDataMigration(ctx, record); err != nil {
		t.Fatalf("create data migration: %v", err)
	}
	if _, err := store.BeginDataMigration(ctx, tenantID, appID, record.ID, "worker-copy-failure", time.Now().Add(time.Minute), dataMigrationLease); err != nil {
		t.Fatalf("begin data migration: %v", err)
	}

	runtime := newDataMigrationTestRuntime(
		t, ctx, store, seed, tenantID, appID, "worker-copy-failure",
		*dataMigrationTestURL,
		"postgres://trpc:development-only-change-me@127.0.0.1:1/trpc_agent_service_test?sslmode=disable",
	)
	if err := runtime.runDataMigrationPass(ctx); err == nil {
		t.Fatal("run data migration pass succeeded while target provider was unavailable")
	}
	var status migration.Status
	var reason string
	if err := pool.QueryRow(ctx, `
SELECT status, COALESCE(failure_reason, '')
FROM platform.data_migration
WHERE migration_id = $1`, record.ID).Scan(&status, &reason); err != nil {
		t.Fatalf("query migration after unavailable target: %v", err)
	}
	if status != migration.StatusDraining || reason != "" {
		t.Fatalf("migration status=%q reason=%q, want resumable draining record", status, reason)
	}
	app, err := store.ResolveAgentApp(ctx, tenantID, appID)
	if err != nil {
		t.Fatalf("resolve app after unavailable target: %v", err)
	}
	if app.ActiveConfigVersion != v1.Version {
		t.Fatalf("active config after unavailable target = %q, want %q", app.ActiveConfigVersion, v1.Version)
	}
}

func TestDataMigrationLeaseTakeoverRejectsPreviousWorker(t *testing.T) {
	if *dataMigrationTestDSN == "" {
		t.Fatal("TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool := openDataMigrationIntegrationPool(t)
	store, err := postgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres store: %v", err)
	}

	seed := time.Now().UnixNano()
	tenantID := fmt.Sprintf("migration-takeover-%x", seed)
	appID := "support"
	schema := fmt.Sprintf("takeover%x", seed&0xffffff)
	t.Cleanup(func() { cleanupDataMigrationIntegration(t, pool, tenantID, appID, schema) })
	v1, v2 := dataMigrationAppConfigs(tenantID, appID, schema)
	if err := store.CreateTenant(ctx, tenant.Tenant{ID: tenantID, Name: "Migration Takeover", Status: tenant.StatusActive}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Migration Takeover", ActiveConfigVersion: v1.Version, Status: tenant.StatusActive,
	}, v1); err != nil {
		t.Fatalf("create agent app: %v", err)
	}
	if err := store.InsertAppConfigVersion(ctx, v2); err != nil {
		t.Fatalf("insert target config: %v", err)
	}

	record := migration.Record{
		ID:                  fmt.Sprintf("migration-takeover-%x", seed),
		TenantID:            tenantID,
		AppID:               appID,
		SourceConfigVersion: v1.Version,
		TargetConfigVersion: v2.Version,
		Status:              migration.StatusPending,
	}
	if err := store.CreateDataMigration(ctx, record); err != nil {
		t.Fatalf("create data migration: %v", err)
	}
	previous, err := store.BeginDataMigration(ctx, tenantID, appID, record.ID, "worker-previous", time.Now().Add(time.Minute), dataMigrationLease)
	if err != nil {
		t.Fatalf("begin data migration: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE platform.data_migration SET lease_until = clock_timestamp() - interval '1 second' WHERE migration_id = $1`, record.ID); err != nil {
		t.Fatalf("expire migration lease: %v", err)
	}
	claimed, found, err := store.ClaimNextDataMigration(ctx, "worker-successor", dataMigrationLease)
	if err != nil {
		t.Fatalf("claim expired data migration: %v", err)
	}
	if !found || claimed.LeaseOwner != "worker-successor" || claimed.RunToken == previous.RunToken {
		t.Fatalf("claimed migration = %+v, previous token = %q", claimed, previous.RunToken)
	}
	if err := store.AdvanceDataMigration(ctx, previous, migration.StatusCopying); !errors.Is(err, migration.ErrLeaseLost) {
		t.Fatalf("previous worker advance error = %v, want lease lost", err)
	}
}

func dataMigrationAppConfigs(tenantID, appID, schema string) (tenant.AppConfig, tenant.AppConfig) {
	redisSecret := tenant.SecretRef{Name: "migration-redis-url", Version: "1"}
	postgresSecret := tenant.SecretRef{Name: "migration-postgres-dsn", Version: "1"}
	base := tenant.AppConfig{
		TenantID: tenantID,
		AppID:    appID,
		Model: tenant.ModelConfig{
			Provider:  "openai",
			Model:     "gpt-4.1-mini",
			APIKeyRef: tenant.SecretRef{Name: "model-key", Version: "1"},
		},
		SecretRefs: []tenant.SecretRef{
			{Name: "model-key", Version: "1"},
			redisSecret,
			postgresSecret,
		},
	}
	v1 := base
	v1.Version = "v1"
	v1.BackendConfig = tenant.BackendConfig{
		Name: "redis-source",
		Session: tenant.BackendRef{
			Kind:      tenant.BackendRedis,
			Provider:  "redis",
			Name:      "redis-source",
			SecretRef: redisSecret,
		},
	}
	v2 := base
	v2.Version = "v2"
	v2.BackendConfig = tenant.BackendConfig{
		Name: "postgres-target",
		Session: tenant.BackendRef{
			Kind:      tenant.BackendSQL,
			Provider:  "postgres",
			Name:      "postgres-target",
			SecretRef: postgresSecret,
			Options:   map[string]string{"schema": schema},
		},
	}
	return v1, v2
}

func dataMigrationExecution(t *testing.T, key session.Key, config tenant.AppConfig) worker.Execution {
	t.Helper()
	runtime := tenant.RuntimeContext{
		TenantID:           config.TenantID,
		AppID:              config.AppID,
		ConfigVersion:      config.Version,
		SessionPrincipalID: key.UserID,
		SessionID:          key.SessionID,
		UserID:             key.UserID,
		TraceID:            "migration-integration",
	}
	return worker.Execution{Tenant: runtime, Config: config}
}

func dataMigrationSessionKey(t *testing.T, tenantID, appID string) session.Key {
	t.Helper()
	appName, err := (tenant.Scope{TenantID: tenantID, AppID: appID}).Key("runner")
	if err != nil {
		t.Fatalf("build session app name: %v", err)
	}
	return session.Key{AppName: appName, UserID: "user-1", SessionID: "session-1"}
}

func newDataMigrationTestRuntime(
	t *testing.T,
	ctx context.Context,
	store *postgres.Store,
	seed int64,
	tenantID string,
	appID string,
	owner string,
	sessionRedisURL string,
	postgresDSN string,
) *workerRuntime {
	t.Helper()
	redisClient, err := platformredis.NewClient(ctx, *dataMigrationTestURL)
	if err != nil {
		t.Fatalf("create platform redis client: %v", err)
	}
	stream, err := platformredis.NewStream(redisClient, fmt.Sprintf("migration-test:%x", seed), "integration", time.Second)
	if err != nil {
		_ = redisClient.Close()
		t.Fatalf("create redis stream: %v", err)
	}
	getenv := migrationSecretEnvironment(
		tenantID,
		appID,
		sessionRedisURL,
		postgresDSN,
	)
	artifacts, err := artifactcos.NewResolver(
		environmentSecretProvider{getenv: getenv},
		environmentCOSEndpointResolver{getenv: getenv},
	)
	if err != nil {
		_ = redisClient.Close()
		t.Fatalf("new artifact resolver: %v", err)
	}
	t.Cleanup(func() { _ = artifacts.Close() })
	runtime, err := newWorkerRuntime(workerRuntimeDependencies{
		store:       store,
		redisClient: redisClient,
		stream:      stream,
		owner:       owner,
		getenv:      getenv,
		artifacts:   artifacts,
	})
	if err != nil {
		_ = redisClient.Close()
		t.Fatalf("new worker runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.close() })
	return runtime
}

func migrationSecretEnvironment(tenantID, appID, redisURL, postgresDSN string) func(string) string {
	scope := tenant.Scope{TenantID: tenantID, AppID: appID}
	return environmentReader(map[string]string{
		scopedSecretEnvironmentKey(scope, tenant.SecretRef{Name: "migration-redis-url", Version: "1"}):    redisURL,
		scopedSecretEnvironmentKey(scope, tenant.SecretRef{Name: "migration-postgres-dsn", Version: "1"}): postgresDSN,
	})
}

func openDataMigrationIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(*dataMigrationTestDSN)
	if err != nil {
		t.Fatalf("parse postgres test DSN: %v", err)
	}
	if !strings.HasSuffix(config.ConnConfig.Database, "_test") {
		t.Fatalf("postgres integration database %q must end in _test", config.ConnConfig.Database)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("open postgres integration pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func cleanupDataMigrationIntegration(t *testing.T, pool *pgxpool.Pool, tenantID, appID, schema string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, `DELETE FROM platform.execution WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID); err != nil {
		t.Errorf("delete execution: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM platform.data_migration WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID); err != nil {
		t.Errorf("delete data migration: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM platform.session_lane WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID); err != nil {
		t.Errorf("delete session lane: %v", err)
	}
	// Published AppConfig rows are intentionally immutable. The test uses a
	// unique tenant ID and the dedicated _test database, so control-plane rows
	// remain after mutable migration data is removed above.
	if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE"); err != nil {
		t.Errorf("drop target session schema: %v", err)
	}
}
