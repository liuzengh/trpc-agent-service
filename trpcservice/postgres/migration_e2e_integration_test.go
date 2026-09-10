//go:build integration && e2e

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionpostgres "trpc.group/trpc-go/trpc-agent-go/session/postgres"
	redisprovider "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

const (
	migrationE2EWorkerBinaryEnv = "TRPC_AGENT_SERVICE_MIGRATION_WORKER_BINARY"
	migrationE2EReportEnv       = "TRPC_MIGRATION_E2E_REPORT"
	migrationVerifyReportEnv    = "TRPC_MIGRATION_VERIFY_E2E_REPORT"
	migrationE2ESessionCount    = 12
)

// TestSessionMigrationAcrossRealWorkerProcesses proves that the production
// migration loop claims a durable Redis-to-PostgreSQL migration, resumes from
// its checkpoint after a real process crash, verifies all source sessions, and
// cuts over only after verification succeeds.
func TestSessionMigrationAcrossRealWorkerProcesses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pool, postgresDSN := openRuntimePool(t)
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres: %v", err)
	}

	runID := strings.ReplaceAll(uuid.NewString(), "-", "")
	tenantID := "migration-e2e-" + runID
	appID := "support"
	schema := "migration_e2e_" + runID[:12]
	v1, v2 := migrationE2EConfigs(tenantID, appID, schema)
	controlPlane := admin.API{Repository: store}
	if err := controlPlane.CreateTenant(ctx, tenant.Tenant{
		ID: tenantID, Name: "Migration E2E", Status: tenant.StatusActive,
	}); err != nil {
		t.Fatalf("create migration tenant: %v", err)
	}
	if err := controlPlane.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Migration E2E",
		ActiveConfigVersion: v1.Version, Status: tenant.StatusActive,
	}, v1); err != nil {
		t.Fatalf("create migration app: %v", err)
	}
	if err := controlPlane.PublishAppConfig(ctx, v2); err != nil {
		t.Fatalf("publish migration target config: %v", err)
	}
	issued, err := controlPlane.IssueCredential(ctx, tenant.Scope{TenantID: tenantID, AppID: appID}, time.Time{})
	if err != nil {
		t.Fatalf("issue migration credential: %v", err)
	}

	redisURL := runtimeRedisURL()
	redisClient, err := platformredis.NewClient(ctx, redisURL)
	if err != nil {
		t.Fatalf("new migration redis client: %v", err)
	}
	defer redisClient.Close()
	streamName := "trpc-agent-service:migration-e2e:" + uuid.NewString()
	group := "migration-e2e-workers"
	stream, err := platformredis.NewStream(redisClient, streamName, group, 5*time.Second)
	if err != nil {
		t.Fatalf("new migration stream: %v", err)
	}
	if err := stream.Init(ctx); err != nil {
		t.Fatalf("initialize migration stream: %v", err)
	}
	dispatcher, err := relay.New(store, stream, "migration-e2e-relay")
	if err != nil {
		t.Fatalf("new migration relay: %v", err)
	}
	relayCtx, stopRelay := context.WithCancel(ctx)
	relayDone := make(chan error, 1)
	go func() { relayDone <- dispatcher.Run(relayCtx) }()
	t.Cleanup(func() {
		stopRelay()
		if err := <-relayDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("migration relay stopped with error: %v", err)
		}
	})

	// Seed empty-history sessions. The Redis adapter cannot enumerate summary
	// inventory for non-empty sessions, so this success path intentionally uses
	// the provider's provably empty-summary case.
	workerBinary := buildMigrationWorker(t)
	appName, err := (tenant.Scope{TenantID: tenantID, AppID: appID}).Key("runner")
	if err != nil {
		t.Fatalf("build migration session app name: %v", err)
	}
	keys := make([]session.Key, 0, migrationE2ESessionCount)
	sourceService, err := redisprovider.NewService(redisprovider.WithRedisClientURL(redisURL))
	if err != nil {
		t.Fatalf("open migration source session service: %v", err)
	}
	var source session.Service = sourceService
	t.Cleanup(func() {
		for _, key := range keys {
			if err := source.DeleteSession(context.Background(), key); err != nil {
				t.Logf("delete migration source session %s: %v", key.SessionID, err)
			}
		}
		_ = source.Close()
	})
	trackService, ok := source.(session.TrackService)
	if !ok {
		t.Fatal("redis source session service does not implement TrackService")
	}
	for index := 0; index < migrationE2ESessionCount; index++ {
		key := session.Key{
			AppName: appName, UserID: "service:" + issued.Credential.ID,
			SessionID: fmt.Sprintf("migration-session-%02d", index),
		}
		keys = append(keys, key)
		if _, err := pool.Exec(ctx, `
INSERT INTO platform.session_lane (tenant_id, app_id, session_principal_id, session_id)
VALUES ($1, $2, $3, $4)`, tenantID, appID, key.UserID, key.SessionID); err != nil {
			t.Fatalf("create migration session lane %d: %v", index, err)
		}
		if _, err := source.CreateSession(ctx, key, session.StateMap{"topic": []byte("migration")}); err != nil {
			t.Fatalf("create migration source session %d: %v", index, err)
		}
		loaded, err := source.GetSession(ctx, key)
		if err != nil || loaded == nil {
			t.Fatalf("read source session %d: session=%v err=%v", index, loaded, err)
		}
		if err := trackService.AppendTrackEvent(ctx, loaded, &session.TrackEvent{
			Track: session.Track("migration-turn"), Payload: json.RawMessage(`{"stage":"source"}`), Timestamp: time.Unix(int64(index+1), 0).UTC(),
		}); err != nil {
			t.Fatalf("append source track %d: %v", index, err)
		}
	}

	record := migrationE2ERecord(tenantID, appID, v1.Version, v2.Version)
	if err := store.CreateDataMigration(ctx, record); err != nil {
		t.Fatalf("create migration: %v", err)
	}
	if started, err := controlPlane.BeginDataMigration(ctx, tenant.Scope{TenantID: tenantID, AppID: appID}, record.ID, "", time.Now().Add(time.Minute), 30*time.Second); err != nil {
		t.Fatalf("begin migration through admin API: %v", err)
	} else if started.Status != "DRAINING" || started.LeaseOwner != "" {
		t.Fatalf("migration begin = %#v", started)
	}

	evidence := migrationE2EEvidence{
		MigrationID: record.ID, TenantID: tenantID, AppID: appID,
		SourceConfig: v1.Version, TargetConfig: v2.Version, Stream: streamName,
	}
	defer func() { writeMigrationE2EEvidence(t, evidence) }()

	first := startMigrationWorker(t, ctx, workerBinary, migrationWorkerEnv(postgresDSN, redisURL, streamName, group), "migration-worker-a")
	checkpoint, err := waitMigrationE2EState(ctx, pool, record.ID, func(value migrationE2EState) bool {
		return value.Status == "COPYING" && value.CopyProgress > 0 && value.CopyProgress < value.TotalSessions
	})
	if err != nil {
		t.Fatalf("wait migration checkpoint before crash: %v", err)
	}
	evidence.CrashCheckpoint = checkpoint
	app, err := store.ResolveAgentApp(ctx, tenantID, appID)
	if err != nil {
		t.Fatalf("resolve app during migration: %v", err)
	}
	if app.ActiveConfigVersion != v1.Version {
		t.Fatalf("active config changed before verify: %q", app.ActiveConfigVersion)
	}
	stopRuntimeWorker(first, true)

	second := startMigrationWorker(t, ctx, workerBinary, migrationWorkerEnv(postgresDSN, redisURL, streamName, group), "migration-worker-b")
	resumed, err := waitMigrationE2EState(ctx, pool, record.ID, func(value migrationE2EState) bool {
		return value.LeaseOwner == "migration-worker-b" && value.CopyProgress > checkpoint.CopyProgress
	})
	if err != nil {
		t.Fatalf("wait migration resume from checkpoint: %v", err)
	}
	evidence.Resume = resumed
	terminal, err := waitMigrationE2EState(ctx, pool, record.ID, func(value migrationE2EState) bool {
		return value.Status == "SUCCEEDED"
	})
	if err != nil {
		t.Fatalf("wait migration terminal state: %v", err)
	}
	evidence.Terminal = terminal
	if terminal.TotalSessions != migrationE2ESessionCount ||
		terminal.CopyProgress != terminal.TotalSessions ||
		terminal.VerifyProgress != terminal.TotalSessions ||
		terminal.SuccessCount != terminal.TotalSessions ||
		terminal.LeaseOwner != "" || terminal.FailureStage != "" {
		t.Fatalf("migration terminal checkpoint = %#v", terminal)
	}
	app, err = store.ResolveAgentApp(ctx, tenantID, appID)
	if err != nil {
		t.Fatalf("resolve app after migration: %v", err)
	}
	if app.ActiveConfigVersion != v2.Version {
		t.Fatalf("active config after migration = %q, want %q", app.ActiveConfigVersion, v2.Version)
	}
	evidence.ActiveConfig = app.ActiveConfigVersion

	target, err := sessionpostgres.NewService(
		sessionpostgres.WithPostgresClientDSN(postgresDSN),
		sessionpostgres.WithSchema(schema),
	)
	if err != nil {
		t.Fatalf("open migrated postgres session service: %v", err)
	}
	t.Cleanup(func() { _ = target.Close() })
	for _, key := range keys {
		loaded, err := target.GetSession(ctx, key)
		if err != nil || loaded == nil {
			t.Fatalf("read migrated session %s: session=%v err=%v", key.SessionID, loaded, err)
		}
		if len(loaded.Events) != 0 || string(loaded.State["topic"]) != "migration" {
			t.Fatalf("migrated session %s lost state/events: %#v", key.SessionID, loaded)
		}
		if len(loaded.Tracks[session.Track("migration-turn")].Events) != 1 {
			t.Fatalf("migrated session %s lost track data: %#v", key.SessionID, loaded.Tracks)
		}
	}
	stopRuntimeWorker(second, true)
}

// TestSessionMigrationVerifyFailureDoesNotCutover proves that a target
// inconsistency discovered by the real VERIFYING phase fails closed: the
// source remains authoritative and a post-failure request still pins v1.
func TestSessionMigrationVerifyFailureDoesNotCutover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pool, postgresDSN := openRuntimePool(t)
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate postgres: %v", err)
	}

	runID := strings.ReplaceAll(uuid.NewString(), "-", "")
	tenantID := "migration-verify-e2e-" + runID
	appID := "support"
	schema := "mig_verify_" + runID[:12]
	v1, v2 := migrationE2EConfigs(tenantID, appID, schema)
	controlPlane := admin.API{Repository: store}
	if err := controlPlane.CreateTenant(ctx, tenant.Tenant{
		ID: tenantID, Name: "Migration Verify E2E", Status: tenant.StatusActive,
	}); err != nil {
		t.Fatalf("create migration verify tenant: %v", err)
	}
	if err := controlPlane.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Migration Verify E2E",
		ActiveConfigVersion: v1.Version, Status: tenant.StatusActive,
	}, v1); err != nil {
		t.Fatalf("create migration verify app: %v", err)
	}
	if err := controlPlane.PublishAppConfig(ctx, v2); err != nil {
		t.Fatalf("publish migration verify target: %v", err)
	}
	issued, err := controlPlane.IssueCredential(ctx, tenant.Scope{TenantID: tenantID, AppID: appID}, time.Time{})
	if err != nil {
		t.Fatalf("issue migration verify credential: %v", err)
	}

	redisURL := runtimeRedisURL()
	redisClient, err := platformredis.NewClient(ctx, redisURL)
	if err != nil {
		t.Fatalf("new migration verify redis client: %v", err)
	}
	defer redisClient.Close()
	streamName := "trpc-agent-service:migration-verify-e2e:" + uuid.NewString()
	group := "migration-verify-e2e-workers"
	stream, err := platformredis.NewStream(redisClient, streamName, group, 5*time.Second)
	if err != nil {
		t.Fatalf("new migration verify stream: %v", err)
	}
	if err := stream.Init(ctx); err != nil {
		t.Fatalf("initialize migration verify stream: %v", err)
	}
	dispatcher, err := relay.New(store, stream, "migration-verify-e2e-relay")
	if err != nil {
		t.Fatalf("new migration verify relay: %v", err)
	}
	relayCtx, stopRelay := context.WithCancel(ctx)
	relayDone := make(chan error, 1)
	go func() { relayDone <- dispatcher.Run(relayCtx) }()
	t.Cleanup(func() {
		stopRelay()
		if err := <-relayDone; err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("migration verify relay stopped with error: %v", err)
		}
	})

	workerBinary := buildMigrationWorker(t)
	workerEnv := runtimeWorkerEnv(postgresDSN, redisURL, streamName, group)
	sessionID := "migration-verify-session"

	appName, err := (tenant.Scope{TenantID: tenantID, AppID: appID}).Key("runner")
	if err != nil {
		t.Fatalf("build migration verify session app name: %v", err)
	}
	key := session.Key{AppName: appName, UserID: "service:" + issued.Credential.ID, SessionID: sessionID}
	sourceService, err := redisprovider.NewService(redisprovider.WithRedisClientURL(redisURL))
	if err != nil {
		t.Fatalf("open migration verify source session service: %v", err)
	}
	t.Cleanup(func() {
		if err := sourceService.DeleteSession(context.Background(), key); err != nil {
			t.Logf("delete migration verify source session: %v", err)
		}
		_ = sourceService.Close()
	})
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.session_lane (tenant_id, app_id, session_principal_id, session_id)
VALUES ($1, $2, $3, $4)`, tenantID, appID, key.UserID, key.SessionID); err != nil {
		t.Fatalf("create migration verify session lane: %v", err)
	}
	if _, err := sourceService.CreateSession(ctx, key, session.StateMap{"topic": []byte("migration")}); err != nil {
		t.Fatalf("create migration verify source session: %v", err)
	}
	record := migrationE2ERecord(tenantID, appID, v1.Version, v2.Version)
	if err := store.CreateDataMigration(ctx, record); err != nil {
		t.Fatalf("create migration verify record: %v", err)
	}
	if started, err := controlPlane.BeginDataMigration(ctx, tenant.Scope{TenantID: tenantID, AppID: appID}, record.ID, "", time.Now().Add(time.Minute), 30*time.Second); err != nil {
		t.Fatalf("begin migration verify through admin API: %v", err)
	} else if started.Status != "DRAINING" || started.LeaseOwner != "" {
		t.Fatalf("migration verify begin = %#v", started)
	}

	evidence := migrationVerifyE2EEvidence{
		MigrationID: record.ID, TenantID: tenantID, AppID: appID,
		SourceConfig: v1.Version, TargetConfig: v2.Version, Stream: streamName,
	}
	defer func() { writeMigrationVerifyE2EEvidence(t, evidence) }()

	migrationEnv := migrationWorkerEnv(postgresDSN, redisURL, streamName, group)
	migrationEnv["TRPC_AGENT_SERVICE_FAULT_PAUSE_AFTER_MIGRATION_COPY"] = "10s"
	worker := startMigrationWorker(t, ctx, workerBinary, migrationEnv, "migration-verify-worker")
	verifying, err := waitMigrationE2EState(ctx, pool, record.ID, func(value migrationE2EState) bool {
		return value.Status == "VERIFYING" && value.CopyProgress == 1 && value.VerifyProgress == 0
	})
	if err != nil {
		t.Fatalf("wait migration verify pause: %v", err)
	}
	evidence.VerifyStart = verifying

	// The real migration copier creates the configured target schema. Open the
	// official target service only after the durable VERIFYING checkpoint.
	targetService, err := sessionpostgres.NewService(
		sessionpostgres.WithPostgresClientDSN(postgresDSN),
		sessionpostgres.WithSchema(schema),
	)
	if err != nil {
		t.Fatalf("open migration verify target session service: %v", err)
	}
	t.Cleanup(func() { _ = targetService.Close() })
	targetSession, err := targetService.GetSession(ctx, key)
	if err != nil || targetSession == nil {
		t.Fatalf("read copied target session before corruption: session=%v err=%v", targetSession, err)
	}
	if err := targetService.UpdateSessionState(ctx, key, session.StateMap{"topic": []byte("corrupted-by-verify-e2e")}); err != nil {
		t.Fatalf("inject target inconsistency: %v", err)
	}

	terminal, err := waitMigrationE2EState(ctx, pool, record.ID, func(value migrationE2EState) bool {
		return value.Status == "FAILED"
	})
	if err != nil {
		t.Fatalf("wait migration verify failure: %v", err)
	}
	evidence.Terminal = terminal
	if terminal.FailureStage != "verify" || terminal.LeaseOwner != "" || terminal.CopyProgress != 1 || terminal.VerifyProgress != 0 {
		t.Fatalf("migration verify failure checkpoint = %#v", terminal)
	}
	if terminal.FailureReason == "" || strings.Contains(terminal.FailureReason, postgresDSN) ||
		strings.Contains(terminal.FailureReason, "development-only-change-me") || strings.Contains(terminal.FailureReason, redisURL) {
		t.Fatalf("migration verify failure leaked sensitive connection data: %q", terminal.FailureReason)
	}
	evidence.FailureReasonSafe = true
	stopRuntimeWorker(worker, true)

	admitter := gateway.New(store)
	app, err := store.ResolveAgentApp(ctx, tenantID, appID)
	if err != nil {
		t.Fatalf("resolve app after migration verify failure: %v", err)
	}
	if app.ActiveConfigVersion != v1.Version {
		t.Fatalf("active config after migration verify failure = %q, want %q", app.ActiveConfigVersion, v1.Version)
	}
	evidence.ActiveConfig = app.ActiveConfigVersion

	// A post-failure request must still use the Redis source selected by v1.
	postFailureWorker := startRuntimeWorker(t, ctx, buildRuntimeWorker(t), workerEnv, "migration-verify-source-after-failure", "")
	postRequestID := "migration-verify-after-failure-" + runID
	postRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://migration-verify-e2e.local/v1/chat/completions", nil)
	if err != nil {
		t.Fatalf("build post-failure authentication request: %v", err)
	}
	postRequest.Header.Set("Authorization", "Bearer "+issued.APIKey)
	postIdentity, err := (auth.HTTPAPIKeyResolver{Credentials: store, Directory: store}).Resolve(ctx, postRequest, auth.RequestIdentity{
		SessionID: sessionID, TraceID: postRequestID,
	})
	if err != nil {
		t.Fatalf("authenticate post-failure request: %v", err)
	}
	if _, err := admitter.Handle(ctx, gateway.Request{
		RequestID: postRequestID, IdempotencyKey: "migration-verify-after-failure-idem",
		Tenant: postIdentity, Message: gateway.Message{Text: "post verify failure"},
	}); err != nil {
		t.Fatalf("admit post-failure request: %v", err)
	}
	postExecution, err := waitRuntimeExecution(ctx, pool, postRequestID, func(value runtimeExecution) bool {
		return value.Status == "SUCCEEDED"
	})
	if err != nil {
		t.Fatalf("wait post-failure execution: %v", err)
	}
	if postExecution.ConfigVersion != v1.Version {
		t.Fatalf("post-failure request config = %q, want %q", postExecution.ConfigVersion, v1.Version)
	}
	evidence.PostFailureRequest = postExecution
	stopRuntimeWorker(postFailureWorker, true)
}

type migrationE2EState struct {
	Status         string
	LeaseOwner     string
	TotalSessions  int64
	CopyProgress   int64
	VerifyProgress int64
	SuccessCount   int64
	FailureStage   string
	FailureReason  string
}

func readMigrationE2EState(ctx context.Context, pool *pgxpool.Pool, migrationID string) (migrationE2EState, error) {
	var value migrationE2EState
	err := pool.QueryRow(ctx, `
SELECT status, COALESCE(lease_owner, ''), total_sessions, copy_progress,
       verify_progress, success_count, COALESCE(last_failure_stage, ''),
       COALESCE(failure_reason, '')
FROM platform.data_migration
WHERE migration_id = $1`, migrationID).Scan(
		&value.Status, &value.LeaseOwner, &value.TotalSessions, &value.CopyProgress,
		&value.VerifyProgress, &value.SuccessCount, &value.FailureStage, &value.FailureReason,
	)
	return value, err
}

func waitMigrationE2EState(ctx context.Context, pool *pgxpool.Pool, migrationID string, predicate func(migrationE2EState) bool) (migrationE2EState, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		value, err := readMigrationE2EState(ctx, pool, migrationID)
		if err == nil && predicate(value) {
			return value, nil
		}
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return migrationE2EState{}, err
		}
		select {
		case <-ctx.Done():
			return migrationE2EState{}, fmt.Errorf("wait migration %s: %w", migrationID, ctx.Err())
		case <-ticker.C:
		}
	}
}

func migrationE2EConfigs(tenantID, appID, schema string) (tenant.AppConfig, tenant.AppConfig) {
	base := tenant.AppConfig{
		TenantID: tenantID,
		AppID:    appID,
		Model: tenant.ModelConfig{
			Provider:  tenant.ModelProviderOpenAI,
			Model:     "migration-e2e-model",
			APIKeyRef: tenant.SecretRef{Name: "migration-e2e-model-key", Version: "1"},
		},
		SecretRefs: []tenant.SecretRef{{Name: "migration-e2e-model-key", Version: "1"}},
	}
	v1 := base
	v1.Version = "v1"
	v1.BackendConfig = tenant.BackendConfig{
		Name:    "migration-redis-source",
		Session: tenant.BackendRef{Kind: tenant.BackendRedis, Provider: "redis", Name: "migration-redis-source"},
	}
	v2 := base
	v2.Version = "v2"
	v2.BackendConfig = tenant.BackendConfig{
		Name: "migration-postgres-target",
		Session: tenant.BackendRef{
			Kind: tenant.BackendSQL, Provider: "postgres", Name: "migration-postgres-target",
			Options: map[string]string{"schema": schema},
		},
	}
	return v1, v2
}

func migrationE2ERecord(tenantID, appID, source, target string) migration.Record {
	return migration.Record{
		ID: uuid.NewString(), TenantID: tenantID, AppID: appID,
		SourceConfigVersion: source, TargetConfigVersion: target,
		Status: migration.StatusPending,
	}
}

func buildMigrationWorker(t *testing.T) string {
	t.Helper()
	if value := strings.TrimSpace(os.Getenv(migrationE2EWorkerBinaryEnv)); value != "" {
		return value
	}
	name := "trpc-agent-service-migration-worker"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	path := filepath.Join(t.TempDir(), name)
	command := exec.Command("go", "build", "-o", path, "./cmd/trpc-service")
	command.Dir = repositoryRoot(t)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build migration worker: %v\n%s", err, output)
	}
	return path
}

func migrationWorkerEnv(postgresDSN, redisURL, stream, group string) map[string]string {
	return map[string]string{
		"TRPC_AGENT_SERVICE_ROLE":               "worker",
		"TRPC_AGENT_SERVICE_POSTGRES_DSN":       postgresDSN,
		"TRPC_AGENT_SERVICE_REDIS_URL":          redisURL,
		"TRPC_AGENT_SERVICE_REDIS_STREAM":       stream,
		"TRPC_AGENT_SERVICE_REDIS_GROUP":        group,
		"TRPC_AGENT_SERVICE_WORKER_CONCURRENCY": "1",
		"TRPC_AGENT_SERVICE_MODEL_TIMEOUT":      "15s",
		"TRPC_AGENT_SERVICE_SHUTDOWN_TIMEOUT":   "10s",
	}
}

func startMigrationWorker(t *testing.T, ctx context.Context, binary string, baseEnv map[string]string, owner string) *runtimeWorker {
	t.Helper()
	address := freeTCPAddress(t)
	env := append([]string(nil), os.Environ()...)
	for key, value := range baseEnv {
		env = append(env, key+"="+value)
	}
	env = append(env,
		"TRPC_AGENT_SERVICE_WORKER_ID="+owner,
		"TRPC_AGENT_SERVICE_HTTP_ADDR="+address,
	)
	logs := &lockedBuffer{}
	command := exec.CommandContext(ctx, binary)
	command.Dir = repositoryRoot(t)
	command.Env = env
	command.Stdout = logs
	command.Stderr = logs
	if err := command.Start(); err != nil {
		t.Fatalf("start migration worker %s: %v", owner, err)
	}
	worker := &runtimeWorker{owner: owner, addr: address, cmd: command, logs: logs, done: make(chan struct{})}
	go func() {
		err := command.Wait()
		worker.waitMu.Lock()
		worker.waitErr = err
		worker.waitMu.Unlock()
		close(worker.done)
	}()
	if err := waitRuntimeWorkerReady(ctx, worker); err != nil {
		stopRuntimeWorker(worker, true)
		t.Fatalf("wait migration worker %s ready: %v\n%s", owner, err, worker.logs.String())
	}
	t.Cleanup(func() { stopRuntimeWorker(worker, true) })
	return worker
}

type migrationE2EEvidence struct {
	MigrationID     string            `json:"migration_id"`
	TenantID        string            `json:"tenant_id"`
	AppID           string            `json:"app_id"`
	SourceConfig    string            `json:"source_config"`
	TargetConfig    string            `json:"target_config"`
	Stream          string            `json:"stream"`
	CrashCheckpoint migrationE2EState `json:"crash_checkpoint"`
	Resume          migrationE2EState `json:"resume"`
	Terminal        migrationE2EState `json:"terminal"`
	ActiveConfig    string            `json:"active_config"`
}

type migrationVerifyE2EEvidence struct {
	MigrationID        string            `json:"migration_id"`
	TenantID           string            `json:"tenant_id"`
	AppID              string            `json:"app_id"`
	SourceConfig       string            `json:"source_config"`
	TargetConfig       string            `json:"target_config"`
	Stream             string            `json:"stream"`
	VerifyStart        migrationE2EState `json:"verify_start"`
	Terminal           migrationE2EState `json:"terminal"`
	ActiveConfig       string            `json:"active_config"`
	FailureReasonSafe  bool              `json:"failure_reason_safe"`
	PostFailureRequest runtimeExecution  `json:"post_failure_request"`
}

func writeMigrationE2EEvidence(t *testing.T, evidence migrationE2EEvidence) {
	t.Helper()
	path := strings.TrimSpace(os.Getenv(migrationE2EReportEnv))
	if path == "" {
		t.Logf("migration e2e evidence: %+v", evidence)
		return
	}
	encoded, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Logf("encode migration e2e evidence: %v", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Logf("create migration e2e evidence directory: %v", err)
		return
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Logf("write migration e2e evidence: %v", err)
	}
}

func writeMigrationVerifyE2EEvidence(t *testing.T, evidence migrationVerifyE2EEvidence) {
	t.Helper()
	path := strings.TrimSpace(os.Getenv(migrationVerifyReportEnv))
	if path == "" {
		t.Logf("migration verify e2e evidence: %+v", evidence)
		return
	}
	encoded, err := json.MarshalIndent(evidence, "", "  ")
	if err != nil {
		t.Logf("encode migration verify e2e evidence: %v", err)
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Logf("create migration verify e2e evidence directory: %v", err)
		return
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Logf("write migration verify e2e evidence: %v", err)
	}
}
