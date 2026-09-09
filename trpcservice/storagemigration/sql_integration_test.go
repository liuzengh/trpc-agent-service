package storagemigration

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestPostgresMigrationWorkerResumesAndCopiesTenantMemory(t *testing.T) {
	baseDSN := os.Getenv("TRPC_AGENT_POSTGRES_TEST_DSN")
	if baseDSN == "" {
		t.Skip("set TRPC_AGENT_POSTGRES_TEST_DSN to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	control, err := sql.Open("pgx", baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	if err := repository.Migrate(ctx, func(ctx context.Context, script string) error { _, err := control.ExecContext(ctx, script); return err }, repository.DirectionUp); err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprint(time.Now().UnixNano())
	sourceSchema, targetSchema := "migration_source_"+suffix, "migration_target_"+suffix
	if _, err := control.ExecContext(ctx, "CREATE SCHEMA "+sourceSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := control.ExecContext(ctx, "CREATE SCHEMA "+targetSchema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = control.ExecContext(cleanupCtx, "DROP SCHEMA IF EXISTS "+sourceSchema+" CASCADE")
		_, _ = control.ExecContext(cleanupCtx, "DROP SCHEMA IF EXISTS "+targetSchema+" CASCADE")
	})
	sourceDSN := withSearchPath(t, baseDSN, sourceSchema)
	targetDSN := withSearchPath(t, baseDSN, targetSchema)
	sourceDB, err := sql.Open("pgx", sourceDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer sourceDB.Close()
	targetDB, err := sql.Open("pgx", targetDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer targetDB.Close()
	const memoryDDL = `CREATE TABLE runtime_memories (memory_id TEXT PRIMARY KEY,app_name TEXT NOT NULL,user_id TEXT NOT NULL,memory_data JSONB NOT NULL,created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,deleted_at TIMESTAMP);`
	if _, err := sourceDB.ExecContext(ctx, memoryDDL); err != nil {
		t.Fatal(err)
	}
	if _, err := targetDB.ExecContext(ctx, memoryDDL+`CREATE TABLE storage_migration_items (source_route_hash TEXT NOT NULL,table_name TEXT NOT NULL,source_key TEXT NOT NULL,checksum TEXT NOT NULL,copied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),PRIMARY KEY(source_route_hash,table_name,source_key));`); err != nil {
		t.Fatal(err)
	}
	tenantID, appID := "migration-"+suffix, "assistant"
	appName, _ := tenant.CanonicalAppName(tenantID, appID)
	for index := 0; index < 5; index++ {
		if _, err := sourceDB.ExecContext(ctx, `INSERT INTO runtime_memories(memory_id,app_name,user_id,memory_data) VALUES($1,$2,'user','{}')`, fmt.Sprintf("memory-%02d", index), appName); err != nil {
			t.Fatal(err)
		}
	}
	configStore, err := repository.NewSQLStore(control)
	if err != nil {
		t.Fatal(err)
	}
	_, err = configStore.PublishConfig(ctx, repository.ConfigRecord{TenantID: tenantID, TenantName: tenantID, TenantEnabled: false, Payload: []byte("test"), SHA256: "test", CreatedBy: "test", Apps: []tenant.AgentApp{{ID: appID, Name: "Assistant", Enabled: true}}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("PR12_SOURCE_DSN", sourceDSN)
	t.Setenv("PR12_TARGET_DSN", targetDSN)
	router, err := storage.NewRouter(baseDSN, control, func(ref tenant.SecretRef) (string, error) { return os.Getenv(ref.Key), nil })
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	source := tenant.BackendConfig{Type: tenant.BackendPostgres, Credential: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "PR12_SOURCE_DSN"}}
	target := tenant.BackendConfig{Type: tenant.BackendPostgres, Credential: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "PR12_TARGET_DSN"}}
	job, err := NewJob(tenantID, appID, 1, DomainMemory, source, target, "integration")
	if err != nil {
		t.Fatal(err)
	}
	store := &SQLStore{DB: control}
	job, err = store.Create(ctx, job)
	if err != nil {
		t.Fatal(err)
	}
	copier := &PostgresCopier{Router: router}
	for attempts := 0; attempts < 20; attempts++ {
		claim, ok, err := store.Claim(ctx, "worker-a", 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatal("migration was not claimable")
		}
		progress, err := copier.Step(ctx, claim, 2)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Save(ctx, claim, progress); err != nil {
			t.Fatal(err)
		}
		job, err = store.Get(ctx, tenantID, job.JobID)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status == StatusCompleted {
			break
		}
	}
	if job.Status != StatusCompleted || job.SourceRows != 5 || job.CopiedRows != 5 {
		t.Fatalf("job=%+v", job)
	}
	var count int
	if err := targetDB.QueryRowContext(ctx, `SELECT COUNT(*) FROM runtime_memories WHERE app_name=$1`, appName).Scan(&count); err != nil || count != 5 {
		t.Fatalf("target count=%d err=%v", count, err)
	}
	jobs, err := store.List(ctx, tenantID)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("jobs=%d err=%v", len(jobs), err)
	}
}

func TestRunnerSessionMigratesRedisPostgresBothWays(t *testing.T) {
	baseDSN := os.Getenv("TRPC_AGENT_POSTGRES_TEST_DSN")
	redisURL := os.Getenv("TRPC_AGENT_REDIS_TEST_URL")
	if baseDSN == "" || redisURL == "" {
		t.Skip("set PostgreSQL and Redis integration endpoints")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	control, err := sql.Open("pgx", baseDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	if err := repository.Migrate(ctx, func(ctx context.Context, script string) error {
		_, err := control.ExecContext(ctx, script)
		return err
	}, repository.DirectionUp); err != nil {
		t.Fatal(err)
	}

	suffix := fmt.Sprint(time.Now().UnixNano())
	tenantID, appID := "session-migration-"+suffix, "assistant"
	appName, _ := tenant.CanonicalAppName(tenantID, appID)
	var ledgerHashes []string
	configStore, err := repository.NewSQLStore(control)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := configStore.PublishConfig(ctx, repository.ConfigRecord{
		TenantID: tenantID, TenantName: tenantID, TenantEnabled: false,
		Payload: []byte("test"), SHA256: "test", CreatedBy: "integration",
		Apps: []tenant.AgentApp{{ID: appID, Name: "Assistant", Enabled: true}},
	}, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, hash := range ledgerHashes {
			_, _ = control.ExecContext(cleanupCtx, `DELETE FROM storage_migration_items WHERE source_route_hash=$1 AND table_name='runtime_session_snapshot'`, hash)
		}
		for _, table := range []string{"runtime_session_summaries", "runtime_session_track_events", "runtime_session_events", "runtime_session_states", "runtime_user_states", "runtime_app_states"} {
			_, _ = control.ExecContext(cleanupCtx, `DELETE FROM `+table+` WHERE app_name=$1`, appName)
		}
		_, _ = control.ExecContext(cleanupCtx, `DELETE FROM session_heads WHERE tenant_id=$1`, tenantID)
		_, _ = control.ExecContext(cleanupCtx, `DELETE FROM agent_apps WHERE tenant_id=$1`, tenantID)
		_, _ = control.ExecContext(cleanupCtx, `DELETE FROM config_versions WHERE tenant_id=$1`, tenantID)
		_, _ = control.ExecContext(cleanupCtx, `DELETE FROM tenants WHERE tenant_id=$1`, tenantID)
	})

	router, err := storage.NewRouter(baseDSN, control, func(tenant.SecretRef) (string, error) { return "", nil })
	if err != nil {
		t.Fatal(err)
	}
	defer router.Close()
	redisSource := tenant.BackendConfig{Type: tenant.BackendRedis, Endpoint: redisURL, Namespace: "migration-source-" + suffix}
	postgres := tenant.BackendConfig{Type: tenant.BackendPostgres}
	redisTarget := tenant.BackendConfig{Type: tenant.BackendRedis, Endpoint: redisURL, Namespace: "migration-target-" + suffix}
	key := session.Key{AppName: appName, UserID: "synthetic-user", SessionID: "synthetic-session"}
	if _, err := control.ExecContext(ctx, `INSERT INTO session_heads(tenant_id,app_id,user_id,session_id) VALUES($1,$2,$3,$4)`, tenantID, appID, key.UserID, key.SessionID); err != nil {
		t.Fatal(err)
	}
	source, err := router.SessionForRoute(ctx, tenantID, appID, redisSource)
	if err != nil {
		t.Fatal(err)
	}
	created, err := source.CreateSession(ctx, key, session.StateMap{"local": []byte("synthetic-state")})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.UpdateAppState(ctx, appName, session.StateMap{session.StateAppPrefix + "version": []byte("v1")}); err != nil {
		t.Fatal(err)
	}
	if err := source.UpdateUserState(ctx, session.UserKey{AppName: appName, UserID: key.UserID}, session.StateMap{session.StateUserPrefix + "locale": []byte("zh")}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	item := event.Event{Response: &model.Response{ID: "synthetic-event", Object: model.ObjectTypeChatCompletion, Choices: []model.Choice{{Message: model.NewUserMessage("synthetic-content")}}, Timestamp: now, Done: true}, InvocationID: "synthetic-invocation"}
	if err := source.AppendEvent(ctx, created, &item); err != nil {
		t.Fatal(err)
	}
	if tracks, ok := source.(session.TrackService); !ok {
		t.Fatal("Redis session backend does not implement tracks")
	} else if err := tracks.AppendTrackEvent(ctx, created, &session.TrackEvent{Track: "workflow", Payload: []byte(`{"step":1}`), Timestamp: now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	summaries := map[string]*session.Summary{"": {Summary: "synthetic-summary", Topics: []string{"migration"}, UpdatedAt: now.Add(2 * time.Second)}}
	if err := router.ImportSessionSummaries(ctx, tenantID, appID, redisSource, key, summaries); err != nil {
		t.Fatal(err)
	}
	before, err := source.GetSession(ctx, key, session.WithEventNum(100))
	if err != nil || before == nil || len(before.Events) != 1 || len(before.Summaries) != 1 {
		t.Fatalf("source fixture events/summaries are incomplete: events=%d summaries=%d err=%v", sessionEventCount(before), sessionSummaryCount(before), err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}

	copier := &PostgresCopier{Router: router}
	forward, err := NewJob(tenantID, appID, 1, DomainSession, redisSource, postgres, "integration")
	if err != nil {
		t.Fatal(err)
	}
	ledgerHashes = append(ledgerHashes, forward.SourceRouteHash)
	forward = runCopierToCompletion(t, ctx, copier, forward)
	if forward.SourceRows != 1 || forward.CopiedRows != 1 {
		t.Fatalf("forward progress source=%d copied=%d", forward.SourceRows, forward.CopiedRows)
	}
	assertSessionRouteMatches(t, ctx, router, tenantID, appID, postgres, key, forward.SourceRouteHash)

	reverse, err := NewJob(tenantID, appID, 2, DomainSession, postgres, redisTarget, "integration")
	if err != nil {
		t.Fatal(err)
	}
	ledgerHashes = append(ledgerHashes, reverse.SourceRouteHash)
	reverse = runCopierToCompletion(t, ctx, copier, reverse)
	if reverse.SourceRows != 1 || reverse.CopiedRows != 1 {
		t.Fatalf("reverse progress source=%d copied=%d", reverse.SourceRows, reverse.CopiedRows)
	}
	assertSessionRouteMatches(t, ctx, router, tenantID, appID, redisTarget, key, reverse.SourceRouteHash)
}

func sessionEventCount(value *session.Session) int {
	if value == nil {
		return 0
	}
	return len(value.Events)
}

func sessionSummaryCount(value *session.Session) int {
	if value == nil {
		return 0
	}
	return len(value.Summaries)
}

func runCopierToCompletion(t *testing.T, ctx context.Context, copier *PostgresCopier, job Job) Job {
	t.Helper()
	for attempts := 0; attempts < 10; attempts++ {
		progress, err := copier.Step(ctx, job, 1)
		if err != nil {
			if job.Target.Type == tenant.BackendPostgres {
				appName, _ := tenant.CanonicalAppName(job.TenantID, job.AppID)
				var events, summaries int
				var eligible bool
				_ = copier.Router.MigrationLedgerDB().QueryRowContext(ctx, `SELECT
					(SELECT COUNT(*) FROM runtime_session_events WHERE app_name=$1 AND deleted_at IS NULL),
					(SELECT COUNT(*) FROM runtime_session_summaries WHERE app_name=$1 AND deleted_at IS NULL),
					EXISTS(SELECT 1 FROM runtime_session_summaries su JOIN runtime_session_states st USING(app_name,user_id,session_id) WHERE su.app_name=$1 AND su.updated_at>=st.created_at)`, appName).Scan(&events, &summaries, &eligible)
				t.Fatalf("%v (target rows: events=%d summaries=%d summary_eligible=%t)", err, events, summaries, eligible)
			}
			t.Fatal(err)
		}
		job.Checkpoint, job.SourceRows, job.CopiedRows = progress.Checkpoint, progress.SourceRows, progress.CopiedRows
		if progress.Done {
			return job
		}
	}
	t.Fatal("session migration did not complete")
	return Job{}
}

func assertSessionRouteMatches(t *testing.T, ctx context.Context, router *storage.Router, tenantID, appID string, route tenant.BackendConfig, key session.Key, sourceHash string) {
	t.Helper()
	service, err := router.SessionForRoute(ctx, tenantID, appID, route)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	current, err := service.GetSession(ctx, key, session.WithEventNum(100))
	if err != nil || current == nil {
		t.Fatalf("migrated session available=%t err=%v", current != nil, err)
	}
	if len(current.Summaries) != 1 || current.Summaries[""].Summary != "synthetic-summary" {
		t.Fatal("migrated summary does not match")
	}
	var checksum string
	if err := router.MigrationLedgerDB().QueryRowContext(ctx, `SELECT checksum FROM storage_migration_items WHERE source_route_hash=$1 AND table_name='runtime_session_snapshot'`, sourceHash).Scan(&checksum); err != nil || checksum == "" {
		t.Fatalf("migration ledger checksum missing: %v", err)
	}
}

func withSearchPath(t *testing.T, raw, schema string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
