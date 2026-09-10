package migrations

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// TestPostgreSQLControlPlaneMigration runs only when CI (or a developer) points
// it at a disposable, empty PostgreSQL database. Keeping the DSN separate from
// the application's POSTGRES_DSN avoids accidentally mutating a development
// database during the normal test suite.
func TestPostgreSQLControlPlaneMigration(t *testing.T) {
	dsn := os.Getenv("POSTGRES_MIGRATION_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_MIGRATION_TEST_DSN is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn := openMigrationConnection(t, ctx, dsn)

	var alreadyMigrated bool
	if err := conn.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_catalog.pg_tables
			WHERE schemaname = 'public' AND tablename = 'tenant'
		)`,
	).Scan(&alreadyMigrated); err != nil {
		t.Fatalf("check clean PostgreSQL database: %v", err)
	}
	if alreadyMigrated {
		t.Skip("migration smoke test requires an empty PostgreSQL database")
	}
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate migration test source")
	}
	migrationDir := filepath.Dir(sourceFile)
	for _, name := range []string{
		"0001_control_plane.up.sql",
		"0002_control_plane_repository_functions.up.sql",
		"0003_runtime_storage.up.sql",
		"0004_runtime_session_delete_cascade.up.sql",
		"0005_runtime_event_history.up.sql",
		"0006_audit_usage.up.sql",
		"0007_execution_audit_handoff.up.sql",
		"0008_runtime_reply_target.up.sql",
		"0009_runtime_reply_correlation.up.sql",
		"0010_agent_app_canary.up.sql",
		"0011_reply_trace_parent.up.sql",
		"0012_runtime_capabilities.up.sql",
		"0013_execution_queue.up.sql",
		"0014_wecom_aibot_channel.up.sql",
		"0015_runtime_attachments.up.sql",
		"0016_runtime_reply_media.up.sql",
		"0017_agent_chain.up.sql",
	} {
		path := filepath.Join(migrationDir, name)
		contents, err := os.ReadFile(path) // #nosec G304 -- names are fixed migration files under the repository root.
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if _, err := conn.Exec(ctx, string(contents)); err != nil {
			t.Fatalf("execute %s: %v", name, err)
		}
	}

	assertBoolean(t, ctx, conn,
		`SELECT public.jsonb_has_safe_keys('{"nested":{"bot_token":"not-a-secret"}}'::jsonb)`, false,
	)
	assertBoolean(t, ctx, conn,
		`SELECT public.jsonb_has_safe_keys('{"nested":{"public_value":"ok"}}'::jsonb)`, true,
	)
	assertBoolean(t, ctx, conn,
		`SELECT public.control_plane_endpoint_is_safe('postgres://user:password@db.example.com')`, false,
	)
	assertBoolean(t, ctx, conn,
		`SELECT public.control_plane_endpoint_is_safe('postgres://db.example.com:5432')`, true,
	)
	assertBoolean(t, ctx, conn,
		`SELECT public.control_plane_secret_ref_is_safe('secret ref')`, false,
	)

	// The admin role can execute the complete controlled writer surface, but it
	// cannot bypass it with direct table DML.
	setRole(t, ctx, conn, "tenant_admin_writer")
	if _, err := conn.Exec(ctx, `
		SELECT public.control_plane_create_tenant(
			't_01ARZ3NDEKTSV4RRFFQ69G5FAV', 'smoke', 'Smoke Tenant', 'active',
			NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, '', 90, 'basic',
			1.0::REAL, NULL::TEXT, NULL::TEXT, 1::BIGINT, clock_timestamp(), clock_timestamp()
		)`); err != nil {
		t.Fatalf("admin controlled tenant create: %v", err)
	}
	expectExecError(t, ctx, conn, `
		INSERT INTO public.tenant (tenant_id, tenant_key, display_name)
		VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAW', 'bypass', 'Bypass')
	`)
	var tenantCount int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM public.tenant WHERE tenant_key = 'smoke'`).Scan(&tenantCount); err != nil {
		t.Fatalf("admin tenant snapshot read: %v", err)
	}
	if tenantCount != 1 {
		t.Fatalf("controlled tenant create count = %d, want 1", tenantCount)
	}
	resetRole(t, ctx, conn)

	setRole(t, ctx, conn, "tenant_app_writer")
	expectExecError(t, ctx, conn, `SELECT count(*) FROM public.tenant`)
	expectExecError(t, ctx, conn, `
		SELECT public.control_plane_create_tenant(
			't_01ARZ3NDEKTSV4RRFFQ69G5FAW', 'worker', 'Worker', 'active',
			NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, NULL::BIGINT, '', 90, 'basic',
			1.0::REAL, NULL::TEXT, NULL::TEXT, 1::BIGINT, clock_timestamp(), clock_timestamp()
		)
	`)
	resetRole(t, ctx, conn)

	// Prepare a second tenant and two draft apps as the controlled migration
	// identity. The later assertions deliberately exercise cross-tenant and
	// deferred constraints through transactions.
	setRole(t, ctx, conn, "migration_owner")
	execSQL(t, ctx, conn, `
		INSERT INTO public.tenant (tenant_id, tenant_key, display_name)
		VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAW', 'other', 'Other Tenant');
		INSERT INTO public.model_profile (
			tenant_id, profile_id, profile_key, display_name, status, provider, model,
			content_digest
		) VALUES (
			't_01ARZ3NDEKTSV4RRFFQ69G5FAV', 'mp_01ARZ3NDEKTSV4RRFFQ69G5FAV',
			'primary', 'Primary', 'active', 'public', 'chat', repeat('0', 64)
		);
		INSERT INTO public.agent_app (tenant_id, app_id, app_key, display_name)
		VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAV', 'app_01ARZ3NDEKTSV4RRFFQ69G5FAV', 'primary', 'Primary');
		INSERT INTO public.agent_app (tenant_id, app_id, app_key, display_name)
		VALUES ('t_01ARZ3NDEKTSV4RRFFQ69G5FAW', 'app_01ARZ3NDEKTSV4RRFFQ69G5FAW', 'primary', 'Other');
		INSERT INTO public.agent_app_revision (
			tenant_id, app_id, revision, agent_kind, instruction, model_profile_id,
			generation_config, runtime_policy
		) VALUES (
			't_01ARZ3NDEKTSV4RRFFQ69G5FAV', 'app_01ARZ3NDEKTSV4RRFFQ69G5FAV', 1,
			'llm', 'run', 'mp_01ARZ3NDEKTSV4RRFFQ69G5FAV', '{}'::JSONB, '{}'::JSONB
		);
	`)
	resetRole(t, ctx, conn)

	setRole(t, ctx, conn, "migration_owner")
	expectExecError(t, ctx, conn, `
		INSERT INTO public.agent_app_revision (
			tenant_id, app_id, revision, agent_kind, instruction, model_profile_id,
			generation_config, runtime_policy
		) VALUES (
			't_01ARZ3NDEKTSV4RRFFQ69G5FAW', 'app_01ARZ3NDEKTSV4RRFFQ69G5FAW', 1,
			'llm', 'cross tenant', 'mp_01ARZ3NDEKTSV4RRFFQ69G5FAV', '{}'::JSONB, '{}'::JSONB
		)
	`)
	resetRole(t, ctx, conn)

	// A current pointer to a draft must fail at transaction commit, not merely
	// because the row-level FK happens to exist.
	setRole(t, ctx, conn, "migration_owner")
	expectCommitError(t, ctx, conn, `
		UPDATE public.agent_app
		SET status = 'active', current_revision = 1
		WHERE tenant_id = 't_01ARZ3NDEKTSV4RRFFQ69G5FAV'
		  AND app_id = 'app_01ARZ3NDEKTSV4RRFFQ69G5FAV'
	`)

	setRole(t, ctx, conn, "migration_owner")
	expectExecError(t, ctx, conn, `
		SELECT public.control_plane_publish_agent_app(
			't_01ARZ3NDEKTSV4RRFFQ69G5FAV',
			'app_01ARZ3NDEKTSV4RRFFQ69G5FAV',
			1, 1, 1, repeat('0', 64), clock_timestamp(), clock_timestamp(),
			'active', 2, 2, clock_timestamp(), 'draft', 'active', NULL, 2,
			'admin', 'smoke', 'mismatch', 'migration-smoke'
		)
	`)
	resetRole(t, ctx, conn)

	execSQL(t, ctx, conn, `
		UPDATE public.agent_app_revision
		SET state = 'published', content_digest = repeat('0', 64), published_at = clock_timestamp()
		WHERE tenant_id = 't_01ARZ3NDEKTSV4RRFFQ69G5FAV'
		  AND app_id = 'app_01ARZ3NDEKTSV4RRFFQ69G5FAV' AND revision = 1;
		UPDATE public.agent_app
		SET status = 'active', current_revision = 1
		WHERE tenant_id = 't_01ARZ3NDEKTSV4RRFFQ69G5FAV'
		  AND app_id = 'app_01ARZ3NDEKTSV4RRFFQ69G5FAV';
		INSERT INTO public.agent_app_revision (
			tenant_id, app_id, revision, state, agent_kind, instruction, model_profile_id,
			generation_config, runtime_policy, content_digest, published_at
		) VALUES (
			't_01ARZ3NDEKTSV4RRFFQ69G5FAV', 'app_01ARZ3NDEKTSV4RRFFQ69G5FAV', 2,
			'published', 'llm', 'run again', 'mp_01ARZ3NDEKTSV4RRFFQ69G5FAV',
			'{}'::JSONB, '{}'::JSONB, repeat('1', 64), clock_timestamp()
		);
		UPDATE public.agent_app
		SET current_revision = 2
		WHERE tenant_id = 't_01ARZ3NDEKTSV4RRFFQ69G5FAV'
		  AND app_id = 'app_01ARZ3NDEKTSV4RRFFQ69G5FAV';
	`)
	resetRole(t, ctx, conn)

	// The SECURITY DEFINER rollback writer must bind the stored pointer and
	// both event pointers to the requested published target.
	setRole(t, ctx, conn, "tenant_admin_writer")
	expectExecError(t, ctx, conn, `
		SELECT public.control_plane_rollback_agent_app(
			't_01ARZ3NDEKTSV4RRFFQ69G5FAV',
			'app_01ARZ3NDEKTSV4RRFFQ69G5FAV',
			1, 1, 2, 2, clock_timestamp(), repeat('0', 64), 2, 2,
			'active', 'active', 'admin', 'smoke', 'mismatch', 'rollback-smoke'
		)
	`)
	resetRole(t, ctx, conn)

	setRole(t, ctx, conn, "tenant_admin_writer")
	var rollbackEventID int64
	if err := conn.QueryRow(ctx, `
		SELECT public.control_plane_rollback_agent_app(
			't_01ARZ3NDEKTSV4RRFFQ69G5FAV',
			'app_01ARZ3NDEKTSV4RRFFQ69G5FAV',
			1, 1, 1, 2, clock_timestamp(), repeat('0', 64), 2, 1,
			'active', 'active', 'admin', 'smoke', 'success', 'rollback-success'
		)
	`).Scan(&rollbackEventID); err != nil {
		t.Fatalf("admin controlled rollback: %v", err)
	}
	resetRole(t, ctx, conn)

	var appCurrentRevision, appVersion int64
	if err := conn.QueryRow(ctx, `
		SELECT current_revision, version FROM public.agent_app
		WHERE tenant_id = 't_01ARZ3NDEKTSV4RRFFQ69G5FAV'
		  AND app_id = 'app_01ARZ3NDEKTSV4RRFFQ69G5FAV'
	`).Scan(&appCurrentRevision, &appVersion); err != nil {
		t.Fatalf("read rollback app result: %v", err)
	}
	if appCurrentRevision != 1 || appVersion != 2 {
		t.Fatalf("rollback app result = revision %d version %d, want revision 1 version 2", appCurrentRevision, appVersion)
	}
	var eventPreviousRevision, eventCurrentRevision int64
	if err := conn.QueryRow(ctx, `
		SELECT previous_revision, current_revision FROM public.agent_app_change_outbox WHERE event_id = $1
	`, rollbackEventID).Scan(&eventPreviousRevision, &eventCurrentRevision); err != nil {
		t.Fatalf("read rollback event result: %v", err)
	}
	if eventPreviousRevision != 2 || eventCurrentRevision != 1 {
		t.Fatalf("rollback event result = %d -> %d, want 2 -> 1", eventPreviousRevision, eventCurrentRevision)
	}

	// The deferred backend binding guard rejects an incomplete profile but
	// accepts a root and its binding inserted in one transaction.
	setRole(t, ctx, conn, "migration_owner")
	expectCommitError(t, ctx, conn, `
		INSERT INTO public.backend_profile (
			tenant_id, profile_id, profile_key, display_name, status, content_digest
		) VALUES (
			't_01ARZ3NDEKTSV4RRFFQ69G5FAV', 'bp_01ARZ3NDEKTSV4RRFFQ69G5FAV',
			'incomplete', 'Incomplete', 'active', repeat('1', 64)
		)
	`)
	execSQL(t, ctx, conn, `
		INSERT INTO public.backend_profile (
			tenant_id, profile_id, profile_key, display_name, status, content_digest
		) VALUES (
			't_01ARZ3NDEKTSV4RRFFQ69G5FAV', 'bp_01ARZ3NDEKTSV4RRFFQ69G5FAW',
			'complete', 'Complete', 'active', repeat('2', 64)
		);
		INSERT INTO public.backend_profile_binding (
			tenant_id, profile_id, capability, provider
		) VALUES (
			't_01ARZ3NDEKTSV4RRFFQ69G5FAV', 'bp_01ARZ3NDEKTSV4RRFFQ69G5FAW',
			'session', 'inmemory'
		);
	`)
	resetRole(t, ctx, conn)
}

func openMigrationConnection(t *testing.T, ctx context.Context, dsn string) *pgx.Conn {
	t.Helper()
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatalf("connect PostgreSQL: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Logf("close PostgreSQL connection: %v", err)
		}
	})
	return conn
}

func assertBoolean(t *testing.T, ctx context.Context, conn *pgx.Conn, query string, want bool) {
	t.Helper()
	var got bool
	if err := conn.QueryRow(ctx, query).Scan(&got); err != nil {
		t.Fatalf("query boolean %q: %v", query, err)
	}
	if got != want {
		t.Fatalf("query boolean %q = %v, want %v", query, got, want)
	}
}

func setRole(t *testing.T, ctx context.Context, conn *pgx.Conn, role string) {
	t.Helper()
	if _, err := conn.Exec(ctx, "SET ROLE "+role); err != nil {
		t.Fatalf("set role %s: %v", role, err)
	}
}

func resetRole(t *testing.T, ctx context.Context, conn *pgx.Conn) {
	t.Helper()
	if _, err := conn.Exec(ctx, "RESET ROLE"); err != nil {
		t.Fatalf("reset role: %v", err)
	}
}

func execSQL(t *testing.T, ctx context.Context, conn *pgx.Conn, statement string) {
	t.Helper()
	if _, err := conn.Exec(ctx, statement); err != nil {
		t.Fatalf("execute SQL: %v", err)
	}
}

func expectExecError(t *testing.T, ctx context.Context, conn *pgx.Conn, statement string) {
	t.Helper()
	if _, err := conn.Exec(ctx, statement); err == nil {
		t.Fatalf("expected SQL error for %s", statement)
	}
}

func expectCommitError(t *testing.T, ctx context.Context, conn *pgx.Conn, statement string) {
	t.Helper()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	if _, err := tx.Exec(ctx, "SET LOCAL ROLE migration_owner"); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("set local migration role: %v", err)
	}
	if _, err := tx.Exec(ctx, statement); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("expected deferred statement to reach commit: %v", err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatalf("expected deferred constraint error for %s", statement)
	}
}
