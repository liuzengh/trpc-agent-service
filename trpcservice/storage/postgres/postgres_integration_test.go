package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5"
)

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
	migrator, err := NewMigratorWithPool(pool, cfg, source)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		if err := migrator.Down(cleanupCtx, 1); err != nil {
			t.Logf("test database cleanup failed: %v", err)
		}
	}()

	if err := migrator.Up(ctx); err != nil {
		t.Fatal(err)
	}
	current, err := migrator.Current(ctx)
	if err != nil || current.Version != 1 {
		t.Fatalf("unexpected current migration: %+v, %v", current, err)
	}
	if err := migrator.Up(ctx); err != nil {
		t.Fatalf("repeat Up failed: %v", err)
	}

	checksumSource := fstest.MapFS{
		"000001_changed.up.sql":   {Data: []byte("SELECT 1")},
		"000001_changed.down.sql": {Data: []byte("SELECT 1")},
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

	failedSource := fstest.MapFS{
		"000002_broken.up.sql":   {Data: []byte("CREATE TABLE migration_failure_probe (id integer); SELECT * FROM missing_migration_table;")},
		"000002_broken.down.sql": {Data: []byte("DROP TABLE IF EXISTS migration_failure_probe;")},
	}
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
		SELECT 1 FROM schema_migration WHERE version = 2
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
		"tenant_config_version",
	}).Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 15 {
		t.Fatalf("expected 15 business tables, got %d", tableCount)
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

	if err := migrator.Down(ctx, 1); err != nil {
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
