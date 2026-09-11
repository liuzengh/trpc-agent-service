package log

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cyl6/trpc-agent-service/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newAuditPostgresIntegration(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if strings.TrimSpace(dsn) == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "audit_it_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	parsed, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE")
		admin.Close()
		t.Fatal(err)
	}
	if parsed.ConnConfig.RuntimeParams == nil {
		parsed.ConnConfig.RuntimeParams = map[string]string{}
	}
	parsed.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, parsed)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE")
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA "+quoted+" CASCADE")
		admin.Close()
	})
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrations.ApplyAll(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return pool, schema
}

func TestPostgresAuditIsIdempotentHashLinkedAndRecoversFromSpool(t *testing.T) {
	pool, schema := newAuditPostgresIntegration(t)
	spool := t.TempDir()
	sink, err := NewPostgresSink(pool, spool)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	entry := Entry{
		TenantID: "tenant-a", RequestID: "request-a", SessionID: "session-a",
		Decision: "allow", Reason: "token=raw-provider-secret", AgentName: "agent",
	}
	if err := sink.Write(entry); err != nil {
		t.Fatal(err)
	}
	if err := sink.Write(entry); err != nil {
		t.Fatalf("duplicate audit write: %v", err)
	}
	entry.AuditID = StableAuditID(entry)
	conflict := entry
	conflict.Reason = "different reason"
	if err := sink.Write(conflict); !errors.Is(err, ErrAuditConflict) {
		t.Fatalf("audit conflict = %v, want ErrAuditConflict", err)
	}
	records, err := sink.ReadTenant(ctx, entry.TenantID, 0, 100)
	if err != nil || len(records) != 1 {
		t.Fatalf("audit records = %d, %v", len(records), err)
	}
	if err := sink.VerifyTenantChain(ctx, entry.TenantID); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(records[0].Payload), "raw-provider-secret") {
		t.Fatal("raw audit secret was persisted")
	}

	// Close the first pool to force the same write path used during a database
	// outage. The fsynced spool is then drained by a fresh node/pool.
	pool.Close()
	spooled := entry
	spooled.RequestID = "request-recovered"
	spooled.Reason = "password=raw-recovered-secret"
	spooled.AuditID = ""
	if err := sink.Write(spooled); err != nil {
		t.Fatal(err)
	}
	files, err := filepath.Glob(filepath.Join(spool, "*.jsonl"))
	if err != nil || len(files) != 1 {
		t.Fatalf("audit spool files = %v, %v", files, err)
	}
	info, err := os.Stat(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("audit spool mode = %o, want 600", info.Mode().Perm())
	}
	spoolBytes, err := os.ReadFile(files[0])
	if err != nil || strings.Contains(string(spoolBytes), "raw-recovered-secret") {
		t.Fatalf("spool leaked raw secret: %v", err)
	}

	parsed, err := pgxpool.ParseConfig(os.Getenv("TEST_POSTGRES_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.ConnConfig.RuntimeParams == nil {
		parsed.ConnConfig.RuntimeParams = map[string]string{}
	}
	parsed.ConnConfig.RuntimeParams["search_path"] = schema
	replacement, err := pgxpool.NewWithConfig(ctx, parsed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(replacement.Close)
	sink2, err := NewPostgresSink(replacement, spool)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink2.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	recovered, err := sink2.ReadTenant(ctx, entry.TenantID, 0, 100)
	if err != nil || len(recovered) != 2 {
		t.Fatalf("recovered audit records = %d, %v", len(recovered), err)
	}
	if err := sink2.VerifyTenantChain(ctx, entry.TenantID); err != nil {
		t.Fatal(err)
	}
	if files, err := filepath.Glob(filepath.Join(spool, "*.jsonl")); err != nil || len(files) != 0 {
		t.Fatalf("spool was not drained: files=%v err=%v", files, err)
	}
}
