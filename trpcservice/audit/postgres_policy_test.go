package audit

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
)

func TestPostgresAuditPolicyIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	root, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal("open test DB")
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(root)
	schema := fmt.Sprintf("audit_test_%x", time.Now().UnixNano())
	if _, err := root.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal("create isolated audit schema")
	}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := root.ExecContext(cleanup, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error("cleanup isolated audit schema")
		}
	}()
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("invalid test DSN")
	}
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal("open scoped DB")
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(db)
	if err := database.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := controlplane.SeedBootstrap(ctx, db, controlplane.DefaultBootstrapData()); err != nil {
		t.Fatal(err)
	}
	w, _ := NewPostgresWriter(db)
	event := Event{ID: "audit-fixed", TenantID: "tutorial-tenant", Decision: "run_completed", OccurredAt: time.Now().UTC().Add(-48 * time.Hour)}
	if err := w.Record(ctx, event); err != nil {
		t.Fatal(err)
	}
	if err := w.Record(ctx, event); err != nil {
		t.Fatal("idempotent audit replay failed")
	}
	changed := event
	changed.Decision = "different"
	if err := w.Record(ctx, changed); !errors.Is(err, ErrEventConflict) {
		t.Fatal("audit identity conflict not rejected")
	}
	if n, err := w.Prune(ctx, event.TenantID, time.Now(), 100); err != nil || n != 0 {
		t.Fatal("default retention deleted records")
	}
	if _, err := db.ExecContext(ctx, `UPDATE tenant SET audit_policy='{"retention_days":1}' WHERE tenant_id='tutorial-tenant'`); err != nil {
		t.Fatal(err)
	}
	// The definer function must not resolve the caller's pg_temp.audit_log.
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(conn)
	if _, err := conn.ExecContext(ctx, "CREATE TEMP TABLE audit_log (LIKE "+schema+".audit_log INCLUDING ALL)"); err != nil {
		t.Fatal(err)
	}
	newEvent := sanitizeEvent(Event{TenantID: event.TenantID, Decision: "safe"})
	encoded, _ := json.Marshal(newEvent)
	var accepted bool
	if err := conn.QueryRowContext(ctx, "SELECT platform_audit_append($1::jsonb)", string(encoded)).Scan(&accepted); err != nil || !accepted {
		t.Fatal("safe append failed")
	}
	var temporaryCount int
	if err := conn.QueryRowContext(ctx, "SELECT count(*) FROM pg_temp.audit_log").Scan(&temporaryCount); err != nil || temporaryCount != 0 {
		t.Fatal("audit function used attacker temporary table")
	}
	if n, err := w.Prune(ctx, event.TenantID, time.Now(), 100); err != nil || n != 1 {
		t.Fatalf("retention n=%d err=%v", n, err)
	}
	var receipt int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM "+schema+".audit_log WHERE decision='audit_retention_pruned'").Scan(&receipt); err != nil || receipt != 1 {
		t.Fatal("retention receipt missing")
	}
}
