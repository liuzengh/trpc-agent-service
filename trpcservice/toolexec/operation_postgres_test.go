package toolexec

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
)

func isolatedToolDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL is not set")
	}
	root, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal("open test database")
	}
	t.Cleanup(func() { _ = root.Close() })
	schema := fmt.Sprintf("tool_test_%x", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := root.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal("create isolated test schema")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := root.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Errorf("cleanup test-owned schema %s: %v", schema, err)
		}
	})
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("parse test URL")
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	db, err := sql.Open("pgx", parsed.String())
	if err != nil {
		t.Fatal("open scoped connection")
	}
	db.SetMaxOpenConns(8)
	t.Cleanup(func() { _ = db.Close() })
	if err := database.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := controlplane.SeedBootstrap(ctx, db, controlplane.DefaultBootstrapData()); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestPostgresBusinessOperationsIntegration(t *testing.T) {
	db := isolatedToolDB(t)
	journal, _ := NewPostgresJournal(db)
	testOperationsContract(t, NewPostgresOperations(db), journal, NewWorkItems(db))
	var rows int
	if err := db.QueryRow(`SELECT count(*) FROM work_item`).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 4 {
		t.Fatalf("business rows=%d, expected one for each successful distinct scope/key", rows)
	}
}
