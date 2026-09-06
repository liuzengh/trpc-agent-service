package wecommcp

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

// Never migrates or clears the daily service schema; all writes are contained
// in a newly generated, test-owned schema and only that schema is removed.
func TestPostgresMCPStateIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	root, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal("open test database")
	}
	t.Cleanup(func() { _ = root.Close() })
	schema := fmt.Sprintf("mcp_test_%x", time.Now().UnixNano())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := root.ExecContext(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal("create isolated schema")
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := root.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error("cleanup isolated MCP schema")
		}
	})
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal("invalid test URL")
	}
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal("open scoped connection")
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := database.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := controlplane.SeedBootstrap(ctx, db, controlplane.DefaultBootstrapData()); err != nil {
		t.Fatal(err)
	}
	store := &PostgresStore{db: db}
	if err := store.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	testStateContract(t, store)
	// Fresh Store instance observes the same durable attempt, not process memory.
	reopened := &PostgresStore{db: db}
	previous, owner, err := reopened.BeginDelivery(ctx, DeliveryKey{"tutorial-tenant", "tutorial-http-binding", "outbound-part-1"}, "input")
	if err != nil || owner || previous.Status != "sent" {
		t.Fatal("restart lost delivery state")
	}
	if _, err := store.Checkpoint(ctx, PollKey{"wrong-tenant", "tutorial-http-binding", "chat"}, "cfg", time.Now()); err == nil {
		t.Fatal("cross-tenant binding foreign key not enforced")
	}
}
