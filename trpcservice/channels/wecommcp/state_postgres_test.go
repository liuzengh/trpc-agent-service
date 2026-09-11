package wecommcp

import (
	"context"
	"database/sql"
	"encoding/json"
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
	testPostgresRecoveryAudit(t, db)
}

func testPostgresRecoveryAudit(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	b := fixtureBinding()
	b.ID = "recovery-binding"
	b.TenantID = "tutorial-tenant"
	b.AppID = "tutorial-app"
	b.CallbackKey = "recovery-route"
	b.Status = "disabled"
	b.Version = 2
	cfg, _ := ParseBinding(b)
	cfg.StartAt = now.Add(-time.Hour).Format(time.RFC3339)
	b.Config, _ = json.Marshal(cfg)
	repo, _ := controlplane.NewPostgresRepository(db)
	if err := repo.CreateChannelBinding(ctx, b); err != nil {
		t.Fatal(err)
	}
	s := &PostgresStore{db: db}
	key := PollKey{b.TenantID, b.ID, endpointHash("group-1")}
	cp, err := s.Checkpoint(ctx, key, ConfigFingerprint(b, cfg), cfg.Start())
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE FUNCTION reject_recovery_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test audit unavailable'; END $$;
CREATE TRIGGER reject_recovery BEFORE INSERT ON channel_checkpoint_recovery FOR EACH ROW EXECUTE FUNCTION reject_recovery_audit();`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecoverCheckpoint(ctx, b, key.ChatHash, cp.Version, "resume", now.Add(-time.Minute), true); err == nil {
		t.Fatal("recovery succeeded without durable audit")
	}
	unchanged, err := s.Checkpoint(ctx, key, cp.ConfigHash, cfg.Start())
	if err != nil || unchanged.Version != cp.Version || !unchanged.Through.Equal(cp.Through) {
		t.Fatal("failed recovery changed checkpoint")
	}
	if _, err := db.Exec(`DROP TRIGGER reject_recovery ON channel_checkpoint_recovery; DROP FUNCTION reject_recovery_audit();`); err != nil {
		t.Fatal(err)
	}
	next, err := s.RecoverCheckpoint(ctx, b, key.ChatHash, cp.Version, "resume", now.Add(-time.Minute), true)
	if err != nil || next.Version != cp.Version+1 {
		t.Fatal("recovery failed: ", err)
	}
	var records int
	if err := db.QueryRow(`SELECT count(*) FROM channel_checkpoint_recovery WHERE tenant_id=$1 AND channel_binding_id=$2`, b.TenantID, b.ID).Scan(&records); err != nil || records != 1 {
		t.Fatal("missing recovery audit")
	}
	if _, err := repo.UpdateChannelBinding(ctx, b.TenantID, b.ID, b.Config, "active", b.Version); err != nil {
		t.Fatal(err)
	}
	if _, err := s.RecoverCheckpoint(ctx, b, key.ChatHash, next.Version, "resume", now, true); err == nil {
		t.Fatal("stale disabled binding bypassed server recheck")
	}
}
