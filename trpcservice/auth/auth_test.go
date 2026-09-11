package auth

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
)

// setupAuthTest migrates a real MySQL and creates one active tenant plus one
// suspended tenant, so the tests can prove a revoked tenant's credentials
// stop working, not just a revoked user's.
//
// It reads AUTH_MYSQL_TEST_DSN, not MYSQL_TEST_DSN. These tests drop and
// recreate every table on every run, and controlplane's integration tests do
// the same to their database; `go test ./...` runs the two packages at once,
// so pointing them at one database would have one reset() drop tables out
// from under the other's migrate() — measured, not hypothetical: the failure
// looks like "Failed to open the referenced table 'tenants'" on whichever
// package loses the race, which is nothing to do with the code under test.
// Two databases, two packages, no interference.
func setupAuthTest(t *testing.T) (*controlplane.DB, string, string) {
	t.Helper()
	dsn := os.Getenv("AUTH_MYSQL_TEST_DSN")
	if dsn == "" {
		t.Skip("AUTH_MYSQL_TEST_DSN not set; skipping real-mysql auth test")
	}
	db, err := tasmysql.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	resetAuthSchema(t, db)
	if _, err := tasmysql.Migrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	ctx := context.Background()
	cdp := controlplane.NewDB(db)
	if err := cdp.CreateTenant(ctx, "acme", "Acme"); err != nil {
		t.Fatal(err)
	}
	if err := cdp.CreateTenant(ctx, "globex", "Globex"); err != nil {
		t.Fatal(err)
	}
	return cdp, "acme", "globex"
}

func resetAuthSchema(t *testing.T, db *sql.DB) {
	t.Helper()
	tables := []string{
		"tool_call_attempts", "tool_calls", "artifacts", "memory_entries", "document_chunks", "documents",
		"knowledge_bindings", "knowledge_bases", "tool_bindings",
		"channel_identities", "channel_bindings",
		"agent_revisions", "agent_apps", "backend_profiles", "model_profiles",
		"tenant_users", "principals", "tenants", "schema_migrations",
	}
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		t.Fatal(err)
	}
	for _, tbl := range tables {
		if _, err := db.Exec("DROP TABLE IF EXISTS " + tbl); err != nil {
			t.Fatalf("drop %s: %v", tbl, err)
		}
	}
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		t.Fatal(err)
	}
}

func TestResolveGrantsOnlyItsOwnTenantsRole(t *testing.T) {
	cdp, acme, globex := setupAuthTest(t)
	r := NewResolver(cdp)
	ctx := context.Background()

	acmeToken := "secret-acme-admin"
	if _, err := r.CreateBootstrapAdmin(ctx, acme, "owner@acme", acmeToken); err != nil {
		t.Fatalf("bootstrap acme admin: %v", err)
	}
	globexToken := "secret-globex-admin"
	if _, err := r.CreateBootstrapAdmin(ctx, globex, "owner@globex", globexToken); err != nil {
		t.Fatalf("bootstrap globex admin: %v", err)
	}

	actor, err := r.Resolve(ctx, "Bearer "+acmeToken)
	if err != nil {
		t.Fatalf("resolve acme: %v", err)
	}
	if actor.TenantID != acme || !actor.CanAdmin() {
		t.Fatalf("wrong actor: %+v", actor)
	}

	// A token from one tenant must never resolve into the other's scope,
	// even though both roles are "admin".
	other, err := r.Resolve(ctx, "Bearer "+globexToken)
	if err != nil {
		t.Fatalf("resolve globex: %v", err)
	}
	if other.TenantID == actor.TenantID {
		t.Fatalf("two tenants' tokens resolved to the same tenant id: %q", other.TenantID)
	}

	if _, err := r.Resolve(ctx, "Bearer wrong-token"); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("bad token: %v, want ErrUnauthenticated", err)
	}
	if _, err := r.Resolve(ctx, ""); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("missing token: %v, want ErrUnauthenticated", err)
	}
}

func TestSuspendedTenantStopsAuthenticating(t *testing.T) {
	cdp, acme, _ := setupAuthTest(t)
	r := NewResolver(cdp)
	ctx := context.Background()

	token := "still-a-valid-looking-token"
	if _, err := r.CreateBootstrapAdmin(ctx, acme, "owner@acme", token); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	if _, err := r.Resolve(ctx, "Bearer "+token); err != nil {
		t.Fatalf("resolve before suspension: %v", err)
	}

	if err := cdp.SetTenantStatus(ctx, acme, "suspended"); err != nil {
		t.Fatalf("suspend tenant: %v", err)
	}

	if _, err := r.Resolve(ctx, "Bearer "+token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("resolve after suspension: %v, want ErrUnauthenticated", err)
	}
}

func TestRequireAdminOnlyAllowsAdmin(t *testing.T) {
	user := Actor{TenantID: "acme", Role: RoleUser}
	if err := user.RequireAdmin(); !errors.Is(err, ErrForbidden) {
		t.Fatalf("plain user RequireAdmin: %v, want ErrForbidden", err)
	}
	admin := Actor{TenantID: "acme", Role: RoleAdmin}
	if err := admin.RequireAdmin(); err != nil {
		t.Fatalf("admin RequireAdmin: %v, want nil", err)
	}
}

func TestTokenIsStoredOnlyAsAHash(t *testing.T) {
	// Not a database test — this is the "no plaintext credential at rest"
	// claim made about a single function, kept honest without needing a
	// connection.
	token := "hunter2"
	hash := HashToken(token)
	if hash == token {
		t.Fatal("HashToken returned its input")
	}
	if len(hash) != 64 {
		t.Fatalf("expected a sha256 hex digest, got %d chars", len(hash))
	}
	if !TokenMatches(token, hash) {
		t.Fatal("TokenMatches rejected the token it was derived from")
	}
	if TokenMatches("hunter3", hash) {
		t.Fatal("TokenMatches accepted a different token")
	}
}
