package recovery_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
)

func TestIsolatedAdminCatalog(t *testing.T) {
	if os.Getenv("TEST_RECOVERY_DOCKER") != "1" {
		t.Skip("isolated Docker checks disabled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	_, addr := isolatedPostgres(t, ctx)
	db, err := sql.Open("pgx", "postgres://drill@"+addr+"/source?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	for db.PingContext(ctx) != nil {
		select {
		case <-ctx.Done():
			t.Fatal("isolated database not ready")
		case <-time.After(100 * time.Millisecond):
		}
	}
	if err = database.Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	r, err := controlplane.NewPostgresRepository(db)
	if err != nil {
		t.Fatal(err)
	}
	data := controlplane.DefaultBootstrapData()
	if err = controlplane.SeedBootstrap(ctx, db, data); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"tenants", "apps", "revisions", "channels", "backends"} {
		page, err := r.ListCatalog(ctx, controlplane.CatalogQuery{Kind: kind, TenantIDs: []string{"tutorial-tenant"}, Limit: 1})
		if err != nil || len(page.Items) != 1 {
			t.Fatalf("SQL catalog %s: %v", kind, err)
		}
		var item map[string]any
		if json.Unmarshal(page.Items[0], &item) != nil || item["tenant_id"] != "tutorial-tenant" {
			t.Fatal("SQL catalog lost tenant scope")
		}
		empty, err := r.ListCatalog(ctx, controlplane.CatalogQuery{Kind: kind, TenantIDs: []string{"other-tenant"}, Limit: 1})
		if err != nil || len(empty.Items) != 0 {
			t.Fatal("SQL catalog cross tenant leak", err)
		}
	}
	all, err := r.ListCatalog(ctx, controlplane.CatalogQuery{Kind: "tenants", AllTenants: true, Limit: 100})
	if err != nil || len(all.Items) != 1 {
		t.Fatal("superadmin catalog", err)
	}
}
