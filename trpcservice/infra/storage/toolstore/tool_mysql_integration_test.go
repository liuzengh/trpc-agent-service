//go:build integration

package toolstore

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go/modules/mysql"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage"
)

// TestMySQLToolGrantsCarryTheirTenant exercises the SQL the run-time RBAC check
// depends on: the owning tenant is stored with the grant, a foreign tenant does
// not inherit it, and a legacy row (empty tenant) stays valid for everyone so an
// upgrade does not silently revoke tools.
func TestMySQLToolGrantsCarryTheirTenant(t *testing.T) {
	ctx := context.Background()
	c, err := mysql.Run(ctx, "mysql:8.0",
		mysql.WithUsername("test"), mysql.WithPassword("test"), mysql.WithDatabase("test"),
		mysql.WithScripts("../../../../deployments/mysql/init/004_tools.sql"))
	if err != nil {
		t.Fatalf("mysql run: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })

	dsn, err := c.ConnectionString(ctx, "parseTime=true")
	if err != nil {
		t.Fatalf("dsn: %v", err)
	}
	db, err := storage.OpenMySQL(dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	reg := NewMySQLRegistry(db)
	if err := reg.Grant(ctx, "acme", "agent-1", "echo"); err != nil {
		t.Fatalf("grant: %v", err)
	}

	allowed, err := reg.IsAllowedForTenant(ctx, "acme", "agent-1", "echo")
	if err != nil || !allowed {
		t.Fatalf("owning tenant: allowed=%v err=%v, want true", allowed, err)
	}
	if allowed, err := reg.IsAllowedForTenant(ctx, "globex", "agent-1", "echo"); err != nil || allowed {
		t.Errorf("foreign tenant: allowed=%v err=%v, want false", allowed, err)
	}
	if allowed, err := reg.IsAllowed(ctx, "agent-1", "echo"); err != nil || !allowed {
		t.Errorf("tenant-agnostic check: allowed=%v err=%v, want true", allowed, err)
	}

	// A grant written before the column existed must keep authorising.
	if _, err := db.ExecContext(ctx,
		`INSERT INTO agent_tool_grants (agent_id, tool_id) VALUES ('agent-legacy', 'echo')`); err != nil {
		t.Fatalf("insert legacy grant: %v", err)
	}
	for _, tenant := range []string{"acme", "globex"} {
		if allowed, err := reg.IsAllowedForTenant(ctx, tenant, "agent-legacy", "echo"); err != nil || !allowed {
			t.Errorf("legacy grant for %s: allowed=%v err=%v, want true", tenant, allowed, err)
		}
	}

	// Re-granting under another tenant moves ownership; revoking clears it.
	if err := reg.Grant(ctx, "globex", "agent-1", "echo"); err != nil {
		t.Fatalf("re-grant: %v", err)
	}
	if allowed, _ := reg.IsAllowedForTenant(ctx, "acme", "agent-1", "echo"); allowed {
		t.Error("re-granting under globex must move ownership away from acme")
	}
	if err := reg.Revoke(ctx, "agent-1", "echo"); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if allowed, _ := reg.IsAllowedForTenant(ctx, "globex", "agent-1", "echo"); allowed {
		t.Error("revoke must clear the grant for every tenant")
	}
}
