package tenant

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/liuzengh/trpc-agent-service/trpcservice/dbscope"
)

func TestTenantRoleRLSHidesOtherTenantRows(t *testing.T) {
	ownerDSN := os.Getenv("TEST_POSTGRES_DSN")
	if ownerDSN == "" {
		t.Skip("TEST_POSTGRES_DSN is required")
	}
	owner, err := sql.Open("pgx", ownerDSN)
	if err != nil {
		t.Fatalf("open owner database: %v", err)
	}
	defer owner.Close()
	ctx := context.Background()
	for _, tenantID := range []string{"rls-tenant-a", "rls-tenant-b"} {
		if _, err := owner.ExecContext(ctx, "INSERT INTO tenants (id) VALUES ($1) ON CONFLICT (id) DO NOTHING", tenantID); err != nil {
			t.Fatalf("seed tenant %s: %v", tenantID, err)
		}
		if _, err := owner.ExecContext(ctx, "INSERT INTO applications (tenant_id,app_code,status) VALUES ($1,'rls-check','active') ON CONFLICT (tenant_id,app_code) DO NOTHING", tenantID); err != nil {
			t.Fatalf("seed application %s: %v", tenantID, err)
		}
	}
	tx, err := dbscope.BeginTenantTransaction(ctx, owner, "rls-tenant-a")
	if err != nil {
		t.Fatalf("begin tenant transaction: %v", err)
	}
	defer tx.Rollback()
	var own, other int
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM applications WHERE tenant_id='rls-tenant-a'").Scan(&own); err != nil {
		t.Fatalf("read own tenant: %v", err)
	}
	if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM applications WHERE tenant_id='rls-tenant-b'").Scan(&other); err != nil {
		t.Fatalf("read other tenant: %v", err)
	}
	if own != 1 || other != 0 {
		t.Fatalf("RLS counts own=%d other=%d, want 1/0", own, other)
	}
}
