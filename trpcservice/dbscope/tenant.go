package dbscope

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const tenantRole = "trpc_tenant"

// BeginTenantTransaction starts a transaction that executes with the database
// tenant role and one explicit tenant context. The shared application pool
// remains connected with the platform role for cross-tenant control-plane
// operations; tenant-scoped callers must enter through this boundary so RLS
// still applies without a second database connection.
func BeginTenantTransaction(ctx context.Context, database *sql.DB, tenantID string) (*sql.Tx, error) {
	if database == nil {
		return nil, fmt.Errorf("PostgreSQL database is required")
	}
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("tenant ID is required")
	}

	tx, err := database.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "SET LOCAL ROLE "+tenantRole); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("set tenant PostgreSQL role: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "SELECT set_config('app.tenant_id', $1, true)", tenantID); err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("set tenant context: %w", err)
	}
	return tx, nil
}
