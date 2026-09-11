package dbscope

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

const tenantRole = "trpc_tenant"

// ValidatePlatformRole verifies the database-login invariant used by the
// shared pool. Platform operations intentionally use a role that can bypass
// RLS, while tenant-scoped transactions explicitly SET ROLE trpc_tenant. A
// deployment with an ordinary login role would otherwise fail indirectly as
// empty reads or policy-denied writes on FORCE RLS tables.
func ValidatePlatformRole(ctx context.Context, database *sql.DB) error {
	if database == nil {
		return fmt.Errorf("PostgreSQL database is required")
	}
	var (
		roleName         string
		superuser        bool
		bypassRLS        bool
		tenantRoleMember bool
	)
	err := database.QueryRowContext(ctx, `
SELECT current_user,
       role.rolsuper,
       role.rolbypassrls,
       pg_has_role(current_user, 'trpc_tenant', 'MEMBER')
FROM pg_roles AS role
WHERE role.rolname = current_user`).Scan(&roleName, &superuser, &bypassRLS, &tenantRoleMember)
	if err != nil {
		return fmt.Errorf("inspect PostgreSQL platform role: %w", err)
	}
	return validatePlatformRoleCapabilities(roleName, superuser, bypassRLS, tenantRoleMember)
}

func validatePlatformRoleCapabilities(roleName string, superuser, bypassRLS, tenantRoleMember bool) error {
	if !superuser && !bypassRLS {
		return fmt.Errorf("PostgreSQL platform role %q must have BYPASSRLS (or be a superuser) for platform-scoped storage", roleName)
	}
	if !tenantRoleMember {
		return fmt.Errorf("PostgreSQL platform role %q must be a member of %s", roleName, tenantRole)
	}
	return nil
}

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
	if err := ScopeTenantTransaction(ctx, tx, tenantID); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return tx, nil
}

// ScopeTenantTransaction applies the non-privileged tenant role and tenant
// context to an existing transaction. Framework-owned PostgreSQL adapters use
// this when the framework controls transaction creation itself.
func ScopeTenantTransaction(ctx context.Context, tx *sql.Tx, tenantID string) error {
	if tx == nil {
		return fmt.Errorf("PostgreSQL transaction is required")
	}
	tenantID = strings.TrimSpace(tenantID)
	if tenantID == "" {
		return fmt.Errorf("tenant ID is required")
	}
	if _, err := tx.ExecContext(ctx, "SET LOCAL ROLE "+tenantRole); err != nil {
		return fmt.Errorf("set tenant PostgreSQL role: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "SELECT set_config('app.tenant_id', $1, true)", tenantID); err != nil {
		return fmt.Errorf("set tenant context: %w", err)
	}
	return nil
}
