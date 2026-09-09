package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	tenantctx "github.com/liuzengh/trpc-agent-service/trpcservice/storage/tenantctx"
)

// Re-exports of the shared tenant-context helpers. The implementation lives
// in storage/tenantctx so that packages which import both this package and
// queue do not create import cycles in test binaries.
var (
	// ErrTenantContextRequired reports a missing or empty tenant identity.
	ErrTenantContextRequired = tenantctx.ErrTenantContextRequired
	// ErrRuntimeRolePrivileged reports a privileged runtime role.
	ErrRuntimeRolePrivileged = tenantctx.ErrRuntimeRolePrivileged
)

// SetTenantContext binds the tenant to the transaction for RLS policies.
func SetTenantContext(ctx context.Context, tx pgx.Tx, tenantID string) error {
	return tenantctx.SetTenantContext(ctx, tx, tenantID)
}

// WithTenantContext runs fn inside a transaction bound to the tenant.
func WithTenantContext(ctx context.Context, pool *pgxpool.Pool, tenantID, operation string, fn func(ctx context.Context, tx pgx.Tx) error) error {
	return tenantctx.WithTenantContext(ctx, pool, tenantID, operation, fn)
}

// EnsureRuntimeRoleLimits verifies the pooled connection runs as a
// constrained role (non-superuser, NOBYPASSRLS, non-owner).
func EnsureRuntimeRoleLimits(ctx context.Context, pool *pgxpool.Pool) error {
	return tenantctx.EnsureRuntimeRoleLimits(ctx, pool)
}
