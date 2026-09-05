// Package tenantctx binds a tenant identity to a PostgreSQL transaction via
// the transaction-local custom GUC that the P2-01 RLS policies compare
// against. The value is set with set_config(..., true), so commit, rollback,
// cancellation and panic all clear it before the connection is reused. It is
// caller-settable by any database client and therefore an isolation
// boundary, never an authentication mechanism.
package tenantctx

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// GUC is the transaction-local custom GUC key used by the RLS policies.
const GUC = "trpc.tenant_id"

// ErrTenantContextRequired reports a missing or empty tenant identity.
var ErrTenantContextRequired = errors.New("postgres: tenant context required")

// ErrRuntimeRolePrivileged reports a runtime role that must not serve
// business traffic: superuser, BYPASSRLS or owner of a protected table.
var ErrRuntimeRolePrivileged = errors.New("postgres: runtime role is privileged")

func ContextError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return storage.ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "42501":
			return storage.ErrTenantMismatch
		case "23505":
			return storage.ErrConflict
		case "23514", "23503":
			return storage.ErrInvalidArgument
		}
	}
	return storage.ErrBackendUnavailable
}

// SetTenantContext binds the tenant to the transaction for RLS policies.
// It must be called before the first business statement of the transaction.
func SetTenantContext(ctx context.Context, tx pgx.Tx, tenantID string) error {
	if strings.TrimSpace(tenantID) == "" {
		return ErrTenantContextRequired
	}
	if _, err := tx.Exec(ctx, "SELECT set_config('"+GUC+"', $1, true)", tenantID); err != nil {
		return ContextError(err)
	}
	return nil
}

// WithTenantContext runs fn inside a transaction whose tenant GUC is set
// before fn executes. The transaction is rolled back on error, panic and
// context cancellation; a commit failure is reported as an unknown
// transaction outcome so callers never mistake it for success.
func WithTenantContext(ctx context.Context, pool *pgxpool.Pool, tenantID, operation string, fn func(ctx context.Context, tx pgx.Tx) error) error {
	if pool == nil {
		return storage.ErrBackendUnavailable
	}
	if strings.TrimSpace(tenantID) == "" {
		return ErrTenantContextRequired
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return ContextError(err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.Background())
		}
	}()
	if err := SetTenantContext(ctx, tx, tenantID); err != nil {
		return err
	}
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: %s: %v", storage.ErrTransactionOutcomeUnknown, operation, ContextError(err))
	}
	committed = true
	return nil
}

// EnsureRuntimeRoleLimits verifies that the pooled connection runs as a role
// that RLS actually constrains: not a superuser, not BYPASSRLS, and not the
// owner of any protected table in the search-path schema.
func EnsureRuntimeRoleLimits(ctx context.Context, pool *pgxpool.Pool) error {
	if pool == nil {
		return storage.ErrBackendUnavailable
	}
	var roleSuper, roleBypass bool
	var roleOwnerOf string
	err := pool.QueryRow(ctx, `
SELECT r.rolsuper, r.rolbypassrls,
       COALESCE((SELECT t.tablename
                 FROM pg_tables t
                 WHERE t.schemaname = current_schema()
                   AND t.tablename IN ('tenant', 'job_queue', 'outbox_message', 'execution_result')
                   AND t.tableowner = current_user
                 LIMIT 1), '')
FROM pg_roles r
WHERE r.rolname = current_user`).Scan(&roleSuper, &roleBypass, &roleOwnerOf)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrRuntimeRolePrivileged
	}
	if err != nil {
		return ContextError(err)
	}
	if roleSuper || roleBypass || roleOwnerOf != "" {
		return fmt.Errorf("%w: super=%t bypassrls=%t owner_of=%q", ErrRuntimeRolePrivileged, roleSuper, roleBypass, roleOwnerOf)
	}
	return nil
}
