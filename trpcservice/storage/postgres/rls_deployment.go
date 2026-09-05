package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	tenantctx "github.com/liuzengh/trpc-agent-service/trpcservice/storage/tenantctx"
)

// EnsureTenantRuntimeRole idempotently provisions the deployment's runtime
// application role for one schema and grants exactly the privileges the
// RLS-constrained business paths need: schema usage, DML on existing tables
// and EXECUTE on the narrow claim/resolve functions. The role is created
// LOGIN, NOSUPERUSER, NOBYPASSRLS, NOCREATEDB, NOCREATEROLE, NOINHERIT and
// never owns database objects. Deployment tooling (the P1-09 migrator
// entrypoint and test fixtures) calls this with the migration-owner
// connection; identifiers are validated Go-side and interpolated once.
func EnsureTenantRuntimeRole(ctx context.Context, admin *pgxpool.Pool, schema, roleName, password string) error {
	if admin == nil {
		return storage.ErrBackendUnavailable
	}
	if !validIdentifier(schema) || !validIdentifier(roleName) || strings.TrimSpace(password) == "" {
		return storage.ErrInvalidArgument
	}
	tx, err := admin.Begin(ctx)
	if err != nil {
		return tenantctx.ContextError(err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.Background())
		}
	}()
	if _, err = tx.Exec(ctx, fmt.Sprintf(
		`DO $role$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '%s') THEN
        CREATE ROLE %s LOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE NOBYPASSRLS;
    END IF;
END
$role$;`, roleName, roleName)); err != nil {
		return tenantctx.ContextError(err)
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf("ALTER ROLE %s WITH PASSWORD '%s'", roleName, password)); err != nil {
		return tenantctx.ContextError(err)
	}
	// Schema usage plus default privileges: tables and functions created later
	// by this migration owner (the startup migration gate or the standalone
	// migrate step) automatically expose their bounded DML/EXECUTE surface to
	// the runtime role, so provisioning order does not depend on deployment
	// scripts remembering to re-grant.
	if _, err = tx.Exec(ctx, fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s", schema, roleName)); err != nil {
		return tenantctx.ContextError(err)
	}
	for _, statement := range []string{
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %s", schema, roleName),
		fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN SCHEMA %s GRANT EXECUTE ON FUNCTIONS TO %s", schema, roleName),
	} {
		if _, err = tx.Exec(ctx, statement); err != nil {
			return tenantctx.ContextError(err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: ensure runtime role: %v", storage.ErrTransactionOutcomeUnknown, tenantctx.ContextError(err))
	}
	committed = true
	return nil
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 63 {
		return false
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' {
			continue
		}
		if r >= '0' && r <= '9' {
			continue
		}
		if r == '_' {
			continue
		}
		return false
	}
	return true
}

// GrantRuntimeSchemaPrivileges grants the runtime role its bounded DML and
// function-execution privileges after the migrations created the tables.
func GrantRuntimeSchemaPrivileges(ctx context.Context, admin *pgxpool.Pool, schema, roleName string) error {
	if admin == nil {
		return storage.ErrBackendUnavailable
	}
	if !validIdentifier(schema) || !validIdentifier(roleName) {
		return storage.ErrInvalidArgument
	}
	tx, err := admin.Begin(ctx)
	if err != nil {
		return tenantctx.ContextError(err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.Background())
		}
	}()
	for _, statement := range []string{
		fmt.Sprintf("GRANT USAGE ON SCHEMA %s TO %s", schema, roleName),
		fmt.Sprintf("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %s TO %s", schema, roleName),
		fmt.Sprintf("GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA %s TO %s", schema, roleName),
		// The migration catalog stays owner-only; the runtime role must not
		// be able to read or influence migration state.
		fmt.Sprintf("REVOKE ALL ON %s.schema_migration FROM %s", schema, roleName),
	} {
		if _, err = tx.Exec(ctx, statement); err != nil {
			return tenantctx.ContextError(err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: grant runtime privileges: %v", storage.ErrTransactionOutcomeUnknown, tenantctx.ContextError(err))
	}
	committed = true
	return nil
}
