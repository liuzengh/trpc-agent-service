package postgres

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Target is the server-observed identity, not a claim parsed from a DSN.
type Target struct {
	Database       string
	Schema         string
	Role           string
	Superuser      bool
	CreateDatabase bool
	CreateRole     bool
	Replication    bool
	BypassRLS      bool
	SchemaOwner    bool
	SchemaCreate   bool
}

// InspectTarget uses the effective schema, including role-default search_path.
func InspectTarget(ctx context.Context, pool *pgxpool.Pool) (Target, error) {
	var target Target
	var sessionRole string
	if err := pool.QueryRow(ctx, `SELECT current_database(), COALESCE(current_schema(), ''), current_user, session_user,
 r.rolsuper, r.rolcreatedb, r.rolcreaterole, r.rolreplication, r.rolbypassrls,
 COALESCE(n.nspowner = r.oid, false), COALESCE(has_schema_privilege(current_user, n.oid, 'CREATE'), false)
 FROM pg_roles r LEFT JOIN pg_namespace n ON n.nspname = current_schema()
 WHERE r.rolname = current_user`).Scan(&target.Database, &target.Schema, &target.Role, &sessionRole,
		&target.Superuser, &target.CreateDatabase, &target.CreateRole, &target.Replication, &target.BypassRLS,
		&target.SchemaOwner, &target.SchemaCreate); err != nil {
		return Target{}, errors.New("inspect postgres target")
	}
	if !directRoleIdentity(sessionRole, target.Role) {
		return Target{}, errors.New("postgres connection must authenticate directly as its effective role")
	}
	if !validTarget(target) {
		return Target{}, errors.New("postgres target requires a non-system business schema")
	}
	return target, nil
}

func validTarget(target Target) bool {
	return target.Database != "" && target.Role != "" && target.Schema != "" &&
		target.Schema != "public" && target.Schema != "information_schema" && !strings.HasPrefix(target.Schema, "pg_")
}

// CheckTargets binds migrations and runtime to one database/schema but distinct
// identities. PostgreSQL grants, not search_path alone, enforce data isolation.
func CheckTargets(migration, runtime Target) error {
	if !validTarget(migration) || !validTarget(runtime) {
		return errors.New("postgres target requires a non-system business schema")
	}
	if migration.Database != runtime.Database || migration.Schema != runtime.Schema {
		return errors.New("migration and runtime postgres targets differ")
	}
	if migration.Superuser || migration.CreateDatabase || migration.CreateRole || migration.Replication || migration.BypassRLS {
		return errors.New("migration postgres role must not have global administrative privileges")
	}
	if !migration.SchemaOwner || !migration.SchemaCreate {
		return errors.New("migration postgres role must own and create in its business schema")
	}
	if runtime.Superuser || runtime.CreateDatabase || runtime.CreateRole || runtime.Replication ||
		runtime.BypassRLS || runtime.SchemaOwner || runtime.SchemaCreate {
		return errors.New("runtime postgres role must not have administrative or schema creation privileges")
	}
	if migration.Role == runtime.Role {
		return errors.New("migration and runtime postgres roles must differ")
	}
	return nil
}

// CheckConnectionURLs limits the V1 bootstrap to one explicitly configured
// PostgreSQL target. Effective database/schema/role are still checked online.
func CheckConnectionURLs(migrationURL, runtimeURL string) error {
	migration, err := pgxpool.ParseConfig(migrationURL)
	if err != nil {
		return errors.New("parse postgres configuration")
	}
	runtime, err := pgxpool.ParseConfig(runtimeURL)
	if err != nil {
		return errors.New("parse postgres configuration")
	}
	if migration.ConnConfig.Host != runtime.ConnConfig.Host || migration.ConnConfig.Port != runtime.ConnConfig.Port || migration.ConnConfig.Database != runtime.ConnConfig.Database {
		return errors.New("migration and runtime postgres connection targets differ")
	}
	return nil
}

func directRoleIdentity(sessionRole, effectiveRole string) bool {
	return sessionRole != "" && sessionRole == effectiveRole
}
