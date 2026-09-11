package bootstrap

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/migrations"
)

type databaseTarget struct {
	database, schema, role, sessionRole        string
	superuser, createDB, createRole, bypassRLS bool
	replication                                bool
	schemaOwner, schemaCreate                  bool
}

type databaseIdentity struct {
	schema, migrationRole, runtimeRole string
}

func gatewayDatabaseIdentity() databaseIdentity {
	return databaseIdentity{schema: "gateway", migrationRole: "gateway_migrator", runtimeRole: "gateway_runtime"}
}

// openDatabase preflights both deployment identities before any DDL. Only the
// runtime pool escapes; each pool keeps the role's deployment-set search_path.
func openDatabase(ctx context.Context, c Config) (*pgxpool.Pool, error) {
	return openDatabaseForTarget(ctx, c, gatewayDatabaseIdentity())
}

// An explicit internal target keeps random-schema tests separate from the fixed
// production identity; no environment variable or Config field overrides it.
func openDatabaseForTarget(ctx context.Context, c Config, expected databaseIdentity) (_ *pgxpool.Pool, err error) {
	if c.MigrationDatabaseURL == "" || c.DatabaseURL == "" {
		return nil, errors.New("Gateway runtime and migration database URLs are required")
	}
	migrationConfig, err := pgxpool.ParseConfig(c.MigrationDatabaseURL)
	if err != nil {
		return nil, errors.New("invalid Gateway migration database configuration")
	}
	runtimeConfig, err := pgxpool.ParseConfig(c.DatabaseURL)
	if err != nil {
		return nil, errors.New("invalid Gateway runtime database configuration")
	}
	if err = validateConfiguredDatabaseTargets(migrationConfig, runtimeConfig); err != nil {
		return nil, err
	}
	migration, err := pgxpool.NewWithConfig(ctx, migrationConfig)
	if err != nil {
		return nil, errors.New("invalid Gateway migration database configuration")
	}
	defer migration.Close()
	migrationTarget, err := inspectDatabase(ctx, migration)
	if err != nil {
		return nil, errors.New("Gateway migration database inspection failed")
	}
	runtime, err := pgxpool.NewWithConfig(ctx, runtimeConfig)
	if err != nil {
		return nil, errors.New("invalid Gateway runtime database configuration")
	}
	defer func() {
		if err != nil {
			runtime.Close()
		}
	}()
	runtimeTarget, err := inspectDatabase(ctx, runtime)
	if err != nil {
		return nil, errors.New("Gateway runtime database inspection failed")
	}
	if err = validateDatabaseTargets(migrationTarget, runtimeTarget, expected); err != nil {
		return nil, err
	}
	if err = migrations.ApplyForRuntime(ctx, migration, runtimeTarget.role); err != nil {
		// Driver errors can contain a connection string or server-provided details.
		return nil, errors.New("Gateway database migration failed")
	}
	var ledgerWrite bool
	err = runtime.QueryRow(ctx, `SELECT has_table_privilege(current_user,
		'gateway_schema_migrations', 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')`).Scan(&ledgerWrite)
	if err != nil || ledgerWrite {
		return nil, errors.New("Gateway runtime migration ledger privileges are invalid")
	}
	return runtime, nil
}

func inspectDatabase(ctx context.Context, pool *pgxpool.Pool) (databaseTarget, error) {
	var target databaseTarget
	err := pool.QueryRow(ctx, `SELECT current_database(), COALESCE(current_schema(), ''), current_user, session_user,
		r.rolsuper, r.rolcreatedb, r.rolcreaterole, r.rolbypassrls, r.rolreplication,
		COALESCE(n.nspowner = r.oid, false),
		COALESCE(has_schema_privilege(current_user, n.oid, 'CREATE'), false)
		FROM pg_roles r LEFT JOIN pg_namespace n ON n.nspname = current_schema()
		WHERE r.rolname = current_user`).Scan(&target.database, &target.schema, &target.role, &target.sessionRole,
		&target.superuser, &target.createDB, &target.createRole, &target.bypassRLS, &target.replication,
		&target.schemaOwner, &target.schemaCreate)
	return target, err
}

func validateConfiguredDatabaseTargets(migration, runtime *pgxpool.Config) error {
	m, r := migration.ConnConfig, runtime.ConnConfig
	if m.Host != r.Host || m.Port != r.Port || m.Database != r.Database {
		return errors.New("Gateway migration and runtime connection targets differ")
	}
	return nil
}

func validateDatabaseTargets(migration, runtime databaseTarget, expected databaseIdentity) error {
	for _, target := range []databaseTarget{migration, runtime} {
		if target.sessionRole != target.role {
			return errors.New("Gateway database connections must authenticate directly as their deployment roles")
		}
		if target.database == "" || target.role == "" || target.schema == "" ||
			target.schema == "public" || target.schema == "information_schema" ||
			strings.HasPrefix(target.schema, "pg_") {
			return errors.New("Gateway database requires an explicit business schema")
		}
	}
	if migration.database != runtime.database || migration.schema != runtime.schema {
		return errors.New("Gateway migration and runtime database targets differ")
	}
	if migration.role == runtime.role {
		return errors.New("Gateway migration and runtime roles must differ")
	}
	if migration.superuser || migration.createDB || migration.createRole || migration.bypassRLS || migration.replication ||
		!migration.schemaOwner || !migration.schemaCreate {
		return errors.New("Gateway migration role must be an unprivileged business schema owner")
	}
	if expected.schema == "" || expected.migrationRole == "" || expected.runtimeRole == "" ||
		migration.schema != expected.schema || runtime.schema != expected.schema ||
		migration.role != expected.migrationRole || runtime.role != expected.runtimeRole {
		return errors.New("Gateway database identity does not match its workload")
	}
	if runtime.superuser || runtime.createDB || runtime.createRole || runtime.bypassRLS || runtime.replication ||
		runtime.schemaOwner || runtime.schemaCreate {
		return errors.New("Gateway runtime role must have business DML privileges only")
	}
	return nil
}
