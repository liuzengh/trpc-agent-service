package bootstrap

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	sharedpostgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/infra/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/migrations"
)

// openDatabase checks both effective targets before DDL, closes the privileged
// pool before returning, and gives the application only its runtime connection.
func openDatabase(ctx context.Context, config Config) (*pgxpool.Pool, error) {
	return openDatabaseForSchema(ctx, config, "control")
}

// Production always supplies the fixed Control schema above. Package-local tests
// may exercise the same startup protocol in an isolated temporary schema; there
// is no process-config or environment override for this ownership binding.
func openDatabaseForSchema(ctx context.Context, config Config, schema string) (*pgxpool.Pool, error) {
	if strings.TrimSpace(config.DatabaseURL) == "" {
		return nil, errors.New("CONTROL_DATABASE_URL is required")
	}
	if strings.TrimSpace(config.MigrationDatabaseURL) == "" {
		return nil, errors.New("CONTROL_MIGRATION_DATABASE_URL is required")
	}
	if err := sharedpostgres.CheckConnectionURLs(config.MigrationDatabaseURL, config.DatabaseURL); err != nil {
		return nil, err
	}
	migrationPool, err := sharedpostgres.Open(ctx, config.MigrationDatabaseURL)
	if err != nil {
		return nil, err
	}
	defer migrationPool.Close()
	runtimePool, err := sharedpostgres.Open(ctx, config.DatabaseURL)
	if err != nil {
		return nil, err
	}
	ready := false
	defer func() {
		if !ready {
			runtimePool.Close()
		}
	}()
	migrationTarget, err := sharedpostgres.InspectTarget(ctx, migrationPool)
	if err != nil {
		return nil, err
	}
	runtimeTarget, err := sharedpostgres.InspectTarget(ctx, runtimePool)
	if err != nil {
		return nil, err
	}
	if err := checkControlDatabaseTargets(migrationTarget, runtimeTarget, schema); err != nil {
		return nil, err
	}
	if err := sharedpostgres.MigrateForRuntime(ctx, migrationPool, migrations.Files, runtimeTarget.Role); err != nil {
		// Driver errors can include SQL detail and private data; startup exposes only
		// a fixed failure class, never either connection string or driver error text.
		return nil, errors.New("migrate control database")
	}
	var canMutateLedger bool
	if err := runtimePool.QueryRow(ctx, `SELECT has_table_privilege(current_user, 'control_schema_migrations', 'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')`).Scan(&canMutateLedger); err != nil {
		return nil, errors.New("inspect runtime migration ledger privileges")
	}
	if canMutateLedger {
		return nil, errors.New("runtime postgres role must not mutate the migration ledger")
	}
	ready = true
	return runtimePool, nil
}

func checkControlDatabaseTargets(migration, runtime sharedpostgres.Target, schema string) error {
	if err := sharedpostgres.CheckTargets(migration, runtime); err != nil {
		return err
	}
	if migration.Schema != schema || runtime.Schema != schema || migration.Role != "control_migrator" || runtime.Role != "control_runtime" {
		return errors.New("control database requires its assigned schema and control_migrator/control_runtime roles")
	}
	return nil
}
