// Command trpc-migrate applies the service ledger and optional configured
// backend schemas. The business service uses matching read-only verification
// gates and can run without DDL privileges.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cyl6/trpc-agent-service/migrations"
	"github.com/cyl6/trpc-agent-service/trpcservice/backend"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
)

func main() {
	dsnEnv := flag.String("dsn-env", "TRPC_AGENT_DB_DSN", "environment variable containing the PostgreSQL DSN")
	configPath := flag.String("config", "", "optional service config whose backend schemas should be prepared")
	timeout := flag.Duration("timeout", time.Minute, "overall migration timeout")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	if err := run(ctx, *dsnEnv); err != nil {
		// Never print err: connection parsers and drivers may include the DSN.
		log.Printf("trpc-migrate failed: category=%s", migrationFailureCategory(err))
		os.Exit(1)
	}
	if *configPath != "" {
		if err := migrateConfiguredBackends(ctx, *configPath); err != nil {
			log.Printf("trpc-migrate failed: category=%s", migrationFailureCategory(err))
			os.Exit(1)
		}
	}
	fmt.Printf("trpc-migrate applied and verified migrations through %s\n", migrations.LatestVersion)
}

type vectorSchemaTarget struct {
	dsnEnv    string
	table     string
	dimension int
}

func migrateConfiguredBackends(ctx context.Context, configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return errors.New("load backend migration config")
	}
	secretRoot := os.Getenv("TRPC_SECRET_ROOT")
	if secretRoot == "" {
		secretRoot = "/run/secrets"
	}
	if err := config.ConfigureSecretResolver(cfg.Secrets.Provider, secretRoot); err != nil {
		return errors.New("configure secret resolver")
	}
	targets := make(map[string]vectorSchemaTarget)
	artifactDSNEnvs := make(map[string]struct{})
	for _, tenant := range cfg.Tenants {
		if tenant.Data.Artifact.Type == "object" {
			artifactDSNEnvs[tenant.Data.Artifact.MigrationDSNEnv] = struct{}{}
		}
		knowledge := tenant.Data.Knowledge
		if knowledge.Type != "vector" {
			continue
		}
		key := knowledge.MigrationDSNEnv + "\x1f" + knowledge.Namespace
		if prior, ok := targets[key]; ok && prior.dimension != knowledge.EmbeddingDimension {
			return errors.New("vector backend table has conflicting dimensions")
		}
		targets[key] = vectorSchemaTarget{dsnEnv: knowledge.MigrationDSNEnv,
			table: knowledge.Namespace, dimension: knowledge.EmbeddingDimension}
	}
	artifactKeys := make([]string, 0, len(artifactDSNEnvs))
	for dsnEnv := range artifactDSNEnvs {
		artifactKeys = append(artifactKeys, dsnEnv)
	}
	sort.Strings(artifactKeys)
	for _, dsnEnv := range artifactKeys {
		dsn, err := config.Secret(dsnEnv)
		if err != nil {
			return errors.New("artifact metadata migration secret unavailable")
		}
		if err := migrateArtifactSchema(ctx, dsn); err != nil {
			return errors.New("migrate artifact metadata backend")
		}
	}
	keys := make([]string, 0, len(targets))
	for key := range targets {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		target := targets[key]
		if err := migrateVectorSchema(ctx, target); err != nil {
			return err
		}
	}
	return nil
}

func migrateArtifactSchema(ctx context.Context, dsn string) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return safeMigrationError(ctx, "configure artifact metadata PostgreSQL")
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return safeMigrationError(ctx, "connect to artifact metadata PostgreSQL")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return safeMigrationError(ctx, "begin artifact metadata transaction")
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := migrations.ApplyArtifactObjects(ctx, tx); err != nil {
		return safeMigrationError(ctx, "apply artifact metadata migration", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return safeMigrationError(ctx, "commit artifact metadata migration")
	}
	if err := migrations.VerifyArtifactObjects(ctx, pool); err != nil {
		return safeMigrationError(ctx, "verify artifact metadata migration", err)
	}
	return nil
}

func migrateVectorSchema(ctx context.Context, target vectorSchemaTarget) error {
	dsn, err := config.Secret(target.dsnEnv)
	if err != nil {
		return errors.New("vector backend migration secret unavailable")
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return errors.New("configure vector backend migration")
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return errors.New("connect vector backend migration")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return errors.New("begin vector backend migration")
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext(current_database()), hashtext($1))`,
		"trpc_knowledge:"+target.table); err != nil {
		return errors.New("lock vector backend migration")
	}
	if err := backend.PreparePostgresKnowledge(ctx, tx, target.table, target.dimension); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return errors.New("commit vector backend migration")
	}
	return nil
}

func run(ctx context.Context, dsnEnv string) error {
	dsn, err := config.Secret(dsnEnv)
	if err != nil {
		return err
	}
	return migrate(ctx, dsn)
}

func migrate(ctx context.Context, dsn string) error {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return safeMigrationError(ctx, "configure PostgreSQL")
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return safeMigrationError(ctx, "connect to PostgreSQL")
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return safeMigrationError(ctx, "begin transaction")
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := migrations.ApplyAll(ctx, tx); err != nil {
		return safeMigrationError(ctx, "apply migrations", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return safeMigrationError(ctx, "commit migrations")
	}
	if err := migrations.VerifyAll(ctx, pool); err != nil {
		return safeMigrationError(ctx, "verify migrations", err)
	}
	return nil
}

func safeMigrationError(ctx context.Context, stage string, causes ...error) error {
	if ctx != nil && ctx.Err() != nil {
		return fmt.Errorf("trpc-migrate: %s: %w", stage, ctx.Err())
	}
	for _, cause := range causes {
		switch {
		case errors.Is(cause, migrations.ErrRuntimePipelineChecksumMismatch):
			return fmt.Errorf("trpc-migrate: %s: %w", stage, migrations.ErrRuntimePipelineChecksumMismatch)
		case errors.Is(cause, migrations.ErrToolOperationsChecksumMismatch):
			return fmt.Errorf("trpc-migrate: %s: %w", stage, migrations.ErrToolOperationsChecksumMismatch)
		case errors.Is(cause, migrations.ErrDataMigrationsChecksumMismatch):
			return fmt.Errorf("trpc-migrate: %s: %w", stage, migrations.ErrDataMigrationsChecksumMismatch)
		case errors.Is(cause, migrations.ErrArtifactObjectsChecksumMismatch):
			return fmt.Errorf("trpc-migrate: %s: %w", stage, migrations.ErrArtifactObjectsChecksumMismatch)
		case errors.Is(cause, migrations.ErrUsageBudgetChecksumMismatch):
			return fmt.Errorf("trpc-migrate: %s: %w", stage, migrations.ErrUsageBudgetChecksumMismatch)
		case errors.Is(cause, migrations.ErrConfigControlPlaneChecksumMismatch):
			return fmt.Errorf("trpc-migrate: %s: %w", stage, migrations.ErrConfigControlPlaneChecksumMismatch)
		case errors.Is(cause, migrations.ErrDataLifecycleChecksumMismatch):
			return fmt.Errorf("trpc-migrate: %s: %w", stage, migrations.ErrDataLifecycleChecksumMismatch)
		case errors.Is(cause, migrations.ErrProductionSafetyChecksumMismatch):
			return fmt.Errorf("trpc-migrate: %s: %w", stage, migrations.ErrProductionSafetyChecksumMismatch)
		}
	}
	return fmt.Errorf("trpc-migrate: %s failed", stage)
}

func migrationFailureCategory(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, migrations.ErrRuntimePipelineChecksumMismatch),
		errors.Is(err, migrations.ErrToolOperationsChecksumMismatch),
		errors.Is(err, migrations.ErrDataMigrationsChecksumMismatch),
		errors.Is(err, migrations.ErrArtifactObjectsChecksumMismatch),
		errors.Is(err, migrations.ErrUsageBudgetChecksumMismatch),
		errors.Is(err, migrations.ErrDataLifecycleChecksumMismatch):
		return "checksum_mismatch"
	case errors.Is(err, migrations.ErrProductionSafetyChecksumMismatch):
		return "checksum_mismatch"
	default:
		return "migration_failed"
	}
}
