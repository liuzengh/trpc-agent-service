package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	tenantctx "github.com/liuzengh/trpc-agent-service/trpcservice/storage/tenantctx"
)

// RestoreConfig is the operator input of one restore run.
type RestoreConfig struct {
	BackupDir     string
	TargetURL     string
	MigrationsDir string
	Timeout       time.Duration
	// beforeRestore is a test seam invoked immediately before the pg_restore
	// child starts (after every preflight and the empty-target check). It
	// lets the integration suite place a deterministic table lock so the
	// restore child blocks mid-COPY before cancellation. Production callers
	// leave it nil.
	beforeRestore func(ctx context.Context) error
}

// RestoreResult reports the completed restore: per-table row counts (equal
// to the manifest) and the executed audit list. No identifiers.
type RestoreResult struct {
	TableSum      int64
	Audits        []string
	PostgresMajor int
}

// RunRestore executes the restore protocol on an isolated target:
//
//  1. full backup verification (marker, manifest, checksum, TOC audit);
//  2. target preflight: privileged FORCE-RLS-exempt operator role (never the
//     runtime role), PostgreSQL major match, not the backup source;
//  3. migration gate: initialize the target with the release migrations and
//     require schema_migration to equal the manifest exactly;
//  4. catalog/table-set/schema-fingerprint match and empty-business-target
//     check;
//  5. data-only restore in ONE transaction with exit-on-error and with
//     constraints and triggers never disabled;
//  6. post-restore audits: row counts, constraint validation, FK integrity,
//     RLS/FORCE policies, definer ownership, runtime role probes.
//
// Any failure leaves the target in the "migrations applied, business tables
// empty" state (the restore transaction aborts server-side), so the drill
// can clean up and retry. No truncate/drop repair and no partial overwrite
// is ever attempted.
func RunRestore(ctx context.Context, cfg RestoreConfig, runner ClientRunner) (RestoreResult, error) {
	result := RestoreResult{}
	if cfg.TargetURL == "" {
		return result, fmt.Errorf("%w: restore requires target dsn", ErrInvalidConfig)
	}
	manifest, err := VerifyBackup(ctx, VerifyConfig{BackupDir: cfg.BackupDir, MigrationsDir: cfg.MigrationsDir}, runner)
	if err != nil {
		return result, err
	}
	params, err := parseDSN(cfg.TargetURL)
	if err != nil {
		return result, err
	}
	pool, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: cfg.TargetURL, MaxConns: 4, MinConns: 1})
	if err != nil {
		return result, fmt.Errorf("%w: target pool", ErrDependencyUnavailable)
	}
	defer pool.Close()

	// Preflight: restore is the explicit high-privilege offline step. It must
	// run as the migration/recovery owner (superuser or BYPASSRLS on the
	// isolated target) and must never run as the business runtime role.
	if err := requirePrivilegedRole(ctx, pool, "trpc_runtime"); err != nil {
		return result, err
	}
	major, err := readPostgresMajor(ctx, pool)
	if err != nil {
		return result, err
	}
	if major != manifest.Manifest.PostgresMajor {
		return result, fmt.Errorf("%w: postgres major mismatch target=%d manifest=%d", ErrForbiddenState, major, manifest.Manifest.PostgresMajor)
	}
	if sourceIdentityFingerprint(params.Host, params.Port, params.Database) == manifest.Manifest.SourceIdentity {
		return result, fmt.Errorf("%w: restore target equals the backup source", ErrForbiddenState)
	}

	// Migration gate: initialize the target with the same release migrations
	// the manifest digest was computed from, then require exact equality.
	migrator, err := pgstore.NewMigratorWithPool(pool, pgstore.PostgresConfig{URL: cfg.TargetURL}, os.DirFS(cfg.MigrationsDir))
	if err != nil {
		return result, fmt.Errorf("%w: target migrator: %v", ErrInvalidConfig, err)
	}
	if err := migrator.Up(ctx); err != nil {
		return result, restoreGateError("migration gate", err)
	}
	migrator.Close()
	if err := requireMigrationCatalogMatch(ctx, pool, manifest.Manifest); err != nil {
		return result, err
	}

	schema, err := readCatalogTables(ctx, pool)
	if err != nil {
		return result, err
	}
	if schema != manifest.Manifest.Schema {
		return result, fmt.Errorf("%w: target schema mismatch", ErrForbiddenState)
	}
	fingerprint, err := readSchemaFingerprint(ctx, pool, schema)
	if err != nil {
		return result, err
	}
	if fingerprint != manifest.Manifest.SchemaFingerprint {
		return result, fmt.Errorf("%w: target schema fingerprint mismatch", ErrForbiddenState)
	}
	// Non-empty business targets are refused unconditionally: this is the
	// same-database and partial-overwrite protection.
	empty, err := readRowCounts(ctx, pool, schema, tenantTables)
	if err != nil {
		return result, err
	}
	for _, stat := range manifest.Manifest.Tables {
		if empty[stat.Name] != 0 {
			return result, fmt.Errorf("%w: restore target business table is not empty", ErrForbiddenState)
		}
	}

	// Data-only restore in one transaction. Constraints and triggers stay
	// enabled: no --disable-triggers, no session_replication_role tricks.
	pgpassEnv, pgpassCleanup, err := writePGPassFile(params)
	if err != nil {
		return result, err
	}
	defer pgpassCleanup()
	env := libpqEnv(params, pgpassEnv)
	args := []string{
		"--data-only",
		"--exit-on-error",
		"--single-transaction",
		"--dbname=" + params.Database,
		filepath.Join(cfg.BackupDir, manifest.Manifest.Archive.File),
	}
	if cfg.beforeRestore != nil {
		if err := cfg.beforeRestore(ctx); err != nil {
			return result, err
		}
	}
	mounts := []Mount{{Host: cfg.BackupDir, Container: cfg.BackupDir, ReadOnly: true}}
	if err := runner.Run(ctx, ToolPgRestore, args, env, mounts, io.Discard); err != nil {
		return result, err
	}

	// Post-restore fact verification.
	counts, err := readRowCounts(ctx, pool, schema, tenantTables)
	if err != nil {
		return result, err
	}
	for _, stat := range manifest.Manifest.Tables {
		if counts[stat.Name] != stat.Rows {
			return result, fmt.Errorf("%w: restored row count mismatch", ErrIntegrityMismatch)
		}
		result.TableSum += counts[stat.Name]
	}
	if err := auditPostRestore(ctx, pool, schema, manifest.Manifest); err != nil {
		return result, err
	}
	if err := requireMigrationCatalogMatch(ctx, pool, manifest.Manifest); err != nil {
		return result, err
	}
	if err := auditRuntimeTransactions(ctx, pool, schema); err != nil {
		return result, err
	}
	result.Audits = []string{
		"row_counts", "constraints_validated", "foreign_keys", "rls_force_policies",
		"definer_functions", "runtime_role_probes", "migration_catalog_unchanged", "tenant_transaction_probes",
	}
	result.PostgresMajor = major
	return result, nil
}

// requireMigrationCatalogMatch compares the target schema_migration
// directory with the manifest: same versions, names, checksums; the archive
// never carried this data and must not have been able to influence it.
func requireMigrationCatalogMatch(ctx context.Context, pool pgxQuery, manifest Manifest) error {
	rows, err := pool.Query(ctx, `SELECT version, name, checksum FROM schema_migration ORDER BY version`)
	if err != nil {
		return classifyQueryError("target migration catalog", err)
	}
	defer rows.Close()
	var applied []MigrationEntry
	for rows.Next() {
		var entry MigrationEntry
		if err := rows.Scan(&entry.Version, &entry.Name, &entry.Checksum); err != nil {
			return classifyQueryError("target migration catalog scan", err)
		}
		applied = append(applied, entry)
	}
	if err := rows.Err(); err != nil {
		return classifyQueryError("target migration catalog iterate", err)
	}
	if len(applied) != len(manifest.Migrations) {
		return fmt.Errorf("%w: target migration version count mismatch", ErrForbiddenState)
	}
	for i := range applied {
		if applied[i] != manifest.Migrations[i] {
			return fmt.Errorf("%w: target migration version or checksum mismatch", ErrForbiddenState)
		}
	}
	return nil
}

// auditRuntimeTransactions replays the P2-01 probes with the runtime role
// against the restored data: the runtime role cannot read the migration
// catalog and sees zero rows without tenant context.
func auditRuntimeTransactions(ctx context.Context, pool pgxQuery, schema string) error {
	var exists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = 'trpc_runtime')`).Scan(&exists); err != nil {
		return classifyQueryError("runtime probe role", err)
	}
	if !exists {
		return nil
	}
	begin, ok := pool.(interface {
		Begin(ctx context.Context) (pgx.Tx, error)
	})
	if !ok {
		return fmt.Errorf("%w: runtime probe transaction unavailable", ErrInvalidConfig)
	}
	// Probe 1: the runtime role must NOT read the migration catalog. The
	// expected permission failure aborts the probe transaction, so each
	// probe runs in its own transaction.
	{
		tx, err := begin.Begin(ctx)
		if err != nil {
			return classifyQueryError("runtime probe tx", err)
		}
		if _, err := tx.Exec(ctx, "SET LOCAL ROLE trpc_runtime"); err != nil {
			_ = tx.Rollback(context.Background())
			return classifyQueryError("runtime probe set role", err)
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`SELECT count(*) FROM %s`, pgx.Identifier{schema, migrationCatalogTable}.Sanitize())); err == nil {
			_ = tx.Rollback(context.Background())
			return fmt.Errorf("%w: runtime role can read the migration catalog", ErrForbiddenState)
		}
		_ = tx.Rollback(context.Background())
	}
	// Probe 2: with tenant context the runtime role sees rows; without it,
	// FORCE RLS must hide everything.
	{
		tx, err := begin.Begin(ctx)
		if err != nil {
			return classifyQueryError("runtime probe tx", err)
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		if _, err := tx.Exec(ctx, "SET LOCAL ROLE trpc_runtime"); err != nil {
			return classifyQueryError("runtime probe set role", err)
		}
		if err := tenantctx.SetTenantContext(ctx, tx, "p202-probe-nonexistent-tenant"); err != nil {
			return err
		}
		var visible int
		if err := tx.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s.session`, pgx.Identifier{schema}.Sanitize())).Scan(&visible); err != nil {
			return classifyQueryError("runtime probe session read", err)
		}
		if visible != 0 {
			return fmt.Errorf("%w: runtime role sees rows without a valid tenant context", ErrForbiddenState)
		}
	}
	return nil
}

// restoreGateError maps migration-gate failures onto the restore boundary
// without leaking backend error text.
func restoreGateError(step string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, pgstore.ErrMigrationChecksum):
		return fmt.Errorf("%w: %s checksum mismatch", ErrForbiddenState, step)
	case errors.Is(err, pgstore.ErrUnknownMigrationVersion):
		return fmt.Errorf("%w: %s unknown migration version", ErrForbiddenState, step)
	case errors.Is(err, pgstore.ErrMigrationMissingVersion):
		return fmt.Errorf("%w: %s missing migration version", ErrForbiddenState, step)
	case ctxErr(err):
		return fmt.Errorf("%w: %s", ErrTimeoutOrCancelled, step)
	default:
		return fmt.Errorf("%w: %s failed", ErrRestoreFailed, step)
	}
}
