// Package migrations exposes immutable, embedded database migrations to the
// independent migration command and read-only service startup gates. Both use
// the same checksum, so missing migrations and schema-version drift fail
// closed without giving business processes DDL privileges.
package migrations

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	RuntimePipelineVersion     = "002"
	RuntimePipelineDescription = "durable runtime queue and strict session turns"
	// LatestVersion is advanced only when a new immutable migration is appended
	// to both ApplyAll and VerifyAll.
	LatestVersion = ProductionSafetyVersion
)

var (
	// ErrRuntimePipelineNotApplied means the runtime data-plane migration has
	// not been recorded in the current PostgreSQL schema. Business processes
	// fail closed on this error; only the independent migrator may create it.
	ErrRuntimePipelineNotApplied = errors.New("runtime pipeline migration is not applied")
	// ErrRuntimePipelineChecksumMismatch means version 002 was recorded with
	// bytes different from this binary's immutable embedded migration.
	ErrRuntimePipelineChecksumMismatch = errors.New("runtime pipeline migration checksum mismatch")
	// ErrRuntimePipelineVerificationFailed is intentionally free of driver
	// details. In particular, a hostile or third-party error must never echo a
	// connection string into a service startup error.
	ErrRuntimePipelineVerificationFailed = errors.New("runtime pipeline migration verification failed")
)

// RuntimePipelineQuerier is the least-privilege surface required by a
// business process at startup. A runtime account only needs SELECT on the
// migration ledger; it does not need CREATE or ALTER privileges.
type RuntimePipelineQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

//go:embed 002_runtime_pipeline.sql
var runtimePipelineSource string

// RuntimePipelineChecksum is the SHA-256 of the complete immutable migration
// file, including its transaction wrapper.
func RuntimePipelineChecksum() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(runtimePipelineSource)))
}

// RuntimePipelineDDL returns the migration body without BEGIN/COMMIT. The
// migration runner already owns a transaction, so the wrapper must not commit
// it early.
func RuntimePipelineDDL() (string, error) {
	source := strings.TrimSpace(runtimePipelineSource)
	const begin = "BEGIN;"
	const commit = "COMMIT;"
	if !strings.HasPrefix(source, begin) || !strings.HasSuffix(source, commit) {
		return "", errors.New("runtime pipeline migration must be wrapped by BEGIN/COMMIT")
	}
	body := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(strings.TrimPrefix(source, begin)), commit))
	if body == "" {
		return "", errors.New("runtime pipeline migration body is empty")
	}
	return body, nil
}

// VerifyRuntimePipeline performs a read-only startup gate for migration 002.
// It neither creates the migration ledger nor repairs missing objects. Driver
// errors are deliberately collapsed to stable sentinels so DSNs and provider
// diagnostics cannot escape through startup logs.
func VerifyRuntimePipeline(ctx context.Context, q RuntimePipelineQuerier) error {
	if q == nil {
		return ErrRuntimePipelineVerificationFailed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var recordedChecksum string
	err := q.QueryRow(ctx,
		`SELECT checksum FROM schema_migrations WHERE version = $1`,
		RuntimePipelineVersion,
	).Scan(&recordedChecksum)
	switch {
	case err == nil:
		if recordedChecksum != RuntimePipelineChecksum() {
			return fmt.Errorf(
				"%w: version %s",
				ErrRuntimePipelineChecksumMismatch,
				RuntimePipelineVersion,
			)
		}
		return nil
	case errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("%w: version %s", ErrRuntimePipelineNotApplied, RuntimePipelineVersion)
	case isUndefinedMigrationLedger(err):
		return fmt.Errorf("%w: version %s", ErrRuntimePipelineNotApplied, RuntimePipelineVersion)
	case ctx.Err() != nil:
		return fmt.Errorf("%w: %w", ErrRuntimePipelineVerificationFailed, ctx.Err())
	default:
		return ErrRuntimePipelineVerificationFailed
	}
}

// ApplyAll is the ordered entrypoint for the independent migrator. New
// immutable migrations are appended here; callers must never reorder or
// mutate an already published migration.
func ApplyAll(ctx context.Context, tx pgx.Tx) error {
	if err := ApplyRuntimePipeline(ctx, tx); err != nil {
		return fmt.Errorf("apply migrations through %s: %w", LatestVersion, err)
	}
	if err := ApplyToolOperations(ctx, tx); err != nil {
		return fmt.Errorf("apply migrations through %s: %w", LatestVersion, err)
	}
	if err := ApplyDataMigrations(ctx, tx); err != nil {
		return fmt.Errorf("apply migrations through %s: %w", LatestVersion, err)
	}
	if err := ApplyArtifactObjects(ctx, tx); err != nil {
		return fmt.Errorf("apply migrations through %s: %w", LatestVersion, err)
	}
	if err := ApplyUsageBudget(ctx, tx); err != nil {
		return fmt.Errorf("apply migrations through %s: %w", LatestVersion, err)
	}
	if err := ApplyConfigControlPlane(ctx, tx); err != nil {
		return fmt.Errorf("apply migrations through %s: %w", LatestVersion, err)
	}
	if err := ApplyDataLifecycle(ctx, tx); err != nil {
		return fmt.Errorf("apply migrations through %s: %w", LatestVersion, err)
	}
	if err := ApplyProductionSafety(ctx, tx); err != nil {
		return fmt.Errorf("apply migrations through %s: %w", LatestVersion, err)
	}
	return nil
}

// VerifyAll is the matching read-only gate used by business processes. Keep
// its order aligned with ApplyAll as migrations are appended.
func VerifyAll(ctx context.Context, q RuntimePipelineQuerier) error {
	if err := VerifyRuntimePipeline(ctx, q); err != nil {
		return fmt.Errorf("verify migrations through %s: %w", LatestVersion, err)
	}
	if err := VerifyToolOperations(ctx, q); err != nil {
		return fmt.Errorf("verify migrations through %s: %w", LatestVersion, err)
	}
	if err := VerifyDataMigrations(ctx, q); err != nil {
		return fmt.Errorf("verify migrations through %s: %w", LatestVersion, err)
	}
	if err := VerifyArtifactObjects(ctx, q); err != nil {
		return fmt.Errorf("verify migrations through %s: %w", LatestVersion, err)
	}
	if err := VerifyUsageBudget(ctx, q); err != nil {
		return fmt.Errorf("verify migrations through %s: %w", LatestVersion, err)
	}
	if err := VerifyConfigControlPlane(ctx, q); err != nil {
		return fmt.Errorf("verify migrations through %s: %w", LatestVersion, err)
	}
	if err := VerifyDataLifecycle(ctx, q); err != nil {
		return fmt.Errorf("verify migrations through %s: %w", LatestVersion, err)
	}
	if err := VerifyProductionSafety(ctx, q); err != nil {
		return fmt.Errorf("verify migrations through %s: %w", LatestVersion, err)
	}
	return nil
}

func isUndefinedMigrationLedger(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	// undefined_table and undefined_column both mean this binary's migration
	// contract is not installed in the selected schema.
	return pgErr.Code == "42P01" || pgErr.Code == "42703"
}

// ApplyRuntimePipeline applies migration 002 in the caller's transaction.
// Queue and Session bootstrap paths share one advisory lock and one migration
// record, so concurrent first starts cannot race PostgreSQL catalogs. A changed
// checksum for an already-applied version is rejected rather than silently
// accepting schema drift.
func ApplyRuntimePipeline(ctx context.Context, tx pgx.Tx) error {
	if tx == nil {
		return errors.New("runtime pipeline migration transaction is nil")
	}
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(
			hashtext(current_database()),
			hashtext(current_schema() || ':trpc_runtime_pipeline_migrations')
		)`); err != nil {
		return fmt.Errorf("lock runtime pipeline migration: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version text PRIMARY KEY,
			description text NOT NULL,
			checksum text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
		)`); err != nil {
		return fmt.Errorf("ensure migration ledger: %w", err)
	}

	wantChecksum := RuntimePipelineChecksum()
	var recordedChecksum string
	err := tx.QueryRow(ctx,
		`SELECT checksum FROM schema_migrations WHERE version = $1`,
		RuntimePipelineVersion,
	).Scan(&recordedChecksum)
	switch {
	case err == nil:
		if recordedChecksum != wantChecksum {
			return fmt.Errorf(
				"%w: version %s",
				ErrRuntimePipelineChecksumMismatch, RuntimePipelineVersion,
			)
		}
		return nil
	case !errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("read runtime pipeline migration record: %w", err)
	}

	ddl, err := RuntimePipelineDDL()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("apply runtime pipeline migration %s: %w", RuntimePipelineVersion, err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO schema_migrations (version, description, checksum)
		VALUES ($1, $2, $3)`,
		RuntimePipelineVersion, RuntimePipelineDescription, wantChecksum,
	); err != nil {
		return fmt.Errorf("record runtime pipeline migration %s: %w", RuntimePipelineVersion, err)
	}
	return nil
}
