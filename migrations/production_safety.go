package migrations

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

const (
	ProductionSafetyVersion     = "009"
	ProductionSafetyDescription = "durable fail-closed content safety decisions and lease recovery"
)

var (
	ErrProductionSafetyNotApplied       = errors.New("production safety migration is not applied")
	ErrProductionSafetyChecksumMismatch = errors.New("production safety migration checksum mismatch")
	ErrProductionSafetyVerification     = errors.New("production safety migration verification failed")
)

//go:embed 009_production_safety.sql
var productionSafetySource string

func ProductionSafetyChecksum() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(productionSafetySource)))
}

func ProductionSafetyDDL() (string, error) {
	return unwrapTransaction(productionSafetySource, "production safety")
}

func VerifyProductionSafety(ctx context.Context, q RuntimePipelineQuerier) error {
	if q == nil {
		return ErrProductionSafetyVerification
	}
	var checksum string
	err := q.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, ProductionSafetyVersion).Scan(&checksum)
	switch {
	case err == nil:
		if checksum != ProductionSafetyChecksum() {
			return fmt.Errorf("%w: version %s", ErrProductionSafetyChecksumMismatch, ProductionSafetyVersion)
		}
		return nil
	case errors.Is(err, pgx.ErrNoRows), isUndefinedMigrationLedger(err):
		return fmt.Errorf("%w: version %s", ErrProductionSafetyNotApplied, ProductionSafetyVersion)
	case ctx != nil && ctx.Err() != nil:
		return fmt.Errorf("%w: %w", ErrProductionSafetyVerification, ctx.Err())
	default:
		return ErrProductionSafetyVerification
	}
}

func ApplyProductionSafety(ctx context.Context, tx pgx.Tx) error {
	if tx == nil {
		return errors.New("production safety migration transaction is nil")
	}
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(
			hashtext(current_database()),
			hashtext(current_schema() || ':trpc_runtime_pipeline_migrations')
		)`); err != nil {
		return fmt.Errorf("lock production safety migration: %w", err)
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
	want := ProductionSafetyChecksum()
	var recorded string
	err := tx.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, ProductionSafetyVersion).Scan(&recorded)
	switch {
	case err == nil:
		if recorded != want {
			return fmt.Errorf("%w: version %s", ErrProductionSafetyChecksumMismatch, ProductionSafetyVersion)
		}
		return nil
	case !errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("read production safety migration record: %w", err)
	}
	ddl, err := ProductionSafetyDDL()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("apply production safety migration %s: %w", ProductionSafetyVersion, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, description, checksum) VALUES ($1,$2,$3)`, ProductionSafetyVersion, ProductionSafetyDescription, want); err != nil {
		return fmt.Errorf("record production safety migration %s: %w", ProductionSafetyVersion, err)
	}
	return nil
}
