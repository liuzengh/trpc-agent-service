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
	UsageBudgetVersion     = "006"
	UsageBudgetDescription = "persistent model usage reservations and budget settlement"
)

var (
	ErrUsageBudgetNotApplied       = errors.New("usage budget migration is not applied")
	ErrUsageBudgetChecksumMismatch = errors.New("usage budget migration checksum mismatch")
	ErrUsageBudgetVerification     = errors.New("usage budget migration verification failed")
)

//go:embed 006_usage_budget.sql
var usageBudgetSource string

func UsageBudgetChecksum() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(usageBudgetSource)))
}

func UsageBudgetDDL() (string, error) {
	return unwrapTransaction(usageBudgetSource, "usage budget")
}

func VerifyUsageBudget(ctx context.Context, q RuntimePipelineQuerier) error {
	if q == nil {
		return ErrUsageBudgetVerification
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var checksum string
	err := q.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, UsageBudgetVersion).Scan(&checksum)
	switch {
	case err == nil:
		if checksum != UsageBudgetChecksum() {
			return fmt.Errorf("%w: version %s", ErrUsageBudgetChecksumMismatch, UsageBudgetVersion)
		}
		return nil
	case errors.Is(err, pgx.ErrNoRows), isUndefinedMigrationLedger(err):
		return fmt.Errorf("%w: version %s", ErrUsageBudgetNotApplied, UsageBudgetVersion)
	case ctx.Err() != nil:
		return fmt.Errorf("%w: %w", ErrUsageBudgetVerification, ctx.Err())
	default:
		return ErrUsageBudgetVerification
	}
}

func ApplyUsageBudget(ctx context.Context, tx pgx.Tx) error {
	if tx == nil {
		return errors.New("usage budget migration transaction is nil")
	}
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(
			hashtext(current_database()),
			hashtext(current_schema() || ':trpc_runtime_pipeline_migrations')
		)`); err != nil {
		return fmt.Errorf("lock usage budget migration: %w", err)
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
	want := UsageBudgetChecksum()
	var recorded string
	err := tx.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, UsageBudgetVersion).Scan(&recorded)
	switch {
	case err == nil:
		if recorded != want {
			return fmt.Errorf("%w: version %s", ErrUsageBudgetChecksumMismatch, UsageBudgetVersion)
		}
		return nil
	case !errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("read usage budget migration record: %w", err)
	}
	ddl, err := UsageBudgetDDL()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("apply usage budget migration %s: %w", UsageBudgetVersion, err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO schema_migrations (version, description, checksum)
		VALUES ($1, $2, $3)`, UsageBudgetVersion, UsageBudgetDescription, want); err != nil {
		return fmt.Errorf("record usage budget migration %s: %w", UsageBudgetVersion, err)
	}
	return nil
}
