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
	DataLifecycleVersion     = "008"
	DataLifecycleDescription = "durable summaries, visibility watermarks, audit recovery, and migration reconciliation"
)

var (
	ErrDataLifecycleNotApplied       = errors.New("data lifecycle migration is not applied")
	ErrDataLifecycleChecksumMismatch = errors.New("data lifecycle migration checksum mismatch")
	ErrDataLifecycleVerification     = errors.New("data lifecycle migration verification failed")
)

//go:embed 008_data_lifecycle.sql
var dataLifecycleSource string

func DataLifecycleChecksum() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(dataLifecycleSource)))
}

func DataLifecycleDDL() (string, error) {
	return unwrapTransaction(dataLifecycleSource, "data lifecycle")
}

func VerifyDataLifecycle(ctx context.Context, q RuntimePipelineQuerier) error {
	if q == nil {
		return ErrDataLifecycleVerification
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var checksum string
	err := q.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, DataLifecycleVersion).Scan(&checksum)
	switch {
	case err == nil:
		if checksum != DataLifecycleChecksum() {
			return fmt.Errorf("%w: version %s", ErrDataLifecycleChecksumMismatch, DataLifecycleVersion)
		}
		return nil
	case errors.Is(err, pgx.ErrNoRows), isUndefinedMigrationLedger(err):
		return fmt.Errorf("%w: version %s", ErrDataLifecycleNotApplied, DataLifecycleVersion)
	case ctx.Err() != nil:
		return fmt.Errorf("%w: %w", ErrDataLifecycleVerification, ctx.Err())
	default:
		return ErrDataLifecycleVerification
	}
}

func ApplyDataLifecycle(ctx context.Context, tx pgx.Tx) error {
	if tx == nil {
		return errors.New("data lifecycle migration transaction is nil")
	}
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(
			hashtext(current_database()),
			hashtext(current_schema() || ':trpc_runtime_pipeline_migrations')
		)`); err != nil {
		return fmt.Errorf("lock data lifecycle migration: %w", err)
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
	want := DataLifecycleChecksum()
	var recorded string
	err := tx.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, DataLifecycleVersion).Scan(&recorded)
	switch {
	case err == nil:
		if recorded != want {
			return fmt.Errorf("%w: version %s", ErrDataLifecycleChecksumMismatch, DataLifecycleVersion)
		}
		return nil
	case !errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("read data lifecycle migration record: %w", err)
	}
	ddl, err := DataLifecycleDDL()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("apply data lifecycle migration %s: %w", DataLifecycleVersion, err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO schema_migrations (version, description, checksum)
		VALUES ($1, $2, $3)`, DataLifecycleVersion, DataLifecycleDescription, want); err != nil {
		return fmt.Errorf("record data lifecycle migration %s: %w", DataLifecycleVersion, err)
	}
	return nil
}
