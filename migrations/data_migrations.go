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
	DataMigrationsVersion     = "004"
	DataMigrationsDescription = "reentrant backend data migration ledger"
)

var (
	ErrDataMigrationsNotApplied       = errors.New("data migrations ledger is not applied")
	ErrDataMigrationsChecksumMismatch = errors.New("data migrations ledger checksum mismatch")
	ErrDataMigrationsVerification     = errors.New("data migrations ledger verification failed")
)

//go:embed 004_data_migrations.sql
var dataMigrationsSource string

func DataMigrationsChecksum() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(dataMigrationsSource)))
}

func DataMigrationsDDL() (string, error) {
	return unwrapTransaction(dataMigrationsSource, "data migrations")
}

func VerifyDataMigrations(ctx context.Context, q RuntimePipelineQuerier) error {
	if q == nil {
		return ErrDataMigrationsVerification
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var checksum string
	err := q.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, DataMigrationsVersion).Scan(&checksum)
	switch {
	case err == nil:
		if checksum != DataMigrationsChecksum() {
			return fmt.Errorf("%w: version %s", ErrDataMigrationsChecksumMismatch, DataMigrationsVersion)
		}
		return nil
	case errors.Is(err, pgx.ErrNoRows), isUndefinedMigrationLedger(err):
		return fmt.Errorf("%w: version %s", ErrDataMigrationsNotApplied, DataMigrationsVersion)
	case ctx.Err() != nil:
		return fmt.Errorf("%w: %w", ErrDataMigrationsVerification, ctx.Err())
	default:
		return ErrDataMigrationsVerification
	}
}

func ApplyDataMigrations(ctx context.Context, tx pgx.Tx) error {
	if tx == nil {
		return errors.New("data migrations transaction is nil")
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext(current_database()), hashtext(current_schema() || ':trpc_runtime_pipeline_migrations'))`); err != nil {
		return fmt.Errorf("lock data migrations schema: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version text PRIMARY KEY, description text NOT NULL, checksum text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
		)`); err != nil {
		return fmt.Errorf("ensure migration ledger: %w", err)
	}
	want := DataMigrationsChecksum()
	var recorded string
	err := tx.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, DataMigrationsVersion).Scan(&recorded)
	switch {
	case err == nil:
		if recorded != want {
			return fmt.Errorf("%w: version %s", ErrDataMigrationsChecksumMismatch, DataMigrationsVersion)
		}
		return nil
	case !errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("read data migrations record: %w", err)
	}
	ddl, err := DataMigrationsDDL()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("apply data migrations %s: %w", DataMigrationsVersion, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, description, checksum) VALUES ($1, $2, $3)`,
		DataMigrationsVersion, DataMigrationsDescription, want); err != nil {
		return fmt.Errorf("record data migrations %s: %w", DataMigrationsVersion, err)
	}
	return nil
}
