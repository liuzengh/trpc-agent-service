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
	ConfigControlPlaneVersion     = "007"
	ConfigControlPlaneDescription = "persistent tenant configuration revisions and release control plane"
)

var (
	ErrConfigControlPlaneNotApplied       = errors.New("config control plane migration is not applied")
	ErrConfigControlPlaneChecksumMismatch = errors.New("config control plane migration checksum mismatch")
	ErrConfigControlPlaneVerification     = errors.New("config control plane migration verification failed")
)

//go:embed 007_config_control_plane.sql
var configControlPlaneSource string

func ConfigControlPlaneChecksum() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(configControlPlaneSource)))
}

func ConfigControlPlaneDDL() (string, error) {
	return unwrapTransaction(configControlPlaneSource, "config control plane")
}

func VerifyConfigControlPlane(ctx context.Context, q RuntimePipelineQuerier) error {
	if q == nil {
		return ErrConfigControlPlaneVerification
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var checksum string
	err := q.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, ConfigControlPlaneVersion).Scan(&checksum)
	switch {
	case err == nil:
		if checksum != ConfigControlPlaneChecksum() {
			return fmt.Errorf("%w: version %s", ErrConfigControlPlaneChecksumMismatch, ConfigControlPlaneVersion)
		}
		return nil
	case errors.Is(err, pgx.ErrNoRows), isUndefinedMigrationLedger(err):
		return fmt.Errorf("%w: version %s", ErrConfigControlPlaneNotApplied, ConfigControlPlaneVersion)
	case ctx.Err() != nil:
		return fmt.Errorf("%w: %w", ErrConfigControlPlaneVerification, ctx.Err())
	default:
		return ErrConfigControlPlaneVerification
	}
}

func ApplyConfigControlPlane(ctx context.Context, tx pgx.Tx) error {
	if tx == nil {
		return errors.New("config control plane migration transaction is nil")
	}
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(
			hashtext(current_database()),
			hashtext(current_schema() || ':trpc_runtime_pipeline_migrations')
		)`); err != nil {
		return fmt.Errorf("lock config control plane migration: %w", err)
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
	want := ConfigControlPlaneChecksum()
	var recorded string
	err := tx.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, ConfigControlPlaneVersion).Scan(&recorded)
	switch {
	case err == nil:
		if recorded != want {
			return fmt.Errorf("%w: version %s", ErrConfigControlPlaneChecksumMismatch, ConfigControlPlaneVersion)
		}
		return nil
	case !errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("read config control plane migration record: %w", err)
	}
	ddl, err := ConfigControlPlaneDDL()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("apply config control plane migration %s: %w", ConfigControlPlaneVersion, err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO schema_migrations (version, description, checksum)
		VALUES ($1, $2, $3)`, ConfigControlPlaneVersion, ConfigControlPlaneDescription, want); err != nil {
		return fmt.Errorf("record config control plane migration %s: %w", ConfigControlPlaneVersion, err)
	}
	return nil
}
