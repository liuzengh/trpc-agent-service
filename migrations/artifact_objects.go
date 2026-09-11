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
	ArtifactObjectsVersion     = "005"
	ArtifactObjectsDescription = "recoverable object artifact metadata ledger"
)

var (
	ErrArtifactObjectsNotApplied       = errors.New("artifact object ledger is not applied")
	ErrArtifactObjectsChecksumMismatch = errors.New("artifact object ledger checksum mismatch")
	ErrArtifactObjectsVerification     = errors.New("artifact object ledger verification failed")
)

//go:embed 005_artifact_objects.sql
var artifactObjectsSource string

func ArtifactObjectsChecksum() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(artifactObjectsSource)))
}

func ArtifactObjectsDDL() (string, error) {
	return unwrapTransaction(artifactObjectsSource, "artifact objects")
}

func VerifyArtifactObjects(ctx context.Context, q RuntimePipelineQuerier) error {
	if q == nil {
		return ErrArtifactObjectsVerification
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var checksum string
	err := q.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, ArtifactObjectsVersion).Scan(&checksum)
	switch {
	case err == nil:
		if checksum != ArtifactObjectsChecksum() {
			return fmt.Errorf("%w: version %s", ErrArtifactObjectsChecksumMismatch, ArtifactObjectsVersion)
		}
		return nil
	case errors.Is(err, pgx.ErrNoRows), isUndefinedMigrationLedger(err):
		return fmt.Errorf("%w: version %s", ErrArtifactObjectsNotApplied, ArtifactObjectsVersion)
	case ctx.Err() != nil:
		return fmt.Errorf("%w: %w", ErrArtifactObjectsVerification, ctx.Err())
	default:
		return ErrArtifactObjectsVerification
	}
}

func ApplyArtifactObjects(ctx context.Context, tx pgx.Tx) error {
	if tx == nil {
		return errors.New("artifact objects transaction is nil")
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext(current_database()), hashtext(current_schema() || ':trpc_runtime_pipeline_migrations'))`); err != nil {
		return fmt.Errorf("lock artifact objects schema: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version text PRIMARY KEY, description text NOT NULL, checksum text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
		)`); err != nil {
		return fmt.Errorf("ensure migration ledger: %w", err)
	}
	want := ArtifactObjectsChecksum()
	var recorded string
	err := tx.QueryRow(ctx, `SELECT checksum FROM schema_migrations WHERE version = $1`, ArtifactObjectsVersion).Scan(&recorded)
	switch {
	case err == nil:
		if recorded != want {
			return fmt.Errorf("%w: version %s", ErrArtifactObjectsChecksumMismatch, ArtifactObjectsVersion)
		}
		return nil
	case !errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("read artifact objects record: %w", err)
	}
	ddl, err := ArtifactObjectsDDL()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("apply artifact objects %s: %w", ArtifactObjectsVersion, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, description, checksum) VALUES ($1, $2, $3)`,
		ArtifactObjectsVersion, ArtifactObjectsDescription, want); err != nil {
		return fmt.Errorf("record artifact objects %s: %w", ArtifactObjectsVersion, err)
	}
	return nil
}
