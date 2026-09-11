package migrations

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

const (
	ToolOperationsVersion     = "003"
	ToolOperationsDescription = "durable tool side-effect operation ledger"
)

var (
	ErrToolOperationsNotApplied       = errors.New("tool operations migration is not applied")
	ErrToolOperationsChecksumMismatch = errors.New("tool operations migration checksum mismatch")
	ErrToolOperationsVerification     = errors.New("tool operations migration verification failed")
)

//go:embed 003_tool_operations.sql
var toolOperationsSource string

func ToolOperationsChecksum() string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(toolOperationsSource)))
}

// ToolOperationsDDL removes the file-level transaction wrapper because the
// independent migrator already owns the transaction and migration ledger.
func ToolOperationsDDL() (string, error) {
	return unwrapTransaction(toolOperationsSource, "tool operations")
}

func VerifyToolOperations(ctx context.Context, q RuntimePipelineQuerier) error {
	if q == nil {
		return ErrToolOperationsVerification
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var recordedChecksum string
	err := q.QueryRow(ctx,
		`SELECT checksum FROM schema_migrations WHERE version = $1`,
		ToolOperationsVersion,
	).Scan(&recordedChecksum)
	switch {
	case err == nil:
		if recordedChecksum != ToolOperationsChecksum() {
			return fmt.Errorf("%w: version %s", ErrToolOperationsChecksumMismatch, ToolOperationsVersion)
		}
		return nil
	case errors.Is(err, pgx.ErrNoRows), isUndefinedMigrationLedger(err):
		return fmt.Errorf("%w: version %s", ErrToolOperationsNotApplied, ToolOperationsVersion)
	case ctx.Err() != nil:
		return fmt.Errorf("%w: %w", ErrToolOperationsVerification, ctx.Err())
	default:
		// Driver diagnostics can contain a DSN. Keep startup verification safe.
		return ErrToolOperationsVerification
	}
}

func ApplyToolOperations(ctx context.Context, tx pgx.Tx) error {
	if tx == nil {
		return errors.New("tool operations migration transaction is nil")
	}
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(
			hashtext(current_database()),
			hashtext(current_schema() || ':trpc_runtime_pipeline_migrations')
		)`); err != nil {
		return fmt.Errorf("lock tool operations migration: %w", err)
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

	wantChecksum := ToolOperationsChecksum()
	var recordedChecksum string
	err := tx.QueryRow(ctx,
		`SELECT checksum FROM schema_migrations WHERE version = $1`,
		ToolOperationsVersion,
	).Scan(&recordedChecksum)
	switch {
	case err == nil:
		if recordedChecksum != wantChecksum {
			return fmt.Errorf("%w: version %s", ErrToolOperationsChecksumMismatch, ToolOperationsVersion)
		}
		return nil
	case !errors.Is(err, pgx.ErrNoRows):
		return fmt.Errorf("read tool operations migration record: %w", err)
	}

	ddl, err := ToolOperationsDDL()
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("apply tool operations migration %s: %w", ToolOperationsVersion, err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO schema_migrations (version, description, checksum)
		VALUES ($1, $2, $3)`,
		ToolOperationsVersion, ToolOperationsDescription, wantChecksum,
	); err != nil {
		return fmt.Errorf("record tool operations migration %s: %w", ToolOperationsVersion, err)
	}
	return nil
}

func unwrapTransaction(source, name string) (string, error) {
	trimmed := strings.TrimSpace(source)
	const begin = "BEGIN;"
	const commit = "COMMIT;"
	if !strings.HasPrefix(trimmed, begin) || !strings.HasSuffix(trimmed, commit) {
		return "", fmt.Errorf("%s migration must be wrapped by BEGIN/COMMIT", name)
	}
	body := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(strings.TrimPrefix(trimmed, begin)), commit))
	if body == "" {
		return "", fmt.Errorf("%s migration body is empty", name)
	}
	return body, nil
}
