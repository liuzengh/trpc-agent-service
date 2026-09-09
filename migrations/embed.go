// Package migrations provides the versioned, embedded database migration
// runner used by deployment entry points.
package migrations

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

// migrationLockID is a stable PostgreSQL advisory lock namespace for this
// schema. The transaction-scoped lock serializes concurrent migration Jobs.
const migrationLockID int64 = 0x747270636167656e

const webhookRouteKeyBackfillVersion = "026_webhook_route_key_backfill"
const queueInspectionIndexesVersion = "034_reliable_queue_inspection_indexes"
const tenantQueueScheduleVersion = "035_tenant_queue_schedule"
const tenantQueueAdmissionIndexVersion = "036_tenant_queue_admission_index"

//go:embed *.up.sql *.down.sql
var files embed.FS

// Applied describes migration state for operator output.
type Applied struct {
	Version   string
	AppliedAt time.Time
}

// Runner applies embedded migrations transactionally.
type Runner struct {
	db *sql.DB
}

// NewRunner creates a migration runner.
func NewRunner(db *sql.DB) *Runner { return &Runner{db: db} }

// Up applies every pending migration in lexical version order.
func (r *Runner) Up(ctx context.Context) error {
	if err := r.ensureTable(ctx); err != nil {
		return err
	}
	versions, err := embeddedVersions()
	if err != nil {
		return err
	}
	for _, version := range versions {
		script, err := files.ReadFile(version + ".up.sql")
		if err != nil {
			return fmt.Errorf("read migration %s: %w", version, err)
		}
		script = canonicalScript(script)
		checksum := scriptChecksum(script)
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", version, err)
		}
		if err := lockMigrations(ctx, tx); err != nil {
			tx.Rollback()
			return err
		}
		var storedChecksum string
		err = tx.QueryRowContext(ctx, `SELECT checksum FROM schema_migrations WHERE version=$1`, version).Scan(&storedChecksum)
		switch {
		case err == nil:
			// Retain the one-time bootstrap for installations predating checksum
			// tracking. Otherwise accept only this script's exact LF or CRLF
			// digest; different SQL or mixed historical line endings are drift.
			if storedChecksum != "" && !matchesScriptChecksum(script, storedChecksum) {
				tx.Rollback()
				return fmt.Errorf("migration %s checksum drift: database=%s embedded=%s", version, storedChecksum, checksum)
			}
			// Preserve accepted historical digests for the original release
			// binary; only new migrations and untracked rows receive LF hashes.
			if storedChecksum == "" {
				result, err := tx.ExecContext(ctx, `UPDATE schema_migrations SET checksum=$2 WHERE version=$1 AND checksum=$3`, version, checksum, storedChecksum)
				if err != nil {
					tx.Rollback()
					return fmt.Errorf("bootstrap migration %s checksum: %w", version, err)
				}
				if affected, err := result.RowsAffected(); err != nil || affected != 1 {
					tx.Rollback()
					return fmt.Errorf("migration %s checksum changed during bootstrap", version)
				}
			}
			if err := tx.Commit(); err != nil {
				return fmt.Errorf("release migration lock %s: %w", version, err)
			}
			continue
		case err != sql.ErrNoRows:
			tx.Rollback()
			return fmt.Errorf("check migration %s: %w", version, err)
		}
		if err := lockMigrationTables(ctx, tx, version); err != nil {
			tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(ctx, string(script)); err != nil {
			tx.Rollback()
			return fmt.Errorf("apply migration %s: %w", version, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version,checksum) VALUES ($1,$2)`, version, checksum); err != nil {
			tx.Rollback()
			return fmt.Errorf("record migration %s: %w", version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", version, err)
		}
	}
	return nil
}

// Down rolls back at most steps applied migrations, newest first.
func (r *Runner) Down(ctx context.Context, steps int) error {
	if steps <= 0 {
		return fmt.Errorf("migration down steps must be positive")
	}
	if err := r.ensureTable(ctx); err != nil {
		return err
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin rollback: %w", err)
	}
	defer tx.Rollback()
	if err := lockMigrations(ctx, tx); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT version,checksum FROM schema_migrations ORDER BY version DESC LIMIT $1`, steps)
	if err != nil {
		return fmt.Errorf("list rollback migrations: %w", err)
	}
	type appliedMigration struct {
		version  string
		checksum string
	}
	var versions []appliedMigration
	for rows.Next() {
		var item appliedMigration
		if err := rows.Scan(&item.version, &item.checksum); err != nil {
			rows.Close()
			return fmt.Errorf("scan rollback migration: %w", err)
		}
		versions = append(versions, item)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, item := range versions {
		upScript, err := files.ReadFile(item.version + ".up.sql")
		if err != nil {
			return fmt.Errorf("read applied migration %s: %w", item.version, err)
		}
		if !matchesScriptChecksum(upScript, item.checksum) {
			return fmt.Errorf("migration %s checksum drift blocks rollback", item.version)
		}
		script, err := files.ReadFile(item.version + ".down.sql")
		if err != nil {
			return fmt.Errorf("read rollback %s: %w", item.version, err)
		}
		if err := lockMigrationTables(ctx, tx, item.version); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(canonicalScript(script))); err != nil {
			return fmt.Errorf("apply rollback %s: %w", item.version, err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM schema_migrations WHERE version=$1`, item.version); err != nil {
			return fmt.Errorf("record rollback %s: %w", item.version, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit rollback: %w", err)
	}
	return nil
}

// Status returns applied versions in order.
func (r *Runner) Status(ctx context.Context) ([]Applied, error) {
	if err := r.ensureTable(ctx); err != nil {
		return nil, err
	}
	rows, err := r.db.QueryContext(ctx, `SELECT version, applied_at FROM schema_migrations ORDER BY version`)
	if err != nil {
		return nil, fmt.Errorf("migration status: %w", err)
	}
	defer rows.Close()
	var result []Applied
	for rows.Next() {
		var item Applied
		if err := rows.Scan(&item.Version, &item.AppliedAt); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func (r *Runner) ensureTable(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version VARCHAR(255) PRIMARY KEY,
			checksum VARCHAR(64) NOT NULL,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
		ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum VARCHAR(64);
		UPDATE schema_migrations SET checksum = '' WHERE checksum IS NULL;
		ALTER TABLE schema_migrations ALTER COLUMN checksum SET NOT NULL`)
	if err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	return nil
}

func embeddedVersions() ([]string, error) {
	entries, err := files.ReadDir(".")
	if err != nil {
		return nil, err
	}
	versions := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".up.sql") {
			continue
		}
		versions = append(versions, strings.TrimSuffix(entry.Name(), ".up.sql"))
	}
	sort.Strings(versions)
	return versions, nil
}

func scriptChecksum(script []byte) string {
	return rawScriptChecksum(canonicalScript(script))
}

// canonicalScript removes checkout-dependent CRLF sequences only. SQL content,
// whitespace, lone CR bytes and the presence of a final newline remain material.
// Execute these same bytes so the stored checksum identifies the applied SQL.
func canonicalScript(script []byte) []byte {
	return bytes.ReplaceAll(script, []byte("\r\n"), []byte("\n"))
}

func matchesScriptChecksum(script []byte, stored string) bool {
	canonical := canonicalScript(script)
	if stored == rawScriptChecksum(canonical) {
		return true
	}
	// Before canonical checksums, Windows builds hashed full CRLF checkouts.
	// Derive that one exact alternative; never trust arbitrary stored digests.
	legacy := bytes.ReplaceAll(canonical, []byte("\n"), []byte("\r\n"))
	return stored == rawScriptChecksum(legacy)
}

func rawScriptChecksum(script []byte) string {
	digest := sha256.Sum256(script)
	return hex.EncodeToString(digest[:])
}

func lockMigrations(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockID); err != nil {
		return fmt.Errorf("acquire migration advisory lock: %w", err)
	}
	return nil
}

func lockMigrationTables(ctx context.Context, tx *sql.Tx, version string) error {
	statement := migrationPreflightStatement(version)
	if statement == "" {
		return nil
	}
	if _, err := tx.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("prepare migration %s: %w", version, err)
	}
	return nil
}

func migrationPreflightStatement(version string) string {
	switch version {
	case webhookRouteKeyBackfillVersion:
		// TenantRepository.Update acquires a tenant row before replacing channel
		// rows. Taking this compatible table lock before migration 026 obtains
		// the tenant_channels DDL lock keeps both paths in the same order.
		return `LOCK TABLE tenants IN SHARE ROW EXCLUSIVE MODE`
	case queueInspectionIndexesVersion, tenantQueueScheduleVersion, tenantQueueAdmissionIndexVersion:
		// Migration 034 uses transactional CREATE INDEX. Fail quickly when a
		// production write workload holds a conflicting table lock rather than
		// waiting indefinitely. CREATE INDEX CONCURRENTLY cannot be used here:
		// Runner intentionally wraps every migration in one transaction so the
		// schema change and checksum row commit atomically.
		return `SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '30min'`
	}
	return ""
}
