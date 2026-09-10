// Package migrations embeds and applies the service database migrations.
package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Files is the ordered, immutable migration set owned by bootstrap.
//
//go:embed 0*_*.sql
var Files embed.FS

var (
	// ErrInvalidHistory reports a migration history that cannot be trusted.
	ErrInvalidHistory = errors.New("invalid migration history")
	// ErrMigration reports failure while applying a migration.
	ErrMigration = errors.New("migration failed")
)

const lockKey = "trpc-agent-service/control-plane-migrations"

type file struct {
	version           int
	name, digest, sql string
}

func orderedFiles() ([]file, error) {
	entries, err := fs.Glob(Files, "0*_*.sql")
	if err != nil {
		return nil, fmt.Errorf("%w: list files", ErrMigration)
	}
	files := make([]file, 0, len(entries))
	for _, name := range entries {
		base := filepath.Base(name)
		parts := strings.SplitN(base, "_", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("%w: invalid file %s", ErrInvalidHistory, base)
		}
		version, err := strconv.Atoi(parts[0])
		if err != nil || version < 1 {
			return nil, fmt.Errorf("%w: invalid version %s", ErrInvalidHistory, base)
		}
		contents, err := fs.ReadFile(Files, name)
		if err != nil {
			return nil, fmt.Errorf("%w: read %s", ErrMigration, base)
		}
		digest := fmt.Sprintf("%x", sha256.Sum256(contents))
		files = append(files, file{version: version, name: base, digest: digest, sql: string(contents)})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].version < files[j].version })
	for i := 1; i < len(files); i++ {
		if files[i-1].version == files[i].version {
			return nil, fmt.Errorf("%w: duplicate version %d", ErrInvalidHistory, files[i].version)
		}
	}
	return files, nil
}

// Apply runs every missing migration while holding a database-wide advisory
// lock. A digest mismatch or history gap fails closed.
func Apply(ctx context.Context, db *sql.DB) error {
	if ctx == nil || db == nil {
		return ErrMigration
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	files, err := orderedFiles()
	if err != nil {
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return ErrMigration
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock(hashtext($1))", lockKey); err != nil {
		return ErrMigration
	}
	defer func() {
		_, _ = conn.ExecContext(context.Background(), "SELECT pg_advisory_unlock(hashtext($1))", lockKey)
	}()
	if err := ensureHistory(ctx, conn); err != nil {
		return err
	}
	history, err := readHistory(ctx, conn)
	if err != nil {
		return err
	}
	if err := validateHistory(history, files); err != nil {
		return err
	}
	for _, migration := range files {
		applied, ok := history[migration.version]
		if ok {
			if applied != migration.digest {
				return fmt.Errorf("%w: version %d digest mismatch", ErrInvalidHistory, migration.version)
			}
			continue
		}
		if migration.version != nextVersion(history) {
			return fmt.Errorf("%w: version %d is not the next migration", ErrInvalidHistory, migration.version)
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("%w: begin %s", ErrMigration, migration.name)
		}
		if err := execMigration(ctx, tx, migration.sql); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("%w: %s", ErrMigration, migration.name)
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO public.schema_migrations(version, name, sha256, applied_at) VALUES ($1, $2, $3, $4)", migration.version, migration.name, migration.digest, time.Now().UTC()); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("%w: record %s", ErrMigration, migration.name)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("%w: commit %s", ErrMigration, migration.name)
		}
		history[migration.version] = migration.digest
	}
	return nil
}

// Verify checks the embedded migration history without mutating the database.
func Verify(ctx context.Context, db *sql.DB) error {
	if ctx == nil || db == nil {
		return ErrMigration
	}
	files, err := orderedFiles()
	if err != nil {
		return err
	}
	conn, err := db.Conn(ctx)
	if err != nil {
		return ErrMigration
	}
	defer func() { _ = conn.Close() }()
	history, err := readHistory(ctx, conn)
	if err != nil {
		return err
	}
	if len(history) != len(files) {
		return ErrInvalidHistory
	}
	for _, migration := range files {
		if history[migration.version] != migration.digest {
			return ErrInvalidHistory
		}
	}
	return nil
}

func ensureHistory(ctx context.Context, conn *sql.Conn) error {
	statement := "CREATE TABLE IF NOT EXISTS public.schema_migrations (" +
		"version INTEGER PRIMARY KEY, name TEXT NOT NULL, sha256 TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL)"
	if _, err := conn.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("%w: history table", ErrMigration)
	}
	return nil
}

func readHistory(ctx context.Context, conn *sql.Conn) (map[int]string, error) {
	rows, err := conn.QueryContext(ctx, "SELECT version, sha256 FROM public.schema_migrations ORDER BY version")
	if err != nil {
		return nil, fmt.Errorf("%w: read history", ErrInvalidHistory)
	}
	defer func() { _ = rows.Close() }()
	history := map[int]string{}
	for rows.Next() {
		var version int
		var digest string
		if err := rows.Scan(&version, &digest); err != nil {
			return nil, fmt.Errorf("%w: scan history", ErrInvalidHistory)
		}
		history[version] = digest
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: iterate history", ErrInvalidHistory)
	}
	return history, nil
}

func execMigration(ctx context.Context, tx *sql.Tx, statement string) error {
	statement = strings.TrimSpace(statement)
	statement = strings.TrimPrefix(statement, "BEGIN;")
	statement = strings.TrimSpace(statement)
	statement = strings.TrimSuffix(statement, "COMMIT;")
	statement = strings.TrimSpace(statement)
	_, err := tx.ExecContext(ctx, statement)
	return err
}

func nextVersion(history map[int]string) int {
	max := 0
	for version := range history {
		if version > max {
			max = version
		}
	}
	return max + 1
}

func validateHistory(history map[int]string, files []file) error {
	if len(files) == 0 {
		return fmt.Errorf("%w: no embedded migrations", ErrInvalidHistory)
	}
	maxVersion := files[len(files)-1].version
	for version := range history {
		if version < 1 || version > maxVersion {
			return fmt.Errorf("%w: unknown version %d", ErrInvalidHistory, version)
		}
	}
	return nil
}
