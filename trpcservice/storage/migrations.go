// Package storage provides platform persistence and tenant-level backend routing.
package storage

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

const migrationLockName = "trpc-agent-service:schema-migrations"

// ApplyMigrations applies all embedded up migrations once. The migration files
// use idempotent DDL so an interrupted MySQL DDL sequence can be safely retried.
func ApplyMigrations(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("migration database is required")
	}
	var acquired int
	if err := db.QueryRowContext(ctx, `SELECT GET_LOCK(?, 10)`, migrationLockName).Scan(&acquired); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	if acquired != 1 {
		return fmt.Errorf("migration lock was not acquired")
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), `SELECT RELEASE_LOCK(?)`, migrationLockName)
	}()

	if _, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migration (
		version BIGINT PRIMARY KEY,
		applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("create schema_migration table: %w", err)
	}

	files, err := upMigrationFiles()
	if err != nil {
		return err
	}
	for _, file := range files {
		version, err := migrationVersion(file)
		if err != nil {
			return err
		}
		var exists int
		if err := db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM schema_migration WHERE version = ?`, version,
		).Scan(&exists); err != nil {
			return fmt.Errorf("check migration %d: %w", version, err)
		}
		if exists > 0 {
			continue
		}
		data, err := migrationFS.ReadFile(file)
		if err != nil {
			return fmt.Errorf("read migration %q: %w", file, err)
		}
		for index, statement := range splitSQLStatements(string(data)) {
			if _, err := db.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("apply migration %d statement %d: %w", version, index+1, err)
			}
		}
		if _, err := db.ExecContext(ctx,
			`INSERT INTO schema_migration (version) VALUES (?)`, version,
		); err != nil {
			return fmt.Errorf("record migration %d: %w", version, err)
		}
	}
	return nil
}

func upMigrationFiles() ([]string, error) {
	files, err := fs.Glob(migrationFS, "migrations/*.up.sql")
	if err != nil {
		return nil, fmt.Errorf("list embedded migrations: %w", err)
	}
	sort.Slice(files, func(i, j int) bool {
		left, _ := migrationVersion(files[i])
		right, _ := migrationVersion(files[j])
		return left < right
	})
	return files, nil
}

func migrationVersion(file string) (int64, error) {
	name := path.Base(file)
	prefix, _, found := strings.Cut(name, "_")
	if !found {
		return 0, fmt.Errorf("migration %q has no numeric prefix", file)
	}
	version, err := strconv.ParseInt(prefix, 10, 64)
	if err != nil || version <= 0 {
		return 0, fmt.Errorf("migration %q has invalid version prefix", file)
	}
	return version, nil
}

func splitSQLStatements(source string) []string {
	var statements []string
	for _, candidate := range strings.Split(source, ";") {
		statement := strings.TrimSpace(candidate)
		if statement != "" {
			statements = append(statements, statement)
		}
	}
	return statements
}
