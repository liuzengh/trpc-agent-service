// Package postgres owns the Control API process PostgreSQL connection and
// migration mechanics. Business SQL remains in its owning module.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Open creates and verifies the shared process connection pool.
func Open(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	if strings.TrimSpace(databaseURL) == "" {
		return nil, errors.New("postgres connection configuration is required")
	}
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, errors.New("parse postgres configuration")
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, errors.New("open postgres pool")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.New("postgres connection unavailable")
	}
	return pool, nil
}

// Migrate applies embedded SQL files exactly once. A transaction-scoped
// advisory lock serializes startup across Control API replicas.
func Migrate(ctx context.Context, pool *pgxpool.Pool, files fs.FS) error {
	return migrate(ctx, pool, files, "")
}

// MigrateForRuntime also removes runtime access to mutate the migration ledger.
// The migration role owns the ledger; default table DML grants must not let the
// runtime role manufacture an applied migration or erase its history.
func MigrateForRuntime(ctx context.Context, pool *pgxpool.Pool, files fs.FS, runtimeRole string) error {
	if runtimeRole == "" {
		return errors.New("runtime postgres role is required for migration grants")
	}
	return migrate(ctx, pool, files, runtimeRole)
}

func migrate(ctx context.Context, pool *pgxpool.Pool, files fs.FS, runtimeRole string) error {

	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return fmt.Errorf("read embedded migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		if err := applyMigration(ctx, pool, files, name, runtimeRole); err != nil {
			return err
		}
	}
	return nil
}

func applyMigration(ctx context.Context, pool *pgxpool.Pool, files fs.FS, name, runtimeRole string) error {
	contents, err := fs.ReadFile(files, name)
	if err != nil {
		return fmt.Errorf("read migration %s: %w", name, err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", name, err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()

	// Stable signed int64 chosen solely for the Control API migration lock.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", int64(731004281)); err != nil {
		return fmt.Errorf("lock migration %s: %w", name, err)
	}
	// The ledger is created only after the same transaction acquired the lock,
	// including concurrent first startup into an empty schema.
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS control_schema_migrations (
			version text PRIMARY KEY,
			applied_at timestamptz NOT NULL DEFAULT now()
		)
	`); err != nil {
		return fmt.Errorf("create migration ledger: %w", err)
	}
	if runtimeRole != "" {
		role := pgx.Identifier{runtimeRole}.Sanitize()
		if _, err := tx.Exec(ctx, "REVOKE ALL PRIVILEGES ON TABLE control_schema_migrations FROM "+role+", PUBLIC; GRANT SELECT ON TABLE control_schema_migrations TO "+role); err != nil {
			return fmt.Errorf("protect migration ledger: %w", err)
		}
	}
	var applied bool
	if err := tx.QueryRow(
		ctx,
		"SELECT EXISTS (SELECT 1 FROM control_schema_migrations WHERE version = $1)",
		name,
	).Scan(&applied); err != nil {
		return fmt.Errorf("check migration %s: %w", name, err)
	}
	if applied {
		return tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, string(contents)); err != nil {
		return fmt.Errorf("apply migration %s: %w", name, err)
	}
	if _, err := tx.Exec(
		ctx,
		"INSERT INTO control_schema_migrations (version) VALUES ($1)",
		name,
	); err != nil {
		return fmt.Errorf("record migration %s: %w", name, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %s: %w", name, err)
	}
	return nil
}
