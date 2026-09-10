// Package postgres persists platform-owned multi-tenant control and execution
// data in PostgreSQL. Its embedded migrations prepare only platform-owned
// schemas; cross-backend data migration belongs to an external coordinator.
package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
)

const schemaMigrationLockKey int64 = 0x747270635f6173

//go:embed migrations/*.sql
var schemaMigrationFiles embed.FS

type schemaMigration struct {
	version  int64
	name     string
	contents string
	checksum [sha256.Size]byte
}

// Migrate applies the embedded PostgreSQL platform schema. The development
// history is represented by one immutable initialization migration; it does
// not move tenant data between backend implementations. A PostgreSQL
// transaction-level advisory lock serializes concurrent service nodes while
// applying the schema. Applied migration contents are immutable and verified
// by checksum.
func (s *Store) Migrate(ctx context.Context) error {
	if err := s.validate(); err != nil {
		return err
	}
	schemaMigrations, err := loadSchemaMigrations()
	if err != nil {
		return err
	}

	if err := s.bootstrapSchemaMigrations(ctx); err != nil {
		return err
	}

	known := make(map[int64]schemaMigration, len(schemaMigrations))
	for _, migration := range schemaMigrations {
		known[migration.version] = migration
	}

	for _, migration := range schemaMigrations {
		tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", migration.version, err)
		}
		if err := applySchemaMigration(ctx, tx, migration, known); err != nil {
			rollback(tx)
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			rollback(tx)
			return fmt.Errorf("commit migration %d: %w", migration.version, err)
		}
	}
	return nil
}

func (s *Store) bootstrapSchemaMigrations(ctx context.Context) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin migration bootstrap: %w", err)
	}
	defer func() {
		rollback(tx)
	}()

	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", schemaMigrationLockKey); err != nil {
		return fmt.Errorf("lock migration bootstrap: %w", err)
	}
	if _, err := tx.Exec(ctx, `
CREATE SCHEMA IF NOT EXISTS platform;
CREATE TABLE IF NOT EXISTS platform.schema_migration (
    version BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    checksum BYTEA NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT schema_migration_checksum_length CHECK (octet_length(checksum) = 32)
);`); err != nil {
		return fmt.Errorf("bootstrap migration schema: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration bootstrap: %w", err)
	}
	return nil
}

func applySchemaMigration(
	ctx context.Context,
	tx pgx.Tx,
	migration schemaMigration,
	known map[int64]schemaMigration,
) error {
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", schemaMigrationLockKey); err != nil {
		return fmt.Errorf("lock migration %d: %w", migration.version, err)
	}
	applied, err := readAppliedSchemaMigrations(ctx, tx)
	if err != nil {
		return err
	}
	if err := validateAppliedSchemaMigrations(applied, known); err != nil {
		return err
	}
	if _, ok := applied[migration.version]; ok {
		return nil
	}
	if _, err := tx.Exec(ctx, migration.contents); err != nil {
		return fmt.Errorf("apply schema migration %d %s: %w", migration.version, migration.name, err)
	}
	if _, err := tx.Exec(
		ctx,
		`INSERT INTO platform.schema_migration (version, name, checksum) VALUES ($1, $2, $3)`,
		migration.version,
		migration.name,
		migration.checksum[:],
	); err != nil {
		return fmt.Errorf("record schema migration %d: %w", migration.version, err)
	}
	return nil
}

func validateAppliedSchemaMigrations(
	applied map[int64][]byte,
	known map[int64]schemaMigration,
) error {
	for version, checksum := range applied {
		migration, ok := known[version]
		if !ok {
			return fmt.Errorf("database contains unknown schema migration %d", version)
		}
		if !bytes.Equal(checksum, migration.checksum[:]) {
			return fmt.Errorf("schema migration %d checksum does not match", version)
		}
	}
	return nil
}

func readAppliedSchemaMigrations(ctx context.Context, tx pgx.Tx) (map[int64][]byte, error) {
	rows, err := tx.Query(ctx, `SELECT version, checksum FROM platform.schema_migration ORDER BY version`)
	if err != nil {
		return nil, fmt.Errorf("query applied migrations: %w", err)
	}
	defer rows.Close()

	applied := make(map[int64][]byte)
	for rows.Next() {
		var version int64
		var checksum []byte
		if err := rows.Scan(&version, &checksum); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		applied[version] = bytes.Clone(checksum)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations: %w", err)
	}
	return applied, nil
}

func loadSchemaMigrations() ([]schemaMigration, error) {
	entries, err := schemaMigrationFiles.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}
	migrations := make([]schemaMigration, 0, len(entries))
	versions := make(map[int64]struct{}, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		versionText, name, ok := strings.Cut(strings.TrimSuffix(entry.Name(), ".sql"), "_")
		if !ok || name == "" {
			return nil, fmt.Errorf("migration filename %q is invalid", entry.Name())
		}
		version, err := strconv.ParseInt(versionText, 10, 64)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("migration filename %q has invalid version", entry.Name())
		}
		if _, ok := versions[version]; ok {
			return nil, fmt.Errorf("migration version %d is duplicated", version)
		}
		contents, err := schemaMigrationFiles.ReadFile("migrations/" + entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", entry.Name(), err)
		}
		if len(bytes.TrimSpace(contents)) == 0 {
			return nil, fmt.Errorf("schema migration %q is empty", entry.Name())
		}
		contents = bytes.ReplaceAll(contents, []byte("\r\n"), []byte("\n"))
		versions[version] = struct{}{}
		migrations = append(migrations, schemaMigration{
			version:  version,
			name:     name,
			contents: string(contents),
			checksum: sha256.Sum256(contents),
		})
	}
	if len(migrations) == 0 {
		return nil, errors.New("no platform schema migrations are embedded")
	}
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].version < migrations[j].version
	})
	return migrations, nil
}
