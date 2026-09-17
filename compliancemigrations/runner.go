// Package compliancemigrations owns the deliberately small schema installed
// in the independent compliance PostgreSQL database.
package compliancemigrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
)

//go:embed *.sql
var files embed.FS

type Migration struct{ Version, Name, Up, Down string }

// All returns the embedded compliance migrations in ascending version order.
func All() ([]Migration, error) {
	entries, err := files.ReadDir(".")
	if err != nil {
		return nil, err
	}
	byVersion := make(map[string]*Migration)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		parts := strings.SplitN(strings.TrimSuffix(entry.Name(), ".sql"), "_", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid compliance migration name %q", entry.Name())
		}
		nameParts := strings.SplitN(parts[1], ".", 2)
		if len(nameParts) != 2 || (nameParts[1] != "up" && nameParts[1] != "down") {
			return nil, fmt.Errorf("invalid compliance migration direction %q", entry.Name())
		}
		body, err := files.ReadFile(entry.Name())
		if err != nil {
			return nil, err
		}
		migration := byVersion[parts[0]]
		if migration == nil {
			migration = &Migration{Version: parts[0], Name: nameParts[0]}
			byVersion[parts[0]] = migration
		}
		if migration.Name != nameParts[0] {
			return nil, fmt.Errorf("compliance migration name mismatch for %s", parts[0])
		}
		if nameParts[1] == "up" {
			migration.Up = string(body)
		} else {
			migration.Down = string(body)
		}
	}
	versions := make([]string, 0, len(byVersion))
	for version := range byVersion {
		versions = append(versions, version)
	}
	sort.Strings(versions)
	result := make([]Migration, 0, len(versions))
	for _, version := range versions {
		migration := byVersion[version]
		if migration.Up == "" || migration.Down == "" {
			return nil, fmt.Errorf("compliance migration %s lacks up/down pair", version)
		}
		if _, err := transactionBody(migration.Up); err != nil {
			return nil, fmt.Errorf("compliance migration %s: %w", version, err)
		}
		if _, err := transactionBody(migration.Down); err != nil {
			return nil, fmt.Errorf("compliance migration %s: %w", version, err)
		}
		result = append(result, *migration)
	}
	return result, nil
}

type Runner struct{ DB *sql.DB }

// Up applies all pending compliance migrations in order.
func (r Runner) Up(ctx context.Context) error { return r.UpTo(ctx, "") }

// UpTo applies compliance migrations through target (inclusive). An empty
// target applies every embedded migration.
func (r Runner) UpTo(ctx context.Context, target string) error {
	if r.DB == nil {
		return errors.New("compliance migration database is required")
	}
	if _, err := r.DB.ExecContext(ctx, `CREATE SCHEMA IF NOT EXISTS compliance`); err != nil {
		return err
	}
	if _, err := r.DB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS compliance.schema_migrations(
version text PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp())`); err != nil {
		return err
	}
	all, err := All()
	if err != nil {
		return err
	}
	if target == "" && len(all) > 0 {
		target = all[len(all)-1].Version
	}
	for _, migration := range all {
		if target != "" && migration.Version > target {
			break
		}
		want := checksum(migration.Up)
		var applied string
		err := r.DB.QueryRowContext(ctx, `SELECT checksum FROM compliance.schema_migrations WHERE version=$1`, migration.Version).Scan(&applied)
		if err == nil {
			if applied != want {
				return fmt.Errorf("compliance migration %s checksum mismatch", migration.Version)
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		body, err := transactionBody(migration.Up)
		if err != nil {
			return fmt.Errorf("compliance migration %s: %w", migration.Version, err)
		}
		tx, err := r.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		// The bootstrap schema already exists so the migration remains
		// idempotent only through its immutable version row, not through
		// permissive DDL.
		body = strings.Replace(body, "CREATE SCHEMA compliance;", "", 1)
		execErr := error(nil)
		if _, execErr = tx.ExecContext(ctx, body); execErr == nil {
			_, execErr = tx.ExecContext(ctx, `INSERT INTO compliance.schema_migrations(version,checksum) VALUES($1,$2)`, migration.Version, want)
		}
		if execErr != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply compliance migration %s: %w", migration.Version, execErr)
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// Ready verifies every embedded migration is applied with the exact immutable
// checksum. It is read-only and safe for production readiness.
func (r Runner) Ready(ctx context.Context) error {
	if r.DB == nil {
		return errors.New("compliance migration database is required")
	}
	all, err := All()
	if err != nil {
		return err
	}
	for _, migration := range all {
		var applied string
		if err := r.DB.QueryRowContext(ctx, `SELECT checksum FROM compliance.schema_migrations WHERE version=$1`, migration.Version).Scan(&applied); err != nil {
			return fmt.Errorf("compliance migration %s is not ready", migration.Version)
		}
		if applied != checksum(migration.Up) {
			return fmt.Errorf("compliance migration %s checksum mismatch", migration.Version)
		}
	}
	return nil
}

// Down reverses every embedded migration in descending order. It is only for
// disposable test databases; production does not expose a down path.
func (r Runner) Down(ctx context.Context) error {
	if r.DB == nil {
		return errors.New("compliance migration database is required")
	}
	all, err := All()
	if err != nil {
		return err
	}
	for index := len(all) - 1; index >= 0; index-- {
		migration := all[index]
		var exists bool
		if err := r.DB.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM compliance.schema_migrations WHERE version=$1)`, migration.Version).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			continue
		}
		body, err := transactionBody(migration.Down)
		if err != nil {
			return fmt.Errorf("compliance migration %s: %w", migration.Version, err)
		}
		tx, err := r.DB.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, body); err == nil {
			_, err = tx.ExecContext(ctx, `DELETE FROM compliance.schema_migrations WHERE version=$1`, migration.Version)
		}
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("down compliance migration %s: %w", migration.Version, err)
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func transactionBody(value string) (string, error) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(value, "BEGIN;") || !strings.HasSuffix(value, "COMMIT;") {
		return "", errors.New("compliance migration is not transaction wrapped")
	}
	value = strings.TrimSpace(strings.TrimPrefix(value, "BEGIN;"))
	return strings.TrimSpace(strings.TrimSuffix(value, "COMMIT;")), nil
}

func checksum(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
