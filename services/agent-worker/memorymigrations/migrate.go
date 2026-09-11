// Package memorymigrations explicitly prepares the managed PostgreSQL Memory store.
// Business request initialization must never call Apply.
package memorymigrations

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var Files embed.FS

// Apply requires the pre-provisioned memory_migrator identity and namespace.
// The administrator that provisions schemas is not a valid migration identity.
func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	var valid bool
	err := pool.QueryRow(ctx, `SELECT current_user='memory_migrator' AND session_user=current_user
  AND current_schema()='runtime_memory' AND n.nspowner=r.oid
  AND NOT r.rolsuper AND NOT r.rolcreatedb AND NOT r.rolcreaterole AND NOT r.rolreplication AND NOT r.rolbypassrls
  AND NOT EXISTS(SELECT 1 FROM pg_auth_members WHERE member=r.oid)
  FROM pg_roles r JOIN pg_namespace n ON n.nspname='runtime_memory' WHERE r.rolname=current_user`).Scan(&valid)
	if err != nil || !valid {
		return errors.New("memory migration identity/owner invalid")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(731004285)`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS runtime_memory.memory_schema_migrations(version text PRIMARY KEY,digest text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `REVOKE ALL ON runtime_memory.memory_schema_migrations FROM PUBLIC,memory_runtime; GRANT SELECT ON runtime_memory.memory_schema_migrations TO memory_runtime`); err != nil {
		return err
	}
	entries, err := Files.ReadDir(".")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		body, err := Files.ReadFile(entry.Name())
		if err != nil {
			return err
		}
		sum := sha256.Sum256(body)
		digest := hex.EncodeToString(sum[:])
		var old *string
		if err = tx.QueryRow(ctx, `SELECT (SELECT digest FROM runtime_memory.memory_schema_migrations WHERE version=$1)`, entry.Name()).Scan(&old); err != nil {
			return err
		}
		if old != nil {
			if *old != digest {
				return fmt.Errorf("memory migration digest changed: %s", entry.Name())
			}
			continue
		}
		if _, err = tx.Exec(ctx, string(body)); err != nil {
			return fmt.Errorf("memory migration %s: %w", entry.Name(), err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO runtime_memory.memory_schema_migrations(version,digest) VALUES($1,$2)`, entry.Name(), digest); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
