// Package migrations owns the Gateway-only schema and its startup executor.
package migrations

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed *.sql
var Files embed.FS

func Apply(ctx context.Context, pool *pgxpool.Pool) error {
	return apply(ctx, pool, "")
}

// ApplyForRuntime applies migrations and removes runtime write privileges from
// the migration ledger before committing. Historical migration SQL is unchanged.
func ApplyForRuntime(ctx context.Context, pool *pgxpool.Pool, runtimeRole string) error {
	if runtimeRole == "" {
		return fmt.Errorf("gateway runtime role is required for migration")
	}
	return apply(ctx, pool, runtimeRole)
}

func apply(ctx context.Context, pool *pgxpool.Pool, runtimeRole string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer rollbackBounded(tx)
	// Lock before creating the ledger, including the first concurrent startup.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(731004282)`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS gateway_schema_migrations(version text PRIMARY KEY,digest text NOT NULL,applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	entries, err := Files.ReadDir(".")
	if err != nil {
		return err
	}
	var names []string
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		body, err := Files.ReadFile(name)
		if err != nil {
			return err
		}
		sum := sha256.Sum256(body)
		digest := hex.EncodeToString(sum[:])
		var exists bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM gateway_schema_migrations WHERE version=$1)`, name).Scan(&exists); err != nil {
			return err
		}
		if exists {
			var old string
			if err = tx.QueryRow(ctx, `SELECT digest FROM gateway_schema_migrations WHERE version=$1`, name).Scan(&old); err != nil {
				return err
			}
			if old != digest {
				return fmt.Errorf("gateway migration digest changed: %s", name)
			}
			continue
		}
		if _, err = tx.Exec(ctx, string(body)); err != nil {
			return fmt.Errorf("migration %s: %w", name, err)
		}
		if _, err = tx.Exec(ctx, `INSERT INTO gateway_schema_migrations(version,digest) VALUES($1,$2)`, name, digest); err != nil {
			return err
		}
	}
	if runtimeRole != "" {
		// Default privileges grant DML on business tables, not authority to forge
		// or remove the history used to validate future migrations.
		_, err = tx.Exec(ctx, `REVOKE ALL PRIVILEGES ON TABLE gateway_schema_migrations FROM `+pgx.Identifier{runtimeRole}.Sanitize()+`, PUBLIC`)
		if err != nil {
			return err
		}
		if _, err = tx.Exec(ctx, `GRANT SELECT ON TABLE gateway_schema_migrations TO `+pgx.Identifier{runtimeRole}.Sanitize()); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// A startup deadline may already be canceled when migration cleanup begins.
// Use a fresh bounded context: cancellation should not skip cleanup, and a
// stalled connection should not keep shutdown waiting without a deadline.
func rollbackBounded(tx interface{ Rollback(context.Context) error }) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
