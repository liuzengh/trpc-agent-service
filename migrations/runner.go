package migrations

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
)

const EmptyVersion = "empty"

type database interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

type Runner struct{ db database }

type Plan struct {
	Current string
	Target  string
	Pending []string
}

func NewRunner(db *sql.DB) *Runner {
	if db == nil {
		return &Runner{}
	}
	return &Runner{db: db}
}

// NewRunnerOnConn binds all migration operations to one PostgreSQL session.
// Production migration commands use it while holding a session advisory lock.
func NewRunnerOnConn(conn *sql.Conn) *Runner {
	if conn == nil {
		return &Runner{}
	}
	return &Runner{db: conn}
}

// Ready verifies that every embedded migration is applied with the exact
// immutable checksum. It is read-only and is safe for production readiness;
// schema changes remain an explicit deployment step through Up.
func (r *Runner) Ready(ctx context.Context) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("migration runner is not configured")
	}
	all, err := All()
	if err != nil {
		return err
	}
	// Readiness verifies everything this binary understands but deliberately
	// tolerates later migration rows. That preserves N-1 Pods during an expand
	// migration and keeps binary rollback possible inside the observation window.
	for _, migration := range all {
		var applied string
		if err := r.db.QueryRowContext(ctx, `SELECT checksum FROM public.schema_migrations WHERE version=$1`, migration.Version).Scan(&applied); err != nil {
			return fmt.Errorf("migration %s is not ready", migration.Version)
		}
		if applied != migrationChecksum(migration.Up) {
			return fmt.Errorf("migration %s checksum mismatch", migration.Version)
		}
	}
	return nil
}

func LatestVersion() (string, error) {
	all, err := All()
	if err != nil {
		return "", err
	}
	if len(all) == 0 {
		return "", fmt.Errorf("no embedded migrations")
	}
	return all[len(all)-1].Version, nil
}

// Plan validates the complete applied prefix and returns the forward-only
// work through target. An empty target means the latest embedded migration.
func (r *Runner) Plan(ctx context.Context, target string) (Plan, error) {
	if r == nil || r.db == nil {
		return Plan{}, fmt.Errorf("migration runner is not configured")
	}
	all, err := All()
	if err != nil {
		return Plan{}, err
	}
	applied, err := r.applied(ctx)
	if err != nil {
		return Plan{}, err
	}
	return buildPlan(all, applied, target)
}

func (r *Runner) applied(ctx context.Context) (map[string]string, error) {
	var table sql.NullString
	if err := r.db.QueryRowContext(ctx, `SELECT to_regclass('public.schema_migrations')::text`).Scan(&table); err != nil {
		return nil, fmt.Errorf("inspect migration table: %w", err)
	}
	result := make(map[string]string)
	if !table.Valid || table.String == "" {
		return result, nil
	}
	rows, err := r.db.QueryContext(ctx, `SELECT version,checksum FROM public.schema_migrations ORDER BY version`)
	if err != nil {
		return nil, fmt.Errorf("read migration state: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var version, checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			return nil, fmt.Errorf("read migration state: %w", err)
		}
		if _, duplicate := result[version]; duplicate {
			return nil, fmt.Errorf("duplicate migration version %s", version)
		}
		result[version] = checksum
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read migration state: %w", err)
	}
	return result, nil
}

func buildPlan(all []Migration, applied map[string]string, target string) (Plan, error) {
	if len(all) == 0 {
		return Plan{}, fmt.Errorf("no embedded migrations")
	}
	if target == "" {
		target = all[len(all)-1].Version
	}
	targetIndex := -1
	known := make(map[string]int, len(all))
	for index, migration := range all {
		known[migration.Version] = index
		if migration.Version == target {
			targetIndex = index
		}
	}
	if targetIndex < 0 {
		return Plan{}, fmt.Errorf("migration target %s does not exist", target)
	}
	for version := range applied {
		if _, ok := known[version]; !ok {
			return Plan{}, fmt.Errorf("unknown applied migration %s", version)
		}
	}
	currentIndex := -1
	missing := false
	for index, migration := range all {
		checksum, exists := applied[migration.Version]
		if !exists {
			missing = true
			continue
		}
		if missing {
			return Plan{}, fmt.Errorf("migration history has a gap before %s", migration.Version)
		}
		if checksum != migrationChecksum(migration.Up) {
			return Plan{}, fmt.Errorf("migration %s checksum mismatch", migration.Version)
		}
		currentIndex = index
	}
	if currentIndex > targetIndex {
		return Plan{}, fmt.Errorf("migration downgrade from %s to %s is forbidden", all[currentIndex].Version, target)
	}
	current := EmptyVersion
	if currentIndex >= 0 {
		current = all[currentIndex].Version
	}
	pending := make([]string, 0, targetIndex-currentIndex)
	for index := currentIndex + 1; index <= targetIndex; index++ {
		pending = append(pending, all[index].Version)
	}
	return Plan{Current: current, Target: target, Pending: pending}, nil
}

func (r *Runner) Up(ctx context.Context) error {
	return r.UpTo(ctx, "")
}

// UpTo applies migrations through target (inclusive). An empty target applies
// all migrations. It is primarily useful for controlled upgrade rehearsals.
func (r *Runner) UpTo(ctx context.Context, target string) error {
	plan, err := r.Plan(ctx, target)
	if err != nil {
		return err
	}
	if _, err := r.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS public.schema_migrations(
version text PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	all, err := All()
	if err != nil {
		return err
	}
	target = plan.Target
	for _, migration := range all {
		if target != "" && migration.Version > target {
			break
		}
		checksum := migrationChecksum(migration.Up)
		var applied string
		err := r.db.QueryRowContext(ctx, `SELECT checksum FROM public.schema_migrations WHERE version=$1`, migration.Version).Scan(&applied)
		if err == nil {
			if applied != checksum {
				return fmt.Errorf("migration %s checksum mismatch", migration.Version)
			}
			continue
		}
		if err != sql.ErrNoRows {
			return err
		}
		body, err := transactionBody(migration.Up)
		if err != nil {
			return fmt.Errorf("migration %s: %w", migration.Version, err)
		}
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, body); err == nil {
			_, err = tx.ExecContext(ctx, `INSERT INTO public.schema_migrations(version,checksum) VALUES($1,$2)`, migration.Version, checksum)
		}
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("apply migration %s: %w", migration.Version, err)
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) DownAll(ctx context.Context) error {
	all, err := All()
	if err != nil {
		return err
	}
	for index := len(all) - 1; index >= 0; index-- {
		migration := all[index]
		var exists bool
		if err := r.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM public.schema_migrations WHERE version=$1)`, migration.Version).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			continue
		}
		body, err := transactionBody(migration.Down)
		if err != nil {
			return fmt.Errorf("migration %s: %w", migration.Version, err)
		}
		tx, err := r.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, body); err == nil {
			_, err = tx.ExecContext(ctx, `DELETE FROM public.schema_migrations WHERE version=$1`, migration.Version)
		}
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("down migration %s: %w", migration.Version, err)
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	_, err = r.db.ExecContext(ctx, `DROP TABLE IF EXISTS public.schema_migrations`)
	return err
}

func (r *Runner) PublicSchemaEmpty(ctx context.Context) (bool, error) {
	var count int
	err := r.db.QueryRowContext(ctx, `SELECT
  (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
   WHERE n.nspname='public' AND c.relkind IN ('r','p','v','m','S','f'))
 + (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public')`).Scan(&count)
	return count == 0, err
}

func transactionBody(input string) (string, error) {
	trimmed := strings.TrimSpace(input)
	if !strings.HasPrefix(trimmed, "BEGIN;") || !strings.HasSuffix(trimmed, "COMMIT;") {
		return "", fmt.Errorf("migration is not transaction wrapped")
	}
	trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "BEGIN;"))
	trimmed = strings.TrimSpace(strings.TrimSuffix(trimmed, "COMMIT;"))
	return trimmed, nil
}

func migrationChecksum(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}
