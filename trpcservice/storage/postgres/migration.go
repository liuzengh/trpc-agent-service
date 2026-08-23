package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const migrationTable = "schema_migration"

var migrationFilePattern = regexp.MustCompile(`^(\d+)_([a-z0-9][a-z0-9_-]*)\.(up|down)\.sql$`)

type MigrationVersion struct {
	Version  int64
	Name     string
	Checksum string
}

type Migrator interface {
	Up(context.Context) error
	Down(context.Context, int) error
	Current(context.Context) (MigrationVersion, error)
	Close()
}

type migration struct {
	version  int64
	name     string
	up       string
	down     string
	checksum string
}

type migrator struct {
	pool             *pgxpool.Pool
	migrations       []migration
	lockKey          int64
	lockTimeout      time.Duration
	statementTimeout time.Duration
	allowDown        bool
	ownsPool         bool
}

// NewMigrator loads paired migration files from source. The source must be
// trusted application files, not user-provided SQL. The context controls pool
// creation and the initial connectivity check.
func NewMigrator(ctx context.Context, cfg PostgresConfig, source fs.FS) (Migrator, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	migrations, err := loadMigrations(source)
	if err != nil {
		return nil, err
	}
	pool, err := NewPool(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return newMigrator(pool, cfg, migrations, true), nil
}

// NewMigratorWithPool is useful when the caller owns pool lifecycle.
func NewMigratorWithPool(pool *pgxpool.Pool, cfg PostgresConfig, source fs.FS) (Migrator, error) {
	if pool == nil {
		return nil, fmt.Errorf("postgres: pool is required")
	}
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	migrations, err := loadMigrations(source)
	if err != nil {
		return nil, err
	}
	return newMigrator(pool, cfg, migrations, false), nil
}

func newMigrator(pool *pgxpool.Pool, cfg PostgresConfig, migrations []migration, ownsPool bool) *migrator {
	return &migrator{
		pool: pool, migrations: migrations, lockKey: cfg.MigrationLockKey,
		lockTimeout: cfg.LockTimeout, statementTimeout: cfg.StatementTimeout,
		allowDown: cfg.AllowDestructiveDown, ownsPool: ownsPool,
	}
}

func (m *migrator) Close() {
	if m != nil && m.ownsPool && m.pool != nil {
		m.pool.Close()
	}
}

func loadMigrations(source fs.FS) ([]migration, error) {
	if source == nil {
		return nil, fmt.Errorf("%w: source is nil", ErrInvalidMigrationSource)
	}
	entries, err := fs.ReadDir(source, ".")
	if err != nil {
		return nil, fmt.Errorf("%w: read source: %v", ErrInvalidMigrationSource, err)
	}
	byVersion := make(map[int64]*migration)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		match := migrationFilePattern.FindStringSubmatch(entry.Name())
		if match == nil {
			return nil, fmt.Errorf("%w: invalid file %q", ErrInvalidMigrationSource, entry.Name())
		}
		version, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("%w: invalid version in %q", ErrMigrationVersion, entry.Name())
		}
		contents, err := fs.ReadFile(source, entry.Name())
		if err != nil {
			return nil, fmt.Errorf("%w: read %q: %v", ErrInvalidMigrationSource, entry.Name(), err)
		}
		item := byVersion[version]
		if item == nil {
			item = &migration{version: version, name: match[2]}
			byVersion[version] = item
		} else if item.name != match[2] {
			return nil, fmt.Errorf("%w: version %d has multiple names", ErrMigrationVersion, version)
		}
		switch match[3] {
		case "up":
			if item.up != "" {
				return nil, fmt.Errorf("%w: duplicate up migration %q", ErrInvalidMigrationSource, entry.Name())
			}
			item.up = string(contents)
		case "down":
			if item.down != "" {
				return nil, fmt.Errorf("%w: duplicate down migration %q", ErrInvalidMigrationSource, entry.Name())
			}
			item.down = string(contents)
		}
	}
	migrations := make([]migration, 0, len(byVersion))
	for _, item := range byVersion {
		if item.up == "" || item.down == "" {
			return nil, fmt.Errorf("%w: version %d must have paired up/down files", ErrInvalidMigrationSource, item.version)
		}
		hash := sha256.New()
		hash.Write([]byte(item.up))
		hash.Write([]byte{0})
		hash.Write([]byte(item.down))
		item.checksum = hex.EncodeToString(hash.Sum(nil))
		migrations = append(migrations, *item)
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].version < migrations[j].version })
	for i := 1; i < len(migrations); i++ {
		if migrations[i-1].version == migrations[i].version {
			return nil, fmt.Errorf("%w: duplicate version %d", ErrMigrationVersion, migrations[i].version)
		}
	}
	return migrations, nil
}

func (m *migrator) Up(ctx context.Context) error {
	return m.withLock(ctx, func(conn *pgxpool.Conn) error {
		applied, err := m.applied(ctx, conn)
		if err != nil {
			return err
		}
		if err := m.validateApplied(ctx, conn, applied); err != nil {
			return err
		}
		for _, item := range m.migrations {
			if existing, ok := applied[item.version]; ok {
				if existing.Name != item.name || existing.Checksum != item.checksum {
					return fmt.Errorf("%w: version %d", ErrMigrationChecksum, item.version)
				}
				continue
			}
			if err := m.apply(ctx, conn, item); err != nil {
				return err
			}
		}
		return nil
	})
}

func (m *migrator) Down(ctx context.Context, steps int) error {
	if !m.allowDown {
		return ErrDestructiveDownDisabled
	}
	if steps < 1 {
		return fmt.Errorf("postgres: steps must be positive")
	}
	return m.withLock(ctx, func(conn *pgxpool.Conn) error {
		applied, err := m.applied(ctx, conn)
		if err != nil {
			return err
		}
		if err := m.validateApplied(ctx, conn, applied); err != nil {
			return err
		}
		ordered := make([]migration, 0, len(m.migrations))
		for i := len(m.migrations) - 1; i >= 0; i-- {
			if _, ok := applied[m.migrations[i].version]; ok {
				ordered = append(ordered, m.migrations[i])
			}
		}
		if steps > len(ordered) {
			steps = len(ordered)
		}
		for _, item := range ordered[:steps] {
			tx, err := conn.Begin(ctx)
			if err != nil {
				return fmt.Errorf("postgres: begin down %d: %w", item.version, err)
			}
			if _, err = tx.Exec(ctx, item.down); err == nil {
				_, err = tx.Exec(ctx, "DELETE FROM schema_migration WHERE version = $1", item.version)
			}
			if err != nil {
				_ = tx.Rollback(ctx)
				return fmt.Errorf("postgres: down migration %d: %w", item.version, err)
			}
			if err = tx.Commit(ctx); err != nil {
				return fmt.Errorf("postgres: commit down %d: %w", item.version, err)
			}
		}
		return nil
	})
}

func (m *migrator) Current(ctx context.Context) (MigrationVersion, error) {
	var current MigrationVersion
	err := m.withLock(ctx, func(conn *pgxpool.Conn) error {
		applied, err := m.applied(ctx, conn)
		if err != nil {
			return err
		}
		if err := m.validateApplied(ctx, conn, applied); err != nil {
			return err
		}
		for _, item := range m.migrations {
			if version, ok := applied[item.version]; ok && version.Version > current.Version {
				current = version
			}
		}
		return nil
	})
	return current, err
}

func (m *migrator) withLock(ctx context.Context, fn func(*pgxpool.Conn) error) error {
	conn, err := m.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("postgres: acquire migration connection: %w", err)
	}
	defer conn.Release()
	var previousLockTimeout, previousStatementTimeout string
	if err := conn.QueryRow(ctx, "SHOW lock_timeout").Scan(&previousLockTimeout); err != nil {
		return fmt.Errorf("postgres: read lock timeout: %w", err)
	}
	if err := conn.QueryRow(ctx, "SHOW statement_timeout").Scan(&previousStatementTimeout); err != nil {
		return fmt.Errorf("postgres: read statement timeout: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), "SELECT set_config('lock_timeout', $1, false)", previousLockTimeout)
		_, _ = conn.Exec(context.Background(), "SELECT set_config('statement_timeout', $1, false)", previousStatementTimeout)
	}()
	if _, err := conn.Exec(ctx, "SELECT set_config('lock_timeout', $1, false)", durationSetting(m.lockTimeout)); err != nil {
		return fmt.Errorf("postgres: configure lock timeout: %w", err)
	}
	if _, err := conn.Exec(ctx, "SELECT set_config('statement_timeout', $1, false)", durationSetting(m.statementTimeout)); err != nil {
		return fmt.Errorf("postgres: configure statement timeout: %w", err)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", m.lockKey); err != nil {
		return fmt.Errorf("postgres: acquire migration lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", m.lockKey) }()
	return fn(conn)
}

func durationSetting(value time.Duration) string {
	return fmt.Sprintf("%dms", value.Milliseconds())
}

func (m *migrator) applied(ctx context.Context, conn *pgxpool.Conn) (map[int64]MigrationVersion, error) {
	rows, err := conn.Query(ctx, "SELECT version, name, checksum FROM schema_migration ORDER BY version")
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return map[int64]MigrationVersion{}, nil
		}
		return nil, fmt.Errorf("postgres: read migration metadata: %w", err)
	}
	defer rows.Close()
	result := make(map[int64]MigrationVersion)
	for rows.Next() {
		var item MigrationVersion
		if err := rows.Scan(&item.Version, &item.Name, &item.Checksum); err != nil {
			return nil, fmt.Errorf("postgres: scan migration metadata: %w", err)
		}
		result[item.Version] = item
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("postgres: read migration metadata: %w", err)
	}
	return result, nil
}

func (m *migrator) validateApplied(ctx context.Context, conn *pgxpool.Conn, applied map[int64]MigrationVersion) error {
	known := make(map[int64]migration, len(m.migrations))
	for _, item := range m.migrations {
		known[item.version] = item
	}
	maxApplied := int64(0)
	for version := range applied {
		if version > maxApplied {
			maxApplied = version
		}
	}
	for _, item := range m.migrations {
		if item.version >= maxApplied {
			break
		}
		if _, ok := applied[item.version]; !ok {
			return fmt.Errorf("%w: version %d", ErrMigrationMissingVersion, item.version)
		}
	}
	for version, existing := range applied {
		item, ok := known[version]
		if !ok {
			return fmt.Errorf("%w: version %d", ErrUnknownMigrationVersion, version)
		}
		if existing.Name != item.name || existing.Checksum != item.checksum {
			return fmt.Errorf("%w: version %d", ErrMigrationChecksum, version)
		}
	}
	if _, ok := applied[1]; ok {
		var tableCount int
		if err := conn.QueryRow(ctx, `SELECT count(*)
			FROM information_schema.tables
			WHERE table_schema = current_schema()
				AND table_name = ANY($1)`, initialTableNames).Scan(&tableCount); err != nil {
			return fmt.Errorf("postgres: validate initial schema: %w", err)
		}
		if tableCount != len(initialTableNames) {
			return fmt.Errorf("postgres: initial schema drift: expected %d tables, found %d", len(initialTableNames), tableCount)
		}
	}
	return nil
}

var initialTableNames = []string{
	"tenant", "agent_app", "channel_binding", "user_identity", "session",
	"session_event", "message_dedup", "memory", "summary", "artifact",
	"audit_log", "outbox_message", "dead_letter", "agent_release",
	"tenant_config_version",
}

func (m *migrator) apply(ctx context.Context, conn *pgxpool.Conn, item migration) error {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("postgres: begin migration %d: %w", item.version, err)
	}
	_, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migration (
		version bigint PRIMARY KEY,
		name text NOT NULL,
		checksum text NOT NULL,
		applied_at timestamptz NOT NULL DEFAULT now()
	)`)
	if err == nil {
		_, err = tx.Exec(ctx, item.up)
	}
	if err == nil {
		_, err = tx.Exec(ctx, `INSERT INTO schema_migration (version, name, checksum) VALUES ($1, $2, $3)`, item.version, item.name, item.checksum)
	}
	if err != nil {
		_ = tx.Rollback(ctx)
		return fmt.Errorf("postgres: up migration %d: %w", item.version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit migration %d: %w", item.version, err)
	}
	return nil
}
