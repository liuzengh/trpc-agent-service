package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/migrations"
)

// ErrOut of order guard rails: a migration file whose checksum changed after
// it was already applied is a sign of editing history instead of adding a
// step, which is exactly what a versioned migration set exists to prevent.
var (
	// ErrChecksumMismatch means a file already recorded as applied no longer
	// hashes to what it did when it was applied.
	ErrChecksumMismatch = errors.New("mysql: migration checksum mismatch")
	// ErrLockNotAcquired means another process holds the migration lock and
	// the wait budget ran out.
	ErrLockNotAcquired = errors.New("mysql: migration lock held by another process")
)

// lockName namespaces GET_LOCK for this one database's schema; the name must
// stay under MySQL's 64-character GET_LOCK limit.
const lockName = "trpc-agent-service:migrate"

// lockWait is how long a migrator waits for another process before giving up
// and refusing to run. Migration is not on a request path — the CLI or the
// deploy job invokes it — so waiting a bounded time beats racing, and
// erroring out beats hanging forever against a wedged peer.
const lockWait = 30 * time.Second

// Applied describes one migration file's recorded state.
type Applied struct {
	Version   string
	Checksum  string
	AppliedAt time.Time
	ElapsedMS int64
}

// Ledger is the schema_migrations table. It is the only platform table that
// is intentionally not tenant-scoped: it describes the database itself, not
// any tenant's data.
type Ledger struct{ db *sql.DB }

// NewLedger prepares a ledger handle and creates the table if this is the
// first run against an empty database.
func NewLedger(db *sql.DB) *Ledger { return &Ledger{db: db} }

const ledgerDDL = `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     VARCHAR(255) NOT NULL,
    checksum    CHAR(64)     NOT NULL,
    applied_at  TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    elapsed_ms  BIGINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (version)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci`

// EnsureLedgerTable creates schema_migrations. Separate from Migrate so a
// caller can inspect ledger state (e.g. a status subcommand) without
// implying a write.
func (l *Ledger) EnsureLedgerTable(ctx context.Context) error {
	if _, err := l.db.ExecContext(ctx, ledgerDDL); err != nil {
		return fmt.Errorf("mysql: create schema_migrations: %w", err)
	}
	return nil
}

// Applied returns the recorded migrations in version order.
func (l *Ledger) Applied(ctx context.Context) ([]Applied, error) {
	rows, err := l.db.QueryContext(ctx,
		"SELECT version, checksum, applied_at, elapsed_ms FROM schema_migrations ORDER BY version")
	if err != nil {
		return nil, fmt.Errorf("mysql: read schema_migrations: %w", err)
	}
	defer rows.Close()
	var out []Applied
	for rows.Next() {
		var a Applied
		if err := rows.Scan(&a.Version, &a.Checksum, &a.AppliedAt, &a.ElapsedMS); err != nil {
			return nil, fmt.Errorf("mysql: scan schema_migrations: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// MigrateResult reports what a run did, so a caller can distinguish "already
// up to date" from "applied N steps" instead of inferring it from silence.
type MigrateResult struct {
	Applied      []string
	AlreadyKnown []string
}

// Migrate acquires the schema lock and applies every pending migration file
// from migrations.FS, in lexical order.
//
// The lock matters more than it looks: without it, a rolling deploy that
// starts several replicas at once has each of them racing the same ALTERs.
// MySQL's GET_LOCK is connection-scoped, so this holds one pinned connection
// for the whole run rather than letting the pool rotate it.
func Migrate(ctx context.Context, db *sql.DB) (*MigrateResult, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("mysql: migrate conn: %w", err)
	}
	defer conn.Close()

	if err := acquireLock(ctx, conn); err != nil {
		return nil, err
	}
	defer func() {
		// Release is best-effort on a context that may already be done; the
		// lock is session-scoped anyway, so a lost release dies with the
		// connection rather than wedging the next run.
		_, _ = conn.ExecContext(context.WithoutCancel(ctx), "SELECT RELEASE_LOCK(?)", lockName)
	}()

	ledger := NewLedger(db)
	if err := ledger.EnsureLedgerTable(ctx); err != nil {
		return nil, err
	}
	already, err := ledger.Applied(ctx)
	if err != nil {
		return nil, err
	}
	byVersion := make(map[string]string, len(already))
	for _, a := range already {
		byVersion[a.Version] = a.Checksum
	}

	files, err := migrations.FS.ReadDir(".")
	if err != nil {
		return nil, fmt.Errorf("mysql: read embedded migrations: %w", err)
	}
	names := make([]string, 0, len(files))
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".sql") {
			continue
		}
		names = append(names, f.Name())
	}
	sort.Strings(names)

	res := &MigrateResult{}
	for _, name := range names {
		body, err := migrations.FS.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("mysql: read %s: %w", name, err)
		}
		sum := checksum(body)
		if prev, ok := byVersion[name]; ok {
			if prev != sum {
				return nil, fmt.Errorf("%w: %s was applied with checksum %s and now hashes to %s",
					ErrChecksumMismatch, name, prev, sum)
			}
			res.AlreadyKnown = append(res.AlreadyKnown, name)
			continue
		}
		start := time.Now()
		if err := execFile(ctx, conn, body); err != nil {
			return nil, fmt.Errorf("mysql: apply %s: %w", name, err)
		}
		elapsed := time.Since(start).Milliseconds()
		if _, err := conn.ExecContext(ctx,
			"INSERT INTO schema_migrations (version, checksum, elapsed_ms) VALUES (?, ?, ?)",
			name, sum, elapsed); err != nil {
			return nil, fmt.Errorf("mysql: record %s: %w", name, err)
		}
		res.Applied = append(res.Applied, name)
		slog.Info("migration applied", "version", name, "elapsed_ms", elapsed)
	}
	return res, nil
}

func acquireLock(ctx context.Context, conn *sql.Conn) error {
	wait := int(lockWait.Seconds())
	if wait < 1 {
		wait = 1
	}
	var got sql.NullInt64
	// GET_LOCK is written as a prepared-free exec with a placeholder rather
	// than string interpolation: the timeout must stay a value, not part of
	// the SQL text.
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, ?)", lockName, wait).Scan(&got); err != nil {
		return fmt.Errorf("mysql: acquire migration lock: %w", err)
	}
	if !got.Valid || got.Int64 != 1 {
		return ErrLockNotAcquired
	}
	return nil
}

// execFile runs one migration file's statements on the pinned connection.
// MySQL driver multiStatements is off by default, so a file's statements are
// split rather than sent as one script. Every CREATE TABLE / ALTER in this
// project's migrations is DDL, which MySQL commits implicitly — there is no
// partial-file transaction to roll back mid-way, so a failed file can leave a
// half-applied schema; that is why every statement below is also written to
// be individually valid and why 0001's tables are all IF NOT EXISTS.
func execFile(ctx context.Context, conn *sql.Conn, body []byte) error {
	for _, stmt := range splitStatements(string(body)) {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("%w (statement: %s)", err, truncate(stmt, 120))
		}
	}
	return nil
}

func checksum(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func truncate(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func splitStatements(script string) []string {
	var out []string
	var buf strings.Builder
	for _, line := range strings.Split(script, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") || trimmed == "" {
			continue
		}
		buf.WriteString(line)
		buf.WriteString("\n")
		if strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSpace(strings.TrimSuffix(buf.String(), ";"))
			if stmt != "" {
				out = append(out, stmt)
			}
			buf.Reset()
		}
	}
	// A trailing statement with no final semicolon is still a statement.
	if stmt := strings.TrimSpace(buf.String()); stmt != "" {
		out = append(out, strings.TrimSuffix(stmt, ";"))
	}
	return out
}
