// Package postgres provides PostgreSQL connection, transaction, error, and
// serialization primitives shared by domain-owned PostgreSQL implementations.
// It deliberately contains no control-plane repository or domain model logic.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	storageerrors "github.com/XnLemon/trpc-agent-service/trpcservice/storage/errors"
	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// ErrStorage is the stable error category returned for unexpected database
// failures. The underlying driver error is intentionally not exposed to the
// Gateway because it may contain connection details or provider metadata.
var ErrStorage = storageerrors.ErrPostgres

// Options configures a database/sql pool opened by Open. A caller that
// already owns a pool can pass it directly to repository constructors.
type Options struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// Open creates and pings a pgx-backed database/sql pool. The ping is part of
// bootstrap readiness; it is not repeated by repository constructors.
func Open(ctx context.Context, dsn string, options Options) (*sql.DB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dsn = normalizeDSN(dsn)
	if dsn == "" {
		return nil, ErrStorage
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, ErrStorage
	}
	if options.MaxOpenConns > 0 {
		db.SetMaxOpenConns(options.MaxOpenConns)
	}
	if options.MaxIdleConns > 0 {
		db.SetMaxIdleConns(options.MaxIdleConns)
	}
	if options.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(options.ConnMaxLifetime)
	}
	if options.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(options.ConnMaxIdleTime)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	return db, nil
}

// Ping is used by readiness probes and does not disclose the driver error.
func Ping(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return ErrStorage
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := db.PingContext(ctx); err != nil {
		return MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	return nil
}

func normalizeDSN(dsn string) string {
	dsn = strings.TrimSpace(dsn)
	for _, prefix := range []string{"postgresql+psycopg://", "postgres+psycopg://"} {
		if strings.HasPrefix(strings.ToLower(dsn), prefix) {
			return "postgresql://" + dsn[len(prefix):]
		}
	}
	return dsn
}

// Queryer is the shared read/write surface implemented by both sql.DB and
// sql.Tx. Domain repositories use it to keep query helpers transaction-agnostic.
type Queryer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// RowScanner is implemented by sql.Row and sql.Rows for domain-specific row
// decoding without coupling those decoders to a concrete query method.
type RowScanner interface{ Scan(...any) error }

// Begin starts a read-committed transaction and maps unexpected driver errors
// to ErrStorage without disclosing driver details.
func Begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	if db == nil {
		return nil, ErrStorage
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	return tx, nil
}

// Rollback makes a best-effort rollback for a transaction that did not commit.
func Rollback(tx *sql.Tx) {
	if tx != nil {
		_ = tx.Rollback()
	}
}

// MapError maps portable SQL and PostgreSQL errors to domain error categories.
func MapError(ctx context.Context, err error, notFound, duplicate, conflict, invalid error) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, sql.ErrNoRows) {
		return notFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return duplicate
		case "23503", "23514", "22P02", "22001":
			return invalid
		case "40001", "40P01":
			return conflict
		}
	}
	return ErrStorage
}

// Commit commits a transaction while preserving cancellation and error-redaction
// behavior used by all PostgreSQL repositories.
func Commit(ctx context.Context, tx *sql.Tx) error {
	if tx == nil {
		return ErrStorage
	}
	if err := ctx.Err(); err != nil {
		Rollback(tx)
		return err
	}
	if err := tx.Commit(); err != nil {
		return MapError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	return nil
}

// AsUTC normalizes persisted timestamps at the storage boundary.
func AsUTC(value time.Time) time.Time {
	return value.UTC()
}

// NullableInt converts a nullable SQL integer to an owned optional value.
func NullableInt(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	copy := value.Int64
	return &copy
}

// NullableString converts a nullable SQL string to an owned optional value.
func NullableString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	copy := value.String
	return &copy
}

// NullableText represents an optional persisted string argument.
func NullableText(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

// MonotonicNow produces a UTC timestamp that never predates an existing
// persisted timestamp.
func MonotonicNow(previous time.Time) time.Time {
	now := time.Now().UTC()
	if now.Before(previous) {
		return previous
	}
	return now
}
