// Package mysql owns the platform's connection to the control-plane and
// runtime database (approved plan: "MySQL 是权威事实源"). It is deliberately
// thin: database/sql plus a driver, no ORM. Everything tenant-scoped — every
// table except the migration ledger itself — goes through
// trpcservice/controlplane's Scope, not through this package directly.
package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	// The package is aliased because this file's own package is also named
	// mysql (storage/mysql) — a collision that only shows up when the driver
	// stops being a blank import.
	gomysql "github.com/go-sql-driver/mysql"
)

// defaultPool bounds one process's connection use. MaxOpen is modest because
// the roles that matter for lease behaviour (worker claims, delivery
// attempts) are low-concurrency by design; a big pool here would only mean a
// big number of sessions a MySQL outage can strand at once. It is a function
// rather than a package var so no caller can mutate the shared default.
func defaultPool() Pool {
	return Pool{
		MaxOpenConns:    16,
		MaxIdleConns:    4,
		ConnMaxLifetime: 30 * time.Minute,
	}
}

// Pool configures the underlying database/sql pool.
type Pool struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// IsDuplicateKey reports whether err is MySQL's 1062 (duplicate entry on a
// unique index). Callers that use a unique key as the real concurrency guard
// — the inbox's "one row per accepted message", the plan-cache's version CAS
// — need to tell "someone else already did this exact insert" apart from
// "the database is broken", and the only correct way to do that is to ask the
// driver, not to pattern-match on an error string a future driver version
// could reword.
func IsDuplicateKey(err error) bool {
	var me *gomysql.MySQLError
	if errors.As(err, &me) {
		return me.Number == 1062
	}
	return false
}

// Open connects to dsn and proves the database is actually reachable before
// returning it: a control-plane database that cannot answer a SELECT 1 must
// stop a boot rather than surface as the first query's error, which is what
// storage.NewSessionService already does for Redis (same rationale, same
// fail-fast contract).
func Open(ctx context.Context, dsn string) (*sql.DB, error) {
	return OpenPool(ctx, dsn, defaultPool())
}

// normalizeDSN turns on parseTime regardless of what the caller wrote.
//
// Without it the driver hands back TIMESTAMP columns as raw bytes and every
// scan into a time.Time fails at the first query, not at connect time — which
// is a confusing way for a correctly-written DSN to be wrong. This was
// measured, not anticipated: the migration ledger's own applied_at column is
// the query that trips it. Silently overriding beats rejecting, because
// there is no deployment this platform supports where the byte-scan behaviour
// is the one you want.
func normalizeDSN(dsn string) (string, error) {
	cfg, err := gomysql.ParseDSN(dsn)
	if err != nil {
		return "", fmt.Errorf("mysql: parse dsn: %w", err)
	}
	if cfg.ParseTime {
		return dsn, nil
	}
	cfg.ParseTime = true
	return cfg.FormatDSN(), nil
}

// OpenPool is Open with an explicit pool size.
func OpenPool(ctx context.Context, dsn string, pool Pool) (*sql.DB, error) {
	dsn, err := normalizeDSN(dsn)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("mysql: open: %w", err)
	}
	if pool.MaxOpenConns > 0 {
		db.SetMaxOpenConns(pool.MaxOpenConns)
	}
	if pool.MaxIdleConns > 0 {
		db.SetMaxIdleConns(pool.MaxIdleConns)
	}
	if pool.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(pool.ConnMaxLifetime)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysql: ping: %w", err)
	}
	return db, nil
}
