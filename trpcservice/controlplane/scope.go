// Package controlplane is the tenant-scoped data access layer over MySQL.
//
// It exists as its own package, separate from storage/mysql, because the
// guarantee the platform needs is not "SQL runs" — it is "SQL that cannot
// address another tenant's rows". storage/mysql opens a connection and owns
// the schema; this package owns the rule that every read, write, and
// cross-reference inside it carries a tenant.
//
// The rule is enforced by convention plus test, not by SQL rewriting. A
// Scope's helpers take exactly the arguments the query has placeholders for;
// there is no implicit argument injected behind the caller's back. That is a
// deliberate choice, not an omission: an UPDATE statement's placeholders run
// SET-before-WHERE, so a helper that silently prepended the tenant id would
// be filling the wrong placeholder in exactly the queries where scoping
// matters most (measured during this package's first draft). The invariant
// this package actually relies on is in controlplane_scoping_test.go, which
// fails the build if any repository query is missing a tenant predicate.
package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// ErrNoScope is returned instead of running a query that could reach across
// tenants. It is a programming error, not a runtime condition to retry: no
// caller should be able to build an empty Scope and then be surprised that a
// SELECT had no tenant predicate in it.
var ErrNoScope = errors.New("controlplane: an empty tenant scope is not a valid query")

// ErrCrossTenantReference means a write would link two rows that belong to
// different tenants — an app pointing at another tenant's revision, a
// knowledge binding across tenants. The database's composite foreign keys
// make most of these unrepresentable (see migrations/0001_control.sql);
// this error covers the references SQL cannot see, such as a row the
// publisher named by id that the schema has no foreign key for.
var ErrCrossTenantReference = errors.New("controlplane: reference crosses tenants")

// Scope is a tenant's handle on its own rows. Its methods take the tenant id
// explicitly as query arguments the way any other SQL does — the point of the
// type is that a method cannot be reached without one, and that it will
// refuse to run if the caller hands it an empty one.
type Scope struct {
	tenantID string
	db       *sql.DB
}

// DB is the control plane's handle on the database.
type DB struct{ db *sql.DB }

// NewDB wraps a connection pool opened by storage/mysql.
func NewDB(db *sql.DB) *DB { return &DB{db: db} }

// Scope returns a handle restricted to tenantID.
func (d *DB) Scope(tenantID string) (Scope, error) {
	if tenantID == "" {
		return Scope{}, ErrNoScope
	}
	return Scope{tenantID: tenantID, db: d.db}, nil
}

// MustScope is Scope for a tenant id this process produced itself (a
// bootstrap tenant, say) rather than one that arrived from a request. It
// panics on an empty id: that is a bug in this process, not bad input.
func (d *DB) MustScope(tenantID string) Scope {
	s, err := d.Scope(tenantID)
	if err != nil {
		panic(err)
	}
	return s
}

// TenantID reports the scope's tenant, for callers that must pass it as an
// explicit query argument. It is a method rather than a public field so that
// a Scope value can never be edited into addressing a different tenant.
func (s Scope) TenantID() string { return s.tenantID }

func (s Scope) check() error {
	if s.db == nil || s.tenantID == "" {
		return ErrNoScope
	}
	return nil
}

// QueryRow runs a scoped query. The tenant predicate must be part of query
// and its id must be one of args — see the package doc on why that is not
// checked here automatically.
func (s Scope) QueryRow(ctx context.Context, query string, args ...any) (*sql.Row, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	return s.db.QueryRowContext(ctx, query, args...), nil
}

// Query runs a scoped multi-row query.
func (s Scope) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	return s.db.QueryContext(ctx, query, args...)
}

// Exec runs a scoped write.
func (s Scope) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if err := s.check(); err != nil {
		return nil, err
	}
	return s.db.ExecContext(ctx, query, args...)
}

// WithTx runs fn inside one READ COMMITTED transaction scoped to this tenant.
// READ COMMITTED is deliberate rather than the MySQL default REPEATABLE READ:
// every claim and commit in this platform is "lock the row, re-read its
// current state, decide" — an isolation level that hands back a stale
// snapshot for the second reader in the same transaction would defeat that.
func (s Scope) WithTx(ctx context.Context, fn func(*TxScope) error) error {
	if err := s.check(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("controlplane: begin tx: %w", err)
	}
	defer func() {
		// A panic inside fn must not leave the transaction open holding row
		// locks until the connection's idle timeout.
		if r := recover(); r != nil {
			_ = tx.Rollback()
			panic(r)
		}
	}()
	if err := fn(&TxScope{tenantID: s.tenantID, tx: tx}); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("controlplane: rollback after %v failed: %w", err, rbErr)
		}
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("controlplane: commit: %w", err)
	}
	return nil
}

// TxScope is a Scope bound to an open transaction. Same contract, no way to
// escape the transaction it came from.
type TxScope struct {
	tenantID string
	tx       *sql.Tx
}

// TenantID reports the scope's tenant.
func (t *TxScope) TenantID() string { return t.tenantID }

// QueryRow runs a query inside the transaction.
func (t *TxScope) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return t.tx.QueryRowContext(ctx, query, args...)
}

// Exec runs a write inside the transaction.
func (t *TxScope) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

// Query runs a multi-row query inside the transaction.
func (t *TxScope) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}
