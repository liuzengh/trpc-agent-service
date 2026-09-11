// Package sqlutil holds small MySQL helpers shared by the per-domain store
// subpackages (tenantstore, agentstore, ...). Kept in its own package so each
// store stays focused on its domain SQL instead of re-declaring these.
package sqlutil

import (
	"database/sql"
	"errors"
	"fmt"

	gosql "github.com/go-sql-driver/mysql"
)

// RowScanner is satisfied by both *sql.Row and *sql.Rows.
type RowScanner interface {
	Scan(dest ...any) error
}

// IsDuplicate reports a MySQL duplicate-key error (1062).
func IsDuplicate(err error) bool {
	var my *gosql.MySQLError
	return errors.As(err, &my) && my.Number == 1062
}

// Null converts an empty string to SQL NULL for nullable columns.
func Null(v string) any {
	if v == "" {
		return nil
	}
	return v
}

// RowsAffected maps a write that matched no row to the given not-found
// sentinel. Callers pass their own sentinel so one helper serves every store.
func RowsAffected(res sql.Result, notFound error, id string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: %q", notFound, id)
	}
	return nil
}

// NoRows maps sql.ErrNoRows to the caller's not-found sentinel.
func NoRows(err error, notFound error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return notFound
	}
	return err
}
