// Package postgresadapter persists Agent aggregates in PostgreSQL. Every query
// carries tenant_id so object identifiers never become an authorization scope.
package postgresadapter

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DB is the PostgreSQL surface required by the Agent adapter.
type DB interface {
	Begin(context.Context) (pgx.Tx, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

type Store struct {
	db DB
}

func NewStore(db DB) *Store {
	return &Store{db: db}
}

type queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}
