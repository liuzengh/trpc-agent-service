// Package postgresadapter persists Deployment publication facts in PostgreSQL.
// Every query carries tenant_id so object identifiers never become an
// authorization scope.
package postgresadapter

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
)

// DB is the PostgreSQL surface required by the Deployment adapter.
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

func isConstraint(err error, name string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.ConstraintName == name
}

var _ application.PublicationStore = (*Store)(nil)
var _ application.QueryStore = (*Store)(nil)
