package postgres

import (
	"context"
	"database/sql"
	"errors"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	pgstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

func mapError(ctx context.Context, err error, notFound, duplicate, conflict, invalid error) error {
	mapped := pgstorage.MapError(ctx, err, notFound, duplicate, conflict, invalid)
	if errors.Is(mapped, pgstorage.ErrStorage) {
		return runtimestorage.ErrStorage
	}
	return mapped
}

func begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	tx, err := pgstorage.Begin(ctx, db)
	if errors.Is(err, pgstorage.ErrStorage) {
		return nil, runtimestorage.ErrStorage
	}
	return tx, err
}

func commit(ctx context.Context, tx *sql.Tx) error {
	err := pgstorage.Commit(ctx, tx)
	if errors.Is(err, pgstorage.ErrStorage) {
		return runtimestorage.ErrStorage
	}
	return err
}
