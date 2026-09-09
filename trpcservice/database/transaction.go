package database

import (
	"context"
	"database/sql"
)

type transactionKey struct{}
type scopedTransaction struct {
	db *sql.DB
	tx *sql.Tx
}

func Transaction(ctx context.Context, db *sql.DB) *sql.Tx {
	value, _ := ctx.Value(transactionKey{}).(scopedTransaction)
	if value.db == db {
		return value.tx
	}
	return nil
}
func InTransaction(ctx context.Context, db *sql.DB, fn func(context.Context) error) error {
	if Transaction(ctx, db) != nil {
		return fn(ctx)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(context.WithValue(ctx, transactionKey{}, scopedTransaction{db, tx})); err != nil {
		return err
	}
	return tx.Commit()
}
