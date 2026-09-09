// Package backend adapts concrete PostgreSQL and Redis clients to the small
// interfaces used by the platform.
package backend

import (
	"context"
	"database/sql"
	"errors"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// OpenPostgres creates and verifies the shared database/sql pool.
func OpenPostgres(ctx context.Context, dsn string) (*sql.DB, error) {
	if ctx == nil || dsn == "" {
		return nil, errors.New("backend: PostgreSQL context and DSN are required")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(32)
	db.SetMaxIdleConns(8)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}
