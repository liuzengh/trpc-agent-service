// Package database owns shared SQL connections and schema migrations.
package database

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

// OpenPostgres creates and probes a PostgreSQL connection pool. The caller
// owns the returned pool.
func OpenPostgres(
	ctx context.Context,
	cfg config.ControlPlaneConfig,
) (*sql.DB, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	db, err := sql.Open("pgx", cfg.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("open PostgreSQL: %w", err)
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping PostgreSQL: %w", err)
	}
	return db, nil
}
