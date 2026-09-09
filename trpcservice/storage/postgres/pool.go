package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

func NewPool(ctx context.Context, cfg PostgresConfig) (*pgxpool.Pool, error) {
	cfg, err := cfg.withDefaults()
	if err != nil {
		return nil, err
	}
	poolConfig, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("postgres: invalid connection configuration: %w", sanitizeError(err))
	}
	poolConfig.MaxConns = cfg.MaxConns
	poolConfig.MinConns = cfg.MinConns
	poolConfig.MaxConnLifetime = cfg.ConnMaxLifetime
	poolConfig.ConnConfig.ConnectTimeout = cfg.ConnectTimeout
	if cfg.SearchPath != "" {
		poolConfig.ConnConfig.RuntimeParams["search_path"] = cfg.SearchPath
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("postgres: create pool: %w", sanitizeError(err))
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", sanitizeError(err))
	}
	return pool, nil
}

func sanitizeError(err error) error {
	if err == nil {
		return nil
	}
	// pgx may include connection details in parse errors. Keep the public
	// error suitable for logs; the underlying cause remains available to tests
	// through its category, not through credentials or DSN text.
	return fmt.Errorf("%s", redactConnectionText(err.Error()))
}

func redactConnectionText(message string) string {
	for _, prefix := range []string{"postgres://", "postgresql://"} {
		for {
			start := indexOf(message, prefix)
			if start < 0 {
				break
			}
			end := start
			for end < len(message) && message[end] != ' ' && message[end] != '\n' {
				end++
			}
			message = message[:start] + "[REDACTED-DSN]" + message[end:]
		}
	}
	return message
}

func indexOf(value, needle string) int {
	for i := 0; i+len(needle) <= len(value); i++ {
		if value[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
