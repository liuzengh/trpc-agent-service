package postgres

import (
	"fmt"
	"time"
)

// PostgresConfig contains connection-pool and migration-lock settings.
// URL is supplied by the caller and is never included in returned errors.
type PostgresConfig struct {
	URL                  string
	MaxConns             int32
	MinConns             int32
	ConnMaxLifetime      time.Duration
	ConnectTimeout       time.Duration
	MigrationLockKey     int64
	LockTimeout          time.Duration
	StatementTimeout     time.Duration
	SearchPath           string
	AllowDestructiveDown bool
}

func (c PostgresConfig) withDefaults() (PostgresConfig, error) {
	if c.URL == "" {
		return PostgresConfig{}, fmt.Errorf("postgres: URL is required")
	}
	if c.MaxConns == 0 {
		c.MaxConns = 4
	}
	if c.MinConns == 0 {
		c.MinConns = 1
	}
	if c.MaxConns < 1 || c.MinConns < 0 || c.MinConns > c.MaxConns {
		return PostgresConfig{}, fmt.Errorf("postgres: invalid connection pool limits")
	}
	if c.ConnMaxLifetime < 0 || c.ConnectTimeout < 0 {
		return PostgresConfig{}, fmt.Errorf("postgres: durations cannot be negative")
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 10 * time.Second
	}
	if c.MigrationLockKey == 0 {
		c.MigrationLockKey = 0x747270632d703005
	}
	if c.LockTimeout == 0 {
		c.LockTimeout = 30 * time.Second
	}
	if c.StatementTimeout == 0 {
		c.StatementTimeout = 10 * time.Minute
	}
	if c.LockTimeout < 0 || c.StatementTimeout < 0 {
		return PostgresConfig{}, fmt.Errorf("postgres: migration timeouts cannot be negative")
	}
	return c, nil
}
