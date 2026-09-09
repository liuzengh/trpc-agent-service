package postgres

import "errors"

var (
	ErrInvalidMigrationSource  = errors.New("postgres: invalid migration source")
	ErrMigrationChecksum       = errors.New("postgres: migration checksum mismatch")
	ErrMigrationVersion        = errors.New("postgres: invalid migration version")
	ErrUnknownMigrationVersion = errors.New("postgres: database contains unknown migration version")
	ErrMigrationMissingVersion = errors.New("postgres: database is missing a required migration version")
	ErrDestructiveDownDisabled = errors.New("postgres: destructive down migration is disabled")
)
