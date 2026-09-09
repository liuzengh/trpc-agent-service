package repository

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/migrations"
)

// Direction selects a schema migration direction.
type Direction string

const (
	// DirectionUp creates the control-plane schema.
	DirectionUp Direction = "up"
	// DirectionDown removes the control-plane schema.
	DirectionDown Direction = "down"
)

// MigrationSQL returns the immutable SQL for direction.
func MigrationSQL(direction Direction) (string, error) {
	switch direction {
	case DirectionUp:
		return migrations.Up(), nil
	case DirectionDown:
		return migrations.Down(), nil
	default:
		return "", fmt.Errorf("repository: unsupported migration direction %q", direction)
	}
}

// Migrate executes the idempotent schema migration as one database statement.
func Migrate(ctx context.Context, exec func(context.Context, string) error, direction Direction) error {
	if exec == nil {
		return errors.New("repository: nil migration executor")
	}
	script, err := MigrationSQL(direction)
	if err != nil {
		return err
	}
	if err := exec(ctx, script); err != nil {
		return fmt.Errorf("repository: migrate %s: %w", direction, err)
	}
	return nil
}
