package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/liuzengh/trpc-agent-service/migrations"
)

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		fmt.Fprintf(os.Stderr, "usage: %s [serve|migrate]\n", os.Args[0])
		return
	}
	if err := runCommand(os.Args[1:], os.Getenv, runService, runMigrations); err != nil {
		fmt.Fprintf(os.Stderr, "trpc-agent-service: %v\n", err)
		os.Exit(1)
	}
}

// runCommand keeps the migration image deliberately small: its only required
// dependency is DATABASE_URL, whereas serve composes the complete runtime.
func runCommand(args []string, getenv environment, serve func() error, migrate func(context.Context, environment) error) error {
	if len(args) == 1 && args[0] == "serve" {
		return serve()
	}
	if len(args) == 1 && args[0] == "migrate" {
		return migrate(context.Background(), getenv)
	}
	if len(args) == 0 {
		return fmt.Errorf("command is required (expected serve or migrate)")
	}
	return fmt.Errorf("unknown command %q (expected serve or migrate)", args[0])
}

func runMigrations(ctx context.Context, getenv environment) error {
	databaseURL, err := required(getenv, "DATABASE_URL")
	if err != nil {
		return err
	}
	database, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open PostgreSQL: %w", err)
	}
	defer func() { _ = database.Close() }()
	if err := database.PingContext(ctx); err != nil {
		return fmt.Errorf("ping PostgreSQL: %w", err)
	}
	if err := migrations.Apply(ctx, database); err != nil {
		return fmt.Errorf("apply database migrations: %w", err)
	}
	return nil
}
