package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/migrations"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
)

const (
	databasePingTimeout     = 10 * time.Second
	defaultMigrationTimeout = 30 * time.Minute
)

func main() {
	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL is required")
	}
	if err := storage.ValidateServicePostgresURL(dsn, os.Getenv("DATABASE_ALLOW_INSECURE") == "true"); err != nil {
		log.Fatal("invalid DATABASE_URL")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("open database: error=%s", telemetry.StableErrorCode(err))
	}
	defer db.Close()

	pingCtx, cancelPing := context.WithTimeout(rootCtx, databasePingTimeout)
	err = db.PingContext(pingCtx)
	cancelPing()
	if err != nil {
		log.Fatalf("ping database: error=%s", telemetry.StableErrorCode(err))
	}

	timeout, err := parseMigrationTimeout(os.Getenv("MIGRATION_TIMEOUT"))
	if err != nil {
		log.Fatalf("migration configuration failed: error=%s", telemetry.StableErrorCode(err))
	}
	ctx, cancel := operationContext(rootCtx, timeout)
	defer cancel()

	runner := migrations.NewRunner(db)
	command := "up"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	switch command {
	case "up":
		if err := runner.Up(ctx); err != nil {
			log.Fatalf("migration failed: error=%s", telemetry.StableErrorCode(err))
		}
		log.Println("migrations applied")
	case "down":
		if os.Getenv("ALLOW_DESTRUCTIVE_MIGRATION") != "true" {
			log.Fatal("down is destructive; set ALLOW_DESTRUCTIVE_MIGRATION=true for this invocation")
		}
		steps := 1
		if len(os.Args) > 2 {
			parsed, err := strconv.Atoi(os.Args[2])
			if err != nil || parsed <= 0 {
				log.Fatal("down steps must be a positive integer")
			}
			steps = parsed
		}
		if err := runner.Down(ctx, steps); err != nil {
			log.Fatalf("migration rollback failed: error=%s", telemetry.StableErrorCode(err))
		}
		log.Printf("rolled back %d migration(s)", steps)
	case "status":
		status, err := runner.Status(ctx)
		if err != nil {
			log.Fatalf("migration status failed: error=%s", telemetry.StableErrorCode(err))
		}
		for _, item := range status {
			fmt.Printf("%s\t%s\n", item.Version, item.AppliedAt.Format(time.RFC3339))
		}
	default:
		log.Fatalf("unknown command %q; use up, down [steps], or status", command)
	}
}

func parseMigrationTimeout(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return defaultMigrationTimeout, nil
	}
	timeout, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("MIGRATION_TIMEOUT must be a Go duration or 0: %w", err)
	}
	if timeout < 0 {
		return 0, fmt.Errorf("MIGRATION_TIMEOUT must not be negative")
	}
	return timeout, nil
}

func operationContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout == 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}
