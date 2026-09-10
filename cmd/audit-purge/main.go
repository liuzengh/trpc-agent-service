package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
)

const (
	postgresDSNEnv        = "TRPC_AGENT_SERVICE_POSTGRES_DSN"
	purgeBatchSizeEnv     = "TRPC_AGENT_SERVICE_AUDIT_PURGE_BATCH_SIZE"
	defaultPurgeBatchSize = 1000
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Print(platformlog.SafeError(err))
		os.Exit(1)
	}
}

func run(parent context.Context) error {
	if parent == nil {
		parent = context.Background()
	}
	dsn := strings.TrimSpace(os.Getenv(postgresDSNEnv))
	if dsn == "" {
		return fmt.Errorf("%s is required", postgresDSNEnv)
	}
	batchSize := defaultPurgeBatchSize
	if raw := strings.TrimSpace(os.Getenv(purgeBatchSizeEnv)); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			return fmt.Errorf("%s must be a positive integer", purgeBatchSizeEnv)
		}
		batchSize = value
	}
	ctx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	store, err := postgres.New(pool)
	if err != nil {
		return err
	}
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate postgres: %w", err)
	}
	deleted, err := store.PurgeExpiredAuditEvents(ctx, batchSize)
	if err != nil {
		return err
	}
	fmt.Printf("deleted_audit_events=%d\n", deleted)
	return nil
}
