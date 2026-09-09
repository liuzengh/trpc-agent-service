package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
)

func main() {
	redisAddress := flag.String("redis", "127.0.0.1:6379", "Redis source address or URL")
	sqlitePath := flag.String("sqlite", "data/stage2.db", "SQLite destination path")
	postgresEnv := flag.String("postgres-env", "", "environment variable containing target PostgreSQL DSN")
	tenant := flag.String("tenant", "", "tenant to migrate")
	checkpoint := flag.String("checkpoint", "data/stage2-migration.checkpoint", "resume checkpoint path")
	batch := flag.Int("batch-size", 100, "sessions per checkpoint batch")
	dryRun := flag.Bool("dry-run", false, "compare source and target without writing")
	profiles := flag.String("profiles", "", "server-owned backend profiles JSON")
	profile := flag.String("reindex-profile", "", "rebuild this vector generation; keep writers stopped until profile cutover")
	resumeWrites := flag.Bool("resume-source-writes", false, "explicitly clear a source freeze after recovery or rollback")
	flag.Parse()
	if *tenant == "" {
		fmt.Fprintln(os.Stderr, "error: -tenant is required")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if *profile != "" {
		store, err := platform.OpenBackendProfile(*profiles, *profile)
		if err != nil {
			fatal()
		}
		if closer, ok := store.(interface{ Close() error }); ok {
			defer closer.Close()
		}
		if err := platform.ReindexKnowledge(ctx, store, *tenant); err != nil {
			fatal()
		}
		fmt.Println(`{"status":"completed"}`)
		return
	}
	source := platform.NewRedisStore(*redisAddress)
	defer source.Close()
	if *resumeWrites {
		if err := source.ResumeTenantWrites(ctx, *tenant); err != nil {
			fatal()
		}
		fmt.Println(`{"status":"source_writable"}`)
		return
	}
	var destination *platform.SQLStore
	var err error
	if *postgresEnv != "" {
		dsn := os.Getenv(*postgresEnv)
		if dsn == "" {
			fatal()
		}
		destination, err = platform.NewPostgresStore(dsn)
	} else {
		destination, err = platform.NewSQLiteStore(*sqlitePath)
	}
	if err != nil {
		fatal()
	}
	defer destination.Close()
	report, err := platform.MigrateRedisToSQL(ctx, source, destination, platform.MigrationOptions{TenantID: *tenant, DryRun: *dryRun, BatchSize: *batch, CheckpointPath: *checkpoint})
	_ = json.NewEncoder(os.Stdout).Encode(report)
	if err != nil {
		fatal()
	}
}
func fatal() { fmt.Fprintln(os.Stderr, "error: migration failed"); os.Exit(1) }
