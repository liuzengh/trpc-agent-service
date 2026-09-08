package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/database"
)

func main() {
	if err := run(); err != nil {
		log.Printf("migration failed: %v", err)
		os.Exit(1)
	}
}

func run() error {
	_, _ = config.LoadDotEnv(".env")
	cfg, err := config.LoadControlPlaneConfigFromEnv()
	if err != nil {
		return err
	}
	if cfg.Backend != config.ControlPlaneBackendPostgres {
		return fmt.Errorf("migration requires PostgreSQL control plane")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, err := database.OpenPostgres(ctx, cfg)
	if err != nil {
		return err
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(db)
	if err := database.Migrate(ctx, db); err != nil {
		return err
	}
	if cfg.BootstrapTutorial {
		if err := controlplane.SeedBootstrap(ctx, db, controlplane.DefaultBootstrapData()); err != nil {
			return err
		}
	}
	log.Printf("database migration completed")
	return nil
}
