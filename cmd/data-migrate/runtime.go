package main

import (
	"context"
	"errors"
	"io"
	"os"

	_ "github.com/lib/pq"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/migrationruntime"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/runtimeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
)

func runCommand(ctx context.Context, options commandOptions, output io.Writer) error {
	dsn := os.Getenv("DATABASE_URL")
	if err := storage.ValidateServicePostgresURL(dsn, os.Getenv("DATABASE_ALLOW_INSECURE") == "true"); err != nil {
		return errors.New("invalid DATABASE_URL")
	}
	profiles, err := storage.LoadBackendProfiles(os.Getenv("STORAGE_BACKEND_PROFILES"), os.LookupEnv)
	if err != nil {
		return errors.New("invalid storage profiles")
	}
	manifest := os.Getenv("DATA_PLANE_PROFILES")
	if manifest == "" {
		manifest = "[]"
	}
	dataProfiles, err := runtimeplane.LoadProfiles(manifest, os.LookupEnv)
	if err != nil {
		return errors.New("invalid data-plane profiles")
	}
	budget, err := controlplane.ParseRuntimeDatabaseBudget(os.Getenv("CONTROL_DB_MAX_CONNECTIONS"), 10)
	if err != nil {
		return err
	}
	databases, err := controlplane.OpenRuntimeDatabases(ctx, dsn, controlplane.RuntimeDatabaseOptions{MaxConnections: budget})
	if err != nil {
		return err
	}
	defer databases.Close()
	runtime, err := migrationruntime.New(migrationruntime.Options{
		DB: databases.Control, GateDB: databases.MigrationGate, StorageProfiles: profiles, DataPlaneProfiles: dataProfiles,
		Owner: options.owner, LeaseTTL: options.lease, BatchSize: options.batchSize,
	})
	if err != nil {
		return err
	}
	defer runtime.Close()
	return execute(ctx, runtime.Coordinator, options, output)
}
