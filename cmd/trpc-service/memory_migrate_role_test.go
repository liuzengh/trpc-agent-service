package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestLoadMemoryMigrationConfigRequiresConfirmation(t *testing.T) {
	values := map[string]string{
		"TRPC_POSTGRES_DSN": "postgres://control", "TRPC_REDIS_ADDRESS": "redis:6379",
		"TRPC_MEMORY_MIGRATION_TENANT_ID": "tenant-a", "TRPC_MEMORY_MIGRATION_ID": "memory-a", "TRPC_MEMORY_MIGRATION_WORKER_ID": "worker-a",
	}
	if _, err := loadMemoryMigrationConfig(func(key string) string { return values[key] }); err == nil || !strings.Contains(err.Error(), "CONFIRM") {
		t.Fatalf("missing confirmation error=%v", err)
	}
	values["TRPC_MEMORY_MIGRATION_CONFIRM"] = "true"
	values["TRPC_MEMORY_MIGRATION_TIMEOUT"] = "15m"
	config, err := loadMemoryMigrationConfig(func(key string) string { return values[key] })
	if err != nil || config.Timeout != 15*time.Minute || config.RepairLimit != 100 {
		t.Fatalf("config=%+v err=%v", config, err)
	}
}

func TestRunRoleRecognizesMemoryMigrateBeforeOpeningDependencies(t *testing.T) {
	err := runRole(context.Background(), func(string) string { return "" }, testLogger(), "memory-migrate")
	if err == nil || !strings.Contains(err.Error(), "memory migration configuration rejected") {
		t.Fatalf("error=%v", err)
	}
}
