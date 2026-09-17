package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestLoadSessionMigrationConfigRequiresExplicitConfirmation(t *testing.T) {
	values := map[string]string{
		"TRPC_POSTGRES_DSN":                "postgres://control/service",
		"TRPC_SESSION_MIGRATION_TENANT_ID": "tenant-a", "TRPC_SESSION_MIGRATION_ID": "session-a", "TRPC_SESSION_MIGRATION_WORKER_ID": "worker-a",
	}
	if _, err := loadSessionMigrationConfig(mapEnvironment(values)); err == nil || !strings.Contains(err.Error(), "CONFIRM") {
		t.Fatalf("missing confirmation err=%v", err)
	}
	values["TRPC_SESSION_MIGRATION_CONFIRM"] = "true"
	values["TRPC_SESSION_MIGRATION_BATCH_LIMIT"] = "50"
	values["TRPC_SESSION_MIGRATION_REPAIR_LIMIT"] = "25"
	values["TRPC_SESSION_MIGRATION_TIMEOUT"] = "15m"
	config, err := loadSessionMigrationConfig(mapEnvironment(values))
	if err != nil || config.BatchLimit != 50 || config.RepairLimit != 25 || config.Timeout != 15*time.Minute || config.Connections["default"] != values["TRPC_POSTGRES_DSN"] {
		t.Fatalf("config=%+v err=%v", config, err)
	}
}

func TestLoadSessionMigrationConfigFailsClosedOnUnsafeOperatorInput(t *testing.T) {
	valid := map[string]string{
		"TRPC_POSTGRES_DSN": "postgres://control/service", "TRPC_SESSION_MIGRATION_TENANT_ID": "tenant-a",
		"TRPC_SESSION_MIGRATION_ID": "session-a", "TRPC_SESSION_MIGRATION_WORKER_ID": "worker-a", "TRPC_SESSION_MIGRATION_CONFIRM": "true",
	}
	for _, mutate := range []func(map[string]string){
		func(values map[string]string) { values["TRPC_SESSION_MIGRATION_BATCH_LIMIT"] = "1001" },
		func(values map[string]string) { values["TRPC_SESSION_MIGRATION_REPAIR_LIMIT"] = "0" },
		func(values map[string]string) {
			values["TRPC_SESSION_MIGRATION_LEASE"] = "15m"
			values["TRPC_SESSION_MIGRATION_TIMEOUT"] = "15m"
		},
		func(values map[string]string) { values["TRPC_SESSION_MIGRATION_TIMEOUT"] = "2h1m" },
		func(values map[string]string) { values["TRPC_SESSION_MIGRATION_TENANT_ID"] = "tenant\nforged" },
		func(values map[string]string) {
			values["TRPC_SESSION_POSTGRES_CONNECTIONS"] = `{"default":"postgres://forbidden"}`
		},
	} {
		values := make(map[string]string, len(valid))
		for key, value := range valid {
			values[key] = value
		}
		mutate(values)
		if _, err := loadSessionMigrationConfig(mapEnvironment(values)); err == nil {
			t.Fatalf("unsafe values accepted: %#v", values)
		}
	}
}

func TestRunRoleRecognizesSessionMigrateBeforeOpeningDependencies(t *testing.T) {
	err := runRole(context.Background(), func(string) string { return "" }, testLogger(), "session-migrate")
	if err == nil || !strings.Contains(err.Error(), "session migration configuration rejected") {
		t.Fatalf("err=%v", err)
	}
}
