package main

import (
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/migrations"
)

func TestLoadSchemaMigrationConfig(t *testing.T) {
	config, err := loadSchemaMigrationConfig(mapEnvironment(map[string]string{
		"TRPC_POSTGRES_DSN":               "postgres://database/service",
		"TRPC_MIGRATION_EXPECTED_CURRENT": migrations.EmptyVersion,
		"TRPC_MIGRATION_TARGET":           "000001",
		"TRPC_MIGRATION_TIMEOUT":          "15m",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if config.ExpectedCurrent != migrations.EmptyVersion || config.Target != "000001" || config.Timeout != 15*time.Minute {
		t.Fatalf("config=%+v", config)
	}
}

func TestLoadSchemaMigrationConfigFailsClosed(t *testing.T) {
	valid := map[string]string{
		"TRPC_POSTGRES_DSN":               "postgres://database/service",
		"TRPC_MIGRATION_EXPECTED_CURRENT": migrations.EmptyVersion,
		"TRPC_MIGRATION_TARGET":           "000001",
	}
	for _, mutation := range []func(map[string]string){
		func(values map[string]string) { delete(values, "TRPC_POSTGRES_DSN") },
		func(values map[string]string) { values["TRPC_MIGRATION_EXPECTED_CURRENT"] = "latest" },
		func(values map[string]string) { values["TRPC_MIGRATION_TARGET"] = "26" },
		func(values map[string]string) { values["TRPC_MIGRATION_TARGET"] = migrations.EmptyVersion },
		func(values map[string]string) { values["TRPC_MIGRATION_TIMEOUT"] = "2h1m" },
	} {
		values := make(map[string]string, len(valid))
		for key, value := range valid {
			values[key] = value
		}
		mutation(values)
		if _, err := loadSchemaMigrationConfig(mapEnvironment(values)); err == nil {
			t.Fatalf("expected rejection for %v", values)
		}
	}
}

func TestValidateSchemaMigrationTransition(t *testing.T) {
	already, err := validateSchemaMigrationTransition(migrations.Plan{Current: "000001", Target: "000001"}, migrations.EmptyVersion)
	if err != nil || !already {
		t.Fatalf("idempotent retry rejected: already=%v err=%v", already, err)
	}
	already, err = validateSchemaMigrationTransition(migrations.Plan{
		Current: migrations.EmptyVersion, Target: "000001", Pending: []string{"000001"},
	}, migrations.EmptyVersion)
	if err != nil || already {
		t.Fatalf("forward plan rejected: already=%v err=%v", already, err)
	}
	if _, err := validateSchemaMigrationTransition(migrations.Plan{
		Current: "000099", Target: "000001", Pending: []string{"000001"},
	}, migrations.EmptyVersion); err == nil || !strings.Contains(err.Error(), "source mismatch") {
		t.Fatalf("expected source mismatch, got %v", err)
	}
}
