package postgres

import (
	"context"
	"strings"
	"testing"
)

func TestCheckTargets(t *testing.T) {
	valid := Target{Database: "platform", Schema: "control", Role: "control_migrator", SchemaOwner: true, SchemaCreate: true}
	runtime := Target{Database: "platform", Schema: "control", Role: "control_runtime"}
	if err := CheckTargets(valid, runtime); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		target Target
	}{
		{"database mismatch", Target{Database: "other", Schema: "control", Role: "control_runtime"}},
		{"schema mismatch", Target{Database: "platform", Schema: "gateway", Role: "control_runtime"}},
		{"same role", valid},
		{"public", Target{Database: "platform", Schema: "public", Role: "control_runtime"}},
		{"catalog", Target{Database: "platform", Schema: "pg_catalog", Role: "control_runtime"}},
		{"temporary", Target{Database: "platform", Schema: "pg_temp_3", Role: "control_runtime"}},
		{"information schema", Target{Database: "platform", Schema: "information_schema", Role: "control_runtime"}},
		{"missing schema", Target{Database: "platform", Role: "control_runtime"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if CheckTargets(valid, tc.target) == nil {
				t.Fatal("invalid target accepted")
			}
		})
	}
}

func TestOpenDoesNotExposeDSN(t *testing.T) {
	const secret = "private-dsn-canary"
	_, err := Open(context.Background(), "postgres://user:"+secret+"@invalid:%zz/control")
	if err == nil || strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "postgres://") {
		t.Fatal("expected redacted configuration failure")
	}
	_, err = Open(context.Background(), " ")
	if err == nil {
		t.Fatal("empty URL must not use implicit environment defaults")
	}
}

func TestCheckTargetsRejectsPrivilegedRuntime(t *testing.T) {
	migration := Target{Database: "platform", Schema: "control", Role: "control_migrator", SchemaOwner: true, SchemaCreate: true}
	for _, tc := range []struct {
		name  string
		apply func(*Target)
	}{
		{"superuser", func(t *Target) { t.Superuser = true }},
		{"create database", func(t *Target) { t.CreateDatabase = true }},
		{"create role", func(t *Target) { t.CreateRole = true }},
		{"replication", func(t *Target) { t.Replication = true }},
		{"bypass RLS", func(t *Target) { t.BypassRLS = true }},
		{"schema owner", func(t *Target) { t.SchemaOwner = true }},
		{"schema create", func(t *Target) { t.SchemaCreate = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runtime := Target{Database: "platform", Schema: "control", Role: "control_runtime"}
			tc.apply(&runtime)
			if CheckTargets(migration, runtime) == nil {
				t.Fatal("privileged runtime role accepted")
			}
		})
	}
}

func TestCheckConnectionURLs(t *testing.T) {
	const migration = "postgres://migrator:private-migration-canary@localhost:5432/platform"
	if err := CheckConnectionURLs(migration, "postgres://runtime:private-runtime-canary@localhost:5432/platform"); err != nil {
		t.Fatal(err)
	}
	for _, runtime := range []string{
		"postgres://runtime:private-runtime-canary@elsewhere:5432/platform",
		"postgres://runtime:private-runtime-canary@localhost:5433/platform",
		"postgres://runtime:private-runtime-canary@localhost:5432/other",
		"postgres://runtime:private-runtime-canary@localhost:%zz/platform",
	} {
		err := CheckConnectionURLs(migration, runtime)
		if err == nil || strings.Contains(err.Error(), "canary") {
			t.Fatal("expected sanitized connection-target rejection")
		}
	}
}

func TestCheckTargetsRejectsInvalidMigrator(t *testing.T) {
	runtime := Target{Database: "platform", Schema: "control", Role: "control_runtime"}
	for _, tc := range []struct {
		name  string
		apply func(*Target)
	}{
		{"superuser", func(t *Target) { t.Superuser = true }},
		{"create database", func(t *Target) { t.CreateDatabase = true }},
		{"create role", func(t *Target) { t.CreateRole = true }},
		{"replication", func(t *Target) { t.Replication = true }},
		{"bypass RLS", func(t *Target) { t.BypassRLS = true }},
		{"not schema owner", func(t *Target) { t.SchemaOwner = false }},
		{"no schema create", func(t *Target) { t.SchemaCreate = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			migration := Target{Database: "platform", Schema: "control", Role: "control_migrator", SchemaOwner: true, SchemaCreate: true}
			tc.apply(&migration)
			if CheckTargets(migration, runtime) == nil {
				t.Fatal("invalid migrator accepted")
			}
		})
	}
}

func TestDirectRoleIdentity(t *testing.T) {
	if !directRoleIdentity("control_runtime", "control_runtime") {
		t.Fatal("direct runtime login rejected")
	}
	if !directRoleIdentity("control_migrator", "control_migrator") {
		t.Fatal("direct migrator login rejected")
	}
	for _, pair := range [][2]string{{"v1_admin", "control_runtime"}, {"v1_admin", "control_migrator"}, {"control_migrator", "control_runtime"}, {"", ""}} {
		if directRoleIdentity(pair[0], pair[1]) {
			t.Fatal("assumed-role connection accepted")
		}
	}
}
