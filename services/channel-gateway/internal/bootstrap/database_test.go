package bootstrap

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestValidateDatabaseTargets(t *testing.T) {
	migration := databaseTarget{database: "agent_service", schema: "gateway", role: "gateway_migrator", sessionRole: "gateway_migrator", schemaOwner: true, schemaCreate: true}
	runtime := databaseTarget{database: "agent_service", schema: "gateway", role: "gateway_runtime", sessionRole: "gateway_runtime"}
	if err := validateDatabaseTargets(migration, runtime, gatewayDatabaseIdentity()); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*databaseTarget, *databaseTarget)
	}{
		{"migration_role_switch", func(m, _ *databaseTarget) { m.sessionRole = "postgres" }},
		{"runtime_role_switch", func(_, r *databaseTarget) { r.sessionRole = "postgres" }},
		{"migration_superuser", func(m, _ *databaseTarget) { m.superuser = true }},
		{"migration_createdb", func(m, _ *databaseTarget) { m.createDB = true }},
		{"migration_createrole", func(m, _ *databaseTarget) { m.createRole = true }},
		{"migration_bypassrls", func(m, _ *databaseTarget) { m.bypassRLS = true }},
		{"migration_replication", func(m, _ *databaseTarget) { m.replication = true }},
		{"migration_nonowner", func(m, _ *databaseTarget) { m.schemaOwner = false }},
		{"migration_no_create", func(m, _ *databaseTarget) { m.schemaCreate = false }},
		{"other_workload", func(m, r *databaseTarget) {
			m.schema, r.schema = "control", "control"
			m.role, r.role = "control_migrator", "control_runtime"
			m.sessionRole, r.sessionRole = m.role, r.role
		}},
		{"wrong_migration_role", func(m, _ *databaseTarget) { m.role = "other_migrator"; m.sessionRole = m.role }},
		{"wrong_runtime_role", func(_, r *databaseTarget) { r.role = "other_runtime"; r.sessionRole = r.role }},
		{"database_mismatch", func(_, r *databaseTarget) { r.database = "other" }},
		{"schema_mismatch", func(_, r *databaseTarget) { r.schema = "other" }},
		{"runtime_public", func(_, r *databaseTarget) { r.schema = "public" }},
		{"migration_public", func(m, _ *databaseTarget) { m.schema = "public" }},
		{"catalog", func(m, r *databaseTarget) { m.schema, r.schema = "pg_catalog", "pg_catalog" }},
		{"temporary", func(m, r *databaseTarget) { m.schema, r.schema = "pg_temp_1", "pg_temp_1" }},
		{"information_schema", func(m, r *databaseTarget) { m.schema, r.schema = "information_schema", "information_schema" }},
		{"absent_schema", func(_, r *databaseTarget) { r.schema = "" }},
		{"same_role", func(m, r *databaseTarget) { r.role = m.role; r.sessionRole = r.role }},
		{"superuser", func(_, r *databaseTarget) { r.superuser = true }},
		{"createdb", func(_, r *databaseTarget) { r.createDB = true }},
		{"createrole", func(_, r *databaseTarget) { r.createRole = true }},
		{"bypassrls", func(_, r *databaseTarget) { r.bypassRLS = true }},
		{"replication", func(_, r *databaseTarget) { r.replication = true }},
		{"schema_owner", func(_, r *databaseTarget) { r.schemaOwner = true }},
		{"schema_create", func(_, r *databaseTarget) { r.schemaCreate = true }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, r := migration, runtime
			tc.mutate(&m, &r)
			if err := validateDatabaseTargets(m, r, gatewayDatabaseIdentity()); err == nil {
				t.Fatal("invalid database target accepted")
			}
		})
	}
}

func TestMigrationDatabaseURLRequiredWithoutFallback(t *testing.T) {
	c := configWithAccounts(t, nil)
	c.MigrationDatabaseURL = ""
	if err := c.Validate(); err == nil {
		t.Fatal("runtime database URL used as implicit migration URL")
	}
	if _, err := New(context.Background(), c); err == nil {
		t.Fatal("startup accepted absent migration database URL")
	}
}

func TestLoadConfigRequiresSeparateMigrationURL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.json")
	if err := os.WriteFile(path, []byte("[]"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GATEWAY_ACCOUNT_SOURCE", "fixture")
	t.Setenv("GATEWAY_DATABASE_URL", "postgres://runtime/db")
	t.Setenv("GATEWAY_MIGRATION_DATABASE_URL", "")
	t.Setenv("GATEWAY_NATS_URL", "nats://unused:4222")
	t.Setenv("GATEWAY_NATS_TOPOLOGY_FILE", "../../../../deploy/nats/streams.yaml")
	t.Setenv("GATEWAY_TELEGRAM_ACCOUNTS_FILE", path)
	t.Setenv("GATEWAY_WECOM_ACCOUNTS_FILE", "")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("loader accepted missing migration URL")
	}
	t.Setenv("GATEWAY_MIGRATION_DATABASE_URL", "postgres://migrator/db")
	c, err := LoadConfig()
	if err != nil || c.MigrationDatabaseURL != "postgres://migrator/db" || c.DatabaseURL != "postgres://runtime/db" {
		t.Fatalf("loader did not retain separate database connections: %v", err)
	}
}

func TestDatabaseConfigurationErrorDoesNotExposeDSN(t *testing.T) {
	_, err := openDatabase(context.Background(), Config{DatabaseURL: "postgres://runtime/db", MigrationDatabaseURL: "postgres://user:dsn-fixture-secret@[invalid"})
	if err == nil || strings.Contains(err.Error(), "dsn-fixture-secret") || strings.Contains(err.Error(), "postgres://") {
		t.Fatalf("expected sanitized connection configuration error, got %v", err)
	}
}

func TestConfiguredDatabaseTargetsMustMatch(t *testing.T) {
	m, err := pgxpool.ParseConfig("postgres://migrator:fixture-secret@db.example:5432/agent_service")
	if err != nil {
		t.Fatal("parse fixture failed")
	}
	for _, tc := range []struct {
		url   string
		valid bool
	}{
		{"postgres://runtime:other-secret@db.example:5432/agent_service", true},
		{"postgres://runtime:other-secret@other.example:5432/agent_service", false},
		{"postgres://runtime:other-secret@db.example:5433/agent_service", false},
		{"postgres://runtime:other-secret@db.example:5432/other_database", false},
	} {
		r, err := pgxpool.ParseConfig(tc.url)
		if err != nil {
			t.Fatal("parse fixture failed")
		}
		err = validateConfiguredDatabaseTargets(m, r)
		if (err == nil) != tc.valid {
			t.Fatalf("valid=%v err=%v", tc.valid, err)
		}
	}
}
