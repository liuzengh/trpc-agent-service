package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/cyl6/trpc-agent-service/migrations"
)

func TestRunRequiresDSNEnvironmentReference(t *testing.T) {
	t.Setenv("MIGRATOR_TEST_DSN", "")
	err := run(context.Background(), "MIGRATOR_TEST_DSN")
	if err == nil {
		t.Fatal("run() accepted an empty DSN environment variable")
	}
}

func TestMigrateConfigurationErrorDoesNotLeakDSN(t *testing.T) {
	const secret = "fixture-migration-credential"
	dsn := "postgres://migrator:" + secret + "@%zz/runtime"
	err := migrate(context.Background(), dsn)
	if err == nil {
		t.Fatal("migrate() accepted an invalid DSN")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), dsn) {
		t.Fatalf("migrate() leaked DSN: %v", err)
	}
}

func TestArtifactMetadataMigrationConfigurationErrorDoesNotLeakDSN(t *testing.T) {
	const secret = "fixture-artifact-credential"
	dsn := "postgres://migrator:" + secret + "@%zz/artifacts"
	err := migrateArtifactSchema(context.Background(), dsn)
	if err == nil {
		t.Fatal("artifact migration accepted an invalid DSN")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), dsn) {
		t.Fatalf("artifact migration leaked DSN: %v", err)
	}
}

func TestMigrationFailureCategory(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"timeout":        {err: context.DeadlineExceeded, want: "timeout"},
		"canceled":       {err: context.Canceled, want: "canceled"},
		"drift":          {err: migrations.ErrRuntimePipelineChecksumMismatch, want: "checksum_mismatch"},
		"tool drift":     {err: migrations.ErrToolOperationsChecksumMismatch, want: "checksum_mismatch"},
		"data drift":     {err: migrations.ErrDataMigrationsChecksumMismatch, want: "checksum_mismatch"},
		"artifact drift": {err: migrations.ErrArtifactObjectsChecksumMismatch, want: "checksum_mismatch"},
		"generic":        {err: errors.New("secret provider detail"), want: "migration_failed"},
	} {
		t.Run(name, func(t *testing.T) {
			if got := migrationFailureCategory(tc.err); got != tc.want {
				t.Fatalf("migrationFailureCategory() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestMigrateConfiguredBackendsIsNoopWithoutVectorTenants(t *testing.T) {
	if err := migrateConfiguredBackends(context.Background(), "../../config/example.yaml"); err != nil {
		t.Fatalf("non-vector config backend migration = %v", err)
	}
}
