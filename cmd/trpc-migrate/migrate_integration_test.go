//go:build integration

package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
)

// migrateBinary builds the command once per test process.
func migrateBinary(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "trpc-migrate")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/trpc-migrate")
	build.Dir = repoRootForMigrate(t)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build trpc-migrate: %v\n%s", err, output)
	}
	return binary
}

func runMigrate(t *testing.T, binary string, env map[string]string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary)
	cmd.Env = os.Environ()
	cmd.Dir = repoRootForMigrate(t)
	for key, value := range env {
		cmd.Env = append(cmd.Env, key+"="+value)
	}
	output, err := cmd.CombinedOutput()
	exit := 0
	if err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if !ok {
			t.Fatalf("run trpc-migrate: %v", err)
		}
		exit = exitErr.ExitCode()
	}
	return exit, string(output)
}

func repoRootForMigrate(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, "../.."))
}

func TestMigrateCommandAgainstRealPostgreSQL(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set; migration command integration is unavailable")
	}
	binary := migrateBinary(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := postgres.NewPool(ctx, postgres.PostgresConfig{URL: dsn, MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatal("PostgreSQL fixture unavailable")
	}
	defer admin.Close()
	schema := fmt.Sprintf("p109_mig_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal("PostgreSQL fixture schema unavailable")
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
	})

	// Fresh database initialization must succeed.
	exit, output := runMigrate(t, binary, map[string]string{
		"DATABASE_URL":    dsn,
		"DATABASE_SCHEMA": schema,
		"MIGRATE_TIMEOUT": "120",
	})
	if exit != exitOK {
		t.Fatalf("fresh init exit=%d output=%s", exit, output)
	}
	if !strings.Contains(output, "current_version=15") {
		t.Fatalf("fresh init did not reach version 15: %s", output)
	}
	if strings.Contains(output, dsn) || strings.Contains(output, "postgres://") {
		t.Fatalf("output leaked the DSN: %s", output)
	}

	// Re-running on a current database is idempotent.
	exit, output = runMigrate(t, binary, map[string]string{
		"DATABASE_URL":    dsn,
		"DATABASE_SCHEMA": schema,
	})
	if exit != exitOK || !strings.Contains(output, "current_version=15") {
		t.Fatalf("idempotent rerun exit=%d output=%s", exit, output)
	}

	// Snapshot the original catalog values so every tamper can be restored
	// exactly, independent of UPDATE subquery snapshot semantics.
	var originalChecksum, originalV1Name, originalV1Checksum string
	if err := admin.QueryRow(ctx, fmt.Sprintf("SELECT checksum FROM %s.schema_migration WHERE version=2", schema)).Scan(&originalChecksum); err != nil {
		t.Fatal(err)
	}
	if err := admin.QueryRow(ctx, fmt.Sprintf("SELECT name, checksum FROM %s.schema_migration WHERE version=1", schema)).Scan(&originalV1Name, &originalV1Checksum); err != nil {
		t.Fatal(err)
	}

	// Tampered checksum must fail closed with the stable category.
	if _, err := admin.Exec(ctx, fmt.Sprintf("UPDATE %s.schema_migration SET checksum='p109-tampered' WHERE version=2", schema)); err != nil {
		t.Fatal(err)
	}
	exit, output = runMigrate(t, binary, map[string]string{
		"DATABASE_URL":    dsn,
		"DATABASE_SCHEMA": schema,
	})
	if exit != exitChecksumMismatch || !strings.Contains(output, "category=checksum_mismatch") {
		t.Fatalf("checksum mismatch exit=%d output=%s", exit, output)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf("UPDATE %s.schema_migration SET checksum=$1 WHERE version=2", schema), originalChecksum); err != nil {
		t.Fatal(err)
	}

	// Unknown future version must fail closed.
	if _, err := admin.Exec(ctx, fmt.Sprintf("INSERT INTO %s.schema_migration (version,name,checksum) VALUES (999,'future','future')", schema)); err != nil {
		t.Fatal(err)
	}
	exit, output = runMigrate(t, binary, map[string]string{
		"DATABASE_URL":    dsn,
		"DATABASE_SCHEMA": schema,
	})
	if exit != exitUnknownVersion || !strings.Contains(output, "category=unknown_version") {
		t.Fatalf("unknown version exit=%d output=%s", exit, output)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf("DELETE FROM %s.schema_migration WHERE version=999", schema)); err != nil {
		t.Fatal(err)
	}

	// Missing recorded version must fail closed.
	if _, err := admin.Exec(ctx, fmt.Sprintf("DELETE FROM %s.schema_migration WHERE version=1", schema)); err != nil {
		t.Fatal(err)
	}
	exit, output = runMigrate(t, binary, map[string]string{
		"DATABASE_URL":    dsn,
		"DATABASE_SCHEMA": schema,
	})
	if exit != exitMissingVersion || !strings.Contains(output, "category=missing_version") {
		t.Fatalf("missing version exit=%d output=%s", exit, output)
	}
	if _, err := admin.Exec(ctx, fmt.Sprintf("INSERT INTO %s.schema_migration (version,name,checksum) VALUES ($1,$2,$3)", schema), int64(1), originalV1Name, originalV1Checksum); err != nil {
		t.Fatal(err)
	}
}

func TestMigrateCommandInvalidConfiguration(t *testing.T) {
	binary := migrateBinary(t)
	exit, output := runMigrate(t, binary, map[string]string{"DATABASE_URL": ""})
	if exit != exitInvalidConfig || !strings.Contains(output, "category=invalid_config") {
		t.Fatalf("missing DSN exit=%d output=%s", exit, output)
	}
	exit, output = runMigrate(t, binary, map[string]string{"DATABASE_URL": "mysql://x/y"})
	if exit != exitInvalidConfig {
		t.Fatalf("wrong scheme exit=%d output=%s", exit, output)
	}
	exit, output = runMigrate(t, binary, map[string]string{
		"DATABASE_URL":    "postgres://127.0.0.1:1/db?sslmode=disable",
		"MIGRATE_TIMEOUT": "1",
	})
	if exit != exitUnavailable {
		t.Fatalf("unreachable exit=%d output=%s", exit, output)
	}
	if strings.Contains(output, "postgres://") {
		t.Fatalf("output leaked the DSN: %s", output)
	}
}
