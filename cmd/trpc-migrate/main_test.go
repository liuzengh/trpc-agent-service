package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
)

func TestCategoryExitCodeMapping(t *testing.T) {
	tests := []struct {
		category string
		want     int
	}{
		{"invalid_config", exitInvalidConfig},
		{"unavailable", exitUnavailable},
		{"checksum_mismatch", exitChecksumMismatch},
		{"missing_version", exitMissingVersion},
		{"unknown_version", exitUnknownVersion},
		{"invalid_source", exitInvalidSource},
		{"failed", exitFailed},
		{"timeout", exitFailed},
		{"anything_else", exitFailed},
	}
	for _, tc := range tests {
		if got := categoryExitCode(tc.category); got != tc.want {
			t.Fatalf("category=%s exit=%d want=%d", tc.category, got, tc.want)
		}
	}
}

func TestNewErrorCategoryClassification(t *testing.T) {
	if got := newErrorCategory(nil); got != "invalid_config" {
		t.Fatalf("nil error category=%s", got)
	}
	if got := newErrorCategory(fmt.Errorf("wrap: %w", postgres.ErrInvalidMigrationSource)); got != "invalid_source" {
		t.Fatalf("invalid source category=%s", got)
	}
	if got := newErrorCategory(fmt.Errorf("wrap: %w", postgres.ErrMigrationVersion)); got != "invalid_source" {
		t.Fatalf("invalid version category=%s", got)
	}
	if got := newErrorCategory(errors.New("connection refused")); got != "unavailable" {
		t.Fatalf("connectivity category=%s", got)
	}
}

func TestUpErrorCategoryClassification(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, "failed"},
		{"checksum", fmt.Errorf("wrap: %w", postgres.ErrMigrationChecksum), "checksum_mismatch"},
		{"missing", fmt.Errorf("wrap: %w", postgres.ErrMigrationMissingVersion), "missing_version"},
		{"unknown", fmt.Errorf("wrap: %w", postgres.ErrUnknownMigrationVersion), "unknown_version"},
		{"deadline", fmt.Errorf("wrap: %w", context.DeadlineExceeded), "timeout"},
		{"canceled", fmt.Errorf("wrap: %w", context.Canceled), "timeout"},
		{"other", errors.New("syntax error"), "failed"},
	}
	for _, tc := range tests {
		if got := upErrorCategory(tc.err); got != tc.want {
			t.Fatalf("%s category=%s want=%s", tc.name, got, tc.want)
		}
	}
}

func TestValidPostgresURL(t *testing.T) {
	valid := []string{
		"postgres://u:p@127.0.0.1:5432/db?sslmode=disable",
		"postgresql://trpc_app@postgres:5432/trpc_agent",
	}
	for _, dsn := range valid {
		if !validPostgresURL(dsn) {
			t.Fatalf("expected valid: %s", dsn)
		}
	}
	invalid := []string{
		"",
		"   ",
		"http://127.0.0.1:5432/db",
		"postgres://",
		"postgresql:///db",
		"postgres://u:p@ /db",
		"postgres://%zz@host/db",
	}
	for _, dsn := range invalid {
		if validPostgresURL(dsn) {
			t.Fatalf("expected invalid: %q", dsn)
		}
	}
}

func TestMigrateTimeoutBounds(t *testing.T) {
	t.Setenv("MIGRATE_TIMEOUT", "")
	if timeout, err := migrateTimeout(); err != nil || timeout != defaultMigrateTimeout {
		t.Fatalf("default timeout=%s err=%v", timeout, err)
	}
	t.Setenv("MIGRATE_TIMEOUT", "5")
	if timeout, err := migrateTimeout(); err != nil || timeout != 5*time.Second {
		t.Fatalf("5s timeout=%s err=%v", timeout, err)
	}
	for _, raw := range []string{"abc", "0", "-3", "1801"} {
		t.Setenv("MIGRATE_TIMEOUT", raw)
		if _, err := migrateTimeout(); err == nil {
			t.Fatalf("expected rejection for %q", raw)
		}
	}
}
