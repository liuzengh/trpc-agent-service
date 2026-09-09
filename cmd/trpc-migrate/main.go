// Command trpc-migrate is the P1-09 standalone migration lifecycle step.
//
// It applies pending PostgreSQL migrations using the same migration runner,
// checksum validation and lock semantics as the service startup gate, then
// exits. It never starts business workers, dispatchers, vector workers or
// telemetry exporters, and it never prints DSNs, passwords or raw database
// errors: every failure is reduced to one stable redacted category on stderr
// and a stable process exit code so deployment tooling can branch on it.
//
// Environment:
//
//	DATABASE_URL     required postgres:// or postgresql:// URL.
//	DATABASE_SCHEMA  optional search path (default: server default).
//	MIGRATIONS_DIR   optional migration directory (default: migrations).
//	MIGRATE_TIMEOUT  optional bounded wall clock budget (default 2m, 1s..30m).
//
// Exit codes:
//
//	0 migrations applied or already current
//	2 invalid configuration
//	3 dependency unavailable
//	4 migration checksum mismatch
//	5 required migration version missing from the database
//	6 database contains an unknown future migration version
//	7 invalid migration source
//	8 migration execution failure, cancellation or deadline exceeded
package main

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
)

const (
	exitOK                = 0
	exitInvalidConfig     = 2
	exitUnavailable       = 3
	exitChecksumMismatch  = 4
	exitMissingVersion    = 5
	exitUnknownVersion    = 6
	exitInvalidSource     = 7
	exitFailed            = 8
	defaultMigrateTimeout = 2 * time.Minute
	minimumMigrateTimeout = time.Second
	maximumMigrateTimeout = 30 * time.Minute
	defaultMigrationsDir  = "migrations"
)

func main() {
	os.Exit(run())
}

func run() int {
	timeout, err := migrateTimeout()
	if err != nil {
		return fail("invalid_config")
	}
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if !validPostgresURL(dsn) {
		return fail("invalid_config")
	}
	migrationsDir := strings.TrimSpace(os.Getenv("MIGRATIONS_DIR"))
	if migrationsDir == "" {
		migrationsDir = defaultMigrationsDir
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	migrator, err := postgres.NewMigrator(ctx, postgres.PostgresConfig{
		URL:        dsn,
		SearchPath: strings.TrimSpace(os.Getenv("DATABASE_SCHEMA")),
	}, os.DirFS(migrationsDir))
	if migrator != nil {
		defer migrator.Close()
	}
	if err != nil {
		return fail(newErrorCategory(err))
	}
	if err := migrator.Up(ctx); err != nil {
		return fail(upErrorCategory(err))
	}
	current, err := migrator.Current(ctx)
	if err != nil {
		return fail("failed")
	}
	if role := strings.TrimSpace(os.Getenv("RUNTIME_ROLE")); role != "" {
		if err := ensureRuntimeRole(ctx, role); err != nil {
			return fail("invalid_config")
		}
	}
	fmt.Fprintf(os.Stderr, "migration ok current_version=%d\n", current.Version)
	return exitOK
}

// ensureRuntimeRole provisions the deployment's NOBYPASSRLS runtime role and
// grants its bounded privileges after the migrations created the tables. The
// password comes from RUNTIME_PASSWORD_FILE and never appears in argv, logs
// or errors.
func ensureRuntimeRole(ctx context.Context, role string) error {
	password := strings.TrimSpace(os.Getenv("RUNTIME_PASSWORD"))
	if password == "" {
		passwordFile := strings.TrimSpace(os.Getenv("RUNTIME_PASSWORD_FILE"))
		if passwordFile == "" {
			return errors.New("runtime role provisioning requires RUNTIME_PASSWORD_FILE")
		}
		raw, readErr := os.ReadFile(passwordFile)
		if readErr != nil || strings.TrimSpace(string(raw)) == "" {
			return errors.New("runtime role provisioning requires a non-empty runtime password")
		}
		password = strings.TrimSpace(string(raw))
	}
	schema := strings.TrimSpace(os.Getenv("DATABASE_SCHEMA"))
	if schema == "" {
		schema = "public"
	}
	admin, err := postgres.NewPool(ctx, postgres.PostgresConfig{URL: strings.TrimSpace(os.Getenv("DATABASE_URL")), SearchPath: schema, MaxConns: 2, MinConns: 1})
	if err != nil {
		return err
	}
	defer admin.Close()
	if err := postgres.EnsureTenantRuntimeRole(ctx, admin, schema, role, password); err != nil {
		return err
	}
	return postgres.GrantRuntimeSchemaPrivileges(ctx, admin, schema, role)
}

// fail writes one stable redacted category and returns the mapped exit code.
// The database URL, credentials and raw backend errors are never emitted.
func fail(category string) int {
	fmt.Fprintf(os.Stderr, "migration failed category=%s\n", category)
	return categoryExitCode(category)
}

func categoryExitCode(category string) int {
	switch category {
	case "invalid_config":
		return exitInvalidConfig
	case "unavailable":
		return exitUnavailable
	case "checksum_mismatch":
		return exitChecksumMismatch
	case "missing_version":
		return exitMissingVersion
	case "unknown_version":
		return exitUnknownVersion
	case "invalid_source":
		return exitInvalidSource
	default:
		return exitFailed
	}
}

// newErrorCategory classifies migrator construction failures. Connectivity
// problems are "unavailable"; source layout problems are "invalid_source";
// everything else fails as "invalid_config" without leaking details.
func newErrorCategory(err error) string {
	switch {
	case err == nil:
		return "invalid_config"
	case errors.Is(err, postgres.ErrInvalidMigrationSource), errors.Is(err, postgres.ErrMigrationVersion):
		return "invalid_source"
	default:
		return "unavailable"
	}
}

// upErrorCategory classifies migration execution failures.
func upErrorCategory(err error) string {
	switch {
	case err == nil:
		return "failed"
	case errors.Is(err, postgres.ErrMigrationChecksum):
		return "checksum_mismatch"
	case errors.Is(err, postgres.ErrMigrationMissingVersion):
		return "missing_version"
	case errors.Is(err, postgres.ErrUnknownMigrationVersion):
		return "unknown_version"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return "timeout"
	default:
		return "failed"
	}
}

// validPostgresURL accepts only non-empty postgres/postgresql URLs with a
// host. The value is never echoed back on rejection.
func validPostgresURL(raw string) bool {
	if raw == "" {
		return false
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return false
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" {
		return false
	}
	return strings.TrimSpace(parsed.Host) != ""
}

func migrateTimeout() (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv("MIGRATE_TIMEOUT"))
	if raw == "" {
		return defaultMigrateTimeout, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("invalid MIGRATE_TIMEOUT")
	}
	timeout := time.Duration(value) * time.Second
	if timeout < minimumMigrateTimeout || timeout > maximumMigrateTimeout {
		return 0, fmt.Errorf("MIGRATE_TIMEOUT out of bounds")
	}
	return timeout, nil
}
