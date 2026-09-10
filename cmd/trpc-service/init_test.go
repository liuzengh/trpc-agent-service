package main

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/bootstrap"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

func TestParseInitOptionsUsesExplicitMetadata(t *testing.T) {
	t.Setenv(envInitTenantKey, "from-environment")
	t.Setenv(envInitTenantName, "Environment Tenant")
	t.Setenv(envInitAppKey, "environment-app")
	t.Setenv(envInitAppName, "Environment App")
	t.Setenv(envInitAppDescription, "description")

	options, help, err := parseInitOptions([]string{"--confirm", "--tenant-name", "Command Tenant"}, io.Discard)
	if err != nil || help || !options.confirm {
		t.Fatalf("init options = %+v help=%v err=%v", options, help, err)
	}
	if options.config.TenantKey != "from-environment" || options.config.TenantDisplayName != "Command Tenant" || options.config.AppKey != "environment-app" || options.config.AppDisplayName != "Environment App" || options.config.AppDescription != "description" {
		t.Fatalf("init metadata = %+v", options.config)
	}
	if _, help, err := parseInitOptions([]string{"--help"}, io.Discard); err != nil || !help {
		t.Fatalf("init help = help:%v err:%v", help, err)
	}
	if _, _, err := parseInitOptions([]string{"--unknown"}, io.Discard); err == nil {
		t.Fatal("unknown init flag was accepted")
	}
	if _, _, err := parseInitOptions([]string{"unexpected"}, io.Discard); err == nil {
		t.Fatal("unexpected init argument was accepted")
	}
}

func TestParseDemoOptionsUsesEnvironmentAndFlags(t *testing.T) {
	t.Setenv("TRPC_DEMO_TENANT_KEY", "from-environment")
	t.Setenv("TRPC_DEMO_APP_KEY", "environment-app")
	t.Setenv("TRPC_DEMO_MODEL_KEY", "environment-model")
	options, help, err := parseDemoOptions([]string{"--confirm", "--tenant-name", "Command Tenant", "--backend-key", "command-backend"}, io.Discard)
	if err != nil || help || !options.confirm {
		t.Fatalf("demo options = %+v help=%v err=%v", options, help, err)
	}
	if options.config.TenantKey != "from-environment" || options.config.TenantDisplayName != "Command Tenant" || options.config.AppKey != "environment-app" || options.config.ModelProfileKey != "environment-model" || options.config.BackendProfileKey != "command-backend" {
		t.Fatalf("demo metadata = %+v", options.config)
	}
	if _, help, err := parseDemoOptions([]string{"--help"}, io.Discard); err != nil || !help {
		t.Fatalf("demo help = help:%v err:%v", help, err)
	}
	if _, _, err := parseDemoOptions([]string{"--unknown"}, io.Discard); err == nil {
		t.Fatal("unknown demo flag was accepted")
	}
	if _, _, err := parseDemoOptions([]string{"unexpected"}, io.Discard); err == nil {
		t.Fatal("unexpected demo argument was accepted")
	}
}

func TestRunDemoRequiresConfirmationAndDatabaseConfiguration(t *testing.T) {
	previousOpen := openInitDatabase
	openInitDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) {
		t.Fatal("database was opened before demo preconditions")
		return nil, nil
	}
	defer func() { openInitDatabase = previousOpen }()
	t.Setenv(bootstrapPostgresDSN, "postgres://demo-user@example.test/db")
	if err := runMain(context.Background(), []string{"demo"}, io.Discard, io.Discard, nil); err == nil || !errors.Is(err, bootstrap.ErrInitializationAuthorization) || !strings.Contains(err.Error(), "--confirm") {
		t.Fatalf("missing demo confirmation error = %v", err)
	}
	t.Setenv(bootstrapPostgresDSN, "")
	if err := runMain(context.Background(), []string{"demo", "--confirm"}, io.Discard, io.Discard, nil); err == nil || !errors.Is(err, bootstrap.ErrInvalidConfig) || !strings.Contains(err.Error(), bootstrapPostgresDSN) {
		t.Fatalf("missing demo DSN error = %v", err)
	}
}

func TestRunInitRequiresConfirmationAndValidConfiguration(t *testing.T) {
	previousOpen := openInitDatabase
	openInitDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) {
		t.Fatal("database was opened before init confirmation/configuration")
		return nil, nil
	}
	defer func() { openInitDatabase = previousOpen }()

	t.Setenv(bootstrapPostgresDSN, "postgres://init-user@example.test/db")
	if err := runMain(context.Background(), []string{"init"}, io.Discard, io.Discard, nil); err == nil || !errors.Is(err, bootstrap.ErrInitializationAuthorization) || !strings.Contains(err.Error(), "--confirm") {
		t.Fatalf("missing confirmation error = %v", err)
	}

	for _, name := range []string{envInitTenantKey, envInitTenantName, envInitAppKey, envInitAppName} {
		t.Setenv(name, "")
	}
	err := runMain(context.Background(), []string{"init", "--confirm"}, io.Discard, io.Discard, nil)
	if !errors.Is(err, bootstrap.ErrInvalidConfig) {
		t.Fatalf("invalid initialization configuration = %v", err)
	}
}

func TestRunInitAppliesAndVerifiesMigrationsAndPrintsOnlyIDs(t *testing.T) {
	t.Setenv(bootstrapPostgresDSN, "postgres://init-user@example.test/db")
	t.Setenv(envInitTenantKey, "acme")
	t.Setenv(envInitTenantName, "Acme")
	t.Setenv(envInitAppKey, "assistant")
	t.Setenv(envInitAppName, "Assistant")
	t.Setenv(envInitAppDescription, "Initial app")
	t.Setenv("TRPC_MODEL_API_KEY", "model-secret")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	previousOpen := openInitDatabase
	previousApply := applyInitMigrations
	previousVerify := verifyInitMigrations
	var applyCalls, verifyCalls int
	openInitDatabase = func(_ context.Context, dsn string, _ postgres.Options) (*sql.DB, error) {
		if dsn != "postgres://init-user@example.test/db" {
			t.Fatalf("init DSN = %q", dsn)
		}
		return db, nil
	}
	applyInitMigrations = func(context.Context, *sql.DB) error {
		applyCalls++
		return nil
	}
	verifyInitMigrations = func(context.Context, *sql.DB) error {
		verifyCalls++
		return nil
	}
	defer func() {
		openInitDatabase = previousOpen
		applyInitMigrations = previousApply
		verifyInitMigrations = previousVerify
	}()

	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT tenant_id FROM public\\.tenant").WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}))
	mock.ExpectQuery("SELECT tenant_id, app_id FROM public\\.agent_app").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_id"}))
	mock.ExpectExec("SELECT public\\.control_plane_create_tenant").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("SELECT public\\.control_plane_create_agent_app").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT 1").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"present"}).AddRow(1))
	mock.ExpectCommit()
	mock.ExpectClose()

	var output strings.Builder
	if err := runMain(context.Background(), []string{"init", "--confirm"}, &output, io.Discard, nil); err != nil {
		t.Fatal(err)
	}
	if applyCalls != 1 || verifyCalls != 1 {
		t.Fatalf("migration calls = apply:%d verify:%d", applyCalls, verifyCalls)
	}
	value := output.String()
	if !strings.Contains(value, "TRPC_TENANT_ID='t_") || !strings.Contains(value, "TRPC_APP_ID='app_") {
		t.Fatalf("init output = %q", value)
	}
	for _, secret := range []string{"postgres://", "password", "model-secret", "TRPC_MODEL_API_KEY"} {
		if strings.Contains(value, secret) {
			t.Fatalf("init output contains %q: %q", secret, value)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRunInitPreservesCancellation(t *testing.T) {
	t.Setenv(bootstrapPostgresDSN, "postgres://init-user@example.test/db")
	t.Setenv(envInitTenantKey, "acme")
	t.Setenv(envInitTenantName, "Acme")
	t.Setenv(envInitAppKey, "assistant")
	t.Setenv(envInitAppName, "Assistant")
	previousOpen := openInitDatabase
	openInitDatabase = func(ctx context.Context, _ string, _ postgres.Options) (*sql.DB, error) {
		return nil, ctx.Err()
	}
	defer func() { openInitDatabase = previousOpen }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runMain(ctx, []string{"init", "--confirm"}, io.Discard, io.Discard, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled init error = %v", err)
	}
}

func TestRunInitHelpAndMissingDatabaseConfiguration(t *testing.T) {
	t.Setenv(bootstrapPostgresDSN, "")
	var output strings.Builder
	if err := runMain(context.Background(), []string{"init", "--help"}, &output, io.Discard, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), initUsage) || !strings.Contains(output.String(), bootstrapPostgresDSN) {
		t.Fatalf("init help output = %q", output.String())
	}
	setValidInitEnvironment(t)
	t.Setenv(bootstrapPostgresDSN, "")
	if err := runMain(context.Background(), []string{"init", "--confirm"}, io.Discard, io.Discard, nil); err == nil || !errors.Is(err, bootstrap.ErrInvalidConfig) || !strings.Contains(err.Error(), bootstrapPostgresDSN) {
		t.Fatalf("missing init DSN error = %v", err)
	}
}

func TestRunInitRedactsDependencyFailures(t *testing.T) {
	setValidInitEnvironment(t)
	tests := []struct {
		name   string
		open   func(context.Context, string, postgres.Options) (*sql.DB, error)
		apply  func(context.Context, *sql.DB) error
		verify func(context.Context, *sql.DB) error
		want   error
	}{
		{
			name: "open",
			open: func(context.Context, string, postgres.Options) (*sql.DB, error) {
				return nil, errors.New("database credentials")
			},
			want: bootstrap.ErrInvalidConfig,
		},
		{
			name: "nil database",
			open: func(context.Context, string, postgres.Options) (*sql.DB, error) { return nil, nil },
			want: bootstrap.ErrInitialization,
		},
		{
			name:  "apply migrations",
			apply: func(context.Context, *sql.DB) error { return errors.New("migration credentials") },
			want:  bootstrap.ErrInvalidConfig,
		},
		{
			name:   "verify migrations",
			apply:  func(context.Context, *sql.DB) error { return nil },
			verify: func(context.Context, *sql.DB) error { return errors.New("verification credentials") },
			want:   bootstrap.ErrInvalidConfig,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			previousOpen := openInitDatabase
			previousApply := applyInitMigrations
			previousVerify := verifyInitMigrations
			defer func() {
				openInitDatabase = previousOpen
				applyInitMigrations = previousApply
				verifyInitMigrations = previousVerify
			}()
			var db *sql.DB
			var mock sqlmock.Sqlmock
			if test.open == nil {
				var err error
				db, mock, err = sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				mock.ExpectClose()
				test.open = func(context.Context, string, postgres.Options) (*sql.DB, error) { return db, nil }
			}
			openInitDatabase = test.open
			applyInitMigrations = test.apply
			if applyInitMigrations == nil {
				applyInitMigrations = func(context.Context, *sql.DB) error { return nil }
			}
			verifyInitMigrations = test.verify
			if verifyInitMigrations == nil {
				verifyInitMigrations = func(context.Context, *sql.DB) error { return nil }
			}
			err := runMain(context.Background(), []string{"init", "--confirm"}, io.Discard, io.Discard, nil)
			if !errors.Is(err, test.want) {
				t.Fatalf("dependency error = %v, want %v", err, test.want)
			}
			for _, secret := range []string{"database credentials", "migration credentials", "verification credentials"} {
				if strings.Contains(err.Error(), secret) {
					t.Fatalf("dependency error disclosed %q: %v", secret, err)
				}
			}
			if mock != nil {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRunInitHandlesInitializationAndOutputFailures(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(sqlmock.Sqlmock)
		closeError error
		output     io.Writer
		want       error
	}{
		{
			name: "initialization",
			setup: func(mock sqlmock.Sqlmock) {
				mock.ExpectBegin()
				mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnError(errors.New("initialization credentials"))
				mock.ExpectRollback()
			},
			want: bootstrap.ErrInitialization,
		},
		{
			name:       "close",
			setup:      expectInitCreation,
			closeError: errors.New("close credentials"),
			want:       bootstrap.ErrInitialization,
		},
		{
			name:   "output",
			setup:  expectInitCreation,
			output: initErrorWriter{err: errors.New("output failure")},
			want:   errors.New("output failure"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setValidInitEnvironment(t)
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			previousOpen := openInitDatabase
			previousApply := applyInitMigrations
			previousVerify := verifyInitMigrations
			openInitDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) { return db, nil }
			applyInitMigrations = func(context.Context, *sql.DB) error { return nil }
			verifyInitMigrations = func(context.Context, *sql.DB) error { return nil }
			defer func() {
				openInitDatabase = previousOpen
				applyInitMigrations = previousApply
				verifyInitMigrations = previousVerify
			}()
			test.setup(mock)
			if test.closeError != nil {
				mock.ExpectClose().WillReturnError(test.closeError)
			} else {
				mock.ExpectClose()
			}
			output := test.output
			if output == nil {
				output = io.Discard
			}
			err = runMain(context.Background(), []string{"init", "--confirm"}, output, io.Discard, nil)
			if test.name == "output" {
				if err == nil || err.Error() != "output failure" {
					t.Fatalf("output error = %v", err)
				}
			} else if !errors.Is(err, test.want) || strings.Contains(err.Error(), "credentials") {
				t.Fatalf("init failure = %v, want %v", err, test.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRunInitCancelsOnSignal(t *testing.T) {
	setValidInitEnvironment(t)
	signals := make(chan os.Signal, 1)
	previousOpen := openInitDatabase
	openInitDatabase = func(ctx context.Context, _ string, _ postgres.Options) (*sql.DB, error) {
		signals <- syscall.SIGTERM
		<-ctx.Done()
		return nil, ctx.Err()
	}
	defer func() { openInitDatabase = previousOpen }()
	if err := runMain(context.Background(), []string{"init", "--confirm"}, io.Discard, io.Discard, signals); !errors.Is(err, context.Canceled) {
		t.Fatalf("signal cancellation error = %v", err)
	}
}

func setValidInitEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv(bootstrapPostgresDSN, "postgres://init-user@example.test/db")
	t.Setenv(envInitTenantKey, "acme")
	t.Setenv(envInitTenantName, "Acme")
	t.Setenv(envInitAppKey, "assistant")
	t.Setenv(envInitAppName, "Assistant")
	t.Setenv(envInitAppDescription, "Initial app")
}

func expectInitCreation(mock sqlmock.Sqlmock) {
	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT tenant_id FROM public\\.tenant").WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}))
	mock.ExpectQuery("SELECT tenant_id, app_id FROM public\\.agent_app").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_id"}))
	mock.ExpectExec("SELECT public\\.control_plane_create_tenant").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("SELECT public\\.control_plane_create_agent_app").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT 1").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"present"}).AddRow(1))
	mock.ExpectCommit()
}

type initErrorWriter struct {
	err error
}

func (w initErrorWriter) Write([]byte) (int, error) { return 0, w.err }
