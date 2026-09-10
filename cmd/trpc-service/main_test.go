package main

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/bootstrap"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	servicelog "github.com/XnLemon/trpc-agent-service/trpcservice/log"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"go.uber.org/zap"
)

func TestServiceOptionsAndServerDefaults(t *testing.T) {
	options, help, err := parseServiceOptions(nil, io.Discard)
	if err != nil || help || options.address != defaultListenAddress || options.shutdownTimeout != defaultShutdownTimeout {
		t.Fatalf("default options = %+v help=%v err=%v", options, help, err)
	}
	options, help, err = parseServiceOptions([]string{"-h"}, io.Discard)
	if err != nil || !help {
		t.Fatalf("help options = %+v help=%v err=%v", options, help, err)
	}
	options, help, err = parseServiceOptions([]string{"-addr", "127.0.0.1:0", "-shutdown-timeout", "2s"}, io.Discard)
	if err != nil || help || options.address != "127.0.0.1:0" || options.shutdownTimeout != 2*time.Second {
		t.Fatalf("custom options = %+v help=%v err=%v", options, help, err)
	}
	for _, args := range [][]string{{"-addr", ""}, {"-shutdown-timeout", "0"}, {"unexpected"}} {
		if _, _, err := parseServiceOptions(args, io.Discard); err == nil {
			t.Fatalf("invalid options were accepted: %v", args)
		}
	}
	server := newServiceHTTPServer(http.NotFoundHandler(), options)
	if server.Addr != options.address || server.Handler == nil || server.ReadHeaderTimeout <= 0 || server.ReadTimeout <= 0 || server.WriteTimeout <= 0 || server.IdleTimeout <= 0 {
		t.Fatalf("server defaults = %+v", server)
	}
}

func TestRunMainHelpAndSupervisorShutdown(t *testing.T) {
	var output strings.Builder
	if err := runMain(context.Background(), []string{"--help"}, &output, io.Discard, nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "usage:") {
		t.Fatalf("help output = %q", output.String())
	}

	handler, err := gateway.NewHTTPHandler(gateway.HTTPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	serveStarted := make(chan struct{})
	serveRelease := make(chan struct{})
	serve := func() error {
		close(serveStarted)
		<-serveRelease
		return http.ErrServerClosed
	}
	shutdownCalled := make(chan context.Context, 1)
	shutdown := func(ctx context.Context) error {
		shutdownCalled <- ctx
		return nil
	}
	signals := make(chan os.Signal, 1)
	result := make(chan error, 1)
	go func() { result <- runService(context.Background(), signals, handler, time.Second, serve, shutdown) }()
	<-serveStarted
	signals <- syscall.SIGTERM
	select {
	case shutdownContext := <-shutdownCalled:
		if _, ok := shutdownContext.Deadline(); !ok {
			t.Fatal("shutdown context has no deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown was not called")
	}
	if handler.Ready() {
		t.Fatal("handler remained ready after signal shutdown")
	}
	close(serveRelease)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestRunMainStartsAndStopsConfiguredHTTPServer(t *testing.T) {
	var output strings.Builder
	restoreLogger, err := configureLogger(&output)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restoreLogger)

	previousBootstrapRuntime := newBootstrapRuntime
	newBootstrapRuntime = func(context.Context) (*bootstrap.Runtime, error) {
		return bootstrap.NewUnavailable()
	}
	defer func() { newBootstrapRuntime = previousBootstrapRuntime }()

	oldArgs := os.Args
	os.Args = []string{"trpc-service", "--help"}
	main()
	os.Args = oldArgs

	signals := make(chan os.Signal, 1)
	result := make(chan error, 1)
	go func() {
		result <- runMain(context.Background(), []string{"-addr", "127.0.0.1:0", "-shutdown-timeout", "500ms"}, &output, io.Discard, signals)
	}()
	time.Sleep(100 * time.Millisecond)
	signals <- syscall.SIGTERM
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("configured HTTP server did not stop")
	}
	if !strings.Contains(output.String(), "[trpc-service] service listening") || !strings.Contains(output.String(), "127.0.0.1:0") {
		t.Fatalf("server output = %q", output.String())
	}
}

func TestConfigureLoggerUsesConfiguredWriter(t *testing.T) {
	var output strings.Builder
	restoreLogger, err := configureLogger(&output)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restoreLogger)

	packageLog.Info("configured logger")

	if !strings.Contains(output.String(), "[trpc-service] configured logger") {
		t.Fatalf("logger output = %q", output.String())
	}
}

func TestRunMainFailsFastWithoutProductionConfiguration(t *testing.T) {
	t.Setenv("TRPC_POSTGRES_DSN", "")
	var output strings.Builder
	err := runMain(context.Background(), []string{"-addr", "127.0.0.1:0"}, &output, io.Discard, nil)
	if !errors.Is(err, bootstrap.ErrInvalidConfig) {
		t.Fatalf("missing production configuration error = %v", err)
	}
	if strings.Contains(err.Error(), "postgres://") {
		t.Fatalf("configuration error disclosed a DSN: %v", err)
	}
}

func TestMainExitsWhenLoggerConfigurationFails(t *testing.T) {
	previousLogger := newLogger
	previousExit := exitProcess
	previousArgs := os.Args
	t.Cleanup(func() {
		newLogger = previousLogger
		exitProcess = previousExit
		os.Args = previousArgs
	})

	newLogger = func(servicelog.Config) (*zap.Logger, error) {
		return nil, errors.New("logger unavailable")
	}
	exitCode := 0
	exitProcess = func(code int) { exitCode = code }
	os.Args = []string{"trpc-service"}

	main()
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
}

func TestMainExitsWhenServiceCommandFails(t *testing.T) {
	previousBootstrapRuntime := newBootstrapRuntime
	previousExit := exitProcess
	previousArgs := os.Args
	t.Cleanup(func() {
		newBootstrapRuntime = previousBootstrapRuntime
		exitProcess = previousExit
		os.Args = previousArgs
	})

	newBootstrapRuntime = func(context.Context) (*bootstrap.Runtime, error) {
		return nil, errors.New("bootstrap failed")
	}
	exitCode := 0
	exitProcess = func(code int) { exitCode = code }
	os.Args = []string{"trpc-service"}

	main()
	if exitCode != 1 {
		t.Fatalf("exit code = %d, want 1", exitCode)
	}
}

func TestMapInitCommandErrorPreservesCancellation(t *testing.T) {
	for _, expected := range []error{context.Canceled, context.DeadlineExceeded} {
		if got := mapInitCommandError(context.Background(), expected, "database unavailable"); !errors.Is(got, expected) {
			t.Fatalf("mapInitCommandError(%v) = %v, want original cancellation", expected, got)
		}
	}
}

func TestRunServiceContextAndServeErrors(t *testing.T) {
	handler, err := gateway.NewHTTPHandler(gateway.HTTPConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	shutdownCalled := make(chan struct{})
	serve := func() error {
		<-ctx.Done()
		return nil
	}
	shutdown := func(context.Context) error {
		close(shutdownCalled)
		return nil
	}
	result := make(chan error, 1)
	go func() { result <- runService(ctx, nil, handler, time.Second, serve, shutdown) }()
	cancel()
	select {
	case <-shutdownCalled:
	case <-time.After(time.Second):
		t.Fatal("context shutdown was not called")
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}

	serveError := errors.New("bind failed")
	err = runService(context.Background(), nil, handler, time.Second, func() error { return serveError }, func(context.Context) error { return nil })
	if !errors.Is(err, serveError) || !strings.Contains(err.Error(), "bind failed") {
		t.Fatalf("serve error = %v", err)
	}
	if handler.Ready() {
		t.Fatal("handler remained ready after serve failure")
	}
	if err := runService(context.Background(), nil, nil, time.Second, nil, nil); err == nil {
		t.Fatal("invalid supervisor configuration was accepted")
	}
}

func preserveDemoHooks(t *testing.T) {
	t.Helper()
	previousOpen := openInitDatabase
	previousApply := applyInitMigrations
	previousVerify := verifyInitMigrations
	previousInitialize := initializeDemo
	previousWrite := writeDemoResult
	t.Cleanup(func() {
		openInitDatabase = previousOpen
		applyInitMigrations = previousApply
		verifyInitMigrations = previousVerify
		initializeDemo = previousInitialize
		writeDemoResult = previousWrite
	})
}

func TestRunDemoRejectsInvalidInputs(t *testing.T) {
	setValidDemoEnvironment(t)
	preserveDemoHooks(t)

	if err := runMain(context.Background(), []string{"demo", "--unknown"}, io.Discard, io.Discard, nil); err == nil {
		t.Fatal("unknown demo flag was accepted")
	}
	if err := runDemo(context.Background(), []string{"--help"}, &strings.Builder{}, io.Discard, nil); err != nil {
		t.Fatalf("demo help = %v", err)
	}
	if err := runDemo(context.Background(), []string{"--help"}, initErrorWriter{err: errors.New("help output failed")}, io.Discard, nil); err == nil || err.Error() != "help output failed" {
		t.Fatalf("demo help output error = %v", err)
	}
	if err := runDemo(nil, []string{"--confirm"}, io.Discard, io.Discard, nil); !errors.Is(err, bootstrap.ErrInvalidConfig) {
		t.Fatalf("nil demo context = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	openInitDatabase = func(ctx context.Context, _ string, _ postgres.Options) (*sql.DB, error) {
		return nil, ctx.Err()
	}
	if err := runDemo(canceled, []string{"--confirm"}, io.Discard, io.Discard, make(chan os.Signal)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled demo context = %v", err)
	}
}

func TestRunDemoCoversLifecycleAndFailureBranches(t *testing.T) {
	setValidDemoEnvironment(t)
	preserveDemoHooks(t)
	tests := []struct {
		name       string
		open       func(context.Context, string, postgres.Options) (*sql.DB, error)
		apply      func(context.Context, *sql.DB) error
		verify     func(context.Context, *sql.DB) error
		initialize func(context.Context, *sql.DB, bootstrap.DemoConfig) (bootstrap.DemoResult, error)
		write      func(io.Writer, bootstrap.DemoResult) error
		closeError error
		want       error
		wantText   string
	}{
		{
			name: "open error",
			open: func(context.Context, string, postgres.Options) (*sql.DB, error) {
				return nil, errors.New("database credentials")
			},
			want: bootstrap.ErrInvalidConfig,
		},
		{
			name: "nil database",
			open: func(context.Context, string, postgres.Options) (*sql.DB, error) { return nil, nil },
			want: bootstrap.ErrDemoInitialization,
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
		{
			name: "initialize error",
			initialize: func(context.Context, *sql.DB, bootstrap.DemoConfig) (bootstrap.DemoResult, error) {
				return bootstrap.DemoResult{}, errors.New("demo failed")
			},
			wantText: "demo failed",
		},
		{
			name: "close error",
			initialize: func(context.Context, *sql.DB, bootstrap.DemoConfig) (bootstrap.DemoResult, error) {
				return bootstrap.DemoResult{}, nil
			},
			closeError: errors.New("close credentials"),
			want:       bootstrap.ErrDemoInitialization,
		},
		{
			name: "result output",
			initialize: func(context.Context, *sql.DB, bootstrap.DemoConfig) (bootstrap.DemoResult, error) {
				return bootstrap.DemoResult{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", AppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAV", ModelProfileID: "model_01ARZ3NDEKTSV4RRFFQ69G5FAV", BackendProfileID: "backend_01ARZ3NDEKTSV4RRFFQ69G5FAV", Revision: 1}, nil
			},
			write:    func(io.Writer, bootstrap.DemoResult) error { return errors.New("result output failed") },
			wantText: "result output failed",
		},
		{
			name: "success",
			initialize: func(context.Context, *sql.DB, bootstrap.DemoConfig) (bootstrap.DemoResult, error) {
				return bootstrap.DemoResult{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"}, nil
			},
			write: func(writer io.Writer, result bootstrap.DemoResult) error {
				if result.TenantID == "" || writer == nil {
					return errors.New("invalid demo result")
				}
				return nil
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setValidDemoEnvironment(t)
			var db *sql.DB
			var mock sqlmock.Sqlmock
			if test.open == nil {
				var err error
				db, mock, err = sqlmock.New()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
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
			initializeDemo = test.initialize
			if initializeDemo == nil {
				initializeDemo = func(context.Context, *sql.DB, bootstrap.DemoConfig) (bootstrap.DemoResult, error) {
					return bootstrap.DemoResult{}, nil
				}
			}
			writeDemoResult = test.write
			if writeDemoResult == nil {
				writeDemoResult = func(io.Writer, bootstrap.DemoResult) error { return nil }
			}
			if test.closeError != nil {
				mock.ExpectClose().WillReturnError(test.closeError)
			} else if mock != nil {
				mock.ExpectClose()
			}
			err := runDemo(context.Background(), []string{"--confirm"}, io.Discard, io.Discard, nil)
			if test.wantText != "" {
				if err == nil || err.Error() != test.wantText {
					t.Fatalf("demo output error = %v", err)
				}
			} else if !errors.Is(err, test.want) {
				t.Fatalf("demo error = %v, want %v", err, test.want)
			}
			if err != nil && strings.Contains(err.Error(), "credentials") {
				t.Fatalf("demo error disclosed credentials: %v", err)
			}
			if mock != nil {
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestRunDemoCancelsOnSignal(t *testing.T) {
	setValidDemoEnvironment(t)
	previousOpen := openInitDatabase
	t.Cleanup(func() { openInitDatabase = previousOpen })
	signals := make(chan os.Signal, 1)
	openInitDatabase = func(ctx context.Context, _ string, _ postgres.Options) (*sql.DB, error) {
		signals <- syscall.SIGTERM
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err := runDemo(context.Background(), []string{"--confirm"}, io.Discard, io.Discard, signals); !errors.Is(err, context.Canceled) {
		t.Fatalf("signal cancellation error = %v", err)
	}
}

func setValidDemoEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv(bootstrapPostgresDSN, "postgres://demo-user@example.test/db")
	for _, name := range []string{"TRPC_DEMO_TENANT_KEY", "TRPC_DEMO_TENANT_NAME", "TRPC_DEMO_APP_KEY", "TRPC_DEMO_APP_NAME", "TRPC_DEMO_APP_DESCRIPTION", "TRPC_DEMO_MODEL_KEY", "TRPC_DEMO_BACKEND_KEY"} {
		t.Setenv(name, "")
	}
}
