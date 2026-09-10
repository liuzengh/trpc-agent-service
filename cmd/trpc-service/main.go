// Package main starts the tRPC Agent service.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/XnLemon/trpc-agent-service/migrations"
	"github.com/XnLemon/trpc-agent-service/trpcservice"
	"github.com/XnLemon/trpc-agent-service/trpcservice/bootstrap"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"go.uber.org/zap"
)

const (
	defaultListenAddress     = "127.0.0.1:8080"
	defaultShutdownTimeout   = 10 * time.Second
	defaultReadHeaderTimeout = 5 * time.Second
	defaultReadTimeout       = 30 * time.Second
	defaultWriteTimeout      = 30 * time.Second
	defaultIdleTimeout       = 60 * time.Second
	initUsage                = "usage: trpc-service init --confirm [-tenant-key key] [-tenant-name name] [-app-key key] [-app-name name] [-app-description text]"
	demoUsage                = "usage: trpc-service demo --confirm [-tenant-key key] [-tenant-name name] [-app-key key] [-app-name name] [-model-key key] [-backend-key key]"
	bootstrapPostgresDSN     = "TRPC_POSTGRES_DSN"

	envInitTenantKey      = "TRPC_INIT_TENANT_KEY"
	envInitTenantName     = "TRPC_INIT_TENANT_NAME"
	envInitAppKey         = "TRPC_INIT_APP_KEY"
	envInitAppName        = "TRPC_INIT_APP_NAME"
	envInitAppDescription = "TRPC_INIT_APP_DESCRIPTION"
)

type serviceOptions struct {
	address         string
	shutdownTimeout time.Duration
}

type initOptions struct {
	confirm bool
	help    bool
	config  bootstrap.InitConfig
}

type demoOptions struct {
	confirm bool
	help    bool
	config  bootstrap.DemoConfig
}

var (
	openInitDatabase     = postgres.Open
	applyInitMigrations  = migrations.Apply
	verifyInitMigrations = migrations.Verify
	initializeDemo       = bootstrap.InitializeDemo
	writeDemoResult      = bootstrap.WriteDemoResult
)

var newBootstrapRuntime = bootstrap.NewFromEnvironment

var exitProcess = os.Exit

func main() {
	restoreLogger, err := configureLogger(os.Stderr)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		exitProcess(1)
		return
	}
	defer restoreLogger()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(signals)
	if err := runMain(context.Background(), os.Args[1:], os.Stdout, os.Stderr, signals); err != nil {
		packageLog.Error("service command failed", zap.Error(err))
		exitProcess(1)
	}
}

func runMain(ctx context.Context, args []string, stdout, stderr io.Writer, signals <-chan os.Signal) error {
	if len(args) > 0 && args[0] == "init" {
		return runInit(ctx, args[1:], stdout, stderr, signals)
	}
	if len(args) > 0 && args[0] == "demo" {
		return runDemo(ctx, args[1:], stdout, stderr, signals)
	}
	options, help, err := parseServiceOptions(args, stderr)
	if err != nil {
		return err
	}
	if help {
		_, _ = fmt.Fprintf(stdout, "trpc-agent-service %s\nusage: trpc-service [-addr address] [-shutdown-timeout duration]\n       trpc-service init --confirm [options]\n       trpc-service demo --confirm [options]\n", trpcservice.Version)
		return nil
	}
	bootstrapRuntime, err := newBootstrapRuntime(ctx)
	if err != nil {
		return err
	}
	handler := bootstrapRuntime.HandlerValue()
	server := newServiceHTTPServer(handler.Handler(), options)
	packageLog.Info("service listening", zap.String("version", trpcservice.Version), zap.String("address", options.address))
	returnErr := runService(ctx, signals, handler, options.shutdownTimeout, server.ListenAndServe, server.Shutdown)
	return errors.Join(returnErr, bootstrapRuntime.Close())
}

func runInit(ctx context.Context, args []string, stdout, stderr io.Writer, signals <-chan os.Signal) error {
	options, help, err := parseInitOptions(args, stderr)
	if err != nil {
		return err
	}
	if help {
		_, err := fmt.Fprintln(stdout, initUsage+"\n\nThe database DSN is read from TRPC_POSTGRES_DSN. Metadata may also be supplied with TRPC_INIT_TENANT_KEY, TRPC_INIT_TENANT_NAME, TRPC_INIT_APP_KEY, TRPC_INIT_APP_NAME, and TRPC_INIT_APP_DESCRIPTION.")
		return err
	}
	if !options.confirm {
		return fmt.Errorf("%w: use --confirm", bootstrap.ErrInitializationAuthorization)
	}
	dsn, config, err := loadInitEnvironment(options.config)
	if err != nil {
		return err
	}
	if ctx == nil {
		return bootstrap.ErrInvalidConfig
	}
	initContext := ctx
	if signals != nil {
		var cancel context.CancelFunc
		initContext, cancel = context.WithCancel(ctx)
		defer cancel()
		go func() {
			select {
			case <-signals:
				cancel()
			case <-initContext.Done():
			}
		}()
	}
	db, err := openInitDatabase(initContext, dsn, postgres.Options{MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		return mapInitCommandError(initContext, err, "PostgreSQL is unavailable")
	}
	if db == nil {
		return bootstrap.ErrInitialization
	}
	applyErr := applyInitMigrations(initContext, db)
	if applyErr == nil {
		applyErr = verifyInitMigrations(initContext, db)
	}
	if applyErr != nil {
		_ = db.Close()
		return mapInitCommandError(initContext, applyErr, "database migrations are not ready")
	}
	result, initErr := bootstrap.Initialize(initContext, db, config)
	closeErr := db.Close()
	if initErr != nil {
		return initErr
	}
	if closeErr != nil {
		return fmt.Errorf("%w: database close failed", bootstrap.ErrInitialization)
	}
	return bootstrap.WriteInitResult(stdout, result)
}

func runDemo(ctx context.Context, args []string, stdout, stderr io.Writer, signals <-chan os.Signal) error {
	options, help, err := parseDemoOptions(args, stderr)
	if err != nil {
		return err
	}
	if help {
		_, err := fmt.Fprintln(stdout, demoUsage+"\n\nThe database DSN is read from TRPC_POSTGRES_DSN. Demo metadata may also be supplied with TRPC_DEMO_TENANT_KEY, TRPC_DEMO_TENANT_NAME, TRPC_DEMO_APP_KEY, TRPC_DEMO_APP_NAME, TRPC_DEMO_APP_DESCRIPTION, TRPC_DEMO_MODEL_KEY, and TRPC_DEMO_BACKEND_KEY.")
		return err
	}
	if !options.confirm {
		return fmt.Errorf("%w: use --confirm", bootstrap.ErrInitializationAuthorization)
	}
	dsn := strings.TrimSpace(os.Getenv(bootstrapPostgresDSN))
	if dsn == "" {
		return fmt.Errorf("%w: %s is required", bootstrap.ErrInvalidConfig, bootstrapPostgresDSN)
	}
	if ctx == nil {
		return bootstrap.ErrInvalidConfig
	}
	demoContext := ctx
	if signals != nil {
		var cancel context.CancelFunc
		demoContext, cancel = context.WithCancel(ctx)
		defer cancel()
		go func() {
			select {
			case <-signals:
				cancel()
			case <-demoContext.Done():
			}
		}()
	}
	db, err := openInitDatabase(demoContext, dsn, postgres.Options{MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		return mapInitCommandError(demoContext, err, "PostgreSQL is unavailable")
	}
	if db == nil {
		return bootstrap.ErrDemoInitialization
	}
	applyErr := applyInitMigrations(demoContext, db)
	if applyErr == nil {
		applyErr = verifyInitMigrations(demoContext, db)
	}
	if applyErr != nil {
		_ = db.Close()
		return mapInitCommandError(demoContext, applyErr, "database migrations are not ready")
	}
	result, demoErr := initializeDemo(demoContext, db, options.config)
	closeErr := db.Close()
	if demoErr != nil {
		return demoErr
	}
	if closeErr != nil {
		return fmt.Errorf("%w: database close failed", bootstrap.ErrDemoInitialization)
	}
	return writeDemoResult(stdout, result)
}

func parseInitOptions(args []string, stderr io.Writer) (initOptions, bool, error) {
	options := initOptions{config: bootstrap.InitConfig{
		TenantKey:         strings.TrimSpace(os.Getenv(envInitTenantKey)),
		TenantDisplayName: strings.TrimSpace(os.Getenv(envInitTenantName)),
		AppKey:            strings.TrimSpace(os.Getenv(envInitAppKey)),
		AppDisplayName:    strings.TrimSpace(os.Getenv(envInitAppName)),
		AppDescription:    strings.TrimSpace(os.Getenv(envInitAppDescription)),
	}}
	flags := flag.NewFlagSet("trpc-service init", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.BoolVar(&options.help, "help", false, "show help")
	flags.BoolVar(&options.help, "h", false, "show help")
	flags.BoolVar(&options.confirm, "confirm", false, "authorize first-run initialization")
	flags.StringVar(&options.config.TenantKey, "tenant-key", options.config.TenantKey, "initial tenant key")
	flags.StringVar(&options.config.TenantDisplayName, "tenant-name", options.config.TenantDisplayName, "initial tenant display name")
	flags.StringVar(&options.config.AppKey, "app-key", options.config.AppKey, "initial agent app key")
	flags.StringVar(&options.config.AppDisplayName, "app-name", options.config.AppDisplayName, "initial agent app display name")
	flags.StringVar(&options.config.AppDescription, "app-description", options.config.AppDescription, "initial agent app description")
	if err := flags.Parse(args); err != nil {
		return initOptions{}, false, err
	}
	if flags.NArg() != 0 {
		return initOptions{}, false, errUnexpectedInitArguments
	}
	return options, options.help, nil
}

func parseDemoOptions(args []string, stderr io.Writer) (demoOptions, bool, error) {
	defaults := bootstrap.DefaultDemoConfig()
	defaults.TenantKey = strings.TrimSpace(os.Getenv("TRPC_DEMO_TENANT_KEY"))
	defaults.TenantDisplayName = strings.TrimSpace(os.Getenv("TRPC_DEMO_TENANT_NAME"))
	defaults.AppKey = strings.TrimSpace(os.Getenv("TRPC_DEMO_APP_KEY"))
	defaults.AppDisplayName = strings.TrimSpace(os.Getenv("TRPC_DEMO_APP_NAME"))
	defaults.AppDescription = strings.TrimSpace(os.Getenv("TRPC_DEMO_APP_DESCRIPTION"))
	defaults.ModelProfileKey = strings.TrimSpace(os.Getenv("TRPC_DEMO_MODEL_KEY"))
	defaults.BackendProfileKey = strings.TrimSpace(os.Getenv("TRPC_DEMO_BACKEND_KEY"))
	flags := flag.NewFlagSet("trpc-service demo", flag.ContinueOnError)
	flags.SetOutput(stderr)
	options := demoOptions{config: defaults}
	flags.BoolVar(&options.help, "help", false, "show help")
	flags.BoolVar(&options.help, "h", false, "show help")
	flags.BoolVar(&options.confirm, "confirm", false, "authorize demo initialization")
	flags.StringVar(&options.config.TenantKey, "tenant-key", options.config.TenantKey, "demo tenant key")
	flags.StringVar(&options.config.TenantDisplayName, "tenant-name", options.config.TenantDisplayName, "demo tenant display name")
	flags.StringVar(&options.config.AppKey, "app-key", options.config.AppKey, "demo agent app key")
	flags.StringVar(&options.config.AppDisplayName, "app-name", options.config.AppDisplayName, "demo agent app display name")
	flags.StringVar(&options.config.AppDescription, "app-description", options.config.AppDescription, "demo agent app description")
	flags.StringVar(&options.config.ModelProfileKey, "model-key", options.config.ModelProfileKey, "demo model profile key")
	flags.StringVar(&options.config.BackendProfileKey, "backend-key", options.config.BackendProfileKey, "demo backend profile key")
	if err := flags.Parse(args); err != nil {
		return demoOptions{}, false, err
	}
	if flags.NArg() != 0 {
		return demoOptions{}, false, errUnexpectedDemoArguments
	}
	return options, options.help, nil
}

func loadInitEnvironment(config bootstrap.InitConfig) (string, bootstrap.InitConfig, error) {
	dsn := strings.TrimSpace(os.Getenv(bootstrapPostgresDSN))
	if dsn == "" {
		return "", bootstrap.InitConfig{}, fmt.Errorf("%w: %s is required", bootstrap.ErrInvalidConfig, bootstrapPostgresDSN)
	}
	if err := config.Validate(); err != nil {
		return "", bootstrap.InitConfig{}, err
	}
	return dsn, config, nil
}

func parseServiceOptions(args []string, stderr io.Writer) (serviceOptions, bool, error) {
	options := serviceOptions{address: defaultListenAddress, shutdownTimeout: defaultShutdownTimeout}
	flags := flag.NewFlagSet("trpc-service", flag.ContinueOnError)
	flags.SetOutput(stderr)
	help := flags.Bool("help", false, "show help")
	flags.BoolVar(help, "h", false, "show help")
	flags.StringVar(&options.address, "addr", options.address, "HTTP listen address")
	flags.DurationVar(&options.shutdownTimeout, "shutdown-timeout", options.shutdownTimeout, "graceful shutdown timeout")
	if err := flags.Parse(args); err != nil {
		return serviceOptions{}, false, err
	}
	if flags.NArg() != 0 {
		return serviceOptions{}, false, fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if options.address == "" {
		return serviceOptions{}, false, fmt.Errorf("listen address cannot be empty")
	}
	if options.shutdownTimeout <= 0 {
		return serviceOptions{}, false, fmt.Errorf("shutdown timeout must be positive")
	}
	return options, *help, nil
}

func newServiceHTTPServer(handler http.Handler, options serviceOptions) *http.Server {
	return &http.Server{
		Addr:              options.address,
		Handler:           handler,
		ReadHeaderTimeout: defaultReadHeaderTimeout,
		ReadTimeout:       defaultReadTimeout,
		WriteTimeout:      defaultWriteTimeout,
		IdleTimeout:       defaultIdleTimeout,
	}
}

func runService(ctx context.Context, signals <-chan os.Signal, handler *gateway.HTTPHandler, shutdownTimeout time.Duration, serve func() error, shutdown func(context.Context) error) error {
	if ctx == nil || serve == nil || shutdown == nil || shutdownTimeout <= 0 {
		return errInvalidServiceSupervisorConfiguration
	}
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- serve()
	}()
	select {
	case err := <-serveResult:
		if ctx.Err() != nil {
			return shutdownService(handler, shutdownTimeout, shutdown)
		}
		closeErr := closeGatewayHandler(handler)
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return closeErr
		}
		if closeErr != nil {
			return errors.Join(err, closeErr)
		}
		return err
	case <-ctx.Done():
		return shutdownService(handler, shutdownTimeout, shutdown)
	case <-signals:
		return shutdownService(handler, shutdownTimeout, shutdown)
	}
}

func shutdownService(handler *gateway.HTTPHandler, timeout time.Duration, shutdown func(context.Context) error) error {
	if handler != nil {
		handler.BeginShutdown()
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	shutdownErr := shutdown(shutdownContext)
	return errors.Join(shutdownErr, closeGatewayHandler(handler))
}

func closeGatewayHandler(handler *gateway.HTTPHandler) error {
	if handler == nil {
		return nil
	}
	return handler.Close()
}
