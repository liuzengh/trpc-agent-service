package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/liuzengh/trpc-agent-service/migrations"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	configpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/config/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/health"
	knowledgedriverpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver/postgres"
	migrationpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/migration/postgres"
	sessionmigrationpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/migration/sessiondriver/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretfs "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/filesystem"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

const adminAuthResourceID = "admin-auth"

func adminAuthScope(tenantID string, version int64) secrets.Scope {
	return secrets.Scope{TenantID: tenantID, Subject: "admin", Purpose: secrets.PurposeAdminAuth,
		ResourceID: adminAuthResourceID, ResourceVersion: version}
}

func runAdminRole(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || logger == nil {
		return errors.New("invalid process dependencies")
	}
	configValue, err := loadAdminConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	telemetryProvider, err := newRoleTelemetry(parent, getenv, "admin", logger)
	if err != nil {
		return fmt.Errorf("telemetry configuration rejected: %w", err)
	}
	defer shutdownRoleTelemetry(telemetryProvider, logger)
	db, err := sql.Open("pgx", configValue.PostgresDSN)
	if err != nil {
		return errors.New("postgres client initialization failed")
	}
	defer db.Close()
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetMaxIdleConns(4)
	db.SetMaxOpenConns(16)

	secretProvider, err := secretfs.New(configValue.SecretRoot, 64<<10)
	if err != nil {
		return errors.New("secret provider configuration rejected")
	}
	tenantRepo := tenantpostgres.New(db)
	authRef := secrets.SecretRef{Ref: configValue.AdminAuthSecretRef, Version: configValue.AdminAuthSecretVersion}
	authValue, err := secretProvider.Resolve(parent, adminAuthScope(configValue.AdminProbeTenant, configValue.AdminAuthSecretVersion), authRef)
	if err != nil {
		return errors.New("admin auth secret resolution failed")
	}
	versionCheck := func(tenantID string, version int64) error {
		current, getErr := tenantRepo.Get(parent, tenantID)
		if getErr != nil {
			return getErr
		}
		if current.Version != version || current.Status == tenant.StatusDisabled {
			return admin.ErrForbidden
		}
		return nil
	}
	resolver, err := admin.NewHMACPrincipalResolver(authValue.Bytes, admin.HMACPrincipalOptions{
		VersionCheck: versionCheck, ClockSkew: configValue.AdminAuthClockSkew})
	clear(authValue.Bytes)
	if err != nil {
		return errors.New("admin auth resolver configuration rejected")
	}
	defer resolver.Close()

	lifecycle := worker.NewLifecycle()
	monitor, err := health.NewMonitor(lifecycle, []health.Dependency{
		{Name: "postgres", Probe: db.PingContext},
		{Name: "postgres_schema", Probe: migrations.NewRunner(db).Ready},
		{Name: "secret_provider", Probe: secretProvider.ProbeRoot},
		{Name: "admin_auth_secret", Probe: func(ctx context.Context) error {
			return secretProvider.Probe(ctx, adminAuthScope(configValue.AdminProbeTenant, configValue.AdminAuthSecretVersion), authRef)
		}},
	}, configValue.ProbeTimeout, configValue.ProbeInterval)
	if err != nil {
		return errors.New("readiness configuration rejected")
	}
	if err := monitor.ProbeOnce(parent); err != nil {
		return errors.New("initial dependency probe interrupted")
	}

	listener, err := net.Listen("tcp", configValue.ListenAddress)
	if err != nil {
		return errors.New("HTTP listener initialization failed")
	}
	defer listener.Close()
	mux := http.NewServeMux()
	mux.Handle("/livez", health.Handler{Checker: monitor})
	mux.Handle("/readyz", health.Handler{Checker: monitor})
	api := admin.Handler{Service: admin.Service{Configs: configpostgres.New(db, tenantRepo), Migrations: migrationpostgres.New(db),
		SessionMigrationPublisher: sessionmigrationpostgres.NewPublisher(db), KnowledgeMigrationPublisher: knowledgedriverpostgres.NewPublisher(db)}, Principals: resolver,
		Catalog: admin.PostgreSQLCatalog{DB: db}}
	console := admin.Console{API: readinessGate{Checker: monitor, Handler: api}, Principals: resolver,
		AllowInsecureSessionCookie: configValue.AdminAllowInsecureSessionCookie}
	mux.Handle("/admin", console)
	mux.Handle("/admin/", console)
	mux.Handle("/v1/", console)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}

	processCtx, cancelProcess := context.WithCancel(parent)
	defer cancelProcess()
	stopSignals := worker.InstallSignalDrain(processCtx, lifecycle)
	defer stopSignals()
	errorsCh := make(chan error, 2)
	go func() {
		if runErr := monitor.Run(processCtx); runErr != nil && !errors.Is(runErr, context.Canceled) {
			errorsCh <- errors.New("readiness monitor stopped")
		}
	}()
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errorsCh <- errors.New("HTTP server stopped")
		}
	}()
	if err := lifecycle.MarkReady(); err != nil {
		return errors.New("process lifecycle transition failed")
	}
	logger.Printf("trpc-agent-service admin lifecycle/readiness listening on %s", configValue.ListenAddress)

	var terminalErr error
	select {
	case <-parent.Done():
		lifecycle.BeginDrain()
	case <-lifecycle.Drain():
	case terminalErr = <-errorsCh:
		lifecycle.BeginDrain()
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), configValue.ShutdownTimeout)
	defer cancelShutdown()
	cancelProcess()
	if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil && terminalErr == nil {
		terminalErr = errors.New("HTTP shutdown timed out")
	}
	lifecycle.MarkStopped()
	return terminalErr
}
