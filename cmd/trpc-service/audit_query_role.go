package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/liuzengh/trpc-agent-service/compliancemigrations"
	"github.com/liuzengh/trpc-agent-service/migrations"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/query"
	querycompliance "github.com/liuzengh/trpc-agent-service/trpcservice/audit/query/compliance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/health"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretfs "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/filesystem"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

const auditQueryAuthResourceID = "audit-query-auth"

type auditQueryConfig struct {
	ListenAddress         string
	PostgresDSN           string
	CompliancePostgresDSN string
	SecretRoot            string
	AuthSecretRef         string
	AuthSecretVersion     int64
	ProbeTenant           string
	MaxPage               int
	MaxWindow             time.Duration
	ProbeTimeout          time.Duration
	ProbeInterval         time.Duration
	ShutdownTimeout       time.Duration
}

func loadAuditQueryConfig(getenv func(string) string) (auditQueryConfig, error) {
	if getenv == nil {
		return auditQueryConfig{}, errors.New("environment reader is required")
	}
	config := auditQueryConfig{
		ListenAddress:         valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		PostgresDSN:           strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")),
		CompliancePostgresDSN: strings.TrimSpace(getenv("TRPC_AUDIT_COMPLIANCE_POSTGRES_DSN")),
		SecretRoot:            strings.TrimSpace(getenv("TRPC_SECRET_ROOT")),
		AuthSecretRef:         strings.TrimSpace(getenv("TRPC_AUDIT_QUERY_AUTH_SECRET_REF")),
		ProbeTenant:           strings.TrimSpace(getenv("TRPC_AUDIT_QUERY_PROBE_TENANT_ID")),
		MaxPage:               200,
		MaxWindow:             31 * 24 * time.Hour,
		ProbeTimeout:          5 * time.Second,
		ProbeInterval:         15 * time.Second,
		ShutdownTimeout:       30 * time.Second,
	}
	var err error
	if config.AuthSecretVersion, err = envInt64(getenv, "TRPC_AUDIT_QUERY_AUTH_SECRET_VERSION", 0); err != nil || config.AuthSecretVersion < 1 {
		return auditQueryConfig{}, errors.New("invalid TRPC_AUDIT_QUERY_AUTH_SECRET_VERSION")
	}
	if config.MaxPage, err = envInt(getenv, "TRPC_AUDIT_QUERY_MAX_PAGE", config.MaxPage); err != nil || config.MaxPage < 1 || config.MaxPage > 1000 {
		return auditQueryConfig{}, errors.New("invalid TRPC_AUDIT_QUERY_MAX_PAGE")
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
		min    time.Duration
	}{
		{"TRPC_PROBE_TIMEOUT", &config.ProbeTimeout, time.Millisecond},
		{"TRPC_PROBE_INTERVAL", &config.ProbeInterval, time.Millisecond},
		{"TRPC_SHUTDOWN_TIMEOUT", &config.ShutdownTimeout, time.Second},
		{"TRPC_AUDIT_QUERY_MAX_WINDOW", &config.MaxWindow, time.Hour},
	} {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.min {
			return auditQueryConfig{}, errors.New("invalid " + item.name)
		}
	}
	if config.ListenAddress == "" || config.PostgresDSN == "" || config.CompliancePostgresDSN == "" ||
		config.CompliancePostgresDSN == config.PostgresDSN || config.SecretRoot == "" ||
		config.AuthSecretRef == "" || config.ProbeTenant == "" {
		return auditQueryConfig{}, errors.New("required audit query dependency configuration is missing")
	}
	return config, nil
}

func auditQueryAuthScope(tenantID string, version int64) secrets.Scope {
	return secrets.Scope{TenantID: tenantID, Subject: "audit-query", Purpose: secrets.PurposeAuditQueryAuth,
		ResourceID: auditQueryAuthResourceID, ResourceVersion: version}
}

func runAuditQueryRole(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || logger == nil {
		return errors.New("invalid process dependencies")
	}
	config, err := loadAuditQueryConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	telemetryProvider, err := newRoleTelemetry(parent, getenv, "audit-query", logger)
	if err != nil {
		return fmt.Errorf("telemetry configuration rejected: %w", err)
	}
	defer shutdownRoleTelemetry(telemetryProvider, logger)
	db, err := sql.Open("pgx", config.PostgresDSN)
	if err != nil {
		return errors.New("postgres client initialization failed")
	}
	defer db.Close()
	complianceDB, err := sql.Open("pgx", config.CompliancePostgresDSN)
	if err != nil {
		return errors.New("compliance postgres client initialization failed")
	}
	defer complianceDB.Close()

	secretProvider, err := secretfs.New(config.SecretRoot, 64<<10)
	if err != nil {
		return errors.New("secret provider configuration rejected")
	}
	tenantRepo := tenantpostgres.New(db)
	authRef := secrets.SecretRef{Ref: config.AuthSecretRef, Version: config.AuthSecretVersion}
	authValue, err := secretProvider.Resolve(parent, auditQueryAuthScope(config.ProbeTenant, config.AuthSecretVersion), authRef)
	if err != nil {
		return errors.New("audit query auth secret resolution failed")
	}
	versionCheck := func(tenantID string, version int64) error {
		current, getErr := tenantRepo.Get(parent, tenantID)
		if getErr != nil {
			return getErr
		}
		if current.Version != version || current.Status == tenant.StatusDisabled {
			return query.ErrForbidden
		}
		return nil
	}
	resolver, err := query.NewHMACPrincipalResolver(authValue.Bytes, query.ResolverOptions{
		VersionCheck: versionCheck, ClockSkew: 30 * time.Second})
	clear(authValue.Bytes)
	if err != nil {
		return errors.New("audit query resolver configuration rejected")
	}
	defer resolver.Close()

	store := querycompliance.New(complianceDB)
	registry := &metrics.AuditQueryRegistry{}
	handler := query.Handler{Store: store, Principals: resolver, MaxWindow: config.MaxWindow, MaxPage: config.MaxPage,
		Observer: registry}
	lifecycle := worker.NewLifecycle()
	monitor, err := health.NewMonitor(lifecycle, []health.Dependency{
		{Name: "postgres", Probe: db.PingContext},
		{Name: "postgres_schema", Probe: migrations.NewRunner(db).Ready},
		{Name: "compliance_postgres", Probe: complianceDB.PingContext},
		{Name: "compliance_schema", Probe: compliancemigrations.Runner{DB: complianceDB}.Ready},
	}, config.ProbeTimeout, config.ProbeInterval)
	if err != nil {
		return errors.New("readiness configuration rejected")
	}
	if err := monitor.ProbeOnce(parent); err != nil {
		return errors.New("initial dependency probe interrupted")
	}
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return errors.New("HTTP listener initialization failed")
	}
	defer listener.Close()
	mux := http.NewServeMux()
	mux.Handle("/livez", health.Handler{Checker: monitor})
	mux.Handle("/readyz", health.Handler{Checker: monitor})
	mux.Handle("/metrics", registry)
	mux.Handle("/v1/", handler)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	processCtx, cancelProcess := context.WithCancel(parent)
	defer cancelProcess()
	stopSignals := worker.InstallSignalDrain(processCtx, lifecycle)
	defer stopSignals()
	errorsCh := make(chan error, 2)
	start := func(name string, operation func(context.Context) error) {
		go func() {
			if runErr := operation(processCtx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				errorsCh <- fmt.Errorf("%s stopped", name)
			}
		}()
	}
	start("readiness monitor", monitor.Run)
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errorsCh <- errors.New("HTTP server stopped")
		}
	}()
	if err := lifecycle.MarkReady(); err != nil {
		return errors.New("process lifecycle transition failed")
	}
	logger.Printf("trpc-agent-service audit-query listening on %s", config.ListenAddress)
	var terminalErr error
	select {
	case <-parent.Done():
		lifecycle.BeginDrain()
	case <-lifecycle.Drain():
	case terminalErr = <-errorsCh:
		lifecycle.BeginDrain()
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), config.ShutdownTimeout)
	defer cancelShutdown()
	cancelProcess()
	if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil && terminalErr == nil {
		terminalErr = errors.New("HTTP shutdown timed out")
	}
	lifecycle.MarkStopped()
	return terminalErr
}
