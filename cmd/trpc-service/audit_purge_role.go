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
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/purge"
	purgecompliance "github.com/liuzengh/trpc-agent-service/trpcservice/audit/purge/compliance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/health"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

type auditPurgeConfig struct {
	CompliancePostgresDSN string
	PollInterval          time.Duration
	DryRun                bool
	RequireApproval       bool
	MaxAttempts           int
	MaxBatchSize          int64
	Owner                 string
	ProbeTimeout          time.Duration
	ProbeInterval         time.Duration
	ShutdownTimeout       time.Duration
}

func loadAuditPurgeConfig(getenv func(string) string) (auditPurgeConfig, error) {
	if getenv == nil {
		return auditPurgeConfig{}, errors.New("environment reader is required")
	}
	config := auditPurgeConfig{
		CompliancePostgresDSN: strings.TrimSpace(getenv("TRPC_AUDIT_COMPLIANCE_POSTGRES_DSN")),
		PollInterval:          time.Minute,
		DryRun:                true,
		RequireApproval:       true,
		MaxAttempts:           3,
		MaxBatchSize:          50000,
		Owner:                 strings.TrimSpace(getenv("TRPC_AUDIT_PURGE_OWNER")),
		ProbeTimeout:          5 * time.Second,
		ProbeInterval:         15 * time.Second,
		ShutdownTimeout:       30 * time.Second,
	}
	var err error
	if config.DryRun, err = envBool(getenv, "TRPC_AUDIT_PURGE_DRY_RUN", true); err != nil {
		return auditPurgeConfig{}, errors.New("invalid TRPC_AUDIT_PURGE_DRY_RUN")
	}
	if config.RequireApproval, err = envBool(getenv, "TRPC_AUDIT_PURGE_REQUIRE_APPROVAL", true); err != nil {
		return auditPurgeConfig{}, errors.New("invalid TRPC_AUDIT_PURGE_REQUIRE_APPROVAL")
	}
	if config.MaxAttempts, err = envInt(getenv, "TRPC_AUDIT_PURGE_MAX_ATTEMPTS", config.MaxAttempts); err != nil || config.MaxAttempts < 1 || config.MaxAttempts > 100 {
		return auditPurgeConfig{}, errors.New("invalid TRPC_AUDIT_PURGE_MAX_ATTEMPTS")
	}
	if config.MaxBatchSize, err = envInt64(getenv, "TRPC_AUDIT_PURGE_MAX_BATCH_SIZE", config.MaxBatchSize); err != nil || config.MaxBatchSize < 1 || config.MaxBatchSize > 1_000_000 {
		return auditPurgeConfig{}, errors.New("invalid TRPC_AUDIT_PURGE_MAX_BATCH_SIZE")
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
		min    time.Duration
	}{
		{"TRPC_PROBE_TIMEOUT", &config.ProbeTimeout, time.Millisecond},
		{"TRPC_PROBE_INTERVAL", &config.ProbeInterval, time.Millisecond},
		{"TRPC_SHUTDOWN_TIMEOUT", &config.ShutdownTimeout, time.Second},
		{"TRPC_AUDIT_PURGE_POLL_INTERVAL", &config.PollInterval, time.Second},
	} {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.min {
			return auditPurgeConfig{}, errors.New("invalid " + item.name)
		}
	}
	if config.CompliancePostgresDSN == "" || config.Owner == "" {
		return auditPurgeConfig{}, errors.New("required audit purge dependency configuration is missing")
	}
	return config, nil
}

func runAuditPurgeRole(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || logger == nil {
		return errors.New("invalid process dependencies")
	}
	config, err := loadAuditPurgeConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	telemetryProvider, err := newRoleTelemetry(parent, getenv, "audit-purge", logger)
	if err != nil {
		return fmt.Errorf("telemetry configuration rejected: %w", err)
	}
	defer shutdownRoleTelemetry(telemetryProvider, logger)
	complianceDB, err := sql.Open("pgx", config.CompliancePostgresDSN)
	if err != nil {
		return errors.New("compliance postgres client initialization failed")
	}
	defer complianceDB.Close()

	store := purgecompliance.New(complianceDB)
	reconciler := purge.Reconciler{Store: store, Owner: config.Owner, DryRun: config.DryRun,
		RequireApproval: config.RequireApproval, MaxAttempts: config.MaxAttempts, MaxBatchSize: config.MaxBatchSize}
	registry := &metrics.AuditPurgeRegistry{}
	lifecycle := worker.NewLifecycle()
	monitor, err := health.NewMonitor(lifecycle, []health.Dependency{
		{Name: "compliance_postgres", Probe: complianceDB.PingContext},
		{Name: "compliance_schema", Probe: compliancemigrations.Runner{DB: complianceDB}.Ready},
	}, config.ProbeTimeout, config.ProbeInterval)
	if err != nil {
		return errors.New("readiness configuration rejected")
	}
	if err := monitor.ProbeOnce(parent); err != nil {
		return errors.New("initial dependency probe interrupted")
	}
	listener, err := net.Listen("tcp", valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"))
	if err != nil {
		return errors.New("HTTP listener initialization failed")
	}
	defer listener.Close()
	mux := http.NewServeMux()
	mux.Handle("/livez", health.Handler{Checker: monitor})
	mux.Handle("/readyz", health.Handler{Checker: monitor})
	mux.Handle("/metrics", registry)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	processCtx, cancelProcess := context.WithCancel(parent)
	defer cancelProcess()
	stopSignals := worker.InstallSignalDrain(processCtx, lifecycle)
	defer stopSignals()
	errorsCh := make(chan error, 3)
	start := func(name string, operation func(context.Context) error) {
		go func() {
			if runErr := operation(processCtx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				errorsCh <- fmt.Errorf("%s stopped", name)
			}
		}()
	}
	start("readiness monitor", monitor.Run)
	start("audit purge reconciler", func(ctx context.Context) error {
		ticker := time.NewTicker(config.PollInterval)
		defer ticker.Stop()
		for {
			stats, runErr := reconciler.ProcessOnce(ctx)
			if runErr != nil {
				logger.Printf("audit purge pass failed: %v", runErr)
			} else {
				registry.Observe(stats.Planned, stats.Approved, stats.Executed, stats.Quarantined)
				registry.ObserveDeleted(stats.Deleted)
				registry.ObserveGauge(stats.OverdueTenants, stats.LegalHolds)
				logger.Printf("audit purge pass planned=%d approved=%d executed=%d skipped=%d quarantined=%d deleted=%d overdue=%d holds=%d",
					stats.Planned, stats.Approved, stats.Executed, stats.Skipped, stats.Quarantined, stats.Deleted, stats.OverdueTenants, stats.LegalHolds)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		}
	})
	go func() {
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errorsCh <- errors.New("HTTP server stopped")
		}
	}()
	if err := lifecycle.MarkReady(); err != nil {
		return errors.New("process lifecycle transition failed")
	}
	logger.Printf("trpc-agent-service audit-purge running dry_run=%v require_approval=%v", config.DryRun, config.RequireApproval)
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
