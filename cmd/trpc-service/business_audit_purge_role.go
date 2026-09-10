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

	"github.com/liuzengh/trpc-agent-service/migrations"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit/purgebusiness"
	purgebusinesspostgres "github.com/liuzengh/trpc-agent-service/trpcservice/audit/purgebusiness/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/health"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

type businessAuditPurgeConfig struct {
	PostgresDSN, Owner                           string
	PollInterval, Retention                      time.Duration
	ProbeTimeout, ProbeInterval, ShutdownTimeout time.Duration
	DryRun                                       bool
	MaxAttempts                                  int
	MaxBatchSize                                 int64
}

func loadBusinessAuditPurgeConfig(getenv func(string) string) (businessAuditPurgeConfig, error) {
	if getenv == nil {
		return businessAuditPurgeConfig{}, errors.New("environment reader is required")
	}
	config := businessAuditPurgeConfig{PostgresDSN: strings.TrimSpace(getenv("TRPC_BUSINESS_AUDIT_PURGE_POSTGRES_DSN")),
		Owner: strings.TrimSpace(getenv("TRPC_BUSINESS_AUDIT_PURGE_OWNER")), PollInterval: time.Minute,
		Retention: 180 * 24 * time.Hour, DryRun: true, MaxAttempts: 3, MaxBatchSize: 1000,
		ProbeTimeout: 5 * time.Second, ProbeInterval: 15 * time.Second, ShutdownTimeout: 30 * time.Second}
	var err error
	if config.DryRun, err = envBool(getenv, "TRPC_BUSINESS_AUDIT_PURGE_DRY_RUN", true); err != nil {
		return businessAuditPurgeConfig{}, errors.New("invalid TRPC_BUSINESS_AUDIT_PURGE_DRY_RUN")
	}
	if config.MaxAttempts, err = envInt(getenv, "TRPC_BUSINESS_AUDIT_PURGE_MAX_ATTEMPTS", config.MaxAttempts); err != nil || config.MaxAttempts < 1 || config.MaxAttempts > 100 {
		return businessAuditPurgeConfig{}, errors.New("invalid TRPC_BUSINESS_AUDIT_PURGE_MAX_ATTEMPTS")
	}
	if config.MaxBatchSize, err = envInt64(getenv, "TRPC_BUSINESS_AUDIT_PURGE_MAX_BATCH_SIZE", config.MaxBatchSize); err != nil || config.MaxBatchSize < 1 || config.MaxBatchSize > 1_000_000 {
		return businessAuditPurgeConfig{}, errors.New("invalid TRPC_BUSINESS_AUDIT_PURGE_MAX_BATCH_SIZE")
	}
	for _, item := range []struct {
		name   string
		target *time.Duration
		min    time.Duration
	}{
		{"TRPC_PROBE_TIMEOUT", &config.ProbeTimeout, time.Millisecond},
		{"TRPC_PROBE_INTERVAL", &config.ProbeInterval, time.Millisecond},
		{"TRPC_SHUTDOWN_TIMEOUT", &config.ShutdownTimeout, time.Second},
		{"TRPC_BUSINESS_AUDIT_PURGE_POLL_INTERVAL", &config.PollInterval, time.Second},
		{"TRPC_BUSINESS_AUDIT_RETENTION", &config.Retention, 24 * time.Hour},
	} {
		if *item.target, err = envDuration(getenv, item.name, *item.target); err != nil || *item.target < item.min {
			return businessAuditPurgeConfig{}, errors.New("invalid " + item.name)
		}
	}
	if config.PostgresDSN == "" || config.Owner == "" {
		return businessAuditPurgeConfig{}, errors.New("required business audit purge dependency configuration is missing")
	}
	return config, nil
}

func runBusinessAuditPurgeRole(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || logger == nil {
		return errors.New("invalid process dependencies")
	}
	config, err := loadBusinessAuditPurgeConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	telemetryProvider, err := newRoleTelemetry(parent, getenv, "business-audit-purge", logger)
	if err != nil {
		return fmt.Errorf("telemetry configuration rejected: %w", err)
	}
	defer shutdownRoleTelemetry(telemetryProvider, logger)
	db, err := sql.Open("pgx", config.PostgresDSN)
	if err != nil {
		return errors.New("postgres client initialization failed")
	}
	defer db.Close()
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetMaxIdleConns(4)
	db.SetMaxOpenConns(16)

	lifecycle := worker.NewLifecycle()
	monitor, err := health.NewMonitor(lifecycle, []health.Dependency{{Name: "postgres", Probe: db.PingContext},
		{Name: "postgres_schema", Probe: migrations.NewRunner(db).Ready}}, config.ProbeTimeout, config.ProbeInterval)
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
	registry := &metrics.BusinessAuditPurgeRegistry{}
	mux := http.NewServeMux()
	mux.Handle("/livez", health.Handler{Checker: monitor})
	mux.Handle("/readyz", health.Handler{Checker: monitor})
	mux.Handle("/metrics", registry)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	reconciler := purgebusiness.Reconciler{Store: purgebusinesspostgres.New(db), Owner: config.Owner,
		Retention: config.Retention, DryRun: config.DryRun, MaxAttempts: config.MaxAttempts, MaxBatchSize: config.MaxBatchSize}
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
	start("business audit purge reconciler", func(ctx context.Context) error {
		ticker := time.NewTicker(config.PollInterval)
		defer ticker.Stop()
		for {
			stats, runErr := reconciler.ProcessOnce(ctx)
			if runErr != nil {
				logger.Printf("business audit purge pass failed: %v", runErr)
			} else {
				registry.Observe(stats)
				logger.Printf("business audit purge pass planned=%d completed=%d skipped=%d failed=%d quarantined=%d deleted=%d", stats.Planned, stats.Completed, stats.Skipped, stats.Failed, stats.Quarantined, stats.Deleted)
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
	logger.Printf("trpc-agent-service business-audit-purge running dry_run=%v retention=%s", config.DryRun, config.Retention)
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
