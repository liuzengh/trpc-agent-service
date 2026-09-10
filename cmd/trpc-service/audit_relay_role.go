package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/liuzengh/trpc-agent-service/compliancemigrations"
	"github.com/liuzengh/trpc-agent-service/migrations"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	compliancepostgres "github.com/liuzengh/trpc-agent-service/trpcservice/audit/compliancepostgres"
	auditpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/audit/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/health"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func runAuditRelayRole(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || logger == nil {
		return errors.New("invalid process dependencies")
	}
	config, err := loadAuditRelayConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	telemetryProvider, err := newRoleTelemetry(parent, getenv, "audit-relay", logger)
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
	complianceDB, err := sql.Open("pgx", config.AuditCompliancePostgresDSN)
	if err != nil {
		return errors.New("compliance postgres client initialization failed")
	}
	defer complianceDB.Close()
	complianceDB.SetConnMaxLifetime(30 * time.Minute)
	complianceDB.SetMaxIdleConns(4)
	complianceDB.SetMaxOpenConns(16)

	owner := config.AuditOwner
	if owner == "" {
		host, _ := os.Hostname()
		owner = fmt.Sprintf("%s-%d-audit-relay", valueOr(host, "trpc-service"), os.Getpid())
	}
	store := auditpostgres.New(db)
	complianceSink := compliancepostgres.Sink{DB: complianceDB}
	outbox := messagingpostgres.New(db)
	registry := &metrics.AuditRegistry{SnapshotTTL: 3 * config.AuditLagPollInterval}
	auditRelay := audit.Relay{Base: relay.Base{Outbox: outbox, Kind: "audit", Owner: owner, BatchSize: config.AuditBatchSize,
		ClaimTTL: config.AuditClaimTTL, ClaimRenewInterval: config.AuditClaimRenew, RetryDelay: config.AuditRetryDelay,
		PollInterval: config.AuditPollInterval, Telemetry: telemetryProvider}, Resolver: store, Sink: complianceSink,
		OnError: func(error) { logger.Printf("audit relay degraded") }, Observer: registry, Alerts: registry}
	backlogMonitor := audit.BacklogMonitor{Source: store, Observer: registry, PollInterval: config.AuditLagPollInterval,
		MaxOldestAge: config.AuditLagAlertAge, MaxActive: config.AuditLagAlertCount,
		OnAlertChange: func(alerting bool) {
			if alerting {
				logger.Printf("audit backlog alert active")
				return
			}
			logger.Printf("audit backlog alert cleared")
		}}

	lifecycle := worker.NewLifecycle()
	monitor, err := health.NewMonitor(lifecycle, []health.Dependency{{Name: "postgres", Probe: db.PingContext},
		{Name: "postgres_schema", Probe: migrations.NewRunner(db).Ready},
		{Name: "compliance_postgres", Probe: complianceDB.PingContext},
		{Name: "compliance_schema", Probe: compliancemigrations.Runner{DB: complianceDB}.Ready},
		{Name: "audit_database_separation", Probe: func(ctx context.Context) error {
			return verifyIndependentAuditDatabases(ctx, db, complianceDB)
		}}}, config.ProbeTimeout, config.ProbeInterval)
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
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	processCtx, cancelProcess := context.WithCancel(parent)
	defer cancelProcess()
	stopSignals := worker.InstallSignalDrain(processCtx, lifecycle)
	defer stopSignals()
	errorsCh := make(chan error, 3)
	var background sync.WaitGroup
	start := func(name string, operation func(context.Context) error) {
		background.Add(1)
		go func() {
			defer background.Done()
			if runErr := operation(processCtx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				errorsCh <- fmt.Errorf("%s stopped", name)
			}
		}()
	}
	start("readiness monitor", monitor.Run)
	start("audit relay", auditRelay.Run)
	start("audit backlog monitor", backlogMonitor.Run)
	background.Add(1)
	go func() {
		defer background.Done()
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errorsCh <- errors.New("HTTP server stopped")
		}
	}()
	if err := lifecycle.MarkReady(); err != nil {
		return errors.New("process lifecycle transition failed")
	}
	logger.Printf("trpc-agent-service audit-relay lifecycle/readiness listening on %s", config.ListenAddress)
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
	done := make(chan struct{})
	go func() { background.Wait(); close(done) }()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		if terminalErr == nil {
			terminalErr = errors.New("background shutdown timed out")
		}
	}
	lifecycle.MarkStopped()
	return terminalErr
}

func verifyIndependentAuditDatabases(ctx context.Context, source, target *sql.DB) error {
	if source == nil || target == nil {
		return errors.New("audit database clients are required")
	}
	type identity struct {
		database string
		address  string
		port     int
	}
	read := func(db *sql.DB) (identity, error) {
		var value identity
		err := db.QueryRowContext(ctx, `SELECT current_database(),COALESCE(inet_server_addr()::text,''),COALESCE(inet_server_port(),0)`).Scan(
			&value.database, &value.address, &value.port)
		return value, err
	}
	left, err := read(source)
	if err != nil {
		return err
	}
	right, err := read(target)
	if err != nil {
		return err
	}
	if left == right {
		return errors.New("audit source and compliance target are the same database")
	}
	return nil
}

func runAuditComplianceMigrate(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || getenv == nil || logger == nil {
		return errors.New("invalid migration dependencies")
	}
	dsn := strings.TrimSpace(getenv("TRPC_AUDIT_COMPLIANCE_POSTGRES_DSN"))
	if dsn == "" {
		return errors.New("compliance postgres configuration is missing")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return errors.New("compliance postgres client initialization failed")
	}
	defer db.Close()
	if err := db.PingContext(parent); err != nil {
		return errors.New("compliance postgres unavailable")
	}
	if err := (compliancemigrations.Runner{DB: db}).Up(parent); err != nil {
		return fmt.Errorf("compliance migration failed: %w", err)
	}
	logger.Printf("compliance schema migration complete")
	return nil
}
