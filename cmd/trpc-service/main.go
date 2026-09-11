package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/cyl6/trpc-agent-service/migrations"
	"github.com/cyl6/trpc-agent-service/trpcservice"
	"github.com/cyl6/trpc-agent-service/trpcservice/adminauth"
	agentruntime "github.com/cyl6/trpc-agent-service/trpcservice/agent"
	"github.com/cyl6/trpc-agent-service/trpcservice/aibotbridge"
	"github.com/cyl6/trpc-agent-service/trpcservice/budget"
	"github.com/cyl6/trpc-agent-service/trpcservice/channels"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/configcontrol"
	"github.com/cyl6/trpc-agent-service/trpcservice/contentsafety"
	"github.com/cyl6/trpc-agent-service/trpcservice/coordination"
	"github.com/cyl6/trpc-agent-service/trpcservice/health"
	auditlog "github.com/cyl6/trpc-agent-service/trpcservice/log"
	"github.com/cyl6/trpc-agent-service/trpcservice/memoryvisibility"
	"github.com/cyl6/trpc-agent-service/trpcservice/metrics"
	"github.com/cyl6/trpc-agent-service/trpcservice/observability"
	"github.com/cyl6/trpc-agent-service/trpcservice/queue"
	"github.com/cyl6/trpc-agent-service/trpcservice/store"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant/governance"
	"github.com/cyl6/trpc-agent-service/trpcservice/tooloperation"
	"github.com/cyl6/trpc-agent-service/trpcservice/web"
	"github.com/cyl6/trpc-agent-service/trpcservice/worker"

	s3storage "trpc.group/trpc-go/trpc-agent-go/storage/s3"
	ametric "trpc.group/trpc-go/trpc-agent-go/telemetry/metric"
	atrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"
)

func main() {
	configPath := flag.String("config", "config/example.yaml", "path to service config YAML")
	showVersion := flag.Bool("version", false, "print version information and exit")
	flag.Parse()
	if *showVersion {
		fmt.Printf("trpc-agent-service %s (commit %s)\n", trpcservice.Version, trpcservice.GitCommit)
		return
	}
	if err := run(*configPath); err != nil {
		// Startup errors may originate in third-party clients that echo an
		// endpoint or provider response. Keep the ordinary process log stable;
		// operators diagnose details through controlled startup probes.
		log.Printf("trpc-agent-service startup failed: category=%s", startupFailureCategory(err))
		os.Exit(1)
	}
}

func startupFailureCategory(err error) string {
	switch {
	case errors.Is(err, syscall.EADDRINUSE):
		return "address_in_use"
	case errors.Is(err, syscall.EACCES):
		return "permission_denied"
	default:
		return "initialization_failed"
	}
}

func run(configPath string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := config.LoadStatic(configPath)
	if err != nil {
		return err
	}
	secretRoot := os.Getenv("TRPC_SECRET_ROOT")
	if secretRoot == "" {
		secretRoot = "/run/secrets"
	}
	if err := config.ConfigureSecretResolver(cfg.Secrets.Provider, secretRoot); err != nil {
		return err
	}
	controlStore, closeControlStore, err := buildControlStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeControlStore()
	effectiveTenants, err := bootstrapAndLoadTenants(ctx, cfg, controlStore)
	if err != nil {
		return err
	}
	effectiveConfig := *cfg
	effectiveConfig.Tenants = effectiveTenants
	if err := effectiveConfig.Validate(); err != nil {
		return err
	}
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	metricCleanup := func() error { return nil }
	traceCleanup := func() error { return nil }
	if cfg.Telemetry.OTLPEndpoint != "" {
		meterProvider, metricErr := ametric.NewMeterProvider(
			context.Background(),
			ametric.WithEndpoint(cfg.Telemetry.OTLPEndpoint),
			ametric.WithProtocol(cfg.Telemetry.OTLPProtocol),
			ametric.WithServiceName(cfg.Telemetry.ServiceName),
		)
		if metricErr != nil {
			return errors.New("initialize telemetry metrics")
		}
		if metricErr = ametric.InitMeterProvider(meterProvider); metricErr != nil {
			_ = meterProvider.Shutdown(context.Background())
			return errors.New("initialize framework metric instruments")
		}
		otel.SetMeterProvider(meterProvider)
		metricCleanup = func() error {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return meterProvider.Shutdown(shutdownCtx)
		}
		opts := []atrace.Option{
			atrace.WithEndpoint(cfg.Telemetry.OTLPEndpoint),
			atrace.WithProtocol(cfg.Telemetry.OTLPProtocol),
			atrace.WithServiceName(cfg.Telemetry.ServiceName),
			sensitiveSpanPolicy(),
		}
		traceCleanup, err = atrace.Start(context.Background(), opts...)
		if err != nil {
			_ = metricCleanup()
			return err
		}
	}
	defer func() { _ = traceCleanup() }()
	defer func() { _ = metricCleanup() }()

	tenantRegistry, err := tenant.NewRegistry(&effectiveConfig)
	if err != nil {
		return err
	}
	nodeID, err := controlNodeID(cfg)
	if err != nil {
		return err
	}
	controlController, err := configcontrol.NewController(configcontrol.ControllerOptions{
		Store: controlStore, NodeID: nodeID,
		RefreshInterval:   cfg.ControlPlane.RefreshInterval,
		HeartbeatInterval: cfg.ControlPlane.HeartbeatInterval,
		HeartbeatTTL:      cfg.ControlPlane.HeartbeatTTL,
		AckTimeout:        cfg.ControlPlane.AckTimeout,
		Apply: func(state configcontrol.TenantState, active config.TenantConfig, canary *config.TenantConfig) error {
			return tenantRegistry.PublishControlState(state, active, canary)
		},
	})
	if err != nil {
		return err
	}
	if err := controlController.Start(ctx); err != nil {
		controlController.Close()
		return err
	}
	defer controlController.Close()
	httpClient := &http.Client{Timeout: 15 * time.Second}
	wecomAIBotAdapter := channels.NewWeComAIBot()
	channelRegistry := channels.NewRegistry(
		channels.NewTelegram(httpClient), channels.NewSlack(httpClient),
		channels.NewWeCom(httpClient), wecomAIBotAdapter,
	)
	coordinator, err := buildCoordinator(cfg.Coordination)
	if err != nil {
		return err
	}
	defer coordinator.Close()
	rateLimiter, ok := coordinator.(coordination.RateLimiter)
	if !ok {
		return errors.New("coordination backend does not provide shared rate limiting")
	}
	budgetLedger, closeBudget, err := buildBudgetLedger(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeBudget()
	visibilityStore, closeVisibility, err := buildMemoryVisibility(ctx, cfg, nodeID)
	if err != nil {
		return err
	}
	defer closeVisibility()
	approvals, closeApprovals, err := buildApprovalBackend(ctx, cfg.Coordination)
	if err != nil {
		return err
	}
	defer closeApprovals()
	runtimeManager := agentruntime.NewManagerWithBudgetAndVisibility(budgetLedger, visibilityStore)
	defer runtimeManager.Close()
	metricsExporter := metrics.NewMetrics()
	restoreStorageMetrics := observability.SetStorageMetrics(metricsExporter)
	defer restoreStorageMetrics()
	toolOperations, closeToolOperations, err := buildToolOperationLedger(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeToolOperations()
	safetyChecker, closeSafety, safetyProbe, err := buildContentSafety(ctx, cfg, nodeID)
	if err != nil {
		return err
	}
	defer closeSafety()
	secretValues := configuredSecrets(cfg)
	auditSink, closeAuditSink, err := buildAuditSink(ctx, &effectiveConfig, cfg)
	if err != nil {
		return err
	}
	defer closeAuditSink()
	auditRouter := auditlog.NewRouterWithSQL(tenantRegistry, os.Stdout, secretValues, auditSink)
	defer auditRouter.Close()
	stopAuditDrain := func() {}
	if auditSink != nil {
		stopAuditDrain = auditSink.StartDrain(ctx, 5*time.Second)
	}
	defer stopAuditDrain()
	workerService := worker.NewService(
		runtimeManager, coordinator, channelRegistry, governance.NewFilterWithRateLimiter(rateLimiter), auditRouter, metricsExporter,
		worker.Options{
			LockTTL: cfg.Coordination.LockTTL, DedupTTL: cfg.Coordination.DedupTTL,
			ToolOperations: toolOperations,
			Approvals:      approvals,
			Budget:         budgetLedger,
			Safety:         safetyChecker,
			SafetyPolicy:   cfg.ContentSafety.PolicyVersion,
		},
	)
	workQueue, queueCloser, err := buildQueue(ctx, cfg, workerService, tenantRegistry, metricsExporter)
	if err != nil {
		return err
	}
	defer func() { _ = queueCloser.Close() }()
	healthRegistry := health.NewRegistry(cfg.Health.ProbeInterval, cfg.Health.ProbeTimeout, metricsExporter)
	if readiness, ok := workQueue.(queue.ReadinessProbe); ok {
		_ = healthRegistry.Add(health.Probe{Name: "queue", Backend: cfg.Queue.Backend, Required: true, Check: readiness.Ready})
	}
	_ = healthRegistry.Add(health.Probe{Name: "control_plane", Backend: cfg.ControlPlane.Backend, Required: true, Check: func(context.Context) error {
		if !controlController.Ready() {
			return errors.New("control plane not synchronized")
		}
		return nil
	}})
	if pinger, ok := coordinator.(interface{ Ping(context.Context) error }); ok {
		_ = healthRegistry.Add(health.Probe{Name: "coordination", Backend: cfg.Coordination.Backend, Required: true, Check: pinger.Ping})
	}
	if safetyProbe != nil {
		_ = healthRegistry.Add(health.Probe{Name: "content_safety", Backend: cfg.ContentSafety.Backend, Required: true, Check: safetyProbe})
	}
	if auditSink != nil {
		_ = healthRegistry.Add(health.Probe{Name: "audit", Backend: "postgres", Required: true, Check: auditSink.Ready})
	}
	// Data backends are created lazily by the runtime manager. Probe the
	// configured stores here as well so readiness does not claim success merely
	// because the durable Inbox is reachable while Session/Memory/Artifact or
	// Vector storage is broken.
	dependencyGroups := []struct {
		name string
		pick func(config.TenantConfig) config.BackendConfig
	}{
		{name: "session", pick: func(t config.TenantConfig) config.BackendConfig { return t.Data.Session }},
		{name: "memory", pick: func(t config.TenantConfig) config.BackendConfig { return t.Data.Memory }},
		{name: "summary", pick: func(t config.TenantConfig) config.BackendConfig { return t.Data.Summary }},
		{name: "artifact", pick: func(t config.TenantConfig) config.BackendConfig { return t.Data.Artifact }},
		{name: "knowledge", pick: func(t config.TenantConfig) config.BackendConfig { return t.Data.Knowledge }},
	}
	for _, group := range dependencyGroups {
		group := group
		if !hasConfiguredBackend(effectiveConfig.Tenants, group.pick) {
			continue
		}
		_ = healthRegistry.Add(health.Probe{
			// Local/demo keeps lazily configured showcase tenants usable without
			// starting every optional backend. Production makes every configured
			// data path a hard readiness dependency.
			Name: group.name, Backend: "configured", Required: cfg.DeploymentMode == "production",
			Check: func(probeCtx context.Context) error {
				return probeBackendGroup(probeCtx, effectiveConfig.Tenants, group.pick)
			},
		})
	}
	if cfg.Telemetry.OTLPEndpoint != "" {
		_ = healthRegistry.Add(health.Probe{
			Name: "telemetry", Backend: "otlp", Required: false,
			Check: func(probeCtx context.Context) error {
				return probeEndpoint(probeCtx, cfg.Telemetry.OTLPEndpoint, cfg.Telemetry.OTLPProtocol)
			},
		})
	}
	healthRegistry.SetGate("not_draining", true)
	healthRegistry.Start(ctx)
	defer healthRegistry.Close()
	wecomAIBots, err := aibotbridge.Start(ctx, tenantRegistry, workQueue, wecomAIBotAdapter)
	if err != nil {
		return err
	}
	defer func() { _ = wecomAIBots.Close() }()
	gatewayServer := web.NewServer(
		tenantRegistry, channelRegistry, workQueue, workerService, metricsExporter,
		cfg.Server.AdminTokenEnv, configPath, &effectiveConfig,
	)
	gatewayServer.SetControlPlane(controlController)
	gatewayServer.SetHealthRegistry(healthRegistry)
	if auditSink != nil {
		gatewayServer.SetAdminAudit(func(auditCtx context.Context, principal adminauth.Principal, action adminauth.Action, tenantID, operation, outcome string) error {
			if tenantID == "" {
				tenantID = "__admin__"
			}
			traceID := ""
			if span := oteltrace.SpanFromContext(auditCtx); span.SpanContext().IsValid() {
				traceID = span.SpanContext().TraceID().String()
			}
			return auditSink.WriteContext(auditCtx, auditlog.Entry{
				Timestamp: time.Now().UTC(), TenantID: tenantID,
				UserID: adminauth.SubjectHash(principal.Subject), Decision: outcome,
				Reason: string(action), OperationKey: operation, OperationPhase: "admin",
				OperationState: outcome, TraceID: traceID, ConfigRevision: "admin-control-plane",
			})
		})
	}
	if cfg.Admin.Mode == "oidc" {
		oidc, oidcErr := adminauth.NewOIDC(adminauth.OIDCOptions{
			Issuer: cfg.Admin.Issuer, Audience: cfg.Admin.Audience, JWKSURL: cfg.Admin.JWKSURL,
			RolesClaim: cfg.Admin.RolesClaim, TenantsClaim: cfg.Admin.TenantsClaim,
			ClockSkew: cfg.Admin.ClockSkew, CacheTTL: cfg.Admin.JWKSCacheTTL,
		})
		if oidcErr != nil {
			return oidcErr
		}
		gatewayServer.SetAdminAuthenticator(oidc)
	}
	httpServer := &http.Server{
		Addr: cfg.Server.Address, Handler: gatewayServer.Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second,
		IdleTimeout: 60 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Printf("trpc-agent-service listening on %s", cfg.Server.Address)
		errCh <- httpServer.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
		gatewayServer.SetDraining()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		return httpServer.Shutdown(shutdownCtx)
	case serveErr := <-errCh:
		if errors.Is(serveErr, http.ErrServerClosed) {
			return nil
		}
		return serveErr
	}
}

func buildMemoryVisibility(ctx context.Context, cfg *config.Config, nodeID string) (memoryvisibility.Store, func(), error) {
	if cfg == nil {
		return nil, func() {}, errors.New("memory visibility config is unavailable")
	}
	needsStore := false
	for _, tenant := range cfg.Tenants {
		if tenant.Data.Memory.Type != "disabled" {
			needsStore = true
			break
		}
	}
	if !needsStore {
		return nil, func() {}, nil
	}
	if cfg.Queue.Backend != "postgres" {
		return memoryvisibility.NewMemory(nodeID), func() {}, nil
	}
	dsn, err := config.Secret(cfg.Queue.DSNEnv)
	if err != nil {
		return nil, func() {}, err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, func() {}, errors.New("create memory visibility database pool")
	}
	closePool := func() { pool.Close() }
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	pingErr := pool.Ping(pingCtx)
	cancel()
	if pingErr != nil {
		closePool()
		return nil, func() {}, errors.New("memory visibility database is unavailable")
	}
	if err := migrations.VerifyAll(ctx, pool); err != nil {
		closePool()
		return nil, func() {}, err
	}
	store, err := memoryvisibility.NewPostgres(pool, fmt.Sprintf("%s-%d", nodeID, time.Now().UnixNano()))
	if err != nil {
		closePool()
		return nil, func() {}, err
	}
	return store, closePool, nil
}

func buildContentSafety(ctx context.Context, cfg *config.Config, nodeID string) (contentsafety.Checker, func(), func(context.Context) error, error) {
	if cfg == nil || !cfg.ContentSafety.Enabled || cfg.ContentSafety.Backend == "disabled" {
		return nil, func() {}, nil, nil
	}
	if cfg.ContentSafety.Backend == "memory" {
		return contentsafety.NewMemory(nil), func() {}, func(context.Context) error { return nil }, nil
	}
	if cfg.ContentSafety.Backend != "postgres" || cfg.Queue.Backend != "postgres" {
		return nil, func() {}, nil, errors.New("content safety requires postgres queue backend")
	}
	dsn, err := config.Secret(cfg.Queue.DSNEnv)
	if err != nil {
		return nil, func() {}, nil, err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, func() {}, nil, errors.New("create content safety database pool")
	}
	closePool := func() { pool.Close() }
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	pingErr := pool.Ping(pingCtx)
	cancel()
	if pingErr != nil {
		closePool()
		return nil, func() {}, nil, errors.New("content safety database is unavailable")
	}
	if err := migrations.VerifyAll(ctx, pool); err != nil {
		closePool()
		return nil, func() {}, nil, err
	}
	checker, err := contentsafety.NewPostgres(pool, nodeID, cfg.ContentSafety.LeaseTTL, nil)
	if err != nil {
		closePool()
		return nil, func() {}, nil, err
	}
	probe := func(probeCtx context.Context) error {
		return pool.Ping(probeCtx)
	}
	return checker, closePool, probe, nil
}

func buildAuditSink(ctx context.Context, effective *config.Config, static *config.Config) (*auditlog.PostgresSink, func(), error) {
	if effective == nil || static == nil {
		return nil, func() {}, errors.New("audit configuration is unavailable")
	}
	needsSQL := false
	spoolRoot := "data/audit-spool"
	for _, tenant := range effective.Tenants {
		if tenant.Audit.Enabled && tenant.Audit.Sink == "sql" {
			needsSQL = true
			if tenant.Audit.SpoolPath != "" {
				spoolRoot = tenant.Audit.SpoolPath
			}
		}
	}
	if !needsSQL {
		return nil, func() {}, nil
	}
	if static.Queue.Backend != "postgres" || static.Queue.DSNEnv == "" {
		return nil, func() {}, errors.New("sql audit sink requires queue postgres")
	}
	dsn, err := config.Secret(static.Queue.DSNEnv)
	if err != nil {
		return nil, func() {}, err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, func() {}, errors.New("create audit database pool")
	}
	closePool := func() { pool.Close() }
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	pingErr := pool.Ping(pingCtx)
	cancel()
	if pingErr != nil {
		closePool()
		return nil, func() {}, errors.New("audit database is unavailable")
	}
	if err := migrations.VerifyAll(ctx, pool); err != nil {
		closePool()
		return nil, func() {}, err
	}
	sink, err := auditlog.NewPostgresSink(pool, spoolRoot)
	if err != nil {
		closePool()
		return nil, func() {}, err
	}
	if err := sink.Drain(ctx); err != nil {
		closePool()
		return nil, func() {}, fmt.Errorf("drain audit spool: %w", err)
	}
	return sink, closePool, nil
}

func buildControlStore(ctx context.Context, cfg *config.Config) (configcontrol.Store, func(), error) {
	if cfg == nil {
		return nil, func() {}, errors.New("control plane config is unavailable")
	}
	switch cfg.ControlPlane.Backend {
	case "", "inmemory", "memory":
		return configcontrol.NewMemoryStore(), func() {}, nil
	case "postgres":
		dsn, err := config.Secret(cfg.Queue.DSNEnv)
		if err != nil {
			return nil, func() {}, err
		}
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return nil, func() {}, errors.New("create control plane database pool")
		}
		closePool := func() { pool.Close() }
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		pingErr := pool.Ping(pingCtx)
		cancel()
		if pingErr != nil {
			closePool()
			return nil, func() {}, errors.New("control plane database is unavailable")
		}
		if err := migrations.VerifyAll(ctx, pool); err != nil {
			closePool()
			return nil, func() {}, err
		}
		return configcontrol.NewPostgresWithTTL(pool, cfg.ControlPlane.HeartbeatTTL), closePool, nil
	default:
		return nil, func() {}, errors.New("unsupported control plane backend")
	}
}

func bootstrapAndLoadTenants(ctx context.Context, cfg *config.Config, store configcontrol.Store) ([]config.TenantConfig, error) {
	states, err := store.ListStates(ctx)
	if err != nil {
		return nil, err
	}
	if len(states) == 0 {
		if err := cfg.Validate(); err != nil {
			return nil, err
		}
		if err := store.Bootstrap(ctx, cfg.Tenants, "bootstrap", "initial YAML bootstrap"); err != nil {
			return nil, err
		}
		states, err = store.ListStates(ctx)
		if err != nil {
			return nil, err
		}
	}
	effective := make([]config.TenantConfig, 0, len(states))
	for _, state := range states {
		revision, err := store.GetRevision(ctx, state.TenantID, state.ActiveRevision)
		if err != nil {
			return nil, err
		}
		effective = append(effective, revision.Tenant)
	}
	return effective, nil
}

func controlNodeID(cfg *config.Config) (string, error) {
	if cfg.ControlPlane.Backend != "postgres" {
		if value := os.Getenv(cfg.ControlPlane.NodeIDEnv); value != "" {
			return value, nil
		}
		return "local", nil
	}
	if cfg.ControlPlane.NodeIDEnv == "" {
		return "", errors.New("control plane postgres requires node_id_env")
	}
	return config.Secret(cfg.ControlPlane.NodeIDEnv)
}

func buildBudgetLedger(ctx context.Context, cfg *config.Config) (budget.Ledger, func(), error) {
	if cfg == nil {
		return nil, func() {}, errors.New("budget ledger config is unavailable")
	}
	switch cfg.Queue.Backend {
	case "", "inmemory", "memory":
		// In-memory queue modes remain useful for local/demo runs, but they do
		// not claim cross-process durability. Production PostgreSQL mode below
		// never falls back to this implementation after a connection failure.
		return budget.NewMemory(), func() {}, nil
	case "postgres":
		dsn, err := config.Secret(cfg.Queue.DSNEnv)
		if err != nil {
			return nil, func() {}, err
		}
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return nil, func() {}, errors.New("create budget database pool")
		}
		closePool := func() { pool.Close() }
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		pingErr := pool.Ping(pingCtx)
		cancel()
		if pingErr != nil {
			closePool()
			return nil, func() {}, errors.New("budget database is unavailable")
		}
		if err := migrations.VerifyAll(ctx, pool); err != nil {
			closePool()
			return nil, func() {}, err
		}
		ledger, err := budget.NewPostgres(pool)
		if err != nil {
			closePool()
			return nil, func() {}, err
		}
		return ledger, closePool, nil
	default:
		return nil, func() {}, errors.New("unsupported budget ledger backend")
	}
}

func buildToolOperationLedger(
	ctx context.Context,
	cfg *config.Config,
) (tooloperation.Ledger, func(), error) {
	if cfg == nil {
		return nil, func() {}, errors.New("tool operation ledger config is unavailable")
	}
	switch cfg.Queue.Backend {
	case "", "inmemory", "memory":
		return tooloperation.NewMemory(), func() {}, nil
	case "postgres":
		dsn, err := config.Secret(cfg.Queue.DSNEnv)
		if err != nil {
			return nil, func() {}, err
		}
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return nil, func() {}, errors.New("create tool operation database pool")
		}
		closePool := func() { pool.Close() }
		if err := migrations.VerifyAll(ctx, pool); err != nil {
			closePool()
			return nil, func() {}, err
		}
		ledger, err := tooloperation.NewPostgres(pool)
		if err != nil {
			closePool()
			return nil, func() {}, errors.New("create tool operation ledger")
		}
		return ledger, closePool, nil
	default:
		return nil, func() {}, errors.New("unsupported tool operation ledger backend")
	}
}

// buildQueue assembles the message pipeline selected by cfg.Queue.Backend:
// "inmemory" keeps the legacy in-process queue; "postgres" (and "memory" for
// local runs) enables the durable Inbox/Outbox where callbacks are ACKed only
// after persistence.
func buildQueue(
	ctx context.Context,
	cfg *config.Config,
	workerService *worker.Service,
	tenants *tenant.Registry,
	exporter *metrics.Metrics,
) (queue.Dispatcher, io.Closer, error) {
	switch cfg.Queue.Backend {
	case "", "inmemory":
		q := queue.NewInProcess(workerService, cfg.Server.QueueSize, cfg.Server.WorkerCount, func(error) {
			// Provider errors can echo input or a newly rotated secret. Ordinary
			// process logs therefore expose only a stable category.
			log.Printf("worker task failed: category=processing_failed")
		})
		return q, q, nil
	case "memory":
		st := store.NewMemory()
		return newDurableQueue(cfg, st, workerService, tenants, exporter)
	case "postgres":
		dsn, err := config.Secret(cfg.Queue.DSNEnv)
		if err != nil {
			return nil, nil, err
		}
		st, err := store.NewPostgres(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		if err := st.VerifySchema(ctx); err != nil {
			_ = st.Close()
			return nil, nil, err
		}
		return newDurableQueue(cfg, st, workerService, tenants, exporter)
	default:
		return nil, nil, errors.New("unsupported queue backend")
	}
}

func newDurableQueue(
	cfg *config.Config,
	st store.Store,
	workerService *worker.Service,
	tenants *tenant.Registry,
	exporter *metrics.Metrics,
) (queue.Dispatcher, io.Closer, error) {
	d := queue.NewDurable(st, workerService, tenants, workerService, exporter, queue.DurableOptions{
		PollInterval:      cfg.Queue.PollInterval,
		LeaseTTL:          cfg.Queue.LeaseTTL,
		BatchSize:         cfg.Queue.BatchSize,
		InboxMaxAttempts:  cfg.Queue.InboxMaxAttempts,
		OutboxMaxAttempts: cfg.Queue.OutboxMaxAttempts,
		RetryBase:         cfg.Queue.RetryBase,
		RetryMax:          cfg.Queue.RetryMax,
		WorkerCount:       cfg.Server.WorkerCount,
	})
	d.Start()
	return d, d, nil
}

// sensitiveSpanPolicy drops payload-bearing attributes at their framework
// source. The Collector repeats these deletions as defense in depth, but a
// source-side rule also prevents sensitive values from being marshaled into a
// span when the application exports to a differently configured Collector.
func sensitiveSpanPolicy() atrace.Option {
	rules := []atrace.AttributePolicyOption{
		atrace.WithAttributeRule(atrace.OperationChat, atrace.AttrLLMRequest, atrace.Drop()),
		atrace.WithAttributeRule(atrace.OperationChat, atrace.AttrLLMResponse, atrace.Drop()),
		atrace.WithAttributeRule(atrace.OperationChat, atrace.AttrInputMessages, atrace.Drop()),
		atrace.WithAttributeRule(atrace.OperationChat, atrace.AttrInputMessagesOTel, atrace.Drop()),
		atrace.WithAttributeRule(atrace.OperationChat, atrace.AttrOutputMessages, atrace.Drop()),
		atrace.WithAttributeRule(atrace.OperationChat, atrace.AttrOutputMessagesOTel, atrace.Drop()),
		atrace.WithAttributeRule(atrace.OperationChat, atrace.AttributeKey("gen_ai.request.tool.definitions"), atrace.Drop()),
		atrace.WithAttributeRule(atrace.OperationInvokeAgent, atrace.AttrInputMessages, atrace.Drop()),
		atrace.WithAttributeRule(atrace.OperationInvokeAgent, atrace.AttrInputMessagesOTel, atrace.Drop()),
		atrace.WithAttributeRule(atrace.OperationInvokeAgent, atrace.AttrOutputMessages, atrace.Drop()),
		atrace.WithAttributeRule(atrace.OperationInvokeAgent, atrace.AttrOutputMessagesOTel, atrace.Drop()),
		atrace.WithAttributeRule(atrace.OperationInvokeAgent, atrace.AttributeKey("gen_ai.system_instructions"), atrace.Drop()),
		atrace.WithAttributeRule(atrace.OperationExecuteTool, atrace.AttributeKey("gen_ai.tool.call.arguments"), atrace.Drop()),
		atrace.WithAttributeRule(atrace.OperationExecuteTool, atrace.AttributeKey("gen_ai.tool.call.result"), atrace.Drop()),
		atrace.WithAttributeRule(atrace.OperationWorkflow, atrace.AttributeKey("gen_ai.workflow.request"), atrace.Drop()),
		atrace.WithAttributeRule(atrace.OperationWorkflow, atrace.AttributeKey("gen_ai.workflow.response"), atrace.Drop()),
	}
	return atrace.WithSpanAttributePolicy(rules...)
}

func buildCoordinator(cfg config.CoordinationConfig) (coordination.Coordinator, error) {
	switch cfg.Backend {
	case "inmemory":
		return coordination.NewInMemory(), nil
	case "redis":
		rawURL, err := config.Secret(cfg.RedisURLEnv)
		if err != nil {
			return nil, err
		}
		return coordination.NewRedis(rawURL, cfg.KeyPrefix)
	default:
		return nil, errors.New("unsupported coordination backend")
	}
}

func hasConfiguredBackend(tenants []config.TenantConfig, pick func(config.TenantConfig) config.BackendConfig) bool {
	for _, tenant := range tenants {
		typ := strings.ToLower(strings.TrimSpace(pick(tenant).Type))
		if typ != "" && typ != "disabled" && typ != "inmemory" {
			return true
		}
	}
	return false
}

// probeBackendGroup checks each distinct configured backend without logging
// its DSN, endpoint credentials, or provider error. The probe name is a
// stable component name; tenant and secret references are intentionally not
// exported as health labels.
func probeBackendGroup(ctx context.Context, tenants []config.TenantConfig, pick func(config.TenantConfig) config.BackendConfig) error {
	seen := make(map[string]struct{})
	for _, tenant := range tenants {
		backend := pick(tenant)
		typ := strings.ToLower(strings.TrimSpace(backend.Type))
		if typ == "" || typ == "disabled" || typ == "inmemory" {
			continue
		}
		key := strings.Join([]string{typ, backend.DSNEnv, backend.Endpoint, backend.Bucket, backend.Namespace}, "\x1f")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if err := probeBackend(ctx, backend); err != nil {
			return err
		}
	}
	return nil
}

func probeBackend(ctx context.Context, backend config.BackendConfig) error {
	switch strings.ToLower(strings.TrimSpace(backend.Type)) {
	case "sql", "vector":
		return probePostgresReference(ctx, backend.DSNEnv)
	case "redis":
		return probeRedisReference(ctx, backend.DSNEnv)
	case "object":
		if err := probePostgresReference(ctx, backend.DSNEnv); err != nil {
			return err
		}
		return probeObjectStore(ctx, backend)
	case "external":
		return probeEndpoint(ctx, backend.Endpoint, "http")
	default:
		return errors.New("configured dependency backend is unavailable")
	}
}

func probePostgresReference(ctx context.Context, reference string) error {
	if strings.TrimSpace(reference) == "" {
		return errors.New("postgres dependency reference is missing")
	}
	dsn, err := config.Secret(reference)
	if err != nil || strings.TrimSpace(dsn) == "" {
		return errors.New("postgres dependency secret is unavailable")
	}
	pool, err := pgxpool.New(ctxOrBackground(ctx), dsn)
	if err != nil {
		return errors.New("postgres dependency is unavailable")
	}
	defer pool.Close()
	if err := pool.Ping(ctxOrBackground(ctx)); err != nil {
		return errors.New("postgres dependency is unavailable")
	}
	return nil
}

func probeRedisReference(ctx context.Context, reference string) error {
	if strings.TrimSpace(reference) == "" {
		return errors.New("redis dependency reference is missing")
	}
	rawURL, err := config.Secret(reference)
	if err != nil || strings.TrimSpace(rawURL) == "" {
		return errors.New("redis dependency secret is unavailable")
	}
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return errors.New("redis dependency configuration is invalid")
	}
	client := redis.NewClient(options)
	defer client.Close()
	if err := client.Ping(ctxOrBackground(ctx)).Err(); err != nil {
		return errors.New("redis dependency is unavailable")
	}
	return nil
}

func probeObjectStore(ctx context.Context, backend config.BackendConfig) error {
	options := []s3storage.ClientBuilderOpt{s3storage.WithBucket(backend.Bucket), s3storage.WithRetries(1)}
	if backend.Endpoint != "" {
		options = append(options, s3storage.WithEndpoint(backend.Endpoint))
	}
	if backend.Region != "" {
		options = append(options, s3storage.WithRegion(backend.Region))
	}
	if backend.PathStyle {
		options = append(options, s3storage.WithPathStyle(true))
	}
	if backend.AccessKeyEnv != "" {
		accessKey, accessErr := config.Secret(backend.AccessKeyEnv)
		secretKey, secretErr := config.Secret(backend.SecretKeyEnv)
		if accessErr != nil || secretErr != nil || accessKey == "" || secretKey == "" {
			return errors.New("object storage credentials are unavailable")
		}
		options = append(options, s3storage.WithCredentials(accessKey, secretKey))
	}
	if backend.SessionTokenEnv != "" {
		token, err := config.Secret(backend.SessionTokenEnv)
		if err != nil || token == "" {
			return errors.New("object storage session credential is unavailable")
		}
		options = append(options, s3storage.WithSessionToken(token))
	}
	client, err := s3storage.NewClient(ctxOrBackground(ctx), options...)
	if err != nil {
		return errors.New("object storage is unavailable")
	}
	defer client.Close()
	if _, err := client.ListObjects(ctxOrBackground(ctx), ""); err != nil {
		return errors.New("object storage is unavailable")
	}
	return nil
}

func probeEndpoint(ctx context.Context, endpoint, protocol string) error {
	raw := strings.TrimSpace(endpoint)
	if raw == "" {
		return errors.New("dependency endpoint is missing")
	}
	defaultPort := "80"
	if strings.EqualFold(protocol, "https") {
		defaultPort = "443"
	} else if strings.EqualFold(protocol, "grpc") || strings.EqualFold(protocol, "grpc-gateway") {
		defaultPort = "4317"
	} else if strings.EqualFold(protocol, "http") && strings.Contains(raw, "otel") {
		defaultPort = "4318"
	}
	host, port := "", ""
	if parsed, err := url.Parse(raw); err == nil && parsed.Host != "" {
		host, port = parsed.Hostname(), parsed.Port()
		if port == "" && parsed.Scheme != "" {
			if parsed.Scheme == "https" {
				port = "443"
			} else {
				port = defaultPort
			}
		}
	} else {
		hostPort := strings.SplitN(strings.TrimPrefix(strings.TrimPrefix(raw, "http://"), "https://"), "/", 2)[0]
		if parsedHost, parsedPort, splitErr := net.SplitHostPort(hostPort); splitErr == nil {
			host, port = parsedHost, parsedPort
		} else {
			host, port = hostPort, defaultPort
		}
	}
	if host == "" {
		return errors.New("dependency endpoint is invalid")
	}
	if port == "" {
		port = defaultPort
	}
	dialer := net.Dialer{Timeout: 1 * time.Second}
	conn, err := dialer.DialContext(ctxOrBackground(ctx), "tcp", net.JoinHostPort(host, port))
	if err != nil {
		return errors.New("dependency endpoint is unavailable")
	}
	return conn.Close()
}

func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func configuredSecrets(cfg *config.Config) []string {
	values := make([]string, 0)
	for _, name := range cfg.SecretEnvNames() {
		// Resolve references through the configured provider as well as plain
		// environment variables. This keeps file/Secret-Manager backed values
		// in the in-process redaction set without ever serializing the value.
		if value, err := config.Secret(name); err == nil && value != "" {
			values = append(values, value)
		}
	}
	return values
}
