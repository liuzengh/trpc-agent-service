// Command trpc-service starts the multi-tenant Agent platform.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/app/web"
	"github.com/liuzengh/trpc-agent-service/trpcservice/app/worker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/llm"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/member"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/skill"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/config"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/health"
	srvlog "github.com/liuzengh/trpc-agent-service/trpcservice/infra/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/agentstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/bindingstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/dlqstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/knowledgestore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/ledgerstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/llmstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/skillstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/tenantstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage/toolstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/workspace"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	fmetric "trpc.group/trpc-go/trpc-agent-go/telemetry/metric"
	ftrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"
	fwtool "trpc.group/trpc-go/trpc-agent-go/tool"
	fwtoolcodeexec "trpc.group/trpc-go/trpc-agent-go/tool/codeexec"
)

// dockerExec runs agent code blocks in disposable containers (the code-exec
// built-in tool). Constructed once; the docker daemon is only contacted on
// the first execution.
var dockerExec = workspace.NewDockerExecutor()

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func main() {
	role := flag.String("role", "all", "service role: gateway|worker|admin|all")
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	fmt.Printf("trpc-agent-service %s (role=%s)\n", trpcservice.Version, *role)

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Printf("config load failed, using defaults: %v\n", err)
		cfg = config.Default()
	}
	cfg.Role = *role

	logger := srvlog.New(cfg.Log.Level)
	slog.SetDefault(logger)

	cleanupTelemetry := setupTelemetry(cfg.Telemetry, logger)
	defer cleanupTelemetry()

	// Signal-driven shutdown: SIGINT/SIGTERM cancels every background loop
	// (worker consumer, outbox dispatcher, IM gateway) and lets the HTTP
	// server drain before the deferred audit/telemetry cleanup runs.
	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	mux.Handle("/healthz", health.Handler())

	// With a MySQL DSN configured, the management domains (tenants, endpoints,
	// tools, agents) persist across restarts; otherwise they stay in memory.
	var db *sql.DB
	if cfg.MySQL.DSN != "" {
		db, err = storage.OpenMySQL(cfg.MySQL.DSN)
		if err != nil {
			// A configured-but-unreachable MySQL is a degraded state, not a
			// silent fallback: persistence, audit and the worker all stay off.
			// Make it loud (error log + /healthz 503) so a transient outage at
			// startup is visible instead of running in-memory unnoticed.
			logger.Error("mysql unavailable, running degraded (no persistence/audit/worker)", "err", err)
			health.SetDegraded("mysql unavailable")
		}
	}
	var reg *llm.Registry
	var tenantMgr *tenant.Manager
	var agentMgr *agent.Manager
	var toolReg *tool.Registry
	var kbMgr *knowledge.Manager
	var skillMgr *skill.Manager
	if db != nil {
		reg = llmstore.NewMySQLRegistry(db, nil)
		tenantMgr = tenantstore.NewMySQLManager(db)
	} else {
		reg = llm.NewRegistry(nil)
		tenantMgr = tenant.NewManager()
	}

	// Per-tenant data-backend router: session / memory / vector / artifact /
	// audit are selected per tenant via Tenant.DataBackend (summary follows the
	// session backend). Defaults here match the production backends.
	router := storage.NewRouter(tenantMgr,
		storage.SessionConfig{Backend: storage.BackendRedis, RedisURL: cfg.Redis.URL, MySQLDSN: cfg.MySQL.DSN},
		storage.MemoryConfig{Backend: storage.BackendRedis, RedisURL: cfg.Redis.URL, MySQLDSN: cfg.MySQL.DSN},
	)

	// Tenant-routed vector-store factory: Milvus by default, in-memory opt-in.
	var milvusVSF knowledge.VectorStoreFactory
	if cfg.Milvus.Address != "" {
		milvusVSF = knowledgestore.MilvusVectorStoreFactory(cfg.Milvus.Address, cfg.Milvus.Username, cfg.Milvus.Password)
	} else {
		slog.Warn("milvus address not configured, knowledge bases stay in memory")
	}
	vsf := storage.NewRouterVectorFactory(router, milvusVSF, knowledge.InMemoryVectorStoreFactory())

	if db != nil {
		agentMgr = agentstore.NewMySQLManager(db, reg)
		toolReg = toolstore.NewMySQLRegistry(db)
		kbMgr = knowledgestore.NewMySQLManager(db, vsf, knowledge.RegistryEmbedderFactory(reg))
		skillMgr = skillstore.NewMySQLManager(db)
	} else {
		agentMgr = agent.NewManager(reg)
		toolReg = tool.NewRegistry()
		kbMgr = knowledge.NewManager(vsf, knowledge.RegistryEmbedderFactory(reg))
		skillMgr = skill.NewManager()
	}
	registerBuiltinTools(toolReg)

	// Member management + auth. With MySQL we read tenant_members; without
	// MySQL we fall back to an in-memory store for local development.
	var memberMgr *member.Manager
	if db != nil {
		memberMgr = member.NewManagerWithStore(member.NewMySQLStore(db))
	} else {
		memberMgr = member.NewManager()
	}
	if err := web.EnsureInitialOwner(
		runCtx,
		tenantMgr,
		memberMgr,
		envOrDefault("ADMIN_TENANT_ID", "t-demo"),
		envOrDefault("ADMIN_USER_ID", "admin"),
		envOrDefault("ADMIN_PASSWORD", "admin123"),
	); err != nil {
		logger.Warn("initial owner bootstrap failed", "err", err)
	}

	// Audit recorder writes governance decisions + per-request accounting.
	// Per-tenant routing (MySQL default, in-memory opt-in) applies when MySQL
	// is up; without MySQL audit stays disabled. The concrete MySQL recorder
	// also serves the admin read side (GET /audit /usage).
	var auditor audit.Recorder
	var auditRec *audit.MySQLRecorder
	if db != nil {
		auditRec = audit.NewMySQL(db)
		auditor = storage.NewRouterAuditRecorder(router, auditRec, nil)
		defer func() { _ = auditRec.Close() }()
	}

	// IM channel bindings (wecom/feishu account -> tenant + agent). The store
	// is MySQL when available, in-memory otherwise; this is pure admin config,
	// independent of the worker.
	var bindStore channels.BindingStore
	if db != nil {
		bindStore = bindingstore.NewMySQLBindingStore(db)
	} else {
		bindStore = channels.NewMemBindingStore()
	}

	// Unified credential store. Master key comes from env (wins) or config.
	// With MySQL the store is disabled when no master key is present, so
	// plaintext credentials are never written at rest; without MySQL a
	// plaintext in-memory store serves dev.
	var secretStore secret.Store
	masterKey := os.Getenv("TRPC_SECRET_MASTER_KEY")
	if masterKey == "" {
		masterKey = cfg.Secret.MasterKey
	}
	if db != nil {
		if masterKey == "" {
			logger.Warn("secret master key missing, credential store disabled")
		} else if s, err := secret.NewMySQLStore(db, masterKey); err != nil {
			logger.Error("secret store unavailable", "err", err)
		} else {
			secretStore = s
		}
	} else {
		secretStore = secret.NewMemStore()
	}
	// Models resolve APIKeyRef via the credential store (nil store keeps the
	// legacy plaintext APIKey path). The same store receives plaintext API
	// keys at write time so plaintext never reaches the durable endpoint row.
	reg.SetKeySource(secretStore)
	reg.SetKeySink(secretStore)

	// Auth middleware: wraps all API routes. Skip paths are unauthenticated.
	jwtSecret := os.Getenv("TRPC_JWT_SECRET")
	if jwtSecret == "" {
		jwtSecret = cfg.Secret.MasterKey // fallback to master key if no dedicated JWT secret
	}
	if jwtSecret == "" {
		jwtSecret = "dev-jwt-secret-change-me" // dev mode only
	}
	authMW := web.NewAuthMiddleware(memberMgr, jwtSecret)

	web.NewTenantAPI(tenantMgr).Register(mux)
	web.NewMemberAPI(memberMgr).Register(mux)
	agentAPI := web.NewAgentAPI(agentMgr)
	agentAPI.SetGrants(toolReg, skillMgr)
	agentAPI.Register(mux)
	web.NewEndpointAPI(reg).Register(mux)
	web.NewToolAPI(toolReg).Register(mux)
	web.NewKnowledgeAPI(kbMgr).Register(mux)
	web.NewSkillAPI(skillMgr).Register(mux)
	channelAPI := web.NewChannelAPI(bindStore)
	channelAPI.Register(mux)
	if secretStore != nil {
		web.NewSecretAPI(secretStore).Register(mux)
	}
	if auditRec != nil {
		web.NewAuditAPI(auditRec).Register(mux)
		web.NewUsageAPI(auditRec).Register(mux)
	}

	// Login is unauthenticated; registration and session validation are
	// protected by the outer auth middleware.
	mux.HandleFunc("/auth/login", authMW.Login)
	mux.HandleFunc("/auth/register", authMW.Register)
	mux.HandleFunc("/auth/me", authMW.Me)

	// Worker + outbox dispatcher + IM gateway: started when the role includes
	// worker and both Redis and MySQL are configured.
	startRuntime(runCtx, cfg, db, router, tenantMgr, agentMgr, toolReg, kbMgr, skillMgr, auditor, auditRec, secretStore, bindStore, mux, channelAPI, logger)

	logger.Info("starting server", "addr", cfg.Server.HTTPAddr, "role", cfg.Role)
	// Middleware order matters: CORS is outermost so that every response —
	// including auth rejections (401/403) — carries the CORS headers the
	// browser needs to read the status instead of reporting a network error.
	// The auth middleware then lets preflights through and enforces the Bearer
	// token: /healthz and /auth/login are public, everything else needs a token.
	skipAuth := []string{"/healthz", "/auth/login"}
	srv := &http.Server{
		Addr:    cfg.Server.HTTPAddr,
		Handler: web.CORS(authMW.Wrap(skipAuth, web.RequireRoutePermission(mux))),
	}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.ListenAndServe() }()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) && runCtx.Err() == nil {
			logger.Error("server stopped", "err", err)
			os.Exit(1)
		}
	case <-runCtx.Done():
		logger.Info("shutting down: signal received")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Warn("http server drain", "err", err)
		}
	}
}

// startRuntime wires and starts the worker, outbox dispatcher and IM gateway
// when the role includes worker and both Redis and MySQL are configured (the
// bus needs Redis, the outbox MySQL). Extracted from main so the composition
// root stays a thin assembly over the wiring below. router is the shared
// per-tenant data-backend router built in main (session/memory defaults come
// from config; vector/artifact/audit dispatchers hang off the same tenant
// selections).
func startRuntime(runCtx context.Context, cfg *config.Config, db *sql.DB, router *storage.Router,
	tenantMgr *tenant.Manager, agentMgr *agent.Manager, toolReg *tool.Registry,
	kbMgr *knowledge.Manager, skillMgr *skill.Manager, auditor audit.Recorder,
	auditRec *audit.MySQLRecorder, secretStore secret.Store,
	bindStore channels.BindingStore, mux *http.ServeMux,
	channelAPI *web.ChannelAPI, logger *slog.Logger,
) {
	if !isWorkerRole(cfg.Role) || db == nil || cfg.Redis.URL == "" {
		return
	}
	rb, err := bus.NewRedisFromURL(cfg.Redis.URL)
	if err != nil {
		logger.Error("redis bus unavailable, worker disabled", "err", err)
		return
	}

	outbox := bus.NewOutbox(db)

	// Dead-letter policy: after 5 business failures an inbound message is
	// moved out of the live stream (Redis DLQ copy) and persisted to MySQL so
	// an operator can inspect and replay it via the admin API.
	dlqStore := dlqstore.NewStore(db)
	rb.SetDeadLetter(bus.DefaultMaxDeliveries, func(ctx context.Context, e bus.DeadLetter) {
		entry := dlqstore.Entry{
			MessageID:   e.MessageID,
			StreamEntry: e.StreamEntry,
			TenantID:    e.TenantID,
			AgentID:     e.AgentID,
			SessionID:   e.SessionID,
			Channel:     e.Channel,
			UserID:      e.UserID,
			TraceID:     e.TraceID,
			Payload:     e.Payload,
			FailReason:  e.FailReason,
			Attempts:    e.Attempts,
		}
		if err := dlqStore.Record(ctx, entry); err != nil {
			slog.Error("dlq persistence failed (redis copy retained)", "message", e.MessageID, "err", err)
		}
	})
	web.NewDLQAPI(dlqStore, rb).Register(mux)

	// Artifact persistence on MinIO when configured; without it the runner
	// just does not persist artifacts. When MinIO is up, per-tenant routing
	// applies (a tenant may opt into the in-memory artifact backend); with no
	// MinIO the domain stays disabled rather than silently degrading to an
	// ephemeral in-memory store.
	var artSvc artifact.Service
	if cfg.MinIO.Endpoint != "" {
		bucket := cfg.MinIO.Bucket
		if bucket == "" {
			bucket = "artifacts"
		}
		svc, err := storage.NewMinioArtifactService(context.Background(),
			cfg.MinIO.Endpoint, cfg.MinIO.AccessKey, cfg.MinIO.SecretKey, bucket, cfg.MinIO.UseSSL)
		if err != nil {
			logger.Error("minio unavailable, artifact persistence disabled", "err", err)
		} else {
			artSvc = storage.NewRouterArtifactService(router, svc, nil)
			logger.Info("artifact persistence enabled", "endpoint", cfg.MinIO.Endpoint, "bucket", bucket)
		}
	}

	// Data-domain assembly point: session/memory/vector/artifact/audit all
	// resolve per tenant through the Router (or a Router-wrapped dispatcher);
	// knowledge keeps its manager (metadata + routed vector stores). Summary
	// has no standalone domain (lives in the session backend).
	dss := storage.NewDataStores(router, kbMgr, artSvc, auditor)

	// Admin chat rides the same worker pipeline: POST /chat publishes inbound,
	// replies come back over outbound and are SSE-forwarded.
	web.NewChatAPI(rb).Register(mux)
	// Business conversation ledger writes every turn (USER+ASSISTANT) for the
	// session-history API; it shares the worker's MySQL.
	ledger := ledgerstore.NewMySQLLedger(db)
	web.NewChatHistoryAPI(ledger).Register(mux)

	w := worker.New(rb, agentMgr, worker.NewToolResolver(toolReg, builtinToolSource, dss.Knowledge), outbox, dss.Router, skillMgr, dss.Auditor, dss.Artifacts, ledger)
	// Tenant governance: per-tenant quota + audit_policy come from the tenants
	// table (configured in the tenant UI); budgets apply only when a tenant
	// sets quota.token_quota > 0. The token meter reads usage_records when the
	// audit recorder is wired; without it no budget is ever enforced.
	var usageFn func(ctx context.Context, tenantID string) (int64, error)
	if auditRec != nil {
		usageFn = func(ctx context.Context, tenantID string) (int64, error) {
			sums, err := auditRec.UsageSummary(ctx, audit.UsageQuery{TenantID: tenantID, Dimension: audit.UsageDimensionToken})
			if err != nil {
				return 0, err
			}
			var total int64
			for _, s := range sums {
				total += int64(s.Total)
			}
			return total, nil
		}
	}
	w.SetGovernance(tenantMgr, usageFn)

	go func() {
		if err := w.Run(runCtx); err != nil {
			logger.Error("worker stopped", "err", err)
		}
	}()
	go func() {
		if err := outbox.Run(runCtx, rb, time.Second); err != nil {
			logger.Error("outbox dispatcher stopped", "err", err)
		}
	}()

	// IM gateway: binding-driven connection manager. Reload connects each bound
	// account at startup; ChannelAPI reconciles live adapters on every binding
	// create/delete.
	imMgr := channels.NewManager(rb, bindStore, secretStore, buildAdapter)
	if cfg.RateLimit.Enable && cfg.Redis.URL != "" {
		// Inbound rate limiting shares the bus's Redis so the limit holds
		// across gateway nodes; a limiter error fails open.
		if rcli, err := bus.NewRedisFromURL(cfg.Redis.URL); err != nil {
			logger.Error("rate limiter unavailable, inbound limiting disabled", "err", err)
		} else {
			imMgr.SetRateLimiter(channels.NewRedisRateLimiter(rcli.Client(), cfg.RateLimit.PerMinute, time.Minute))
			logger.Info("IM inbound rate limiting enabled", "per_minute", cfg.RateLimit.PerMinute)
		}
	}
	channelAPI.SetManager(imMgr)
	if err := imMgr.Reload(context.Background()); err != nil {
		logger.Error("IM gateway reload failed", "err", err)
	}
	go func() {
		if err := imMgr.Run(runCtx); err != nil {
			logger.Error("IM gateway stopped", "err", err)
		}
	}()
	logger.Info("worker started", "group", worker.Group)
}

// setupTelemetry wires OpenTelemetry trace + metrics when an OTLP endpoint is
// configured; otherwise it binds the platform metrics to the noop provider so
// recording stays safe. The returned cleanup shuts the tracer down.
func setupTelemetry(t config.TelemetryConfig, logger *slog.Logger) func() {
	metrics.Init() // bind to the noop provider first
	if t.OTLPEndpoint == "" {
		return func() {}
	}
	serviceName := t.ServiceName
	if serviceName == "" {
		serviceName = "trpc-agent-service"
	}
	ctx := context.Background()

	cleanup, err := ftrace.Start(ctx,
		ftrace.WithEndpoint(t.OTLPEndpoint),
		ftrace.WithServiceName(serviceName),
	)
	if err != nil {
		logger.Error("trace init failed", "err", err)
		return func() {}
	}

	mp, err := fmetric.NewMeterProvider(ctx,
		fmetric.WithEndpoint(t.OTLPEndpoint),
		fmetric.WithServiceName(serviceName),
	)
	if err != nil {
		logger.Error("metric provider init failed", "err", err)
		return func() { _ = cleanup() }
	}
	if err := fmetric.InitMeterProvider(mp); err != nil {
		logger.Error("metric init failed", "err", err)
		return func() { _ = cleanup() }
	}
	metrics.Init() // re-bind platform metrics to the real provider
	return func() { _ = cleanup() }
}

// isWorkerRole reports whether the role runs message workers.
func isWorkerRole(role string) bool {
	return role == "worker" || role == "all"
}

// builtinTool couples a built-in tool's registry definition with its runtime
// factory, so both the registration loop and the tool-source lookup read one
// table — adding a built-in tool no longer touches an if-chain.
type builtinTool struct {
	def     tool.Definition
	factory func() fwtool.Tool
}

// builtinTools is the platform's built-in tool catalog. Builtin tools are
// global (no scope), so every tenant sees them. Code execution is inherently
// dangerous: risk_level=high makes every code-exec call require human approval
// (approval rail 2), independent of per-agent config.
var builtinTools = []builtinTool{
	{
		def:     tool.Definition{ID: "echo", Name: "echo", Description: "Returns the input text unchanged.", RiskLevel: tool.RiskLow},
		factory: tool.EchoTool,
	},
	{
		def:     tool.Definition{ID: "get-current-time", Name: "get_current_time", Description: "Returns the current date and time.", RiskLevel: tool.RiskLow},
		factory: tool.CurrentTimeTool,
	},
	{
		def:     tool.Definition{ID: "code-exec", Name: "execute_code", Description: "Execute Python or Bash code in an isolated container and return the output. High risk: human approval required before each run.", RiskLevel: tool.RiskHigh},
		factory: func() fwtool.Tool { return fwtoolcodeexec.NewTool(dockerExec) },
	},
}

// builtinToolSource resolves built-in tool ids to their implementations.
func builtinToolSource(id string) (fwtool.Tool, bool) {
	for _, bt := range builtinTools {
		if bt.def.ID == id {
			return bt.factory(), true
		}
	}
	return nil, false
}

// registerBuiltinTools registers the platform's built-in tools.
func registerBuiltinTools(reg *tool.Registry) {
	for _, bt := range builtinTools {
		if err := reg.Register(context.Background(), bt.def); err != nil {
			slog.Error("register builtin tool failed", "tool", bt.def.ID, "err", err)
		}
	}
}
