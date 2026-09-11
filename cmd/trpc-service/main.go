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

// devJWTSecret is the signing secret used only when no secret is configured.
// It is public knowledge (it lives in the source), so it must never sign a
// production token; server.production=true turns its use into a startup error.
const devJWTSecret = "dev-jwt-secret-change-me"

// envOrDefault returns the environment value, or the fallback when unset.
func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// adminPassword resolves the bootstrap owner password. With no explicit
// password the built-in development default is used, but only when the node is
// not declared production: a well-known password on a public deployment would
// hand over the owner account. An empty password disables the bootstrap.
func adminPassword(cfg *config.Config, logger *slog.Logger) string {
	if pw := os.Getenv("ADMIN_PASSWORD"); pw != "" {
		return pw
	}
	if cfg.Server.Production {
		logger.Warn("ADMIN_PASSWORD unset in production: skipping the default owner password",
			"hint", "set ADMIN_PASSWORD to bootstrap the owner account")
		return ""
	}
	logger.Warn("using the built-in development owner password; never run this in production",
		"hint", "set ADMIN_PASSWORD, or server.production=true to disable the default")
	return "admin123"
}

func main() {
	role := flag.String("role", "all", "service role: gateway|worker|admin|all")
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	// Resolve the role before anything else: an unknown value must fail loudly
	// instead of silently producing a node with no capabilities.
	plan, planErr := planFor(*role)
	if planErr != nil {
		fmt.Fprintf(os.Stderr, "invalid -role: %v\n", planErr)
		os.Exit(2)
	}

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
	// Build identity for rollouts: which platform version is this process, and
	// which instance answered. Public on purpose (see web/version.go).
	mux.Handle("/version", web.VersionHandler(web.BuildInfo{
		Version: buildVersion,
		GitSHA:  buildGitSHA,
		Role:    cfg.Role,
	}))

	// With a MySQL DSN configured, the management domains (tenants, endpoints,
	// tools, agents) persist across restarts; otherwise they stay in memory.
	// The connection is waited for (see deps.go), so a node that boots in
	// parallel with its database does not silently fall back to memory.
	var db *sql.DB
	if cfg.MySQL.DSN != "" {
		db = openMySQLWhenReady(runCtx, cfg.MySQL.DSN, logger)
		if db == nil {
			// A configured-but-unreachable MySQL is a degraded state, not a
			// silent fallback: persistence, audit and the worker all stay off.
			// Make it loud (/healthz 503) so the outage is visible.
			health.Report("mysql", "unavailable")
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
	// Long-term memory: the budget is the single switch for the feature (see
	// config.MemoryConfig). The agent manager applies it to every built agent
	// and the worker reads it back to decide whether to expose memory tools.
	agentMgr.SetPreloadMemory(cfg.Memory.PreloadCount)

	// Member management + auth. With MySQL we read tenant_members; without
	// MySQL we fall back to an in-memory store for local development.
	var memberMgr *member.Manager
	if db != nil {
		memberMgr = member.NewManagerWithStore(member.NewMySQLStore(db))
	} else {
		memberMgr = member.NewManager()
	}
	// Bootstrap owner: an empty password (production without ADMIN_PASSWORD)
	// skips bootstrapping entirely rather than creating a well-known account.
	// Only the control-plane node bootstraps: it owns the member table, and
	// letting every data-plane node race the same insert makes each of their
	// starts log a spurious duplicate-key failure.
	if pw := adminPassword(cfg, logger); pw == "" {
		logger.Info("owner bootstrap skipped: no ADMIN_PASSWORD configured")
	} else if plan.AdminAPI {
		if err := web.EnsureInitialOwner(
			runCtx,
			tenantMgr,
			memberMgr,
			envOrDefault("ADMIN_TENANT_ID", "t-demo"),
			envOrDefault("ADMIN_USER_ID", "admin"),
			pw,
		); err != nil {
			logger.Warn("initial owner bootstrap failed", "err", err)
		}
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
	//
	// The JWT secret has three sources, in order: a dedicated TRPC_JWT_SECRET, the
	// credential-store master key, and finally a built-in development value. The
	// last one is a real security hole outside development — anyone who reads the
	// source can mint tokens for any tenant — so it is gated.
	jwtSecret := os.Getenv("TRPC_JWT_SECRET")
	if jwtSecret == "" {
		jwtSecret = cfg.Secret.MasterKey // fallback to master key if no dedicated JWT secret
	}
	if jwtSecret == "" {
		if cfg.Server.Production {
			logger.Error("refusing to start: production mode requires a signing secret",
				"hint", "set TRPC_JWT_SECRET (or secret.master_key)")
			os.Exit(1)
		}
		logger.Warn("using the built-in development JWT secret; never run this in production",
			"hint", "set TRPC_JWT_SECRET, or server.production=true to make this fatal")
		jwtSecret = devJWTSecret
	}
	authMW := web.NewAuthMiddleware(memberMgr, jwtSecret)

	// IM channel API: registered on the admin node (binding CRUD) and needed by
	// the gateway node, which reconciles live adapters when bindings change.
	channelAPI := web.NewChannelAPI(bindStore)
	channelAPI.SetAgentSource(agentMgr)
	if auditRec != nil {
		channelAPI.SetAuditor(auditRec)
	}

	// Admin REST surface: management APIs. A worker or gateway node does not
	// expose it (see rolePlan) so a compromised data-plane node cannot rewrite
	// platform configuration.
	if plan.AdminAPI {
		registerAdminAPI(mux, adminAPI{
			tenants:   tenantMgr,
			members:   memberMgr,
			agents:    agentMgr,
			tools:     toolReg,
			kb:        kbMgr,
			skills:    skillMgr,
			channels:  channelAPI,
			secrets:   secretStore,
			auditor:   auditRec,
			endpoints: reg,
		})
	}

	// Login is unauthenticated; registration and session validation are
	// protected by the outer auth middleware. The auth routes are needed on any
	// node that serves REST, so they follow the admin surface.
	if plan.AdminAPI {
		mux.HandleFunc("/auth/login", authMW.Login)
		mux.HandleFunc("/auth/register", authMW.Register)
		mux.HandleFunc("/auth/me", authMW.Me)
	}

	// Routes that must bypass the platform auth middleware: the health probe,
	// the build-identity probe (a rollout has to be verifiable before anyone
	// logs in) and login are public by nature, and the IM callback ingress is
	// authenticated by the platform's own signature. startDataPlane mounts the
	// ingress (it owns the IM manager) and reports the paths it needs here.
	skipAuth := []string{"/healthz", "/version", "/auth/login"}

	// Data plane: the worker loop, the outbox dispatcher and the IM gateway, each
	// gated by the role plan. All three need Redis; the worker and the ledger
	// also need MySQL.
	skipAuth = append(skipAuth, startDataPlane(runCtx, dataPlaneDeps{
		plan:        plan,
		cfg:         cfg,
		db:          db,
		router:      router,
		tenantMgr:   tenantMgr,
		agentMgr:    agentMgr,
		toolReg:     toolReg,
		kbMgr:       kbMgr,
		skillMgr:    skillMgr,
		auditor:     auditor,
		auditRec:    auditRec,
		secretStore: secretStore,
		bindStore:   bindStore,
		mux:         mux,
		channelAPI:  channelAPI,
		logger:      logger,
	})...)

	logger.Info("starting server", "addr", cfg.Server.HTTPAddr, "role", cfg.Role)
	// Middleware order matters: CORS is outermost so that every response —
	// including auth rejections (401/403) — carries the CORS headers the
	// browser needs to read the status instead of reporting a network error.
	// The auth middleware then lets preflights through and enforces the Bearer
	// token: everything not listed in skipAuth needs a token.
	readHeaderTimeout := cfg.Server.ReadHeaderTimeout
	if readHeaderTimeout <= 0 {
		readHeaderTimeout = config.DefaultReadHeaderTimeout
	}
	srv := &http.Server{
		Addr:              cfg.Server.HTTPAddr,
		Handler:           web.CORS(authMW.Wrap(skipAuth, web.RequireRoutePermission(mux))),
		ReadHeaderTimeout: readHeaderTimeout,
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

// adminAPI is the dependency set of the management REST surface. It is a struct
// rather than a parameter list so adding a domain does not ripple through the
// assembly function's signature.
type adminAPI struct {
	tenants   *tenant.Manager
	members   *member.Manager
	agents    *agent.Manager
	tools     *tool.Registry
	kb        *knowledge.Manager
	skills    *skill.Manager
	channels  *web.ChannelAPI
	secrets   secret.Store
	auditor   *audit.MySQLRecorder
	endpoints *llm.Registry
}

// registerAdminAPI mounts the management REST surface: the platform's control
// plane. Asset writes are audited when a recorder exists.
func registerAdminAPI(mux *http.ServeMux, d adminAPI) {
	web.NewTenantAPI(d.tenants).Register(mux)
	web.NewMemberAPI(d.members).Register(mux)

	agentAPI := web.NewAgentAPI(d.agents)
	agentAPI.SetGrants(d.tools, d.skills)
	endpointAPI := web.NewEndpointAPI(d.endpoints)
	kbAPI := web.NewKnowledgeAPI(d.kb)
	skillAPI := web.NewSkillAPI(d.skills)
	toolAPI := web.NewToolAPI(d.tools)
	toolAPI.SetAgentSource(d.agents)
	if d.auditor != nil {
		agentAPI.SetAuditor(d.auditor)
		endpointAPI.SetAuditor(d.auditor)
		kbAPI.SetAuditor(d.auditor)
		skillAPI.SetAuditor(d.auditor)
	}
	agentAPI.Register(mux)
	endpointAPI.Register(mux)
	toolAPI.Register(mux)
	kbAPI.Register(mux)
	skillAPI.Register(mux)
	d.channels.Register(mux)

	if d.secrets != nil {
		web.NewSecretAPI(d.secrets).Register(mux)
	}
	if d.auditor != nil {
		web.NewAuditAPI(d.auditor).Register(mux)
		web.NewUsageAPI(d.auditor).Register(mux)
	}
}

// dataPlaneDeps is the dependency set of the data plane (worker loop, outbox
// dispatcher, IM gateway). Grouped so the plan's gates read as one decision.
type dataPlaneDeps struct {
	plan        rolePlan
	cfg         *config.Config
	db          *sql.DB
	router      *storage.Router
	tenantMgr   *tenant.Manager
	agentMgr    *agent.Manager
	toolReg     *tool.Registry
	kbMgr       *knowledge.Manager
	skillMgr    *skill.Manager
	auditor     audit.Recorder
	auditRec    *audit.MySQLRecorder
	secretStore secret.Store
	bindStore   channels.BindingStore
	mux         *http.ServeMux
	channelAPI  *web.ChannelAPI
	logger      *slog.Logger
}

// startDataPlane wires and starts the node's data-plane components according to
// the role plan. Each component is gated by the capability it implements:
//
//   - worker loop + outbox + ledger + chat API: plan.WorkerLoop (needs MySQL for
//     the outbox and Redis for the bus)
//   - IM gateway (adapters + outbound fan-out): plan.IMGatway (needs Redis)
//
// A gateway-only node therefore holds no REST surface and consumes no inbound
// messages, while a worker-only node exposes no management API. Extracted from
// main so the composition root stays a thin assembly.
//
// It returns the routes that must bypass the platform auth middleware (currently
// only the IM callback ingress, which the platform authenticates by signature).
func startDataPlane(runCtx context.Context, d dataPlaneDeps) []string {
	needsRedis := d.plan.WorkerLoop || d.plan.IMGatway
	if !needsRedis || d.cfg.Redis.URL == "" {
		if needsRedis {
			d.logger.Error("redis url not configured, data plane disabled",
				"worker", d.plan.WorkerLoop, "gateway", d.plan.IMGatway)
		}
		return nil
	}
	// Wait for Redis before the bus is used: the worker would otherwise start,
	// fail every command, and leave the IM/chat path unresponsive until an
	// operator restarted the node.
	redisWhenReady(runCtx, d.cfg.Redis.URL, d.logger)
	rb, err := bus.NewRedisFromURL(d.cfg.Redis.URL)
	if err != nil {
		d.logger.Error("redis bus unavailable, data plane disabled", "err", err)
		return nil
	}

	if d.plan.WorkerLoop {
		startWorkerLoop(runCtx, d, rb)
	}
	var public []string
	if d.plan.IMGatway {
		public = startIMGatway(runCtx, d, rb)
	}
	return public
}

// startWorkerLoop starts message consumption, the outbox dispatcher and the
// conversation surface. It needs MySQL (outbox + ledger) on top of Redis.
func startWorkerLoop(runCtx context.Context, d dataPlaneDeps, rb *bus.RedisBus) {
	if d.db == nil {
		d.logger.Error("mysql not configured, worker disabled")
		return
	}
	outbox := bus.NewOutbox(d.db)

	// Dead-letter policy: after 5 business failures an inbound message is
	// moved out of the live stream (Redis DLQ copy) and persisted to MySQL so
	// an operator can inspect and replay it via the admin API.
	dlqStore := dlqstore.NewStore(d.db)
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
			d.logger.Error("dlq persistence failed (redis copy retained)", "message", e.MessageID, "err", err)
		}
	})
	web.NewDLQAPI(dlqStore, rb).Register(d.mux)

	// Artifact persistence on MinIO when configured; without it the runner
	// just does not persist artifacts. When MinIO is up, per-tenant routing
	// applies (a tenant may opt into the in-memory artifact backend); with no
	// MinIO the domain stays disabled rather than silently degrading to an
	// ephemeral in-memory store.
	var artSvc artifact.Service
	if d.cfg.MinIO.Endpoint != "" {
		bucket := d.cfg.MinIO.Bucket
		if bucket == "" {
			bucket = "artifacts"
		}
		svc, err := storage.NewMinioArtifactService(context.Background(),
			d.cfg.MinIO.Endpoint, d.cfg.MinIO.AccessKey, d.cfg.MinIO.SecretKey, bucket, d.cfg.MinIO.UseSSL)
		if err != nil {
			d.logger.Error("minio unavailable, artifact persistence disabled", "err", err)
		} else {
			artSvc = storage.NewRouterArtifactService(d.router, svc, nil)
			d.logger.Info("artifact persistence enabled", "endpoint", d.cfg.MinIO.Endpoint, "bucket", bucket)
		}
	}

	// Admin chat rides the same worker pipeline: POST /chat publishes inbound,
	// replies come back over outbound and are SSE-forwarded. Registered here
	// because a chat API without a consumer would accept turns nobody runs.
	web.NewChatAPI(rb).Register(d.mux)
	// Business conversation ledger writes every turn (USER+ASSISTANT) for the
	// session-history API; it shares the worker's MySQL.
	ledger := ledgerstore.NewMySQLLedger(d.db)
	web.NewChatHistoryAPI(ledger).Register(d.mux)

	// Sessions resolve per tenant through the Router; knowledge is also a
	// Router-wrapped domain, and the auditor was already wrapped in main.
	w := worker.New(rb, d.agentMgr, worker.NewToolResolver(d.toolReg, builtinToolSource, d.kbMgr),
		outbox, d.router, d.skillMgr, d.auditor, artSvc, ledger)
	// Tenant governance: per-tenant quota + audit_policy come from the tenants
	// table (configured in the tenant UI); budgets apply only when a tenant
	// sets quota.token_quota > 0. The token meter reads usage_records when the
	// audit recorder is wired; without it no budget is ever enforced.
	var usageFn func(ctx context.Context, tenantID string) (int64, error)
	if d.auditRec != nil {
		usageFn = func(ctx context.Context, tenantID string) (int64, error) {
			sums, err := d.auditRec.UsageSummary(ctx, audit.UsageQuery{TenantID: tenantID, Dimension: audit.UsageDimensionToken})
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
	w.SetGovernance(d.tenantMgr, usageFn)

	go func() {
		if err := w.Run(runCtx); err != nil {
			d.logger.Error("worker stopped", "err", err)
		}
	}()
	go func() {
		if err := outbox.Run(runCtx, rb, time.Second); err != nil {
			d.logger.Error("outbox dispatcher stopped", "err", err)
		}
	}()
	d.logger.Info("worker started", "group", worker.Group)
}

// startIMGatway starts the binding-driven IM connection manager: it holds the IM
// long connections and fans outbound replies back to the originating channel.
// It returns the routes the callback ingress added (empty when it is disabled).
func startIMGatway(runCtx context.Context, d dataPlaneDeps, rb *bus.RedisBus) []string {
	imMgr := channels.NewManager(rb, d.bindStore, d.secretStore, buildAdapter)
	if d.cfg.RateLimit.Enable {
		// Inbound rate limiting shares the bus's Redis so the limit holds
		// across gateway nodes; a limiter error fails open.
		if rcli, err := bus.NewRedisFromURL(d.cfg.Redis.URL); err != nil {
			d.logger.Error("rate limiter unavailable, inbound limiting disabled", "err", err)
		} else {
			imMgr.SetRateLimiter(channels.NewRedisRateLimiter(rcli.Client(), d.cfg.RateLimit.PerMinute, time.Minute))
			d.logger.Info("IM inbound rate limiting enabled", "per_minute", d.cfg.RateLimit.PerMinute)
		}
	}
	d.channelAPI.SetManager(imMgr)
	verifiers, builders := webhookChannels()
	var public []string
	if d.cfg.IM.Webhook.Enable {
		served, err := webhookSelection(d.cfg.IM.Webhook.Channels, verifiers, builders)
		if err != nil {
			// A channel the platform cannot call back is a configuration error:
			// refuse to start rather than advertise an endpoint that never
			// receives anything.
			d.logger.Error("IM webhook ingress misconfigured", "err", err)
			os.Exit(2)
		}
		if len(served) == 0 {
			d.logger.Warn("IM webhook ingress enabled with no channels, nothing served")
		}
		if err := imMgr.EnableWebhook(channels.WebhookConfig{
			Channels:  served,
			Verifiers: verifiers,
			Builders:  builders,
		}); err != nil {
			d.logger.Error("IM webhook ingress disabled", "err", err)
		} else {
			// The callback route is unauthenticated by design (the platform
			// signs the request, it cannot present a session), so it is mounted
			// only here and its path joins the auth middleware's skip list.
			webhookAPI := web.NewWebhookAPI(imMgr)
			webhookAPI.Register(d.mux)
			public = webhookAPI.Paths()
			d.logger.Info("IM webhook ingress mounted", "path", web.WebhookPathPrefix, "channels", served)
		}
	}
	if err := imMgr.Reload(context.Background()); err != nil {
		d.logger.Error("IM gateway reload failed", "err", err)
	}
	go func() {
		if err := imMgr.Run(runCtx); err != nil {
			d.logger.Error("IM gateway stopped", "err", err)
		}
	}()
	// Release the IM connections on shutdown: an exited node must not leave
	// half-open sessions (and a stale bot presence) behind on the platform.
	go func() {
		<-runCtx.Done()
		if err := imMgr.Close(); err != nil {
			d.logger.Warn("IM gateway close", "err", err)
		}
	}()
	d.logger.Info("IM gateway started")
	return public
}

// webhookSelection validates the configured callback channels against the ones
// the platform can actually serve and returns the resulting set.
func webhookSelection(channels []string, verifiers map[string]channels.WebhookVerifier, builders map[string]channels.WebhookBuilder) (map[string]bool, error) {
	served := make(map[string]bool, len(channels))
	for _, ch := range channels {
		if ch == "" {
			continue
		}
		if verifiers[ch] == nil || builders[ch] == nil {
			return nil, fmt.Errorf("channel %q has no HTTP callback mode (supported: feishu)", ch)
		}
		served[ch] = true
	}
	return served, nil
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
