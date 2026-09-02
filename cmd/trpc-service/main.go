// Command trpc-service starts the multi-tenant Agent platform.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/bus"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/health"
	"github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/llm"
	srvlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/skill"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workspace"

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

	mux := http.NewServeMux()
	mux.Handle("/healthz", health.Handler())

	// With a MySQL DSN configured, the management domains (tenants, endpoints,
	// tools, agents) persist across restarts; otherwise they stay in memory.
	var db *sql.DB
	if cfg.MySQL.DSN != "" {
		db, err = storage.OpenMySQL(cfg.MySQL.DSN)
		if err != nil {
			logger.Error("mysql open failed, falling back to memory", "err", err)
		}
	}
	var reg *llm.Registry
	var tenantMgr *tenant.Manager
	var agentMgr *agent.Manager
	var toolReg *tool.Registry
	var kbMgr *knowledge.Manager
	var skillMgr *skill.Manager
	if db != nil {
		reg = llm.NewMySQLRegistry(db, nil)
		tenantMgr = tenant.NewMySQLManager(db)
		agentMgr = agent.NewMySQLManager(db, reg)
		toolReg = tool.NewMySQLRegistry(db)
		kbMgr = knowledge.NewMySQLManager(db, vectorStoreFactory(cfg), knowledge.RegistryEmbedderFactory(reg))
		skillMgr = skill.NewMySQLManager(db)
	} else {
		reg = llm.NewRegistry(nil)
		tenantMgr = tenant.NewManager()
		agentMgr = agent.NewManager(reg)
		toolReg = tool.NewRegistry()
		kbMgr = knowledge.NewManager(vectorStoreFactory(cfg), knowledge.RegistryEmbedderFactory(reg))
		skillMgr = skill.NewManager()
	}
	registerBuiltinTools(toolReg)

	// Audit recorder writes governance decisions + per-request accounting to
	// MySQL asynchronously; without MySQL it is nil (audit disabled). The
	// concrete recorder also serves the admin read side (GET /audit).
	var auditor audit.Recorder
	var auditRec *audit.MySQLRecorder
	if db != nil {
		auditRec = audit.NewMySQL(db)
		auditor = auditRec
		defer func() { _ = auditRec.Close() }()
	}

	web.NewTenantAPI(tenantMgr).Register(mux)
	web.NewAgentAPI(agentMgr).Register(mux)
	web.NewEndpointAPI(reg).Register(mux)
	web.NewToolAPI(toolReg).Register(mux)
	web.NewKnowledgeAPI(kbMgr).Register(mux)
	web.NewSkillAPI(skillMgr).Register(mux)
	if auditRec != nil {
		web.NewAuditAPI(auditRec).Register(mux)
	}

	// Worker + outbox dispatcher: run when the role includes worker and both
	// Redis and MySQL are configured (the bus needs Redis, the outbox MySQL).
	if isWorkerRole(cfg.Role) && db != nil && cfg.Redis.URL != "" {
		rb, err := bus.NewRedisFromURL(cfg.Redis.URL)
		if err != nil {
			logger.Error("redis bus unavailable, worker disabled", "err", err)
		} else {
			outbox := bus.NewOutbox(db)
			router := storage.NewRouter(tenantMgr,
				storage.SessionConfig{Backend: storage.BackendRedis, RedisURL: cfg.Redis.URL},
				storage.MemoryConfig{Backend: storage.BackendRedis, RedisURL: cfg.Redis.URL},
			)
			// Artifact persistence on MinIO when configured; without it the
			// runner just does not persist artifacts.
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
					artSvc = svc
					logger.Info("artifact persistence enabled", "endpoint", cfg.MinIO.Endpoint, "bucket", bucket)
				}
			}
			// Data-domain assembly point: session/memory via the Router,
			// knowledge/artifact/audit as their single production backends.
			// Summary has no standalone domain (lives in the session backend).
			dss := storage.NewDataStores(router, kbMgr, artSvc, auditor)
			w := worker.New(rb, agentMgr, toolReg, builtinToolSource, outbox, dss.Router, dss.Knowledge, skillMgr, dss.Auditor, dss.Artifacts)
			go func() {
				if err := w.Run(context.Background()); err != nil {
					logger.Error("worker stopped", "err", err)
				}
			}()
			go func() {
				if err := outbox.Run(context.Background(), rb, time.Second); err != nil {
					logger.Error("outbox dispatcher stopped", "err", err)
				}
			}()
			logger.Info("worker started", "group", worker.Group)
		}
	}

	logger.Info("starting server", "addr", cfg.Server.HTTPAddr, "role", cfg.Role)
	if err := http.ListenAndServe(cfg.Server.HTTPAddr, web.CORS(mux)); err != nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
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

// vectorStoreFactory picks the KB vector backend: Milvus when configured,
// otherwise the in-memory store (dev mode, data lost on restart).
func vectorStoreFactory(cfg *config.Config) knowledge.VectorStoreFactory {
	if cfg.Milvus.Address == "" {
		slog.Warn("milvus address not configured, knowledge bases stay in memory")
		return knowledge.InMemoryVectorStoreFactory()
	}
	return knowledge.MilvusVectorStoreFactory(cfg.Milvus.Address, cfg.Milvus.Username, cfg.Milvus.Password)
}

// builtinToolSource resolves built-in tool ids to their implementations.
func builtinToolSource(id string) (fwtool.Tool, bool) {
	if id == "echo" {
		return tool.EchoTool(), true
	}
	if id == "code-exec" {
		return fwtoolcodeexec.NewTool(dockerExec), true
	}
	return nil, false
}

// registerBuiltinTools registers the platform's built-in tools. Builtin
// tools are global: they carry no scope, so every tenant sees them.
func registerBuiltinTools(reg *tool.Registry) {
	err := reg.Register(context.Background(), tool.Definition{
		ID:          "echo",
		Name:        "echo",
		Description: "Returns the input text unchanged.",
		RiskLevel:   tool.RiskLow,
	})
	if err != nil {
		slog.Error("register builtin tool failed", "err", err)
	}
	// Code execution is inherently dangerous: risk_level=high makes every
	// code-exec call require human approval (approval rail 2), independent
	// of per-agent config.
	err = reg.Register(context.Background(), tool.Definition{
		ID:          "code-exec",
		Name:        "execute_code",
		Description: "Execute Python or Bash code in an isolated container and return the output. High risk: human approval required before each run.",
		RiskLevel:   tool.RiskHigh,
	})
	if err != nil {
		slog.Error("register builtin tool failed", "err", err)
	}
}
