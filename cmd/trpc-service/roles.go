package main

// Reliable-mode roles (approved plan, "同一二进制分角色部署"). Each role runs
// one part of the Inbox → Worker → Outbox chain and nothing else, so a worker
// crash never stops delivery and a delivery backlog never stops execution.
//
//	gateway   the durable receive face: verified callbacks that persist
//	          before ACKing (wechat_kf notifications, wecom/webchat inbox),
//	          plus the webchat mailbox and the health probes
//	worker    claims sessions and executes them
//	delivery  sends committed replies
//	jobs      pulls WeChat KF pages into the inbox
//
// "all" keeps the original single-process behaviour, so every existing script
// and drill keeps meaning exactly what it meant before these roles existed.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/embedding"
	"github.com/liuzengh/trpc-agent-service/trpcservice/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/inbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/jobs"
	"github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/memory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/outbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/minio"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/qdrant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/summary"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workspace"
)

// Role names accepted by -role.
const (
	roleAll      = "all"
	roleGateway  = "gateway"
	roleWorker   = "worker"
	roleDelivery = "delivery"
	roleJobs     = "jobs"
)

// Environment knobs the reliable roles read. They are environment (not YAML)
// on purpose: a replica's identity and a deployment's secret allowlist differ
// per pod, and the shared config file is the wrong place for a per-pod fact.
const (
	// envWorkerID names this process in leases and attempts. Defaults to
	// role+hostname+pid, which is unique enough that two replicas of the same
	// image on one host cannot collide.
	envWorkerID = "WORKER_ID"
	// envSecretEnvAllow is the comma-separated allowlist of environment
	// variable names a model profile's api_key_ref may name. It exists
	// because those references are stored in MySQL, where a tenant admin will
	// eventually write them: without an allowlist "env:ANYTHING" would read
	// whatever the pod happens to have. Default MODEL_API_KEY keeps the
	// single-key development shape working.
	envSecretEnvAllow = "RELIABLE_SECRET_ENV_ALLOW"
	// envKFAPIBase overrides the 微信客服 API host, for the compose stack's
	// fake KF upstream and for private deployments.
	envKFAPIBase = "KF_API_BASE"
	// envWeComAPIBase overrides the 企业微信 API host for the reliable
	// delivery sender, mirroring envKFAPIBase.
	envWeComAPIBase = "WECOM_API_BASE"
)

// workerIdleWait is how long a sweep sleeps once every tenant had nothing to
// do. Short enough that an E2E does not need to know about it, long enough
// that an idle deployment is not a busy loop against MySQL.
const workerIdleWait = 250 * time.Millisecond

// runRole serves one reliable-mode role until the process is signalled.
func runRole(role string, cfg *config.Config, addr string) int {
	if cfg.ControlPlane.Mode != config.ControlPlaneMySQL {
		fmt.Fprintf(os.Stderr, "role %s: control_plane.mode must be mysql (got %q)\n",
			role, cfg.ControlPlane.Mode)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	openCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	db, err := tasmysql.Open(openCtx, cfg.ControlPlane.MySQLDSN)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "role %s: open control plane: %v\n", role, err)
		return 1
	}
	defer db.Close()
	cdp := controlplane.NewDB(db)
	workerID := roleWorkerID(role)

	var runErr error
	switch role {
	case roleGateway:
		runErr = runReceiver(ctx, addr, db, cdp)
	case roleWorker:
		runErr = sweepWorker(ctx, cfg, cdp, workerID)
	case roleDelivery:
		runErr = sweepDelivery(ctx, cdp, workerID)
	case roleJobs:
		runErr = runJobs(ctx, cfg, cdp, workerID)
	default:
		fmt.Fprintf(os.Stderr, "role %q is not a reliable-mode role (want gateway, worker, delivery or jobs)\n", role)
		return 1
	}
	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		fmt.Fprintf(os.Stderr, "role %s: %v\n", role, runErr)
		return 1
	}
	return 0
}

// runReceiver serves the durable receive face: the verified callbacks whose
// only job is to persist before ACKing, the webchat mailbox that pushes
// committed replies to open browser streams, and the health probes. The
// process holds no runner and no session state — execute belongs to the
// worker role, which may well be a different Pod.
func runReceiver(ctx context.Context, addr string, db *sql.DB, cdp *controlplane.DB) error {
	rc := channels.NewReceiver(cdp)
	mux := http.NewServeMux()
	for path, h := range rc.Routes() {
		mux.Handle(path, h)
	}
	mux.Handle("/webchat/stream", channels.NewWebChatMailbox(cdp).Handler())
	mux.HandleFunc(web.HealthPath, func(w http.ResponseWriter, _ *http.Request) {
		writeProbe(w, http.StatusOK, `{"status":"ok"}`)
	})
	// Readiness is a MySQL ping: the receive face can only honour its
	// persist-before-ACK promise while the control plane answers, so a
	// database outage must take this process out of rotation.
	mux.HandleFunc(web.ReadyPath, func(w http.ResponseWriter, r *http.Request) {
		pctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if err := db.PingContext(pctx); err != nil {
			writeProbe(w, http.StatusServiceUnavailable,
				fmt.Sprintf(`{"status":"unavailable","reasons":[%q]}`, err.Error()))
			return
		}
		writeProbe(w, http.StatusOK, `{"status":"ready"}`)
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	slog.Info("role gateway: receiving", "addr", addr,
		"probes", web.HealthPath+" "+web.ReadyPath)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// writeProbe answers one health probe with a JSON body.
func writeProbe(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, body)
}

// sweepWorker claims and executes sessions across every active tenant.
//
// It is a sweep rather than one goroutine per tenant on purpose: `Worker.Run`
// serves one tenant forever, and a process that owns several has to decide how
// to divide its attention. A sweep with SKIP LOCKED scales just as well for
// this milestone's volume and keeps one tenant's empty queue from being
// invisible work.
func sweepWorker(ctx context.Context, cfg *config.Config, cdp *controlplane.DB, workerID string) error {
	resolver := secrets.NewResolver(secrets.AllowedPrefixes{EnvVars: allowedSecretEnv()})
	svc := execution.NewService(cdp, execution.DefaultLeaseTTL)
	// The knowledge stack is deferred, not absent: the claim loop must start
	// without touching MinIO or Qdrant (an outage there would otherwise take
	// the loop down with it), but a revision that pins knowledge_search, a
	// memory tool or artifact_save must still be able to run. The stack is
	// built on the first assembly that needs it; a build failure fails only
	// that message (it stays at its queue head and the next attempt retries),
	// and a deployment without the knowledge section keeps failing such pins
	// loudly.
	stack := newDeferredKnowledgeStack(cfg, cdp, resolver)
	runnerOpts := []execution.RunnerOption{
		execution.WithDeferredKnowledge(stack.knowledge),
		execution.WithDeferredMemory(stack.memory),
		execution.WithDeferredArtifacts(stack.artifacts),
		execution.WithSkillStore(cfg.Skills.Root),
	}
	if cfg.Workspace.Root != "" {
		wsManager, err := workspace.NewManager(cfg.Workspace)
		if err != nil {
			return err
		}
		runnerOpts = append(runnerOpts, execution.WithWorkspaceManager(wsManager))
	}
	worker := execution.NewWorker(svc, execution.WorkerOptions{WorkerID: workerID},
		execution.DefaultRunnerFactory(cdp, resolver, runnerOpts...))

	slog.Info("role worker: polling", "worker_id", workerID,
		"lease_ttl", execution.DefaultLeaseTTL, "secret_env_allow", allowedSecretEnv())
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		tenants, err := cdp.ListActiveTenants(ctx)
		if err != nil {
			slog.Error("worker: list tenants", "err", err)
			if !sleepCtx(ctx, workerIdleWait) {
				return ctx.Err()
			}
			continue
		}
		worked := false
		for _, tenantID := range tenants {
			claim, err := svc.ClaimNext(ctx, tenantID, workerID)
			switch {
			case errors.Is(err, execution.ErrNothingToClaim):
				continue
			case err != nil:
				slog.Warn("worker: claim", "tenant", tenantID, "err", err)
				continue
			}
			worked = true
			if err := worker.RunOne(ctx, claim); err != nil {
				slog.Warn("worker: attempt ended without a commit",
					"tenant", tenantID, "session_pk", claim.SessionPK,
					"execution", claim.ExecutionID, "err", err)
			}
		}
		if !worked && !sleepCtx(ctx, workerIdleWait) {
			return ctx.Err()
		}
	}
}

// sweepDelivery sends committed replies for every active tenant.
func sweepDelivery(ctx context.Context, cdp *controlplane.DB, workerID string) error {
	sender := &channels.DeliverySender{
		KF:      newKfPuller(cdp),
		WeCom:   channels.NewWeComSender(channels.WeComBindingLookup(cdp)).WithAPIBase(wecomAPIBase()),
		WebChat: &channels.WebChatSender{},
	}
	svc := outbox.NewService(cdp, sender)

	slog.Info("role delivery: polling", "worker_id", workerID)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		tenants, err := cdp.ListActiveTenants(ctx)
		if err != nil {
			slog.Error("delivery: list tenants", "err", err)
			if !sleepCtx(ctx, workerIdleWait) {
				return ctx.Err()
			}
			continue
		}
		worked := false
		for _, tenantID := range tenants {
			claim, err := svc.ClaimNext(ctx, tenantID, workerID)
			switch {
			case errors.Is(err, outbox.ErrNothingToSend):
				continue
			case err != nil:
				slog.Warn("delivery: claim", "tenant", tenantID, "err", err)
				continue
			}
			worked = true
			outcome, err := svc.Deliver(ctx, claim)
			if err != nil {
				slog.Warn("delivery: send", "tenant", tenantID, "outbox_id", claim.OutboxID,
					"outcome", outcome, "err", err)
			}
		}
		if !worked && !sleepCtx(ctx, workerIdleWait) {
			return ctx.Err()
		}
	}
}

// runJobs serves two durable queues out of one process: the WeChat KF
// puller (notifications → inbox) keeps its own loop, and the generic job
// runner drains outbox_events (documents, memory, summaries, projections).
// They share a process because both are "background work with no user
// waiting on it"; they do not share a loop because the puller's per-scope
// lease and the job runner's per-row lease are different concurrency rules.
func runJobs(ctx context.Context, cfg *config.Config, cdp *controlplane.DB, workerID string) error {
	resolver := secrets.NewResolver(secrets.AllowedPrefixes{EnvVars: allowedSecretEnv()})
	runner := jobs.New(cdp, workerID, slog.Default())
	// session_project: a cache-warming job for the Redis projection. Without
	// a Redis the row completes with a reason rather than growing the
	// backlog; with one, the snapshot is re-read from MySQL (the authority)
	// and written version-ordered, so a lagging job can never overwrite newer
	// state.
	var proj *coordination.SessionProjection
	if cfg.Storage.Session.Backend == config.BackendRedis {
		coord, err := coordination.NewRedis(cfg.Storage.Session.RedisURL, cfg.Storage.Session.KeyPrefix)
		if err != nil {
			return fmt.Errorf("jobs: projection store: %w", err)
		}
		defer coord.Close()
		proj = coordination.NewSessionProjection(coord, int(cfg.Storage.Session.SessionTTL.Seconds()))
	}
	if proj != nil {
		runner.Register("session_project", func(ctx context.Context, job jobs.Job) error {
			return projectSession(ctx, cdp, proj, job)
		})
	} else {
		runner.Register("session_project", func(ctx context.Context, job jobs.Job) error {
			slog.Debug("jobs: session_project skipped (no projection store in this deployment)", "job", job.ID)
			return nil
		})
	}
	var knowledgeSvc *knowledge.Service
	if parts, ok, err := buildKnowledgeStack(cfg, cdp, resolver); err != nil {
		return err
	} else if ok {
		knowledgeSvc = parts.knowledge
		runner.Register(knowledge.KindDocIndex, func(ctx context.Context, job jobs.Job) error {
			return parts.knowledge.HandleJob(ctx, job.TenantID, job.Kind, job.Payload)
		})
		runner.Register(knowledge.KindDocCleanup, func(ctx context.Context, job jobs.Job) error {
			return parts.knowledge.HandleJob(ctx, job.TenantID, job.Kind, job.Payload)
		})
		runner.Register(memory.KindMemoryIndex, func(ctx context.Context, job jobs.Job) error {
			return parts.memory.HandleJob(ctx, job.TenantID, job.Kind, job.Payload)
		})
		runner.Register(memory.KindMemoryCleanup, func(ctx context.Context, job jobs.Job) error {
			return parts.memory.HandleJob(ctx, job.TenantID, job.Kind, job.Payload)
		})
	}
	summaries, err := summary.New(cdp, resolver)
	if err != nil {
		return err
	}
	runner.Register(summary.KindSessionSummary, func(ctx context.Context, job jobs.Job) error {
		return summaries.HandleJob(ctx, job.TenantID, job.Kind, job.Payload)
	})
	slog.Info("role jobs: pulling KF + draining outbox_events", "worker_id", workerID,
		"kinds", runner.Kinds(), "kf_api_base", kfAPIBase())

	// The job sweep shares the process with the puller: one goroutine drains
	// the queue, another runs reconciliation on a slow ticker. The workspace
	// collector joins them for the same reason the puller does — it is
	// background work nothing waits on — on its own ticker, because no event
	// means "a session directory went stale".
	errCh := make(chan error, 3)
	go func() { errCh <- newKfPuller(cdp).Run(ctx, workerID, time.Second) }()
	go func() { errCh <- sweepJobs(ctx, cdp, runner, knowledgeSvc, workerID) }()
	if cfg.Workspace.Root != "" {
		wsManager, err := workspace.NewManager(cfg.Workspace)
		if err != nil {
			return err
		}
		go func() { errCh <- sweepWorkspaces(ctx, wsManager) }()
	}
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sweepJobs drains outbox_events across tenants, and every
// reconcileInterval re-enqueues work a crash left stuck (see
// knowledge.Reconcile).
const reconcileInterval = 5 * time.Minute

// projectSession refreshes one session's Redis projection from MySQL, the
// authority. The projection carries the commit version, so two racing
// commits cannot regress it; a stale write is the normal outcome of a race
// and lands as written=false rather than an error.
func projectSession(ctx context.Context, cdp *controlplane.DB, proj *coordination.SessionProjection, job jobs.Job) error {
	var payload struct {
		SessionPK int64 `json:"session_pk"`
	}
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("session_project: payload: %w", err)
	}
	if payload.SessionPK == 0 {
		return nil // nothing to project; completing keeps the queue honest
	}
	scope, err := cdp.Scope(job.TenantID)
	if err != nil {
		return err
	}
	var (
		stateJSON []byte
		summary   sql.NullString
		version   uint64
	)
	row, err := scope.QueryRow(ctx, `
		SELECT state, summary, session_version FROM sessions
		WHERE tenant_id = ? AND session_pk = ?`, job.TenantID, payload.SessionPK)
	if err != nil {
		return err
	}
	switch err := row.Scan(&stateJSON, &summary, &version); {
	case errors.Is(err, sql.ErrNoRows):
		return nil // deleted since the job was enqueued: nothing to cache
	case err != nil:
		return fmt.Errorf("session_project: read session: %w", err)
	}
	state := map[string][]byte{}
	if len(stateJSON) > 0 {
		if err := json.Unmarshal(stateJSON, &state); err != nil {
			return fmt.Errorf("session_project: decode state: %w", err)
		}
	}
	if _, err := proj.Project(ctx, coordination.ProjectedSession{
		TenantID: job.TenantID, SessionPK: payload.SessionPK,
		Version: version, State: state, Summary: summary.String,
	}); err != nil {
		return fmt.Errorf("session_project: write projection: %w", err)
	}
	return nil
}

// workspaceGCInterval is how often stale session directories are collected.
// Deliberately slow: a directory living an extra hour costs disk, a busy
// loop costs every worker's IO.
const workspaceGCInterval = time.Hour

// sweepWorkspaces collects stale session directories on a slow ticker,
// starting with one pass at boot so a restart cleans up promptly.
func sweepWorkspaces(ctx context.Context, m *workspace.Manager) error {
	ticker := time.NewTicker(workspaceGCInterval)
	defer ticker.Stop()
	for {
		if n, err := m.GC(time.Now()); err != nil {
			slog.Warn("jobs: workspace gc", "err", err)
		} else if n > 0 {
			slog.Info("jobs: workspace gc removed stale session directories", "count", n)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func sweepJobs(ctx context.Context, cdp *controlplane.DB, runner *jobs.Runner, knowledgeSvc *knowledge.Service, workerID string) error {
	lastReconcile := time.Now()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		tenants, err := cdp.ListActiveTenants(ctx)
		if err != nil {
			slog.Error("jobs: list tenants", "err", err)
			if !sleepCtx(ctx, workerIdleWait) {
				return ctx.Err()
			}
			continue
		}
		worked := false
		for _, tenantID := range tenants {
			ran, err := runner.RunOne(ctx, tenantID)
			if err != nil {
				slog.Warn("jobs: run", "tenant", tenantID, "err", err)
			}
			worked = worked || ran
			if knowledgeSvc != nil && time.Since(lastReconcile) > reconcileInterval {
				if n, err := knowledgeSvc.Reconcile(ctx, tenantID); err != nil {
					slog.Warn("jobs: reconcile", "tenant", tenantID, "err", err)
				} else if n > 0 {
					slog.Info("jobs: reconcile re-enqueued stuck documents", "tenant", tenantID, "count", n)
				}
			}
		}
		if time.Since(lastReconcile) > reconcileInterval {
			lastReconcile = time.Now()
		}
		if !worked && !sleepCtx(ctx, workerIdleWait) {
			return ctx.Err()
		}
	}
}

// knowledgeStack is the shared set of services the worker and jobs roles
// need when the knowledge section is configured.
type knowledgeStack struct {
	knowledge *knowledge.Service
	memory    *memory.Service
	artifacts *artifact.Service
}

// buildKnowledgeStack constructs the pipeline from configuration. A disabled
// section returns ok=false without touching the network: a deployment that
// never set the endpoints must not fail because Qdrant is not there.
func buildKnowledgeStack(cfg *config.Config, cdp *controlplane.DB, resolver *secrets.Resolver) (knowledgeStack, bool, error) {
	if !cfg.Knowledge.Enabled() {
		return knowledgeStack{}, false, nil
	}
	objects, err := minio.New(cfg.Knowledge.MinIOEndpoint, cfg.Knowledge.MinIOUser,
		cfg.Knowledge.MinIOPassword, cfg.Knowledge.MinIORegion, cfg.Knowledge.MinIOBucket)
	if err != nil {
		return knowledgeStack{}, false, fmt.Errorf("knowledge: object store: %w", err)
	}
	ensureCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := objects.EnsureBucket(ensureCtx); err != nil {
		return knowledgeStack{}, false, fmt.Errorf("knowledge: ensure bucket: %w", err)
	}
	vectors, err := qdrant.New(cfg.Knowledge.QdrantURL, cfg.Knowledge.Collection)
	if err != nil {
		return knowledgeStack{}, false, fmt.Errorf("knowledge: vector store: %w", err)
	}
	if err := vectors.EnsureCollection(ensureCtx, cfg.Knowledge.EmbeddingDim); err != nil {
		return knowledgeStack{}, false, fmt.Errorf("knowledge: ensure collection: %w", err)
	}
	apiKey := ""
	if cfg.Knowledge.EmbeddingAPIKeyRef != "" {
		apiKey, err = resolver.Resolve(cfg.Knowledge.EmbeddingAPIKeyRef)
		if err != nil {
			return knowledgeStack{}, false, fmt.Errorf("knowledge: embedding credential: %w", err)
		}
	}
	embedder, err := embedding.NewOpenAI(cfg.Knowledge.EmbeddingBaseURL, apiKey,
		cfg.Knowledge.EmbeddingModel, cfg.Knowledge.EmbeddingDim)
	if err != nil {
		return knowledgeStack{}, false, fmt.Errorf("knowledge: embedder: %w", err)
	}
	kn, err := knowledge.New(knowledge.Options{
		DB: cdp, Objects: objects, Embedder: embedder, Vectors: vectors,
		Log: slog.Default(),
	})
	if err != nil {
		return knowledgeStack{}, false, err
	}
	mem, err := memory.New(cdp, embedder, vectors)
	if err != nil {
		return knowledgeStack{}, false, err
	}
	arts, err := artifact.New(cdp, objects)
	if err != nil {
		return knowledgeStack{}, false, err
	}
	return knowledgeStack{knowledge: kn, memory: mem, artifacts: arts}, true, nil
}

// deferredKnowledgeStack builds the knowledge stack on the first assembly
// that pins one of its tools (see execution.WithDeferredKnowledge). The
// worker's claim loop must start without MinIO or Qdrant — building eagerly
// would let an object or vector store outage stop every tenant's executions
// — while a revision that pins knowledge_search, a memory tool or
// artifact_save must still be able to run. Success is cached; a failure is
// returned to the one assembly that asked and retried on the next, so
// recovery needs no restart. The mutex serializes first assemblies, so two
// racing claims do not build two stacks.
type deferredKnowledgeStack struct {
	cfg      *config.Config
	cdp      *controlplane.DB
	resolver *secrets.Resolver

	mu    sync.Mutex
	parts knowledgeStack
	ready bool
}

// errKnowledgeDisabled is the loud answer a pin gets in a deployment that
// never configured the knowledge section; it mirrors the "no implementation"
// error the runner factory gives when no deferred builder is wired at all.
var errKnowledgeDisabled = errors.New("knowledge: this deployment has knowledge disabled (no knowledge section configured)")

func newDeferredKnowledgeStack(cfg *config.Config, cdp *controlplane.DB, resolver *secrets.Resolver) *deferredKnowledgeStack {
	return &deferredKnowledgeStack{cfg: cfg, cdp: cdp, resolver: resolver}
}

// ensure resolves the one shared build. buildKnowledgeStack keeps its own
// bounded dial timeout, so no caller context is threaded through.
func (d *deferredKnowledgeStack) ensure() (knowledgeStack, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.ready {
		return d.parts, nil
	}
	if !d.cfg.Knowledge.Enabled() {
		return knowledgeStack{}, errKnowledgeDisabled
	}
	parts, ok, err := buildKnowledgeStack(d.cfg, d.cdp, d.resolver)
	if err != nil {
		return knowledgeStack{}, err
	}
	if !ok {
		return knowledgeStack{}, errKnowledgeDisabled
	}
	d.parts, d.ready = parts, true
	return parts, nil
}

// knowledge, memory and artifacts are the builders handed to
// execution.WithDeferred*; they share one build so the three services see
// the same clients.
func (d *deferredKnowledgeStack) knowledge(context.Context) (*knowledge.Service, error) {
	parts, err := d.ensure()
	if err != nil {
		return nil, err
	}
	return parts.knowledge, nil
}

func (d *deferredKnowledgeStack) memory(context.Context) (*memory.Service, error) {
	parts, err := d.ensure()
	if err != nil {
		return nil, err
	}
	return parts.memory, nil
}

func (d *deferredKnowledgeStack) artifacts(context.Context) (*artifact.Service, error) {
	parts, err := d.ensure()
	if err != nil {
		return nil, err
	}
	return parts.artifacts, nil
}

// newKfPuller builds the one puller both delivery and jobs use — the same
// value, because they share the token cache, and because two pullers of the
// same credentials disagreeing would be a bug waiting to happen.
func newKfPuller(cdp *controlplane.DB) *channels.KfPuller {
	return channels.NewKfPuller(channels.KfBindingLookup(cdp), cdp, inbox.NewService(cdp)).
		WithAPIBase(kfAPIBase())
}

// sleepCtx waits for d, or returns false early when the context ends.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// roleWorkerID names this process for leases and attempts.
func roleWorkerID(role string) string {
	if v := strings.TrimSpace(os.Getenv(envWorkerID)); v != "" {
		return v
	}
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%s-%d", role, host, os.Getpid())
}

// allowedSecretEnv reads the model-credential allowlist.
func allowedSecretEnv() []string {
	raw := os.Getenv(envSecretEnvAllow)
	if strings.TrimSpace(raw) == "" {
		raw = "MODEL_API_KEY"
	}
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if v := strings.TrimSpace(part); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// kfAPIBase is the 微信客服 API host override.
func kfAPIBase() string {
	if v := strings.TrimSpace(os.Getenv(envKFAPIBase)); v != "" {
		return v
	}
	return channels.DefaultKFAPIBase
}

// wecomAPIBase is the 企业微信 API host override.
func wecomAPIBase() string {
	if v := strings.TrimSpace(os.Getenv(envWeComAPIBase)); v != "" {
		return v
	}
	return channels.DefaultWeComAPIBase
}
