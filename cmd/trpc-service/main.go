package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"

	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	tasmysql "github.com/liuzengh/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

// healthcheckTimeout bounds the -healthcheck self-probe. It sits above the
// server's own readiness timeout so the probe reports the 503 and its reasons
// instead of a bare client timeout, which tells an operator nothing.
const healthcheckTimeout = 8 * time.Second

func main() {
	configPath := flag.String("config", config.DefaultPath, "path to YAML config file")
	addr := flag.String("addr", ":8080", "listen address")
	healthcheck := flag.Bool("healthcheck", false,
		"probe this process's own "+web.ReadyPath+" and exit 0/1 instead of serving")
	migrate := flag.Bool("migrate", false,
		"apply pending control-plane schema migrations and exit, without serving")
	bootstrapAdminTenant := flag.String("bootstrap-admin-tenant", "",
		"with -bootstrap-admin: tenant to create the first admin in")
	bootstrapAdminSubject := flag.String("bootstrap-admin-subject", "",
		"with -bootstrap-admin: subject (e.g. an email) identifying the operator")
	bootstrapAdminToken := flag.String("bootstrap-admin-token", "",
		"with -bootstrap-admin: the token to store a hash of; only this run sees it in plaintext")
	doBootstrapAdmin := flag.Bool("bootstrap-admin", false,
		"run the one-shot bootstrap-admin command against the configured control plane and exit")
	role := flag.String("role", roleAll,
		"reliable-mode role: all (default, the single-process platform), worker, delivery or jobs")
	resolveTenant := flag.String("resolve-tenant", "",
		"with -resolve-session or -list-blocked: the tenant whose control plane to act on")
	resolveSession := flag.Int64("resolve-session", 0,
		"dispose of a session blocked by a tool-call unknown (with -resolution) and exit")
	resolution := flag.String("resolution", "",
		"with -resolve-session: confirmed (the side effect happened) or cancelled (it did not; the message re-runs)")
	resolveBy := flag.String("resolve-by", "",
		"with -resolve-session: who made the decision, recorded in the ledger")
	listBlocked := flag.Bool("list-blocked", false,
		"print sessions parked for human review, with their unresolved tool calls, and exit")
	ingestDoc := flag.String("ingest-doc", "",
		"upload one file into a knowledge base and exit; the jobs role indexes it")
	ingestKB := flag.String("ingest-kb", "",
		"with -ingest-doc: the knowledge base's public id")
	ingestApp := flag.String("ingest-app", "assistant",
		"with -ingest-doc: the app's public id")
	ingestDocID := flag.String("ingest-doc-id", "",
		"with -ingest-doc: the document's public id (a UUID; stable across re-uploads)")
	ingestTitle := flag.String("ingest-title", "",
		"with -ingest-doc: display title; defaults to the file name")
	ingestMIME := flag.String("ingest-mime", "text/plain",
		"with -ingest-doc: text/plain, text/markdown or text/csv")
	artifactGet := flag.String("artifact-get", "",
		"download one ready artifact to stdout (or -artifact-out) and exit")
	artifactOut := flag.String("artifact-out", "-",
		"with -artifact-get: output path, or - for stdout")
	docStatus := flag.String("doc-status", "",
		"print one document's indexing state and exit")
	sessionState := flag.Int64("session-state", 0,
		"with -resolve-tenant: print one session's committed state (MySQL snapshot + Redis projection) and exit")
	migrateSessions := flag.Bool("migrate-sessions", false,
		"migrate all sessions from the configured Redis backend into the MySQL control plane and exit")
	dryRun := flag.Bool("dry-run", false,
		"with -migrate-sessions: print what would be migrated without writing")
	reindexKB := flag.String("reindex-kb", "",
		"with -resolve-tenant: re-index all ready documents of a knowledge base and exit")
	flag.Parse()

	// The self-probe runs before anything else: it must not load the config or
	// touch the session backend, because its whole job is to ask the running
	// process how it feels over HTTP. A scratch runtime image ships no shell,
	// curl, or wget, so the Compose healthcheck and the Kubernetes exec probe
	// both call the binary this way.
	if *healthcheck {
		os.Exit(runHealthcheck(*addr))
	}

	fmt.Printf("trpc-agent-service %s\n", trpcservice.Version)
	fmt.Println("multi-tenant node-based agent platform on tRPC-Agent-Go")

	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}
	if err := log.Init(cfg.Log.Level, cfg.Log.JSON); err != nil {
		fmt.Fprintf(os.Stderr, "init log: %v\n", err)
		os.Exit(1)
	}
	// Install the log redaction handler. Secrets that reach the log (through
	// error propagation, audit detail, or trace attributes) are masked before
	// hitting stderr. The pattern list is built from the same env var
	// allowlist the platform uses for credential resolution — if a variable
	// is allowed as a secret, its value is a pattern.
	{
		pat := secrets.NewResolver(secrets.AllowedPrefixes{EnvVars: allowedSecretEnv()}).ResolvedSecrets()
		if len(pat) > 0 {
			slog.SetDefault(slog.New(log.WithLogRedaction(slog.Default().Handler(), pat)))
		}
	}

	// -migrate and -bootstrap-admin are one-shot control-plane operations an
	// operator (or a deploy job) runs before any role serves traffic. They
	// deliberately do not bring up the gateway, the runners, or the session
	// backend: a schema migration racing a live worker's claims is exactly the
	// kind of thing this separation exists to avoid.
	if *migrate {
		os.Exit(runMigrate(cfg))
	}
	if *doBootstrapAdmin {
		os.Exit(runBootstrapAdmin(cfg, *bootstrapAdminTenant, *bootstrapAdminSubject, *bootstrapAdminToken))
	}
	if *listBlocked {
		if *resolveTenant == "" {
			fmt.Fprintln(os.Stderr, "list-blocked: -resolve-tenant is required")
			os.Exit(2)
		}
		os.Exit(runListBlocked(cfg, *resolveTenant))
	}
	if *resolveSession != 0 {
		if *resolveTenant == "" || *resolution == "" {
			fmt.Fprintln(os.Stderr, "resolve-session: -resolve-tenant and -resolution are required")
			os.Exit(2)
		}
		by := *resolveBy
		if by == "" {
			by = "cli"
		}
		os.Exit(runResolveSession(cfg, *resolveTenant, *resolveSession, *resolution, by))
	}
	if *ingestDoc != "" {
		if *resolveTenant == "" || *ingestKB == "" || *ingestDocID == "" {
			fmt.Fprintln(os.Stderr, "ingest-doc: -resolve-tenant, -ingest-kb and -ingest-doc-id are required")
			os.Exit(2)
		}
		os.Exit(runIngestDoc(cfg, *resolveTenant, *ingestApp, *ingestKB, *ingestDocID,
			*ingestTitle, *ingestMIME, *ingestDoc))
	}
	if *artifactGet != "" {
		if *resolveTenant == "" {
			fmt.Fprintln(os.Stderr, "artifact-get: -resolve-tenant is required")
			os.Exit(2)
		}
		os.Exit(runArtifactGet(cfg, *resolveTenant, *artifactGet, *artifactOut))
	}
	if *docStatus != "" {
		if *resolveTenant == "" {
			fmt.Fprintln(os.Stderr, "doc-status: -resolve-tenant is required")
			os.Exit(2)
		}
		os.Exit(runDocStatus(cfg, *resolveTenant, *docStatus))
	}
	if *sessionState != 0 {
		if *resolveTenant == "" {
			fmt.Fprintln(os.Stderr, "session-state: -resolve-tenant is required")
			os.Exit(2)
		}
		os.Exit(runSessionState(cfg, *resolveTenant, *sessionState))
	}
	if *migrateSessions {
		os.Exit(runMigrateSessions(cfg, *dryRun))
	}
	if *reindexKB != "" {
		if *resolveTenant == "" {
			fmt.Fprintln(os.Stderr, "reindex-kb: -resolve-tenant is required")
			os.Exit(2)
		}
		os.Exit(runReindexKB(cfg, *resolveTenant, *reindexKB))
	}

	// A reliable-mode role replaces the single-process platform entirely: it
	// serves one part of the Inbox → Worker → Outbox chain and nothing else.
	// Running both would be two answers to "who dispatches a message" in one
	// deployment, which is exactly the ambiguity the roles exist to remove.
	if *role != roleAll {
		os.Exit(runRole(*role, cfg, *addr))
	}

	// Redis backs both framework sessions and cross-replica coordination. Load
	// the shared runtime config before constructing runners so a rescheduled Pod
	// starts from the latest authenticated admin change, not its ConfigMap copy.
	//
	// In control-plane mysql mode this whole snapshot-override step is skipped:
	// MySQL is the authority now (approved plan, "重启不被 YAML/Redis 覆盖"), and
	// letting a Redis copy still win here would mean two facts sources that can
	// disagree across a restart, with the older one silently in charge.
	var coord *coordination.Redis
	if cfg.Storage.Session.Backend == config.BackendRedis {
		coord, err = coordination.NewRedis(cfg.Storage.Session.RedisURL, cfg.Storage.Session.KeyPrefix)
		if err != nil {
			fmt.Fprintf(os.Stderr, "init coordination: %v\n", err)
			os.Exit(1)
		}
		defer coord.Close()
		if cfg.ControlPlane.Mode != config.ControlPlaneMySQL {
			if persisted, err := admin.NewRedisRuntimeStore(coord).Load(context.Background()); err != nil {
				if !errors.Is(err, admin.ErrSnapshotUnavailable) {
					fmt.Fprintf(os.Stderr, "load runtime config: %v\n", err)
					os.Exit(1)
				}
				// The snapshot store is the same Redis the session probe a few
				// lines below is about to test. Exiting here would blame
				// "runtime config" for what is a session-backend outage; the
				// probe's message names the real dependency. The file config
				// stays in charge until the probe decides.
				slog.Warn("runtime config: snapshot store unavailable; continuing with the file config", "err", err)
			} else if persisted != nil {
				// Storage is deployment-owned and must not be changed through admin.
				persisted.Storage = cfg.Storage
				// Skills are deployment-owned for the same reason: the root is a
				// mount of this Pod, not a tenant setting.
				persisted.Skills = cfg.Skills
				// Workspace is deployment-owned too: the root and its bounds
				// describe this process's filesystem, not the tenant's config.
				persisted.Workspace = cfg.Workspace
				if cfg.Admin.Token != "" {
					persisted.Admin.Token = cfg.Admin.Token
				}
				cfg = persisted
			}
		}
	}
	// Governance trail and telemetry (proposal doc 3.5): the audit file is
	// optional, exporters default to off so local runs stay quiet. Rotation
	// (audit.max_size_mb) is opt-in; 0 keeps the historical single file.
	aud, err := audit.NewWithOptions(audit.Options{
		Path: cfg.Audit.File, MaxSizeMB: cfg.Audit.MaxSizeMB, MaxBackups: cfg.Audit.MaxBackups,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "init audit: %v\n", err)
		os.Exit(1)
	}
	defer aud.Close()
	rec, shutdownTelemetry, err := metrics.Setup(cfg.Telemetry)
	if err != nil {
		fmt.Fprintf(os.Stderr, "init telemetry: %v\n", err)
		os.Exit(1)
	}
	defer func() { _ = shutdownTelemetry(context.Background()) }()
	// Shared session backend (memory or redis per config.storage.session);
	// NewSessionService probes it, so an unreachable Redis stops the boot.
	sess, err := storage.NewSessionService(storage.SessionConfig{
		Backend:    cfg.Storage.Session.Backend,
		RedisURL:   cfg.Storage.Session.RedisURL,
		KeyPrefix:  cfg.Storage.Session.KeyPrefix,
		SessionTTL: cfg.Storage.Session.SessionTTL,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "init session storage: %v\n", err)
		os.Exit(1)
	}
	defer sess.Close()

	// control_plane.mode=mysql opens the authority this deployment is meant
	// to read from. It is a separate failure from the session backend: a
	// MySQL that cannot answer here must stop the boot outright, because a
	// process serving only the legacy file-backed admin API while claiming to
	// be in mysql mode would be worse than not starting. It opens before the
	// runners because the session router reads tenant backend choices from
	// backend_profiles.
	var cdp *controlplane.DB
	if cfg.ControlPlane.Mode == config.ControlPlaneMySQL {
		cpCtx, cpCancel := context.WithTimeout(context.Background(), 5*time.Second)
		cpDB, err := tasmysql.Open(cpCtx, cfg.ControlPlane.MySQLDSN)
		cpCancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "init control plane: %v\n", err)
			os.Exit(1)
		}
		defer cpDB.Close()
		cdp = controlplane.NewDB(cpDB)
	}

	// Per-tenant session backends (backend_profiles): the router resolves one
	// service per tenant over the deployment's Redis endpoint, so a tenant's
	// session_backend choice is honoured at runtime instead of being a ledger
	// entry. File-mode deployments keep the platform-wide single service.
	sessionFor := func(string) session.Service { return sess }
	var router *storage.Router
	if cdp != nil {
		router = storage.NewRouter(sess, cfg.Storage.Session.RedisURL)
		defer router.Close()
		if err := applyBackendProfiles(context.Background(), cdp, router); err != nil {
			fmt.Fprintf(os.Stderr, "init backend profiles: %v\n", err)
			os.Exit(1)
		}
		sessionFor = router.For
	}
	reg, err := agent.NewRegistryWith(cfg, sessionFor)
	if err != nil {
		fmt.Fprintf(os.Stderr, "init runners: %v\n", err)
		os.Exit(1)
	}

	// Admin service owns the live config: adapters resolve bindings through
	// it so tenant hot updates take effect without rewiring the gateway.
	adm := admin.NewService(*configPath, cfg, reg, aud)
	if coord != nil {
		adm.WithRuntimeStore(admin.NewRedisRuntimeStore(coord))
	}
	if cdp != nil {
		adm.WithControlPlane(cdp, auth.NewResolver(cdp))
		// A settings write is the natural reload point for backend profiles:
		// the operator just told the platform something changed, so the router
		// re-reads the control plane here instead of waiting for a restart.
		adm.WithCommitHook(func() {
			if err := applyBackendProfiles(context.Background(), cdp, router); err != nil {
				slog.Warn("backend profiles: reload after settings commit failed", "err", err)
			}
		})
	}
	kf := channels.NewWeChatKf(adm.WeChatKfBinding)
	if coord != nil {
		kf.WithStateStore(coord)
	}
	gw := channels.NewGateway(reg,
		channels.NewWebChat(),
		channels.NewWeCom(adm.WeComBinding),
		kf,
	).WithGovernance(channels.Governance{
		PolicyFor: adm.Guardrails,
		// Read per dispatch, like the policy above: retuning the envelope needs
		// no rewiring of the gateway.
		LimitsFor: adm.Agent,
		Audit:     aud,
		Metrics:   rec,
	})
	if coord != nil {
		gw.WithCoordinator(coord)
	}
	srv := &http.Server{
		Addr: *addr,
		// Readiness is a closure over the live session service, evaluated per
		// probe: Redis going down mid-flight starts failing /readyz on the next
		// beat and recovers the same way, with no restart and no rewiring.
		Handler: web.NewServer(gw.Handler(), adm.Handler(), reg.IDs,
			func(ctx context.Context) error { return storage.Ping(ctx, sess) }),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("listening",
		"addr", *addr, "tenants", reg.IDs(),
		// Which source is authoritative is logged next to "session": an
		// operator debugging a config that did not take should see, before
		// anything else, whether this process is reading MySQL or the file.
		"control_plane", cfg.ControlPlane.Mode,
		"session", cfg.Storage.Session.Backend,
		// The envelope is logged at boot so a drill can be checked against what
		// the process actually loaded, not what the config file happens to say.
		"message_timeout", cfg.Agent.MessageTimeout,
		"max_concurrency_per_tenant", cfg.Agent.MaxConcurrencyPerTenant,
		"max_llm_calls", cfg.Agent.MaxLLMCalls,
		"audit", cfg.Audit.File,
		"traces", cfg.Telemetry.Traces.Exporter,
		"metrics", cfg.Telemetry.Metrics.Exporter,
		"chat_ui", "http://localhost"+*addr+"/",
		"admin", "http://localhost"+*addr+"/admin/tenants",
	)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "serve: %v\n", err)
		os.Exit(1)
	}
}

// applyBackendProfiles loads every tenant's latest backend profile into the
// session router: the empty-table case clears it back to the platform
// default, and a reload after a settings write is the same call.
func applyBackendProfiles(ctx context.Context, cdp *controlplane.DB, router *storage.Router) error {
	rows, err := cdp.ListLatestBackendProfiles(ctx)
	if err != nil {
		return err
	}
	settings := make(map[string]storage.BackendSetting, len(rows))
	for _, row := range rows {
		settings[row.TenantID] = storage.BackendSetting{
			Backend:    row.SessionBackend,
			KeyPrefix:  row.RedisKeyPrefix,
			SessionTTL: time.Duration(row.SessionTTLSeconds) * time.Second,
		}
	}
	router.ApplyProfiles(settings)
	return nil
}

// runHealthcheck asks the local /readyz whether the process can serve traffic
// and maps the answer onto an exit code: 0 ready, 1 not ready or unreachable.
// Diagnostics go to stderr and the reasons come from the response body, so a
// failing probe says why instead of just returning a number.
func runHealthcheck(addr string) int {
	url := "http://" + probeHost(addr) + web.ReadyPath
	ctx, cancel := context.WithTimeout(context.Background(), healthcheckTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: build request: %v\n", err)
		return 1
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "healthcheck: %s: %v\n", url, err)
		return 1
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "healthcheck: %s returned %d: %s\n",
			url, resp.StatusCode, strings.TrimSpace(string(body)))
		return 1
	}
	return 0
}

// runMigrate opens the control-plane database and applies pending schema
// migrations. It requires control_plane.mode=mysql: running migrations against
// a legacy deployment's YAML-only config has no database to migrate.
func runMigrate(cfg *config.Config) int {
	if cfg.ControlPlane.Mode != config.ControlPlaneMySQL {
		fmt.Fprintln(os.Stderr, "migrate: control_plane.mode must be mysql")
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	db, err := tasmysql.Open(ctx, cfg.ControlPlane.MySQLDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "migrate: open: %v\n", err)
		return 1
	}
	defer db.Close()
	res, err := tasmysql.Migrate(ctx, db)
	if err != nil {
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		return 1
	}
	if len(res.Applied) == 0 {
		fmt.Printf("migrate: already up to date (%d known)\n", len(res.AlreadyKnown))
		return 0
	}
	fmt.Printf("migrate: applied %v\n", res.Applied)
	return 0
}

// runBootstrapAdmin creates a tenant (if it does not exist yet) and that
// tenant's first admin credential. This cannot go through the Admin API: the
// API requires an admin token, and the first one is exactly what does not
// exist yet — and in a fresh reliable-mode deployment the tenant itself has
// no other way to come into being, since the YAML file no longer creates one.
func runBootstrapAdmin(cfg *config.Config, tenantID, subject, token string) int {
	if cfg.ControlPlane.Mode != config.ControlPlaneMySQL {
		fmt.Fprintln(os.Stderr, "bootstrap-admin: control_plane.mode must be mysql")
		return 1
	}
	if tenantID == "" || subject == "" || token == "" {
		fmt.Fprintln(os.Stderr, "bootstrap-admin: needs -bootstrap-admin-tenant, -bootstrap-admin-subject, -bootstrap-admin-token")
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := tasmysql.Open(ctx, cfg.ControlPlane.MySQLDSN)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bootstrap-admin: open: %v\n", err)
		return 1
	}
	defer db.Close()
	if _, err := tasmysql.Migrate(ctx, db); err != nil {
		fmt.Fprintf(os.Stderr, "bootstrap-admin: migrate: %v\n", err)
		return 1
	}
	cdp := controlplane.NewDB(db)
	// The tenant is created here rather than demanded from elsewhere: a
	// reliable-mode deployment has no YAML tenants to fall back on, so refusing
	// would make a fresh install unbootstrappable. An existing tenant is left
	// exactly as it is (a second bootstrap only adds another admin).
	if _, err := cdp.GetTenant(ctx, tenantID); errors.Is(err, controlplane.ErrNotFound) {
		if err := cdp.CreateTenant(ctx, tenantID, tenantID); err != nil {
			fmt.Fprintf(os.Stderr, "bootstrap-admin: create tenant %q: %v\n", tenantID, err)
			return 1
		}
		fmt.Printf("bootstrap-admin: created tenant %q\n", tenantID)
	} else if err != nil {
		fmt.Fprintf(os.Stderr, "bootstrap-admin: read tenant %q: %v\n", tenantID, err)
		return 1
	}
	resolver := auth.NewResolver(cdp)
	if _, err := resolver.CreateBootstrapAdmin(ctx, tenantID, subject, token); err != nil {
		fmt.Fprintf(os.Stderr, "bootstrap-admin: %v\n", err)
		return 1
	}
	// The token itself is never logged — only that one was created. It came
	// from the operator's own command line, so echoing it back adds nothing
	// they do not already have, and it would end up in CI logs.
	fmt.Printf("bootstrap-admin: created an admin for subject %q in tenant %q\n", subject, tenantID)
	return 0
}

// probeHost turns a listen address into something a client can dial: a bare
// ":8080" or an explicit any-interface bind listens on every address but is
// not itself connectable.
func probeHost(addr string) string {
	i := strings.LastIndex(addr, ":")
	if i < 0 {
		return addr
	}
	switch addr[:i] {
	case "", "0.0.0.0", "[::]":
		return "127.0.0.1" + addr[i:]
	}
	return addr
}
