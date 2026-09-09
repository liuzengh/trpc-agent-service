package main

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"slices"
	"strconv"
	"syscall"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/mock"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecomws"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wxkf"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	openaiembed "trpc.group/trpc-go/trpc-agent-go/knowledge/embedder/openai"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"
	sessionsummary "trpc.group/trpc-go/trpc-agent-go/session/summary"
	ttool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "serve":
			role := "all"
			if len(os.Args) > 2 {
				role = os.Args[2]
			}
			switch role {
			case "all", "gateway", "worker", "admin":
				if err := serve(role); err != nil {
					zap.L().Fatal("serve exited", zap.Error(err))
				}
				return
			}
		case "-h", "--help":
			fmt.Fprintf(os.Stderr, "usage: %s serve [all|gateway|worker|admin]\n", os.Args[0])
			return
		}
	}

	fmt.Printf("trpc-agent-service %s\n", trpcservice.Version)
	fmt.Println("multi-tenant node-based agent platform on tRPC-Agent-Go")
	fmt.Fprintf(os.Stderr, "usage: %s serve [all|gateway|worker|admin]\n", os.Args[0])
}

// serve runs one deployment role: "all" packs everything into
// one process (local/demo); "gateway" serves IM callbacks and outbound
// delivery; "worker" consumes the inbound stream and runs agents; "admin"
// serves the Admin API and housekeeping (archival).
func serve(role string) error {
	cfg := config.Load()
	plog.Init(cfg.LogLevel, cfg.LogFormat != "json")
	defer plog.Sync()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Config validation before any dependency probe: an unauthenticated Admin
	// API can repoint a tenant's model endpoint, so a missing
	// token has to refuse the role rather than degrade into an
	// unauthenticated listener. Deliberately ahead of Redis/PG — otherwise an
	// infra outage would decide whether the gate runs.
	if role == "all" || role == "admin" {
		if err := checkAdminToken(cfg); err != nil {
			return err
		}
	}

	// One secret resolver per process: file backend for local
	// dev, KMS sidecar when configured, always behind the short-TTL cache.
	secrets, err := buildSecretResolver(ctx, cfg)
	if err != nil {
		return err
	}

	// Tracing goes up before anything that emits spans; the endpoint comes
	// from OTEL_EXPORTER_OTLP_ENDPOINT.
	shutdownTrace, err := metrics.InitTracing(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = shutdownTrace()
	}()

	rdb, err := storage.NewRedis(ctx, cfg.RedisAddr)
	if err != nil {
		return err
	}
	defer func() { _ = rdb.Close() }()

	stream := storage.NewStream(rdb)
	if err := stream.EnsureGroup(ctx, storage.StreamInbound, "workers"); err != nil {
		return err
	}
	if err := stream.EnsureGroup(ctx, storage.StreamOutbound, channels.DefaultSenderGroup); err != nil {
		return err
	}
	// wecomws replies ride the same outbound stream but are delivered by the
	// wecomws leader's dedicated consumer group; the main sender skips them.
	if err := stream.EnsureGroup(ctx, storage.StreamOutbound, wecomws.SenderGroup); err != nil {
		return err
	}

	// PG down at startup degrades audit and routing to off with a warning —
	// the message pipeline must not depend on them.
	auditor, resolver, pgPool, pgCleanup := startPGConsumers(ctx, cfg)
	defer pgCleanup()
	// Every role watches config invalidations (publish/rollback/migration
	// read-switch broadcast by the admin role); the TTL remains
	// the fallback when a notification is lost.
	if resolver != nil {
		resolver.WatchInvalidations(ctx, rdb)
	}

	wantGateway := role == "all" || role == "gateway"
	wantWorker := role == "all" || role == "worker"
	wantAdmin := role == "all" || role == "admin"

	// Knowledge base: enabled only with an embeddings-capable endpoint
	// (TRPC_EMBEDDER_*); the default DeepSeek chat endpoint has none.
	// Worker (agent retrieval) and admin (document ingestion) both use it.
	var kb *knowledge.BuiltinKnowledge
	if wantWorker || wantAdmin {
		kb = buildKnowledge(ctx, cfg, secrets)
	}

	var (
		sessByType  map[string]session.Service
		defaultSess string
		processor   agent.Processor = agent.EchoProcessor{}
	)
	// Artifact storage (S3-compatible, MinIO locally): the gateway's media
	// sink and the worker's runner artifact service share one instance.
	var artifacts artifact.Service
	if wantWorker || wantGateway {
		artifacts = buildArtifact(cfg, secrets)
	}
	if wantWorker {
		var sessErr error
		sessByType, defaultSess, sessErr = buildSessionServices(ctx, cfg, pgPool, secrets)
		if sessErr != nil {
			plog.Warnf("session services unavailable (%v)", sessErr)
		}
		defer func() {
			for _, s := range sessByType {
				_ = s.Close()
			}
		}()
		var processorCleanup func()
		processor, processorCleanup = buildProcessor(ctx, cfg, rdb, auditor, pgPool, kb, resolver, sessByType, defaultSess, artifacts, secrets)
		defer processorCleanup()
	} else {
		// The admin role only needs the default backend name (migration
		// from_backend default).
		defaultSess = cfg.SessionBackend
		if defaultSess != "postgres" {
			defaultSess = "redis"
		}
	}

	metricsHandler, err := metrics.InitMetrics()
	if err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(ctx)
	consumer := fmt.Sprintf("%s-%s-%d", role, instanceID(), os.Getpid())

	if wantGateway {
		enqueue := web.EnqueueHandler{
			Stream:       stream,
			Dedup:        storage.NewDeduper(rdb),
			Routes:       resolver,
			Limiter:      storage.NewLimiter(rdb),
			DefaultQPS:   parseFloat(cfg.GatewayRateQPS, 50),
			DefaultBurst: parseInt(cfg.GatewayRateBurst, 100),
		}
		mux := http.NewServeMux()

		// Channel registry for the outbound sender: mock (only when enabled —
		// it is an unauthenticated injector, TRPC_MOCK_CHANNEL=false in prod),
		// wecom and wxkf when configured. Channel misconfiguration disables
		// only that channel. The artifact store doubles as the wecom media sink.
		channelSet := map[string]channels.Channel{}
		if cfg.MockChannel == "true" {
			ch := mock.New()
			ch.RegisterRoutes(mux, enqueue)
			channelSet[ch.Name()] = ch
		} else {
			plog.Infof("mock channel disabled (TRPC_MOCK_CHANNEL=false)")
		}
		var mediaSink channels.MediaStore
		if s3, ok := artifacts.(*storage.S3ArtifactService); ok {
			mediaSink = s3
		}
		// Per-binding outbound identity: binding-scoped replies resolve their
		// own corp/agent/KF credentials through the resolver's cached
		// snapshot. Without tenant routing the adapters stay on the
		// env-global identity (a nil interface, not a typed-nil wrapper).
		var outboundBindings channels.BindingProvider
		if resolver != nil {
			outboundBindings = bindingProvider{resolver}
		}
		if wc := startWecom(cfg, mediaSink, secrets, outboundBindings); wc != nil {
			wc.RegisterRoutes(mux, enqueue)
			channelSet[wc.Name()] = wc
		}
		if kf := startWxkf(cfg, secrets, outboundBindings, wxkfCursorStore{rdb}); kf != nil {
			kf.RegisterRoutes(mux, enqueue)
			channelSet[kf.Name()] = kf
		}
		// wecomws has no HTTP callback (WebSocket long connection); it only
		// joins the registry for the senders, and needs tenant routing to
		// enumerate its bindings.
		var wsStarter channels.Starter
		if resolver == nil {
			// Silent degradation here would black-hole every wecomws message
			// (Send fails, retry, dead-letter) with nothing in the logs.
			plog.Warnf("wecomws channel disabled: tenant routing (resolver) unavailable, " +
				"bindings cannot be enumerated")
		} else if ws := startWecomws(cfg, secrets, wsRoutes{resolver}); ws != nil {
			// *wecomws.Channel implements channels.Starter (long-lived
			// connections); the assignment is the compile-time check.
			channelSet[ws.Name()] = ws
			wsStarter = ws
		}

		// Multi-tenant callback paths:
		// /callback/{channel}/{binding_id} dispatches to the owning adapter
		// with the binding's own credential references, so each tenant's
		// webhook verifies under its own token/AES key. Bindings created
		// after startup become reachable on the next resolver refresh — no
		// restart, no mux surgery. The env-configured paths above stay
		// mounted as the single-binding default.
		if resolver != nil {
			bindingDispatch := web.BindingDispatcher{
				Channels: channelSet,
				Bindings: resolver,
				Handler:  enqueue,
			}
			mux.Handle("GET /callback/{channel}/{binding}", bindingDispatch)
			mux.Handle("POST /callback/{channel}/{binding}", bindingDispatch)
			plog.Infof("binding callbacks enabled (/callback/{channel}/{binding_id})")
		}

		srv := &http.Server{
			Addr:    cfg.HTTPAddr,
			Handler: mux,
			// Timeouts against slow/lazy clients (Slowloris): callback bodies
			// are small and replies are immediate in the async chain, so tight
			// limits are safe. WriteTimeout stays unset for future SSE.
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       10 * time.Second,
			IdleTimeout:       60 * time.Second,
		}
		g.Go(func() error {
			zap.L().Info("gateway listening",
				zap.String("addr", cfg.HTTPAddr), zap.String("mock_callback", "POST /mock/callback"))
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				return err
			}
			return nil
		})
		g.Go(func() error {
			<-gctx.Done()
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return srv.Shutdown(shutdownCtx)
		})

		// Per-tenant send pacing override (tenant.rate_policy send_qps/burst),
		// shared by both sender groups; nil leaves the platform default.
		var sendPolicyFor func(ctx context.Context, tenantID string) (float64, int, bool)
		if resolver != nil {
			sendPolicyFor = func(ctx context.Context, tenantID string) (float64, int, bool) {
				t, err := resolver.TenantByID(ctx, tenantID)
				if err != nil {
					return 0, 0, false
				}
				rl := tenant.ParseRateLimit(t.RatePolicy)
				return rl.SendQPS, rl.SendBurst, rl.SendQPS > 0 && rl.SendBurst > 0
			}
		}

		sender := &channels.Sender{
			Stream:        stream,
			Sent:          storage.NewSentMarker(rdb),
			Channels:      channelSet,
			Name:          consumer + "-s",
			Limiter:       storage.NewLimiter(rdb),
			SendQPS:       parseFloat(cfg.SendRateQPS, 20),
			SendBurst:     parseInt(cfg.SendRateBurst, 40),
			SendPolicyFor: sendPolicyFor,
			// wecomws messages belong to the leader's senders-ws group: ack
			// and step aside here — no send, no rate token, no sent: marker.
			Skip: func(m channels.OutboundMessage) bool { return m.Channel == wecomws.ChannelName },
		}
		g.Go(func() error { return sender.Run(gctx) })

		// wecomws: connections and their dedicated outbound consumer group
		// are owned by the platform-wide leader; other replicas idle until
		// the lease handover.
		if wsStarter != nil {
			wsSender := &channels.Sender{
				Stream:        stream,
				Sent:          storage.NewSentMarker(rdb),
				Channels:      map[string]channels.Channel{wsStarter.Name(): wsStarter},
				Name:          consumer + "-sws",
				Group:         wecomws.SenderGroup,
				Limiter:       storage.NewLimiter(rdb),
				SendQPS:       parseFloat(cfg.SendRateQPS, 20),
				SendBurst:     parseInt(cfg.SendRateBurst, 40),
				SendPolicyFor: sendPolicyFor,
				Skip:          func(m channels.OutboundMessage) bool { return m.Channel != wecomws.ChannelName },
			}
			g.Go(func() error {
				return runWecomwsLeader(gctx, storage.NewLeaderLock(rdb), wsStarter, wsSender, enqueue,
					consumer, parseDuration(cfg.WecomwsLeaderTTL, defaultWecomwsLeaderTTL))
			})
		}
	}

	if wantWorker {
		worker := &agent.Worker{
			Stream: stream, Lock: storage.NewLock(rdb), Processor: processor,
			Processed: storage.NewProcessedMarker(rdb),
			Name:      consumer + "-w",
		}
		g.Go(func() error { return worker.Run(gctx) })

		// Storage migration executor: advances active
		// migrations through backfilling → read switch → observation → done.
		if pgPool != nil && len(sessByType) > 0 {
			// The migrator type-asserts the concrete *PGSessionService for its
			// full-journal reads (storage/migrate.go), so it is handed the
			// undecorated backends; its batch path is not latency-metered.
			backends := make(map[string]session.Service, len(sessByType))
			for typ, svc := range sessByType {
				backends[typ] = storage.UnwrapSessionService(svc)
			}
			migrator := storage.NewMigrator(pgPool, rdb, backends,
				parseDuration(cfg.MigrationObserve, 24*time.Hour))
			g.Go(func() error { migrator.Run(gctx); return nil })
		}
	}

	if wantAdmin {
		if pgPool == nil {
			plog.Warnf("admin role degraded: PG unreachable, Admin API and archiver disabled")
		} else {
			adminAPI := web.NewAdminAPI(pgPool, auditor, rdb, cfg.AdminToken)
			adminAPI.Knowledge = kb
			adminAPI.DefaultSessionBackend = defaultSess
			adminAPI.ModelHosts = cfg.ModelHostAllowlist()

			// The admin API always gets its own listener (TRPC_ADMIN_ADDR):
			// the gateway listener faces the IM platforms (public), the admin
			// listener must not (mTLS optional). In
			// all-in-one mode this moves the admin API off :8080 too. This mux
			// carries nothing but /admin/* — every route on it is token-gated,
			// and /metrics lives on the internal metrics listener below.
			adminMux := http.NewServeMux()
			srv := &http.Server{Addr: cfg.AdminAddr, Handler: adminMux,
				ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
			tlsCfg, err := adminTLSConfig(cfg)
			if err != nil {
				return err
			}
			srv.TLSConfig = tlsCfg
			g.Go(func() error {
				zap.L().Info("admin listening", zap.String("addr", cfg.AdminAddr),
					zap.Bool("mtls", tlsCfg != nil))
				var err error
				if tlsCfg != nil {
					err = srv.ListenAndServeTLS("", "")
				} else {
					err = srv.ListenAndServe()
				}
				if err != nil && !errors.Is(err, http.ErrServerClosed) {
					return err
				}
				return nil
			})
			g.Go(func() error {
				<-gctx.Done()
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				return srv.Shutdown(shutdownCtx)
			})
			adminAPI.RegisterRoutes(adminMux)
			plog.Infof("admin API enabled (/admin/...)")

			// Monthly-ish archival: move old session_event /
			// audit_log rows to the archive tables so the hot tables stay small.
			archiver := storage.NewArchiver(pgPool,
				parseDuration(cfg.ArchiveRetention, 30*24*time.Hour),
				parseDuration(cfg.ArchiveInterval, 24*time.Hour))
			g.Go(func() error { archiver.Run(gctx); return nil })
		}
	}

	// Metrics ride their own internal listener in every role. The gateway's
	// callback mux faces the IM platforms, and these series carry per-tenant
	// traffic volumes, token spend and queue depth — reconnaissance material
	// for anyone who can reach the callbacks. Not token-gated either: the
	// deployment's own liveness/readiness probes scrape this endpoint. The
	// listener is auxiliary, so a bind failure (a second role colocated on the
	// same address in local dev) leaves metrics off instead of taking the
	// process down.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", metricsHandler)
	metricsSrv := &http.Server{Addr: cfg.MetricsAddr, Handler: metricsMux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second}
	g.Go(func() error {
		zap.L().Info("metrics listening", zap.String("addr", cfg.MetricsAddr))
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			plog.Warnf("metrics listener failed (metrics off): %v", err)
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return metricsSrv.Shutdown(shutdownCtx)
	})

	// Queue depth / pending gauges feeding the alerts.
	metrics.StartStreamCollector(gctx, stream, 15*time.Second)

	return g.Wait()
}

// checkAdminToken enforces that the Admin API is internal only: an
// unset TRPC_ADMIN_TOKEN is a fatal misconfiguration rather than a dev-mode
// warning, because the API can repoint a tenant's model endpoint and rewrite
// its policies. Local development opts out with the explicit
// config.AdminTokenDevInsecure sentinel — and even then only on a loopback
// bind: the sentinel is a public constant, not a secret, so pairing it with
// an all-interfaces listen address would just publish an open management
// plane.
func checkAdminToken(cfg config.Config) error {
	if cfg.AdminToken == "" {
		return fmt.Errorf("TRPC_ADMIN_TOKEN must be set to serve the Admin API "+
			"(or %q to run it unauthenticated on localhost only)",
			config.AdminTokenDevInsecure)
	}
	if cfg.AdminToken == config.AdminTokenDevInsecure {
		if !isLoopbackBind(cfg.AdminAddr) {
			return fmt.Errorf("TRPC_ADMIN_TOKEN=%s must not serve on %q: the dev "+
				"sentinel is accepted on loopback binds only — set a real token or "+
				"TRPC_ADMIN_ADDR=127.0.0.1:8081",
				config.AdminTokenDevInsecure, cfg.AdminAddr)
		}
		plog.Warnf("admin API unprotected (TRPC_ADMIN_TOKEN=%s) — dev mode only",
			config.AdminTokenDevInsecure)
	}
	return nil
}

// isLoopbackBind reports whether the listen address is loopback-only. An
// empty host (":8081") binds all interfaces and is not loopback.
func isLoopbackBind(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "" {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return host == "localhost"
}

// startPGConsumers connects to PG and starts the consumers that depend on it:
// the audit flush loop and the tenant resolver used for gateway routing. PG
// being down degrades both to off with a warning; the message pipeline does
// not depend on them (audit catches up from logs/traces during the outage,
// and a nil resolver disables tenant routing). The returned cleanup stops the
// auditor and closes the shared pool.
func startPGConsumers(ctx context.Context, cfg config.Config) (*storage.Auditor, *tenant.Resolver, *pgxpool.Pool, func()) {
	noop := func() {}
	pool, err := storage.NewPG(ctx, cfg.PGDSN)
	if err != nil {
		// PG down at startup: build the pool lazily so routing fails CLOSED
		// (5xx, the IM retries) and recovers when PG returns — never run
		// fail-open with tenant routing silently off.
		plog.Warnf("PG unreachable at startup (%v); routing fails closed until PG recovers", err)
		pool, err = storage.NewPGLazy(ctx, cfg.PGDSN)
		if err != nil {
			plog.Errorf("PG DSN invalid: %v", err)
			return nil, nil, nil, noop
		}
	}
	a := storage.NewAuditor(pool)
	a.Start()
	plog.Infof("audit and tenant routing enabled")
	return a, tenant.NewResolver(tenant.NewPGStore(pool)), pool, func() {
		a.Close()
		pool.Close()
	}
}

// schemeIs reports whether an external endpoint URL carries the wanted
// scheme: the IM token endpoints put the corp secret in the query string, so
// an http:// base ships it in cleartext across every network between here and
// the IM. The model endpoint is held to the same bar by the tenant config
// validator.
func schemeIs(base, want string) bool {
	u, err := url.Parse(base)
	return err == nil && u.Scheme == want && u.Hostname() != ""
}

// startWecom builds the WeCom channel from env config; it returns nil (with a
// warning) when the channel is not configured or its secrets are missing, so
// the rest of the platform keeps serving the other channels. media is the
// artifact store for inbound media_id fetches (nil degrades to placeholders);
// bindings resolves the per-binding outbound identity at Send time (nil keeps
// every reply on the env-global identity).
func startWecom(cfg config.Config, media channels.MediaStore, secrets config.SecretResolver, bindings channels.BindingProvider) *wecom.Channel {
	if cfg.WecomCorpID == "" {
		return nil
	}
	if !schemeIs(cfg.WecomAPIBase, "https") {
		plog.Warnf("wecom channel disabled: TRPC_WECOM_API_BASE %q must use https", cfg.WecomAPIBase)
		return nil
	}
	agentID, err := strconv.Atoi(cfg.WecomAgentID)
	if err != nil || agentID == 0 {
		plog.Warnf("wecom channel disabled: invalid TRPC_WECOM_AGENT_ID %q", cfg.WecomAgentID)
		return nil
	}
	wc, err := wecom.New(wecom.Config{
		CorpID:    cfg.WecomCorpID,
		AgentID:   agentID,
		TokenRef:  cfg.WecomTokenRef,
		AESKeyRef: cfg.WecomAESKeyRef,
		SecretRef: cfg.WecomSecretRef,
		APIBase:   cfg.WecomAPIBase,
		Media:     media,
		Bindings:  bindings,
	}, secrets)
	if err != nil {
		plog.Warnf("wecom channel disabled: %v", err)
		return nil
	}
	plog.Infof("wecom channel enabled (callback: POST /wecom/callback)")
	return wc
}

// startWxkf builds the WeChat KF channel from env config; it returns nil
// (with a warning) when the channel is not configured or its secrets are
// missing, so the other channels keep serving.
func startWxkf(cfg config.Config, secrets config.SecretResolver, bindings channels.BindingProvider, cursors wxkf.CursorStore) *wxkf.Channel {
	if cfg.WxkfCorpID == "" || cfg.WxkfKfAccount == "" {
		return nil
	}
	if !schemeIs(cfg.WxkfAPIBase, "https") {
		plog.Warnf("wxkf channel disabled: TRPC_WXKF_API_BASE %q must use https", cfg.WxkfAPIBase)
		return nil
	}
	ch, err := wxkf.New(wxkf.Config{
		CorpID:    cfg.WxkfCorpID,
		KfAccount: cfg.WxkfKfAccount,
		TokenRef:  cfg.WxkfTokenRef,
		AESKeyRef: cfg.WxkfAESKeyRef,
		SecretRef: cfg.WxkfSecretRef,
		APIBase:   cfg.WxkfAPIBase,
		Bindings:  bindings,
		Cursors:   cursors,
	}, secrets)
	if err != nil {
		plog.Warnf("wxkf channel disabled: %v", err)
		return nil
	}
	plog.Infof("wxkf channel enabled (callback: POST /wxkf/callback)")
	return ch
}

// startWecomws builds the WeCom smart-bot WebSocket channel from env config;
// nil (silently) when TRPC_WECOMWS_ADDR is unset. Bots and their secret
// references live in channel_binding rows enumerated through the routes
// adapter.
func startWecomws(cfg config.Config, secrets config.SecretResolver, routes wecomws.RoutesProvider) *wecomws.Channel {
	if cfg.WecomwsAddr == "" {
		return nil
	}
	if !schemeIs(cfg.WecomwsAddr, "wss") {
		plog.Warnf("wecomws channel disabled: TRPC_WECOMWS_ADDR %q must use wss", cfg.WecomwsAddr)
		return nil
	}
	ws, err := wecomws.New(secrets,
		wecomws.WithAddr(cfg.WecomwsAddr),
		wecomws.WithRoutes(routes),
		wecomws.WithPingInterval(parseDuration(cfg.WecomwsPingInterval, wecomws.DefaultPingInterval)),
		wecomws.WithSegmentBytes(parseInt(cfg.WecomwsSegmentBytes, wecomws.DefaultSegmentBytes)),
		wecomws.WithResyncInterval(parseDuration(cfg.WecomwsResyncInterval, wecomws.DefaultResyncInterval)),
	)
	if err != nil {
		plog.Warnf("wecomws channel disabled: %v", err)
		return nil
	}
	plog.Infof("wecomws channel enabled (addr %s, leader ttl %s)", cfg.WecomwsAddr,
		parseDuration(cfg.WecomwsLeaderTTL, defaultWecomwsLeaderTTL))
	return ws
}

// wsRoutes projects the tenant resolver onto the wecomws binding source: the
// channel only needs the binding id, webhook path and config jsonb.
type wsRoutes struct{ r *tenant.Resolver }

func (w wsRoutes) RoutesByChannel(ctx context.Context, channel string) ([]wecomws.Binding, error) {
	routes, err := w.r.RoutesByChannel(ctx, channel)
	if err != nil {
		return nil, err
	}
	out := make([]wecomws.Binding, 0, len(routes))
	for _, route := range routes {
		out = append(out, wecomws.Binding{
			ID:          route.Binding.ID,
			WebhookPath: route.Binding.WebhookPath,
			Config:      route.Binding.Config,
		})
	}
	return out, nil
}

// bindingProvider projects the tenant resolver onto the outbound binding
// lookup: the adapters only need the binding id and config jsonb to resolve
// the per-binding outbound identity at Send time. The resolver is
// cache-backed, so the per-send lookup is a snapshot read.
type bindingProvider struct{ r *tenant.Resolver }

func (b bindingProvider) BindingByID(ctx context.Context, id string) (channels.OutboundBinding, error) {
	binding, err := b.r.BindingByID(ctx, id)
	if err != nil {
		return channels.OutboundBinding{}, err
	}
	return channels.OutboundBinding{ID: binding.ID, Config: binding.Config}, nil
}

// wxkfCursorStore persists the wxkf sync_msg pull position per KF account in
// Redis. No TTL: a stale cursor only re-pulls history the platform still
// serves (three days), and inbound dedup absorbs the overlap.
type wxkfCursorStore struct{ rdb *redis.Client }

func (s wxkfCursorStore) Get(ctx context.Context, openKfID string) (string, error) {
	v, err := s.rdb.Get(ctx, "wxkf:cursor:"+openKfID).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	return v, err
}

func (s wxkfCursorStore) Set(ctx context.Context, openKfID, cursor string) error {
	return s.rdb.Set(ctx, "wxkf:cursor:"+openKfID, cursor, 0).Err()
}

// Re-campaign pacing: the base wait doubles per consecutive quick failure
// toward the cap, so an instantly failing child (e.g. misconfiguration) does
// not spin the acquire/run/release cycle on a fixed 2s cadence forever.
const (
	recampaignBase          = 2 * time.Second
	recampaignCap           = time.Minute
	defaultWecomwsLeaderTTL = 15 * time.Second
)

func recampaignWait(failStreak int) time.Duration {
	d := recampaignBase
	for i := 0; i < failStreak && d < recampaignCap; i++ {
		d *= 2
	}
	return min(d, recampaignCap)
}

// runWecomwsLeader campaigns for the platform-wide wecomws leadership and,
// while held, runs the bot connections and the senders-ws consumer group.
// Losing the lease (another replica took over and kicked our connections)
// or a child failure tears everything down; the loop re-campaigns until ctx
// is done, so the whole group can take over after a leader crash.
func runWecomwsLeader(ctx context.Context, leader *storage.LeaderLock, starter channels.Starter, wsSender *channels.Sender, h channels.Handler, owner string, ttl time.Duration) error {
	failStreak := 0
	for ctx.Err() == nil {
		release, lost, err := leader.Acquire(ctx, wecomws.ChannelName, owner, ttl)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			plog.Warnf("wecomws leader acquire: %v", err)
			wait := recampaignWait(failStreak)
			failStreak++
			if !wecomws.Sleep(ctx, wait) {
				return nil
			}
			continue
		}
		plog.Infof("wecomws leadership acquired (owner %s)", owner)
		started := time.Now()
		runCtx, cancel := context.WithCancel(ctx)
		inner, innerCtx := errgroup.WithContext(runCtx)
		inner.Go(func() error { return starter.Start(innerCtx, h) })
		inner.Go(func() error { return wsSender.Run(innerCtx) })
		var innerErr error
		innerDone := make(chan struct{})
		go func() {
			innerErr = inner.Wait()
			close(innerDone)
		}()
		select {
		case <-lost:
			plog.Warnf("wecomws leadership lost (owner %s): stopping connections and senders-ws", owner)
		case <-ctx.Done():
		case <-innerDone:
		}
		cancel()
		release()
		<-innerDone // drain: children stopped, run loops returned
		wait := recampaignBase
		if innerErr != nil && ctx.Err() == nil {
			plog.Errorf("wecomws leader role failed, re-campaigning: %v", innerErr)
			wait = recampaignWait(failStreak)
			if time.Since(started) < recampaignCap {
				failStreak++
			} else {
				failStreak = 0
			}
		} else {
			failStreak = 0
		}
		if ctx.Err() != nil {
			return nil
		}
		if !wecomws.Sleep(ctx, wait) {
			return nil
		}
	}
	return nil
}

// buildSessionServices builds one session service per supported backend
// plus the platform default selection. WithEnableTracing
// is required beyond observability: with tracing disabled, the redis session
// service's startSpan falls back to the caller's active span and its defer
// span.End() would end OUR worker span prematurely (framework quirk).
func buildSessionServices(ctx context.Context, cfg config.Config, pgPool *pgxpool.Pool, secrets config.SecretResolver) (map[string]session.Service, string, error) {
	byType := map[string]session.Service{}
	rs, err := sessionredis.NewService(
		sessionredis.WithRedisClientURL("redis://"+cfg.RedisAddr),
		sessionredis.WithEnableTracing(true),
	)
	if err != nil {
		return nil, "", fmt.Errorf("redis session service: %w", err)
	}
	// Each backend is wrapped with per-op latency metrics at creation, so the
	// assembler's migration fanout composes already-metered services and
	// per-backend attribution survives a migration. The migrator itself gets
	// the undecorated services (see its construction below).
	byType["redis"] = &storage.MetricsSessionService{Backend: "redis", Inner: rs}
	if pgPool != nil {
		var sessOpts []storage.PGSessionOption
		if sm := buildSummarizer(ctx, cfg, secrets); sm != nil {
			sessOpts = append(sessOpts, storage.WithSummarizer(sm))
		}
		byType["postgres"] = &storage.MetricsSessionService{
			Backend: "postgres",
			Inner:   storage.NewPGSessionService(pgPool, sessOpts...),
		}
	} else if cfg.SessionBackend == "postgres" {
		plog.Warnf("TRPC_SESSION_BACKEND=postgres but PG is unreachable, defaulting to redis")
	}
	def := cfg.SessionBackend
	if _, ok := byType[def]; !ok {
		plog.Warnf("session backend %q unavailable, defaulting to redis", def)
		def = "redis"
	}
	return byType, def, nil
}

// buildArtifact builds the S3-compatible artifact store (MinIO locally);
// unreachable endpoints degrade to nil with a warning.
func buildArtifact(cfg config.Config, secrets config.SecretResolver) artifact.Service {
	if cfg.S3Endpoint == "" {
		return nil
	}
	svc, err := storage.NewS3ArtifactService(storage.S3ArtifactConfig{
		Endpoint:     cfg.S3Endpoint,
		AccessKeyRef: cfg.S3AccessKeyRef,
		SecretKeyRef: cfg.S3SecretKeyRef,
		Bucket:       cfg.S3Bucket,
		Secure:       cfg.S3Secure == "true",
		Prefix:       "artifact/",
		Secrets:      secrets,
	})
	if err != nil {
		plog.Warnf("artifact store unavailable (%v), artifacts disabled", err)
		return nil
	}
	plog.Infof("artifact store enabled (s3 %s, bucket %s)", cfg.S3Endpoint, cfg.S3Bucket)
	return svc
}

// buildProcessor assembles the processing chain: the platform tool registry,
// the dangerous-tool Approver, the per-app Assembler (one Runner per
// agent_app, rebuilt when the resolver reports a config change), and the
// Guarded guardrail wrapper that owns input/output checks and message-level
// auditing. Apps whose model key is missing are served by the echo fallback,
// so the pipeline stays demoable without LLM access.
// The returned cleanup closes resources.
func buildProcessor(ctx context.Context, cfg config.Config, rdb *redis.Client, auditor *storage.Auditor, pgPool *pgxpool.Pool, kb *knowledge.BuiltinKnowledge, resolver *tenant.Resolver, sessByType map[string]session.Service, defaultSess string, artifacts artifact.Service, secrets config.SecretResolver) (agent.Processor, func()) {
	noop := func() {}

	registry := tool.DemoTools()
	approver := agent.NewApprover(rdb, registry, 0)

	// Cost accounting: token counts are always recorded in audit; prices turn
	// them into cost where configured.
	if cfg.ModelPrices != "" {
		var table map[string][2]float64
		if err := json.Unmarshal([]byte(cfg.ModelPrices), &table); err != nil {
			plog.Warnf("invalid TRPC_MODEL_PRICES, cost tracking disabled: %v", err)
		} else {
			agent.SetModelPricing(table)
		}
	}
	wrap := func(inner agent.Processor) agent.Processor {
		g := &agent.Guarded{
			Inner:    inner,
			Approver: approver,
			Auditor:  auditor,
			Input:    []agent.InputChecker{agent.SensitiveWordInput(agent.DefaultBlockedWords)},
			Output:   []agent.OutputChecker{agent.RedactOutput()},
			Budget:   storage.NewBudget(rdb),
		}
		// Tenant guardrail policies (guardrail_policy) resolve through the
		// resolver's cached snapshot.
		if resolver != nil {
			g.PolicyFor = func(ctx context.Context, tenantID string) (tenant.GuardrailPolicy, error) {
				t, err := resolver.TenantByID(ctx, tenantID)
				if err != nil {
					return tenant.GuardrailPolicy{}, err
				}
				return tenant.ParseGuardrailPolicy(t.GuardrailPolicy), nil
			}
		}
		return g
	}

	if len(sessByType) == 0 {
		return wrap(agent.EchoProcessor{}), noop
	}

	timeout, err := time.ParseDuration(cfg.ModelTimeout)
	if err != nil || timeout <= 0 {
		plog.Warnf("invalid TRPC_MODEL_TIMEOUT %q, defaulting to %s", cfg.ModelTimeout, agent.DefaultRunTimeout)
		timeout = agent.DefaultRunTimeout
	}

	// Memory service over the PG memory_item table (two-level scope, soft
	// delete), shared by all per-app runners; with an embeddings-capable
	// endpoint the service gains semantic recall (async embedding worker).
	var memService memory.Service
	if pgPool != nil {
		if emb, _, err := buildEmbedder(ctx, cfg, secrets); err == nil && emb != nil {
			memService = storage.NewPGMemoryService(pgPool, storage.WithMemoryEmbedder(emb))
		} else {
			memService = storage.NewPGMemoryService(pgPool)
		}
	}

	callbacks := ttool.NewCallbacks().RegisterBeforeTool(approver.BeforeTool)
	// Guard the typed-nil interfaces: a nil *tenant.Resolver / nil
	// *BuiltinKnowledge must stay nil behind the interface, or the assembler
	// would route to nothing / wire a knowledge tool onto nothing.
	var apps agent.AppProvider
	if resolver != nil {
		apps = resolver
	}
	var kbIface knowledge.Knowledge
	if kb != nil {
		kbIface = kb
	}
	assembler := agent.NewAssembler(agent.AssemblerConfig{
		Apps:      apps,
		Secrets:   secrets,
		Registry:  registry,
		Memory:    memService,
		Knowledge: kbIface,
		Callbacks: callbacks,
		// Session backends: the tenant's storage_config routes its apps
		// between them (migration flow only); the env default serves the rest.
		SessionsByType: sessByType,
		DefaultSession: defaultSess,
		Artifact:       artifacts,
		Defaults: agent.ModelSpec{
			Name:      cfg.ModelName,
			BaseURL:   cfg.ModelBaseURL,
			APIKeyRef: cfg.ModelAPIKeyRef,
		},
		ModelHosts: cfg.ModelHostAllowlist(),
		DefaultApp: cfg.AppName,
		Timeout:    timeout,
		Retries:    1,
	})
	plog.Infof("per-app runner assembler ready (model default=%s, timeout=%s, session backends=%v)",
		cfg.ModelName, timeout, slices.Sorted(maps.Keys(sessByType)))
	guarded := wrap(assembler).(*agent.Guarded)
	// Recall events mark the session state on the tenant's session backend;
	// the marker is best-effort and never blocks the ack.
	guarded.StateMark = func(ctx context.Context, msg channels.InboundMessage, key string, value []byte) error {
		svc, err := assembler.SessionServiceFor(ctx, msg.AppID)
		if err != nil {
			return err
		}
		return svc.UpdateSessionState(ctx,
			session.Key{AppName: msg.AppID, UserID: msg.UserID, SessionID: msg.SessionKey},
			session.StateMap{key: value})
	}
	return guarded, func() { _ = assembler.Close() }
}

// buildSummarizer builds the framework session summarizer on the platform
// default model. Summarization is a background maintenance job, so it uses
// the env default model rather than per-tenant model config. Without a model
// key, summaries are disabled and sessions always replay in full.
func buildSummarizer(ctx context.Context, cfg config.Config, secrets config.SecretResolver) sessionsummary.SessionSummarizer {
	key, err := secrets.Resolve(ctx, cfg.ModelAPIKeyRef)
	if err != nil {
		plog.Warnf("model key %q unavailable (%v), session summaries disabled", cfg.ModelAPIKeyRef, err)
		return nil
	}
	threshold := parseInt(cfg.SummaryEventThreshold, 20)
	m := openai.New(cfg.ModelName,
		openai.WithBaseURL(cfg.ModelBaseURL),
		openai.WithAPIKey(key),
	)
	plog.Infof("session summarizer enabled (event threshold=%d)", threshold)
	return sessionsummary.NewSummarizer(m, sessionsummary.WithEventThreshold(threshold))
}

// instanceID names this replica for consumer-group and lock-owner tokens.
// The PID alone is not enough: a container runs its entrypoint as PID 1, so
// every replica of the same image would share one name and the session lock
// could not tell which replica holds a lease. Hostname separates
// replicas (the pod name under k8s); the random fallback covers hosts that
// have none, where several processes may also start at once.
func instanceID() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			return "anon-" + strconv.FormatInt(time.Now().UnixNano(), 36)
		}
		return "anon-" + hex.EncodeToString(b[:])
	}
	return h
}

// parseInt, parseFloat and parseDuration parse env string values, falling
// back to def with a warning on invalid input.
func parseInt(s string, def int) int {
	v, err := strconv.Atoi(s)
	if err != nil || v <= 0 {
		plog.Warnf("invalid integer %q, using default %d", s, def)
		return def
	}
	return v
}

func parseFloat(s string, def float64) float64 {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v <= 0 {
		plog.Warnf("invalid number %q, using default %v", s, def)
		return def
	}
	return v
}

func parseDuration(s string, def time.Duration) time.Duration {
	v, err := time.ParseDuration(s)
	if err != nil || v <= 0 {
		plog.Warnf("invalid duration %q, using default %s", s, def)
		return def
	}
	return v
}

// adminTLSConfig builds the mTLS config for the split admin listener (mTLS or
// SSO token required). Nil when the three envs are not
// all set; set means TLS + verified client certificates.
func adminTLSConfig(cfg config.Config) (*tls.Config, error) {
	if cfg.AdminTLSCert == "" || cfg.AdminTLSKey == "" || cfg.AdminTLSClientCA == "" {
		return nil, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.AdminTLSCert, cfg.AdminTLSKey)
	if err != nil {
		return nil, fmt.Errorf("admin tls keypair: %w", err)
	}
	ca, err := os.ReadFile(cfg.AdminTLSClientCA)
	if err != nil {
		return nil, fmt.Errorf("admin tls client ca: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("admin tls client ca %s: no PEM certificates", cfg.AdminTLSClientCA)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}

// buildSecretResolver builds the secret backend: the file
// resolver for local development, the KMS sidecar when TRPC_SECRET_RESOLVER=kms
// (its bearer token bootstraps from the file resolver), and in both cases the
// short-TTL cache that absorbs a KMS blip.
//
// A misconfigured KMS fails startup rather than falling back to files. Both
// failures here are configuration (an unreadable bootstrap token, an empty
// endpoint) that retrying cannot fix, and degrading would spend the whole
// process lifetime on plaintext-on-disk secrets behind one warn line —
// including credentials already rotated away in the KMS.
func buildSecretResolver(ctx context.Context, cfg config.Config) (config.SecretResolver, error) {
	file := config.NewFileResolver(cfg.SecretsDir)
	base := config.SecretResolver(file)
	if cfg.SecretResolverType == "kms" {
		token, err := file.Resolve(ctx, cfg.KMSTokenRef)
		if err != nil {
			return nil, fmt.Errorf("kms bootstrap token %q: %w", cfg.KMSTokenRef, err)
		}
		kms, err := config.NewKMSResolver(cfg.KMSEndpoint, token)
		if err != nil {
			return nil, fmt.Errorf("kms resolver: %w", err)
		}
		base = kms
		plog.Infof("secret resolver: kms (%s)", cfg.KMSEndpoint)
	}
	return config.NewCachedResolver(base, parseDuration(cfg.SecretCacheTTL, time.Minute)), nil
}

// buildEmbedder builds the OpenAI-compatible embeddings client shared by
// Knowledge and memory semantic recall; (nil, 0, nil) when TRPC_EMBEDDER_MODEL
// is unset — the default chat endpoint (DeepSeek) has no embeddings API.
func buildEmbedder(ctx context.Context, cfg config.Config, secrets config.SecretResolver) (embedder.Embedder, int, error) {
	if cfg.EmbedderModel == "" {
		return nil, 0, nil
	}
	key, err := secrets.Resolve(ctx, cfg.EmbedderKeyRef)
	if err != nil {
		return nil, 0, fmt.Errorf("embedder key %q: %w", cfg.EmbedderKeyRef, err)
	}
	dim, err := strconv.Atoi(cfg.EmbedderDim)
	if err != nil || dim <= 0 {
		return nil, 0, fmt.Errorf("invalid TRPC_EMBEDDER_DIMENSION %q", cfg.EmbedderDim)
	}
	return openaiembed.New(
		openaiembed.WithModel(cfg.EmbedderModel),
		openaiembed.WithAPIKey(key),
		openaiembed.WithBaseURL(cfg.EmbedderBaseURL),
		openaiembed.WithDimensions(dim),
	), dim, nil
}

// buildKnowledge builds the pgvector-backed knowledge base when an
// embeddings-capable endpoint is configured; otherwise it returns nil and the
// agent runs without knowledge retrieval. It also kicks off the one-shot
// agent.RekeyLegacyDocuments pass over document IDs written before tenant
// scoping — every replica runs it; the pass is idempotent and converges
// under concurrency.
func buildKnowledge(ctx context.Context, cfg config.Config, secrets config.SecretResolver) *knowledge.BuiltinKnowledge {
	emb, dim, err := buildEmbedder(ctx, cfg, secrets)
	if err != nil {
		plog.Warnf("embedder unavailable (%v), knowledge disabled", err)
		return nil
	}
	if emb == nil {
		return nil
	}
	kb, vs, err := agent.NewKnowledgeBase(cfg.PGDSN, cfg.KnowledgeTable, dim, emb)
	if err != nil {
		plog.Warnf("knowledge base unavailable (%v), knowledge disabled", err)
		return nil
	}
	go func() {
		// Detached context: a deployment-time data fix must not die with the
		// startup request's cancellation, but it must not hang forever either.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Minute)
		defer cancel()
		n, err := agent.RekeyLegacyDocuments(rctx, vs)
		if err != nil {
			plog.Warnf("knowledge legacy re-key pass failed (retried at next start): %v", err)
			return
		}
		if n > 0 {
			plog.Infof("knowledge legacy re-key: %d document(s) moved to scoped IDs", n)
		}
	}()
	plog.Infof("knowledge base enabled (model=%s, dim=%d)", cfg.EmbedderModel, dim)
	return kb
}
