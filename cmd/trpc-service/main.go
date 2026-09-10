package main

import (
	"context"
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

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
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
	// Redis backs both framework sessions and cross-replica coordination. Load
	// the shared runtime config before constructing runners so a rescheduled Pod
	// starts from the latest authenticated admin change, not its ConfigMap copy.
	var coord *coordination.Redis
	if cfg.Storage.Session.Backend == config.BackendRedis {
		coord, err = coordination.NewRedis(cfg.Storage.Session.RedisURL, cfg.Storage.Session.KeyPrefix)
		if err != nil {
			fmt.Fprintf(os.Stderr, "init coordination: %v\n", err)
			os.Exit(1)
		}
		defer coord.Close()
		if persisted, err := admin.NewRedisRuntimeStore(coord).Load(context.Background()); err != nil {
			fmt.Fprintf(os.Stderr, "load runtime config: %v\n", err)
			os.Exit(1)
		} else if persisted != nil {
			// Storage is deployment-owned and must not be changed through admin.
			persisted.Storage = cfg.Storage
			if cfg.Admin.Token != "" {
				persisted.Admin.Token = cfg.Admin.Token
			}
			cfg = persisted
		}
	}
	// Governance trail and telemetry (proposal doc 3.5): the audit file is
	// optional, exporters default to off so local runs stay quiet.
	aud, err := audit.New(cfg.Audit.File)
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
	reg, err := agent.NewRegistry(cfg, sess)
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
