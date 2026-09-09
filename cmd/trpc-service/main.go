package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

func main() {
	configPath := flag.String("config", config.DefaultPath, "path to YAML config file")
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

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
	gw := channels.NewGateway(reg,
		channels.NewWebChat(),
		channels.NewWeCom(adm.WeComBinding),
		channels.NewWeChatKf(adm.WeChatKfBinding),
	).WithGovernance(channels.Governance{
		PolicyFor: adm.Guardrails,
		Audit:     aud,
		Metrics:   rec,
	})
	srv := &http.Server{
		Addr:              *addr,
		Handler:           web.NewServer(gw.Handler(), adm.Handler(), reg.IDs),
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
