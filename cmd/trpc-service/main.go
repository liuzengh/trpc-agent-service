package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
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
	reg, err := agent.NewRegistry(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "init runners: %v\n", err)
		os.Exit(1)
	}

	// Admin service owns the live config: adapters resolve bindings through
	// it so tenant hot updates take effect without rewiring the gateway.
	adm := admin.NewService(*configPath, cfg, reg)
	gw := channels.NewGateway(reg, channels.NewWebChat(), channels.NewWeCom(adm.WeComBinding))
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

	fmt.Printf("listening on %s, tenants=%v, chat UI: http://localhost%s/, admin: %s/admin/tenants\n", *addr, reg.IDs(), *addr, *addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "serve: %v\n", err)
		os.Exit(1)
	}
}
