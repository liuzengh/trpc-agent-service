package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/wecombot"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
func run() error {
	config := wecombot.Config{BotID: os.Getenv("WECOM_BOT_ID"), BotSecret: os.Getenv("WECOM_BOT_SECRET"), BridgeToken: os.Getenv("TRPC_SECRET_WECOM_BOT_BRIDGE_TOKEN"), GatewayBaseURL: os.Getenv("WECOM_BOT_GATEWAY_BASE_URL"), RouteKey: os.Getenv("WECOM_BOT_WEBHOOK_ROUTE_KEY"), StateDir: os.Getenv("WECOM_BOT_STATE_DIR")}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8090"
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return wecombot.ErrConfiguration
	}
	connector, err := wecombot.New(config, func(e wecombot.Event) {
		log.Printf("wecom_bot operation=%s outcome=%s code=%d", e.Operation, e.Outcome, e.Code)
	})
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	server := &http.Server{Addr: ":" + port, Handler: connector, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	httpDone := make(chan error, 1)
	connectorDone := make(chan error, 1)
	go func() { httpDone <- server.ListenAndServe() }()
	go func() { connectorDone <- connector.Run(ctx) }()
	var result error
	connectorFinished, httpFinished := false, false
	select {
	case <-ctx.Done():
	case result = <-connectorDone:
		connectorFinished = true
	case result = <-httpDone:
		httpFinished = true
	}
	cancel()
	shutdownCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if server.Shutdown(shutdownCtx) != nil {
		_ = server.Close()
	}
	if !connectorFinished {
		<-connectorDone
	}
	if !httpFinished {
		<-httpDone
	}
	if errors.Is(result, context.Canceled) || errors.Is(result, http.ErrServerClosed) {
		return nil
	}
	return result
}
