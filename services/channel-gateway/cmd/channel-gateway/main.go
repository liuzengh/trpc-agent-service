package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/bootstrap"
	transport "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/infra/nats"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "channel-gateway:", err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) > 1 && os.Args[1] == "probe" {
		if len(os.Args) != 3 {
			return fmt.Errorf("usage: channel-gateway probe URL")
		}
		client := http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get(os.Args[2])
		if err != nil {
			return fmt.Errorf("probe failed")
		}
		defer resp.Body.Close()
		if resp.StatusCode != 204 {
			return fmt.Errorf("probe status %d", resp.StatusCode)
		}
		return nil
	}
	if len(os.Args) > 1 && os.Args[1] == "nats-config" {
		if len(os.Args) != 3 {
			return fmt.Errorf("usage: channel-gateway nats-config permissions.yaml")
		}
		b, err := transport.RenderServerConfig(os.Args[2])
		if err != nil {
			return err
		}
		_, err = os.Stdout.Write(b)
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if len(os.Args) == 2 && os.Args[1] == "reconcile" {
		path := os.Getenv("GATEWAY_NATS_TOPOLOGY_FILE")
		if path == "" {
			path = "deploy/nats/streams.yaml"
		}
		topology, err := transport.LoadTopology(path)
		if err != nil {
			return err
		}
		n, err := transport.Connect(os.Getenv("GATEWAY_NATS_URL"), topology, transport.Auth{User: os.Getenv("GATEWAY_NATS_USER"), Password: os.Getenv("GATEWAY_NATS_PASSWORD"), InboxPrefix: "_INBOX.reconciler", CAFile: os.Getenv("GATEWAY_NATS_CA_FILE")})
		if err != nil {
			return err
		}
		defer n.Close()
		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if err = n.Reconcile(ctx); err != nil {
			return err
		}
		fmt.Println("NATS_RECONCILE=PASS")
		return nil
	}
	if len(os.Args) != 1 {
		return fmt.Errorf("unknown command")
	}
	c, err := bootstrap.LoadConfig()
	if err != nil {
		return err
	}
	initCtx, initCancel := context.WithTimeout(ctx, 20*time.Second)
	app, err := bootstrap.New(initCtx, c)
	initCancel()
	if err != nil {
		return err
	}
	defer app.Close()
	return app.Run(ctx)
}
