package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/DocJlm/trpc-agent-service/trpcservice"
	"github.com/DocJlm/trpc-agent-service/trpcservice/agent"
	"github.com/DocJlm/trpc-agent-service/trpcservice/config"
	"github.com/DocJlm/trpc-agent-service/trpcservice/platform"
	"github.com/DocJlm/trpc-agent-service/trpcservice/queue"
	"github.com/DocJlm/trpc-agent-service/trpcservice/store"
	"github.com/DocJlm/trpc-agent-service/trpcservice/telemetry"
)

func main() {
	if err := run(); err != nil {
		log.Printf("error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	command := "serve"
	args := os.Args[1:]
	if len(args) > 0 && args[0] != "-h" && args[0] != "--help" && args[0][0] != '-' {
		command, args = args[0], args[1:]
	}
	switch command {
	case "serve":
		return serve(args)
	case "migrate":
		return migrate(args)
	case "channel-smoke":
		return channelSmoke(args)
	case "version":
		fmt.Println(trpcservice.Version)
		return nil
	case "help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", command)
	}
}

func serve(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	configPath := flags.String("config", "configs/demo.yaml", "YAML configuration path")
	roleValue := flags.String("role", "all", "all, gateway, worker, or admin")
	fakeModel := flags.Bool("fake-model", false, "use deterministic echo model; never calls DeepSeek")
	if err := flags.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	role, err := platform.ParseRole(*roleValue)
	if err != nil {
		return err
	}
	options := platform.Options{}
	if *fakeModel {
		options.Engine = agent.EchoEngine{}
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	shutdownTelemetry, err := telemetry.Init(ctx, "trpc-agent-service-"+string(role))
	if err != nil {
		return fmt.Errorf("initialize telemetry: %w", err)
	}
	defer shutdownTelemetry(context.Background())
	app, err := platform.New(ctx, cfg, options)
	if err != nil {
		return err
	}
	defer app.Close()
	log.Printf("trpc-agent-service %s role=%s admin=http://%s", trpcservice.Version, role, cfg.HTTPAddr)
	return app.Run(ctx, role)
}

func migrate(args []string) error {
	flags := flag.NewFlagSet("migrate", flag.ContinueOnError)
	configPath := flags.String("config", "configs/demo.yaml", "YAML configuration path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	if cfg.Database.PostgresDSN == "" {
		return errors.New("postgres_dsn is required for migrate")
	}
	repository, err := store.NewPostgresRepository(context.Background(), cfg.Database.PostgresDSN)
	if err != nil {
		return err
	}
	defer repository.Close()
	if err := repository.Migrate(context.Background()); err != nil {
		return err
	}
	if err := repository.SeedTenants(context.Background(), cfg.Tenants); err != nil {
		return err
	}
	log.Printf("database migrated and %d tenants seeded", len(cfg.Tenants))
	return nil
}

func channelSmoke(args []string) error {
	flags := flag.NewFlagSet("channel-smoke", flag.ContinueOnError)
	configPath := flags.String("config", "configs/demo.yaml", "YAML configuration path")
	channel := flags.String("channel", "", "wecom or feishu")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *channel != "wecom" && *channel != "feishu" {
		return errors.New("--channel must be wecom or feishu")
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}
	found := false
	for tenantIndex := range cfg.Tenants {
		for bindingIndex := range cfg.Tenants[tenantIndex].Channels {
			binding := &cfg.Tenants[tenantIndex].Channels[bindingIndex]
			binding.Enabled = binding.Type == *channel
			found = found || binding.Enabled
		}
	}
	if !found {
		return fmt.Errorf("no %s binding exists in config", *channel)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	shutdownTelemetry, err := telemetry.Init(ctx, "trpc-agent-service-smoke-"+*channel)
	if err != nil {
		return fmt.Errorf("initialize telemetry: %w", err)
	}
	defer shutdownTelemetry(context.Background())
	memoryQueue := queue.NewMemoryQueue(64)
	app, err := platform.New(ctx, cfg, platform.Options{
		Repository: store.NewMemoryRepository(), Queue: memoryQueue,
		Locker: queue.NewMemoryLocker(), Engine: agent.EchoEngine{},
	})
	if err != nil {
		return err
	}
	defer app.Close()
	log.Printf("%s smoke client starting; send a text message, press Ctrl+C after echo", *channel)
	return app.Run(ctx, platform.RoleAll)
}

func usage() {
	fmt.Fprintf(os.Stderr, `trpc-agent-service %s

Usage:
  trpc-service serve [--config path] [--role all|gateway|worker|admin] [--fake-model]
  trpc-service migrate [--config path]
  trpc-service channel-smoke --channel wecom|feishu [--config path]
  trpc-service version
`, trpcservice.Version)
}
