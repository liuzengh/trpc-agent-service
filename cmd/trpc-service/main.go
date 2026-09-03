package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	agentservice "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

func main() {
	if err := run(); err != nil {
		log.Printf("trpc-agent-service stopped: %v", err)
		os.Exit(1)
	}
}

func run() error {
	envFile := flag.String("env-file", ".env", "dotenv configuration file")
	addr := flag.String("addr", "", "HTTP listen address (overrides TRPC_AGENT_ADDR)")
	flag.Parse()

	loaded, err := config.LoadDotEnv(*envFile)
	if err != nil {
		return err
	}
	if !loaded && *envFile != ".env" && *envFile != "" {
		return fmt.Errorf("dotenv file %q does not exist", *envFile)
	}

	listenAddr := *addr
	if listenAddr == "" {
		listenAddr = os.Getenv("TRPC_AGENT_ADDR")
	}
	if listenAddr == "" {
		listenAddr = ":8080"
	}

	fmt.Printf("trpc-agent-service %s\n", trpcservice.Version)

	modelConfig, err := config.LoadModelConfigFromEnv()
	if err != nil {
		return fmt.Errorf("load model config: %w", err)
	}
	selectedModel, err := agentservice.BuildModel(modelConfig)
	if err != nil {
		return fmt.Errorf("build model: %w", err)
	}
	sessionConfig, err := config.LoadSessionConfigFromEnv()
	if err != nil {
		return fmt.Errorf("load session config: %w", err)
	}
	coordinatorConfig, err := config.LoadCoordinatorConfigFromEnv()
	if err != nil {
		return fmt.Errorf("load coordinator config: %w", err)
	}
	idempotencyConfig, err := config.LoadIdempotencyConfigFromEnv()
	if err != nil {
		return fmt.Errorf("load idempotency config: %w", err)
	}
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), 5*time.Second)
	sessionService, err := platformstorage.NewSessionService(startupCtx, sessionConfig)
	cancelStartup()
	if err != nil {
		return fmt.Errorf("build session service: %w", err)
	}
	startupCtx, cancelStartup = context.WithTimeout(context.Background(), 5*time.Second)
	sessionCoordinator, err := coordination.New(startupCtx, coordinatorConfig)
	cancelStartup()
	if err != nil {
		_ = sessionService.Close()
		return fmt.Errorf("build session coordinator: %w", err)
	}
	startupCtx, cancelStartup = context.WithTimeout(context.Background(), 5*time.Second)
	idempotencyStore, err := idempotency.New(startupCtx, idempotencyConfig)
	cancelStartup()
	if err != nil {
		_ = sessionCoordinator.Close()
		_ = sessionService.Close()
		return fmt.Errorf("build idempotency store: %w", err)
	}
	runtime, err := agentservice.NewRuntimeWithServices(
		selectedModel,
		sessionService,
		sessionCoordinator,
		idempotencyStore,
		modelConfig.Stream,
	)
	if err != nil {
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		_ = sessionService.Close()
		return fmt.Errorf("create agent runtime: %w", err)
	}
	fmt.Printf(
		"model provider=%s name=%s stream=%t\n",
		modelConfig.Provider,
		selectedModel.Info().Name,
		modelConfig.Stream,
	)
	fmt.Printf(
		"session backend=%s ttl=%s\n",
		sessionConfig.Backend,
		sessionConfig.TTL,
	)
	fmt.Printf(
		"coordinator backend=%s lease_ttl=%s renew_interval=%s\n",
		coordinatorConfig.Backend,
		coordinatorConfig.LeaseTTL,
		coordinatorConfig.RenewInterval,
	)
	fmt.Printf(
		"idempotency backend=%s processing_ttl=%s completed_ttl=%s\n",
		idempotencyConfig.Backend,
		idempotencyConfig.ProcessingTTL,
		idempotencyConfig.CompletedTTL,
	)
	fmt.Printf("tutorial chat server listening on %s\n", listenAddr)
	defer func() {
		if err := runtime.Close(); err != nil {
			log.Printf("close agent runtime: %v", err)
		}
	}()

	server := &http.Server{
		Addr:              listenAddr,
		Handler:           web.NewHandler(runtime),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown HTTP server: %w", err)
	}

	serveErr := <-errCh
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return nil
}
