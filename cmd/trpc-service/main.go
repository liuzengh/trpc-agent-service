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
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"github.com/liuzengh/trpc-agent-service/trpcservice/reply"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
	"golang.org/x/sync/errgroup"
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
	roleFlag := flag.String("role", "", "service role: all, gateway, relay, worker, sender")
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
	roleName := *roleFlag
	if roleName == "" {
		roleName = os.Getenv("TRPC_AGENT_ROLE")
	}
	roles, err := config.ParseRole(roleName)
	if err != nil {
		return err
	}
	if roleName == "" {
		roleName = config.RoleAll
	}

	fmt.Printf("trpc-agent-service %s\n", trpcservice.Version)
	fmt.Printf("service role=%s\n", roleName)

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
	controlPlaneConfig, err := config.LoadControlPlaneConfigFromEnv()
	if err != nil {
		return fmt.Errorf("load control-plane config: %w", err)
	}
	queueConfig, err := config.LoadQueueConfigFromEnv()
	if err != nil {
		return fmt.Errorf("load queue config: %w", err)
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
	startupCtx, cancelStartup = context.WithTimeout(context.Background(), 10*time.Second)
	controlPlaneRepository, err := controlplane.New(startupCtx, controlPlaneConfig)
	cancelStartup()
	if err != nil {
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		_ = sessionService.Close()
		return fmt.Errorf("build control-plane repository: %w", err)
	}
	routeResolver, err := routing.NewControlPlaneResolver(controlPlaneRepository)
	if err != nil {
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		_ = sessionService.Close()
		return fmt.Errorf("build route resolver: %w", err)
	}
	inboundJournal, err := gateway.NewJournalForControlPlane(controlPlaneRepository)
	if err != nil {
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		_ = sessionService.Close()
		return fmt.Errorf("build inbound journal: %w", err)
	}
	gatewayIntake, err := gateway.NewIntake(routeResolver, inboundJournal)
	if err != nil {
		_ = inboundJournal.Close()
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		_ = sessionService.Close()
		return fmt.Errorf("build Gateway intake: %w", err)
	}
	revisionCompiler, err := agentservice.NewRevisionCompiler(
		controlPlaneRepository,
		selectedModel,
		modelConfig.Stream,
	)
	if err != nil {
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		_ = sessionService.Close()
		return fmt.Errorf("build Agent revision compiler: %w", err)
	}
	runtime, err := agentservice.NewRuntimeWithCompilerServices(
		selectedModel,
		revisionCompiler,
		sessionService,
		sessionCoordinator,
		idempotencyStore,
		modelConfig.Stream,
	)
	if err != nil {
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		_ = sessionService.Close()
		return fmt.Errorf("create agent runtime: %w", err)
	}
	startupCtx, cancelStartup = context.WithTimeout(context.Background(), 5*time.Second)
	agentQueue, err := workqueue.New(startupCtx, queueConfig)
	cancelStartup()
	if err != nil {
		_ = runtime.Close()
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		return fmt.Errorf("build Agent work queue: %w", err)
	}
	nodeID := queueConfig.Consumer
	if nodeID == "" {
		hostname, _ := os.Hostname()
		nodeID = fmt.Sprintf("%s-%d", hostname, os.Getpid())
	}
	outboxRelay, err := gateway.NewOutboxRelay(inboundJournal, agentQueue, gateway.RelayOptions{
		WorkerID:     "relay-" + nodeID,
		BatchSize:    100,
		ClaimLease:   30 * time.Second,
		PollInterval: 250 * time.Millisecond,
		RetryDelay:   time.Second,
	})
	if err != nil {
		_ = agentQueue.Close()
		_ = runtime.Close()
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		return fmt.Errorf("build queue outbox relay: %w", err)
	}
	agentWorker, err := worker.New(agentQueue, inboundJournal, runtime, worker.Options{
		WorkerID:    "worker-" + nodeID,
		MaxAttempts: 3,
		RetryDelay:  250 * time.Millisecond,
	})
	if err != nil {
		_ = agentQueue.Close()
		_ = runtime.Close()
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		return fmt.Errorf("build Agent Worker: %w", err)
	}
	channelRegistry, err := channels.NewRegistry(channels.NewTestAdapter())
	if err != nil {
		_ = agentQueue.Close()
		_ = runtime.Close()
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		return fmt.Errorf("build channel registry: %w", err)
	}
	replySender, err := reply.New(
		inboundJournal,
		controlPlaneRepository,
		channelRegistry,
		reply.Options{
			WorkerID:     "sender-" + nodeID,
			BatchSize:    100,
			ClaimLease:   30 * time.Second,
			PollInterval: 250 * time.Millisecond,
			RetryDelay:   time.Second,
			MaxAttempts:  5,
		},
	)
	if err != nil {
		_ = agentQueue.Close()
		_ = runtime.Close()
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		return fmt.Errorf("build Reply Sender: %w", err)
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
	fmt.Printf("control-plane backend=%s\n", controlPlaneConfig.Backend)
	fmt.Printf("queue backend=%s stream=%s group=%s\n", queueConfig.Backend, queueConfig.Stream, queueConfig.Group)
	if roles.Gateway {
		fmt.Printf("Gateway HTTP server listening on %s\n", listenAddr)
	}
	defer func() {
		if err := agentQueue.Close(); err != nil {
			log.Printf("close Agent work queue: %v", err)
		}
	}()
	defer func() {
		if err := gatewayIntake.Close(); err != nil {
			log.Printf("close Gateway intake: %v", err)
		}
	}()
	defer func() {
		if err := controlPlaneRepository.Close(); err != nil {
			log.Printf("close control-plane repository: %v", err)
		}
	}()
	defer func() {
		if err := runtime.Close(); err != nil {
			log.Printf("close agent runtime: %v", err)
		}
	}()

	handlerOptions := []web.Option{
		web.WithRouteResolver(routeResolver),
		web.WithGatewayIntake(gatewayIntake),
		web.WithReadinessCheck("control-plane", controlPlaneRepository.Ready),
		web.WithReadinessCheck("inbound-journal", gatewayIntake.Ready),
	}
	if roles.Relay || roles.Worker {
		handlerOptions = append(
			handlerOptions,
			web.WithReadinessCheck("work-queue", agentQueue.Ready),
		)
	}
	server := &http.Server{
		Addr:              listenAddr,
		Handler:           web.NewHandler(runtime, handlerOptions...),
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

	group, groupCtx := errgroup.WithContext(ctx)
	if roles.Gateway {
		group.Go(func() error {
			err := server.ListenAndServe()
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		})
	}
	if roles.Relay {
		group.Go(func() error { return ignoreCancellation(outboxRelay.Run(groupCtx)) })
	}
	if roles.Worker {
		group.Go(func() error { return ignoreCancellation(agentWorker.Run(groupCtx)) })
	}
	if roles.Sender {
		group.Go(func() error { return ignoreCancellation(replySender.Run(groupCtx)) })
	}

	<-groupCtx.Done()

	if roles.Gateway {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}
	}

	if err := group.Wait(); err != nil {
		return err
	}
	return nil
}

func ignoreCancellation(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
