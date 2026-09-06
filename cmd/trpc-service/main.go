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
	adminservice "github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	agentservice "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/background"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/reply"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
	"golang.org/x/sync/errgroup"
	"trpc.group/trpc-go/trpc-agent-go/model"
	agentrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	sessionsummary "trpc.group/trpc-go/trpc-agent-go/session/summary"
)

func main() {
	log.SetOutput(platformlog.NewRedactingWriter(os.Stderr))
	if err := run(); err != nil {
		log.Printf("trpc-agent-service stopped: %v", err)
		os.Exit(1)
	}
}

func run() error {
	envFile := flag.String("env-file", ".env", "dotenv configuration file")
	addr := flag.String("addr", "", "HTTP listen address (overrides TRPC_AGENT_ADDR)")
	roleFlag := flag.String("role", "", "service role: all, gateway, relay, worker, sender, jobs, admin")
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

	modelConfig := config.ModelConfig{Provider: "disabled"}
	var selectedModel model.Model = agentservice.DisabledModel{}
	if roles.Worker || roles.Jobs {
		modelConfig, err = config.LoadModelConfigFromEnv()
		if err != nil {
			return fmt.Errorf("load model config: %w", err)
		}
		selectedModel, err = agentservice.BuildModel(modelConfig)
		if err != nil {
			return fmt.Errorf("build model: %w", err)
		}
	}
	var httpAPIConfig config.HTTPAPIConfig
	var wecomMCPTargets []config.WeComMCPTarget
	if roles.Gateway {
		wecomMCPTargets, err = config.LoadWeComMCPTargetsFromEnv()
		if err != nil {
			return err
		}
		httpAPIConfig, err = config.LoadHTTPAPIConfigFromEnv()
		if err != nil {
			return fmt.Errorf("load HTTP API config: %w", err)
		}
	}
	apiAccess, err := web.NewAPIAccess(httpAPIConfig)
	if err != nil {
		return err
	}
	secretGrants, err := config.LoadSecretGrantsFromEnv()
	if err != nil {
		return fmt.Errorf("load secret grants: %w", err)
	}
	secretStore, err := secret.NewEnvStore(roles.SecretGrants(secretGrants))
	if err != nil {
		return err
	}
	backends, err := config.LoadRuntimeBackends(roles, len(wecomMCPTargets) > 0)
	if err != nil {
		return fmt.Errorf("load role backend config: %w", err)
	}
	sessionConfig, coordinatorConfig, idempotencyConfig := backends.Session, backends.Coordinator, backends.Idempotency
	controlPlaneConfig, err := config.LoadControlPlaneConfigFromEnv()
	if err != nil {
		return fmt.Errorf("load control-plane config: %w", err)
	}
	queueConfig := backends.Queue
	var adminConfig config.AdminConfig
	if roles.Admin {
		adminConfig, err = config.LoadAdminConfigFromEnv()
		if err != nil {
			return fmt.Errorf("load Admin config: %w", err)
		}
	}
	quotaConfig := backends.Quota
	if roleName == config.RoleAdmin && !adminConfig.Enabled {
		return fmt.Errorf("Admin role requires TRPC_AGENT_ADMIN_ENABLED=true")
	}
	telemetryConfig, err := config.LoadTelemetryConfigFromEnv()
	if err != nil {
		return fmt.Errorf("load telemetry config: %w", err)
	}
	telemetryShutdown, err := platformtelemetry.Setup(context.Background(), telemetryConfig)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := telemetryShutdown(shutdownCtx); err != nil {
			log.Printf("shutdown telemetry: %v", err)
		}
	}()
	metricRecorder, err := platformmetrics.New()
	if err != nil {
		return fmt.Errorf("build platform metrics: %w", err)
	}
	sessionSummarizer := sessionsummary.NewSummarizer(
		selectedModel,
		sessionsummary.WithMaxSummaryWords(500),
	)
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), 5*time.Second)
	sessionService, err := platformstorage.NewSessionService(
		startupCtx,
		sessionConfig,
		platformstorage.WithSessionSummarizer(sessionSummarizer),
	)
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
	startupCtx, cancelStartup = context.WithTimeout(context.Background(), 5*time.Second)
	quotaGuard, err := tenant.NewGuard(startupCtx, controlPlaneRepository, quotaConfig)
	cancelStartup()
	if err != nil {
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		_ = sessionService.Close()
		return fmt.Errorf("build quota guard: %w", err)
	}
	auditWriter, err := audit.NewForControlPlane(controlPlaneRepository)
	if err != nil {
		_ = quotaGuard.Close()
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		_ = sessionService.Close()
		return fmt.Errorf("build audit writer: %w", err)
	}
	approvalRepository, err := approval.NewForControlPlane(controlPlaneRepository)
	if err != nil {
		_ = auditWriter.Close()
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		_ = sessionService.Close()
		return fmt.Errorf("build approval repository: %w", err)
	}
	backgroundJobs, err := background.NewForControlPlane(controlPlaneRepository)
	if err != nil {
		_ = approvalRepository.Close()
		_ = auditWriter.Close()
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		_ = sessionService.Close()
		return fmt.Errorf("build background job repository: %w", err)
	}
	toolExecutionJournal, err := toolexec.NewForControlPlane(controlPlaneRepository)
	if err != nil {
		_ = backgroundJobs.Close()
		_ = approvalRepository.Close()
		_ = auditWriter.Close()
		_ = quotaGuard.Close()
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		_ = sessionService.Close()
		return fmt.Errorf("build tool execution journal: %w", err)
	}
	sessionRouter, err := platformstorage.NewSessionRouter(
		controlPlaneRepository,
		secretStore,
		sessionService,
		sessionSummarizer,
	)
	if err != nil {
		_ = backgroundJobs.Close()
		_ = approvalRepository.Close()
		_ = auditWriter.Close()
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		_ = sessionService.Close()
		return fmt.Errorf("build session router: %w", err)
	}
	memoryRouter, err := platformstorage.NewMemoryRouter(controlPlaneRepository, secretStore)
	if err != nil {
		_ = sessionRouter.Close()
		_ = approvalRepository.Close()
		_ = auditWriter.Close()
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		return fmt.Errorf("build memory router: %w", err)
	}
	artifactRouter, err := platformstorage.NewArtifactRouter(controlPlaneRepository, secretStore)
	if err != nil {
		_ = sessionRouter.Close()
		_ = memoryRouter.Close()
		_ = approvalRepository.Close()
		_ = auditWriter.Close()
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		return fmt.Errorf("build artifact router: %w", err)
	}
	knowledgeRouter, err := platformstorage.NewKnowledgeRouter(controlPlaneRepository, secretStore)
	if err != nil {
		_ = sessionRouter.Close()
		_ = artifactRouter.Close()
		_ = memoryRouter.Close()
		_ = approvalRepository.Close()
		_ = auditWriter.Close()
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		return fmt.Errorf("build knowledge router: %w", err)
	}
	routeResolver, err := routing.NewControlPlaneResolver(controlPlaneRepository)
	if err != nil {
		_ = sessionRouter.Close()
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		return fmt.Errorf("build route resolver: %w", err)
	}
	inboundJournal, err := gateway.NewJournalForControlPlane(controlPlaneRepository)
	if err != nil {
		_ = sessionRouter.Close()
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		return fmt.Errorf("build inbound journal: %w", err)
	}
	gatewayIntake, err := gateway.NewIntake(
		routeResolver,
		inboundJournal,
		gateway.WithAuditWriter(auditWriter),
		gateway.WithMetrics(metricRecorder),
		gateway.WithQuotaGuard(quotaGuard),
	)
	if err != nil {
		_ = sessionRouter.Close()
		_ = inboundJournal.Close()
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		return fmt.Errorf("build Gateway intake: %w", err)
	}
	operations, err := toolexec.NewOperationsForControlPlane(controlPlaneRepository, toolExecutionJournal, auditWriter)
	if err != nil {
		return fmt.Errorf("build business operation service: %w", err)
	}
	toolCatalog := platformtool.DefaultCatalog(platformtool.NewWorkItemTool(operations))
	revisionCompiler, err := agentservice.NewRevisionCompiler(
		controlPlaneRepository,
		selectedModel,
		modelConfig.Stream,
		agentservice.WithToolCatalog(toolCatalog),
		agentservice.WithAuditWriter(auditWriter),
		agentservice.WithApprovalRepository(approvalRepository),
		agentservice.WithKnowledgeProvider(knowledgeRouter),
		agentservice.WithToolExecutionJournal(toolExecutionJournal),
		agentservice.WithSecretStore(secretStore),
	)
	if err != nil {
		_ = sessionRouter.Close()
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
		return fmt.Errorf("build Agent revision compiler: %w", err)
	}
	runtime, err := agentservice.NewRuntimeWithCompilerServices(
		selectedModel,
		revisionCompiler,
		sessionRouter,
		sessionCoordinator,
		idempotencyStore,
		modelConfig.Stream,
		agentrunner.WithMemoryService(memoryRouter),
		agentrunner.WithArtifactService(artifactRouter),
	)
	if err != nil {
		_ = sessionRouter.Close()
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		_ = idempotencyStore.Close()
		_ = sessionCoordinator.Close()
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
	backgroundProcessor, err := background.NewProcessor(
		backgroundJobs,
		controlPlaneRepository,
		sessionRouter,
		memoryRouter,
		knowledgeRouter,
		selectedModel,
		auditWriter,
		background.ProcessorOptions{
			WorkerID: "jobs-" + nodeID, ClaimLease: 2 * time.Minute,
			PollInterval: 500 * time.Millisecond, RetryDelay: time.Second,
		},
	)
	if err != nil {
		_ = agentQueue.Close()
		_ = runtime.Close()
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		return fmt.Errorf("build background job processor: %w", err)
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
		Audit:       auditWriter,
		Metrics:     metricRecorder,
		Approvals:   approvalRepository,
		ToolJournal: toolExecutionJournal,
		Jobs:        backgroundJobs,
		Quota:       quotaGuard,
	})
	if err != nil {
		_ = agentQueue.Close()
		_ = runtime.Close()
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		return fmt.Errorf("build Agent Worker: %w", err)
	}
	wecomAdapter, err := wecom.New(secretStore, nil)
	if err != nil {
		_ = agentQueue.Close()
		_ = runtime.Close()
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		return fmt.Errorf("build WeCom Adapter: %w", err)
	}
	telegramAdapter, err := telegram.New(secretStore, nil)
	if err != nil {
		_ = agentQueue.Close()
		_ = runtime.Close()
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		return fmt.Errorf("build Telegram Adapter: %w", err)
	}
	wecomMCPState, err := wecommcp.NewStore(controlPlaneRepository)
	if err != nil {
		return err
	}
	wecomMCPAdapter, err := wecommcp.New(secretStore, wecomMCPState, nil)
	if err != nil {
		return err
	}
	channelRegistry, err := channels.NewRegistry(
		channels.NewTestAdapter(),
		wecomAdapter,
		telegramAdapter,
		wecomMCPAdapter,
	)
	if err != nil {
		_ = agentQueue.Close()
		_ = runtime.Close()
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		return fmt.Errorf("build channel registry: %w", err)
	}
	approvalService, err := approval.NewService(
		approvalRepository,
		inboundJournal,
		auditWriter,
	)
	if err != nil {
		_ = agentQueue.Close()
		_ = runtime.Close()
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		return fmt.Errorf("build approval service: %w", err)
	}
	callbackGateway, err := gateway.NewCallbackGateway(
		controlPlaneRepository,
		channelRegistry,
		gatewayIntake,
		gateway.WithApprovalDecisionHandler(approvalService),
	)
	if err != nil {
		_ = agentQueue.Close()
		_ = runtime.Close()
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		return fmt.Errorf("build callback Gateway: %w", err)
	}
	pollStartup, cancelPollStartup := context.WithTimeout(context.Background(), 5*time.Second)
	pollCoordinator, err := coordination.New(pollStartup, backends.PollCoordinator)
	cancelPollStartup()
	if err != nil {
		return fmt.Errorf("build channel poll coordinator: %w", err)
	}
	defer func() { _ = pollCoordinator.Close() }()
	wecomPoller, err := gateway.NewWeComPoller(controlPlaneRepository, wecomMCPAdapter, callbackGateway, wecomMCPState, pollCoordinator, gateway.WeComPollOptions{
		Targets: wecomMCPTargets, Interval: 10 * time.Second, Window: time.Minute, Overlap: time.Minute, SettleDelay: 5 * time.Second, Timeout: 45 * time.Second, Audit: auditWriter, Metrics: metricRecorder,
	})
	if err != nil {
		return fmt.Errorf("build WeCom MCP receiver: %w", err)
	}
	var adminHandler http.Handler
	if adminConfig.Enabled {
		adminService, err := adminservice.New(controlPlaneRepository, toolCatalog)
		if err != nil {
			_ = agentQueue.Close()
			_ = runtime.Close()
			_ = gatewayIntake.Close()
			_ = controlPlaneRepository.Close()
			return fmt.Errorf("build Admin service: %w", err)
		}
		adminService.WithAuditWriter(auditWriter)
		adminService.WithKnowledgeRouter(knowledgeRouter)
		adminService.WithBackgroundJobs(backgroundJobs)
		adminService.WithToolOperations(operations, toolExecutionJournal)
		// Admin checks grants but cannot resolve model/IM values on an Admin-only node.
		grantAuthorizer, _ := secret.NewEnvStore(secretGrants)
		adminService.WithSecretAuthorizer(grantAuthorizer)
		principals := make([]adminservice.Principal, 0, len(adminConfig.Principals))
		for _, principal := range adminConfig.Principals {
			principals = append(principals, adminservice.Principal{
				Name: principal.Name, Token: principal.Token,
				Role: principal.Role, TenantIDs: principal.TenantIDs,
			})
		}
		adminHandler, err = adminservice.NewHandlerWithPrincipals(adminService, principals)
		if err != nil {
			_ = agentQueue.Close()
			_ = runtime.Close()
			_ = gatewayIntake.Close()
			_ = controlPlaneRepository.Close()
			return fmt.Errorf("build Admin handler: %w", err)
		}
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
			Audit:        auditWriter,
			Metrics:      metricRecorder,
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
	fmt.Printf("http test api enabled=%t synchronous_chat=%t\n",
		roles.Gateway && httpAPIConfig.Enabled,
		roles.Gateway && roles.Worker && httpAPIConfig.Enabled)
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
	fmt.Printf("wecom_mcp receiver bindings=%d (empty means disabled)\n", len(wecomMCPTargets))
	fmt.Printf("queue backend=%s stream=%s group=%s\n", queueConfig.Backend, queueConfig.Stream, queueConfig.Group)
	serverEnabled := roles.Gateway || roles.Admin
	if serverEnabled {
		fmt.Printf("Gateway HTTP server listening on %s\n", listenAddr)
	}
	defer func() {
		if err := toolExecutionJournal.Close(); err != nil {
			log.Printf("close tool execution journal: %v", err)
		}
	}()
	defer func() {
		if err := quotaGuard.Close(); err != nil {
			log.Printf("close quota guard: %v", err)
		}
	}()
	defer func() {
		if err := backgroundJobs.Close(); err != nil {
			log.Printf("close background jobs: %v", err)
		}
	}()
	defer func() {
		if err := knowledgeRouter.Close(); err != nil {
			log.Printf("close knowledge router: %v", err)
		}
	}()
	defer func() {
		if err := artifactRouter.Close(); err != nil {
			log.Printf("close artifact router: %v", err)
		}
	}()
	defer func() {
		if err := memoryRouter.Close(); err != nil {
			log.Printf("close memory router: %v", err)
		}
	}()
	defer func() {
		if err := approvalRepository.Close(); err != nil {
			log.Printf("close approval repository: %v", err)
		}
	}()
	defer func() {
		if err := auditWriter.Close(); err != nil {
			log.Printf("close audit writer: %v", err)
		}
	}()
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
		web.WithQuotaGuard(quotaGuard),
		web.WithReadinessCheck("control-plane", controlPlaneRepository.Ready),
		web.WithReadinessCheck("inbound-journal", gatewayIntake.Ready),
		web.WithReadinessCheck("audit", auditWriter.Ready),
		web.WithReadinessCheck("approval", approvalRepository.Ready),
		web.WithReadinessCheck("memory-router", memoryRouter.Ready),
		web.WithReadinessCheck("artifact-router", artifactRouter.Ready),
		web.WithReadinessCheck("knowledge-router", knowledgeRouter.Ready),
		web.WithReadinessCheck("background-jobs", backgroundJobs.Ready),
		web.WithReadinessCheck("session-router", sessionRouter.Ready),
		web.WithReadinessCheck("quota", quotaGuard.Ready),
		web.WithReadinessCheck("tool-execution", toolExecutionJournal.Ready),
		web.WithReadinessCheck("tool-operations", operations.Ready),
	}
	if roles.Gateway {
		handlerOptions = append(handlerOptions,
			web.WithAPIAccess(apiAccess), web.WithSynchronousChat(roles.Worker),
			web.WithGatewayIntake(gatewayIntake), web.WithCallbackGateway(callbackGateway))
		if len(wecomMCPTargets) > 0 {
			handlerOptions = append(handlerOptions, web.WithReadinessCheck("wecom-mcp-state", wecomMCPState.Ready))
		}
	}
	if adminHandler != nil {
		handlerOptions = append(handlerOptions, web.WithAdminHandler(adminHandler))
	}
	if roles.Relay || roles.Worker {
		handlerOptions = append(
			handlerOptions,
			web.WithReadinessCheck("work-queue", agentQueue.Ready),
		)
	}
	server := &http.Server{
		Addr: listenAddr,
		Handler: platformtelemetry.HTTPMiddleware(
			web.NewHandler(runtime, handlerOptions...),
		),
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
	if serverEnabled {
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
	if roles.Gateway && len(wecomMCPTargets) > 0 {
		group.Go(func() error { return ignoreCancellation(wecomPoller.Run(groupCtx)) })
	}
	if roles.Worker {
		group.Go(func() error { return ignoreCancellation(agentWorker.Run(groupCtx)) })
	}
	if roles.Sender {
		group.Go(func() error { return ignoreCancellation(replySender.Run(groupCtx)) })
	}
	if roles.Jobs {
		group.Go(func() error { return ignoreCancellation(backgroundProcessor.Run(groupCtx)) })
	}

	<-groupCtx.Done()

	if serverEnabled {
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
