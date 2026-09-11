package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	adminservice "github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	agentservice "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	"github.com/liuzengh/trpc-agent-service/trpcservice/attachments"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/backendregistry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/background"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/connections"
	"github.com/liuzengh/trpc-agent-service/trpcservice/console"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credentials"
	"github.com/liuzengh/trpc-agent-service/trpcservice/docsmcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/modelregistry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/reply"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformskill "github.com/liuzengh/trpc-agent-service/trpcservice/skill"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workqueue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workspace"
	"golang.org/x/sync/errgroup"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	agentrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
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
	var skillRegistry *platformskill.Registry
	var sandbox workspace.Executor
	if roles.Worker || roles.Admin {
		skillsConfig, configErr := config.LoadSkillsConfigFromEnv()
		if configErr != nil {
			return configErr
		}
		skillRegistry, err = platformskill.Load(skillsConfig.Root, skillsConfig.GrantsJSON)
		if err != nil {
			return fmt.Errorf("load deployment skills: %w", err)
		}
		if roles.Worker && skillsConfig.SandboxEnabled {
			sandbox, err = workspace.NewDocker(context.Background(), skillsConfig.Sandbox)
			if err != nil {
				return err
			}
		}
	}
	docsConfig, err := config.LoadDocsMCPConfigFromEnv(roles)
	if err != nil {
		return err
	}
	auditConfig, err := config.LoadAuditConfigFromEnv()
	if err != nil {
		return fmt.Errorf("load audit configuration: %w", err)
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
	envSecrets, err := secret.NewEnvStore(roles.SecretGrants(secretGrants))
	if err != nil {
		return err
	}
	var secretStore secret.Store = envSecrets
	managedConnectionsEnabled := os.Getenv("TRPC_AGENT_MODEL_MASTER_KEY") != ""
	backends, err := config.LoadRuntimeBackends(roles, len(wecomMCPTargets) > 0 || managedConnectionsEnabled)
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
		return fmt.Errorf("admin role requires TRPC_AGENT_ADMIN_ENABLED=true")
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
	sessionSummarizer := background.NewJobSummarizer()
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
	var credentialVault *credentials.Vault
	managedSkills := platformskill.NewStore(controlPlaneRepository)
	skillRegistry.WithManagedStore(managedSkills)
	if roles.Admin || roles.Gateway || roles.Sender || roles.Worker || roles.Jobs {
		credentialVault, err = credentials.New(controlPlaneRepository, os.Getenv("TRPC_AGENT_MODEL_MASTER_KEY"))
		if err != nil {
			return err
		}
		allowed := []string{}
		for _, purpose := range credentials.Purposes {
			if len(roles.SecretGrants([]secret.Grant{{Purpose: purpose}})) > 0 {
				allowed = append(allowed, purpose)
			}
		}
		secretStore = credentials.Routed{Vault: credentialVault, Fallback: envSecrets, Allowed: allowed}
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
	auditWriter, err := audit.NewForControlPlane(controlPlaneRepository, audit.Options{SpoolDirectory: auditConfig.SpoolDirectory, MaxBufferedRecords: auditConfig.MaxBufferedRecords})
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
	knowledgeRouter, err := platformstorage.NewKnowledgeRouter(controlPlaneRepository, secretStore, platformstorage.WithEmbeddingBudget(quotaGuard))
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
	startupCtx, cancelStartup = context.WithTimeout(context.Background(), 5*time.Second)
	err = inboundJournal.Ready(startupCtx)
	cancelStartup()
	if err != nil {
		return fmt.Errorf("check message lifecycle schema: %w", err)
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
	attachmentService, err := attachments.New(controlPlaneRepository, artifactRouter, secretStore, auditWriter)
	if err != nil {
		return fmt.Errorf("build attachments: %w", err)
	}
	consoleStore := console.NewStore(controlPlaneRepository)
	var modelConnections *modelregistry.Store
	if roles.Worker || roles.Jobs || roles.Admin {
		startupCtx, cancelStartup = context.WithTimeout(context.Background(), 5*time.Second)
		modelConnections, err = modelregistry.New(startupCtx, controlPlaneRepository, os.Getenv("TRPC_AGENT_MODEL_MASTER_KEY"), os.Getenv("TRPC_AGENT_MODEL_ALLOWED_ORIGINS"),
			modelregistry.WithEndpointPolicy(os.Getenv("TRPC_AGENT_MODEL_ENDPOINT_POLICY")),
			modelregistry.WithLoopbackAliases(os.Getenv("TRPC_AGENT_MODEL_HOST_ALIASES_JSON")))
		cancelStartup()
		if err != nil {
			return err
		}
	}
	if roles.Worker || adminConfig.Enabled {
		startupCtx, cancelStartup = context.WithTimeout(context.Background(), 5*time.Second)
		err = consoleStore.Ready(startupCtx)
		cancelStartup()
		if err != nil {
			return fmt.Errorf("check console schema and role access: %w", err)
		}
	}
	debugTools := &console.ToolJournal{Store: consoleStore}
	debugApprovals := &console.Approvals{Store: consoleStore}
	runtimeRepository := &console.RuntimeRepository{Repository: controlPlaneRepository, Store: consoleStore}
	runtimeJournal := console.RoutedJournal{Journal: toolExecutionJournal, Debug: debugTools}
	runtimeApprovals := console.RoutedApprovals{Repository: approvalRepository, Debug: debugApprovals}
	skillsService := &platformskill.Service{Registry: skillRegistry, Repository: runtimeRepository, Journal: runtimeJournal, Executor: sandbox}
	toolCatalog := platformtool.DefaultCatalog(platformtool.NewWorkItemTool(operations), attachmentService.ReadTool(), skillsService.RunTool())
	revisionCompiler, err := agentservice.NewRevisionCompiler(
		runtimeRepository,
		selectedModel,
		modelConfig.Stream,
		agentservice.WithToolCatalog(toolCatalog),
		agentservice.WithAuditWriter(auditWriter),
		agentservice.WithApprovalRepository(runtimeApprovals),
		agentservice.WithKnowledgeProvider(knowledgeRouter),
		agentservice.WithToolExecutionJournal(runtimeJournal),
		agentservice.WithSecretStore(secretStore),
		agentservice.WithModelBudget(quotaGuard),
		agentservice.WithModelConnections(modelConnections),
		agentservice.WithSkills(skillRegistry),
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
	debugEngine := &console.Engine{Store: consoleStore, Runtime: runtime, Approvals: debugApprovals, Tools: debugTools, Audit: auditWriter, Quota: quotaGuard, WorkerID: "console-" + console.Hash(nodeID + time.Now().UTC().Format(time.RFC3339Nano))[:24], ModelName: selectedModel.Info().Name, SandboxEnabled: sandbox != nil}
	debugEngine.Repository = controlPlaneRepository
	debugEngine.Probe = func(ctx context.Context) []console.Observation {
		observed := []console.Observation{}
		for _, item := range []struct {
			name  string
			check func(context.Context) error
		}{
			{"session", sessionRouter.Ready}, {"queue", agentQueue.Ready}, {"quota", quotaGuard.Ready},
		} {
			limited, cancel := context.WithTimeout(ctx, time.Second)
			err := item.check(limited)
			cancel()
			state := "ready"
			if err != nil {
				state = "unavailable"
			}
			observed = append(observed, console.Observation{Component: item.name, State: state, ObservedAt: time.Now().UTC()})
		}
		sandboxState := "unavailable"
		if sandbox != nil {
			sandboxState = "unknown"
			if probe, ok := sandbox.(interface{ Ready(context.Context) error }); ok {
				sandboxState = "unavailable"
				if probe.Ready(ctx) == nil {
					sandboxState = "ready"
				}
			}
		}
		observed = append(observed, console.Observation{Component: "sandbox", State: sandboxState, ObservedAt: time.Now().UTC()})
		for _, entry := range sessionRouter.ObserveInitialized(ctx) {
			observed = append(observed, console.Observation{Component: "session_binding", TenantID: entry.TenantID, BindingID: entry.BindingID, ConfigHash: entry.ConfigHash, State: entry.State, ObservedAt: entry.ObservedAt})
		}
		return observed
	}
	debugEngine.Cleanup = func(ctx context.Context, tenantID, appID, userID, sessionID string) error {
		scope := "t/" + tenantID + "/a/" + appID
		bindings, err := controlPlaneRepository.ListBackendBindings(ctx, tenantID, appID)
		if err != nil {
			return err
		}
		hasMemory := false
		for _, binding := range bindings {
			hasMemory = hasMemory || binding.ResourceType == "memory" && binding.MigrationState == "active"
		}
		if hasMemory {
			if err := memoryRouter.ClearMemories(ctx, memory.UserKey{AppName: scope, UserID: userID}); err != nil {
				return err
			}
		}
		return sessionRouter.DeleteSession(ctx, session.Key{AppName: scope, UserID: userID, SessionID: sessionID})
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
			ModelResolver: revisionCompiler.ModelForRevision,
			WorkerID:      "jobs-" + nodeID, ClaimLease: 2 * time.Minute,
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
		Authorize: func(ctx context.Context, task workqueue.AgentTask) error {
			return routeResolver.Revalidate(ctx, task.Scope)
		},
		Concurrency:       queueConfig.WorkerConcurrency,
		Attachments:       attachmentService,
		WorkerID:          "worker-" + nodeID,
		MaxAttempts:       3,
		RetryDelay:        250 * time.Millisecond,
		Audit:             auditWriter,
		Metrics:           metricRecorder,
		Approvals:         approvalRepository,
		ToolJournal:       toolExecutionJournal,
		Jobs:              backgroundJobs,
		Quota:             quotaGuard,
		ModelUsageManaged: true,
	})
	if err != nil {
		_ = agentQueue.Close()
		_ = runtime.Close()
		_ = gatewayIntake.Close()
		_ = controlPlaneRepository.Close()
		return fmt.Errorf("build Agent Worker: %w", err)
	}
	connectionStore := connections.New(controlPlaneRepository, credentialVault, auditWriter, os.Getenv("TRPC_AGENT_PUBLIC_BASE_URL"), nil)
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
	if connectionStore != nil {
		telegramAdapter.WithObserver(connectionStore.ObserveTelegram)
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
	var dynamicTargets func(context.Context) ([]config.WeComMCPTarget, error)
	if connectionStore != nil {
		dynamicTargets = connectionStore.Targets
	}
	wecomPoller, err := gateway.NewWeComPoller(controlPlaneRepository, wecomMCPAdapter, callbackGateway, wecomMCPState, pollCoordinator, gateway.WeComPollOptions{
		TargetProvider: dynamicTargets,
		Targets:        wecomMCPTargets, Interval: 5 * time.Second, Window: time.Minute, Overlap: time.Minute, SettleDelay: 2 * time.Second, Timeout: 45 * time.Second, Audit: auditWriter, Metrics: metricRecorder,
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
		adminService.WithSkills(skillRegistry)
		adminService.WithSkillUploads(managedSkills)
		adminService.WithConsoleStore(consoleStore)
		adminService.WithModelConnections(modelConnections)
		adminService.WithConnections(connectionStore)
		checks := map[string]func(context.Context) error{}
		if roles.Worker {
			checks["session"] = sessionRouter.Ready
			checks["queue"] = agentQueue.Ready
			checks["quota"] = quotaGuard.Ready
		}
		adminService.WithSystemInfo(roleName, os.Getenv("TRPC_AGENT_PUBLIC_BASE_URL"), checks)
		adminService.WithStartupModelName(selectedModel.Info().Name)
		adminService.WithDependencyObservations(func() []adminservice.DependencyCheck {
			// Configuration can prove that this Worker's sandbox is disabled.
			// A constructed executor does not prove live Docker/model readiness.
			if roles.Worker && sandbox == nil {
				return []adminservice.DependencyCheck{{Component: "sandbox", State: "unavailable", Source: "local_worker", ObservedAt: time.Now().UTC()}}
			}
			return nil
		})
		adminService.WithChannelState(wecomMCPState)
		adminService.WithOutboundParts(inboundJournal)
		if reader, ok := inboundJournal.(gateway.RunReader); ok {
			adminService.WithRunReader(reader)
		}
		adminService.WithKnowledgeRouter(knowledgeRouter)
		adminService.WithBackgroundJobs(backgroundJobs)
		adminService.WithToolOperations(operations, toolExecutionJournal)
		// Admin checks grants but cannot resolve model/IM values on an Admin-only node.
		grantAuthorizer, _ := secret.NewEnvStore(secretGrants)
		connectionAuthorizer := credentials.Routed{Vault: credentialVault, Fallback: grantAuthorizer, Allowed: credentials.Purposes}
		adminService.WithSecretAuthorizer(connectionAuthorizer)
		adminService.WithBackendConnections(backendregistry.New(controlPlaneRepository, credentialVault, connectionAuthorizer))
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
	fmt.Printf("wecom_mcp receiver configured_bindings=%d browser_connections=%t\n", len(wecomMCPTargets), connectionStore != nil)
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

	if roles.Gateway || roles.Admin {
		unregister, err := platformmetrics.RegisterBacklog(controlPlaneRepository)
		if err != nil {
			return fmt.Errorf("register backlog metrics: %w", err)
		}
		defer unregister()
	}
	var docsServer *http.Server
	var docsListener net.Listener
	var docsHandler *docsmcp.Handler
	if docsConfig.Enabled {
		index, err := docsmcp.LoadIndex(docsConfig.Root)
		if err != nil {
			return fmt.Errorf("build docs MCP index: %w", err)
		}
		docsHandler, err = docsmcp.NewHandler(index, docsConfig.Token)
		if err != nil {
			return err
		}
		docsListener, err = net.Listen("tcp", docsConfig.Addr)
		if err != nil {
			return fmt.Errorf("listen on docs MCP address: %w", err)
		}
		defer func() { _ = docsListener.Close() }()
		docsServer = &http.Server{Handler: docsHandler, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192}
		fmt.Printf("read-only project docs MCP listening on %s (loopback, authenticated)\n", docsConfig.Addr)
	}
	handlerOptions := []web.Option{
		web.WithRouteResolver(routeResolver),
		web.WithQuotaGuard(quotaGuard),
		web.WithManagedModelUsage(),
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
	if docsHandler != nil {
		handlerOptions = append(handlerOptions, web.WithReadinessCheck("docs-mcp", docsHandler.Ready))
	}
	if roles.Gateway {
		handlerOptions = append(handlerOptions,
			web.WithAPIAccess(apiAccess), web.WithSynchronousChat(roles.Worker),
			web.WithGatewayIntake(gatewayIntake), web.WithCallbackGateway(callbackGateway))
		if len(wecomMCPTargets) > 0 || connectionStore != nil {
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
	if docsServer != nil {
		docsServer.BaseContext = func(net.Listener) context.Context { return groupCtx }
		group.Go(func() error {
			err := docsServer.Serve(docsListener)
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		})
	}
	if maintenance, ok := auditWriter.(*audit.PolicyWriter); ok {
		group.Go(func() error { return ignoreCancellation(maintenance.RunMaintenance(groupCtx, roles.Jobs)) })
	}
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
	if roles.Gateway && (len(wecomMCPTargets) > 0 || connectionStore != nil) {
		group.Go(func() error { return ignoreCancellation(wecomPoller.Run(groupCtx)) })
	}
	if roles.Worker {
		group.Go(func() error { return ignoreCancellation(agentWorker.Run(groupCtx)) })
		group.Go(func() error { return ignoreCancellation(debugEngine.Run(groupCtx)) })
	}
	if roles.Sender {
		group.Go(func() error { return ignoreCancellation(replySender.Run(groupCtx)) })
	}
	if roles.Jobs {
		group.Go(func() error { return ignoreCancellation(backgroundProcessor.Run(groupCtx)) })
	}

	<-groupCtx.Done()

	var shutdownErr error
	servers := []*http.Server{docsServer}
	if serverEnabled {
		servers = append(servers, server)
	}
	for _, current := range servers {
		if current == nil {
			continue
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := current.Shutdown(shutdownCtx); err != nil {
			_ = current.Close()
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("shutdown HTTP server: %w", err))
		}
		cancel()
	}
	return errors.Join(shutdownErr, group.Wait())
}

func ignoreCancellation(err error) error {
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}
