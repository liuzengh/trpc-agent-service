package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admission"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/lark"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/liuzengh/trpc-agent-service/trpcservice/configpub"
	"github.com/liuzengh/trpc-agent-service/trpcservice/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/outbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/ratelimit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/s3object"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"github.com/redis/go-redis/v9"
)

type productionRuntime struct {
	telemetry           *telemetry.Runtime
	pool                *pgxpool.Pool
	objectStore         storage.ObjectStore
	artifactRepository  storage.ArtifactMetadataRepository
	readiness           *productionReadiness
	resolver            tenant.TenantResolver
	ingress             *gateway.Ingress
	worker              *worker.Worker
	dispatcher          *outbox.Dispatcher
	vector              *vectorComposition
	dispatcherStartHook func()
	beginDrainingHook   func()
	poolCloseOnce       sync.Once
	closeLimiterOnce    sync.Once
	closeLimiter        func()
	// admission is the P2-03 process-local ingress budget wired into the
	// webhook ingress; exposed for bounded capacity observation.
	admission *admission.Gate
	forceMu   sync.Mutex
	forceErr  error
}

func (r *productionRuntime) Start(ctx context.Context) error {
	if r == nil || r.worker == nil || r.dispatcher == nil || r.readiness == nil || ctx == nil {
		return errors.New("production runtime is not configured")
	}
	if r.objectStore != nil {
		probe, ok := r.objectStore.(storage.ObjectStoreReadiness)
		if !ok {
			return errors.New("object storage readiness is not configured")
		}
		probeCtx, cancel := context.WithTimeout(ctx, time.Second)
		err := probe.Ready(probeCtx)
		cancel()
		if err != nil {
			return errors.New("object storage is unavailable")
		}
	}
	if err := r.worker.Start(ctx); err != nil {
		return err
	}
	if r.vector != nil {
		if err := r.vector.start(ctx); err != nil {
			return err
		}
	}
	if r.dispatcherStartHook != nil {
		r.dispatcherStartHook()
	}
	go r.dispatcher.Run(ctx)
	r.readiness.setStarted()
	return nil
}

func (r *productionRuntime) Stop(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		return newLifecycleError("runtime", "invalid_context", errProcessRuntimeStop)
	}
	r.BeginDraining()
	var stopErr error
	if r.vector != nil {
		r.vector.stopWorker(ctx)
	}
	if r.worker != nil {
		if err := r.worker.Stop(ctx); err != nil && !errors.Is(err, worker.ErrNotStarted) {
			stopErr = errors.Join(stopErr, newLifecycleError("worker", "stop", errors.Join(errProcessWorkerDrain, err)))
		}
	}
	if r.dispatcher != nil {
		if err := r.dispatcher.Stop(ctx); err != nil && !errors.Is(err, outbox.ErrNotStarted) {
			stopErr = errors.Join(stopErr, newLifecycleError("dispatcher", "stop", errors.Join(errProcessDispatcherDrain, err)))
		}
	}
	if r.vector != nil {
		r.vector.close()
	}
	if r.telemetry != nil {
		// Bounded fresh contexts: the signal context may already be canceled
		// and must not abort the final export flush.
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
		r.telemetry.ForceFlush(flushCtx)
		flushCancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		_ = r.telemetry.Shutdown(shutdownCtx)
		shutdownCancel()
		r.telemetry.Logger().Event(context.Background(), 0, "telemetry_shutdown", "telemetry", "shutdown", "completed")
	}
	r.closePool()
	return stopErr
}

func (r *productionRuntime) BeginDraining() {
	if r == nil {
		return
	}
	if r.readiness != nil {
		r.readiness.setDraining()
	}
	if r.beginDrainingHook != nil {
		r.beginDrainingHook()
	}
}

// ForceClose is the bounded emergency owner used after the process deadline
// or a second signal. It closes the pool immediately and asks public Worker
// and Dispatcher Stop contracts to cancel their receive/send loops.
func (r *productionRuntime) ForceClose() {
	if r == nil {
		return
	}
	r.BeginDraining()
	r.closePool()
	ctx, cancel := context.WithTimeout(context.Background(), defaultForceCloseLimit)
	go func() {
		defer cancel()
		if r.worker != nil {
			if err := r.worker.Stop(ctx); err != nil && !errors.Is(err, worker.ErrNotStarted) {
				r.rememberForceError(err)
			}
		}
		if r.dispatcher != nil {
			if err := r.dispatcher.Stop(ctx); err != nil && !errors.Is(err, outbox.ErrNotStarted) {
				r.rememberForceError(err)
			}
		}
		if r.vector != nil {
			r.vector.stopWorker(ctx)
		}
	}()
}

func (r *productionRuntime) rememberForceError(err error) {
	if r == nil || err == nil {
		return
	}
	r.forceMu.Lock()
	r.forceErr = errors.Join(r.forceErr, err)
	r.forceMu.Unlock()
}

func (r *productionRuntime) closePool() {
	if r != nil && r.pool != nil {
		r.poolCloseOnce.Do(r.pool.Close)
	}
	if r != nil && r.closeLimiter != nil {
		// The P2-03 rate limit Redis client is a bounded, non-fact-source
		// auxiliary connection: it is closed together with the pool on both
		// the graceful and the forced path.
		r.closeLimiterOnce.Do(r.closeLimiter)
	}
}

type productionReadiness struct {
	migration   web.ReadinessGate
	pool        *pgxpool.Pool
	jobQueue    queue.JobQueue
	worker      *worker.Worker
	dispatcher  *outbox.Dispatcher
	objectProbe storage.ObjectStoreReadiness
	vectorProbe storage.ObjectStoreReadiness
	mu          sync.RWMutex
	started     bool
	draining    bool
}

func (r *productionReadiness) Ready(ctx context.Context) error {
	if ctx == nil {
		return errors.New("readiness context is required")
	}
	if r == nil || r.migration == nil || r.pool == nil || r.jobQueue == nil || r.worker == nil || r.dispatcher == nil {
		return errors.New("production dependencies are not configured")
	}
	if err := r.migration.Ready(ctx); err != nil {
		return errors.New("migration is not ready")
	}
	r.mu.RLock()
	started, draining := r.started, r.draining
	r.mu.RUnlock()
	if draining {
		return errors.New("ingress is draining")
	}
	if !started {
		return errors.New("async workers are not started")
	}
	pingCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := r.pool.Ping(pingCtx); err != nil {
		r.pool.Reset()
		return errors.New("database dependency is unavailable")
	}
	if r.vectorProbe != nil {
		vectorCtx, vectorCancel := context.WithTimeout(ctx, time.Second)
		err := r.vectorProbe.Ready(vectorCtx)
		vectorCancel()
		if err != nil {
			return errors.New("vector store dependency is unavailable")
		}
	}
	if r.objectProbe != nil {
		objectCtx, objectCancel := context.WithTimeout(ctx, time.Second)
		err := r.objectProbe.Ready(objectCtx)
		objectCancel()
		if err != nil {
			return errors.New("object storage dependency is unavailable")
		}
	}
	return nil
}

func (r *productionReadiness) setStarted() {
	r.mu.Lock()
	r.started = true
	r.mu.Unlock()
}

func (r *productionReadiness) setDraining() {
	r.mu.Lock()
	r.draining = true
	r.mu.Unlock()
}

type bootstrapRuntimeConfig struct {
	tenant          tenant.Tenant
	agent           tenant.AgentApp
	larkBinding     tenant.ChannelBinding
	telegramBind    tenant.ChannelBinding
	larkSender      lark.Binding
	telegramSend    telegram.Binding
	larkAppID       string
	larkVerify      string
	larkEncrypt     string
	telegramHook    string
	ownerID         string
	objectBackend   string
	objectConfig    s3object.Config
	larkEnabled     bool
	telegramEnabled bool
	secretResolver  tenant.SecretResolver
}

type bootstrapResolver struct {
	config   bootstrapRuntimeConfig
	bindings map[string]tenant.ChannelBinding
}

func newBootstrapResolver(config bootstrapRuntimeConfig) (*bootstrapResolver, error) {
	if err := config.tenant.Validate(); err != nil {
		return nil, fmt.Errorf("bootstrap tenant is invalid")
	}
	if err := config.agent.Validate(); err != nil {
		return nil, fmt.Errorf("bootstrap agent is invalid")
	}
	if config.agent.TenantID != config.tenant.ID || config.agent.ID != config.tenant.DefaultAgentID || config.agent.Version != config.tenant.ConfigVersion {
		return nil, errors.New("bootstrap agent does not match tenant")
	}
	bindings := make(map[string]tenant.ChannelBinding, 2)
	for _, binding := range []tenant.ChannelBinding{config.larkBinding, config.telegramBind} {
		if !binding.Enabled {
			continue
		}
		if err := binding.Validate(); err != nil || binding.TenantID != config.tenant.ID {
			return nil, errors.New("bootstrap binding is invalid")
		}
		key := binding.Channel + "|" + binding.ExternalAppID
		if _, exists := bindings[key]; exists {
			return nil, errors.New("bootstrap binding conflicts")
		}
		bindings[key] = binding
	}
	return &bootstrapResolver{config: config, bindings: bindings}, nil
}
func (r *bootstrapResolver) Resolve(ctx context.Context, req tenant.ResolveRequest) (tenant.TenantContext, error) {
	if err := ctx.Err(); err != nil {
		return tenant.TenantContext{}, err
	}
	binding, ok := r.bindings[req.Channel+"|"+req.ExternalAppID]
	if !ok || binding.TenantID != r.config.tenant.ID {
		return tenant.TenantContext{}, tenant.ErrBindingNotFound
	}
	tc := tenant.TenantContext{
		TenantID:         r.config.tenant.ID,
		AgentAppID:       r.config.agent.ID,
		BindingID:        binding.ID,
		Channel:          binding.Channel,
		ExternalUser:     req.ExternalUser,
		ExternalChat:     req.ExternalChat,
		ExternalThreadID: req.ExternalThreadID,
		InternalUser:     req.ExternalUser,
		RequestID:        req.RequestID,
		MessageID:        req.MessageID,
		TraceID:          req.TraceID,
		ConfigVersion:    r.config.tenant.ConfigVersion,
		BackendPolicy:    r.config.tenant.Backend,
	}
	if err := tc.Validate(); err != nil {
		return tenant.TenantContext{}, err
	}
	return tc, nil
}

func (r *bootstrapResolver) agentSpec(context.Context, tenant.TenantContext) (agent.AgentSpec, error) {
	return agent.AgentSpec{
		TenantID:       r.config.agent.TenantID,
		AgentAppID:     r.config.agent.ID,
		Version:        r.config.agent.Version,
		Name:           r.config.agent.Name,
		ModelProvider:  env("MODEL", "openai"),
		ModelConfigRef: r.config.agent.ModelConfigRef,
		SystemPrompt:   r.config.agent.SystemPrompt,
		ToolPolicyRef:  r.config.agent.ToolPolicyID,
		GuardrailRef:   r.config.agent.GuardrailRef,
	}, nil
}

type bootstrapAgentResolver struct{ resolver *bootstrapResolver }

func (r bootstrapAgentResolver) Resolve(ctx context.Context, _ tenant.TenantContext, _ queue.AgentRefDTO) (agent.AgentSpec, error) {
	if r.resolver == nil {
		return agent.AgentSpec{}, errors.New("bootstrap agent resolver is not configured")
	}
	return r.resolver.agentSpec(ctx, tenant.TenantContext{})
}

type productionAssemblyDependencies struct {
	agentFactory        agent.AgentFactory
	larkSender          channels.Sender
	telegramSender      channels.Sender
	dispatcherStartHook func()
	completionHook      func()
	outboxCompletedHook func()
	// VectorEmbedder/VectorStore/VectorLeases are explicit vector composition
	// seams. Production default is nil: enabled vector configuration without a
	// production Embedder fails closed instead of falling back to a fake.
	VectorEmbedder vector.EmbeddingProvider
	VectorStore    vector.VectorStore
	VectorLeases   storage.LeaseStore
	Telemetry      *telemetry.Runtime
	WebMiddleware  func(http.Handler) http.Handler
}

type productionCompletionObserver struct {
	delegate storage.AtomicCompletionCoordinator
	after    func()
}

func (o productionCompletionObserver) CommitResultAndAck(ctx context.Context, request storage.AtomicCompletionRequest) error {
	err := o.delegate.CommitResultAndAck(ctx, request)
	if err == nil && o.after != nil {
		o.after()
	}
	return err
}

type productionOutboxObserver struct {
	delegate storage.OutboxRepository
	after    func()
}

func (o productionOutboxObserver) Enqueue(ctx context.Context, tc tenant.TenantContext, message storage.OutboxMessage) error {
	return o.delegate.Enqueue(ctx, tc, message)
}

func (o productionOutboxObserver) ClaimBatch(ctx context.Context, tc tenant.TenantContext, owner string, limit int) ([]storage.OutboxMessage, error) {
	return o.delegate.ClaimBatch(ctx, tc, owner, limit)
}

func (o productionOutboxObserver) MarkCompleted(ctx context.Context, tc tenant.TenantContext, owner, id string) error {
	err := o.delegate.MarkCompleted(ctx, tc, owner, id)
	if err == nil && o.after != nil {
		o.after()
	}
	return err
}

func (o productionOutboxObserver) MarkRetry(ctx context.Context, tc tenant.TenantContext, owner, id string, next time.Time, lastError string) error {
	return o.delegate.MarkRetry(ctx, tc, owner, id, next, lastError)
}

func (o productionOutboxObserver) MoveToDLQ(ctx context.Context, tc tenant.TenantContext, owner, id, reason string) error {
	return o.delegate.MoveToDLQ(ctx, tc, owner, id, reason)
}

func assembleProduction(ctx context.Context, responder platformRuntimeResponder) (*productionRuntime, error) {
	return assembleProductionWithDependencies(ctx, responder, productionAssemblyDependencies{})
}

// assembleProductionWithDependencies is an internal test seam. It only
// replaces request execution and channel transport; all durable production
// components still come from the composition root below.
func assembleProductionWithDependencies(ctx context.Context, responder platformRuntimeResponder, dependencies productionAssemblyDependencies) (*productionRuntime, error) {
	telemetryRuntime, telemetryErr := assembleTelemetry(ctx, dependencies)
	if telemetryErr != nil {
		return nil, telemetryErr
	}
	config, err := loadBootstrapRuntimeConfig(ctx)
	if err != nil {
		return nil, err
	}
	objectStore, objectProbe, err := newProductionObjectStore(ctx, config)
	if err != nil {
		return nil, err
	}
	bootstrapResolver, err := newBootstrapResolver(config)
	if err != nil {
		return nil, err
	}
	migrator, err := pgstore.NewMigrator(ctx, pgstore.PostgresConfig{URL: mustEnv("DATABASE_URL"), SearchPath: os.Getenv("DATABASE_SCHEMA")}, os.DirFS(filepathCleanEnv("MIGRATIONS_DIR", "migrations")))
	if err != nil {
		return nil, errors.New("migration initialization failed")
	}
	migrationReady := pgstore.NewMigrationReadiness(migrator)
	if err := migrationReady.Initialize(ctx); err != nil {
		migrator.Close()
		return nil, errors.New("migration initialization failed")
	}
	migrator.Close()
	// P2-01: business traffic runs on a dedicated runtime role that is
	// neither superuser, nor BYPASSRLS, nor owner of any protected table, so
	// row level security actually constrains every pooled connection. The
	// owner credentials above are used only by the migration gate.
	runtimePool, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{
		URL:            mustEnv("DATABASE_RUNTIME_URL"),
		SearchPath:     os.Getenv("DATABASE_SCHEMA"),
		MaxConns:       16,
		MinConns:       2,
		ConnectTimeout: 2 * time.Second,
	})
	if err != nil {
		return nil, errors.New("database pool initialization failed")
	}
	pool := runtimePool
	if err := pgstore.EnsureRuntimeRoleLimits(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("runtime role limits: %w", err)
	}
	closeOnError := true
	// closeRateLimiter is assigned once the optional P2-03 rate limit
	// client is built further below; the deferred cleanup above closes it
	// together with the pool when assembly fails.
	var closeRateLimiter func()
	metadataRegistry, err := pgstore.NewTenantRegistry(pool)
	if err != nil {
		return nil, errors.New("tenant metadata repository initialization failed")
	}
	resolver := tenant.TenantResolver(tenant.RegistryResolver{Registry: metadataRegistry, SecretResolver: config.secretResolver})
	identityResolver := tenant.RepositoryIdentityResolver{Repository: metadataRegistry}
	// P1-08 additive, disabled by default: durable configuration publication
	// composition. When enabled it decorates tenant resolution with rollout
	// assignment and swaps agent resolution to immutable revision snapshots.
	configCompositionRuntime, configCompositionErr := newConfigComposition(pool, telemetryRuntime)
	if configCompositionErr != nil {
		pool.Close()
		return nil, configCompositionErr
	}
	if configCompositionRuntime != nil {
		resolver = assignmentResolver{inner: resolver, co: configCompositionRuntime.coordinator}
	}
	defer func() {
		if closeOnError {
			pool.Close()
			if closeRateLimiter != nil {
				closeRateLimiter()
			}
		}
	}()
	artifactRepository, err := pgstore.NewArtifactMetadataRepository(pool, objectStore)
	if err != nil {
		return nil, errors.New("artifact metadata initialization failed")
	}
	coordination, err := pgstore.NewCoordinationStore(pool)
	if err != nil {
		return nil, errors.New("coordination initialization failed")
	}
	jobQueue, err := queue.NewPostgresQueue(pool, queue.PostgresQueueConfig{})
	if err != nil {
		return nil, errors.New("job queue initialization failed")
	}
	resultRepository, err := pgstore.NewExecutionResultRepository(pool)
	if err != nil {
		return nil, errors.New("execution result repository initialization failed")
	}
	var completion storage.AtomicCompletionCoordinator
	completion, err = pgstore.NewAtomicCompletionCoordinator(pool)
	if err != nil {
		return nil, errors.New("atomic completion initialization failed")
	}
	if dependencies.completionHook != nil {
		completion = productionCompletionObserver{delegate: completion, after: dependencies.completionHook}
	}
	var oRepository storage.OutboxRepository
	oRepository, err = pgstore.NewOutboxRepository(pool, pgstore.OutboxRepositoryConfig{LockDuration: lifecycleDuration("OUTBOX_LOCK_DURATION", storage.DefaultOutboxLockDuration)})
	if err != nil {
		return nil, errors.New("outbox repository initialization failed")
	}
	if dependencies.outboxCompletedHook != nil {
		oRepository = productionOutboxObserver{delegate: oRepository, after: dependencies.outboxCompletedHook}
	}
	factory := agent.AgentFactory(nil)
	if responder != nil {
		factory = responder.Factory()
	}
	if dependencies.agentFactory != nil {
		factory = dependencies.agentFactory
	}
	if factory == nil {
		return nil, errors.New("production requires runner responder")
	}
	sink, err := execution.NewRepositorySink(resultRepository)
	if err != nil {
		return nil, errors.New("execution sink initialization failed")
	}
	executor, err := execution.NewExecutor(coordination, factory, sink)
	if err != nil {
		return nil, errors.New("executor initialization failed")
	}
	resolveAgent := worker.AgentResolver(bootstrapAgentResolver{resolver: bootstrapResolver})
	if configCompositionRuntime != nil {
		// P1-08 enabled: the worker resolves the agent spec from the job's
		// immutable config revision and never from the active configuration.
		resolveAgent = configSnapshotAgentResolver{
			inner: configpub.SnapshotAgentResolver{
				Co:            configCompositionRuntime.coordinator,
				Registry:      metadataRegistry,
				ModelProvider: env("MODEL", "openai"),
			},
			fallback: bootstrapResolver.agentSpec,
		}
	}
	workerConcurrency, workerConcurrencyErr := boundedIntEnv("WORKER_CONCURRENCY", 4, 1, 64)
	if workerConcurrencyErr != nil {
		return nil, workerConcurrencyErr
	}
	dispatcherConcurrency, dispatcherConcurrencyErr := boundedIntEnv("DISPATCHER_CONCURRENCY", 1, 1, 32)
	if dispatcherConcurrencyErr != nil {
		return nil, dispatcherConcurrencyErr
	}
	dispatcherBatch, dispatcherBatchErr := boundedIntEnv("DISPATCHER_CLAIM_BATCH_SIZE", int64(storage.DefaultOutboxBatchSize), 1, int64(outbox.MaxDispatcherBatchSize))
	if dispatcherBatchErr != nil {
		return nil, dispatcherBatchErr
	}
	admissionGate, admissionGateErr := admissionGateFromEnv()
	if admissionGateErr != nil {
		return nil, admissionGateErr
	}
	rateLimiter, closeRateLimiterFn, rateLimiterErr := rateLimiterFromEnv()
	if rateLimiterErr != nil {
		return nil, rateLimiterErr
	}
	closeRateLimiter = closeRateLimiterFn
	workerConfig := worker.Config{
		WorkerID:           config.ownerID,
		Concurrency:        workerConcurrency,
		VisibilityTimeout:  lifecycleDuration("WORKER_VISIBILITY_TIMEOUT", 2*time.Minute),
		LeaseTTL:           lifecycleDuration("WORKER_LEASE_TTL", 2*time.Minute),
		ShutdownTimeout:    lifecycleDuration("WORKER_SHUTDOWN_TIMEOUT", 10*time.Second),
		CleanupTimeout:     lifecycleDuration("WORKER_CLEANUP_TIMEOUT", 5*time.Second),
		LeaseRenewInterval: 0,
		RetryDelay:         lifecycleDuration("WORKER_RETRY_DELAY", 5*time.Second),
		ResolveAgent:       resolveAgent,
	}
	if dependencies.Telemetry != nil {
		workerConfig.Telemetry = dependencies.Telemetry
	}
	durableWorker, err := worker.NewWithAtomicCompletion(jobQueue, executor, workerConfig, completion)
	if err != nil {
		return nil, errors.New("worker initialization failed")
	}
	var larkSender channels.Sender
	if config.larkBinding.Enabled {
		if dependencies.larkSender != nil {
			larkSender = dependencies.larkSender
		} else {
			larkSender, err = newProductionLarkSender(config)
			if err != nil {
				return nil, err
			}
		}
	}
	var telegramSender channels.Sender
	if config.telegramBind.Enabled {
		if dependencies.telegramSender != nil {
			telegramSender = dependencies.telegramSender
		} else {
			telegramSender, err = newProductionTelegramSender(config)
			if err != nil {
				return nil, err
			}
		}
	}
	channelSender, err := outbox.NewChannelSender(channels.SenderFunc(func(sendCtx context.Context, message storage.OutboxMessage) channels.SenderOutcome {
		payload, err := channels.DecodeReplyOutboxMessage(message)
		if err != nil {
			return channels.SenderOutcome{Class: channels.OutcomePermanentFailure, Code: channels.SenderInvalidPayloadCode}
		}
		switch payload.Payload.Channel {
		case lark.Channel:
			if larkSender == nil {
				return channels.SenderOutcome{Class: channels.OutcomePermanentFailure, Code: channels.LarkSenderNotConfiguredCode}
			}
			return larkSender.Send(sendCtx, message)
		case telegram.Channel:
			if telegramSender == nil {
				return channels.SenderOutcome{Class: channels.OutcomePermanentFailure, Code: channels.TelegramSenderNotConfiguredCode}
			}
			return telegramSender.Send(sendCtx, message)
		default:
			return channels.SenderOutcome{Class: channels.OutcomePermanentFailure, Code: channels.SenderUnknownChannelCode}
		}
	}))
	if err != nil {
		return nil, errors.New("channel sender initialization failed")
	}
	dispatcher, err := outbox.NewDispatcher(oRepository, channelSender, outbox.Config{
		OwnerID: config.ownerID, Tenants: []tenant.TenantContext{baseTenantContext(config)},
		ClaimBatchSize: dispatcherBatch, Concurrency: dispatcherConcurrency,
		ClaimInterval: lifecycleDuration("DISPATCHER_CLAIM_INTERVAL", time.Second), ShutdownTimeout: lifecycleDuration("DISPATCHER_SHUTDOWN_TIMEOUT", 5*time.Second),
	})
	if err != nil {
		return nil, errors.New("dispatcher initialization failed")
	}
	adapters := make(map[string]gateway.WebhookAdapter, 2)
	if config.larkBinding.Enabled {
		larkWebhook, webhookErr := lark.NewWebhookAdapter(lark.WebhookConfig{Binding: config.larkSender, VerificationToken: config.larkVerify, EncryptKey: config.larkEncrypt})
		if webhookErr != nil {
			return nil, errors.New("Lark webhook initialization failed")
		}
		adapters[lark.Channel] = larkWebhook
	}
	if config.telegramBind.Enabled {
		telegramWebhook, webhookErr := telegram.NewWebhookAdapter(telegram.WebhookConfig{Binding: config.telegramSend, Secret: config.telegramHook})
		if webhookErr != nil {
			return nil, errors.New("Telegram webhook initialization failed")
		}
		adapters[telegram.Channel] = &telegramWebhookAdapter{inner: telegramWebhook}
	}
	asyncGateway, err := gateway.New(jobQueue)
	if err != nil {
		return nil, errors.New("gateway initialization failed")
	}
	if dependencies.Telemetry != nil {
		asyncGateway.WithTelemetry(dependencies.Telemetry)
	}
	ingressResolveAgent := bootstrapResolver.agentSpec
	if configCompositionRuntime != nil {
		// P1-08 enabled: the ingress resolves the agent spec from the assigned
		// immutable revision snapshot.
		snapshotResolver := configSnapshotAgentResolver{
			inner: configpub.SnapshotAgentResolver{
				Co:            configCompositionRuntime.coordinator,
				Registry:      metadataRegistry,
				ModelProvider: env("MODEL", "openai"),
			},
			fallback: bootstrapResolver.agentSpec,
		}
		ingressResolveAgent = snapshotResolver.ResolveForIngress
	}
	ingress, err := gateway.NewIngress(gateway.IngressConfig{Claims: coordination, Gateway: asyncGateway, Resolver: resolver, Identity: identityResolver, Audit: metadataRegistry, ResolveAgent: ingressResolveAgent, Adapters: adapters, OwnerID: config.ownerID, RateLimiter: rateLimiter, Admission: admissionGate, Telemetry: capacityTelemetryAdapter{metrics: telemetryRuntime.Metrics()}})
	if err != nil {
		return nil, errors.New("webhook ingress initialization failed")
	}
	vectorRuntime, vectorErr := assembleVectorComposition(ctx, vectorCompositionInput{pool: pool, ownerID: config.ownerID, secret: config.secretResolver, dependencies: &dependencies})
	if vectorErr != nil {
		return nil, vectorErr
	}
	var vectorProbe storage.ObjectStoreReadiness
	if vectorRuntime != nil {
		vectorProbe = vectorRuntime.store
	}
	runtime := &productionRuntime{
		telemetry: telemetryRuntime,
		pool:      pool, objectStore: objectStore, artifactRepository: artifactRepository, resolver: resolver, ingress: ingress, worker: durableWorker, dispatcher: dispatcher, vector: vectorRuntime,
		closeLimiter: closeRateLimiter, admission: admissionGate,
		dispatcherStartHook: dependencies.dispatcherStartHook,
		readiness: &productionReadiness{
			migration: migrationReady, pool: pool, jobQueue: jobQueue, worker: durableWorker, dispatcher: dispatcher, objectProbe: objectProbe, vectorProbe: vectorProbe,
		},
	}
	closeOnError = false
	return runtime, nil
}

type platformRuntimeResponder interface {
	Factory() agent.AgentFactory
}

// TelemetryWebMiddleware exposes the optional HTTP observability middleware
// for the serving layer. Nil when telemetry is disabled.
func (r *productionRuntime) TelemetryWebMiddleware() func(http.Handler) http.Handler {
	if r == nil || r.telemetry == nil {
		return nil
	}
	return r.telemetry.HTTPMiddleware
}

type runtimeResponderAdapter struct{ value platform.RuntimeResponder }

func (r runtimeResponderAdapter) Factory() agent.AgentFactory { return r.value.Factory }

// boundedIntEnv parses one strictly validated integer environment variable.
// Empty selects the safe default; any other value that is not an integer in
// [minimum, maximum] fails closed at startup instead of silently falling
// back, so a capacity misconfiguration can never weaken protection.
func boundedIntEnv(name string, fallback, minimum, maximum int64) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return int(fallback), nil
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer in [%d,%d]", name, minimum, maximum)
	}
	if parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("%s must be an integer in [%d,%d]", name, minimum, maximum)
	}
	return int(parsed), nil
}

// boundedDurationEnv parses one strictly validated duration environment
// variable with the same fail-closed semantics as boundedIntEnv.
func boundedDurationEnv(name string, fallback, minimum, maximum time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil || parsed < minimum || parsed > maximum {
		return 0, fmt.Errorf("%s must be a duration in [%s,%s]", name, minimum, maximum)
	}
	return parsed, nil
}

// capacityTelemetryAdapter forwards the bounded P2-03 admission outcomes to
// the low-cardinality telemetry registry. It never carries identifiers.
type capacityTelemetryAdapter struct{ metrics telemetry.Metrics }

func (a capacityTelemetryAdapter) IngressAdmission(outcome string) {
	if a.metrics != nil {
		a.metrics.IngressAdmission(outcome)
	}
}

// admissionGateFromEnv builds the process-local P2-03 admission budget.
// Defaults are always on; invalid relationships fail closed.
func admissionGateFromEnv() (*admission.Gate, error) {
	global, err := boundedIntEnv("INGRESS_MAX_INFLIGHT", 64, 1, admission.MaxSlots)
	if err != nil {
		return nil, err
	}
	perTenant, err := boundedIntEnv("INGRESS_MAX_INFLIGHT_PER_TENANT", 32, 1, admission.MaxSlots)
	if err != nil {
		return nil, err
	}
	perBinding, err := boundedIntEnv("INGRESS_MAX_INFLIGHT_PER_BINDING", 16, 1, admission.MaxSlots)
	if err != nil {
		return nil, err
	}
	return admission.New(global, perTenant, perBinding)
}

// rateLimiterFromEnv builds the optional server-owned three-dimensional
// Redis quota gate. An unset RATE_LIMIT_REDIS_URL keeps rate limiting
// disabled (documented composition choice); a set but invalid URL, window
// or limit fails closed at startup. The client uses bounded timeouts and a
// small pool: the limiter is auxiliary state, never a business fact source.
func rateLimiterFromEnv() (*ratelimit.RateLimiter, func(), error) {
	rawURL := strings.TrimSpace(os.Getenv("RATE_LIMIT_REDIS_URL"))
	if rawURL == "" {
		return nil, func() {}, nil
	}
	options, err := redis.ParseURL(rawURL)
	if err != nil {
		return nil, nil, fmt.Errorf("RATE_LIMIT_REDIS_URL is not a valid redis URL")
	}
	options.DialTimeout = time.Second
	options.ReadTimeout = time.Second
	options.WriteTimeout = time.Second
	options.PoolSize = 4
	window, err := boundedDurationEnv("RATE_LIMIT_WINDOW", time.Minute, time.Second, time.Hour)
	if err != nil {
		return nil, nil, err
	}
	tenantLimit, err := boundedIntEnv("RATE_LIMIT_TENANT_LIMIT", 600, 1, 1000000)
	if err != nil {
		return nil, nil, err
	}
	bindingLimit, err := boundedIntEnv("RATE_LIMIT_BINDING_LIMIT", 300, 1, 1000000)
	if err != nil {
		return nil, nil, err
	}
	chatLimit, err := boundedIntEnv("RATE_LIMIT_CHAT_LIMIT", 120, 1, 1000000)
	if err != nil {
		return nil, nil, err
	}
	client := redis.NewClient(options)
	limiter, err := ratelimit.New(client, "trpc-ingress", ratelimit.LimitPolicy{
		TenantLimit: int64(tenantLimit), BindingLimit: int64(bindingLimit), ChatLimit: int64(chatLimit), Window: window,
	})
	if err != nil {
		_ = client.Close()
		return nil, nil, fmt.Errorf("rate limiter configuration is invalid")
	}
	return limiter, func() { _ = client.Close() }, nil
}

func optionalBoolEnv(name string, fallback bool) (bool, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, errors.New(name + " is invalid")
	}
	return parsed, nil
}

func loadBootstrapRuntimeConfig(ctx context.Context) (bootstrapRuntimeConfig, error) {
	if ctx == nil {
		return bootstrapRuntimeConfig{}, errors.New("bootstrap configuration requires context")
	}
	version, err := strconv.ParseInt(requiredEnv("BOOTSTRAP_AGENT_VERSION"), 10, 64)
	if err != nil || version < 1 {
		return bootstrapRuntimeConfig{}, errors.New("BOOTSTRAP_AGENT_VERSION must be positive")
	}
	tenantID := requiredEnv("BOOTSTRAP_TENANT_ID")
	agentID := requiredEnv("BOOTSTRAP_AGENT_APP_ID")
	larkEnabled, err := optionalBoolEnv("BOOTSTRAP_LARK_ENABLED", false)
	if err != nil {
		return bootstrapRuntimeConfig{}, err
	}
	telegramEnabled, err := optionalBoolEnv("BOOTSTRAP_TELEGRAM_ENABLED", false)
	if err != nil {
		return bootstrapRuntimeConfig{}, err
	}
	larkBindingID := requiredEnv("BOOTSTRAP_LARK_BINDING_ID")
	telegramBindingID := requiredEnv("BOOTSTRAP_TELEGRAM_BINDING_ID")
	larkExternalAppID := requiredEnv("BOOTSTRAP_LARK_EXTERNAL_APP_ID")
	telegramExternalAppID := requiredEnv("BOOTSTRAP_TELEGRAM_EXTERNAL_APP_ID")
	larkAppID := requiredEnv("BOOTSTRAP_LARK_APP_ID")
	larkSecretRef := requiredEnv("BOOTSTRAP_LARK_APP_SECRET_REF")
	telegramTokenRef := requiredEnv("BOOTSTRAP_TELEGRAM_BOT_TOKEN_REF")
	telegramWebhookRef := requiredEnv("BOOTSTRAP_TELEGRAM_WEBHOOK_SECRET_REF")
	larkVerifyRef := requiredEnv("BOOTSTRAP_LARK_VERIFY_TOKEN_REF")
	larkEncryptRef := requiredEnv("BOOTSTRAP_LARK_ENCRYPT_KEY_REF")
	if larkEnabled && (larkAppID == "" || larkSecretRef == "" || larkVerifyRef == "" || larkEncryptRef == "") {
		return bootstrapRuntimeConfig{}, errors.New("Lark enabled configuration is incomplete")
	}
	if telegramEnabled && (telegramTokenRef == "" || telegramWebhookRef == "") {
		return bootstrapRuntimeConfig{}, errors.New("Telegram enabled configuration is incomplete")
	}
	secretResolver := tenant.EnvironmentSecretResolver{}
	var larkVerify, larkEncrypt, telegramHook string
	if larkEnabled {
		if _, err = secretResolver.Resolve(ctx, larkSecretRef); err != nil {
			return bootstrapRuntimeConfig{}, errors.New("Lark app secret is unavailable")
		}
		larkVerify, err = secretResolver.Resolve(ctx, larkVerifyRef)
		if err != nil {
			return bootstrapRuntimeConfig{}, errors.New("Lark verification token is unavailable")
		}
		larkEncrypt, err = secretResolver.Resolve(ctx, larkEncryptRef)
		if err != nil {
			return bootstrapRuntimeConfig{}, errors.New("Lark encryption key is unavailable")
		}
	}
	if telegramEnabled {
		if _, err = secretResolver.Resolve(ctx, telegramTokenRef); err != nil {
			return bootstrapRuntimeConfig{}, errors.New("Telegram bot token is unavailable")
		}
		telegramHook, err = secretResolver.Resolve(ctx, telegramWebhookRef)
		if err != nil {
			return bootstrapRuntimeConfig{}, errors.New("Telegram webhook secret is unavailable")
		}
	}
	objectBackend := strings.ToLower(strings.TrimSpace(env("BOOTSTRAP_OBJECT_BACKEND", "none")))
	objectConfig, err := loadProductionObjectConfigWithResolver(ctx, objectBackend, secretResolver)
	if err != nil {
		return bootstrapRuntimeConfig{}, err
	}
	larkSenderBinding := lark.Binding{TenantID: tenantID, BindingID: larkBindingID, Channel: lark.Channel, AppID: larkAppID, SecretRef: larkSecretRef, ReceiverIDType: requiredEnv("BOOTSTRAP_LARK_RECEIVER_ID_TYPE"), Enabled: larkEnabled}
	telegramSenderBinding := telegram.Binding{TenantID: tenantID, BindingID: telegramBindingID, Channel: telegram.Channel, BotTokenSecretRef: telegramTokenRef, WebhookSecretRef: telegramWebhookRef, Enabled: telegramEnabled}
	larkBinding := tenant.ChannelBinding{TenantID: tenantID, ID: larkBindingID, Channel: lark.Channel, ExternalAppID: larkExternalAppID, SecretRef: larkSecretRef, VerifyTokenRef: larkVerifyRef, Enabled: larkEnabled}
	telegramBinding := tenant.ChannelBinding{TenantID: tenantID, ID: telegramBindingID, Channel: telegram.Channel, ExternalAppID: telegramExternalAppID, SecretRef: telegramTokenRef, VerifyTokenRef: telegramWebhookRef, Enabled: telegramEnabled}
	return bootstrapRuntimeConfig{
		tenant:      tenant.Tenant{ID: tenantID, Name: requiredEnv("BOOTSTRAP_TENANT_NAME"), Status: tenant.StatusActive, ConfigVersion: version, DefaultAgentID: agentID, Backend: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: objectBackend}},
		agent:       tenant.AgentApp{TenantID: tenantID, ID: agentID, Name: requiredEnv("BOOTSTRAP_AGENT_NAME"), Version: version, Status: tenant.StatusActive, ModelConfigRef: env("MODEL_CONFIG_REF", "env"), SystemPrompt: os.Getenv("BOOTSTRAP_AGENT_SYSTEM_PROMPT"), ToolPolicyID: env("TOOL_POLICY_REF", "default"), GuardrailRef: os.Getenv("GUARDRAIL_REF")},
		larkBinding: larkBinding, telegramBind: telegramBinding,
		larkSender: larkSenderBinding, telegramSend: telegramSenderBinding, larkAppID: larkAppID, larkVerify: larkVerify, larkEncrypt: larkEncrypt, telegramHook: telegramHook, ownerID: requiredEnv("ASYNC_OWNER_ID"), objectBackend: objectBackend, objectConfig: objectConfig, secretResolver: secretResolver, larkEnabled: larkEnabled, telegramEnabled: telegramEnabled,
	}, nil
}

func loadProductionObjectConfig(ctx context.Context, backend string) (s3object.Config, error) {
	return loadProductionObjectConfigWithResolver(ctx, backend, tenant.EnvironmentSecretResolver{})
}

func loadProductionObjectConfigWithResolver(ctx context.Context, backend string, secrets tenant.SecretResolver) (s3object.Config, error) {
	if ctx == nil {
		return s3object.Config{}, errors.New("object storage configuration requires context")
	}
	if secrets == nil {
		return s3object.Config{}, errors.New("object storage secret resolver is not configured")
	}
	backend = strings.ToLower(strings.TrimSpace(backend))
	switch backend {
	case "", "none":
		return s3object.Config{}, nil
	case "s3":
		endpoint := requiredEnv("OBJECT_ENDPOINT")
		region := requiredEnv("OBJECT_REGION")
		bucket := requiredEnv("OBJECT_BUCKET")
		accessRef := requiredEnv("OBJECT_ACCESS_KEY_REF")
		secretRef := requiredEnv("OBJECT_SECRET_KEY_REF")
		if endpoint == "" || region == "" || bucket == "" || accessRef == "" || secretRef == "" {
			return s3object.Config{}, errors.New("object storage configuration is incomplete")
		}
		accessKey, err := secrets.Resolve(ctx, accessRef)
		if err != nil {
			return s3object.Config{}, errors.New("object access credential is unavailable")
		}
		secretKey, err := secrets.Resolve(ctx, secretRef)
		if err != nil {
			return s3object.Config{}, errors.New("object secret credential is unavailable")
		}
		cfg := s3object.Config{Endpoint: endpoint, Region: region, Bucket: bucket, AccessKey: accessKey, SecretKey: secretKey}
		if raw := strings.TrimSpace(os.Getenv("OBJECT_USE_PATH_STYLE")); raw != "" {
			cfg.UsePathStyle, err = strconv.ParseBool(raw)
			if err != nil {
				return s3object.Config{}, errors.New("OBJECT_USE_PATH_STYLE is invalid")
			}
		}
		if raw := strings.TrimSpace(os.Getenv("OBJECT_MAX_BYTES")); raw != "" {
			cfg.MaxObjectBytes, err = strconv.ParseInt(raw, 10, 64)
			if err != nil || cfg.MaxObjectBytes < 1 {
				return s3object.Config{}, errors.New("OBJECT_MAX_BYTES is invalid")
			}
		}
		if raw := strings.TrimSpace(os.Getenv("OBJECT_PRESIGN_MAX_TTL")); raw != "" {
			cfg.PresignMaxTTL, err = time.ParseDuration(raw)
			if err != nil || cfg.PresignMaxTTL <= 0 {
				return s3object.Config{}, errors.New("OBJECT_PRESIGN_MAX_TTL is invalid")
			}
		}
		return cfg, nil
	default:
		return s3object.Config{}, errors.New("BOOTSTRAP_OBJECT_BACKEND is unsupported")
	}
}

func newProductionObjectStore(ctx context.Context, config bootstrapRuntimeConfig) (storage.ObjectStore, storage.ObjectStoreReadiness, error) {
	if ctx == nil {
		return nil, nil, errors.New("object storage initialization requires context")
	}
	switch config.objectBackend {
	case "", "none":
		return nil, nil, nil
	case "s3":
		store, err := s3object.New(config.objectConfig)
		if err != nil {
			return nil, nil, errors.New("object storage initialization failed")
		}
		return store, store, nil
	default:
		return nil, nil, errors.New("object storage backend is unsupported")
	}
}

func newProductionLarkSender(config bootstrapRuntimeConfig) (*lark.Sender, error) {
	if config.secretResolver == nil {
		return nil, errors.New("Lark secret resolver is not configured")
	}
	client, err := productionChannelHTTPClient()
	if err != nil {
		return nil, err
	}
	tokens, err := lark.NewHTTPAccessTokenResolver(lark.TokenResolverConfig{Secrets: config.secretResolver, Client: client})
	if err != nil {
		return nil, errors.New("Lark token resolver initialization failed")
	}
	return lark.NewSender(lark.SenderConfig{Bindings: []lark.Binding{config.larkSender}, Tokens: tokens, Client: client})
}

func newProductionTelegramSender(config bootstrapRuntimeConfig) (*telegram.Sender, error) {
	if config.secretResolver == nil {
		return nil, errors.New("Telegram secret resolver is not configured")
	}
	client, err := productionChannelHTTPClient()
	if err != nil {
		return nil, err
	}
	return telegram.NewSender(telegram.SenderConfig{Bindings: []telegram.Binding{config.telegramSend}, Tokens: telegram.TokenResolverFunc(func(ctx context.Context, binding telegram.Binding) (string, error) {
		return config.secretResolver.Resolve(ctx, binding.BotTokenSecretRef)
	}), Client: client})
}

// productionChannelHTTPClient keeps the real channel constructors on the
// default path. G-D subprocess tests may explicitly route those constructors
// to a local deterministic server; missing or partial opt-in never falls back.
func productionChannelHTTPClient() (channels.HTTPDoer, error) {
	mode := strings.TrimSpace(os.Getenv("G_D_TEST_MODE"))
	fakeURL := strings.TrimSpace(os.Getenv("G_D_TEST_PROVIDER_URL"))
	if mode == "" && fakeURL == "" {
		return http.DefaultClient, nil
	}
	if mode != "1" || fakeURL == "" {
		return nil, errors.New("G-D provider test transport configuration is incomplete")
	}
	target, err := url.Parse(fakeURL)
	if err != nil || target.Scheme != "http" || target.Host == "" || target.User != nil {
		return nil, errors.New("G-D provider test transport URL is invalid")
	}
	return &http.Client{Transport: routedProviderTransport{target: target, base: http.DefaultTransport}}, nil
}

type routedProviderTransport struct {
	target *url.URL
	base   http.RoundTripper
}

func (t routedProviderTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if t.target == nil || t.base == nil {
		return nil, errors.New("provider test transport is not configured")
	}
	clone := request.Clone(request.Context())
	urlCopy := *clone.URL
	urlCopy.Scheme = t.target.Scheme
	urlCopy.Host = t.target.Host
	clone.URL = &urlCopy
	return t.base.RoundTrip(clone)
}

func baseTenantContext(config bootstrapRuntimeConfig) tenant.TenantContext {
	binding := config.larkBinding
	if !binding.Enabled {
		binding = config.telegramBind
	}
	if binding.ID == "" || binding.Channel == "" {
		return tenant.TenantContext{TenantID: config.tenant.ID, AgentAppID: config.agent.ID, BindingID: "disabled-bootstrap-binding", Channel: "disabled", RequestID: "bootstrap-request", MessageID: "bootstrap-message", TraceID: "bootstrap-trace", ConfigVersion: config.tenant.ConfigVersion, BackendPolicy: config.tenant.Backend}
	}
	return tenant.TenantContext{TenantID: config.tenant.ID, AgentAppID: config.agent.ID, BindingID: binding.ID, Channel: binding.Channel, RequestID: "bootstrap-request", MessageID: "bootstrap-message", TraceID: "bootstrap-trace", ConfigVersion: config.tenant.ConfigVersion, BackendPolicy: config.tenant.Backend}
}

type telegramWebhookAdapter struct{ inner *telegram.WebhookAdapter }

func (a *telegramWebhookAdapter) Verify(request *http.Request, body []byte) error {
	return a.inner.Verify(request, body)
}

func (a *telegramWebhookAdapter) Parse(body []byte) (channels.Incoming, error) {
	incoming, err := a.inner.Parse(body)
	if err != nil {
		return channels.Incoming{}, err
	}
	threadID := ""
	if incoming.MessageThreadID != nil {
		threadID = strconv.FormatInt(*incoming.MessageThreadID, 10)
	}
	return channels.Incoming{ID: strconv.FormatInt(incoming.UpdateID, 10), Channel: telegram.Channel, UserID: strconv.FormatInt(incoming.UserID, 10), ChatID: strconv.FormatInt(incoming.ChatID, 10), ThreadID: threadID, ChatType: incoming.ChatType, Text: incoming.Text}, nil
}

func resolveEnvSecret(ctx context.Context, ref string) (string, error) {
	return tenant.EnvironmentSecretResolver{}.Resolve(ctx, ref)
}

func requiredEnv(name string) string {
	return strings.TrimSpace(os.Getenv(name))
}

func mustEnv(name string) string { return requiredEnv(name) }

func filepathCleanEnv(name, fallback string) string {
	value := requiredEnv(name)
	if value == "" {
		return fallback
	}
	return value
}
