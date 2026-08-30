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
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/lark"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/liuzengh/trpc-agent-service/trpcservice/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/outbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/platform"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

type productionRuntime struct {
	pool                *pgxpool.Pool
	readiness           *productionReadiness
	resolver            tenant.TenantResolver
	ingress             *gateway.Ingress
	worker              *worker.Worker
	dispatcher          *outbox.Dispatcher
	dispatcherStartHook func()
	beginDrainingHook   func()
	poolCloseOnce       sync.Once
	forceMu             sync.Mutex
	forceErr            error
}

func (r *productionRuntime) Start(ctx context.Context) error {
	if r == nil || r.worker == nil || r.dispatcher == nil || r.readiness == nil || ctx == nil {
		return errors.New("production runtime is not configured")
	}
	if err := r.worker.Start(ctx); err != nil {
		return err
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
}

type productionReadiness struct {
	migration  web.ReadinessGate
	pool       *pgxpool.Pool
	jobQueue   queue.JobQueue
	worker     *worker.Worker
	dispatcher *outbox.Dispatcher
	mu         sync.RWMutex
	started    bool
	draining   bool
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
	tenant       tenant.Tenant
	agent        tenant.AgentApp
	larkBinding  tenant.ChannelBinding
	telegramBind tenant.ChannelBinding
	larkSender   lark.Binding
	telegramSend telegram.Binding
	larkAppID    string
	larkVerify   string
	larkEncrypt  string
	telegramHook string
	ownerID      string
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
	for _, binding := range []tenant.ChannelBinding{config.larkBinding, config.telegramBind} {
		if err := binding.Validate(); err != nil || binding.TenantID != config.tenant.ID || !binding.Enabled {
			return nil, errors.New("bootstrap binding is invalid")
		}
	}
	return &bootstrapResolver{config: config, bindings: map[string]tenant.ChannelBinding{
		config.larkBinding.Channel + "|" + config.larkBinding.ExternalAppID:   config.larkBinding,
		config.telegramBind.Channel + "|" + config.telegramBind.ExternalAppID: config.telegramBind,
	}}, nil
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
	config, err := loadBootstrapRuntimeConfig(ctx)
	if err != nil {
		return nil, err
	}
	resolver, err := newBootstrapResolver(config)
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
	pool, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{
		URL:            mustEnv("DATABASE_URL"),
		SearchPath:     os.Getenv("DATABASE_SCHEMA"),
		MaxConns:       16,
		MinConns:       2,
		ConnectTimeout: 2 * time.Second,
	})
	if err != nil {
		return nil, errors.New("database pool initialization failed")
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			pool.Close()
		}
	}()
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
	workerConfig := worker.Config{
		WorkerID:           config.ownerID,
		Concurrency:        4,
		VisibilityTimeout:  lifecycleDuration("WORKER_VISIBILITY_TIMEOUT", 2*time.Minute),
		LeaseTTL:           lifecycleDuration("WORKER_LEASE_TTL", 2*time.Minute),
		ShutdownTimeout:    lifecycleDuration("WORKER_SHUTDOWN_TIMEOUT", 10*time.Second),
		CleanupTimeout:     lifecycleDuration("WORKER_CLEANUP_TIMEOUT", 5*time.Second),
		LeaseRenewInterval: 0,
		RetryDelay:         lifecycleDuration("WORKER_RETRY_DELAY", 5*time.Second),
		ResolveAgent:       bootstrapAgentResolver{resolver: resolver},
	}
	durableWorker, err := worker.NewWithAtomicCompletion(jobQueue, executor, workerConfig, completion)
	if err != nil {
		return nil, errors.New("worker initialization failed")
	}
	var larkSender channels.Sender
	if dependencies.larkSender != nil {
		larkSender = dependencies.larkSender
	} else {
		larkSender, err = newProductionLarkSender(config)
		if err != nil {
			return nil, err
		}
	}
	var telegramSender channels.Sender
	if dependencies.telegramSender != nil {
		telegramSender = dependencies.telegramSender
	} else {
		telegramSender, err = newProductionTelegramSender(config)
		if err != nil {
			return nil, err
		}
	}
	channelSender, err := outbox.NewChannelSender(channels.SenderFunc(func(sendCtx context.Context, message storage.OutboxMessage) channels.SenderOutcome {
		payload, err := channels.DecodeReplyOutboxMessage(message)
		if err != nil {
			return channels.SenderOutcome{Class: channels.OutcomePermanentFailure, Code: channels.SenderInvalidPayloadCode}
		}
		switch payload.Payload.Channel {
		case lark.Channel:
			return larkSender.Send(sendCtx, message)
		case telegram.Channel:
			return telegramSender.Send(sendCtx, message)
		default:
			return channels.SenderOutcome{Class: channels.OutcomePermanentFailure, Code: channels.SenderUnknownChannelCode}
		}
	}))
	if err != nil {
		return nil, errors.New("channel sender initialization failed")
	}
	dispatcher, err := outbox.NewDispatcher(oRepository, channelSender, outbox.Config{
		OwnerID:         config.ownerID,
		Tenants:         []tenant.TenantContext{baseTenantContext(config)},
		ClaimInterval:   lifecycleDuration("DISPATCHER_CLAIM_INTERVAL", time.Second),
		ShutdownTimeout: lifecycleDuration("DISPATCHER_SHUTDOWN_TIMEOUT", 5*time.Second),
	})
	if err != nil {
		return nil, errors.New("dispatcher initialization failed")
	}
	larkWebhook, err := lark.NewWebhookAdapter(lark.WebhookConfig{Binding: config.larkSender, VerificationToken: config.larkVerify, EncryptKey: config.larkEncrypt})
	if err != nil {
		return nil, errors.New("Lark webhook initialization failed")
	}
	telegramWebhook, err := telegram.NewWebhookAdapter(telegram.WebhookConfig{Binding: config.telegramSend, Secret: config.telegramHook})
	if err != nil {
		return nil, errors.New("Telegram webhook initialization failed")
	}
	asyncGateway, err := gateway.New(jobQueue)
	if err != nil {
		return nil, errors.New("gateway initialization failed")
	}
	ingress, err := gateway.NewIngress(gateway.IngressConfig{
		Resolver: resolver, Claims: coordination, Gateway: asyncGateway, ResolveAgent: resolver.agentSpec,
		Adapters: map[string]gateway.WebhookAdapter{lark.Channel: larkWebhook, telegram.Channel: &telegramWebhookAdapter{inner: telegramWebhook}},
		OwnerID:  config.ownerID,
	})
	if err != nil {
		return nil, errors.New("webhook ingress initialization failed")
	}
	runtime := &productionRuntime{
		pool: pool, resolver: resolver, ingress: ingress, worker: durableWorker, dispatcher: dispatcher,
		dispatcherStartHook: dependencies.dispatcherStartHook,
		readiness: &productionReadiness{
			migration: migrationReady, pool: pool, jobQueue: jobQueue, worker: durableWorker, dispatcher: dispatcher,
		},
	}
	closeOnError = false
	return runtime, nil
}

// platformRuntimeResponder keeps production assembly independent of the
// concrete responder's construction details while retaining the existing
// synchronous /api/chat responder.
type platformRuntimeResponder interface {
	Factory() agent.AgentFactory
}

type runtimeResponderAdapter struct{ value platform.RuntimeResponder }

func (r runtimeResponderAdapter) Factory() agent.AgentFactory { return r.value.Factory }

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
	larkAppID := requiredEnv("BOOTSTRAP_LARK_APP_ID")
	larkBindingID := requiredEnv("BOOTSTRAP_LARK_BINDING_ID")
	telegramBindingID := requiredEnv("BOOTSTRAP_TELEGRAM_BINDING_ID")
	larkExternalAppID := requiredEnv("BOOTSTRAP_LARK_EXTERNAL_APP_ID")
	telegramExternalAppID := requiredEnv("BOOTSTRAP_TELEGRAM_EXTERNAL_APP_ID")
	larkSecretRef := requiredEnv("BOOTSTRAP_LARK_APP_SECRET_REF")
	telegramTokenRef := requiredEnv("BOOTSTRAP_TELEGRAM_BOT_TOKEN_REF")
	telegramWebhookRef := requiredEnv("BOOTSTRAP_TELEGRAM_WEBHOOK_SECRET_REF")
	larkVerify, err := resolveEnvSecret(ctx, requiredEnv("BOOTSTRAP_LARK_VERIFY_TOKEN_REF"))
	if err != nil {
		return bootstrapRuntimeConfig{}, errors.New("Lark verification token is unavailable")
	}
	larkEncrypt, err := resolveEnvSecret(ctx, requiredEnv("BOOTSTRAP_LARK_ENCRYPT_KEY_REF"))
	if err != nil {
		return bootstrapRuntimeConfig{}, errors.New("Lark encryption key is unavailable")
	}
	telegramHook, err := resolveEnvSecret(ctx, telegramWebhookRef)
	if err != nil {
		return bootstrapRuntimeConfig{}, errors.New("Telegram webhook secret is unavailable")
	}
	larkSenderBinding := lark.Binding{TenantID: tenantID, BindingID: larkBindingID, Channel: lark.Channel, AppID: larkAppID, SecretRef: larkSecretRef, ReceiverIDType: requiredEnv("BOOTSTRAP_LARK_RECEIVER_ID_TYPE"), Enabled: true}
	telegramSenderBinding := telegram.Binding{TenantID: tenantID, BindingID: telegramBindingID, Channel: telegram.Channel, BotTokenSecretRef: telegramTokenRef, WebhookSecretRef: telegramWebhookRef, Enabled: true}
	return bootstrapRuntimeConfig{
		tenant:       tenant.Tenant{ID: tenantID, Name: requiredEnv("BOOTSTRAP_TENANT_NAME"), Status: tenant.StatusActive, ConfigVersion: version, DefaultAgentID: agentID, Backend: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "none"}},
		agent:        tenant.AgentApp{TenantID: tenantID, ID: agentID, Name: requiredEnv("BOOTSTRAP_AGENT_NAME"), Version: version, Status: tenant.StatusActive, ModelConfigRef: env("MODEL_CONFIG_REF", "env"), SystemPrompt: os.Getenv("BOOTSTRAP_AGENT_SYSTEM_PROMPT"), ToolPolicyID: env("TOOL_POLICY_REF", "default"), GuardrailRef: os.Getenv("GUARDRAIL_REF")},
		larkBinding:  tenant.ChannelBinding{TenantID: tenantID, ID: larkBindingID, Channel: lark.Channel, ExternalAppID: larkExternalAppID, SecretRef: larkSecretRef, VerifyTokenRef: requiredEnv("BOOTSTRAP_LARK_VERIFY_TOKEN_REF"), Enabled: true},
		telegramBind: tenant.ChannelBinding{TenantID: tenantID, ID: telegramBindingID, Channel: telegram.Channel, ExternalAppID: telegramExternalAppID, SecretRef: telegramTokenRef, VerifyTokenRef: telegramWebhookRef, Enabled: true},
		larkSender:   larkSenderBinding, telegramSend: telegramSenderBinding, larkAppID: larkAppID, larkVerify: larkVerify, larkEncrypt: larkEncrypt, telegramHook: telegramHook, ownerID: requiredEnv("ASYNC_OWNER_ID"),
	}, nil
}

func newProductionLarkSender(config bootstrapRuntimeConfig) (*lark.Sender, error) {
	client, err := productionChannelHTTPClient()
	if err != nil {
		return nil, err
	}
	resolver := lark.SecretResolverFunc(resolveEnvSecret)
	tokens, err := lark.NewHTTPAccessTokenResolver(lark.TokenResolverConfig{Secrets: resolver, Client: client})
	if err != nil {
		return nil, errors.New("Lark token resolver initialization failed")
	}
	return lark.NewSender(lark.SenderConfig{Bindings: []lark.Binding{config.larkSender}, Tokens: tokens, Client: client})
}

func newProductionTelegramSender(config bootstrapRuntimeConfig) (*telegram.Sender, error) {
	client, err := productionChannelHTTPClient()
	if err != nil {
		return nil, err
	}
	return telegram.NewSender(telegram.SenderConfig{Bindings: []telegram.Binding{config.telegramSend}, Tokens: telegram.TokenResolverFunc(func(ctx context.Context, binding telegram.Binding) (string, error) {
		return resolveEnvSecret(ctx, binding.BotTokenSecretRef)
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
	return tenant.TenantContext{TenantID: config.tenant.ID, AgentAppID: config.agent.ID, BindingID: config.larkBinding.ID, Channel: lark.Channel, RequestID: "bootstrap-request", MessageID: "bootstrap-message", TraceID: "bootstrap-trace", ConfigVersion: config.tenant.ConfigVersion, BackendPolicy: config.tenant.Backend}
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
	return channels.Incoming{ID: strconv.FormatInt(incoming.UpdateID, 10), Channel: telegram.Channel, UserID: strconv.FormatInt(incoming.UserID, 10), ChatID: strconv.FormatInt(incoming.ChatID, 10), ThreadID: threadID, Text: incoming.Text}, nil
}

func resolveEnvSecret(ctx context.Context, ref string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if !strings.HasPrefix(ref, "env://") || len(ref) <= len("env://") {
		return "", errors.New("secret reference is not an environment reference")
	}
	name := strings.TrimPrefix(ref, "env://")
	if strings.ContainsAny(name, "\r\n\x00") {
		return "", errors.New("secret reference is invalid")
	}
	value := os.Getenv(name)
	if value == "" {
		return "", errors.New("secret is unavailable")
	}
	return value, nil
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
