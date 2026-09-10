package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/liuzengh/trpc-agent-service/migrations"
	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/assembly"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/knowledgeingest"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/node"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
	sessionsummary "trpc.group/trpc-go/trpc-agent-go/session/summary"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type infrastructure struct {
	channelConnectors        *channelConnectorManager
	producer                 messaging.Producer
	executionManifests       *messaging.ExecutionManifestCodec
	worker                   *messaging.Worker
	replyOutbox              *messaging.ChannelOutboxDispatcher
	configInvalidationOutbox *tenant.ConfigInvalidationDispatcher
	stateStore               storage.StateStore
	executionDedup           storage.ExecutionDedupStore
	webIdempotency           storage.IdempotencyStore
	retryTracker             storage.RetryTracker
	toolExecutions           tool.ExecutionLister
	knowledgeIngestQueue     storage.KnowledgeIngestQueue
	knowledgeIngestWorker    *knowledgeingest.Worker
	knowledgeSourcePolicy    *knowledgeingest.SourceFactory
	knowledgeMigrationStore  storage.KnowledgeMigrationStore
	knowledgeMigrator        web.KnowledgeMigrationManager
	backendProfiles          storage.BackendProfileStore
	runners                  *assembly.Factory
	repository               tenant.Repository
	platformStores           *storage.PlatformStoreFactory
	applicationValidator     *config.PlatformPolicyValidator
	modelCatalog             *config.ModelCatalog
	modelProvider            *assembly.ManagedModelProvider
	secretResolver           credential.SecretResolver
	probes                   map[string]web.DependencyProbe
	authHandler              http.Handler
	sessions                 identity.SessionStore
	authAudits               identity.AuditRecorder
	identities               identity.IdentityStore
	webReplyHub              *web.RedisReplyHub
	agentMemory              web.AgentMemoryProvider
	agentSessions            web.AgentSessionProvider
	sessionMigrationStore    storage.SessionMigrationStore
	sessionMigrator          web.SessionMigrationManager
	toolCatalog              []web.ToolInfo
	observer                 *metrics.OTelObserver
	metricsHandler           http.Handler
	nodes                    node.Store
	nodeLifecycle            *node.Lifecycle
	closeFuncs               []func()
}

func (i *infrastructure) Close() {
	if i == nil {
		return
	}
	for index := len(i.closeFuncs) - 1; index >= 0; index-- {
		i.closeFuncs[index]()
	}
}

// infrastructureBuilder incrementally assembles the components required by a
// service role. Each compose method fills a coherent slice of the runtime and
// returns an error; composeInfrastructure wires them together and owns the
// cleanup rollback when any step fails.
type infrastructureBuilder struct {
	ctx           context.Context
	serviceConfig config.Config
	getenv        environment
	role          serviceRole
	cleanups      []func()

	databaseURL string

	// Kafka 入口配置。由 composeGatewayAndChannels 读取并校验一次，
	// composeWorkerAndNode 直接复用，避免重复读取与隐式依赖前序副作用。
	brokers []string
	topic   string

	// Storage layer.
	database                *sql.DB
	redisClient             *redis.Client
	auditHMACKey            []byte
	executionManifests      *messaging.ExecutionManifestCodec
	repository              tenant.Repository
	postgresStateStore      *storage.PostgresStateStore
	postgresExecutionDedup  *storage.PostgresExecutionDedupStore
	postgresRetryTracker    *storage.PostgresRetryTracker
	stateStore              *storage.ObservedStateStore
	executionDedup          *storage.ObservedExecutionDedupStore
	retryTracker            *storage.ObservedRetryTracker
	webIdempotency          *storage.ObservedIdempotencyStore
	toolExecutionLedger     *tool.PostgresExecutionLedger
	knowledgeIngestQueue    *storage.PostgresKnowledgeIngestQueue
	knowledgeMigrationStore *storage.PostgresKnowledgeMigrationStore
	backendProfiles         *storage.PostgresBackendProfileStore
	configurationCache      *tenant.RedisCache
	webReplyHub             *web.RedisReplyHub
	approvalBroker          *governance.RedisApprovalBroker
	imProgressHub           *messaging.RedisIMProgressHub

	// Provider layer.
	observer              *metrics.OTelObserver
	metricsHandler        http.Handler
	environmentSecrets    *credential.EnvironmentSecretResolver
	secretResolver        credential.SecretResolver
	modelCatalog          *config.ModelCatalog
	modelProvider         *assembly.ManagedModelProvider
	sessionProvider       *assembly.ManagedSessionProvider
	sessionMigrator       *assembly.SessionMigrator
	memoryProvider        *assembly.ManagedMemoryProvider
	storeFactory          *storage.PlatformStoreFactory
	knowledgeSourcePolicy *knowledgeingest.SourceFactory
	knowledgeMigrator     *knowledgeingest.Migrator
	sessionMigrationStore *storage.PostgresSessionMigrationStore

	// Tool registry shared by validation, assembly and the console catalog.
	toolRegistry *assembly.ToolRegistry
	toolNames    []string

	// Role-specific components.
	channelConnectors        *channelConnectorManager
	producer                 messaging.Producer
	worker                   *messaging.Worker
	replyOutbox              *messaging.ChannelOutboxDispatcher
	configInvalidationOutbox *tenant.ConfigInvalidationDispatcher
	runners                  *assembly.Factory
	applicationValidator     *config.PlatformPolicyValidator
	authHandler              http.Handler
	loginSessions            identity.SessionStore
	identityStore            *identity.PostgresIdentityStore
	kafkaProbe               web.DependencyProbe
	knowledgeIngestWorker    *knowledgeingest.Worker
	toolCatalog              []web.ToolInfo
	nodeStore                node.Store
	nodeLifecycle            *node.Lifecycle
}

func (b *infrastructureBuilder) rollback() {
	for i := len(b.cleanups) - 1; i >= 0; i-- {
		b.cleanups[i]()
	}
}

// composeStorage opens PostgreSQL and Redis and builds the tenant repository
// together with the shared persistence stores.
func (b *infrastructureBuilder) composeStorage() error {
	databaseURL, err := required(b.getenv, "DATABASE_URL")
	if err != nil {
		return err
	}
	b.databaseURL = databaseURL
	b.database, err = sql.Open("pgx", databaseURL)
	if err != nil {
		return fmt.Errorf("open PostgreSQL: %w", err)
	}
	b.cleanups = append(b.cleanups, func() { _ = b.database.Close() })
	b.database.SetMaxOpenConns(envInt(b.getenv, "DATABASE_MAX_OPEN_CONNS", 100))
	b.database.SetMaxIdleConns(envInt(b.getenv, "DATABASE_MAX_IDLE_CONNS", 25))
	b.database.SetConnMaxLifetime(5 * time.Minute)
	if err := b.database.PingContext(b.ctx); err != nil {
		return fmt.Errorf("ping PostgreSQL: %w", err)
	}
	if envBool(b.getenv, "AUTO_MIGRATE") {
		if err := migrations.Apply(b.ctx, b.database); err != nil {
			return fmt.Errorf("apply database migrations: %w", err)
		}
	}
	repositorySource, err := tenant.NewPostgresRepository(b.database)
	if err != nil {
		return err
	}
	auditHMACKey, err := required(b.getenv, "AUDIT_HMAC_KEY")
	if err != nil {
		return err
	}
	b.auditHMACKey = []byte(auditHMACKey)
	b.executionManifests, err = messaging.NewExecutionManifestCodec("platform-v1", b.auditHMACKey, 7*24*time.Hour)
	if err != nil {
		return fmt.Errorf("construct execution manifest codec: %w", err)
	}
	b.postgresStateStore, err = storage.NewPostgresStateStore(b.database, b.auditHMACKey)
	if err != nil {
		return err
	}
	b.postgresExecutionDedup, err = storage.NewPostgresExecutionDedupStore(b.database)
	if err != nil {
		return err
	}
	b.postgresRetryTracker, err = storage.NewPostgresRetryTracker(b.database)
	if err != nil {
		return err
	}
	b.toolExecutionLedger, err = tool.NewPostgresExecutionLedger(b.database, b.auditHMACKey)
	if err != nil {
		return fmt.Errorf("construct tool execution ledger: %w", err)
	}
	b.knowledgeIngestQueue, err = storage.NewPostgresKnowledgeIngestQueue(b.database)
	if err != nil {
		return err
	}
	b.knowledgeMigrationStore, err = storage.NewPostgresKnowledgeMigrationStore(b.database)
	if err != nil {
		return err
	}
	b.backendProfiles, err = storage.NewPostgresBackendProfileStore(b.database)
	if err != nil {
		return fmt.Errorf("construct backend profile store: %w", err)
	}

	redisAddress, err := required(b.getenv, "REDIS_ADDR")
	if err != nil {
		return err
	}
	b.redisClient = redis.NewClient(&redis.Options{Addr: redisAddress, Password: b.getenv("REDIS_PASSWORD")})
	b.cleanups = append(b.cleanups, func() { _ = b.redisClient.Close() })
	if err := b.redisClient.Ping(b.ctx).Err(); err != nil {
		return fmt.Errorf("ping Redis: %w", err)
	}
	b.configurationCache, err = tenant.NewRedisCache(b.redisClient)
	if err != nil {
		return err
	}
	b.repository, err = tenant.NewCachedRepository(repositorySource, b.configurationCache, time.Minute)
	if err != nil {
		return err
	}
	b.webReplyHub, err = web.NewRedisReplyHub(b.redisClient)
	if err != nil {
		return err
	}
	b.approvalBroker, err = governance.NewRedisApprovalBroker(b.redisClient, 10*time.Minute)
	if err != nil {
		return fmt.Errorf("construct approval broker: %w", err)
	}
	b.imProgressHub, err = messaging.NewRedisIMProgressHub(b.redisClient)
	if err != nil {
		return fmt.Errorf("construct IM progress hub: %w", err)
	}
	return nil
}

// composeProviders assembles telemetry, secret/model providers and the
// Session / Memory / Knowledge stacks that wrap the framework backends.
func (b *infrastructureBuilder) composeProviders() error {
	telemetryRuntime, err := composeTelemetry(b.ctx, b.getenv)
	if err != nil {
		return err
	}
	b.cleanups = append(b.cleanups, telemetryRuntime.Close)
	b.observer = telemetryRuntime.Observer
	b.metricsHandler = telemetryRuntime.PrometheusHandler
	if err := storage.ObserveFrameworkPostgres(b.observer); err != nil {
		return fmt.Errorf("observe framework PostgreSQL clients: %w", err)
	}
	b.stateStore, err = storage.NewObservedStateStore(b.postgresStateStore, b.observer, "postgres")
	if err != nil {
		return fmt.Errorf("construct observed state store: %w", err)
	}
	b.executionDedup, err = storage.NewObservedExecutionDedupStore(b.postgresExecutionDedup, b.observer, "postgres")
	if err != nil {
		return fmt.Errorf("construct observed execution dedup store: %w", err)
	}
	b.retryTracker, err = storage.NewObservedRetryTracker(b.postgresRetryTracker, b.observer, "postgres")
	if err != nil {
		return fmt.Errorf("construct observed retry tracker: %w", err)
	}
	idempotency, err := storage.NewRedisIdempotencyStore(b.redisClient)
	if err != nil {
		return err
	}
	b.webIdempotency, err = storage.NewObservedIdempotencyStore(idempotency, b.observer, "redis")
	if err != nil {
		return fmt.Errorf("construct observed idempotency store: %w", err)
	}

	b.environmentSecrets, err = credential.NewEnvironmentSecretResolver(b.getenv)
	if err != nil {
		return fmt.Errorf("construct secret resolver: %w", err)
	}
	b.secretResolver, err = credential.NewAllowlistSecretResolver(b.environmentSecrets, b.serviceConfig.Service.AllowedSecretRefs)
	if err != nil {
		return fmt.Errorf("construct managed secret resolver: %w", err)
	}
	b.modelCatalog, err = config.NewModelCatalog(b.serviceConfig.ModelProviders)
	if err != nil {
		return fmt.Errorf("construct model catalog: %w", err)
	}
	b.modelProvider, err = assembly.NewManagedModelProvider(b.secretResolver, b.modelCatalog)
	if err != nil {
		return fmt.Errorf("construct model provider: %w", err)
	}
	dynamicSummarizer := sessionsummary.NewDynamicSummarizer(func(summaryCtx context.Context, sess *agentsession.Session) (sessionsummary.SessionSummarizer, error) {
		tenantID, appCode, ok := strings.Cut(sess.AppName, "/")
		if !ok || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(appCode) == "" {
			return nil, fmt.Errorf("invalid session application name %q", sess.AppName)
		}
		snapshot, err := b.repository.GetActive(summaryCtx, tenantID, appCode)
		if err != nil {
			return nil, fmt.Errorf("resolve summary application: %w", err)
		}
		summaryModel, err := b.modelProvider.Model(summaryCtx, snapshot.Config)
		if err != nil {
			return nil, fmt.Errorf("resolve summary model: %w", err)
		}
		return sessionsummary.NewSummarizer(
			summaryModel,
			sessionsummary.WithEventThreshold(6),
			sessionsummary.WithMaxSummaryWords(120),
		), nil
	})
	b.sessionMigrationStore, err = storage.NewPostgresSessionMigrationStore(b.database)
	if err != nil {
		return fmt.Errorf("construct Session migration store: %w", err)
	}
	b.sessionProvider, err = assembly.NewManagedSessionProvider(b.databaseURL, b.secretResolver, b.backendProfiles, dynamicSummarizer, b.sessionMigrationStore)
	if err != nil {
		return fmt.Errorf("construct Session provider: %w", err)
	}
	b.cleanups = append(b.cleanups, func() { _ = b.sessionProvider.Close() })
	b.sessionMigrator, err = assembly.NewSessionMigrator(b.sessionMigrationStore, b.repository, b.sessionProvider, b.stateStore, b.stateStore)
	if err != nil {
		return fmt.Errorf("construct Session migrator: %w", err)
	}
	httpClient := &http.Client{Timeout: b.serviceConfig.Service.RequestTimeout.Duration}
	b.memoryProvider, err = assembly.NewManagedMemoryProvider(b.databaseURL, b.secretResolver, b.backendProfiles, httpClient)
	if err != nil {
		return fmt.Errorf("construct Memory provider: %w", err)
	}
	b.cleanups = append(b.cleanups, func() { _ = b.memoryProvider.Close() })
	b.storeFactory, err = storage.NewPlatformStoreFactory(
		b.database, b.databaseURL, b.serviceConfig.Service.Knowledge, b.serviceConfig.ModelProviders, b.backendProfiles, b.secretResolver, httpClient,
	)
	if err != nil {
		return fmt.Errorf("construct platform store factory: %w", err)
	}
	extractTimeout := b.serviceConfig.Service.DocumentExtractTimeout.Duration
	if extractTimeout <= 0 {
		extractTimeout = 5 * time.Minute
	}
	b.knowledgeSourcePolicy, err = knowledgeingest.NewSourceFactory(
		b.serviceConfig.Service.Knowledge, b.serviceConfig.Service.DoclingEndpoint, extractTimeout,
	)
	if err != nil {
		return fmt.Errorf("construct knowledge source factory: %w", err)
	}
	b.knowledgeMigrator, err = knowledgeingest.NewMigrator(b.knowledgeMigrationStore, b.repository, b.knowledgeIngestQueue, b.storeFactory, b.backendProfiles)
	if err != nil {
		return fmt.Errorf("construct knowledge migrator: %w", err)
	}
	return nil
}

// composeGatewayAndChannels builds the shared tool registry and, depending on
// the role, the gateway identity/validator stack and the IM channel
// connectors.
func (b *infrastructureBuilder) composeGatewayAndChannels() error {
	brokers, err := csvRequired(b.getenv, "KAFKA_BROKERS")
	if err != nil {
		return err
	}
	topic, err := required(b.getenv, "KAFKA_TOPIC")
	if err != nil {
		return err
	}
	b.brokers = brokers
	b.topic = topic

	// Tenant-selectable tools have one source of truth. The same registry feeds
	// runtime assembly, configuration validation, and the console catalog.
	b.toolRegistry, err = assembly.NewToolRegistry(
		map[string]agenttool.CallableTool{},
		assembly.WithToolSecretResolver(b.secretResolver),
		assembly.WithToolTimeout(b.serviceConfig.Service.RequestTimeout.Duration),
	)
	if err != nil {
		return fmt.Errorf("construct tool registry: %w", err)
	}
	b.toolNames = b.toolRegistry.Names()
	b.toolCatalog = make([]web.ToolInfo, 0, len(b.toolNames))
	for _, descriptor := range b.toolRegistry.Catalog() {
		b.toolCatalog = append(b.toolCatalog, web.ToolInfo{Name: descriptor.Name, Description: descriptor.Description})
	}

	if b.role.runsGateway() {
		b.configInvalidationOutbox, err = tenant.NewConfigInvalidationDispatcher(b.stateStore, b.configurationCache)
		if err != nil {
			return fmt.Errorf("construct configuration cache invalidation dispatcher: %w", err)
		}
		tokenCache, cacheErr := credential.NewRedisTokenCache(b.redisClient)
		if cacheErr != nil {
			return cacheErr
		}
		httpClient := &http.Client{Timeout: b.serviceConfig.Service.RequestTimeout.Duration}
		b.authHandler, b.loginSessions, b.identityStore, err = composeIdentity(b.ctx, b.getenv, b.database, b.redisClient, httpClient, tokenCache, b.environmentSecrets)
		if err != nil {
			return err
		}
		b.applicationValidator, err = config.NewPlatformPolicyValidator(
			b.modelCatalog,
			b.serviceConfig.Service.AllowedSecretRefs,
			b.serviceConfig.Service.ChannelCredentialRefs,
			b.serviceConfig.Service.ToolCredentialRefs,
			b.toolNames,
			b.storeFactory.ArtifactDrivers(),
		)
		if err != nil {
			return fmt.Errorf("construct application policy validator: %w", err)
		}
	}

	if b.role.runsGateway() || b.role.runsChannel() {
		if b.identityStore == nil {
			b.identityStore, err = identity.NewPostgresIdentityStore(b.database)
			if err != nil {
				return fmt.Errorf("construct channel identity store: %w", err)
			}
		}
		feishuTokenCache, cacheErr := feishu.NewRedisCache(b.redisClient)
		if cacheErr != nil {
			return cacheErr
		}
		producerClient, producerErr := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.AllowAutoTopicCreation())
		if producerErr != nil {
			return fmt.Errorf("construct Kafka producer client: %w", producerErr)
		}
		b.cleanups = append(b.cleanups, producerClient.Close)
		b.producer, err = messaging.NewFranzProducer(producerClient, topic)
		if err != nil {
			return err
		}
		httpClient := &http.Client{Timeout: b.serviceConfig.Service.RequestTimeout.Duration}
		b.channelConnectors, err = newChannelConnectorManager(
			b.repository, b.producer, b.secretResolver, httpClient, feishuTokenCache,
			b.redisClient, b.stateStore, b.identityStore, b.approvalBroker, b.imProgressHub, b.storeFactory, b.webReplyHub, b.executionManifests,
		)
		if err != nil {
			return fmt.Errorf("construct channel connector manager: %w", err)
		}
		deliveryPolicy, policyErr := messaging.NewRedisChannelDeliveryPolicy(b.redisClient)
		if policyErr != nil {
			return fmt.Errorf("construct channel delivery policy: %w", policyErr)
		}
		outboxChannels := []channels.Channel{channels.Web, channels.Telegram, channels.WeCom, channels.Feishu}
		switch b.role {
		case roleGateway:
			outboxChannels = []channels.Channel{channels.Web}
		case roleChannel:
			outboxChannels = []channels.Channel{channels.Telegram, channels.WeCom, channels.Feishu}
		}
		b.replyOutbox, err = messaging.NewChannelOutboxDispatcherWithResolver(
			b.stateStore,
			b.channelConnectors,
			messaging.WithSenderObserver(b.observer),
			messaging.WithDeliveryPolicy(deliveryPolicy),
			messaging.WithChannels(outboxChannels...),
		)
		if err != nil {
			return fmt.Errorf("construct channel outbox dispatcher: %w", err)
		}
		b.kafkaProbe = producerClient.Ping
	}
	return nil
}

// composeWorkerAndNode assembles the worker runtime (runner, Kafka consumer,
// governance callbacks) and the node lifecycle registration.
func (b *infrastructureBuilder) composeWorkerAndNode() error {
	// KAFKA_BROKERS / KAFKA_TOPIC 已由 composeGatewayAndChannels 校验并缓存到
	// b.brokers / b.topic，此处复用，避免重复解析环境与吞掉 error。
	brokers := b.brokers
	topic := b.topic

	if b.role.runsWorker() {
		var err error
		b.knowledgeIngestWorker, err = knowledgeingest.NewWorker(b.knowledgeIngestQueue, b.storeFactory, b.knowledgeSourcePolicy)
		if err != nil {
			return fmt.Errorf("construct knowledge ingest worker: %w", err)
		}
		if b.identityStore == nil {
			b.identityStore, err = identity.NewPostgresIdentityStore(b.database)
			if err != nil {
				return fmt.Errorf("construct worker identity store: %w", err)
			}
		}
		durableAuditSink, auditErr := governance.NewDurableToolAuditSink(b.stateStore)
		if auditErr != nil {
			return fmt.Errorf("construct durable tool audit sink: %w", auditErr)
		}
		auditSink, auditErr := governance.NewMultiAuditSink(governance.NewLogAuditSink(slog.Default()), durableAuditSink)
		if auditErr != nil {
			return fmt.Errorf("construct tool audit sinks: %w", auditErr)
		}
		toolPolicy := governance.NewStaticToolPolicy(assembly.GovernedToolNames(b.toolNames...), []string{"password", "token", "secret", "api_key", "authorization"})
		toolCallbacks, toolErr := tool.NewGovernanceCallbacks(toolPolicy, auditSink, b.toolExecutionLedger, b.serviceConfig.Service.RequestTimeout.Duration)
		if toolErr != nil {
			return fmt.Errorf("construct governance tool callbacks: %w", toolErr)
		}
		approvalReviewer, approvalErr := governance.NewInteractiveApprovalReviewer(b.approvalBroker)
		if approvalErr != nil {
			return fmt.Errorf("construct interactive approval reviewer: %w", approvalErr)
		}
		b.runners = assembly.NewFactoryWithModelProvider(
			b.modelProvider, b.toolRegistry, b.sessionProvider, b.storeFactory, b.memoryProvider, b.storeFactory, toolCallbacks,
			assembly.WithApprovalReviewer(approvalReviewer),
			assembly.WithDocumentInputSupport(b.knowledgeSourcePolicy.SupportsDocument("document.pdf")),
		)
		b.cleanups = append(b.cleanups, func() { _ = b.runners.Close() })
		rateLimiter, rateErr := governance.NewRedisTenantRateLimiter(b.redisClient)
		if rateErr != nil {
			return fmt.Errorf("construct tenant rate limiter: %w", rateErr)
		}
		costCatalog, costErr := agent.NewProviderCostCatalog(b.serviceConfig.ModelProviders)
		if costErr != nil {
			return fmt.Errorf("construct model cost catalog: %w", costErr)
		}
		usageGovernor, usageErr := governance.NewPostgresUsageGovernor(b.database)
		if usageErr != nil {
			return fmt.Errorf("construct model usage governor: %w", usageErr)
		}
		runtime, runtimeErr := agent.NewRuntime(
			b.repository,
			b.runners,
			b.webIdempotency,
			b.stateStore,
			b.serviceConfig.Service.RequestTimeout.Duration,
			time.Hour,
			agent.WithInvocationFactory(agent.DefaultInvocationFactory),
			agent.WithObserver(b.observer),
			agent.WithExecutionDedup(b.executionDedup),
			agent.WithReplyDeltaPublisher(b.webReplyHub),
			agent.WithIMReplyDeltaPublisher(b.imProgressHub),
			agent.WithArtifactProvider(b.storeFactory),
			agent.WithModelInputValidator(b.modelCatalog),
			agent.WithDocumentInputExtractor(b.knowledgeSourcePolicy),
			agent.WithTenantRateLimiter(rateLimiter),
			agent.WithTenantRoleResolver(b.identityStore),
			agent.WithModelCostCalculator(costCatalog),
			agent.WithUsageGovernor(usageGovernor),
		)
		if runtimeErr != nil {
			return fmt.Errorf("construct agent runtime: %w", runtimeErr)
		}
		groupID, groupErr := required(b.getenv, "KAFKA_GROUP_ID")
		if groupErr != nil {
			return groupErr
		}
		consumerClient, consumerErr := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumerGroup(groupID), kgo.ConsumeTopics(topic), kgo.DisableAutoCommit(), kgo.AllowAutoTopicCreation())
		if consumerErr != nil {
			return fmt.Errorf("construct Kafka consumer client: %w", consumerErr)
		}
		b.cleanups = append(b.cleanups, consumerClient.Close)
		consumer, consumerErr := messaging.NewFranzConsumer(consumerClient, topic+".dlq")
		if consumerErr != nil {
			return consumerErr
		}
		processor, processorErr := agent.NewKafkaProcessor(runtime, b.repository, b.executionManifests)
		if processorErr != nil {
			return processorErr
		}
		b.worker, err = messaging.NewWorkerWithRetryTracker(consumer, processor, 3, b.retryTracker)
		if err != nil {
			return err
		}
		if b.kafkaProbe == nil {
			b.kafkaProbe = consumerClient.Ping
		}
	}

	if b.kafkaProbe == nil {
		return fmt.Errorf("Kafka probe is unavailable for role %q", b.role)
	}
	nodeStore, err := node.NewPostgresStore(b.database)
	if err != nil {
		return fmt.Errorf("construct node store: %w", err)
	}
	b.nodeStore = nodeStore
	nodeID := strings.TrimSpace(b.getenv("NODE_ID"))
	if nodeID == "" {
		var hostErr error
		nodeID, hostErr = os.Hostname()
		if hostErr != nil || strings.TrimSpace(nodeID) == "" {
			return fmt.Errorf("resolve node ID: %w", hostErr)
		}
	}
	b.nodeLifecycle, err = node.NewLifecycle(b.nodeStore, node.LifecycleConfig{
		NodeID: nodeID, Role: string(b.role), BuildVersion: trpcservice.Version,
		HeartbeatInterval: 5 * time.Second, OfflineAfter: 15 * time.Second,
		Inflight: func() int {
			if b.worker == nil {
				return 0
			}
			return b.worker.ActiveDeliveries()
		},
	})
	if err != nil {
		return fmt.Errorf("construct node lifecycle: %w", err)
	}
	return nil
}

// build materializes the assembled components into the final infrastructure.
func (b *infrastructureBuilder) build() *infrastructure {
	return &infrastructure{
		channelConnectors:        b.channelConnectors,
		producer:                 b.producer,
		executionManifests:       b.executionManifests,
		worker:                   b.worker,
		replyOutbox:              b.replyOutbox,
		configInvalidationOutbox: b.configInvalidationOutbox,
		stateStore:               b.stateStore,
		executionDedup:           b.executionDedup,
		webIdempotency:           b.webIdempotency,
		retryTracker:             b.retryTracker,
		toolExecutions:           b.toolExecutionLedger,
		knowledgeIngestQueue:     b.knowledgeIngestQueue,
		knowledgeIngestWorker:    b.knowledgeIngestWorker,
		knowledgeSourcePolicy:    b.knowledgeSourcePolicy,
		knowledgeMigrationStore:  b.knowledgeMigrationStore,
		knowledgeMigrator:        b.knowledgeMigrator,
		backendProfiles:          b.backendProfiles,
		runners:                  b.runners,
		repository:               b.repository,
		platformStores:           b.storeFactory,
		applicationValidator:     b.applicationValidator,
		modelCatalog:             b.modelCatalog,
		modelProvider:            b.modelProvider,
		secretResolver:           b.secretResolver,
		authHandler:              b.authHandler,
		sessions:                 b.loginSessions,
		authAudits:               b.identityStore,
		identities:               b.identityStore,
		webReplyHub:              b.webReplyHub,
		agentMemory:              b.memoryProvider,
		agentSessions:            b.sessionProvider,
		sessionMigrationStore:    b.sessionMigrationStore,
		sessionMigrator:          b.sessionMigrator,
		toolCatalog:              b.toolCatalog,
		observer:                 b.observer,
		metricsHandler:           b.metricsHandler,
		nodes:                    b.nodeStore,
		nodeLifecycle:            b.nodeLifecycle,
		probes: map[string]web.DependencyProbe{
			"postgres": func(ctx context.Context) error { return b.database.PingContext(ctx) },
			"redis":    func(ctx context.Context) error { return b.redisClient.Ping(ctx).Err() },
			"kafka":    b.kafkaProbe,
			"node":     b.nodeLifecycle.Ready,
		},
		closeFuncs: b.cleanups,
	}
}

func composeInfrastructure(ctx context.Context, serviceConfig config.Config, getenv environment, role serviceRole) (*infrastructure, error) {
	b := &infrastructureBuilder{
		ctx:           ctx,
		serviceConfig: serviceConfig,
		getenv:        getenv,
		role:          role,
		cleanups:      make([]func(), 0, 4),
	}
	for _, step := range []func() error{
		b.composeStorage,
		b.composeProviders,
		b.composeGatewayAndChannels,
		b.composeWorkerAndNode,
	} {
		if err := step(); err != nil {
			b.rollback()
			return nil, err
		}
	}
	return b.build(), nil
}

func envBool(getenv environment, name string) bool {
	switch strings.ToLower(strings.TrimSpace(getenv(name))) {
	case "true", "1", "yes", "on":
		return true
	default:
		return false
	}
}

func envInt(getenv environment, name string, fallback int) int {
	raw := strings.TrimSpace(getenv(name))
	if raw == "" {
		return fallback
	}
	val, err := strconv.Atoi(raw)
	if err != nil || val <= 0 {
		return fallback
	}
	return val
}
