package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	_ "github.com/jackc/pgx/v5/stdlib"
	redisclient "github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/migrations"
	serviceagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	checkpointredis "github.com/liuzengh/trpc-agent-service/trpcservice/agent/checkpointredis"
	agentcondition "github.com/liuzengh/trpc-agent-service/trpcservice/agent/condition"
	agentpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/agentapp/postgres"
	serviceartifact "github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	brokerredis "github.com/liuzengh/trpc-agent-service/trpcservice/broker/redis"
	configpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/config/postgres"
	coordinationredis "github.com/liuzengh/trpc-agent-service/trpcservice/coordination/redis"
	gatewaypostgres "github.com/liuzengh/trpc-agent-service/trpcservice/gateway/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	governancepostgres "github.com/liuzengh/trpc-agent-service/trpcservice/governance/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/health"
	servicememory "github.com/liuzengh/trpc-agent-service/trpcservice/memory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/memorydriver"
	memorymigrationpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/migration/memorydriver/postgres"
	migrationpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/migration/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/scanner/httpdlp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	profilecontrol "github.com/liuzengh/trpc-agent-service/trpcservice/profile/controlplane"
	profilememory "github.com/liuzengh/trpc-agent-service/trpcservice/profile/inmemory"
	progressredis "github.com/liuzengh/trpc-agent-service/trpcservice/progress/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider/modelclient"
	providerpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/provider/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	relayredis "github.com/liuzengh/trpc-agent-service/trpcservice/relay/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretfs "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/filesystem"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets/generation"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets/payloadkey"
	serviceskill "github.com/liuzengh/trpc-agent-service/trpcservice/skill"
	skillpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/skill/postgres"
	storageartifact "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	artifactpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact/postgres"
	serviceknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/storage/knowledge"
	knowledgepostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/knowledge/postgres"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
	objectstores3 "github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore/s3"
	sessionpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/postgres"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	toolcodeexec "github.com/liuzengh/trpc-agent-service/trpcservice/tool/codeexec"
	toolmcp "github.com/liuzengh/trpc-agent-service/trpcservice/tool/mcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	agentmemorypg "trpc.group/trpc-go/trpc-agent-go/memory/postgres"
	agentmemoryredis "trpc.group/trpc-go/trpc-agent-go/memory/redis"
)

func runWorkerRole(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || logger == nil {
		return errors.New("invalid process dependencies")
	}
	configValue, err := loadWorkerConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	telemetryProvider, err := newRoleTelemetry(parent, getenv, "worker", logger)
	if err != nil {
		return fmt.Errorf("telemetry configuration rejected: %w", err)
	}
	defer shutdownRoleTelemetry(telemetryProvider, logger)
	db, err := sql.Open("pgx", configValue.PostgresDSN)
	if err != nil {
		return errors.New("postgres client initialization failed")
	}
	defer db.Close()
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetMaxIdleConns(8)
	db.SetMaxOpenConns(32)

	redis := redisclient.NewClient(&redisclient.Options{Addr: configValue.RedisAddress, Password: configValue.RedisPassword, DB: configValue.RedisDB})
	defer redis.Close()
	awsValue, err := awsconfig.LoadDefaultConfig(parent, awsconfig.WithRegion(configValue.S3Region))
	if err != nil {
		return errors.New("AWS SDK configuration failed")
	}
	objects, err := objectstores3.NewFromConfig(awsValue, configValue.S3Bucket, configValue.S3Endpoint, configValue.S3PathStyle,
		objectstores3.Options{MaxBytes: configValue.S3MaxBytes, AllowInsecure: configValue.S3AllowInsecure})
	if err != nil {
		return errors.New("object store configuration rejected")
	}
	secretProvider, err := secretfs.New(configValue.SecretRoot, 64<<10)
	if err != nil {
		return errors.New("secret provider configuration rejected")
	}
	payloadKeys, err := payloadkey.New(secretProvider, configValue.PayloadKeyRef)
	if err != nil {
		return errors.New("payload key configuration rejected")
	}
	catalog, err := provider.NewCatalog(provider.DeepSeekModelSchema(), provider.FakeModelSchema(), provider.FakeEmbeddingSchema(), provider.OpenAIEmbeddingSchema(), provider.PostgresBackendSchema(), provider.PostgresBackendSchemaV2(), provider.RedisMemoryBackendSchema(), provider.InMemoryBackendSchema(), provider.Mem0MemoryBackendSchema(), provider.QdrantVectorSchema(), provider.LocalQdrantVectorSchema())
	if err != nil {
		return errors.New("provider catalog initialization failed")
	}
	tenantRepo := tenantpostgres.New(db)
	agentRepo := agentpostgres.New(db)
	configRepo := configpostgres.New(db, tenantRepo)
	providerRepo := providerpostgres.New(db, catalog)
	profiles := profilecontrol.Resolver{Tenants: tenantRepo, Agents: agentRepo, Configs: configRepo, Models: providerRepo}
	memoryMigrationDBs := map[string]*sql.DB{"default": db}
	memoryMigrationExtraDBs := make([]*sql.DB, 0, len(configValue.MemoryPostgresConnections)-1)
	for connectionID, dsn := range configValue.MemoryPostgresConnections {
		if connectionID == "default" {
			continue
		}
		client, openErr := sql.Open("pgx", dsn)
		if openErr != nil {
			for _, opened := range memoryMigrationExtraDBs {
				_ = opened.Close()
			}
			return errors.New("memory migration postgres client initialization failed")
		}
		memoryMigrationDBs[connectionID] = client
		memoryMigrationExtraDBs = append(memoryMigrationExtraDBs, client)
	}
	defer func() {
		for _, client := range memoryMigrationExtraDBs {
			_ = client.Close()
		}
	}()
	memoryMigrationRedisClients := map[string]redisclient.UniversalClient{"default": redis}
	memoryMigrationExtraRedisClients := make([]redisclient.UniversalClient, 0, len(configValue.MemoryRedisConnections)-1)
	for connectionID, redisURL := range configValue.MemoryRedisConnections {
		if connectionID == "default" {
			continue
		}
		options, parseErr := redisclient.ParseURL(redisURL)
		if parseErr != nil {
			for _, client := range memoryMigrationExtraRedisClients {
				_ = client.Close()
			}
			return errors.New("memory migration redis client initialization failed")
		}
		client := redisclient.NewClient(options)
		memoryMigrationRedisClients[connectionID] = client
		memoryMigrationExtraRedisClients = append(memoryMigrationExtraRedisClients, client)
	}
	defer func() {
		for _, client := range memoryMigrationExtraRedisClients {
			_ = client.Close()
		}
	}()
	memoryMigrationPlanner := servicememory.MigrationPlanner{
		Authority: migrationpostgres.New(db), Configs: configRepo, Profiles: providerRepo, Ledger: memorymigrationpostgres.New(db),
		BuildTarget: func(_ context.Context, backend provider.BackendProfileSnapshot) (memorydriver.UserApplier, error) {
			switch backend.Provider {
			case "postgres":
				if backend.SchemaVersion != 1 && backend.SchemaVersion != 2 {
					return nil, runtime.ErrCapabilityUnsupported
				}
				connectionID := "default"
				if backend.SchemaVersion == 2 {
					connectionID = backend.Configuration["connection_id"]
				}
				client := memoryMigrationDBs[connectionID]
				if client == nil {
					return nil, runtime.ErrBackendUnavailable
				}
				return memorydriver.PostgresTarget{DB: client}, nil
			case "redis-memory":
				if backend.SchemaVersion != 1 {
					return nil, runtime.ErrCapabilityUnsupported
				}
				connectionID := backend.Configuration["connection_id"]
				client := memoryMigrationRedisClients[connectionID]
				if connectionID == "" || client == nil {
					return nil, runtime.ErrBackendUnavailable
				}
				return memorydriver.RedisTarget{Client: client, KeyPrefix: "trpc-memory"}, nil
			default:
				return nil, runtime.ErrCapabilityUnsupported
			}
		},
	}
	credentialPool := generation.New(secretProvider)
	models := modelclient.Resolver{Profiles: providerRepo, Secrets: secretProvider, Credentials: credentialPool, Subject: "worker-model"}
	toolCatalog, err := buildWorkerToolCatalog(configValue.MCPEndpoints, configValue.CodeExecutors, configValue.CodeExecutorWorkspaceRoot)
	if err != nil {
		return fmt.Errorf("tool catalog rejected: %w", err)
	}
	tools := servicetool.Resolver{Catalog: toolCatalog, Secrets: secretProvider}
	governanceStore := governancepostgres.New(db)
	graphCheckpoints := checkpointredis.Resolver{Client: redis, TTL: configValue.WorkerGraphCheckpointTTL}
	skills := serviceskill.Resolver{Catalog: skillpostgres.New(db), StagingRoot: configValue.SkillStagingRoot}
	knowledgeResolver, err := buildKnowledgeResolver(secretProvider, configRepo, providerRepo, knowledgepostgres.New(db))
	if err != nil {
		return fmt.Errorf("knowledge resolver rejected: %w", err)
	}
	memoryResolver := servicememory.Resolver{
		Profiles: providerRepo, PostgresConnections: configValue.MemoryPostgresConnections, RedisConnections: configValue.MemoryRedisConnections,
		Mem0Connections: convertMem0Connections(configValue.MemoryMem0Connections), Secrets: secretProvider, Subject: "worker-memory", AllowInMemory: configValue.MemoryAllowInMemory,
		BuildPostgres: func(dsn string) (agentmemory.Service, error) {
			return agentmemorypg.NewService(agentmemorypg.WithPostgresClientDSN(dsn), agentmemorypg.WithSkipDBInit(true))
		},
		BuildRedis: func(redisURL, keyPrefix string) (agentmemory.Service, error) {
			return agentmemoryredis.NewService(agentmemoryredis.WithRedisClientURL(redisURL), agentmemoryredis.WithKeyPrefix(keyPrefix))
		},
		BuildMem0: servicememory.NewMem0Service,
		Decorator: memoryMigrationPlanner,
		Telemetry: telemetryProvider,
	}
	// Session/Event/State are framework-owned capabilities. The immutable
	// session binding selects a credential-free connection_id; this resolver
	// maps it to deployment-owned DSNs and constructs only official synchronous
	// trpc-agent-go session/postgres services.
	sdkSessions, err := sessionpostgres.NewProfileServiceResolver(providerRepo, configValue.SessionPostgresConnections)
	if err != nil {
		return errors.New("session service configuration rejected")
	}
	sdkSessionsClosed := false
	defer func() {
		if !sdkSessionsClosed {
			_ = sdkSessions.Close()
		}
	}()
	artifacts := artifactpostgres.NewWithObjectStore(db, objects)
	artifactStores := map[string]storageartifact.Store{"default": artifacts}
	artifactStoreClients := make([]*sql.DB, 0, len(configValue.ArtifactPostgresConnections)-1)
	for connectionID, dsn := range configValue.ArtifactPostgresConnections {
		if connectionID == "default" {
			continue
		}
		artifactDB, openErr := sql.Open("pgx", dsn)
		if openErr != nil {
			for _, client := range artifactStoreClients {
				_ = client.Close()
			}
			return errors.New("artifact data-plane client initialization failed")
		}
		if pingErr := artifactDB.PingContext(parent); pingErr != nil {
			_ = artifactDB.Close()
			for _, client := range artifactStoreClients {
				_ = client.Close()
			}
			return errors.New("artifact data-plane unavailable")
		}
		artifactStores[connectionID] = artifactpostgres.NewWithObjectStore(artifactDB, objects)
		artifactStoreClients = append(artifactStoreClients, artifactDB)
	}
	defer func() {
		for _, client := range artifactStoreClients {
			_ = client.Close()
		}
	}()
	artifactResolver := serviceartifact.Resolver{
		Profiles:            providerRepo,
		PostgresConnections: configValue.ArtifactPostgresConnections,
		Stores:              artifactStores,
	}
	agentFactory := serviceagent.Factory{Profiles: profiles, Models: models, Tools: tools, Skills: skills, Knowledge: knowledgeResolver,
		Conditions:  agentcondition.DefaultRegistry(),
		Checkpoints: graphCheckpoints, Policies: governanceStore, Telemetry: telemetryProvider}
	bundles := profilememory.NewBundleManagerWithPolicy(func(ctx context.Context, key profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		snapshot, resolveErr := profiles.Resolve(ctx, key)
		if resolveErr != nil {
			return nil, nil, resolveErr
		}
		memoryService, memoryErr := memoryResolver.Resolve(ctx, snapshot)
		if memoryErr != nil {
			return nil, nil, fmt.Errorf("resolve tenant memory service: %w", memoryErr)
		}
		artifactService, artifactClose, artifactErr := artifactResolver.Resolve(ctx, snapshot)
		if artifactErr != nil {
			_ = memoryService.Close()
			return nil, nil, fmt.Errorf("resolve tenant artifact service: %w", artifactErr)
		}
		closeServices := func(closeCtx context.Context) error {
			memoryCloseErr := memoryService.Close()
			if artifactClose == nil {
				return memoryCloseErr
			}
			return errors.Join(memoryCloseErr, artifactClose(closeCtx))
		}
		bundleFactory := agentFactory
		bundleFactory.Memory = memoryService
		root, plugins, buildErr := bundleFactory.BuildWithPlugins(ctx, snapshot)
		if buildErr != nil {
			return nil, closeServices, buildErr
		}
		return &serviceagent.Bundle{AppName: snapshot.AppName, Root: root, Memory: memoryService, Artifact: artifactService, Plugins: plugins}, closeServices, nil
	}, profilememory.BundleManagerPolicy{FailureBackoff: configValue.WorkerBundleFailureBackoff, CloseTimeout: configValue.WorkerBundleCloseTimeout})
	credentialInvalidator := modelclient.CredentialInvalidator{Pool: credentialPool, Bundles: bundles, Subject: "worker-model"}

	tasks := gatewaypostgres.NewTaskStore(db)
	runGovernance := governance.Service{Repository: governanceStore, Ledger: governanceStore, Decisions: governanceStore, Telemetry: telemetryProvider}
	var dlpScanner *httpdlp.Scanner
	if configValue.DLPEndpoint != "" {
		authorizer, authErr := newDLPAuthorizer(secretProvider, configValue.DLPBackendVersion, secrets.SecretRef{Ref: configValue.DLPSecretRef, Version: configValue.DLPSecretVersion})
		if authErr != nil {
			return errors.New("worker DLP authorization configuration rejected")
		}
		scanner := httpdlp.Scanner{Endpoint: configValue.DLPEndpoint, Authorize: authorizer, ProbeTenantID: configValue.DLPProbeTenant,
			Timeout: configValue.ProbeTimeout, MaxBytes: 16 << 20, AllowInsecure: configValue.DLPAllowInsecure}
		dlpScanner = &scanner
		guard := governance.ScannerContentGuard{Scanner: scanner}
		runGovernance.InputGuard, runGovernance.OutputGuard = guard, guard
	}
	sessions := sessionpostgres.NewWithTelemetry(db, telemetryProvider)
	payloads := messagingpostgres.NewWithPayloadKeyResolver(db, payloadKeys)
	agentFactory.Confirmations, agentFactory.ToolResults = governanceStore, payloads
	progressPublisher, err := progressredis.NewPublisher(redis, progressredis.Config{Environment: configValue.RedisEnvironment})
	if err != nil {
		return errors.New("progress publisher configuration rejected")
	}
	executor := worker.RunnerExecutor{Tasks: tasks, TenantPolicies: tenantRepo, Profiles: profiles, Bundles: bundles, Sessions: sessions, SessionServices: sdkSessions,
		Payloads: payloads, Artifacts: artifacts, Inputs: worker.JSONTextInputDecoder{},
		Progress:          progressPublisher,
		EventDrainTimeout: configValue.WorkerBundleCloseTimeout, Governance: runGovernance, Confirmations: governanceStore,
		ContinuationTools: agentFactory, Telemetry: telemetryProvider}
	dispatchBroker, err := brokerredis.New(redis, brokerredis.Config{Environment: configValue.RedisEnvironment, Group: configValue.WorkerGroup,
		ShardCount: uint32(configValue.WorkerShardCount), ReadBlock: 250 * time.Millisecond, ReclaimIdle: configValue.WorkerLeaseTTL})
	if err != nil {
		return errors.New("execution broker configuration rejected")
	}
	brokerMetrics := &metrics.BrokerRegistry{SnapshotTTL: 3 * configValue.WorkerBacklogPoll}
	backlogMonitor := broker.BacklogMonitor{Source: dispatchBroker, Observer: brokerMetrics, PollInterval: configValue.WorkerBacklogPoll}
	leases, err := coordinationredis.New(redis, configValue.RedisEnvironment)
	if err != nil {
		return errors.New("lease manager configuration rejected")
	}
	workerID := configValue.WorkerID
	if workerID == "" {
		host, _ := os.Hostname()
		workerID = fmt.Sprintf("%s-%d", valueOr(host, "trpc-worker"), os.Getpid())
	}
	shards := make([]broker.Shard, len(configValue.WorkerShards))
	for index, shard := range configValue.WorkerShards {
		shards[index] = broker.Shard(shard)
	}
	hints := &worker.CancelHintHub{}
	publisher, err := relayredis.NewPublisher(redis, relayredis.Config{Environment: configValue.RedisEnvironment})
	if err != nil {
		return errors.New("execution control publisher configuration rejected")
	}
	controlQueue, err := relayredis.NewExecutionControlQueue(redis, publisher, relayredis.ExecutionControlQueueConfig{
		Group: configValue.WorkerControlGroup, ReadBlock: 250 * time.Millisecond, ReclaimIdle: configValue.WorkerLeaseTTL})
	if err != nil {
		return errors.New("execution control queue configuration rejected")
	}
	configControlQueue, err := relayredis.NewTenantControlQueue(redis, publisher, relayredis.TenantControlQueueConfig{
		Group: configValue.WorkerControlGroup + "-config", ReadBlock: 250 * time.Millisecond, ReclaimIdle: configValue.WorkerLeaseTTL})
	if err != nil {
		return errors.New("config invalidation queue configuration rejected")
	}
	lifecycle := worker.NewLifecycle()
	consumer := worker.Consumer{WorkerID: workerID, Shards: shards, Broker: dispatchBroker, Leases: leases, Sessions: sessions,
		Parker: tasks, Statuses: tasks, Executor: executor, LeaseTTL: configValue.WorkerLeaseTTL, RenewInterval: configValue.WorkerLeaseRenew,
		RetryWait: configValue.WorkerRetryWait, ReclaimInterval: configValue.WorkerReclaimInterval, ReclaimLimit: configValue.WorkerReclaimLimit,
		CancelPollInterval: configValue.WorkerCancelPoll, CancelHints: hints, DrainTimeout: configValue.WorkerDrainTimeout, Lifecycle: lifecycle,
		OnDeliveryError: func(_ context.Context, delivery broker.Delivery, deliveryErr error) {
			logger.Printf("worker delivery degraded tenant=%q request=%q: %v", delivery.Envelope.TenantID, delivery.Envelope.RequestID, deliveryErr)
		}}

	migrationReadiness := migrations.NewRunner(db)
	dependencies := []health.Dependency{
		{Name: "postgres", Probe: db.PingContext}, {Name: "postgres_schema", Probe: migrationReadiness.Ready},
		{Name: "redis", Probe: func(ctx context.Context) error { return redis.Ping(ctx).Err() }},
		{Name: "object_store", Probe: objects.Probe}, {Name: "secret_provider", Probe: secretProvider.ProbeRoot},
		{Name: "payload_key_provider", Probe: func(ctx context.Context) error {
			value, resolveErr := payloadKeys.ResolvePayloadKey(ctx, configValue.WorkerProbeTenant, configValue.PayloadKeyVersion)
			clear(value.Bytes)
			return resolveErr
		}},
	}
	if dlpScanner != nil {
		dependencies = append(dependencies, health.Dependency{Name: "governance_dlp", Probe: dlpScanner.Probe})
	}
	monitor, err := health.NewMonitor(lifecycle, dependencies, configValue.ProbeTimeout, configValue.ProbeInterval)
	if err != nil {
		return errors.New("readiness configuration rejected")
	}
	if err := monitor.ProbeOnce(parent); err != nil {
		return errors.New("initial dependency probe interrupted")
	}
	listener, err := net.Listen("tcp", configValue.ListenAddress)
	if err != nil {
		return errors.New("HTTP listener initialization failed")
	}
	defer listener.Close()
	mux := http.NewServeMux()
	mux.Handle("/livez", health.Handler{Checker: monitor})
	mux.Handle("/readyz", health.Handler{Checker: monitor})
	mux.Handle("/metrics", brokerMetrics)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}

	processCtx, cancelProcess := context.WithCancel(parent)
	defer cancelProcess()
	stopSignals := worker.InstallSignalDrain(processCtx, lifecycle)
	defer stopSignals()
	errorsCh := make(chan error, 9)
	consumerDone := make(chan error, 1)
	var background sync.WaitGroup
	start := func(name string, operation func(context.Context) error) {
		background.Add(1)
		go func() {
			defer background.Done()
			if operationErr := operation(processCtx); operationErr != nil && !errors.Is(operationErr, context.Canceled) {
				errorsCh <- fmt.Errorf("%s stopped", name)
			}
		}()
	}
	start("readiness monitor", monitor.Run)
	start("broker backlog monitor", backlogMonitor.Run)
	start("progress publisher", progressPublisher.Run)
	start("config invalidation relay", relay.TenantControlRelay{Outbox: payloads, Controls: publisher,
		Kind: "config-invalidation", Owner: workerID + "-config-relay", BatchSize: configValue.WorkerReclaimLimit,
		ClaimTTL: configValue.WorkerLeaseTTL, ClaimRenewInterval: configValue.WorkerLeaseRenew,
		RetryDelay: configValue.WorkerRetryWait, PollInterval: configValue.WorkerReclaimInterval,
		Telemetry: telemetryProvider}.Run)
	start("execution control consumer", func(ctx context.Context) error {
		return controlQueue.ConsumeExecutionControl(ctx, relay.ExecutionControlConsumerOptions{ConsumerID: workerID + "-control"}, hints.ConsumeExecutionControl)
	})
	start("execution control reclaimer", func(ctx context.Context) error {
		ticker := time.NewTicker(configValue.WorkerReclaimInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				deliveries, reclaimErr := controlQueue.ReclaimExecutionControls(ctx, relay.ExecutionControlConsumerOptions{ConsumerID: workerID + "-control", Limit: configValue.WorkerReclaimLimit})
				if reclaimErr != nil {
					logger.Printf("worker execution-control reclaim degraded: %v", reclaimErr)
					continue
				}
				for _, delivery := range deliveries {
					if handleErr := hints.ConsumeExecutionControl(ctx, delivery); handleErr != nil {
						logger.Printf("worker execution-control hint rejected: %v", handleErr)
						continue
					}
					if ackErr := controlQueue.AckExecutionControl(ctx, delivery); ackErr != nil {
						logger.Printf("worker execution-control reclaim ACK degraded: %v", ackErr)
					}
				}
			}
		}
	})
	consumeConfigInvalidation := func(ctx context.Context, delivery relay.TenantControlDelivery) error {
		event := delivery.Event
		switch {
		case event.Kind != "config-invalidation":
			return nil
		case strings.HasPrefix(event.PayloadRef, "provider-profile://"):
			return credentialInvalidator.ConsumeProfileInvalidation(ctx, providerRepo, event.TenantID, event.AggregateID, event.Version, event.PayloadRef)
		case strings.HasPrefix(event.PayloadRef, "config://"):
			// Config snapshots are immutable and executions carry their exact
			// version. Retiring only idle bundles therefore makes new work pick
			// up the published snapshot without disrupting in-flight requests.
			bundles.RetireTenant(event.TenantID)
			return nil
		case strings.HasPrefix(event.PayloadRef, "memory-migration://"):
			// Migration state changes do not necessarily publish a new tenant
			// ConfigSnapshot. Retire the immutable bundle so the next request
			// re-projects dual-write/reverse-write state from PostgreSQL.
			bundles.RetireTenant(event.TenantID)
			return nil
		default:
			return nil
		}
	}
	start("credential invalidation consumer", func(ctx context.Context) error {
		return configControlQueue.ConsumeTenantControl(ctx, relay.TenantControlConsumerOptions{ConsumerID: workerID + "-config"}, consumeConfigInvalidation)
	})
	start("credential invalidation reclaimer", func(ctx context.Context) error {
		ticker := time.NewTicker(configValue.WorkerReclaimInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				deliveries, reclaimErr := configControlQueue.ReclaimTenantControls(ctx, relay.TenantControlConsumerOptions{ConsumerID: workerID + "-config", Limit: configValue.WorkerReclaimLimit})
				if reclaimErr != nil {
					logger.Printf("worker credential invalidation reclaim degraded: %v", reclaimErr)
					continue
				}
				for _, delivery := range deliveries {
					if handleErr := consumeConfigInvalidation(ctx, delivery); handleErr != nil {
						logger.Printf("worker credential invalidation rejected: %v", handleErr)
						continue
					}
					if ackErr := configControlQueue.AckTenantControl(ctx, delivery); ackErr != nil {
						logger.Printf("worker credential invalidation reclaim ACK degraded: %v", ackErr)
					}
				}
			}
		}
	})
	start("confirmation expiry reconciler", func(ctx context.Context) error {
		reconciler := governance.ConfirmationExpiryReconciler{Coordinator: governanceStore, BatchSize: configValue.WorkerReclaimLimit}
		ticker := time.NewTicker(configValue.WorkerReclaimInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				if _, expiryErr := reconciler.RunOnce(ctx); expiryErr != nil {
					logger.Printf("worker confirmation expiry degraded: %v", expiryErr)
				}
			}
		}
	})
	background.Add(1)
	go func() {
		defer background.Done()
		serveErr := server.Serve(listener)
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errorsCh <- errors.New("HTTP server stopped")
		}
	}()
	go func() {
		consumerDone <- consumer.Run(processCtx)
	}()
	logger.Printf("trpc-agent-service worker id=%q shards=%v lifecycle/readiness listening on %s", workerID, configValue.WorkerShards, configValue.ListenAddress)

	var terminalErr error
	consumerFinished := false
	select {
	case <-parent.Done():
		lifecycle.BeginDrain()
	case <-lifecycle.Drain():
	case consumerErr := <-consumerDone:
		consumerFinished = true
		if consumerErr != nil && !errors.Is(consumerErr, context.Canceled) {
			terminalErr = errors.New("worker consumer stopped")
		}
		lifecycle.BeginDrain()
	case terminalErr = <-errorsCh:
		lifecycle.BeginDrain()
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), configValue.ShutdownTimeout)
	defer cancelShutdown()
	if !consumerFinished {
		select {
		case consumerErr := <-consumerDone:
			consumerFinished = true
			if consumerErr != nil && !errors.Is(consumerErr, context.Canceled) && terminalErr == nil {
				terminalErr = errors.New("worker consumer stopped")
			}
		case <-shutdownCtx.Done():
			if terminalErr == nil {
				terminalErr = errors.New("worker drain timed out")
			}
		}
	}
	cancelProcess()
	if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil && terminalErr == nil {
		terminalErr = errors.New("HTTP shutdown timed out")
	}
	if closeErr := bundles.Close(shutdownCtx); closeErr != nil && terminalErr == nil {
		terminalErr = errors.New("runtime bundle shutdown timed out")
	}
	if closeErr := sdkSessions.Close(); closeErr != nil && terminalErr == nil {
		terminalErr = errors.New("session service shutdown failed")
	}
	sdkSessionsClosed = true
	backgroundDone := make(chan struct{})
	go func() { background.Wait(); close(backgroundDone) }()
	select {
	case <-backgroundDone:
	case <-shutdownCtx.Done():
		if terminalErr == nil {
			terminalErr = errors.New("background shutdown timed out")
		}
	}
	lifecycle.MarkStopped()
	return terminalErr
}

func convertMem0Connections(values map[string]mem0Connection) map[string]servicememory.Mem0Connection {
	converted := make(map[string]servicememory.Mem0Connection, len(values))
	for id, value := range values {
		converted[id] = servicememory.Mem0Connection{Host: value.Host, SelfHostedOSS: value.SelfHostedOSS}
	}
	return converted
}

// buildToolCatalog assembles the process-local, code-owned Tool Catalog from the
// reviewed fixed MCP endpoints. Every endpoint maps to exactly one registration;
// a malformed declaration fails here so the worker refuses startup instead of
// silently running with a half-registered catalog.
func buildToolCatalog(endpoints []mcpEndpoint) (*servicetool.Catalog, error) {
	registrations := make([]servicetool.Registration, 0, len(endpoints))
	for _, endpoint := range endpoints {
		registration, err := toolmcp.NewRegistration(endpoint.TenantID, endpoint.ToolID, endpoint.Version, toolmcp.Config{
			Transport: endpoint.Transport, ServerURL: endpoint.ServerURL, RemoteToolName: endpoint.ToolID, ExpectedDeclarationDigest: endpoint.DeclarationDigest, Timeout: endpoint.Timeout,
			SecretHeader: endpoint.SecretHeader, SecretPrefix: endpoint.SecretPrefix,
		}, secrets.SecretRef{Ref: endpoint.SecretRef, Version: endpoint.SecretVersion})
		if err != nil {
			return nil, fmt.Errorf("MCP endpoint %q: %w", endpoint.ToolID, err)
		}
		registrations = append(registrations, registration)
	}
	return servicetool.NewCatalog(registrations...)
}

// buildWorkerToolCatalog extends the reviewed MCP catalog with the optional,
// service-owned code-execution registrations. The sandbox is intentionally
// absent unless an operator supplies both exact tenant bindings and a dedicated
// workspace root; this prevents a normal worker from accidentally acquiring a
// local shell capability.
func buildWorkerToolCatalog(mcpEndpoints []mcpEndpoint, codeExecutors []codeExecutorEndpoint, workspaceRoot string) (*servicetool.Catalog, error) {
	catalog, err := buildToolCatalog(mcpEndpoints)
	if err != nil {
		return nil, err
	}
	if len(codeExecutors) == 0 {
		return catalog, nil
	}
	config := toolcodeexec.DefaultConfig(workspaceRoot)
	for _, endpoint := range codeExecutors {
		registration, registrationErr := toolcodeexec.NewSandboxRegistration(endpoint.TenantID, endpoint.ToolID, endpoint.Version, endpoint.ContentDigest, config)
		if registrationErr != nil {
			return nil, fmt.Errorf("code executor %q: %w", endpoint.ToolID, registrationErr)
		}
		if registerErr := catalog.Register(registration); registerErr != nil {
			return nil, fmt.Errorf("code executor %q catalog registration: %w", endpoint.ToolID, registerErr)
		}
	}
	return catalog, nil
}

// buildKnowledgeResolver composes the public trpc-agent-go Knowledge runtime
// from tenant ConfigSnapshot -> BackendProfile -> published Manifest. Endpoint
// and credentials therefore remain per-tenant immutable control-plane facts,
// never process-global worker settings.
func buildKnowledgeResolver(secretProvider secrets.Provider, configs serviceknowledge.ConfigSnapshotReader,
	profiles serviceknowledge.KnowledgeProfileReader, manifests serviceknowledge.IngestionStore,
) (serviceagent.KnowledgeResolver, error) {
	if secretProvider == nil || configs == nil || profiles == nil || manifests == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	factory := serviceknowledge.RuntimeFactory{Backends: serviceknowledge.BackendAdapterResolver{
		Configs: configs, Backends: profiles, Secrets: secretProvider, Subject: "worker-knowledge-qdrant"},
		Manifests: manifests, Embedders: serviceknowledge.EmbedderResolver{Profiles: profiles, Secrets: secretProvider, Subject: "worker-knowledge-embedder"}}
	return serviceknowledge.Resolver{Factory: factory, Limits: serviceknowledge.RetrievalLimits{
		MaxQueryBytes: 16 << 10, MaxResults: 20, MaxResultBytes: 1 << 20}}, nil
}
