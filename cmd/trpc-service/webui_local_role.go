package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	redisclient "github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/migrations"
	serviceagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	checkpointredis "github.com/liuzengh/trpc-agent-service/trpcservice/agent/checkpointredis"
	agentcondition "github.com/liuzengh/trpc-agent-service/trpcservice/agent/condition"
	agentapp "github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	agentpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/agentapp/postgres"
	serviceartifact "github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	brokerredis "github.com/liuzengh/trpc-agent-service/trpcservice/broker/redis"
	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/credentials"
	credentialpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/credentials/postgres"
	channeldelivery "github.com/liuzengh/trpc-agent-service/trpcservice/channels/delivery"
	deliverypostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/delivery/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	feishuprotocol "github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu/protocol"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	ingresspostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/webui"
	webuipostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/webui/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	wecomprotocol "github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom/protocol"
	configdomain "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	configpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/config/postgres"
	coordinationredis "github.com/liuzengh/trpc-agent-service/trpcservice/coordination/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	gatewaypostgres "github.com/liuzengh/trpc-agent-service/trpcservice/gateway/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	governancepostgres "github.com/liuzengh/trpc-agent-service/trpcservice/governance/postgres"
	servicememory "github.com/liuzengh/trpc-agent-service/trpcservice/memory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	preprocesspostgres "github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/scanner/clamav"
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
	serviceqdrant "github.com/liuzengh/trpc-agent-service/trpcservice/storage/knowledge/qdrant"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
	sessionpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool/localnote"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	agentmemorypg "trpc.group/trpc-go/trpc-agent-go/memory/postgres"
)

const (
	webUILocalRuntimeLockKey   = "trpc-agent-service:webui-local-runtime"
	webUILocalTenantID         = "t_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	webUILocalAppID            = "app_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	webUILocalChildAppID       = "app_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	webUILocalBindingID        = "local-webui"
	webUILocalAccountID        = "local-webui"
	webUILocalModelID          = "deepseek-local"
	webUILocalModelVersion     = int64(2)
	webUILocalModelName        = "deepseek-v4-flash-vision-exp"
	webUILocalKnowledgeID      = "webui-local-knowledge"
	webUILocalKnowledgeVersion = int64(1)
	webUILocalEmbedderID       = "webui-local-fake-embedder"
	webUILocalMemoryID         = "webui-local-memory"
	webUILocalQdrantID         = "webui-local-qdrant"
	webUILocalQdrantCollection = "webui_local_knowledge"
	webUILocalVectorGeneration = "webui-local-v1"
	webUILocalVectorSize       = 16
	webUILocalSkillID          = "webui_local_guide"
	webUILocalSkillVersion     = int64(1)
	webUILocalRouteKey         = "local-webui"
	webUILocalToken            = "local-webui-token-change-me"
	feishuLocalBindingID       = "local-feishu"
	feishuLocalRouteKey        = "local-feishu"
	wecomLocalBindingID        = "local-wecom"
	wecomLocalRouteKey         = "local-wecom"
	payloadKeyRef              = "secret://local/payload-key"
	webUILocalInstruction      = "You are a concise and helpful assistant. When the user asks to create, save, or record a note, call webui_create_note. Never claim that a note was created before the tool result is available. When an image content part is present, it was securely attached to this request: analyze its visible content directly and do not claim that the image or attachment was unavailable."
)

type webUILocalConfig struct {
	PostgresDSN, RedisAddress, ListenAddress                                   string
	RedisEnvironment, SecretRoot, APIKeyFile, SkillStagingRoot, QdrantEndpoint string
	RouteKey, Token, InstanceID, ClamAVAddress                                 string
	ExclusiveRuntime                                                           bool
	FeishuEnabled                                                              bool
	FeishuAppID, FeishuAppSecret                                               string
	FeishuVerificationToken, FeishuEncryptKey, FeishuBotOpenID                 string
	WeComEnabled                                                               bool
	WeComCorpID, WeComAppSecret                                                string
	WeComCallbackToken, WeComEncodingAESKey                                    string
	WeComAgentID                                                               int64
}

type webUILocalBootstrap struct {
	Tenant       tenant.Tenant
	Config       configdomain.Snapshot
	Route        ingress.BindingRoute
	FeishuRoute  ingress.BindingRoute
	WeComRoute   ingress.BindingRoute
	SecretRoot   string
	PayloadKey   *payloadkey.Resolver
	SecretStore  *secretfs.Provider
	ProviderRepo *providerpostgres.Repository
}

func runWebUILocalRole(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || getenv == nil || logger == nil {
		return errors.New("invalid process dependencies")
	}
	configValue, err := loadWebUILocalConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	telemetryProvider, err := newRoleTelemetry(parent, getenv, "webui-local", logger)
	if err != nil {
		return fmt.Errorf("telemetry configuration rejected: %w", err)
	}
	defer shutdownRoleTelemetry(telemetryProvider, logger)
	db, err := sql.Open("pgx", configValue.PostgresDSN)
	if err != nil {
		return errors.New("postgres client initialization failed")
	}
	defer db.Close()
	redis := redisclient.NewClient(&redisclient.Options{Addr: configValue.RedisAddress})
	defer redis.Close()
	if err := db.PingContext(parent); err != nil {
		return errors.New("postgres unavailable")
	}
	if err := redis.Ping(parent).Err(); err != nil {
		return errors.New("redis unavailable")
	}
	progressPublisher, err := progressredis.NewPublisher(redis, progressredis.Config{Environment: configValue.RedisEnvironment})
	if err != nil {
		return errors.New("WebUI progress publisher configuration rejected")
	}
	progressSubscriber, err := progressredis.NewSubscriber(redis, progressredis.Config{Environment: configValue.RedisEnvironment})
	if err != nil {
		return errors.New("WebUI progress subscriber configuration rejected")
	}
	malware := clamav.Scanner{Address: configValue.ClamAVAddress, MaxBytes: 10 << 20}
	if err := malware.Probe(parent); err != nil {
		return errors.New("malware scanner unavailable")
	}
	releaseRuntimeLock, err := acquireWebUILocalRuntimeLock(parent, db, configValue.ExclusiveRuntime)
	if err != nil {
		return err
	}
	defer releaseRuntimeLock()
	bootstrap, err := bootstrapWebUILocal(parent, db, configValue)
	if err != nil {
		return fmt.Errorf("local bootstrap failed: %w", err)
	}

	payloads := messagingpostgres.NewWithPayloadKeyResolver(db, bootstrap.PayloadKey)
	inbox := messagingpostgres.NewWithPayloadKeyResolver(db, bootstrap.PayloadKey)
	tasks := gatewaypostgres.NewTaskStore(db)
	preprocessStore := preprocesspostgres.New(db)
	tenantRepo := tenantpostgres.New(db)
	configRepo := configpostgres.New(db, tenantRepo)
	bindings := ingresspostgres.New(db)
	resolver := ingress.Resolver{Store: bindings, Secrets: bootstrap.SecretStore, TTL: 30 * time.Second}
	webuiMailbox := webuipostgres.New(db)
	webuiAdapter := &webui.Adapter{Protocol: webui.Verifier{}, Mailbox: webuiMailbox}
	endpoint, err := newChannelEndpoint(webuiAdapter, resolver, identity.Mapper{Secrets: bootstrap.SecretStore},
		preprocessStore, payloads, 1, 1<<20, telemetryProvider, configRepo)
	if err != nil {
		return errors.New("WebUI callback configuration rejected")
	}
	browser := webui.BrowserHandler{Callback: endpoint, Routes: bindings, Secrets: bootstrap.SecretStore,
		Messages: webuiMailbox, Results: payloads, ReplyRoutes: inbox, Progress: progressSubscriber}
	adapters := []channel.Adapter{webuiAdapter}
	var feishuEndpoint, wecomEndpoint http.Handler
	// The local profiles share one PostgreSQL volume, so the delivery catalog
	// must resolve every channel that may already be persisted there. Adapter
	// construction is therefore unconditional; the profile switches only gate
	// ingress endpoints and bootstrap, and a disabled channel enqueues no
	// reply outbox work because its callbacks are not mounted.
	providerHTTP := &http.Client{Timeout: 30 * time.Second}
	sendCredentials := credentials.Resolver{Locator: credentialpostgres.New(db), Secrets: bootstrap.SecretStore}
	feishuCredentials := &feishu.CredentialProvider{Secrets: sendCredentials, Client: providerHTTP}
	wecomTokens := &wecom.TokenProvider{Secrets: sendCredentials, Client: providerHTTP}
	feishuAdapter := &feishu.Adapter{Protocol: feishuprotocol.Verifier{}, Sender: feishu.OfficialSender{Tokens: feishuCredentials, Client: providerHTTP, Clients: &feishu.ClientCache{
		Credentials: feishuCredentials,
		NewClient:   func(appID, appSecret string) *lark.Client { return lark.NewClient(appID, appSecret) },
	}}}
	wecomAdapter := &wecom.Adapter{Protocol: wecomprotocol.Verifier{}, Sender: wecom.OfficialSender{Tokens: wecomTokens}}
	adapters = append(adapters, feishuAdapter, wecomAdapter)
	if configValue.FeishuEnabled {
		feishuEndpoint, err = newChannelEndpoint(feishuAdapter, resolver, identity.Mapper{Secrets: bootstrap.SecretStore},
			preprocessStore, payloads, 1, 1<<20, telemetryProvider, configRepo)
		if err != nil {
			return errors.New("Feishu callback configuration rejected")
		}
	}
	if configValue.WeComEnabled {
		wecomEndpoint, err = newChannelEndpoint(wecomAdapter, resolver, identity.Mapper{Secrets: bootstrap.SecretStore},
			preprocessStore, payloads, 1, 1<<20, telemetryProvider, configRepo)
		if err != nil {
			return errors.New("WeCom callback configuration rejected")
		}
	}

	streamBroker, err := brokerredis.New(redis, brokerredis.Config{Environment: configValue.RedisEnvironment,
		Group: "webui-workers", ShardCount: 4, ReadBlock: 250 * time.Millisecond, ReclaimIdle: 30 * time.Second})
	if err != nil {
		return errors.New("broker configuration rejected")
	}
	leases, err := coordinationredis.New(redis, configValue.RedisEnvironment)
	if err != nil {
		return errors.New("lease configuration rejected")
	}
	publisher, err := relayredis.NewPublisher(redis, relayredis.Config{Environment: configValue.RedisEnvironment})
	if err != nil {
		return errors.New("relay publisher configuration rejected")
	}

	appRepo := agentpostgres.New(db)
	profiles := profilecontrol.Resolver{Tenants: tenantRepo, Agents: appRepo, Configs: configRepo, Models: bootstrap.ProviderRepo}
	models := modelclient.Resolver{Profiles: bootstrap.ProviderRepo, Secrets: bootstrap.SecretStore, Credentials: generation.New(bootstrap.SecretStore), Subject: "worker-model"}
	governanceStore := governancepostgres.New(db)
	skills := serviceskill.Resolver{Catalog: skillpostgres.New(db), StagingRoot: configValue.SkillStagingRoot}
	knowledgeResolver, err := buildKnowledgeResolver(bootstrap.SecretStore, configRepo, bootstrap.ProviderRepo, knowledgepostgres.New(db))
	if err != nil {
		return errors.New("knowledge resolver configuration rejected")
	}
	toolCatalog, err := servicetool.NewCatalog(localnote.Registration(webUILocalTenantID))
	if err != nil {
		return errors.New("tool catalog initialization failed")
	}
	tools := servicetool.Resolver{Catalog: toolCatalog, Secrets: bootstrap.SecretStore}
	artifacts := artifactpostgres.New(db)
	artifactResolver := serviceartifact.Resolver{Profiles: bootstrap.ProviderRepo,
		PostgresConnections: map[string]string{"default": configValue.PostgresDSN}, Stores: map[string]storageartifact.Store{"default": artifacts}}
	browser.ReplyRoutes = payloads
	browser.Confirmations = governanceStore
	browser.Actions = governance.ConfirmationActionService{Coordinator: governanceStore}
	graphCheckpoints := checkpointredis.Resolver{Client: redis, TTL: 7 * 24 * time.Hour}
	// The local role uses the same immutable Memory binding and resolver as a
	// distributed Worker. It merely supplies one deployment-owned "default"
	// PostgreSQL connection for its disposable Compose plane.
	memoryResolver := servicememory.Resolver{
		Profiles:            bootstrap.ProviderRepo,
		PostgresConnections: map[string]string{"default": configValue.PostgresDSN},
		BuildPostgres: func(dsn string) (agentmemory.Service, error) {
			return agentmemorypg.NewService(agentmemorypg.WithPostgresClientDSN(dsn), agentmemorypg.WithSkipDBInit(true))
		},
	}
	sdkSessions, err := sessionpostgres.NewOfficialSessionService(configValue.PostgresDSN)
	if err != nil {
		return errors.New("session service configuration rejected")
	}
	defer sdkSessions.Close()
	agentFactory := serviceagent.Factory{Profiles: profiles, Models: models, Tools: tools, Skills: skills, Knowledge: knowledgeResolver,
		Conditions:  agentcondition.DefaultRegistry(),
		Checkpoints: graphCheckpoints,
		Policies:    governanceStore, Confirmations: governanceStore, ToolResults: payloads, Telemetry: telemetryProvider}
	bundles := profilememory.NewBundleManager(func(ctx context.Context, key profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
		snapshot, resolveErr := profiles.Resolve(ctx, key)
		if resolveErr != nil {
			return nil, nil, resolveErr
		}
		memoryService, memoryErr := memoryResolver.Resolve(ctx, snapshot)
		if memoryErr != nil {
			return nil, nil, fmt.Errorf("resolve local tenant memory service: %w", memoryErr)
		}
		artifactService, artifactClose, artifactErr := artifactResolver.Resolve(ctx, snapshot)
		if artifactErr != nil {
			_ = memoryService.Close()
			return nil, nil, fmt.Errorf("resolve local tenant artifact service: %w", artifactErr)
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
	})
	defer bundles.Close(context.Background())
	executor := worker.RunnerExecutor{Tasks: tasks, Profiles: profiles, Bundles: bundles,
		Sessions: sessionpostgres.NewWithTelemetry(db, telemetryProvider), SDKSessions: sdkSessions, Payloads: payloads, Artifacts: artifacts,
		Inputs: worker.JSONTextInputDecoder{}, EventDrainTimeout: 30 * time.Second,
		Progress:   progressPublisher,
		Governance: governance.Service{Repository: governanceStore, Ledger: governanceStore, Decisions: governanceStore}, Confirmations: governanceStore,
		ContinuationTools: agentFactory, Telemetry: telemetryProvider}
	workerConsumer := worker.Consumer{WorkerID: configValue.instanceName("worker"), Shards: []broker.Shard{0, 1, 2, 3}, Broker: streamBroker,
		Leases: leases, Sessions: sessionpostgres.NewWithTelemetry(db, telemetryProvider), Parker: tasks, Statuses: tasks, Executor: executor,
		LeaseTTL: 30 * time.Second, RenewInterval: 10 * time.Second, RetryWait: 250 * time.Millisecond,
		ReclaimInterval: 5 * time.Second, ReclaimLimit: 100, DrainTimeout: 30 * time.Second,
		OnDeliveryError: func(_ context.Context, delivery broker.Delivery, deliveryErr error) {
			logger.Printf("webui worker delivery degraded request=%q: %v", delivery.Envelope.RequestID, deliveryErr)
		}}

	dispatcher := gateway.BrokerDispatcher{Tasks: tasks, Bindings: configRepo}
	media := preprocess.MediaStager{Fetcher: preprocess.MediaRouter{
		Feishu: feishu.OfficialMediaFetcher{Tokens: feishuCredentials, Client: providerHTTP},
		WeCom:  wecom.OfficialMediaFetcher{Tokens: wecomTokens, Client: providerHTTP},
	}, Malware: malware, DLP: localDisabledDLP{}, Artifacts: artifacts, MaxBytes: 10 << 20}
	preprocessor := preprocess.Worker{Store: preprocessStore, Payloads: payloads, Dispatcher: dispatcher,
		Owner: configValue.instanceName("preprocess"), LeaseTTL: 30 * time.Second, RetryDelay: time.Second, MaxAttempts: 8,
		Media: &media, ArtifactRetention: 24 * time.Hour, Telemetry: telemetryProvider}
	dispatchRelay := relay.DispatchRelay{Outbox: inbox, Tasks: tasks, Broker: streamBroker, Owner: configValue.instanceName("dispatch-relay"),
		ShardCount: 4, ClaimTTL: 30 * time.Second, ClaimRenewInterval: 10 * time.Second, PollInterval: 100 * time.Millisecond,
		Telemetry: telemetryProvider}
	replyRelay := relay.ReplyRelay{Outbox: inbox, Results: payloads, Routes: inbox, Replies: publisher,
		Owner: configValue.instanceName("reply-relay"), ClaimTTL: 30 * time.Second, ClaimRenewInterval: 10 * time.Second, PollInterval: 100 * time.Millisecond,
		Telemetry: telemetryProvider}
	wakeupQueue, err := relayredis.NewWakeupQueue(redis, publisher, relayredis.WakeupQueueConfig{
		Group: "webui-wakeup", ReadBlock: 250 * time.Millisecond, ReclaimIdle: 30 * time.Second})
	if err != nil {
		return errors.New("wakeup queue configuration rejected")
	}
	wakeupRelay := relay.WakeupRelay{Outbox: inbox, Wakeups: publisher, Owner: configValue.instanceName("wakeup-relay"),
		ClaimTTL: 30 * time.Second, ClaimRenewInterval: 10 * time.Second, PollInterval: 100 * time.Millisecond,
		Telemetry: telemetryProvider}
	wakeupDispatcher := relay.WakeupDispatcher{ConsumerID: configValue.instanceName("wakeup"), Wakeups: wakeupQueue,
		Store: tasks, Dispatch: streamBroker, ShardCount: 4, ReclaimInterval: 5 * time.Second, ReclaimLimit: 100}

	replyQueue, err := relayredis.NewReplyQueue(redis, publisher, relayredis.ReplyQueueConfig{
		Group: "webui-delivery", ReadBlock: 250 * time.Millisecond, ReclaimIdle: 30 * time.Second})
	if err != nil {
		return errors.New("reply queue configuration rejected")
	}
	deliveryCatalog, err := deliverypostgres.New(db, adapters...)
	if err != nil {
		return errors.New("delivery catalog configuration rejected")
	}
	deliveryService := channeldelivery.Service{Results: payloads, Ledger: inbox, Adapters: deliveryCatalog,
		Owner: configValue.instanceName("delivery"), ClaimTTL: 30 * time.Second, ClaimRenewInterval: 10 * time.Second,
		DefaultRetryDelay: time.Second, MaxRetryDelay: time.Minute, MaxAttempts: 8, MaxReconcileAttempts: 8}
	deliverySupervisor := channeldelivery.Supervisor{Catalog: deliveryCatalog, RefreshInterval: time.Second,
		NewConsumer: func(destination channel.ReplyDestination) (channeldelivery.ConsumerRunner, error) {
			return channeldelivery.Consumer{Queue: replyQueue, Deliverer: deliveryService, Destination: destination,
				ConsumerID: configValue.instanceName("delivery"), ReclaimInterval: 5 * time.Second, ReclaimLimit: 100,
				Telemetry: telemetryProvider}, nil
		}, OnError: func(supervisorErr error) { logger.Printf("webui delivery degraded: %v", supervisorErr) }}

	mux := http.NewServeMux()
	mux.Handle("/webui", browser)
	mux.Handle("/webui/", browser)
	if feishuEndpoint != nil {
		mux.Handle("/callbacks/feishu", feishuEndpoint)
	}
	if wecomEndpoint != nil {
		mux.Handle("/callbacks/wecom", wecomEndpoint)
	}
	mux.HandleFunc("/livez", func(writer http.ResponseWriter, _ *http.Request) { writer.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		if db.PingContext(request.Context()) != nil || redis.Ping(request.Context()).Err() != nil || malware.Probe(request.Context()) != nil {
			http.Error(writer, "not ready", http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
	})
	server := &http.Server{Addr: configValue.ListenAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}

	processCtx, stop := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stop()
	errorsCh := make(chan error, 8)
	var background sync.WaitGroup
	start := func(name string, operation func(context.Context) error) {
		background.Add(1)
		go func() {
			defer background.Done()
			if operationErr := operation(processCtx); operationErr != nil && !errors.Is(operationErr, context.Canceled) {
				select {
				case errorsCh <- fmt.Errorf("%s stopped: %w", name, operationErr):
				case <-processCtx.Done():
				}
			}
		}()
	}
	start("preprocess", func(ctx context.Context) error {
		return runPreprocessLoop(ctx, preprocessor.RunOnce, 100*time.Millisecond, 100, logger)
	})
	start("progress publisher", progressPublisher.Run)
	start("confirmation expiry reconciler", func(ctx context.Context) error {
		reconciler := governance.ConfirmationExpiryReconciler{Coordinator: governanceStore, BatchSize: 100}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
				if _, expiryErr := reconciler.RunOnce(ctx); expiryErr != nil {
					logger.Printf("webui confirmation expiry degraded: %v", expiryErr)
				}
			}
		}
	})
	start("dispatch relay", func(ctx context.Context) error {
		return runRecoverableLoop(ctx, "webui dispatch relay", dispatchRelay.Run, 250*time.Millisecond, logger)
	})
	start("worker", func(ctx context.Context) error {
		return runRecoverableLoop(ctx, "webui worker", workerConsumer.Run, 250*time.Millisecond, logger)
	})
	start("reply relay", func(ctx context.Context) error {
		return runRecoverableLoop(ctx, "webui reply relay", replyRelay.Run, 250*time.Millisecond, logger)
	})
	start("wakeup relay", func(ctx context.Context) error {
		return runRecoverableLoop(ctx, "webui wakeup relay", wakeupRelay.Run, 250*time.Millisecond, logger)
	})
	start("wakeup dispatcher", func(ctx context.Context) error {
		return runRecoverableLoop(ctx, "webui wakeup dispatcher", wakeupDispatcher.Run, 250*time.Millisecond, logger)
	})
	start("delivery", func(ctx context.Context) error {
		return runRecoverableLoop(ctx, "webui delivery", deliverySupervisor.Run, 250*time.Millisecond, logger)
	})
	start("http", func(context.Context) error { return server.ListenAndServe() })
	logger.Printf("WebUI local node=%q ready: http://localhost%s/webui/ route=%q account=%q model=deepseek",
		configValue.InstanceID, configValue.ListenAddress, configValue.RouteKey, webUILocalAccountID)

	var terminalErr error
	select {
	case <-processCtx.Done():
	case terminalErr = <-errorsCh:
	}
	stop()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelShutdown()
	_ = server.Shutdown(shutdownCtx)
	done := make(chan struct{})
	go func() { background.Wait(); close(done) }()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		if terminalErr == nil {
			terminalErr = errors.New("webui local shutdown timed out")
		}
	}
	return terminalErr
}

// runWebUILocalBootstrap establishes only the disposable local control plane.
// Multi-node Compose starts it once before starting independent local nodes.
func runWebUILocalBootstrap(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || getenv == nil || logger == nil {
		return errors.New("invalid process dependencies")
	}
	configValue, err := loadWebUILocalConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	db, err := sql.Open("pgx", configValue.PostgresDSN)
	if err != nil {
		return errors.New("postgres client initialization failed")
	}
	defer db.Close()
	redis := redisclient.NewClient(&redisclient.Options{Addr: configValue.RedisAddress})
	defer redis.Close()
	if err := db.PingContext(parent); err != nil {
		return errors.New("postgres unavailable")
	}
	if err := redis.Ping(parent).Err(); err != nil {
		return errors.New("redis unavailable")
	}
	if _, err := bootstrapWebUILocal(parent, db, configValue); err != nil {
		return fmt.Errorf("local bootstrap failed: %w", err)
	}
	logger.Printf("WebUI local bootstrap complete instance=%q", configValue.InstanceID)
	return nil
}

func loadWebUILocalConfig(getenv func(string) string) (webUILocalConfig, error) {
	value := webUILocalConfig{PostgresDSN: strings.TrimSpace(getenv("TRPC_POSTGRES_DSN")),
		RedisAddress: strings.TrimSpace(getenv("TRPC_REDIS_ADDRESS")), ListenAddress: valueOr(getenv("TRPC_LISTEN_ADDRESS"), ":8080"),
		RedisEnvironment:        valueOr(getenv("TRPC_REDIS_ENVIRONMENT"), "local-runtime"),
		SecretRoot:              valueOr(getenv("TRPC_WEBUI_LOCAL_SECRET_ROOT"), "/tmp/trpc-webui-secrets"),
		SkillStagingRoot:        valueOr(getenv("TRPC_WEBUI_LOCAL_SKILL_STAGING_ROOT"), "/tmp/trpc-webui-skills"),
		QdrantEndpoint:          valueOr(getenv("TRPC_WEBUI_LOCAL_QDRANT_ENDPOINT"), "http://qdrant:6333"),
		APIKeyFile:              valueOr(getenv("TRPC_WEBUI_DEEPSEEK_KEY_FILE"), "/run/secrets/deepseek_api_key"),
		RouteKey:                valueOr(getenv("TRPC_WEBUI_LOCAL_ROUTE_KEY"), webUILocalRouteKey),
		Token:                   valueOr(getenv("TRPC_WEBUI_LOCAL_TOKEN"), webUILocalToken),
		InstanceID:              valueOr(getenv("TRPC_WEBUI_LOCAL_INSTANCE_ID"), "standalone"),
		ClamAVAddress:           valueOr(getenv("TRPC_WEBUI_LOCAL_CLAMAV_ADDRESS"), "clamav:3310"),
		ExclusiveRuntime:        strings.EqualFold(strings.TrimSpace(getenv("TRPC_WEBUI_LOCAL_EXCLUSIVE_RUNTIME")), "true"),
		FeishuEnabled:           strings.EqualFold(strings.TrimSpace(getenv("TRPC_FEISHU_LOCAL_ENABLED")), "true"),
		FeishuAppID:             strings.TrimSpace(getenv("FEISHU_APP_ID")),
		FeishuAppSecret:         strings.TrimSpace(getenv("FEISHU_APP_SECRET")),
		FeishuVerificationToken: strings.TrimSpace(getenv("FEISHU_VERIFICATION_TOKEN")),
		FeishuEncryptKey:        strings.TrimSpace(getenv("FEISHU_ENCRYPT_KEY")),
		FeishuBotOpenID:         strings.TrimSpace(getenv("FEISHU_BOT_OPEN_ID")),
		WeComEnabled:            strings.EqualFold(strings.TrimSpace(getenv("TRPC_WECOM_LOCAL_ENABLED")), "true"),
		WeComCorpID:             strings.TrimSpace(getenv("WECOM_CORP_ID")),
		WeComAppSecret:          strings.TrimSpace(getenv("WECOM_APP_SECRET")),
		WeComCallbackToken:      strings.TrimSpace(getenv("WECOM_CALLBACK_TOKEN")),
		WeComEncodingAESKey:     strings.TrimSpace(getenv("WECOM_ENCODING_AES_KEY")),
	}
	if value.PostgresDSN == "" || value.RedisAddress == "" || strings.TrimSpace(value.Token) != value.Token || len(value.Token) < 16 ||
		strings.TrimSpace(value.RouteKey) != value.RouteKey || value.RouteKey == "" || strings.TrimSpace(value.ClamAVAddress) != value.ClamAVAddress || value.ClamAVAddress == "" || !filepath.IsAbs(value.APIKeyFile) || !filepath.IsAbs(value.SecretRoot) ||
		!filepath.IsAbs(value.SkillStagingRoot) || filepath.Clean(value.SkillStagingRoot) != value.SkillStagingRoot || value.SkillStagingRoot == value.SecretRoot || value.QdrantEndpoint != "http://qdrant:6333" {
		return webUILocalConfig{}, errors.New("required WebUI local configuration is missing or invalid")
	}
	if !validWebUILocalInstanceID(value.InstanceID) {
		return webUILocalConfig{}, errors.New("WebUI local instance ID is invalid")
	}
	if value.FeishuEnabled && (value.FeishuAppID == "" || value.FeishuAppSecret == "" || value.FeishuVerificationToken == "" ||
		value.FeishuEncryptKey == "") {
		return webUILocalConfig{}, errors.New("Feishu local configuration is incomplete")
	}
	if value.WeComEnabled {
		agentID, agentIDErr := strconv.ParseInt(strings.TrimSpace(getenv("WECOM_AGENT_ID")), 10, 64)
		if agentIDErr != nil || agentID <= 0 || value.WeComCorpID == "" || value.WeComAppSecret == "" || value.WeComCallbackToken == "" ||
			value.WeComEncodingAESKey == "" {
			return webUILocalConfig{}, errors.New("WeCom local configuration is incomplete")
		}
		value.WeComAgentID = agentID
	}
	return value, nil
}

func validWebUILocalInstanceID(value string) bool {
	if len(value) == 0 || len(value) > 48 || strings.TrimSpace(value) != value {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
			return false
		}
	}
	return true
}

func (value webUILocalConfig) instanceName(component string) string {
	return "webui-local-" + component + "-" + value.InstanceID
}

// acquireWebUILocalRuntimeLock prevents the standalone local compositions
// from concurrently claiming one shared PostgreSQL/Redis work queue. The
// multi-node profile deliberately leaves ExclusiveRuntime unset because it
// uses explicit, distinct node identities and its own Compose project.
func acquireWebUILocalRuntimeLock(ctx context.Context, db *sql.DB, enabled bool) (func(), error) {
	if !enabled {
		return func() {}, nil
	}
	if ctx == nil || db == nil {
		return nil, errors.New("invalid local runtime lock dependencies")
	}
	connection, err := db.Conn(ctx)
	if err != nil {
		return nil, errors.New("local runtime lock unavailable")
	}
	var acquired bool
	if err := connection.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, webUILocalRuntimeLockKey).Scan(&acquired); err != nil {
		connection.Close()
		return nil, errors.New("local runtime lock unavailable")
	}
	if !acquired {
		connection.Close()
		return nil, errors.New("another standalone local composition is already running; stop webui-local, feishu-local, or wecom-local before starting this profile")
	}
	return func() {
		_, _ = connection.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtext($1))`, webUILocalRuntimeLockKey)
		_ = connection.Close()
	}, nil
}

func bootstrapWebUILocal(ctx context.Context, db *sql.DB, configValue webUILocalConfig) (webUILocalBootstrap, error) {
	if err := migrations.NewRunner(db).Up(ctx); err != nil {
		return webUILocalBootstrap{}, err
	}
	apiKey, err := os.ReadFile(configValue.APIKeyFile)
	if err != nil || strings.TrimSpace(string(apiKey)) == "" || strings.ContainsRune(strings.TrimSpace(string(apiKey)), '\n') {
		return webUILocalBootstrap{}, errors.New("DeepSeek API key file is missing or invalid")
	}
	apiKey = []byte(strings.TrimSpace(string(apiKey)))
	defer clear(apiKey)
	if err := os.MkdirAll(configValue.SecretRoot, 0o700); err != nil {
		return webUILocalBootstrap{}, err
	}
	if err := os.Chmod(configValue.SecretRoot, 0o700); err != nil {
		return webUILocalBootstrap{}, err
	}
	// Skills are staged through the same immutable catalog as the production
	// Worker. Keep their content root distinct from the secret projection; a
	// local WebUI runtime must never make credentials discoverable as Skill
	// files, even in a disposable Compose volume.
	if err := os.MkdirAll(configValue.SkillStagingRoot, 0o700); err != nil {
		return webUILocalBootstrap{}, err
	}
	if err := os.Chmod(configValue.SkillStagingRoot, 0o700); err != nil {
		return webUILocalBootstrap{}, err
	}

	catalog, err := provider.NewCatalog(provider.DeepSeekModelSchema(), provider.FakeModelSchema(), provider.FakeEmbeddingSchema(), provider.OpenAIEmbeddingSchema(), provider.PostgresBackendSchema(), provider.PostgresBackendSchemaV2(), provider.RedisMemoryBackendSchema(), provider.InMemoryBackendSchema(), provider.Mem0MemoryBackendSchema(), provider.QdrantVectorSchema(), provider.LocalQdrantVectorSchema())
	if err != nil {
		return webUILocalBootstrap{}, err
	}
	tenants := tenantpostgres.New(db)
	apps := agentpostgres.New(db)
	configs := configpostgres.New(db, tenants)
	providers := providerpostgres.New(db, catalog)
	governanceStore := governancepostgres.New(db)
	root, err := tenants.Get(ctx, webUILocalTenantID)
	if errors.Is(err, tenant.ErrNotFound) {
		metadata := tenant.ChangeMetadata{ActorType: "system", ActorID: "webui-local", ReasonCode: "local_bootstrap",
			CorrelationID: "webui-local", TraceID: "webui-local"}
		root, err = tenants.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: webUILocalTenantID,
			TenantKey: "webui-local", DisplayName: "WebUI Local"}, ChangeMetadata: metadata})
		if err != nil {
			return webUILocalBootstrap{}, err
		}
		_, err = providers.PublishModel(ctx, provider.ModelProfileSnapshot{TenantID: webUILocalTenantID,
			ProfileID: webUILocalModelID, ProfileKey: "deepseek-local", DisplayName: "DeepSeek Local", Status: "active",
			SchemaVersion: 1, Provider: "deepseek", Model: webUILocalModelName, Endpoint: "https://api.deepseek.com",
			SecretRef: secrets.SecretRef{Ref: "secret://local/deepseek", Version: 1}, Version: webUILocalModelVersion})
		if err != nil {
			return webUILocalBootstrap{}, err
		}
		policy := governance.PolicyV1{SchemaVersion: 1, DefaultAction: governance.ActionAllow,
			AllowedModels: []governance.VersionedRef{{ID: webUILocalModelID, Version: webUILocalModelVersion}},
			Tools:         []governance.ToolRule{{ToolID: localnote.ID, Version: localnote.Version, Dangerous: true, ConfirmationSupported: true}},
			InputDLP:      governance.DLPDisabled, OutputDLP: governance.DLPDisabled}
		policyDigest, _, digestErr := governance.PolicyDigest(policy)
		if digestErr != nil {
			return webUILocalBootstrap{}, digestErr
		}
		if err = governanceStore.PublishPolicy(ctx, governance.PolicySnapshot{TenantID: webUILocalTenantID, Version: 1, SchemaVersion: 1,
			Policy: policy, ContentDigest: policyDigest, PublishedAt: time.Now().UTC()}); err != nil {
			return webUILocalBootstrap{}, err
		}
		appMetadata := agentapp.ChangeMetadata{ActorType: "system", ActorID: "webui-local", Reason: "local_bootstrap",
			CorrelationID: "webui-local", TraceID: "webui-local"}
		app, createErr := apps.Create(ctx, agentapp.CreateInput{App: agentapp.AgentApp{TenantID: webUILocalTenantID,
			AgentAppID: webUILocalAppID, AgentAppKey: "assistant", DisplayName: "WebUI Assistant"}, ChangeMetadata: appMetadata})
		if createErr != nil {
			return webUILocalBootstrap{}, createErr
		}
		draft, draftErr := apps.CreateDraft(ctx, agentapp.CreateDraftInput{TenantID: webUILocalTenantID,
			AgentAppID: webUILocalAppID, ExpectedAppVersion: app.Version,
			Revision: agentapp.Revision{AgentKind: agentapp.AgentKindLLM, Instruction: webUILocalInstruction,
				ModelProfileID: webUILocalModelID, ModelProfileVersion: webUILocalModelVersion,
				ToolRefs: []agentapp.VersionedRef{{ID: localnote.ID, Version: localnote.Version, Required: true}}}, ChangeMetadata: appMetadata})
		if draftErr != nil {
			return webUILocalBootstrap{}, draftErr
		}
		if _, publishErr := apps.Publish(ctx, agentapp.PublishInput{TenantID: webUILocalTenantID, AgentAppID: webUILocalAppID,
			Revision: draft.Revision, ExpectedAppVersion: app.Version + 1, ExpectedDraftVersion: draft.DraftVersion,
			ChangeMetadata: appMetadata}); publishErr != nil {
			return webUILocalBootstrap{}, publishErr
		}
		published, publishErr := configs.Publish(ctx, configdomain.PublishInput{TenantID: webUILocalTenantID,
			ExpectedTenantVersion: root.Version, Metadata: metadata,
			Payload: configdomain.ConfigV1{SchemaVersion: 1, DefaultAgentAppID: webUILocalAppID, PolicyVersion: 1,
				ChannelBindings: []configdomain.ChannelBinding{{BindingID: webUILocalBindingID, Channel: "webui",
					ExternalAccountID: webUILocalAccountID, AgentAppID: webUILocalAppID,
					SecretRef: secrets.SecretRef{Ref: "secret://local/webui-verify", Version: 1}}}}})
		if publishErr != nil {
			return webUILocalBootstrap{}, publishErr
		}
		root = published.Tenant
	} else if err != nil {
		return webUILocalBootstrap{}, err
	}
	snapshot, err := configs.GetCurrent(ctx, webUILocalTenantID)
	if err != nil {
		return webUILocalBootstrap{}, err
	}
	if err = ensureWebUILocalModel(ctx, providers); err != nil {
		return webUILocalBootstrap{}, err
	}
	if err = ensureWebUILocalMemoryBackend(ctx, providers); err != nil {
		return webUILocalBootstrap{}, err
	}
	skillRef, knowledgeRef, err := ensureWebUILocalKnowledgeFixture(ctx, db, providers, configValue)
	if err != nil {
		return webUILocalBootstrap{}, err
	}
	root, snapshot, err = ensureWebUILocalToolControlPlane(ctx, tenants, apps, configs, governanceStore, root, snapshot, webUILocalCapabilityRefs{skill: skillRef, knowledge: knowledgeRef})
	if err != nil {
		return webUILocalBootstrap{}, err
	}
	root, snapshot, err = ensureWebUILocalFeishuBinding(ctx, configs, root, snapshot, configValue)
	if err != nil {
		return webUILocalBootstrap{}, err
	}
	root, snapshot, err = ensureWebUILocalWeComBinding(ctx, configs, root, snapshot, configValue)
	if err != nil {
		return webUILocalBootstrap{}, err
	}
	var binding configdomain.ChannelBinding
	for _, candidate := range snapshot.Payload.ChannelBindings {
		if candidate.BindingID == webUILocalBindingID && candidate.Channel == "webui" {
			binding = candidate
			break
		}
	}
	if binding.BindingID == "" || binding.ExternalAccountID != webUILocalAccountID {
		return webUILocalBootstrap{}, errors.New("existing local control plane is incompatible; recreate the Compose volume")
	}
	var feishuBinding configdomain.ChannelBinding
	if configValue.FeishuEnabled {
		for _, candidate := range snapshot.Payload.ChannelBindings {
			if candidate.BindingID == feishuLocalBindingID && candidate.Channel == "feishu" {
				feishuBinding = candidate
				break
			}
		}
		if feishuBinding.BindingID == "" || feishuBinding.ExternalAccountID != configValue.FeishuAppID {
			return webUILocalBootstrap{}, errors.New("existing Feishu local control plane is incompatible; recreate the Compose volume")
		}
	}
	var wecomBinding configdomain.ChannelBinding
	if configValue.WeComEnabled {
		for _, candidate := range snapshot.Payload.ChannelBindings {
			if candidate.BindingID == wecomLocalBindingID && candidate.Channel == "wecom" {
				wecomBinding = candidate
				break
			}
		}
		if wecomBinding.BindingID == "" || wecomBinding.ExternalAccountID != configValue.WeComCorpID {
			return webUILocalBootstrap{}, errors.New("existing WeCom local control plane is incompatible; recreate the Compose volume")
		}
	}
	secretValues := []struct {
		scope secrets.Scope
		ref   secrets.SecretRef
		value []byte
	}{
		{payloadkey.Scope(webUILocalTenantID, 1), secrets.SecretRef{Ref: payloadKeyRef, Version: 1}, deriveLocalSecret("payload", configValue.Token)},
		{secrets.Scope{TenantID: webUILocalTenantID, Subject: webUILocalBindingID, Purpose: secrets.PurposeChannelVerify,
			ResourceID: webUILocalBindingID, ResourceVersion: snapshot.ConfigVersion}, binding.SecretRef,
			[]byte(fmt.Sprintf(`{"token":%q,"external_account_id":%q}`, configValue.Token, webUILocalAccountID))},
		{secrets.Scope{TenantID: webUILocalTenantID, Subject: webUILocalTenantID, Purpose: secrets.PurposeTenantIdentity,
			ResourceID: webUILocalTenantID, ResourceVersion: 1}, secrets.SecretRef{Ref: "secret://local/identity", Version: 1}, deriveLocalSecret("identity", configValue.Token)},
		{secrets.Scope{TenantID: webUILocalTenantID, Subject: webUILocalTenantID, Purpose: secrets.PurposeTenantSession,
			ResourceID: webUILocalTenantID, ResourceVersion: 1}, secrets.SecretRef{Ref: "secret://local/session", Version: 1}, deriveLocalSecret("session", configValue.Token)},
		{secrets.Scope{TenantID: webUILocalTenantID, Subject: "worker-model", Purpose: secrets.PurposeModelCall,
			ResourceID: webUILocalModelID, ResourceVersion: webUILocalModelVersion}, secrets.SecretRef{Ref: "secret://local/deepseek", Version: 1}, apiKey},
		{secrets.Scope{TenantID: webUILocalTenantID, Subject: "worker-knowledge-qdrant", Purpose: secrets.PurposeBackendConnect,
			ResourceID: webUILocalQdrantID, ResourceVersion: 1}, secrets.SecretRef{Ref: "secret://local/qdrant", Version: 1}, []byte("webui-local-qdrant-token")},
	}
	if configValue.FeishuEnabled {
		secretValues = append(secretValues,
			struct {
				scope secrets.Scope
				ref   secrets.SecretRef
				value []byte
			}{scope: secrets.Scope{TenantID: webUILocalTenantID, Subject: feishuLocalBindingID, Purpose: secrets.PurposeChannelVerify,
				ResourceID: feishuLocalBindingID, ResourceVersion: snapshot.ConfigVersion}, ref: feishuBinding.SecretRef,
				value: []byte(fmt.Sprintf(`{"encrypt_key":%q,"verification_token":%q,"app_id":%q,"bot_open_id":%q}`,
					configValue.FeishuEncryptKey, configValue.FeishuVerificationToken, configValue.FeishuAppID, configValue.FeishuBotOpenID))},
			struct {
				scope secrets.Scope
				ref   secrets.SecretRef
				value []byte
			}{scope: secrets.Scope{TenantID: webUILocalTenantID, Subject: feishuLocalBindingID, Purpose: secrets.PurposeChannelSend,
				ResourceID: feishuLocalBindingID, ResourceVersion: snapshot.ConfigVersion}, ref: feishuBinding.SendSecretRef,
				value: []byte(fmt.Sprintf(`{"app_id":%q,"app_secret":%q}`, configValue.FeishuAppID, configValue.FeishuAppSecret))},
		)
	}
	if configValue.WeComEnabled {
		secretValues = append(secretValues,
			struct {
				scope secrets.Scope
				ref   secrets.SecretRef
				value []byte
			}{scope: secrets.Scope{TenantID: webUILocalTenantID, Subject: wecomLocalBindingID, Purpose: secrets.PurposeChannelVerify,
				ResourceID: wecomLocalBindingID, ResourceVersion: snapshot.ConfigVersion}, ref: wecomBinding.SecretRef,
				value: []byte(fmt.Sprintf(`{"token":%q,"encoding_aes_key":%q,"receive_id":%q,"agent_id":%d}`,
					configValue.WeComCallbackToken, configValue.WeComEncodingAESKey, configValue.WeComCorpID, configValue.WeComAgentID))},
			struct {
				scope secrets.Scope
				ref   secrets.SecretRef
				value []byte
			}{scope: secrets.Scope{TenantID: webUILocalTenantID, Subject: wecomLocalBindingID, Purpose: secrets.PurposeChannelSend,
				ResourceID: wecomLocalBindingID, ResourceVersion: snapshot.ConfigVersion}, ref: wecomBinding.SendSecretRef,
				value: []byte(fmt.Sprintf(`{"corp_id":%q,"corp_secret":%q,"agent_id":%d}`,
					configValue.WeComCorpID, configValue.WeComAppSecret, configValue.WeComAgentID))},
		)
	}
	for _, item := range secretValues {
		if err := writeLocalSecret(configValue.SecretRoot, item.scope, item.ref, item.value); err != nil {
			return webUILocalBootstrap{}, err
		}
	}
	secretStore, err := secretfs.New(configValue.SecretRoot, 64<<10)
	if err != nil {
		return webUILocalBootstrap{}, err
	}
	payloadResolver, err := payloadkey.New(secretStore, payloadKeyRef)
	if err != nil {
		return webUILocalBootstrap{}, err
	}
	route := ingress.BindingRoute{OpaqueBindingID: "webui-local-binding-v1", Channel: "webui",
		RouteKeyDigest: webui.RouteKeyDigest(configValue.RouteKey), TenantID: webUILocalTenantID, AgentAppID: webUILocalAppID,
		ChannelBindingID: webUILocalBindingID, ExternalAccountID: webUILocalAccountID, TenantVersion: root.Version,
		BindingVersion: snapshot.ConfigVersion, SecretRef: binding.SecretRef,
		IdentitySecretRef: secrets.SecretRef{Ref: "secret://local/identity", Version: 1},
		SessionSecretRef:  secrets.SecretRef{Ref: "secret://local/session", Version: 1}, Enabled: true}
	if err := ingresspostgres.New(db).PutBindingRoute(ctx, route); err != nil {
		return webUILocalBootstrap{}, err
	}
	var feishuRoute ingress.BindingRoute
	if configValue.FeishuEnabled {
		feishuRoute = ingress.BindingRoute{OpaqueBindingID: "feishu-local-binding-v1", Channel: "feishu",
			RouteKeyDigest: feishuprotocol.RouteKeyDigest(feishuLocalRouteKey), TenantID: webUILocalTenantID, AgentAppID: webUILocalAppID,
			ChannelBindingID: feishuLocalBindingID, ExternalAccountID: configValue.FeishuAppID, TenantVersion: root.Version,
			BindingVersion: snapshot.ConfigVersion, SecretRef: feishuBinding.SecretRef,
			IdentitySecretRef: secrets.SecretRef{Ref: "secret://local/identity", Version: 1},
			SessionSecretRef:  secrets.SecretRef{Ref: "secret://local/session", Version: 1}, Enabled: true}
		if err := ingresspostgres.New(db).PutBindingRoute(ctx, feishuRoute); err != nil {
			return webUILocalBootstrap{}, err
		}
	}
	var wecomRoute ingress.BindingRoute
	if configValue.WeComEnabled {
		wecomRoute = ingress.BindingRoute{OpaqueBindingID: "wecom-local-binding-v1", Channel: "wecom",
			RouteKeyDigest: wecomprotocol.RouteKeyDigest(wecomLocalRouteKey), TenantID: webUILocalTenantID, AgentAppID: webUILocalAppID,
			ChannelBindingID: wecomLocalBindingID, ExternalAccountID: configValue.WeComCorpID, TenantVersion: root.Version,
			BindingVersion: snapshot.ConfigVersion, SecretRef: wecomBinding.SecretRef,
			IdentitySecretRef: secrets.SecretRef{Ref: "secret://local/identity", Version: 1},
			SessionSecretRef:  secrets.SecretRef{Ref: "secret://local/session", Version: 1}, Enabled: true}
		if err := ingresspostgres.New(db).PutBindingRoute(ctx, wecomRoute); err != nil {
			return webUILocalBootstrap{}, err
		}
	}
	return webUILocalBootstrap{Tenant: root, Config: snapshot, Route: route, SecretRoot: configValue.SecretRoot,
		FeishuRoute: feishuRoute, WeComRoute: wecomRoute, PayloadKey: payloadResolver, SecretStore: secretStore, ProviderRepo: providers}, nil
}

// ensureWebUILocalModel keeps the one local, capability-complete model
// profile immutable and exact. Version 2 is retained as the stable profile
// version so existing local Docker volumes continue to run without a reset.
func ensureWebUILocalModel(ctx context.Context, providers *providerpostgres.Repository) error {
	if ctx == nil || providers == nil {
		return errors.New("invalid WebUI local model repository")
	}
	current, err := providers.GetModel(ctx, webUILocalTenantID, webUILocalModelID, webUILocalModelVersion)
	if err == nil {
		if current.Provider != "deepseek" || current.Model != webUILocalModelName || current.Endpoint != "https://api.deepseek.com" ||
			current.SecretRef != (secrets.SecretRef{Ref: "secret://local/deepseek", Version: 1}) {
			return errors.New("WebUI local model revision is incompatible")
		}
		return nil
	}
	if !errors.Is(err, runtime.ErrNotFound) {
		return err
	}
	_, err = providers.PublishModel(ctx, provider.ModelProfileSnapshot{
		TenantID: webUILocalTenantID, ProfileID: webUILocalModelID, ProfileKey: "deepseek-local", DisplayName: "DeepSeek Local", Status: "active",
		SchemaVersion: 1, Provider: "deepseek", Model: webUILocalModelName, Endpoint: "https://api.deepseek.com",
		SecretRef: secrets.SecretRef{Ref: "secret://local/deepseek", Version: 1}, Version: webUILocalModelVersion,
	})
	return err
}

// ensureWebUILocalMemoryBackend keeps the local fixture explicit: even its
// one PostgreSQL volume is selected by a published, credential-free backend
// profile rather than by an execution-role special case.
func ensureWebUILocalMemoryBackend(ctx context.Context, providers *providerpostgres.Repository) error {
	if ctx == nil || providers == nil {
		return errors.New("invalid WebUI local memory repository")
	}
	current, err := providers.GetBackend(ctx, webUILocalTenantID, webUILocalMemoryID, 1)
	if err == nil {
		if current.Provider != "postgres" || current.SchemaVersion != 2 || current.Status != "active" ||
			current.Configuration["connection_id"] != "default" || !current.Capabilities["strong_ryw"] ||
			current.CredentialRef.Ref != "" || current.CredentialRef.Version != 0 {
			return errors.New("WebUI local memory backend is incompatible")
		}
		return nil
	}
	if !errors.Is(err, runtime.ErrNotFound) {
		return err
	}
	_, err = providers.PublishBackend(ctx, provider.BackendProfileSnapshot{
		TenantID: webUILocalTenantID, ProfileID: webUILocalMemoryID, ProfileKey: "webui-local-memory",
		DisplayName: "WebUI Local Memory", Status: "active", SchemaVersion: 2, Provider: "postgres",
		Configuration: map[string]string{"connection_id": "default"}, Capabilities: provider.CapabilitySet{
			"atomic_turn_commit": true, "strong_ryw": true, "summary_cas": true,
		}, Version: 1,
	})
	return err
}

// ensureWebUILocalKnowledgeFixture publishes one real, small Knowledge
// version and one Skill package through the same durable authorities used by a
// Worker. The chat model remains the explicit local DeepSeek choice; only the
// embedding is deterministic so retrieval adds no second cloud credential.
func ensureWebUILocalKnowledgeFixture(ctx context.Context, db *sql.DB, providers *providerpostgres.Repository, value webUILocalConfig) (agentapp.SkillRef, agentapp.VersionedRef, error) {
	if ctx == nil || db == nil || providers == nil || value.QdrantEndpoint != "http://qdrant:6333" {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, errors.New("invalid WebUI local Knowledge fixture")
	}
	packageRoot := filepath.Join(value.SkillStagingRoot, webUILocalTenantID, "webui-local-v1")
	skillRoot := filepath.Join(packageRoot, webUILocalSkillID)
	if err := os.MkdirAll(skillRoot, 0o700); err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	skillContent := []byte("---\nname: webui_local_guide\ndescription: Local WebUI Knowledge demo guidance\n---\nUse knowledge search when the user asks about the local demo guide.\n")
	skillFile := filepath.Join(skillRoot, "SKILL.md")
	if existing, err := os.ReadFile(skillFile); err == nil && !bytes.Equal(existing, skillContent) {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, errors.New("WebUI local Skill fixture changed; recreate the Compose volume")
	} else if errors.Is(err, os.ErrNotExist) {
		if err = os.WriteFile(skillFile, skillContent, 0o600); err != nil {
			return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
		}
	} else if err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	digest, err := serviceskill.DigestRoot(packageRoot)
	if err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	skills := skillpostgres.New(db)
	pkg, err := skills.Stage(ctx, serviceskill.Package{TenantID: webUILocalTenantID, SkillID: webUILocalSkillID, Version: webUILocalSkillVersion, ContentDigest: digest, RelativePath: "webui-local-v1"})
	if err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	if _, err = skills.Publish(ctx, pkg.TenantID, pkg.SkillID, pkg.Version); err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}

	if _, err = providers.PublishModel(ctx, provider.ModelProfileSnapshot{TenantID: webUILocalTenantID, ProfileID: webUILocalEmbedderID, ProfileKey: "webui-local-fake-embedder", DisplayName: "WebUI Local Fake Embedder", Status: "active", SchemaVersion: 1, Provider: "fake-embedding", Model: "fake-embedding-v1", Options: map[string]string{"dimensions": strconv.Itoa(webUILocalVectorSize)}, Version: 1}); err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	if _, err = providers.PublishBackend(ctx, provider.BackendProfileSnapshot{TenantID: webUILocalTenantID, ProfileID: webUILocalQdrantID, ProfileKey: "webui-local-qdrant", DisplayName: "WebUI Local Qdrant", Status: "active", SchemaVersion: 1, Provider: "qdrant-local", Configuration: map[string]string{"endpoint": value.QdrantEndpoint, "collection": webUILocalQdrantCollection, "vector_size": strconv.Itoa(webUILocalVectorSize), "snapshot_watermark": "webui-local-snapshot-v1", "vector_generation": webUILocalVectorGeneration}, CredentialRef: secrets.SecretRef{Ref: "secret://local/qdrant", Version: 1}, Capabilities: provider.CapabilitySet{"tenant_filter": true, "idempotent_upsert": true, "migration_dual_write": true}, Version: 1}); err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}

	manifests := knowledgepostgres.New(db)
	if manifest, getErr := manifests.GetManifest(ctx, webUILocalTenantID, webUILocalKnowledgeID, webUILocalKnowledgeVersion); getErr == nil {
		if manifest.State != serviceknowledge.ManifestPublished {
			return agentapp.SkillRef{}, agentapp.VersionedRef{}, errors.New("WebUI local Knowledge fixture is not published")
		}
		return agentapp.SkillRef{ID: pkg.SkillID, Version: pkg.Version, ContentDigest: pkg.ContentDigest}, agentapp.VersionedRef{ID: webUILocalKnowledgeID, Version: webUILocalKnowledgeVersion}, nil
	} else if !errors.Is(getErr, runtime.ErrNotFound) {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, getErr
	}
	if err = ensureWebUILocalQdrantCollection(ctx, value.QdrantEndpoint, webUILocalQdrantCollection); err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	text := "The local WebUI Knowledge demo is published through an immutable manifest and retrieved from tenant-scoped Qdrant."
	sourceDigest := localFixtureDigest(text)
	embedder := serviceknowledge.DeterministicEmbedder{Dimensions: webUILocalVectorSize}
	vector64, err := embedder.GetEmbedding(ctx, text)
	if err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	vector := make([]float32, len(vector64))
	for i := range vector {
		vector[i] = float32(vector64[i])
	}
	now := time.Now().UTC()
	if _, err = manifests.BeginManifest(ctx, serviceknowledge.BeginManifestInput{TenantID: webUILocalTenantID, KnowledgeID: webUILocalKnowledgeID, Version: webUILocalKnowledgeVersion, SourceURI: "fixture://webui-local-guide", SourceDigest: sourceDigest, ChunkingPipelineVersion: "webui-local-v1", EmbedderProfileID: webUILocalEmbedderID, EmbedderVersion: 1, VectorCollectionGeneration: webUILocalVectorGeneration, MetadataSchema: []string{"title"}, ContentWatermark: "webui-local-snapshot-v1", CreatedAt: now}); err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	metadata := map[string]string{"title": "WebUI Local Guide"}
	metadataDigest, err := localFixtureMetadataDigest(metadata)
	if err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	chunk := serviceknowledge.ChunkRecord{TenantID: webUILocalTenantID, KnowledgeID: webUILocalKnowledgeID, KnowledgeVersion: webUILocalKnowledgeVersion, ChunkID: "guide", SourceDigest: sourceDigest, ContentDigest: localFixtureDigest(text), MetadataDigest: metadataDigest, EmbeddingProfileID: webUILocalEmbedderID, EmbeddingVersion: 1, VectorGeneration: webUILocalVectorGeneration, Content: text, Metadata: metadata, Vector: vector, CreatedAt: now}
	if _, err = manifests.StageChunk(ctx, chunk); err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	if _, err = manifests.BeginIndexing(ctx, webUILocalTenantID, webUILocalKnowledgeID, webUILocalKnowledgeVersion, 1, now.Add(time.Second)); err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	adapter, err := serviceqdrant.New(serviceqdrant.Config{Endpoint: value.QdrantEndpoint, Collection: webUILocalQdrantCollection, VectorSize: webUILocalVectorSize, SnapshotWatermark: "webui-local-snapshot-v1", VectorGeneration: webUILocalVectorGeneration, AllowInsecureHTTP: true}, nil)
	if err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	imageDigest, err := chunk.MutationDigest()
	if err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	if _, err = adapter.ApplyChunk(ctx, knowledgedriver.ApplyRequest{TenantID: webUILocalTenantID, MigrationID: "webui-local-fixture", MutationID: "guide-v1", Epoch: 1, Image: chunk.ChunkImage(), ImageDigest: imageDigest}); err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	if err = manifests.MarkChunkIndexed(ctx, webUILocalTenantID, webUILocalKnowledgeID, webUILocalKnowledgeVersion, "guide", now.Add(2*time.Second)); err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	verification, err := serviceknowledge.VerificationDigest([]serviceknowledge.ChunkRecord{chunk})
	if err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	if _, err = manifests.BeginVerifying(ctx, webUILocalTenantID, webUILocalKnowledgeID, webUILocalKnowledgeVersion, verification, now.Add(3*time.Second)); err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	if _, err = manifests.RecordProbe(ctx, serviceknowledge.ProbeRecord{TenantID: webUILocalTenantID, KnowledgeID: webUILocalKnowledgeID, KnowledgeVersion: webUILocalKnowledgeVersion, ProbeID: "guide", Query: "local WebUI Knowledge demo", ExpectedChunks: []string{"guide"}, MinRecallPPM: 1_000_000, CreatedAt: now.Add(4 * time.Second)}); err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	if err = manifests.MarkProbeVerified(ctx, webUILocalTenantID, webUILocalKnowledgeID, webUILocalKnowledgeVersion, "guide"); err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	if _, err = manifests.PublishVersion(ctx, webUILocalTenantID, webUILocalKnowledgeID, webUILocalKnowledgeVersion, now.Add(5*time.Second)); err != nil {
		return agentapp.SkillRef{}, agentapp.VersionedRef{}, err
	}
	return agentapp.SkillRef{ID: pkg.SkillID, Version: pkg.Version, ContentDigest: pkg.ContentDigest}, agentapp.VersionedRef{ID: webUILocalKnowledgeID, Version: webUILocalKnowledgeVersion}, nil
}

func ensureWebUILocalQdrantCollection(ctx context.Context, endpoint, collection string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint+"/collections/"+collection, bytes.NewBufferString(fmt.Sprintf(`{"vectors":{"size":%d,"distance":"Dot"}}`, webUILocalVectorSize)))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return errors.New("WebUI local Qdrant collection unavailable")
	}
	return nil
}

func localFixtureDigest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

// localFixtureMetadataDigest uses the exact canonical representation checked
// by knowledge ingestion and migration images. Human-readable shorthand (for
// example "title=value") would stage successfully nowhere and must not be
// used as an integrity digest.
func localFixtureMetadataDigest(value map[string]string) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return localFixtureDigest(string(encoded)), nil
}

func webUILocalKnowledgeBindingReady(values []configdomain.BackendBinding) bool {
	for _, value := range values {
		if value.Domain == "knowledge" && value.BackendProfileID == webUILocalQdrantID && value.BackendVersion == 1 {
			return true
		}
	}
	return false
}

func webUILocalMemoryBindingReady(values []configdomain.BackendBinding) bool {
	for _, value := range values {
		if value.Domain == "memory" && value.BackendProfileID == webUILocalMemoryID && value.BackendVersion == 1 &&
			requiresCapability(value.Required, "strong_ryw") {
			return true
		}
	}
	return false
}

func webUILocalArtifactBindingReady(values []configdomain.BackendBinding) bool {
	for _, value := range values {
		if value.Domain == "artifact" && value.BackendProfileID == webUILocalMemoryID && value.BackendVersion == 1 &&
			requiresCapability(value.Required, "strong_ryw") {
			return true
		}
	}
	return false
}

func requiresCapability(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func upsertWebUILocalKnowledgeBinding(values []configdomain.BackendBinding) []configdomain.BackendBinding {
	result := make([]configdomain.BackendBinding, 0, len(values)+1)
	for _, value := range values {
		if value.Domain != "knowledge" {
			result = append(result, value)
		}
	}
	return append(result, configdomain.BackendBinding{Domain: "knowledge", BackendProfileID: webUILocalQdrantID, BackendVersion: 1, Required: []string{"tenant_filter"}})
}

func upsertWebUILocalMemoryBinding(values []configdomain.BackendBinding) []configdomain.BackendBinding {
	result := make([]configdomain.BackendBinding, 0, len(values)+1)
	for _, value := range values {
		if value.Domain != "memory" {
			result = append(result, value)
		}
	}
	return append(result, configdomain.BackendBinding{Domain: "memory", BackendProfileID: webUILocalMemoryID, BackendVersion: 1, Required: []string{"strong_ryw"}})
}

func upsertWebUILocalArtifactBinding(values []configdomain.BackendBinding) []configdomain.BackendBinding {
	result := make([]configdomain.BackendBinding, 0, len(values)+1)
	for _, value := range values {
		if value.Domain != "artifact" {
			result = append(result, value)
		}
	}
	return append(result, configdomain.BackendBinding{Domain: "artifact", BackendProfileID: webUILocalMemoryID, BackendVersion: 1, Required: []string{"strong_ryw"}})
}

type webUILocalPolicyStore interface {
	GetPolicy(context.Context, string, int64) (governance.PolicySnapshot, error)
	PublishPolicy(context.Context, governance.PolicySnapshot) error
}

type webUILocalCapabilityRefs struct {
	skill     agentapp.SkillRef
	knowledge agentapp.VersionedRef
}

func ensureWebUILocalToolControlPlane(ctx context.Context, tenants tenant.Repository, apps agentapp.Repository,
	configs configdomain.Repository, policies webUILocalPolicyStore, root tenant.Tenant, snapshot configdomain.Snapshot, wanted ...webUILocalCapabilityRefs,
) (tenant.Tenant, configdomain.Snapshot, error) {
	if ctx == nil || tenants == nil || apps == nil || configs == nil || policies == nil || root.TenantID != webUILocalTenantID ||
		snapshot.TenantID != webUILocalTenantID || snapshot.Payload.PolicyVersion < 1 {
		return tenant.Tenant{}, configdomain.Snapshot{}, errors.New("invalid WebUI local control plane")
	}
	app, err := apps.Get(ctx, webUILocalTenantID, webUILocalAppID)
	if err != nil {
		return tenant.Tenant{}, configdomain.Snapshot{}, err
	}
	revision, err := apps.GetRevision(ctx, webUILocalTenantID, webUILocalAppID, app.CurrentRevision)
	if err != nil {
		return tenant.Tenant{}, configdomain.Snapshot{}, err
	}
	policy, err := policies.GetPolicy(ctx, webUILocalTenantID, snapshot.Payload.PolicyVersion)
	if err != nil {
		return tenant.Tenant{}, configdomain.Snapshot{}, err
	}
	appMetadata := agentapp.ChangeMetadata{ActorType: "system", ActorID: "webui-local", Reason: "local_graph_upgrade",
		CorrelationID: "webui-local-graph", TraceID: "webui-local-graph"}
	refs := webUILocalCapabilityRefs{}
	if len(wanted) > 0 {
		refs = wanted[0]
	}
	childRevision, err := ensureWebUILocalGraphChild(ctx, apps, appMetadata, refs)
	if err != nil {
		return tenant.Tenant{}, configdomain.Snapshot{}, err
	}
	if !webUILocalGraphRevisionReady(revision, childRevision) {
		revision = agentapp.Revision{AgentKind: agentapp.AgentKindGraph, AgentSpec: agentapp.AgentSpecV1{
			Nodes: []agentapp.AgentNodeSpecV1{{Key: "assistant", FailurePolicy: agentapp.FailurePolicyFailFast,
				AgentRef: agentapp.PublishedAgentRef{AgentAppID: webUILocalChildAppID, Revision: childRevision.Revision,
					ContentDigest: childRevision.ContentDigest}}},
			EntryNode: "assistant", MaxConcurrency: 1,
			Checkpoint: agentapp.CheckpointPolicyV1{Required: true, Namespace: "webui-local"},
		}}
		draft, createErr := apps.CreateDraft(ctx, agentapp.CreateDraftInput{TenantID: webUILocalTenantID,
			AgentAppID: webUILocalAppID, ExpectedAppVersion: app.Version, Revision: revision, ChangeMetadata: appMetadata})
		if createErr != nil {
			return tenant.Tenant{}, configdomain.Snapshot{}, createErr
		}
		if _, publishErr := apps.Publish(ctx, agentapp.PublishInput{TenantID: webUILocalTenantID, AgentAppID: webUILocalAppID,
			Revision: draft.Revision, ExpectedAppVersion: app.Version + 1, ExpectedDraftVersion: draft.DraftVersion,
			ChangeMetadata: appMetadata}); publishErr != nil {
			return tenant.Tenant{}, configdomain.Snapshot{}, publishErr
		}
	}
	if webUILocalPolicyReady(policy.Policy) && webUILocalMemoryBindingReady(snapshot.Payload.BackendBindings) && webUILocalArtifactBindingReady(snapshot.Payload.BackendBindings) &&
		(len(wanted) == 0 || webUILocalKnowledgeBindingReady(snapshot.Payload.BackendBindings)) {
		return root, snapshot, nil
	}
	if policy.Version == int64(^uint64(0)>>1) {
		return tenant.Tenant{}, configdomain.Snapshot{}, errors.New("WebUI local policy version exhausted")
	}
	policy.Policy.DefaultAction = governance.ActionAllow
	policy.Policy.AllowedModels = upsertWebUILocalModelRef(policy.Policy.AllowedModels)
	policy.Policy.Tools = upsertWebUILocalToolRule(policy.Policy.Tools)
	policy.Version++
	policy.PublishedAt = time.Now().UTC()
	policy.ContentDigest, _, err = governance.PolicyDigest(policy.Policy)
	if err != nil {
		return tenant.Tenant{}, configdomain.Snapshot{}, err
	}
	if err = policies.PublishPolicy(ctx, policy); err != nil {
		return tenant.Tenant{}, configdomain.Snapshot{}, err
	}
	payload := snapshot.Payload
	payload.PolicyVersion = policy.Version
	payload.BackendBindings = upsertWebUILocalMemoryBinding(payload.BackendBindings)
	payload.BackendBindings = upsertWebUILocalArtifactBinding(payload.BackendBindings)
	if len(wanted) > 0 {
		payload.BackendBindings = upsertWebUILocalKnowledgeBinding(payload.BackendBindings)
	}
	metadata := tenant.ChangeMetadata{ActorType: "system", ActorID: "webui-local", ReasonCode: "local_tool_upgrade",
		CorrelationID: "webui-local-tool", TraceID: "webui-local-tool"}
	published, err := configs.Publish(ctx, configdomain.PublishInput{TenantID: webUILocalTenantID,
		ExpectedTenantVersion: root.Version, Payload: payload, Metadata: metadata})
	if err != nil {
		return tenant.Tenant{}, configdomain.Snapshot{}, err
	}
	return published.Tenant, published.Snapshot, nil
}

// ensureWebUILocalFeishuBinding extends the disposable local snapshot only
// when the explicit feishu-local profile is enabled. The production channel
// bindings remain immutable snapshots; a credential change therefore requires
// a fresh local Compose volume instead of mutating an already-published ref.
func ensureWebUILocalFeishuBinding(ctx context.Context, configs configdomain.Repository, root tenant.Tenant,
	snapshot configdomain.Snapshot, value webUILocalConfig,
) (tenant.Tenant, configdomain.Snapshot, error) {
	if !value.FeishuEnabled {
		return root, snapshot, nil
	}
	if ctx == nil || configs == nil || root.TenantID != webUILocalTenantID || snapshot.TenantID != webUILocalTenantID ||
		value.FeishuAppID == "" || value.FeishuAppSecret == "" || value.FeishuVerificationToken == "" || value.FeishuEncryptKey == "" {
		return tenant.Tenant{}, configdomain.Snapshot{}, errors.New("invalid Feishu local control plane")
	}
	payload := snapshot.Payload
	for index, binding := range payload.ChannelBindings {
		if binding.BindingID != feishuLocalBindingID {
			continue
		}
		if binding.Channel != "feishu" || binding.AgentAppID != webUILocalAppID ||
			binding.SecretRef != (secrets.SecretRef{Ref: "secret://local/feishu-verify", Version: 1}) ||
			binding.SendSecretRef != (secrets.SecretRef{Ref: "secret://local/feishu-send", Version: 1}) {
			return tenant.Tenant{}, configdomain.Snapshot{}, errors.New("existing Feishu local binding is incompatible")
		}
		if binding.ExternalAccountID == value.FeishuAppID {
			return root, snapshot, nil
		}
		// App IDs are routing identities, not secret material. Publish a new
		// snapshot instead of overwriting history so a local credential rotation
		// keeps exact-version delivery and old callbacks fail closed.
		payload.ChannelBindings[index].ExternalAccountID = value.FeishuAppID
		published, err := configs.Publish(ctx, configdomain.PublishInput{TenantID: webUILocalTenantID, ExpectedTenantVersion: root.Version,
			Payload: payload, Metadata: tenant.ChangeMetadata{ActorType: "system", ActorID: "feishu-local", ReasonCode: "local_feishu_app_rotation",
				CorrelationID: "feishu-local-app-rotation", TraceID: "feishu-local-app-rotation"}})
		if err != nil {
			return tenant.Tenant{}, configdomain.Snapshot{}, err
		}
		return published.Tenant, published.Snapshot, nil
	}
	payload.ChannelBindings = append(payload.ChannelBindings, configdomain.ChannelBinding{BindingID: feishuLocalBindingID, Channel: "feishu",
		ExternalAccountID: value.FeishuAppID, AgentAppID: webUILocalAppID,
		SecretRef:     secrets.SecretRef{Ref: "secret://local/feishu-verify", Version: 1},
		SendSecretRef: secrets.SecretRef{Ref: "secret://local/feishu-send", Version: 1}})
	published, err := configs.Publish(ctx, configdomain.PublishInput{TenantID: webUILocalTenantID, ExpectedTenantVersion: root.Version,
		Payload: payload, Metadata: tenant.ChangeMetadata{ActorType: "system", ActorID: "feishu-local", ReasonCode: "local_feishu_binding",
			CorrelationID: "feishu-local-binding", TraceID: "feishu-local-binding"}})
	if err != nil {
		return tenant.Tenant{}, configdomain.Snapshot{}, err
	}
	return published.Tenant, published.Snapshot, nil
}

// ensureWebUILocalWeComBinding extends the disposable local snapshot only
// when the explicit wecom-local profile is enabled. Corp IDs are routing
// identities, so a changed value is represented by a new snapshot version;
// the callback and send credentials always remain scoped secret material.
func ensureWebUILocalWeComBinding(ctx context.Context, configs configdomain.Repository, root tenant.Tenant,
	snapshot configdomain.Snapshot, value webUILocalConfig,
) (tenant.Tenant, configdomain.Snapshot, error) {
	if !value.WeComEnabled {
		return root, snapshot, nil
	}
	if ctx == nil || configs == nil || root.TenantID != webUILocalTenantID || snapshot.TenantID != webUILocalTenantID ||
		value.WeComCorpID == "" || value.WeComAppSecret == "" || value.WeComCallbackToken == "" || value.WeComEncodingAESKey == "" || value.WeComAgentID <= 0 {
		return tenant.Tenant{}, configdomain.Snapshot{}, errors.New("invalid WeCom local control plane")
	}
	payload := snapshot.Payload
	for index, binding := range payload.ChannelBindings {
		if binding.BindingID != wecomLocalBindingID {
			continue
		}
		if binding.Channel != "wecom" || binding.AgentAppID != webUILocalAppID ||
			binding.SecretRef != (secrets.SecretRef{Ref: "secret://local/wecom-verify", Version: 1}) ||
			binding.SendSecretRef != (secrets.SecretRef{Ref: "secret://local/wecom-send", Version: 1}) {
			return tenant.Tenant{}, configdomain.Snapshot{}, errors.New("existing WeCom local binding is incompatible")
		}
		if binding.ExternalAccountID == value.WeComCorpID {
			return root, snapshot, nil
		}
		payload.ChannelBindings[index].ExternalAccountID = value.WeComCorpID
		published, err := configs.Publish(ctx, configdomain.PublishInput{TenantID: webUILocalTenantID, ExpectedTenantVersion: root.Version,
			Payload: payload, Metadata: tenant.ChangeMetadata{ActorType: "system", ActorID: "wecom-local", ReasonCode: "local_wecom_corp_rotation",
				CorrelationID: "wecom-local-corp-rotation", TraceID: "wecom-local-corp-rotation"}})
		if err != nil {
			return tenant.Tenant{}, configdomain.Snapshot{}, err
		}
		return published.Tenant, published.Snapshot, nil
	}
	payload.ChannelBindings = append(payload.ChannelBindings, configdomain.ChannelBinding{BindingID: wecomLocalBindingID, Channel: "wecom",
		ExternalAccountID: value.WeComCorpID, AgentAppID: webUILocalAppID,
		SecretRef:     secrets.SecretRef{Ref: "secret://local/wecom-verify", Version: 1},
		SendSecretRef: secrets.SecretRef{Ref: "secret://local/wecom-send", Version: 1}})
	published, err := configs.Publish(ctx, configdomain.PublishInput{TenantID: webUILocalTenantID, ExpectedTenantVersion: root.Version,
		Payload: payload, Metadata: tenant.ChangeMetadata{ActorType: "system", ActorID: "wecom-local", ReasonCode: "local_wecom_binding",
			CorrelationID: "wecom-local-binding", TraceID: "wecom-local-binding"}})
	if err != nil {
		return tenant.Tenant{}, configdomain.Snapshot{}, err
	}
	return published.Tenant, published.Snapshot, nil
}

func ensureWebUILocalGraphChild(ctx context.Context, apps agentapp.Repository, metadata agentapp.ChangeMetadata, wanted webUILocalCapabilityRefs) (agentapp.Revision, error) {
	app, err := apps.Get(ctx, webUILocalTenantID, webUILocalChildAppID)
	if errors.Is(err, agentapp.ErrNotFound) {
		app, err = apps.Create(ctx, agentapp.CreateInput{App: agentapp.AgentApp{TenantID: webUILocalTenantID,
			AgentAppID: webUILocalChildAppID, AgentAppKey: "assistant-llm", DisplayName: "WebUI Assistant LLM"}, ChangeMetadata: metadata})
	}
	if err != nil {
		return agentapp.Revision{}, err
	}
	if app.CurrentRevision > 0 {
		current, currentErr := apps.GetRevision(ctx, webUILocalTenantID, webUILocalChildAppID, app.CurrentRevision)
		if currentErr != nil {
			return agentapp.Revision{}, currentErr
		}
		if webUILocalLLMRevisionReady(current, wanted) {
			return current, nil
		}
	}
	draft, err := apps.CreateDraft(ctx, agentapp.CreateDraftInput{TenantID: webUILocalTenantID,
		AgentAppID: webUILocalChildAppID, ExpectedAppVersion: app.Version,
		Revision: agentapp.Revision{AgentKind: agentapp.AgentKindLLM, Instruction: webUILocalInstruction,
			ModelProfileID: webUILocalModelID, ModelProfileVersion: webUILocalModelVersion,
			ToolRefs:  []agentapp.VersionedRef{{ID: localnote.ID, Version: localnote.Version, Required: true}},
			SkillRefs: optionalWebUILocalSkillRefs(wanted), KnowledgeRefs: optionalWebUILocalKnowledgeRefs(wanted)}, ChangeMetadata: metadata})
	if err != nil {
		return agentapp.Revision{}, err
	}
	published, err := apps.Publish(ctx, agentapp.PublishInput{TenantID: webUILocalTenantID, AgentAppID: webUILocalChildAppID,
		Revision: draft.Revision, ExpectedAppVersion: app.Version + 1, ExpectedDraftVersion: draft.DraftVersion,
		ChangeMetadata: metadata})
	if err != nil {
		return agentapp.Revision{}, err
	}
	return published.Revision, nil
}

func webUILocalLLMRevisionReady(value agentapp.Revision, wanted ...webUILocalCapabilityRefs) bool {
	if value.AgentKind != agentapp.AgentKindLLM || value.Instruction != webUILocalInstruction ||
		value.ModelProfileID != webUILocalModelID || value.ModelProfileVersion != webUILocalModelVersion {
		return false
	}
	for _, ref := range value.ToolRefs {
		if ref.ID == localnote.ID {
			if ref.Version != localnote.Version || !ref.Required {
				return false
			}
			if len(wanted) == 0 || (wanted[0].skill.ID == "" && wanted[0].knowledge.ID == "") {
				return true
			}
			return len(value.SkillRefs) == 1 && value.SkillRefs[0] == wanted[0].skill && len(value.KnowledgeRefs) == 1 && value.KnowledgeRefs[0].ID == wanted[0].knowledge.ID && value.KnowledgeRefs[0].Version == wanted[0].knowledge.Version
		}
	}
	return false
}

func optionalWebUILocalSkillRefs(value webUILocalCapabilityRefs) []agentapp.SkillRef {
	if value.skill.ID == "" {
		return nil
	}
	return []agentapp.SkillRef{value.skill}
}
func optionalWebUILocalKnowledgeRefs(value webUILocalCapabilityRefs) []agentapp.VersionedRef {
	if value.knowledge.ID == "" {
		return nil
	}
	return []agentapp.VersionedRef{value.knowledge}
}

func webUILocalGraphRevisionReady(value, child agentapp.Revision) bool {
	if value.AgentKind != agentapp.AgentKindGraph || len(value.AgentSpec.Nodes) != 1 ||
		value.AgentSpec.EntryNode != "assistant" || value.AgentSpec.MaxConcurrency != 1 ||
		!value.AgentSpec.Checkpoint.Required || value.AgentSpec.Checkpoint.Namespace != "webui-local" {
		return false
	}
	node := value.AgentSpec.Nodes[0]
	return node.Key == "assistant" && node.FailurePolicy == agentapp.FailurePolicyFailFast &&
		node.AgentRef.AgentAppID == webUILocalChildAppID && node.AgentRef.Revision == child.Revision &&
		node.AgentRef.ContentDigest == child.ContentDigest
}

func webUILocalPolicyReady(value governance.PolicyV1) bool {
	if value.DefaultAction != governance.ActionAllow || value.InputDLP != governance.DLPDisabled || value.OutputDLP != governance.DLPDisabled {
		return false
	}
	modelAllowed := false
	for _, ref := range value.AllowedModels {
		if ref.ID == webUILocalModelID && ref.Version == webUILocalModelVersion {
			modelAllowed = true
			break
		}
	}
	if !modelAllowed {
		return false
	}
	for _, rule := range value.Tools {
		if rule.ToolID == localnote.ID {
			return rule.Version == localnote.Version && rule.Dangerous && rule.ConfirmationSupported
		}
	}
	return false
}

func upsertWebUILocalModelRef(values []governance.VersionedRef) []governance.VersionedRef {
	result := make([]governance.VersionedRef, 0, len(values)+1)
	for _, ref := range values {
		if ref.ID != webUILocalModelID {
			result = append(result, ref)
		}
	}
	return append(result, governance.VersionedRef{ID: webUILocalModelID, Version: webUILocalModelVersion})
}

func upsertWebUILocalToolRule(values []governance.ToolRule) []governance.ToolRule {
	result := append([]governance.ToolRule(nil), values...)
	for index := range result {
		if result[index].ToolID == localnote.ID {
			result[index] = governance.ToolRule{ToolID: localnote.ID, Version: localnote.Version, Dangerous: true, ConfirmationSupported: true}
			return result
		}
	}
	return append(result, governance.ToolRule{ToolID: localnote.ID, Version: localnote.Version, Dangerous: true, ConfirmationSupported: true})
}

func deriveLocalSecret(kind, token string) []byte {
	value := sha256.Sum256([]byte("trpc-webui-local-v1\x00" + kind + "\x00" + token))
	return append([]byte(nil), value[:]...)
}

func writeLocalSecret(root string, scope secrets.Scope, ref secrets.SecretRef, value []byte) error {
	name, err := secretfs.StableFilename(scope, ref)
	if err != nil {
		return err
	}
	path := filepath.Join(root, name)
	if existing, readErr := os.ReadFile(path); readErr == nil {
		if !bytes.Equal(existing, value) {
			return errors.New("local secret generation changed; recreate the WebUI container")
		}
		return nil
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	return os.WriteFile(path, value, 0o600)
}
