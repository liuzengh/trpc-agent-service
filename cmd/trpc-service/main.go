package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	artifactcos "github.com/liuzengh/trpc-agent-service/trpcservice/artifact/cos"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	channelsattachments "github.com/liuzengh/trpc-agent-service/trpcservice/channels/attachments"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/ingress"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

const (
	envRole                             = "TRPC_AGENT_SERVICE_ROLE"
	envPostgresDSN                      = "TRPC_AGENT_SERVICE_POSTGRES_DSN"
	envRedisURL                         = "TRPC_AGENT_SERVICE_REDIS_URL"
	envRedisStream                      = "TRPC_AGENT_SERVICE_REDIS_STREAM"
	envRedisGroup                       = "TRPC_AGENT_SERVICE_REDIS_GROUP"
	envDispatcherID                     = "TRPC_AGENT_SERVICE_DISPATCHER_ID"
	envHTTPAddr                         = "TRPC_AGENT_SERVICE_HTTP_ADDR"
	envWorkerID                         = "TRPC_AGENT_SERVICE_WORKER_ID"
	envAdminToken                       = "TRPC_AGENT_SERVICE_ADMIN_TOKEN"
	envOperatorToken                    = "TRPC_AGENT_SERVICE_OPERATOR_TOKEN"
	envOperatorTenantIDs                = "TRPC_AGENT_SERVICE_OPERATOR_TENANT_IDS"
	envAuditorToken                     = "TRPC_AGENT_SERVICE_AUDITOR_TOKEN"
	envAuditorTenantIDs                 = "TRPC_AGENT_SERVICE_AUDITOR_TENANT_IDS"
	envTencentDBGateways                = "TRPC_AGENT_SERVICE_TENCENTDB_GATEWAYS"
	envShutdownTimeout                  = "TRPC_AGENT_SERVICE_SHUTDOWN_TIMEOUT"
	envCOSEndpoints                     = "TRPC_AGENT_SERVICE_COS_ENDPOINTS"
	envQdrantEndpoints                  = "TRPC_AGENT_SERVICE_QDRANT_ENDPOINTS"
	envOTELProtocol                     = "TRPC_AGENT_SERVICE_OTEL_PROTOCOL"
	envOTELTracesEndpoint               = "TRPC_AGENT_SERVICE_OTEL_TRACES_ENDPOINT"
	envOTELMetricsEndpoint              = "TRPC_AGENT_SERVICE_OTEL_METRICS_ENDPOINT"
	envModelPricing                     = "TRPC_AGENT_SERVICE_MODEL_PRICING"
	envModelTimeout                     = "TRPC_AGENT_SERVICE_MODEL_TIMEOUT"
	envWorkerConcurrency                = "TRPC_AGENT_SERVICE_WORKER_CONCURRENCY"
	envFaultPauseAfterClaim             = "TRPC_AGENT_SERVICE_FAULT_PAUSE_AFTER_CLAIM"
	envFaultPauseAfterMigrationCopy     = "TRPC_AGENT_SERVICE_FAULT_PAUSE_AFTER_MIGRATION_COPY"
	envFaultPauseAfterMigrationCopyItem = "TRPC_AGENT_SERVICE_FAULT_PAUSE_AFTER_MIGRATION_COPY_ITEM"
	envArtifactRetention                = "TRPC_AGENT_SERVICE_ARTIFACT_RETENTION"
	envJaegerURL                        = "TRPC_AGENT_SERVICE_JAEGER_URL"
	envPrometheusURL                    = "TRPC_AGENT_SERVICE_PROMETHEUS_URL"
	envGrafanaURL                       = "TRPC_AGENT_SERVICE_GRAFANA_URL"
	envIMReplyMode                      = "TRPC_AGENT_SERVICE_IM_REPLY_MODE"
	envAdmissionRateLimit               = "TRPC_AGENT_SERVICE_ADMISSION_RATE_LIMIT"
	envAdmissionRateWindow              = "TRPC_AGENT_SERVICE_ADMISSION_RATE_WINDOW"
	envAdmissionConcurrency             = "TRPC_AGENT_SERVICE_ADMISSION_CONCURRENCY"
	envHTTPEventWaitTimeout             = "TRPC_AGENT_SERVICE_HTTP_EVENT_WAIT_TIMEOUT"

	defaultHTTPAddr             = ":8080"
	defaultRedisStream          = "trpc-agent-service:dispatch"
	defaultRedisGroup           = "workers"
	dispatchLeaseDuration       = 30 * time.Second
	sessionLeaseDuration        = 30 * time.Second
	defaultShutdownTimeout      = 30 * time.Second
	defaultModelTimeout         = time.Minute
	defaultWorkerConcurrency    = 4
	defaultArtifactRetention    = 30 * 24 * time.Hour
	defaultJaegerURL            = "http://localhost:16686"
	defaultPrometheusURL        = "http://localhost:19090"
	defaultGrafanaURL           = "http://localhost:13000"
	defaultAdmissionRateLimit   = 60
	defaultAdmissionRateWindow  = time.Minute
	defaultAdmissionConcurrency = 64
	defaultHTTPEventWaitTimeout = 2 * time.Minute
	dataMigrationLease          = 30 * time.Second
	dataMigrationPoll           = time.Second
)

var errWorkerShutdownTimeout = errors.New("worker did not stop before shutdown deadline")

type serviceRole string

const (
	roleGateway serviceRole = "gateway"
	roleChannel serviceRole = "channel"
	roleWorker  serviceRole = "worker"
	roleAll     serviceRole = "all"
)

type serviceConfig struct {
	Role                        serviceRole
	PostgresDSN                 string
	RedisURL                    string
	RedisStream                 string
	RedisGroup                  string
	DispatcherID                string
	HTTPAddr                    string
	WorkerID                    string
	AdminToken                  string
	OperatorToken               string
	OperatorTenantIDs           []string
	AuditorToken                string
	AuditorTenantIDs            []string
	ShutdownTimeout             time.Duration
	ModelTimeout                time.Duration
	ArtifactRetention           time.Duration
	WorkerConcurrency           int
	AdmissionRateLimit          int
	AdmissionRateWindow         time.Duration
	AdmissionConcurrency        int
	HTTPEventWaitTimeout        time.Duration
	PauseAfterClaim             time.Duration
	PauseAfterMigrationCopy     time.Duration
	PauseAfterMigrationCopyItem time.Duration
	JaegerURL                   string
	PrometheusURL               string
	GrafanaURL                  string
	IMReplyMode                 worker.ReplyMode
	Telemetry                   platformtelemetry.Config
	Pricing                     platformmetrics.PricingCatalog
}

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		fmt.Fprintf(os.Stderr, "usage: %s\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "roles: gateway (HTTP/Admin/relay), channel (single IM owner), worker, all (local combined)")
		fmt.Fprintln(os.Stderr, "required: TRPC_AGENT_SERVICE_ROLE, TRPC_AGENT_SERVICE_POSTGRES_DSN, TRPC_AGENT_SERVICE_REDIS_URL")
		fmt.Fprintln(os.Stderr, "worker role also requires TRPC_AGENT_SERVICE_WORKER_ID")
		return
	}

	config, err := configFromEnvironment(os.Getenv)
	if err != nil {
		log.Print(platformlog.SafeError(err))
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runService(ctx, config); err != nil {
		log.Print(platformlog.SafeError(err))
		os.Exit(1)
	}
}

func configFromEnvironment(getenv func(string) string) (serviceConfig, error) {
	if getenv == nil {
		return serviceConfig{}, errors.New("environment reader is required")
	}
	config := serviceConfig{
		Role:                 serviceRole(getenv(envRole)),
		PostgresDSN:          getenv(envPostgresDSN),
		RedisURL:             getenv(envRedisURL),
		RedisStream:          getenv(envRedisStream),
		RedisGroup:           getenv(envRedisGroup),
		DispatcherID:         getenv(envDispatcherID),
		HTTPAddr:             getenv(envHTTPAddr),
		WorkerID:             getenv(envWorkerID),
		AdminToken:           getenv(envAdminToken),
		OperatorToken:        getenv(envOperatorToken),
		OperatorTenantIDs:    splitCommaSeparated(getenv(envOperatorTenantIDs)),
		AuditorToken:         getenv(envAuditorToken),
		AuditorTenantIDs:     splitCommaSeparated(getenv(envAuditorTenantIDs)),
		ShutdownTimeout:      defaultShutdownTimeout,
		ModelTimeout:         defaultModelTimeout,
		ArtifactRetention:    defaultArtifactRetention,
		WorkerConcurrency:    defaultWorkerConcurrency,
		AdmissionRateLimit:   defaultAdmissionRateLimit,
		AdmissionRateWindow:  defaultAdmissionRateWindow,
		AdmissionConcurrency: defaultAdmissionConcurrency,
		HTTPEventWaitTimeout: defaultHTTPEventWaitTimeout,
		JaegerURL:            defaultJaegerURL,
		PrometheusURL:        defaultPrometheusURL,
		GrafanaURL:           defaultGrafanaURL,
		IMReplyMode:          worker.ReplyMode(getenv(envIMReplyMode)),
		Telemetry: platformtelemetry.Config{
			Protocol:       getenv(envOTELProtocol),
			TraceEndpoint:  getenv(envOTELTracesEndpoint),
			MetricEndpoint: getenv(envOTELMetricsEndpoint),
		},
	}
	if config.Role != roleGateway && config.Role != roleChannel && config.Role != roleWorker && config.Role != roleAll {
		return serviceConfig{}, fmt.Errorf("%s must be gateway, channel, worker, or all", envRole)
	}
	if config.PostgresDSN == "" {
		return serviceConfig{}, fmt.Errorf("%s is required", envPostgresDSN)
	}
	if config.RedisURL == "" {
		return serviceConfig{}, fmt.Errorf("%s is required", envRedisURL)
	}
	if config.RedisStream == "" {
		config.RedisStream = defaultRedisStream
	}
	if config.RedisGroup == "" {
		config.RedisGroup = defaultRedisGroup
	}
	if config.HTTPAddr == "" {
		config.HTTPAddr = defaultHTTPAddr
	}
	if config.IMReplyMode == "" {
		config.IMReplyMode = worker.ReplyModeText
	}
	if err := config.IMReplyMode.Validate(); err != nil {
		return serviceConfig{}, fmt.Errorf("%s: %w", envIMReplyMode, err)
	}
	if config.Role.runsWorker() && config.WorkerID == "" {
		return serviceConfig{}, fmt.Errorf("%s is required for worker role", envWorkerID)
	}
	if config.Role.runsGateway() && config.AdminToken == "" {
		return serviceConfig{}, fmt.Errorf("%s is required for gateway role", envAdminToken)
	}
	if config.Role.runsGateway() && config.DispatcherID == "" {
		return serviceConfig{}, fmt.Errorf("%s is required for gateway role", envDispatcherID)
	}
	if config.Role.runsGateway() {
		if err := config.adminAuthConfig().Validate(); err != nil {
			return serviceConfig{}, fmt.Errorf("admin authentication: %w", err)
		}
	}
	if value := getenv(envShutdownTimeout); value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return serviceConfig{}, fmt.Errorf("%s must be a positive duration", envShutdownTimeout)
		}
		config.ShutdownTimeout = duration
	}
	if value := getenv(envModelTimeout); value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return serviceConfig{}, fmt.Errorf("%s must be a positive duration", envModelTimeout)
		}
		config.ModelTimeout = duration
	}
	if value := getenv(envArtifactRetention); value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil || duration < 0 {
			return serviceConfig{}, fmt.Errorf("%s must be a non-negative duration", envArtifactRetention)
		}
		config.ArtifactRetention = duration
	}
	if value := getenv(envWorkerConcurrency); value != "" {
		concurrency, err := strconv.Atoi(value)
		if err != nil || concurrency <= 0 {
			return serviceConfig{}, fmt.Errorf("%s must be a positive integer", envWorkerConcurrency)
		}
		config.WorkerConcurrency = concurrency
	}
	if value := getenv(envAdmissionRateLimit); value != "" {
		limit, err := strconv.Atoi(value)
		if err != nil || limit <= 0 {
			return serviceConfig{}, fmt.Errorf("%s must be a positive integer", envAdmissionRateLimit)
		}
		config.AdmissionRateLimit = limit
	}
	if value := getenv(envAdmissionRateWindow); value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 || duration.Milliseconds() <= 0 {
			return serviceConfig{}, fmt.Errorf("%s must be a positive duration", envAdmissionRateWindow)
		}
		config.AdmissionRateWindow = duration
	}
	if value := getenv(envAdmissionConcurrency); value != "" {
		concurrency, err := strconv.Atoi(value)
		if err != nil || concurrency <= 0 {
			return serviceConfig{}, fmt.Errorf("%s must be a positive integer", envAdmissionConcurrency)
		}
		config.AdmissionConcurrency = concurrency
	}
	if value := getenv(envHTTPEventWaitTimeout); value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return serviceConfig{}, fmt.Errorf("%s must be a positive duration", envHTTPEventWaitTimeout)
		}
		config.HTTPEventWaitTimeout = duration
	}
	if value := getenv(envFaultPauseAfterClaim); value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return serviceConfig{}, fmt.Errorf("%s must be a positive duration", envFaultPauseAfterClaim)
		}
		config.PauseAfterClaim = duration
	}
	if value := getenv(envFaultPauseAfterMigrationCopy); value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return serviceConfig{}, fmt.Errorf("%s must be a positive duration", envFaultPauseAfterMigrationCopy)
		}
		config.PauseAfterMigrationCopy = duration
	}
	if value := getenv(envFaultPauseAfterMigrationCopyItem); value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return serviceConfig{}, fmt.Errorf("%s must be a positive duration", envFaultPauseAfterMigrationCopyItem)
		}
		config.PauseAfterMigrationCopyItem = duration
	}
	if value := getenv(envJaegerURL); value != "" {
		config.JaegerURL = value
	}
	if value := getenv(envPrometheusURL); value != "" {
		config.PrometheusURL = value
	}
	if value := getenv(envGrafanaURL); value != "" {
		config.GrafanaURL = value
	}
	pricing, err := platformmetrics.ParsePricingJSON(getenv(envModelPricing))
	if err != nil {
		return serviceConfig{}, err
	}
	config.Pricing = pricing
	return config, nil
}

func (r serviceRole) runsWorker() bool {
	return r == roleWorker || r == roleAll
}

func (r serviceRole) runsGateway() bool {
	return r == roleGateway || r == roleAll
}

func (r serviceRole) runsChannel() bool {
	return r == roleChannel || r == roleAll
}

func runService(ctx context.Context, config serviceConfig) (serviceErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	config.Telemetry.ServiceName = "trpc-agent-service"
	config.Telemetry.ServiceVersion = trpcservice.Version
	telemetryRuntime, telemetryErr := platformtelemetry.Start(ctx, config.Telemetry)
	if telemetryErr != nil {
		log.Printf("telemetry initialization failed: %s", platformlog.SafeError(telemetryErr))
		telemetryRuntime = platformtelemetry.NewNoop(ctx, config.Telemetry.ServiceName)
	}
	defer func() {
		if shutdownResourceCloseSkipped(serviceErr) {
			return
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		serviceErr = errors.Join(serviceErr, telemetryRuntime.Close(shutdownCtx))
	}()
	metricsRecorder, metricsErr := platformmetrics.New(telemetryRuntime.MeterProvider, config.Pricing)
	if metricsErr != nil {
		log.Printf("metrics initialization failed: %s", platformlog.SafeError(metricsErr))
	}
	pool, err := pgxpool.New(ctx, config.PostgresDSN)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer func() {
		if !shutdownResourceCloseSkipped(serviceErr) {
			pool.Close()
		}
	}()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	secrets, err := platformsecret.NewConfiguredProvider(os.Getenv)
	if err != nil {
		return fmt.Errorf("configure secret backend: %w", err)
	}
	hasher, err := platformsecret.NewExternalIDHasher(secrets, "v1")
	if err != nil {
		return err
	}
	protector, err := platformsecret.NewAEADTargetProtector(secrets, "v1")
	if err != nil {
		return err
	}
	store, err := postgres.New(
		pool,
		postgres.WithChannelIdentityMapping(hasher, protector, []string{"v1"}),
		postgres.WithMetrics(metricsRecorder),
	)
	if err != nil {
		return err
	}
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate postgres: %w", err)
	}
	cosEndpoints := environmentCOSEndpointResolver{getenv: os.Getenv}
	artifacts, err := artifactcos.NewResolver(secrets, cosEndpoints)
	if err != nil {
		return err
	}
	defer func() {
		if !shutdownResourceCloseSkipped(serviceErr) {
			serviceErr = errors.Join(serviceErr, artifacts.Close())
		}
	}()
	redisClient, err := platformredis.NewClient(ctx, config.RedisURL)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() {
		if !shutdownResourceCloseSkipped(serviceErr) {
			serviceErr = errors.Join(serviceErr, redisClient.Close())
		}
	}()
	stream, err := platformredis.NewStream(redisClient, config.RedisStream, config.RedisGroup, dispatchLeaseDuration)
	if err != nil {
		return err
	}
	if err := stream.Init(ctx); err != nil {
		return err
	}
	var ingressHandler, adminHandler http.Handler
	var wecomAdapter *wecom.Adapter
	var feishuAdapter *feishu.Adapter
	if config.Role.runsGateway() || config.Role.runsChannel() {
		admissionLimiter, limiterErr := platformredis.NewAdmissionRateLimiter(
			redisClient, config.AdmissionRateLimit, config.AdmissionRateWindow,
		)
		if limiterErr != nil {
			return fmt.Errorf("create admission rate limiter: %w", limiterErr)
		}
		gatewayHandler, wecom, feishu, handlerErr := newGatewayHandler(
			store, artifacts, secrets, admissionLimiter, config.AdmissionConcurrency, config.HTTPEventWaitTimeout,
		)
		if handlerErr != nil {
			return handlerErr
		}
		wecomAdapter, feishuAdapter = wecom, feishu
		if config.Role.runsGateway() {
			ingressHandler = gatewayHandler
		}
		if config.Role.runsGateway() {
			adminHandler, err = newAdminHandler(store, config.adminAuthConfig(), serviceOperationsReader{
				store: store, redis: redisClient,
				jaegerURL: config.JaegerURL, prometheusURL: config.PrometheusURL, grafanaURL: config.GrafanaURL,
			})
			if err != nil {
				return err
			}
		}
	}

	server, err := startServiceServer(config.HTTPAddr, ingressHandler, adminHandler, func(checkCtx context.Context) error {
		if err := pool.Ping(checkCtx); err != nil {
			if metricsRecorder != nil {
				metricsRecorder.SetBackendReadiness("postgres", false)
			}
			return fmt.Errorf("ping postgres: %w", err)
		}
		if metricsRecorder != nil {
			metricsRecorder.SetBackendReadiness("postgres", true)
		}
		redisErr := redisClient.Ping(checkCtx)
		if metricsRecorder != nil {
			metricsRecorder.SetBackendReadiness("redis", redisErr == nil)
		}
		return redisErr
	})
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		serviceErr = errors.Join(serviceErr, server.shutdown(shutdownCtx))
	}()

	var replies *replyRuntime
	if config.Role.runsChannel() {
		replies, err = newReplyRuntime(store, redisClient, channelReplyOwner(config), wecomAdapter, secrets, metricsRecorder)
		if err != nil {
			return err
		}
		defer func() {
			if !shutdownResourceCloseSkipped(serviceErr) {
				serviceErr = errors.Join(serviceErr, replies.close())
			}
		}()
	}

	var runtime *workerRuntime
	var dispatchRelay *relay.Relay
	if config.Role.runsGateway() {
		dispatchRelay, err = relay.New(store, stream, config.DispatcherID)
		if err != nil {
			return err
		}
	}
	if config.Role.runsWorker() {
		runtime, err = newWorkerRuntime(workerRuntimeDependencies{
			store:                       store,
			redisClient:                 redisClient,
			stream:                      stream,
			owner:                       config.WorkerID,
			getenv:                      os.Getenv,
			secrets:                     secrets,
			artifacts:                   artifacts,
			defaultSessionDSN:           config.PostgresDSN,
			defaultRedisURL:             config.RedisURL,
			metrics:                     metricsRecorder,
			modelTimeout:                config.ModelTimeout,
			artifactRetention:           config.ArtifactRetention,
			concurrency:                 config.WorkerConcurrency,
			pauseAfterClaim:             config.PauseAfterClaim,
			pauseAfterMigrationCopy:     config.PauseAfterMigrationCopy,
			pauseAfterMigrationCopyItem: config.PauseAfterMigrationCopyItem,
			replyMode:                   config.IMReplyMode,
		})
		if err != nil {
			return err
		}
		defer func() {
			if !shutdownResourceCloseSkipped(serviceErr) {
				serviceErr = errors.Join(serviceErr, runtime.close())
			}
		}()
	}

	componentCtx, cancelComponents := context.WithCancel(ctx)
	defer cancelComponents()
	var relayDone <-chan error
	if dispatchRelay != nil {
		done := make(chan error, 1)
		go func() { done <- dispatchRelay.Run(componentCtx) }()
		relayDone = done
	}
	var providerDone <-chan error
	if config.Role.runsChannel() && (wecomAdapter != nil || feishuAdapter != nil) {
		done := make(chan error, 1)
		go func() {
			defer close(done)
			done <- runChannelAdapters(componentCtx, wecomAdapter, feishuAdapter)
		}()
		providerDone = done
	}
	var replyDone <-chan error
	if replies != nil && runtime == nil {
		done := make(chan error, 1)
		go func() { done <- replies.sender.Run(componentCtx) }()
		replyDone = done
	}
	log.Printf("trpc-agent-service %s role=%s http=%s", trpcservice.Version, config.Role, config.HTTPAddr)
	if runtime == nil {
		server.MarkReady()
		result, replyErr, replyStopped := waitForGatewayShutdown(componentCtx, server, config.ShutdownTimeout, relayDone, providerDone, replyDone)
		cancelComponents()
		if !replyStopped {
			shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), config.ShutdownTimeout)
			replyErr, replyStopped = awaitReplyExit(shutdownCtx, replyDone, cancelComponents)
			cancelShutdown()
		}
		return errors.Join(result, awaitProviderExit(providerDone, config.ShutdownTimeout), replyShutdownError(replyErr, replyStopped))
	}
	var replySender *worker.ReplySender
	if replies != nil {
		replySender = replies.sender
	}
	result := runWorkerUntilShutdown(componentCtx, runtime, replySender, server, config.ShutdownTimeout, relayDone, providerDone)
	cancelComponents()
	return errors.Join(result, awaitProviderExit(providerDone, config.ShutdownTimeout))
}

func newGatewayHandler(
	store *postgres.Store,
	artifacts *artifactcos.Resolver,
	secrets platformsecret.SecretProvider,
	rateLimiter gateway.AdmissionRateLimiter,
	admissionConcurrency int,
	durableEventWaitTimeout time.Duration,
) (http.Handler, *wecom.Adapter, *feishu.Adapter, error) {
	if store == nil || artifacts == nil || secrets == nil {
		return nil, nil, nil, errors.New("gateway dependencies are required")
	}
	if rateLimiter == nil {
		return nil, nil, nil, errors.New("admission rate limiter is required")
	}
	admissionSlots, err := gateway.NewAdmissionConcurrency(admissionConcurrency)
	if err != nil {
		return nil, nil, nil, err
	}
	events, err := postgres.NewExecutionEventJournal(store)
	if err != nil {
		return nil, nil, nil, err
	}
	admitter := gateway.New(store)
	admitter.Metrics = store.Metrics()
	admitter.RateLimiter = rateLimiter
	admitter.AdmissionConcurrency = admissionSlots
	queued, err := gateway.NewQueuedRunner(
		admitter,
		events,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	openAIHandler, err := ingress.NewOpenAIHandlerWithOptions(auth.HTTPAPIKeyResolver{
		Credentials: store,
		Directory:   store,
	}, queued, ingress.WithDurableEventWaitTimeout(durableEventWaitTimeout))
	if err != nil {
		return nil, nil, nil, err
	}
	attachmentIngestor, err := channelsattachments.NewIngestor(
		store,
		secrets,
		artifacts,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	wecomAdapter, err := wecom.NewAdapter(
		store,
		admitter,
		secrets,
		wecom.WithAttachmentIngestor(attachmentIngestor),
		wecom.WithCommandHandler(store),
		wecom.WithMetrics(store.Metrics()),
	)
	if err != nil {
		return nil, nil, nil, err
	}
	feishuAdapter, err := feishu.NewAdapter(
		store,
		admitter,
		secrets,
		feishu.WithAttachmentIngestor(attachmentIngestor),
		feishu.WithCommandHandler(store),
		feishu.WithRecallAdmitter(store),
		feishu.WithMetrics(store.Metrics()),
	)
	if err != nil {
		return nil, nil, nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/", openAIHandler)
	return mux, wecomAdapter, feishuAdapter, nil
}

func runChannelAdapters(
	ctx context.Context,
	wecomAdapter *wecom.Adapter,
	feishuAdapter *feishu.Adapter,
) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type runner func(context.Context) error
	runners := make([]runner, 0, 2)
	if wecomAdapter != nil {
		runners = append(runners, wecomAdapter.Run)
	}
	if feishuAdapter != nil {
		runners = append(runners, feishuAdapter.Run)
	}
	if len(runners) == 0 {
		return nil
	}
	done := make(chan error, len(runners))
	var wg sync.WaitGroup
	for _, run := range runners {
		wg.Add(1)
		go func(run runner) {
			defer wg.Done()
			done <- run(runCtx)
		}(run)
	}
	go func() {
		wg.Wait()
		close(done)
	}()
	var firstErr error
	for err := range done {
		if err != nil && !errors.Is(err, context.Canceled) && firstErr == nil {
			firstErr = err
			cancel()
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return firstErr
}

func newAdminHandler(store *postgres.Store, authConfig admin.AdminAuthConfig, operations ...admin.OperationsReader) (http.Handler, error) {
	if store == nil {
		return nil, errors.New("postgres store is required")
	}
	api := admin.API{
		Bindings:   store,
		Repository: store,
	}
	if len(operations) > 0 {
		if operations[0] == nil {
			return nil, errors.New("admin operations provider is required")
		}
		api.Operations = operations[0]
	}
	return admin.NewHTTPHandlerWithAuth(api, authConfig)
}

func (c serviceConfig) adminAuthConfig() admin.AdminAuthConfig {
	return admin.AdminAuthConfig{
		SystemAdminToken:  c.AdminToken,
		OperatorToken:     c.OperatorToken,
		OperatorTenantIDs: c.OperatorTenantIDs,
		AuditorToken:      c.AuditorToken,
		AuditorTenantIDs:  c.AuditorTenantIDs,
	}
}

func splitCommaSeparated(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

type serviceOperationsReader struct {
	store                                *postgres.Store
	redis                                *platformredis.Client
	jaegerURL, prometheusURL, grafanaURL string
}

func (r serviceOperationsReader) OperationsSummary(ctx context.Context) (admin.OperationsSummary, error) {
	return r.summary(ctx, nil)
}

func (r serviceOperationsReader) OperationsSummaryForPrincipal(
	ctx context.Context,
	principal admin.AdminPrincipal,
) (admin.OperationsSummary, error) {
	if principal.Role == admin.RoleSystemAdmin {
		return r.summary(ctx, nil)
	}
	return r.summary(ctx, principal.TenantIDs)
}

func (r serviceOperationsReader) summary(ctx context.Context, tenantIDs []string) (admin.OperationsSummary, error) {
	if r.store == nil || r.redis == nil {
		return admin.OperationsSummary{}, errors.New("service operations dependencies are required")
	}
	var summary admin.OperationsSummary
	var err error
	if tenantIDs == nil {
		summary, err = r.store.OperationsSummary(ctx)
	} else {
		summary, err = r.store.OperationsSummaryForTenants(ctx, tenantIDs)
	}
	if err != nil {
		if r.store.Metrics() != nil {
			r.store.Metrics().SetBackendReadiness("postgres", false)
		}
		return admin.OperationsSummary{}, err
	}
	redisStatus := "READY"
	redisErr := r.redis.Ping(ctx)
	if r.store.Metrics() != nil {
		r.store.Metrics().SetBackendReadiness("redis", redisErr == nil)
	}
	if redisErr != nil {
		redisStatus = "NOT_READY"
		summary.GatewayReadiness = "NOT_READY"
	}
	summary.Backends = append(summary.Backends, admin.BackendReadiness{
		Name: "redis", Provider: "redis", Status: redisStatus,
	})
	summary.JaegerURL = r.jaegerURL
	summary.PrometheusURL = r.prometheusURL
	summary.GrafanaURL = r.grafanaURL
	return summary, nil
}

func awaitProviderExit(done <-chan error, timeout time.Duration) error {
	if done == nil {
		return nil
	}
	if timeout <= 0 {
		timeout = time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	select {
	case err, ok := <-done:
		if !ok {
			return nil
		}
		return nonCancellationError(err)
	case <-ctx.Done():
		return fmt.Errorf("%w: channel adapters did not stop: %w", errWorkerShutdownTimeout, ctx.Err())
	}
}

func shutdownResourceCloseSkipped(err error) bool {
	return errors.Is(err, errWorkerShutdownTimeout) || errors.Is(err, context.DeadlineExceeded)
}

func waitForGatewayShutdown(
	ctx context.Context,
	server *serviceServer,
	shutdownTimeout time.Duration,
	relayDone <-chan error,
	providerDone <-chan error,
	replyDone <-chan error,
) (error, error, bool) {
	defer server.MarkNotReady()
	select {
	case err := <-relayDone:
		server.MarkNotReady()
		return nonCancellationError(err), nil, false
	case err := <-providerDone:
		server.MarkNotReady()
		return nonCancellationError(err), nil, false
	case err := <-replyDone:
		server.MarkNotReady()
		return nil, err, true
	case <-ctx.Done():
		server.MarkNotReady()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return server.shutdown(shutdownCtx), nil, false
	case <-server.done:
		return server.wait(), nil, false
	}
}

func runWorkerUntilShutdown(
	ctx context.Context,
	runtime *workerRuntime,
	replySender *worker.ReplySender,
	server *serviceServer,
	shutdownTimeout time.Duration,
	relayDone <-chan error,
	providerDone <-chan error,
) error {
	defer server.MarkNotReady()
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	// Auxiliary loops must stop as soon as shutdown begins. The consumer keeps
	// its run context until the grace deadline so a claimed execution can drain.
	auxCtx, cancelAux := context.WithCancel(runCtx)
	defer cancelAux()
	done := make(chan error, 1)
	go func() {
		done <- runtime.consumer.Run(runCtx)
	}()
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		runtime.runHeartbeat(auxCtx)
	}()
	migrationDone := make(chan error, 1)
	go func() {
		migrationDone <- runtime.runDataMigrations(auxCtx)
	}()
	var replyDone <-chan error
	if replySender != nil {
		done := make(chan error, 1)
		go func() { done <- replySender.Run(auxCtx) }()
		replyDone = done
	}
	server.MarkReady()

	var cause error
	serverStopped := false
	var replyErr error
	replyStopped := false
	select {
	case err := <-relayDone:
		server.MarkNotReady()
		runtime.consumer.StopClaiming()
		cancelAux()
		cause = nonCancellationError(err)
	case err := <-providerDone:
		server.MarkNotReady()
		runtime.consumer.StopClaiming()
		cancelAux()
		cause = nonCancellationError(err)
	case err := <-done:
		server.MarkNotReady()
		runtime.consumer.StopClaiming()
		cancelAux()
		cause = err
	case err := <-replyDone:
		server.MarkNotReady()
		runtime.consumer.StopClaiming()
		cancelAux()
		replyErr = err
		replyStopped = true
	case err := <-migrationDone:
		server.MarkNotReady()
		runtime.consumer.StopClaiming()
		cancelAux()
		cause = nonCancellationError(err)
	case <-server.done:
		server.MarkNotReady()
		runtime.consumer.StopClaiming()
		cancelAux()
		cause = server.wait()
		serverStopped = true
	case <-ctx.Done():
		server.MarkNotReady()
		runtime.consumer.StopClaiming()
		cancelAux()
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	var serverErr error
	if !serverStopped {
		serverErr = server.shutdown(shutdownCtx)
	}
	if serverErr != nil {
		cancelRun()
	}
	workerErr, workerStopped := awaitWorkerExit(shutdownCtx, done, cancelRun)
	migrationErr, migrationStopped := awaitDataMigrationExit(shutdownCtx, migrationDone)
	if !replyStopped {
		replyErr, replyStopped = awaitReplyExit(shutdownCtx, replyDone, cancelRun)
	}
	heartbeatStopped := awaitHeartbeatExit(shutdownCtx, heartbeatDone)
	return dataMigrationShutdownResult(
		workerShutdownResult(
			errors.Join(
				cause,
				serverErr,
				replyShutdownError(replyErr, replyStopped),
				heartbeatShutdownError(heartbeatStopped),
			),
			workerErr,
			workerStopped,
		),
		migrationErr,
		migrationStopped,
	)
}

func awaitWorkerExit(ctx context.Context, done <-chan error, cancel context.CancelFunc) (error, bool) {
	select {
	case err := <-done:
		return err, true
	case <-ctx.Done():
		cancel()
		select {
		case err := <-done:
			return err, true
		default:
			return ctx.Err(), false
		}
	}
}

func awaitDataMigrationExit(ctx context.Context, done <-chan error) (error, bool) {
	select {
	case err := <-done:
		return err, true
	case <-ctx.Done():
		select {
		case err := <-done:
			return err, true
		default:
			return ctx.Err(), false
		}
	}
}

func awaitReplyExit(ctx context.Context, done <-chan error, cancel context.CancelFunc) (error, bool) {
	if done == nil {
		return nil, true
	}
	select {
	case err := <-done:
		return err, true
	case <-ctx.Done():
		cancel()
		select {
		case err := <-done:
			return err, true
		default:
			return ctx.Err(), false
		}
	}
}

func replyShutdownError(err error, stopped bool) error {
	if !stopped {
		return fmt.Errorf("%w: reply sender: %s", errWorkerShutdownTimeout, platformlog.SafeError(err))
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func awaitHeartbeatExit(ctx context.Context, done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	case <-ctx.Done():
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
}

func heartbeatShutdownError(stopped bool) error {
	if stopped {
		return nil
	}
	return fmt.Errorf("%w: worker heartbeat", errWorkerShutdownTimeout)
}

func nonCancellationError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func workerShutdownResult(serverErr, workerErr error, stopped bool) error {
	if !stopped {
		return errors.Join(serverErr, fmt.Errorf("%w: %s", errWorkerShutdownTimeout, platformlog.SafeError(workerErr)))
	}
	return errors.Join(serverErr, workerErr)
}

func dataMigrationShutdownResult(baseErr, migrationErr error, stopped bool) error {
	if !stopped {
		return errors.Join(baseErr, fmt.Errorf("%w: data migration worker: %s", errWorkerShutdownTimeout, platformlog.SafeError(migrationErr)))
	}
	if errors.Is(migrationErr, context.Canceled) {
		return baseErr
	}
	return errors.Join(baseErr, migrationErr)
}
