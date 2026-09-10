package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	redisclient "github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/migrations"
	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/credentials"
	credentialpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/credentials/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/delivery"
	deliverypostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/delivery/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/webui"
	webuipostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/webui/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/health"
	relayredis "github.com/liuzengh/trpc-agent-service/trpcservice/relay/redis"
	secretfs "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/filesystem"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets/payloadkey"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func runChannelDeliveryRole(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || logger == nil {
		return errors.New("invalid process dependencies")
	}
	configValue, err := loadChannelDeliveryConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	telemetryProvider, err := newRoleTelemetry(parent, getenv, "channel-delivery", logger)
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
	secretProvider, err := secretfs.New(configValue.SecretRoot, 64<<10)
	if err != nil {
		return errors.New("secret provider configuration rejected")
	}
	payloadKeys, err := payloadkey.New(secretProvider, configValue.PayloadKeyRef)
	if err != nil {
		return errors.New("payload key configuration rejected")
	}
	store := messagingpostgres.NewWithPayloadKeyResolver(db, payloadKeys)
	sendCredentials := credentials.Resolver{Locator: credentialpostgres.New(db), Secrets: secretProvider}
	providerHTTP := &http.Client{Timeout: configValue.ChannelProviderTimeout}
	feishuCredentials := &feishu.CredentialProvider{Secrets: sendCredentials, Client: providerHTTP}
	feishuAdapter := &feishu.Adapter{Sender: feishu.OfficialSender{Tokens: feishuCredentials, Client: providerHTTP, Clients: &feishu.ClientCache{Credentials: feishuCredentials,
		NewClient: func(appID, appSecret string) *lark.Client {
			return lark.NewClient(appID, appSecret, lark.WithHttpClient(providerHTTP))
		}}}}
	wecomTokens := &wecom.TokenProvider{Secrets: sendCredentials, Client: providerHTTP}
	wecomAdapter := &wecom.Adapter{Sender: wecom.OfficialSender{Tokens: wecomTokens, Client: providerHTTP}}
	adapters := []channel.Adapter{feishuAdapter, wecomAdapter}
	if configValue.WebUIEnabled {
		adapters = append(adapters, &webui.Adapter{Mailbox: webuipostgres.New(db)})
	}
	catalog, err := deliverypostgres.New(db, adapters...)
	if err != nil {
		return errors.New("delivery catalog configuration rejected")
	}
	publisher, err := relayredis.NewPublisher(redis, relayredis.Config{Environment: configValue.RedisEnvironment})
	if err != nil {
		return errors.New("reply stream configuration rejected")
	}
	queue, err := relayredis.NewReplyQueue(redis, publisher, relayredis.ReplyQueueConfig{Group: configValue.ChannelDeliveryGroup,
		ReadBlock: configValue.ChannelReplyReadBlock, ReclaimIdle: configValue.ChannelReplyReclaimIdle})
	if err != nil {
		return errors.New("reply queue configuration rejected")
	}
	owner, _ := os.Hostname()
	owner = fmt.Sprintf("%s-%d-channel-delivery", valueOr(owner, "trpc-service"), os.Getpid())
	service := delivery.Service{Results: store, Ledger: store, Adapters: catalog, Owner: owner,
		ClaimTTL: configValue.ChannelDeliveryClaimTTL, ClaimRenewInterval: configValue.ChannelDeliveryClaimRenew,
		DefaultRetryDelay: configValue.ChannelDeliveryRetryDelay, MaxRetryDelay: configValue.ChannelDeliveryMaxRetry,
		MaxAttempts: configValue.ChannelDeliveryMaxAttempts, MaxReconcileAttempts: configValue.ChannelDeliveryMaxReconcile}
	supervisor := delivery.Supervisor{Catalog: catalog, RefreshInterval: configValue.ChannelDeliveryRefresh,
		NewConsumer: func(destination channel.ReplyDestination) (delivery.ConsumerRunner, error) {
			return delivery.Consumer{Queue: queue, Deliverer: service, Destination: destination, ConsumerID: owner,
				ReclaimInterval: configValue.ChannelReplyReclaimInterval, ReclaimLimit: configValue.ChannelReplyReclaimLimit,
				Telemetry: telemetryProvider,
				OnDeliveryError: func(delivery channel.ReplyDelivery, err error) {
					logger.Printf("channel delivery degraded tenant=%s binding=%s delivery=%s: %v", delivery.Event.TenantID, delivery.Event.ChannelBindingID, delivery.ID, err)
				}}, nil
		}, OnError: func(err error) { logger.Printf("channel delivery supervisor degraded: %v", err) }}

	lifecycle := worker.NewLifecycle()
	monitor, err := health.NewMonitor(lifecycle, []health.Dependency{{Name: "postgres", Probe: db.PingContext},
		{Name: "postgres_schema", Probe: migrations.NewRunner(db).Ready}, {Name: "redis", Probe: func(ctx context.Context) error { return redis.Ping(ctx).Err() }},
		{Name: "secret_provider", Probe: secretProvider.ProbeRoot}, {Name: "delivery_catalog", Probe: catalog.Probe},
		{Name: "payload_key_provider", Probe: func(ctx context.Context) error {
			value, resolveErr := payloadKeys.ResolvePayloadKey(ctx, configValue.ChannelProbeTenant, configValue.PayloadKeyVersion)
			clear(value.Bytes)
			return resolveErr
		}},
	}, configValue.ProbeTimeout, configValue.ProbeInterval)
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
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	processCtx, cancelProcess := context.WithCancel(parent)
	defer cancelProcess()
	stopSignals := worker.InstallSignalDrain(processCtx, lifecycle)
	defer stopSignals()
	errorsCh := make(chan error, 3)
	var background sync.WaitGroup
	start := func(name string, operation func(context.Context) error) {
		background.Add(1)
		go func() {
			defer background.Done()
			if runErr := operation(processCtx); runErr != nil && !errors.Is(runErr, context.Canceled) {
				errorsCh <- fmt.Errorf("%s stopped", name)
			}
		}()
	}
	start("readiness monitor", monitor.Run)
	start("channel delivery supervisor", supervisor.Run)
	background.Add(1)
	go func() {
		defer background.Done()
		if serveErr := server.Serve(listener); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			errorsCh <- errors.New("HTTP server stopped")
		}
	}()
	if err := lifecycle.MarkReady(); err != nil {
		return errors.New("process lifecycle transition failed")
	}
	logger.Printf("trpc-agent-service channel-delivery lifecycle/readiness listening on %s", configValue.ListenAddress)
	var terminalErr error
	select {
	case <-parent.Done():
		lifecycle.BeginDrain()
	case <-lifecycle.Drain():
	case terminalErr = <-errorsCh:
		lifecycle.BeginDrain()
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), configValue.ShutdownTimeout)
	defer cancelShutdown()
	cancelProcess()
	if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil && terminalErr == nil {
		terminalErr = errors.New("HTTP shutdown timed out")
	}
	done := make(chan struct{})
	go func() { background.Wait(); close(done) }()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		if terminalErr == nil {
			terminalErr = errors.New("background shutdown timed out")
		}
	}
	lifecycle.MarkStopped()
	return terminalErr
}
