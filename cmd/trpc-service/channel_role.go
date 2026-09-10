package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	redisclient "github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/migrations"
	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	feishuprotocol "github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu/protocol"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/httpcallback"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress"
	ingresspostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/ingress/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/webui"
	webuipostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/webui/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	wecomprotocol "github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom/protocol"
	configpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/config/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/health"
	preprocesspostgres "github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/postgres"
	progressredis "github.com/liuzengh/trpc-agent-service/trpcservice/progress/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	secretfs "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/filesystem"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets/payloadkey"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

// runChannelRole owns only callback ingress. Provider adapters are kept
// transport-neutral here: they verify/decode and persist durable facts, while
// sender/token clients remain in the channel delivery role.
func runChannelRole(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || logger == nil {
		return errors.New("invalid process dependencies")
	}
	configValue, err := loadChannelConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	telemetryProvider, err := newRoleTelemetry(parent, getenv, "channel", logger)
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
	var progressRedis *redisclient.Client
	var progressSubscriber *progressredis.Subscriber
	if configValue.WebUIEnabled {
		progressRedis = redisclient.NewClient(&redisclient.Options{Addr: configValue.RedisAddress, Password: configValue.RedisPassword, DB: configValue.RedisDB})
		defer progressRedis.Close()
		if err := progressRedis.Ping(parent).Err(); err != nil {
			return errors.New("WebUI progress Redis unavailable")
		}
		progressSubscriber, err = progressredis.NewSubscriber(progressRedis, progressredis.Config{Environment: configValue.RedisEnvironment})
		if err != nil {
			return errors.New("WebUI progress subscriber configuration rejected")
		}
	}
	secretProvider, err := secretfs.New(configValue.SecretRoot, 64<<10)
	if err != nil {
		return errors.New("secret provider configuration rejected")
	}
	payloadKeys, err := payloadkey.New(secretProvider, configValue.PayloadKeyRef)
	if err != nil {
		return errors.New("payload key configuration rejected")
	}
	payloads := messagingpostgres.NewWithPayloadKeyResolver(db, payloadKeys)
	intake := preprocesspostgres.New(db)
	bindingStore := ingresspostgres.New(db)
	bindingResolver := ingress.Resolver{Store: bindingStore, Secrets: secretProvider, TTL: configValue.ChannelCandidateTTL}
	identityMapper := identity.Mapper{Secrets: secretProvider}
	configRepo := configpostgres.New(db, tenantpostgres.New(db))

	feishuAdapter := &feishu.Adapter{Protocol: feishuprotocol.Verifier{}}
	feishuEndpoint, err := newChannelEndpoint(feishuAdapter, bindingResolver, identityMapper, intake, payloads, configValue.PayloadKeyVersion, configValue.ChannelCallbackMaxBody, telemetryProvider, configRepo)
	if err != nil {
		return errors.New("Feishu callback configuration rejected")
	}
	wecomAdapter := &wecom.Adapter{Protocol: wecomprotocol.Verifier{}}
	wecomEndpoint, err := newChannelEndpoint(wecomAdapter, bindingResolver, identityMapper, intake, payloads, configValue.PayloadKeyVersion, configValue.ChannelCallbackMaxBody, telemetryProvider, configRepo)
	if err != nil {
		return errors.New("WeCom callback configuration rejected")
	}
	callbackRoutes := []httpcallback.Route{
		httpcallback.Route{Pattern: "/callbacks/feishu", Endpoint: feishuEndpoint},
		httpcallback.Route{Pattern: "/callbacks/wecom", Endpoint: wecomEndpoint},
	}
	var webuiBrowser http.Handler
	if configValue.WebUIEnabled {
		webuiMailbox := webuipostgres.New(db)
		webuiAdapter := &webui.Adapter{Protocol: webui.Verifier{}, Mailbox: webuiMailbox}
		webuiEndpoint, endpointErr := newChannelEndpoint(webuiAdapter, bindingResolver, identityMapper, intake, payloads, configValue.PayloadKeyVersion, configValue.ChannelCallbackMaxBody, telemetryProvider, configRepo)
		if endpointErr != nil {
			return errors.New("WebUI callback configuration rejected")
		}
		callbackRoutes = append(callbackRoutes, httpcallback.Route{Pattern: "/callbacks/webui", Endpoint: webuiEndpoint})
		webuiBrowser = webui.BrowserHandler{Callback: webuiEndpoint, Routes: bindingStore, Secrets: secretProvider,
			Messages: webuiMailbox, Results: payloads, ReplyRoutes: payloads, Progress: progressSubscriber}
	}
	callbackMux, err := httpcallback.NewMux(callbackRoutes...)
	if err != nil {
		return errors.New("callback mux configuration rejected")
	}

	lifecycle := worker.NewLifecycle()
	migrationReadiness := migrations.NewRunner(db)
	dependencies := []health.Dependency{
		{Name: "postgres", Probe: db.PingContext},
		{Name: "postgres_schema", Probe: migrationReadiness.Ready},
		{Name: "secret_provider", Probe: secretProvider.ProbeRoot},
		{Name: "payload_key_provider", Probe: func(ctx context.Context) error {
			value, resolveErr := payloadKeys.ResolvePayloadKey(ctx, configValue.ChannelProbeTenant, configValue.PayloadKeyVersion)
			clear(value.Bytes)
			return resolveErr
		}},
	}
	if progressRedis != nil {
		dependencies = append(dependencies, health.Dependency{Name: "webui_progress_redis", Probe: func(ctx context.Context) error {
			return progressRedis.Ping(ctx).Err()
		}})
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
	mux.Handle("/callbacks/", readinessGate{Checker: monitor, Handler: callbackMux})
	if webuiBrowser != nil {
		mux.Handle("/webui", readinessGate{Checker: monitor, Handler: webuiBrowser})
		mux.Handle("/webui/", readinessGate{Checker: monitor, Handler: webuiBrowser})
	}
	mux.Handle("/livez", health.Handler{Checker: monitor})
	mux.Handle("/readyz", health.Handler{Checker: monitor})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}

	processCtx, cancelProcess := context.WithCancel(parent)
	defer cancelProcess()
	stopSignals := worker.InstallSignalDrain(processCtx, lifecycle)
	defer stopSignals()
	errorsCh := make(chan error, 4)
	var background sync.WaitGroup
	background.Add(1)
	go func() {
		defer background.Done()
		if monitorErr := monitor.Run(processCtx); monitorErr != nil && !errors.Is(monitorErr, context.Canceled) {
			errorsCh <- errors.New("readiness monitor stopped")
		}
	}()
	background.Add(1)
	go func() {
		defer background.Done()
		if reaperErr := runChannelCandidateReaper(processCtx, bindingStore, channelCandidateReapInterval(configValue.ChannelCandidateTTL), 100, logger); reaperErr != nil && !errors.Is(reaperErr, context.Canceled) {
			errorsCh <- errors.New("candidate reaper stopped")
		}
	}()
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
	logger.Printf("trpc-agent-service channel lifecycle/readiness listening on %s", configValue.ListenAddress)

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
	backgroundDone := make(chan struct{})
	go func() {
		background.Wait()
		close(backgroundDone)
	}()
	select {
	case <-backgroundDone:
	case <-shutdownCtx.Done():
		if terminalErr == nil {
			terminalErr = errors.New("background shutdown timed out")
		}
	}
	lifecycle.MarkStopped()
	if terminalErr != nil {
		return terminalErr
	}
	return nil
}

func runChannelCandidateReaper(ctx context.Context, store ingress.Store, interval time.Duration, batch int, logger *roleLogger) error {
	if ctx == nil || store == nil || interval <= 0 || batch < 1 || logger == nil {
		return runtime.ErrInvariantViolation
	}
	for {
		if _, err := store.BurnExpiredCandidates(ctx, time.Now().UTC(), batch); err != nil {
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, runtime.ErrInvariantViolation) {
				return err
			}
			logger.Printf("channel candidate reaper degraded: %v", err)
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func channelCandidateReapInterval(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return time.Minute
	}
	interval := ttl / 2
	if interval < time.Second {
		interval = time.Second
	}
	if interval > time.Minute {
		interval = time.Minute
	}
	return interval
}

// readinessGate prevents accepting provider work while the role is starting,
// unhealthy, draining, or stopped. This keeps the callback ACK boundary
// aligned with the same cached dependency snapshot exposed by /readyz.
type readinessGate struct {
	Checker interface{ Ready() bool }
	Handler http.Handler
}

func (g readinessGate) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if g.Checker == nil || !g.Checker.Ready() {
		http.Error(writer, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	if g.Handler == nil {
		http.Error(writer, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	g.Handler.ServeHTTP(writer, request)
}

func newChannelEndpoint(adapter channel.HTTPAdapter, resolver ingress.Resolver, identityMapper identity.Mapper, intake *preprocesspostgres.Store, payloads *messagingpostgres.Store, keyVersion int64, maxBody int64, provider telemetry.Provider, configs ingress.ConfigSelector) (*httpcallback.Endpoint, error) {
	if adapter == nil || resolver.Store == nil || resolver.Secrets == nil || identityMapper.Secrets == nil || intake == nil || payloads == nil || keyVersion < 1 || maxBody < 1 {
		return nil, runtime.ErrInvariantViolation
	}
	verification := ingress.Service{Adapter: adapter, Bindings: resolver, Telemetry: provider}
	pipeline := ingress.Pipeline{Verification: verification, Identity: identityMapper, Intake: intake, Payloads: payloads, KeyVersion: keyVersion, Telemetry: provider, Configs: configs}
	challenge := ingress.ChallengeService{Adapter: adapter, Bindings: resolver}
	endpoint, err := httpcallback.NewEndpoint(adapter, pipeline, challenge)
	if err != nil {
		return nil, err
	}
	endpoint.MaxBody = maxBody
	return endpoint, nil
}
