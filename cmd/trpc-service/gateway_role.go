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

	"github.com/liuzengh/trpc-agent-service/migrations"
	configpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/config/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	gatewaypostgres "github.com/liuzengh/trpc-agent-service/trpcservice/gateway/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/health"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretfs "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/filesystem"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets/payloadkey"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

const gatewayAuthResourceID = "gateway-auth"

func gatewayAuthScope(tenantID string, version int64) secrets.Scope {
	return secrets.Scope{TenantID: tenantID, Subject: "gateway", Purpose: secrets.PurposeGatewayAuth,
		ResourceID: gatewayAuthResourceID, ResourceVersion: version}
}

func runGatewayRole(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || logger == nil {
		return errors.New("invalid process dependencies")
	}
	configValue, err := loadGatewayConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	telemetryProvider, err := newRoleTelemetry(parent, getenv, "gateway", logger)
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

	secretProvider, err := secretfs.New(configValue.SecretRoot, 64<<10)
	if err != nil {
		return errors.New("secret provider configuration rejected")
	}
	payloadKeys, err := payloadkey.New(secretProvider, configValue.PayloadKeyRef)
	if err != nil {
		return errors.New("payload key configuration rejected")
	}
	tenantRepo := tenantpostgres.New(db)
	configRepo := configpostgres.New(db, tenantRepo)
	authRef := secrets.SecretRef{Ref: configValue.GatewayAuthSecretRef, Version: configValue.GatewayAuthSecretVersion}
	authValue, err := secretProvider.Resolve(parent, gatewayAuthScope(configValue.GatewayProbeTenant, configValue.GatewayAuthSecretVersion), authRef)
	if err != nil {
		return errors.New("gateway auth secret resolution failed")
	}
	authResolver, err := gateway.NewHMACPrincipalResolver(authValue.Bytes, tenantRepo, gateway.HMACPrincipalOptions{ClockSkew: configValue.GatewayAuthClockSkew})
	clear(authValue.Bytes)
	if err != nil {
		return errors.New("gateway auth resolver configuration rejected")
	}
	defer authResolver.Close()
	cursorValue, err := secretProvider.Resolve(parent, gatewayAuthScope(configValue.GatewayProbeTenant, configValue.GatewayAuthSecretVersion), authRef)
	if err != nil {
		return errors.New("gateway cursor secret resolution failed")
	}
	cursors, err := gateway.NewCursorCodec(cursorValue.Bytes)
	clear(cursorValue.Bytes)
	if err != nil {
		return errors.New("gateway cursor configuration rejected")
	}
	defer cursors.Close()

	tasks := gatewaypostgres.NewTaskStore(db)
	payloads := messagingpostgres.NewWithPayloadKeyResolver(db, payloadKeys)
	submitter := gateway.RunSubmitter{Inbox: payloads, Payloads: payloads,
		Dispatcher: gateway.BrokerDispatcher{Tasks: tasks, Bindings: configRepo}, PayloadKeyVersion: configValue.PayloadKeyVersion,
		Telemetry: telemetryProvider}
	events := gateway.TerminalEventStore{Tasks: tasks, Results: payloads}
	routes := gatewaypostgres.HTTPRouteResolver{Tenants: tenantRepo, Configs: configRepo}
	bridge := gateway.NewGatewayRunnerBridge(submitter, events)
	bridge.PollInterval = configValue.GatewaySSEPollInterval
	bridge.ReplayLimit = int(configValue.GatewaySSEReplayLimit)
	defer bridge.Close()
	protocolRunner := gateway.CanonicalRunner{Next: bridge}
	a2aTasks := &gateway.DurableA2ATaskManager{Submitter: submitter, Tasks: tasks, Events: events,
		PollInterval: configValue.GatewaySSEPollInterval, ReplayLimit: int(configValue.GatewaySSEReplayLimit)}
	lifecycle := worker.NewLifecycle()
	migrationReadiness := migrations.NewRunner(db)
	monitor, err := health.NewMonitor(lifecycle, []health.Dependency{
		{Name: "postgres", Probe: db.PingContext},
		{Name: "postgres_schema", Probe: migrationReadiness.Ready},
		{Name: "secret_provider", Probe: secretProvider.ProbeRoot},
		{Name: "gateway_auth_secret", Probe: func(ctx context.Context) error {
			return secretProvider.Probe(ctx, gatewayAuthScope(configValue.GatewayProbeTenant, configValue.GatewayAuthSecretVersion), authRef)
		}},
		{Name: "payload_key_provider", Probe: func(ctx context.Context) error {
			value, probeErr := payloadKeys.ResolvePayloadKey(ctx, configValue.GatewayProbeTenant, configValue.PayloadKeyVersion)
			clear(value.Bytes)
			return probeErr
		}},
	}, configValue.ProbeTimeout, configValue.ProbeInterval)
	if err != nil {
		return errors.New("readiness configuration rejected")
	}
	if err := monitor.ProbeOnce(parent); err != nil {
		return errors.New("initial dependency probe interrupted")
	}
	// Rebuild the handler after the monitor exists so POST admission is gated
	// by the same lifecycle/readiness authority exposed at /readyz.
	api, err := gateway.NewHTTPHandler(gateway.HTTPHandlerOptions{Submitter: submitter, Tasks: tasks, Events: events,
		Principals: authResolver, Routes: routes, Cursors: cursors, Readiness: monitor,
		MaxBody: configValue.GatewayMaxBody, PollInterval: configValue.GatewaySSEPollInterval,
		ReplayLimit: int(configValue.GatewaySSEReplayLimit), MaxSubscribers: configValue.GatewaySSEMaxSubscribers})
	if err != nil {
		return errors.New("gateway HTTP readiness composition rejected")
	}
	a2aTasks.Readiness = monitor
	protocol, err := gateway.NewProtocolHTTPHandler(gateway.ProtocolHTTPOptions{Runner: protocolRunner, Resolver: authResolver,
		Readiness: monitor, MaxBody: configValue.GatewayMaxBody, RunTimeout: configValue.GatewayProtocolTimeout,
		A2A: a2aTasks, PublicURL: configValue.GatewayPublicURL})
	if err != nil {
		return errors.New("gateway protocol composition rejected")
	}

	listener, err := net.Listen("tcp", configValue.ListenAddress)
	if err != nil {
		return errors.New("HTTP listener initialization failed")
	}
	defer listener.Close()
	mux := http.NewServeMux()
	mux.Handle("/livez", health.Handler{Checker: monitor})
	mux.Handle("/readyz", health.Handler{Checker: monitor})
	mux.Handle("/v1/chat/completions", protocol)
	mux.Handle("/trpc-agent/v1/apps/", protocol)
	mux.Handle("/a2a/v1/apps/", protocol)
	mux.Handle("/v1/agent-runs", api)
	mux.Handle("/v1/agent-runs/", api)
	mux.Handle("/", api)
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}

	processCtx, cancelProcess := context.WithCancel(parent)
	defer cancelProcess()
	stopSignals := worker.InstallSignalDrain(processCtx, lifecycle)
	defer stopSignals()
	errorsCh := make(chan error, 2)
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
	logger.Printf("trpc-agent-service gateway lifecycle/readiness listening on %s", configValue.ListenAddress)

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
	if err := server.Shutdown(shutdownCtx); err != nil && terminalErr == nil {
		terminalErr = errors.New("HTTP shutdown timed out")
	}
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
	if terminalErr != nil {
		return terminalErr
	}
	return nil
}
