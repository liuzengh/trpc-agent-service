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

	"github.com/liuzengh/trpc-agent-service/migrations"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/credentials"
	credentialpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/channels/credentials/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	configpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/config/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	gatewaypostgres "github.com/liuzengh/trpc-agent-service/trpcservice/gateway/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/health"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	preprocesspostgres "github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/scanner/clamav"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/scanner/httpdlp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretfs "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/filesystem"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets/payloadkey"
	artifactpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact/postgres"
	messagingpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/postgres"
	objectstores3 "github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore/s3"
	tenantpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

// runPreprocessRole is deliberately separate from the Artifact lifecycle
// process. It owns only the durable preprocess/dispatch path and its scanner,
// object-store and secret dependencies; Redis and channel transports are not
// required for this role.
func runPreprocessRole(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || logger == nil {
		return errors.New("invalid process dependencies")
	}
	configValue, err := loadPreprocessConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	telemetryProvider, err := newRoleTelemetry(parent, getenv, "preprocess", logger)
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

	awsConfig, err := awsconfig.LoadDefaultConfig(parent, awsconfig.WithRegion(configValue.S3Region))
	if err != nil {
		return errors.New("AWS SDK configuration failed")
	}
	objects, err := objectstores3.NewFromConfig(awsConfig, configValue.S3Bucket, configValue.S3Endpoint, configValue.S3PathStyle,
		objectstores3.Options{MaxBytes: configValue.S3MaxBytes, AllowInsecure: configValue.S3AllowInsecure})
	if err != nil {
		return errors.New("object store configuration rejected")
	}
	secretProvider, err := secretfs.New(configValue.SecretRoot, 64<<10)
	if err != nil {
		return errors.New("secret provider configuration rejected")
	}
	dlpSecretRef := secrets.SecretRef{Ref: configValue.DLPSecretRef, Version: configValue.DLPSecretVersion}
	dlpAuthorize, err := newDLPAuthorizer(secretProvider, configValue.DLPBackendVersion, dlpSecretRef)
	if err != nil {
		return errors.New("DLP secret authorization configuration rejected")
	}
	payloadKeys, err := payloadkey.New(secretProvider, configValue.PayloadKeyRef)
	if err != nil {
		return errors.New("payload key configuration rejected")
	}
	dlp := httpdlp.Scanner{Endpoint: configValue.DLPEndpoint, Client: &http.Client{}, ProbeTenantID: configValue.DLPProbeTenant,
		MaxBytes: int(configValue.S3MaxBytes), AllowInsecure: configValue.DLPAllowInsecure, Authorize: dlpAuthorize}
	malware := clamav.Scanner{Address: configValue.ClamAVAddress, MaxBytes: int(configValue.S3MaxBytes)}
	providerHTTP := &http.Client{Timeout: configValue.MediaFetchTimeout}
	sendCredentials := credentials.Resolver{Locator: credentialpostgres.New(db), Secrets: secretProvider}
	feishuCredentials := &feishu.CredentialProvider{Secrets: sendCredentials, Client: providerHTTP}
	wecomTokens := &wecom.TokenProvider{Secrets: sendCredentials, Client: providerHTTP}
	media := preprocess.MediaStager{
		Fetcher: preprocess.MediaRouter{Feishu: feishu.OfficialMediaFetcher{Tokens: feishuCredentials, Client: providerHTTP},
			WeCom:   wecom.OfficialMediaFetcher{Tokens: wecomTokens, Client: providerHTTP},
			Generic: preprocess.SecureHTTPSFetcher{AllowedHosts: hostSet(configValue.MediaAllowedHosts), Timeout: configValue.MediaFetchTimeout}},
		Malware: malware, DLP: dlp,
		Artifacts: artifactpostgres.NewWithObjectStoreOptions(db, objects, artifactpostgres.ObjectStoreOptions{
			PutTimeout: configValue.ArtifactPutTimeout, UploadProtection: configValue.UploadProtection}),
		MaxBytes: configValue.S3MaxBytes,
	}

	payloads := messagingpostgres.NewWithPayloadKeyResolver(db, payloadKeys)
	preprocessStore := preprocesspostgres.New(db)
	tenantRepo := tenantpostgres.New(db)
	configRepo := configpostgres.New(db, tenantRepo)
	dispatcher := gateway.BrokerDispatcher{Tasks: gatewaypostgres.NewTaskStore(db), Bindings: configRepo}
	owner, _ := os.Hostname()
	owner = fmt.Sprintf("%s-%d-preprocess", valueOr(owner, "trpc-service"), os.Getpid())
	preprocessWorker := preprocess.Worker{Store: preprocessStore, Payloads: payloads, Dispatcher: dispatcher, Owner: owner,
		LeaseTTL: configValue.PreprocessLeaseTTL, RetryDelay: configValue.PreprocessRetryDelay,
		MaxAttempts: configValue.PreprocessMaxAttempts, Media: &media, ArtifactRetention: configValue.ArtifactRetention, Telemetry: telemetryProvider}

	lifecycle := worker.NewLifecycle()
	migrationReadiness := migrations.NewRunner(db)
	monitor, err := health.NewMonitor(lifecycle, []health.Dependency{
		{Name: "postgres", Probe: db.PingContext},
		{Name: "postgres_schema", Probe: migrationReadiness.Ready},
		{Name: "object_store", Probe: objects.Probe},
		{Name: "malware_scanner", Probe: malware.Probe},
		{Name: "secret_provider", Probe: func(ctx context.Context) error {
			return secretProvider.Probe(ctx, dlpSecretScope(configValue.DLPProbeTenant, configValue.DLPBackendVersion), dlpSecretRef)
		}},
		{Name: "payload_key_provider", Probe: func(ctx context.Context) error {
			value, resolveErr := payloadKeys.ResolvePayloadKey(ctx, configValue.DLPProbeTenant, configValue.PayloadKeyVersion)
			clear(value.Bytes)
			return resolveErr
		}},
		{Name: "input_dlp", Probe: dlp.Probe},
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
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}

	processCtx, cancelProcess := context.WithCancel(parent)
	defer cancelProcess()
	stopSignals := worker.InstallSignalDrain(processCtx, lifecycle)
	defer stopSignals()
	errorsCh := make(chan error, 4)
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
	start("preprocess worker", func(ctx context.Context) error {
		return runPreprocessLoop(ctx, preprocessWorker.RunOnce, configValue.PreprocessPollInterval, configValue.PreprocessBatchSize, logger)
	})
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
	logger.Printf("trpc-agent-service preprocess lifecycle/readiness listening on %s", configValue.ListenAddress)

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

func runPreprocessLoop(ctx context.Context, runOnce func(context.Context, int) (int, error), interval time.Duration, batch int, logger *roleLogger) error {
	if ctx == nil || runOnce == nil || interval <= 0 || batch < 1 || logger == nil {
		return runtime.ErrInvariantViolation
	}
	for {
		if _, err := runOnce(ctx, batch); err != nil {
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return ctx.Err()
			} else if errors.Is(err, runtime.ErrInvariantViolation) {
				return err
			} else {
				logger.Printf("preprocess worker degraded: %v", err)
			}
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

func hostSet(hosts []string) map[string]struct{} {
	result := make(map[string]struct{}, len(hosts))
	for _, host := range hosts {
		host = strings.ToLower(strings.TrimSpace(host))
		if host != "" {
			result[host] = struct{}{}
		}
	}
	return result
}
