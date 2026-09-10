package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	_ "github.com/jackc/pgx/v5/stdlib"
	redisclient "github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/migrations"
	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/health"
	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/scanner/clamav"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/scanner/httpdlp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	secretfs "github.com/liuzengh/trpc-agent-service/trpcservice/secrets/filesystem"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets/payloadkey"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	artifactpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact/postgres"
	objectstores3 "github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore/s3"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "demo" {
		if err := runDemo(context.Background(), os.Args[2:], os.Stdout, os.Getenv); err != nil {
			fmt.Fprintf(os.Stderr, "trpc-agent-service demo failed: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "mcp-declaration-digest" {
		if err := runMCPDeclarationDigest(os.Args[2:], os.Stdout); err != nil {
			fmt.Fprintf(os.Stderr, "trpc-agent-service MCP declaration digest failed: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "mcp-binding-digests" {
		if err := runMCPBindingDigests(os.Args[2:], os.Stdout, os.Getenv); err != nil {
			fmt.Fprintf(os.Stderr, "trpc-agent-service MCP binding digests failed: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "code-executor-binding-digests" {
		if err := runCodeExecutorBindingDigests(os.Args[2:], os.Stdout, os.Getenv); err != nil {
			fmt.Fprintf(os.Stderr, "trpc-agent-service code executor binding digest failed: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "code-executor-binding-digest" {
		if err := runCodeExecutorBindingDigest(os.Args[2:], os.Stdout, os.Getenv); err != nil {
			fmt.Fprintf(os.Stderr, "trpc-agent-service code executor binding digest failed: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		fmt.Fprintf(os.Stdout, "usage: %s [demo [--confirm]|mcp-declaration-digest|mcp-binding-digests|code-executor-binding-digest|code-executor-binding-digests|demo-server|artifact|preprocess|channel|channel-delivery|gateway|admin|worker|audit-relay|audit-query|audit-purge|business-audit-purge|schema-migrate|session-migrate|knowledge-migrate|memory-migrate|audit-compliance-migrate|webui-local|webui-local-bootstrap|im-local|wecom-local|prestop]\n", os.Args[0])
		fmt.Fprintln(os.Stdout, "Runs the selected production dependency/readiness process (artifact is the default).")
		return
	}
	role := "artifact"
	if len(os.Args) > 1 {
		role = os.Args[1]
	}
	logger, err := servicelog.NewFromEnv(os.Getenv, os.Stderr, role)
	if err != nil {
		fmt.Fprintf(os.Stderr, "trpc-agent-service logging configuration rejected: %v\n", err)
		os.Exit(2)
	}
	processContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runRole(processContext, os.Getenv, logger, role); err != nil {
		logger.Printf("trpc-agent-service stopped: %v", err)
		os.Exit(1)
	}
}

type roleLogger = servicelog.Logger

func run(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	return runRole(parent, getenv, logger, "artifact")
}

func runRole(parent context.Context, getenv func(string) string, logger *roleLogger, role string) error {
	switch role {
	case "artifact":
		return runArtifactRole(parent, getenv, logger)
	case "preprocess":
		return runPreprocessRole(parent, getenv, logger)
	case "channel":
		return runChannelRole(parent, getenv, logger)
	case "channel-delivery":
		return runChannelDeliveryRole(parent, getenv, logger)
	case "gateway":
		return runGatewayRole(parent, getenv, logger)
	case "admin":
		return runAdminRole(parent, getenv, logger)
	case "worker":
		return runWorkerRole(parent, getenv, logger)
	case "audit-relay":
		return runAuditRelayRole(parent, getenv, logger)
	case "audit-query":
		return runAuditQueryRole(parent, getenv, logger)
	case "audit-purge":
		return runAuditPurgeRole(parent, getenv, logger)
	case "business-audit-purge":
		return runBusinessAuditPurgeRole(parent, getenv, logger)
	case "schema-migrate":
		return runSchemaMigrate(parent, getenv, logger)
	case "session-migrate":
		return runSessionMigrate(parent, getenv, logger)
	case "knowledge-migrate":
		return runKnowledgeMigrate(parent, getenv, logger)
	case "memory-migrate":
		return runMemoryMigrate(parent, getenv, logger)
	case "audit-compliance-migrate":
		return runAuditComplianceMigrate(parent, getenv, logger)
	case "prestop":
		return runPreStop(getenv, syscall.Kill, time.Sleep)
	case "webui-local":
		return runWebUILocalRole(parent, getenv, logger)
	case "webui-local-bootstrap":
		return runWebUILocalBootstrap(parent, getenv, logger)
	case "demo-server":
		return runDemoServer(parent, getenv, logger)
	case "im-local":
		return runWebUILocalRole(parent, getenv, logger)
	case "wecom-local":
		return runWebUILocalRole(parent, getenv, logger)
	default:
		return errors.New("unsupported service role")
	}
}

func runArtifactRole(parent context.Context, getenv func(string) string, logger *roleLogger) error {
	if parent == nil || logger == nil {
		return errors.New("invalid process dependencies")
	}
	config, err := loadProductionConfig(getenv)
	if err != nil {
		return fmt.Errorf("configuration rejected: %w", err)
	}
	db, err := sql.Open("pgx", config.PostgresDSN)
	if err != nil {
		return errors.New("postgres client initialization failed")
	}
	defer db.Close()
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetMaxIdleConns(8)
	db.SetMaxOpenConns(32)

	redis := redisclient.NewClient(&redisclient.Options{Addr: config.RedisAddress, Password: config.RedisPassword, DB: config.RedisDB})
	defer redis.Close()

	awsConfig, err := awsconfig.LoadDefaultConfig(parent, awsconfig.WithRegion(config.S3Region))
	if err != nil {
		return errors.New("AWS SDK configuration failed")
	}
	objects, err := objectstores3.NewFromConfig(awsConfig, config.S3Bucket, config.S3Endpoint, config.S3PathStyle,
		objectstores3.Options{MaxBytes: config.S3MaxBytes, AllowInsecure: config.S3AllowInsecure})
	if err != nil {
		return errors.New("object store configuration rejected")
	}
	malware := clamav.Scanner{Address: config.ClamAVAddress, MaxBytes: int(config.S3MaxBytes)}
	secretProvider, err := secretfs.New(config.SecretRoot, 64<<10)
	if err != nil {
		return errors.New("secret provider configuration rejected")
	}
	dlpSecretRef := secrets.SecretRef{Ref: config.DLPSecretRef, Version: config.DLPSecretVersion}
	dlpAuthorize, err := newDLPAuthorizer(secretProvider, config.DLPBackendVersion, dlpSecretRef)
	if err != nil {
		return errors.New("DLP secret authorization configuration rejected")
	}
	payloadKeys, err := payloadkey.New(secretProvider, config.PayloadKeyRef)
	if err != nil {
		return errors.New("payload key configuration rejected")
	}
	dlpClient := &http.Client{}
	dlp := httpdlp.Scanner{Endpoint: config.DLPEndpoint, Client: dlpClient, ProbeTenantID: config.DLPProbeTenant,
		MaxBytes: int(config.S3MaxBytes), AllowInsecure: config.DLPAllowInsecure, Authorize: dlpAuthorize}

	lifecycle := worker.NewLifecycle()
	migrationReadiness := migrations.NewRunner(db)
	monitor, err := health.NewMonitor(lifecycle, []health.Dependency{
		{Name: "postgres", Probe: db.PingContext},
		{Name: "postgres_schema", Probe: migrationReadiness.Ready},
		{Name: "redis", Probe: func(ctx context.Context) error { return redis.Ping(ctx).Err() }},
		{Name: "object_store", Probe: objects.Probe},
		{Name: "malware_scanner", Probe: malware.Probe},
		{Name: "secret_provider", Probe: func(ctx context.Context) error {
			return secretProvider.Probe(ctx, dlpSecretScope(config.DLPProbeTenant, config.DLPBackendVersion), dlpSecretRef)
		}},
		{Name: "payload_key_provider", Probe: func(ctx context.Context) error {
			value, err := payloadKeys.ResolvePayloadKey(ctx, config.DLPProbeTenant, config.PayloadKeyVersion)
			clear(value.Bytes)
			return err
		}},
		{Name: "input_dlp", Probe: dlp.Probe},
	}, config.ProbeTimeout, config.ProbeInterval)
	if err != nil {
		return errors.New("readiness configuration rejected")
	}
	if err := monitor.ProbeOnce(parent); err != nil {
		return errors.New("initial dependency probe interrupted")
	}

	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return errors.New("HTTP listener initialization failed")
	}
	defer listener.Close()
	mux := http.NewServeMux()
	mux.Handle("/livez", health.Handler{Checker: monitor})
	mux.Handle("/readyz", health.Handler{Checker: monitor})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second,
		WriteTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}

	artifactStore := artifactpostgres.NewWithObjectStoreOptions(db, objects, artifactpostgres.ObjectStoreOptions{
		PutTimeout: config.ArtifactPutTimeout, UploadProtection: config.UploadProtection})
	owner, _ := os.Hostname()
	owner = fmt.Sprintf("%s-%d", valueOr(owner, "trpc-service"), os.Getpid())
	uploads := artifact.ObjectLifecycleReconciler{Store: artifactStore, Objects: objects, Owner: owner + "-upload",
		ClaimTTL: config.UploadClaimTTL, BatchSize: config.LifecycleBatchSize, MaxAttempts: config.LifecycleMaxAttempts,
		PollInterval: config.UploadPollInterval, OnError: func(_ context.Context, err error) {
			logger.Printf("artifact upload lifecycle degraded: %v", err)
		}, OnQuarantined: func(_ context.Context, value artifact.ObjectUpload, _ error) {
			logger.Printf("artifact upload quarantined tenant=%q artifact=%q", value.TenantID, value.ArtifactID)
		}}
	retention := artifact.RetentionReconciler{Store: artifactStore, Objects: objects, Owner: owner + "-retention",
		OrphanGrace: config.ArtifactOrphanGrace, ClaimTTL: config.ArtifactClaimTTL, BatchSize: config.LifecycleBatchSize,
		MaxAttempts: config.LifecycleMaxAttempts, PollInterval: config.ArtifactPollInterval,
		OnError: func(_ context.Context, err error) { logger.Printf("artifact retention lifecycle degraded: %v", err) },
		OnQuarantined: func(_ context.Context, value artifact.RetainedArtifact, _ error) {
			logger.Printf("artifact retention quarantined tenant=%q artifact=%q", value.TenantID, value.ArtifactID)
		}}

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
			if err := operation(processCtx); err != nil && !errors.Is(err, context.Canceled) {
				errorsCh <- fmt.Errorf("%s stopped", name)
			}
		}()
	}
	start("readiness monitor", monitor.Run)
	start("artifact upload lifecycle", uploads.Run)
	start("artifact retention lifecycle", retention.Run)
	background.Add(1)
	go func() {
		defer background.Done()
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errorsCh <- errors.New("HTTP server stopped")
		}
	}()
	if err := lifecycle.MarkReady(); err != nil {
		return errors.New("process lifecycle transition failed")
	}
	logger.Printf("trpc-agent-service %s lifecycle/readiness listening on %s", trpcservice.Version, config.ListenAddress)

	var terminalErr error
	select {
	case <-parent.Done():
		lifecycle.BeginDrain()
	case <-lifecycle.Drain():
	case terminalErr = <-errorsCh:
		lifecycle.BeginDrain()
	}
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), config.ShutdownTimeout)
	defer cancelShutdown()
	cancelProcess()
	if err := server.Shutdown(shutdownCtx); err != nil && terminalErr == nil {
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
