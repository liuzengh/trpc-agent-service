package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/auth"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/health"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/pipeline"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/runtimeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

func main() {
	if err := telemetry.ConfigureMetricLabelsFromEnv(); err != nil {
		log.Fatalf("configure tenant metric labels: error=%s", telemetry.StableErrorCode(err))
	}
	dbURL := requiredEnv("DATABASE_URL")
	if err := storage.ValidateServicePostgresURL(dbURL, os.Getenv("DATABASE_ALLOW_INSECURE") == "true"); err != nil {
		log.Fatal("invalid DATABASE_URL")
	}
	workerURL := env("WORKER_ENDPOINT", "https://localhost:9090")
	workerMode, err := configuredWorkerTransportMode()
	if err != nil {
		log.Fatal("invalid WORKER_TRANSPORT_MODE")
	}
	if err := validateWorkerEndpointForMode(workerURL, workerMode); err != nil {
		// Do not include the configured URL in logs: an operator can
		// accidentally place credentials in it, even though the client rejects
		// that form.
		log.Fatal("invalid WORKER_ENDPOINT")
	}
	port := env("PORT", "9091")
	activeKeyID, masterKeys, err := tenant.LoadKeyRingFromEnv(32)
	if err != nil {
		log.Fatalf("configure tenant encryption key ring: error=%s", telemetry.StableErrorCode(err))
	}
	serviceSecret := requiredSecret("SERVICE_AUTH_SECRET", 32)
	backendProfiles, err := storage.LoadBackendProfileValidator(
		requiredEnv("STORAGE_BACKEND_PROFILES"),
	)
	if err != nil {
		log.Fatalf("configure storage backend profiles: error=%s", telemetry.StableErrorCode(err))
	}
	dataPlaneProfiles, err := runtimeplane.LoadProfileValidator(requiredEnv("DATA_PLANE_PROFILES"))
	if err != nil {
		log.Fatalf("configure runtime data-plane profiles: error=%s", telemetry.StableErrorCode(err))
	}
	hostname, _ := os.Hostname()
	owner := env("CONSUMER_ID", "consumer-"+hostname)
	traceShutdown, err := telemetry.SetupTracing(context.Background(), "agent-consumer")
	if err != nil {
		log.Fatalf("configure tracing: error=%s", telemetry.StableErrorCode(err))
	}
	defer traceShutdown(context.Background())

	store, err := reliable.OpenPostgresStore(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("open durable store: error=%s", telemetry.StableErrorCode(err))
	}
	tenantRepo, err := tenant.NewSQLRepository("postgres", dbURL)
	if err != nil {
		store.Close()
		log.Fatalf("open tenant repository: error=%s", telemetry.StableErrorCode(err))
	}
	tenantService, err := tenant.NewServiceWithKeyRing(
		tenantRepo,
		activeKeyID,
		masterKeys,
		tenant.WithStorageConfigValidator(func(_ context.Context, tenantID string, config tenant.StorageConfig) error {
			if err := backendProfiles.ValidateTenantStorage(tenantID, config); err != nil {
				return err
			}
			return dataPlaneProfiles.ValidateTenantStorage(tenantID, config)
		}),
	)
	if err != nil {
		tenantRepo.Close()
		store.Close()
		log.Fatalf("configure tenant encryption: error=%s", telemetry.StableErrorCode(err))
	}
	processTimeout, err := parseProcessTimeout(os.Getenv("PROCESS_TIMEOUT"))
	if err != nil {
		tenantRepo.Close()
		store.Close()
		log.Fatalf("configure process timeout: error=%s", telemetry.StableErrorCode(err))
	}
	expectedExecutionTimeout, err := parseExpectedWorkerExecutionTimeout(os.Getenv("WORKER_EXECUTION_TIMEOUT"))
	if err != nil {
		tenantRepo.Close()
		store.Close()
		log.Fatalf("configure Worker execution budget: error=%s", telemetry.StableErrorCode(err))
	}
	if err := worker.ValidateConsumerProcessTimeout(processTimeout, expectedExecutionTimeout); err != nil {
		tenantRepo.Close()
		store.Close()
		log.Fatalf("configure Consumer/Worker timeout budget: error=%s", telemetry.StableErrorCode(err))
	}
	signer, err := auth.NewSigner("consumer", serviceSecret)
	if err != nil {
		tenantRepo.Close()
		store.Close()
		log.Fatalf("configure service authentication: error=%s", telemetry.StableErrorCode(err))
	}
	var workerClient *worker.HTTPClient
	if workerMode == worker.WorkerTransportProduction {
		workerClient, err = worker.NewAuthenticatedProductionHTTPClientWithProcessBudget(
			workerURL,
			processTimeout,
			expectedExecutionTimeout,
			signer,
		)
		if err != nil {
			tenantRepo.Close()
			store.Close()
			log.Fatal("configure production Worker transport")
		}
	} else {
		workerClient, err = worker.NewAuthenticatedHTTPClientWithProcessBudget(
			workerURL,
			processTimeout,
			expectedExecutionTimeout,
			signer,
		)
		if err != nil {
			tenantRepo.Close()
			store.Close()
			log.Fatal("configure Worker process budget")
		}
	}
	startupTimeout := processTimeout
	if startupTimeout > 10*time.Second {
		startupTimeout = 10 * time.Second
	}
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), startupTimeout)
	err = workerClient.ValidateProcessBudget(startupCtx, processTimeout, expectedExecutionTimeout)
	cancelStartup()
	if err != nil {
		tenantRepo.Close()
		store.Close()
		log.Fatalf("worker startup health check failed: error=%s", telemetry.StableErrorCode(err))
	}
	if envBool("FAIR_QUEUE_ENABLED", false) {
		readiness, ok := interface{}(store).(reliable.FairInboxReadiness)
		if !ok {
			tenantRepo.Close()
			store.Close()
			log.Fatal("FAIR_QUEUE_ENABLED requires durable fair-queue readiness capability")
		}
		readinessCtx, cancelReadiness := context.WithTimeout(context.Background(), 10*time.Second)
		err := readiness.CheckFairInboxReady(readinessCtx)
		cancelReadiness()
		if err != nil {
			tenantRepo.Close()
			store.Close()
			log.Fatalf("FAIR_QUEUE_ENABLED requires migration 035: error=%s", telemetry.StableErrorCode(err))
		}
	}
	consumer, err := pipeline.NewConsumer(
		store,
		tenantService,
		workerClient,
		pipeline.ConsumerConfig{
			Owner:               owner,
			Concurrency:         envInt("CONCURRENCY", 4),
			FairQueue:           envBool("FAIR_QUEUE_ENABLED", false),
			PollInterval:        envDuration("POLL_INTERVAL", 250*time.Millisecond),
			LeaseDuration:       envDuration("LEASE_DURATION", 3*time.Minute),
			ProcessTimeout:      processTimeout,
			ExpiryReapInterval:  envDuration("EXPIRY_REAP_INTERVAL", time.Minute),
			ExpiryReapBatchSize: envInt("EXPIRY_REAP_BATCH_SIZE", 100),
		},
	)
	if err != nil {
		tenantRepo.Close()
		store.Close()
		log.Fatalf("configure consumer: error=%s", telemetry.StableErrorCode(err))
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	consumerDone := make(chan error, 1)
	go func() { consumerDone <- consumer.Run(runCtx) }()

	shutdown := health.NewCoordinator()
	shutdown.OnShutdown("durable-store", func(context.Context) error { return store.Close() })
	shutdown.OnShutdown("tenant-repository", func(context.Context) error { return tenantRepo.Close() })
	shutdown.OnShutdown("consumer", func(ctx context.Context) error {
		cancelRun()
		select {
		case err := <-consumerDone:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	checker := health.New(health.WithDatabase(store), health.WithDrainState(shutdown))
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		body, code := checker.Report(r.Context())
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(body)
	})
	mux.HandleFunc("/live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("/metrics", telemetry.MetricsHandlerFromEnv())

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	shutdownTimeout := envDuration("SHUTDOWN_TIMEOUT", processTimeout+30*time.Second)
	if err := health.ValidateDrainTimeout("Inbox Consumer", processTimeout, shutdownTimeout, 30*time.Second); err != nil {
		log.Fatalf("invalid shutdown configuration: error=%s", telemetry.StableErrorCode(err))
	}
	if err := health.ServeUntilSignalWithTimeout(server, shutdown, "Inbox Consumer", shutdownTimeout); err != nil {
		log.Fatalf("consumer shutdown failed: error=%s", telemetry.StableErrorCode(err))
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envInt(key string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		log.Fatalf("invalid %s", key)
	}
	return parsed
}

func envDuration(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func parseProcessTimeout(value string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return 150 * time.Second, nil
	}
	timeout, err := time.ParseDuration(value)
	if err != nil || timeout <= 0 {
		return 0, fmt.Errorf("PROCESS_TIMEOUT must be a positive duration")
	}
	return timeout, nil
}

func parseExpectedWorkerExecutionTimeout(value string) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return worker.DefaultExecutionTimeout, nil
	}
	timeout, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse WORKER_EXECUTION_TIMEOUT: %w", err)
	}
	if err := worker.ValidateExecutionTimeout(timeout); err != nil {
		return 0, err
	}
	return timeout, nil
}

func requiredSecret(key string, minimumLength int) string {
	value := os.Getenv(key)
	if len(value) < minimumLength {
		log.Fatalf("%s must be configured with at least %d characters", key, minimumLength)
	}
	return value
}

func requiredEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("%s is required", key)
	}
	return value
}

func validateWorkerEndpoint(value string) error {
	mode, err := configuredWorkerTransportMode()
	if err != nil {
		return err
	}
	return validateWorkerEndpointForMode(value, mode)
}

func configuredWorkerTransportMode() (worker.WorkerTransportMode, error) {
	raw := strings.ToLower(strings.TrimSpace(os.Getenv("WORKER_TRANSPORT_MODE")))
	if raw == "" {
		return worker.WorkerTransportProduction, nil
	}
	mode := worker.WorkerTransportMode(raw)
	switch mode {
	case worker.WorkerTransportProduction, worker.WorkerTransportDevelopment, worker.WorkerTransportMesh:
		return mode, nil
	default:
		return "", worker.ErrWorkerTransportModeInvalid
	}
}

func validateWorkerEndpointForMode(value string, mode worker.WorkerTransportMode) error {
	if mode == worker.WorkerTransportMesh && !strings.EqualFold(strings.TrimSpace(os.Getenv("WORKER_MESH_MTLS_ASSERTED")), "true") {
		return worker.ErrWorkerMeshMTLSAssertionRequired
	}
	return worker.ValidateHTTPBaseURLForMode(value, mode)
}
