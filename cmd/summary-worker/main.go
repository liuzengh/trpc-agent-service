package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/health"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/migrationruntime"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/runtimeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	summarycoord "trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/summary"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/summaryruntime"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

var (
	summaryLatencyBuckets = []float64{.1, .25, .5, 1, 2, 5, 10, 20, 30, 60, 120, 300}
	summaryRuns           = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "agent_summary_runs_total",
		Help: "Durable Summary jobs observed by terminal processing result.",
	}, []string{"result"})
	summaryLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "agent_summary_run_duration_seconds",
		Help:    "End-to-end duration of a claimed Summary job.",
		Buckets: summaryLatencyBuckets,
	}, []string{"result"})
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("summary worker stopped: error=%s", telemetry.StableErrorCode(err))
	}
}

func run() error {
	if err := telemetry.ConfigureMetricLabelsFromEnv(); err != nil {
		return fmt.Errorf("configure metric labels: %w", err)
	}
	dbURL, err := required("DATABASE_URL")
	if err != nil {
		return err
	}
	if err := storage.ValidateServicePostgresURL(dbURL, os.Getenv("DATABASE_ALLOW_INSECURE") == "true"); err != nil {
		return errors.New("invalid DATABASE_URL")
	}
	redisURL, err := required("REDIS_URL")
	if err != nil {
		return err
	}
	profileManifest, err := required("STORAGE_BACKEND_PROFILES")
	if err != nil {
		return err
	}
	dataPlaneManifest, err := required("DATA_PLANE_PROFILES")
	if err != nil {
		return err
	}
	maxSummaryWords, err := positiveInt("SUMMARY_MAX_WORDS", 256, 4096)
	if err != nil {
		return err
	}
	maxOutputTokens, err := positiveInt("SUMMARY_MAX_OUTPUT_TOKENS", 512, 8192)
	if err != nil {
		return err
	}
	minEvents, err := positiveInt("SUMMARY_MIN_EVENTS", 20, 100000)
	if err != nil {
		return err
	}
	concurrency, err := positiveInt("SUMMARY_CONCURRENCY", 4, summarycoord.MaxPollConcurrency)
	if err != nil {
		return err
	}
	pollInterval, err := positiveDuration("SUMMARY_POLL_INTERVAL", summarycoord.DefaultPollInterval)
	if err != nil {
		return err
	}
	leaseTTL, err := positiveDuration("SUMMARY_LEASE_TTL", 3*time.Minute)
	if err != nil {
		return err
	}
	jobTimeout, err := positiveDuration("SUMMARY_JOB_TIMEOUT", summarycoord.DefaultJobTimeout)
	if err != nil {
		return err
	}
	sessionLockTTL, err := positiveDuration("SUMMARY_SESSION_LOCK_TTL", storage.DefaultLockTTL)
	if err != nil {
		return err
	}
	shutdownTimeout, err := positiveDuration("SHUTDOWN_TIMEOUT", jobTimeout+30*time.Second)
	if err != nil {
		return err
	}
	if err := health.ValidateDrainTimeout("Summary Worker", jobTimeout, shutdownTimeout, 15*time.Second); err != nil {
		return err
	}
	profiles, err := storage.LoadBackendProfiles(profileManifest, os.LookupEnv)
	if err != nil {
		return fmt.Errorf("configure storage backend profiles: %w", err)
	}
	dataPlaneProfiles, err := runtimeplane.LoadProfileValidator(dataPlaneManifest)
	if err != nil {
		return fmt.Errorf("configure runtime data-plane profiles: %w", err)
	}
	activeKeyID, masterKeys, err := tenant.LoadKeyRingFromEnv(32)
	if err != nil {
		return fmt.Errorf("configure tenant encryption key ring: %w", err)
	}
	secretResolver, err := tenant.NewEnvSecretResolver("TRPC_SECRET_")
	if err != nil {
		return fmt.Errorf("configure tenant secret resolver: %w", err)
	}
	redisOptions, err := parseRedisOptions(redisURL)
	if err != nil {
		return errors.New("invalid REDIS_URL")
	}
	redisClient := redis.NewClient(redisOptions)
	startupCtx, cancelStartup := context.WithTimeout(context.Background(), 10*time.Second)
	if err := redisClient.Ping(startupCtx).Err(); err != nil {
		cancelStartup()
		_ = redisClient.Close()
		return errors.New("Redis is unavailable")
	}
	cancelStartup()

	databaseBudget, err := controlplane.ParseRuntimeDatabaseBudget(os.Getenv("CONTROL_DB_MAX_CONNECTIONS"), 25)
	if err != nil {
		_ = redisClient.Close()
		return err
	}
	databases, err := controlplane.OpenRuntimeDatabases(context.Background(), dbURL, controlplane.RuntimeDatabaseOptions{MaxConnections: databaseBudget})
	if err != nil {
		_ = redisClient.Close()
		return fmt.Errorf("open control database: %w", err)
	}
	defer databases.Close()
	db := databases.Control

	tenantRepo, err := tenant.NewSQLRepository("postgres", dbURL)
	if err != nil {
		_ = db.Close()
		_ = redisClient.Close()
		return fmt.Errorf("open tenant repository: %w", err)
	}
	tenantService, err := tenant.NewServiceWithKeyRing(
		tenantRepo,
		activeKeyID,
		masterKeys,
		tenant.WithStorageConfigValidator(func(_ context.Context, tenantID string, config tenant.StorageConfig) error {
			if err := profiles.ValidateTenantStorage(tenantID, config); err != nil {
				return err
			}
			return dataPlaneProfiles.ValidateTenantStorage(tenantID, config)
		}),
		tenant.WithSecretResolver(secretResolver),
	)
	if err != nil {
		_ = tenantRepo.Close()
		_ = db.Close()
		_ = redisClient.Close()
		return fmt.Errorf("configure tenant service: %w", err)
	}

	migrationRuntime, err := migrationruntime.New(migrationruntime.Options{DB: db, GateDB: databases.MigrationGate, StorageProfiles: profiles})
	if err != nil {
		_ = tenantRepo.Close()
		_ = db.Close()
		_ = redisClient.Close()
		return fmt.Errorf("configure online session migration: %w", err)
	}
	defer migrationRuntime.Close()
	adapter := storage.NewMultiTenantStorageAdapterImplWithOptions(storage.StorageCacheOptions{
		BackendProfiles: profiles, ConfiguredTenants: tenantService.ListTenants,
		SessionDecorator:          migrationRuntime.SessionDecorator(),
		RequireBackendHealthProbe: true,
	})
	sink := summarycoord.NewPostgresSink(db)
	runtime, err := summaryruntime.New(summaryruntime.RuntimeOptions{
		Tenants: tenantService, Versions: controlplane.NewPostgresResolver(db), Services: adapter,
		Redis: redisClient, SecretResolver: secretResolver, Checkpoints: sink,
		MaxSummaryWords: maxSummaryWords,
		MaxOutputTokens: maxOutputTokens,
		MinEvents:       int64(minEvents),
		SessionLockTTL:  sessionLockTTL,
	})
	if err != nil {
		_ = adapter.Close()
		_ = tenantRepo.Close()
		_ = db.Close()
		_ = redisClient.Close()
		return fmt.Errorf("configure summary runtime: %w", err)
	}
	hostname, _ := os.Hostname()
	owner := env("SUMMARY_WORKER_ID", "summary-"+hostname)
	poller, err := summarycoord.NewPoller(summarycoord.PollerConfig{
		OwnerPrefix:  owner,
		Concurrency:  concurrency,
		PollInterval: pollInterval,
		LeaseTTL:     leaseTTL,
		JobTimeout:   jobTimeout,
		Store:        summarycoord.NewPostgresStore(db), Sink: sink, Generator: runtime, TargetResolver: runtime,
		Observe: observeSummary,
	})
	if err != nil {
		_ = adapter.Close()
		_ = tenantRepo.Close()
		_ = db.Close()
		_ = redisClient.Close()
		return fmt.Errorf("configure summary poller: %w", err)
	}
	traceShutdown, err := telemetry.SetupTracing(context.Background(), "agent-summary-worker")
	if err != nil {
		_ = adapter.Close()
		_ = tenantRepo.Close()
		_ = db.Close()
		_ = redisClient.Close()
		return fmt.Errorf("configure tracing: %w", err)
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = traceShutdown(flushCtx)
	}()

	runCtx, cancelRun := context.WithCancel(context.Background())
	pollerDone := make(chan error, 1)
	go func() { pollerDone <- poller.Run(runCtx) }()
	shutdown := health.NewCoordinator()
	shutdown.OnShutdown("redis", func(context.Context) error { return redisClient.Close() })
	shutdown.OnShutdown("control-databases", func(context.Context) error { return databases.Close() })
	shutdown.OnShutdown("migration-runtime", func(context.Context) error { return migrationRuntime.Close() })
	shutdown.OnShutdown("tenant-repository", func(context.Context) error { return tenantRepo.Close() })
	shutdown.OnShutdown("storage", func(context.Context) error { return adapter.Close() })
	shutdown.OnShutdown("summary-poller", func(ctx context.Context) error {
		cancelRun()
		select {
		case err := <-pollerDone:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	checker := health.New(
		health.WithRedis(redisClient), health.WithDatabase(db), health.WithDrainState(shutdown),
	)
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
		Addr: ":" + env("PORT", "9093"), Handler: mux,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second,
		WriteTimeout: 10 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	return health.ServeUntilSignalWithTimeout(server, shutdown, "Summary Worker", shutdownTimeout)
}

func observeSummary(workerID string, job summarycoord.Job, elapsed time.Duration, err error) {
	result := "completed"
	if err != nil {
		result = "failed"
		log.Printf("summary worker=%s job=%d failed: error=%s", workerID, job.ID, telemetry.StableErrorCode(err))
	}
	summaryRuns.WithLabelValues(result).Inc()
	summaryLatency.WithLabelValues(result).Observe(elapsed.Seconds())
}

func required(name string) (string, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return "", fmt.Errorf("%s is required", name)
	}
	return value, nil
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func positiveInt(name string, fallback, maximum int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 || value > maximum {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return value, nil
}

func positiveDuration(name string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return value, nil
}

func parseRedisOptions(value string) (*redis.Options, error) {
	if strings.Contains(value, "://") {
		return redis.ParseURL(value)
	}
	if strings.ContainsAny(value, "\r\n") || !strings.Contains(value, ":") {
		return nil, errors.New("invalid Redis address")
	}
	return &redis.Options{Addr: value}, nil
}
