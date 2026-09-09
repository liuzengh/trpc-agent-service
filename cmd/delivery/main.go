package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/health"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/pipeline"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/runtimeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func main() {
	if err := telemetry.ConfigureMetricLabelsFromEnv(); err != nil {
		log.Fatalf("configure tenant metric labels: error=%s", telemetry.StableErrorCode(err))
	}
	dbURL := requiredEnv("DATABASE_URL")
	if err := storage.ValidateServicePostgresURL(dbURL, os.Getenv("DATABASE_ALLOW_INSECURE") == "true"); err != nil {
		log.Fatal("invalid DATABASE_URL")
	}
	port := env("PORT", "9092")
	activeKeyID, masterKeys, err := tenant.LoadKeyRingFromEnv(32)
	if err != nil {
		log.Fatalf("configure tenant encryption key ring: error=%s", telemetry.StableErrorCode(err))
	}
	secretResolver, err := tenant.NewEnvSecretResolver("TRPC_SECRET_")
	if err != nil {
		log.Fatalf("configure tenant secret resolver: error=%s", telemetry.StableErrorCode(err))
	}
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
	owner := env("DELIVERY_ID", "delivery-"+hostname)
	traceShutdown, err := telemetry.SetupTracing(context.Background(), "agent-delivery")
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
		tenant.WithSecretResolver(secretResolver),
	)
	if err != nil {
		tenantRepo.Close()
		store.Close()
		log.Fatalf("configure tenant encryption: error=%s", telemetry.StableErrorCode(err))
	}
	registry := channel.NewAdapterRegistry()
	registry.Register(channel.ChannelTypeWeWork, channel.NewWeWorkAdapter())
	registry.Register(channel.ChannelTypeTelegram, channel.NewTelegramAdapter())
	if bridgeURL := os.Getenv("WECOM_BOT_BRIDGE_URL"); bridgeURL != "" {
		if err := channel.ValidateWeComBotBridgeURL(bridgeURL); err != nil {
			log.Fatal("invalid WECOM_BOT_BRIDGE_URL")
		}
		registry.Register(channel.ChannelTypeWeComBot, channel.NewWeComBotAdapter(bridgeURL))
	}

	delivery, err := pipeline.NewDelivery(store, tenantService, registry, pipeline.DeliveryConfig{
		Owner:               owner,
		Concurrency:         envInt("CONCURRENCY", 4),
		PollInterval:        envDuration("POLL_INTERVAL", 250*time.Millisecond),
		LeaseDuration:       envDuration("LEASE_DURATION", time.Minute),
		DeliveryTimeout:     envDuration("DELIVERY_TIMEOUT", 30*time.Second),
		ExpiryReapInterval:  envDuration("EXPIRY_REAP_INTERVAL", time.Minute),
		ExpiryReapBatchSize: envInt("EXPIRY_REAP_BATCH_SIZE", 100),
	})
	if err != nil {
		tenantRepo.Close()
		store.Close()
		log.Fatalf("configure delivery: error=%s", telemetry.StableErrorCode(err))
	}

	runCtx, cancelRun := context.WithCancel(context.Background())
	deliveryDone := make(chan error, 1)
	go func() { deliveryDone <- delivery.Run(runCtx) }()

	shutdown := health.NewCoordinator()
	shutdown.OnShutdown("durable-store", func(context.Context) error { return store.Close() })
	shutdown.OnShutdown("tenant-repository", func(context.Context) error { return tenantRepo.Close() })
	shutdown.OnShutdown("delivery", func(ctx context.Context) error {
		cancelRun()
		select {
		case err := <-deliveryDone:
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
	deliveryTimeout := envDuration("DELIVERY_TIMEOUT", 30*time.Second)
	shutdownTimeout := envDuration("SHUTDOWN_TIMEOUT", deliveryTimeout+30*time.Second)
	if err := health.ValidateDrainTimeout("Outbox Delivery", deliveryTimeout, shutdownTimeout, 30*time.Second); err != nil {
		log.Fatalf("invalid shutdown configuration: error=%s", telemetry.StableErrorCode(err))
	}
	if err := health.ServeUntilSignalWithTimeout(server, shutdown, "Outbox Delivery", shutdownTimeout); err != nil {
		log.Fatalf("delivery shutdown failed: error=%s", telemetry.StableErrorCode(err))
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

func envDuration(key string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(os.Getenv(key))
	if err != nil || value <= 0 {
		return fallback
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
