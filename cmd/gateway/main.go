//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/gateway"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/health"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/runtimeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

func main() {
	log.Println("Starting Agent Gateway...")
	if err := telemetry.ConfigureMetricLabelsFromEnv(); err != nil {
		log.Fatalf("configure tenant metric labels: error=%s", telemetry.StableErrorCode(err))
	}

	// Load configuration from environment
	dbURL := requireEnv("DATABASE_URL")
	if err := storage.ValidateServicePostgresURL(dbURL, os.Getenv("DATABASE_ALLOW_INSECURE") == "true"); err != nil {
		log.Fatal("invalid DATABASE_URL")
	}
	redisURL := requireEnv("REDIS_URL")
	port := getEnv("PORT", "8080")
	activeKeyID, masterKeys, err := tenant.LoadKeyRingFromEnv(32)
	if err != nil {
		log.Fatalf("configure tenant encryption key ring: error=%s", telemetry.StableErrorCode(err))
	}
	secretResolver, err := tenant.NewEnvSecretResolver("TRPC_SECRET_")
	if err != nil {
		log.Fatalf("configure tenant secret resolver: error=%s", telemetry.StableErrorCode(err))
	}
	backendProfiles, err := storage.LoadBackendProfileValidator(
		requireEnv("STORAGE_BACKEND_PROFILES"),
	)
	if err != nil {
		log.Fatalf("configure storage backend profiles: error=%s", telemetry.StableErrorCode(err))
	}
	dataPlaneProfiles, err := runtimeplane.LoadProfileValidator(requireEnv("DATA_PLANE_PROFILES"))
	if err != nil {
		log.Fatalf("configure runtime data-plane profiles: error=%s", telemetry.StableErrorCode(err))
	}
	traceShutdown, err := telemetry.SetupTracing(context.Background(), "agent-gateway")
	if err != nil {
		log.Fatalf("configure tracing: error=%s", telemetry.StableErrorCode(err))
	}
	defer traceShutdown(context.Background())

	// Initialize tenant repository
	tenantRepo, err := tenant.NewSQLRepository("postgres", dbURL)
	if err != nil {
		log.Fatalf("failed to create tenant repository: error=%s", telemetry.StableErrorCode(err))
	}
	defer tenantRepo.Close()

	// Initialize tenant service
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
		log.Fatalf("configure tenant encryption: error=%s", telemetry.StableErrorCode(err))
	}

	// Initialize Redis client
	redisOpts, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("failed to parse redis URL: error=%s", telemetry.StableErrorCode(err))
	}
	redisClient := redis.NewClient(redisOpts)
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		redisClient.Close()
		log.Fatalf("Redis is required for fail-closed tenant rate limiting: error=%s", telemetry.StableErrorCode(err))
	}

	// Coordinate graceful shutdown: fail readiness first, drain in-flight
	// webhook deliveries, then release dependencies in reverse registration
	// order. Redis is registered first so it closes last.
	shutdown := health.NewCoordinator()
	shutdown.OnShutdown("redis", func(context.Context) error { return redisClient.Close() })
	auditDB, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("failed to open Gateway audit database: error=%s", telemetry.StableErrorCode(err))
	}
	auditDB.SetMaxOpenConns(5)
	auditDB.SetMaxIdleConns(2)
	auditDB.SetConnMaxLifetime(30 * time.Minute)
	if err := auditDB.PingContext(context.Background()); err != nil {
		auditDB.Close()
		log.Fatalf("failed to ping Gateway audit database: error=%s", telemetry.StableErrorCode(err))
	}
	shutdown.OnShutdown("audit-database", func(context.Context) error { return auditDB.Close() })
	auditCollector := telemetry.NewCollectorWithAuditSinkAndIdentityKey(
		telemetry.NewDurableAuditSink(telemetry.NewSQLAuditWriter(auditDB), os.Stderr),
		[]byte(os.Getenv("AUDIT_IDENTITY_HMAC_KEY")),
	)

	// PostgreSQL is the message durability boundary. If it is unavailable the
	// Gateway must fail closed rather than acknowledge work it cannot recover.
	inboxStore, err := reliable.OpenPostgresStore(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("failed to initialize durable inbox: error=%s", telemetry.StableErrorCode(err))
	}
	shutdown.OnShutdown("durable-inbox", func(context.Context) error { return inboxStore.Close() })
	if fairQueueEnabled() {
		if _, ok := interface{}(inboxStore).(reliable.QueueAdmissionStore); !ok {
			log.Fatal("FAIR_QUEUE_ENABLED requires atomic queue admission capability")
		}
		readiness, ok := interface{}(inboxStore).(reliable.FairInboxReadiness)
		if !ok {
			log.Fatal("FAIR_QUEUE_ENABLED requires durable fair-queue readiness capability")
		}
		readinessCtx, cancelReadiness := context.WithTimeout(context.Background(), 10*time.Second)
		err := readiness.CheckFairInboxReady(readinessCtx)
		cancelReadiness()
		if err != nil {
			log.Fatalf("FAIR_QUEUE_ENABLED requires migration 035: error=%s", telemetry.StableErrorCode(err))
		}
	}

	// Initialize channel adapters
	adapterRegistry := channel.NewAdapterRegistry()
	adapterRegistry.Register(channel.ChannelTypeWeWork, channel.NewWeWorkAdapter())
	adapterRegistry.Register(channel.ChannelTypeTelegram, channel.NewTelegramAdapter())
	adapterRegistry.Register(channel.ChannelTypeWeComBot, channel.NewWeComBotAdapter(""))

	// Create gateway server with a drain-aware health checker, so readiness
	// fails as soon as shutdown starts even while Redis is still reachable.
	healthChecker := health.New(
		health.WithRedis(redisClient),
		health.WithDatabase(inboxStore),
		health.WithDrainState(shutdown),
	)
	gatewayServer := gateway.NewDurableServer(
		tenantService, adapterRegistry, redisClient, healthChecker, inboxStore,
		auditCollector,
	)

	// Setup HTTP routes
	mux := http.NewServeMux()
	mux.HandleFunc("/webhook", gatewayServer.HandleWebhook)
	mux.HandleFunc("/health", gatewayServer.HealthCheck)
	mux.HandleFunc("/live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.Handle("/metrics", telemetry.MetricsHandlerFromEnv())

	// Only /webhook is drain-tracked. /health must keep answering during the
	// drain so orchestrators observe the not-ready transition, and /metrics
	// must stay scrapeable until the process exits.
	handler := http.NewServeMux()
	handler.Handle("/health", mux)
	handler.Handle("/live", mux)
	handler.Handle("/metrics", mux)
	handler.Handle("/webhook", shutdown.Middleware(telemetry.PublicHTTPMiddleware("gateway.webhook", mux)))

	// Start HTTP server
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	// Run until SIGTERM/SIGINT, then stop the listener, drain in-flight
	// work, and release dependencies. See health.ServeUntilSignal.
	if err := health.ServeUntilSignal(server, shutdown, "Gateway"); err != nil {
		log.Fatalf("gateway shutdown failed: error=%s", telemetry.StableErrorCode(err))
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func requireEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("%s is required", key)
	}
	return value
}

func fairQueueEnabled() bool {
	value := os.Getenv("FAIR_QUEUE_ENABLED")
	if value == "" {
		return false
	}
	enabled, err := strconv.ParseBool(value)
	if err != nil {
		log.Fatal("invalid FAIR_QUEUE_ENABLED")
	}
	return enabled
}
