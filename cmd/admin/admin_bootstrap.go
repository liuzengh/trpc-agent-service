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
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/adminauth"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/health"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/platformtool"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/runtimeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

func runAdmin() {
	log.Println("Starting Admin API...")
	if err := telemetry.ConfigureMetricLabelsFromEnv(); err != nil {
		log.Fatalf("configure tenant metric labels: error=%s", telemetry.StableErrorCode(err))
	}

	// Load configuration
	dbURL := requireEnv("DATABASE_URL")
	if err := storage.ValidateServicePostgresURL(dbURL, os.Getenv("DATABASE_ALLOW_INSECURE") == "true"); err != nil {
		log.Fatal("invalid DATABASE_URL")
	}
	port := getEnv("PORT", "8081")
	activeKeyID, masterKeys, err := tenant.LoadKeyRingFromEnv(32)
	if err != nil {
		log.Fatalf("configure tenant encryption key ring: error=%s", telemetry.StableErrorCode(err))
	}
	backendProfiles, err := storage.LoadBackendProfileValidator(
		requireEnv("STORAGE_BACKEND_PROFILES"),
	)
	if err != nil {
		log.Fatalf("configure storage backend profiles: error=%s", telemetry.StableErrorCode(err))
	}
	secretResolver, err := tenant.NewEnvSecretResolver("TRPC_SECRET_")
	if err != nil {
		log.Fatalf("configure secret resolver: error=%s", telemetry.StableErrorCode(err))
	}
	dataPlaneProfiles, err := runtimeplane.LoadProfileValidator(requireEnv("DATA_PLANE_PROFILES"))
	if err != nil {
		log.Fatalf("configure runtime data-plane profiles: error=%s", telemetry.StableErrorCode(err))
	}
	// Fail-closed admin auth: startup refuses weak or missing credentials.
	adminToken := requireSecret("ADMIN_API_TOKEN", 32)
	adminAuthenticator, err := adminauth.NewAuthenticator(adminToken, os.Getenv("ADMIN_PRINCIPALS_JSON"))
	if err != nil {
		log.Fatalf("configure admin authentication: error=%s", telemetry.StableErrorCode(err))
	}
	traceShutdown, err := telemetry.SetupTracing(context.Background(), "agent-admin")
	if err != nil {
		log.Fatalf("configure tracing: error=%s", telemetry.StableErrorCode(err))
	}
	defer traceShutdown(context.Background())

	// Initialize tenant repository
	tenantRepo, err := tenant.NewSQLRepository("postgres", dbURL)
	if err != nil {
		log.Fatalf("failed to create tenant repository: error=%s", telemetry.StableErrorCode(err))
	}
	// Coordinate graceful shutdown: fail readiness first, drain in-flight admin
	// requests, then close the database.
	shutdown := health.NewCoordinator()
	shutdown.OnShutdown("database", func(context.Context) error { return tenantRepo.Close() })

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
	controlDB, err := sql.Open("postgres", dbURL)
	if err != nil {
		log.Fatalf("open control-plane database: error=%s", telemetry.StableErrorCode(err))
	}
	controlDB.SetMaxOpenConns(25)
	controlDB.SetMaxIdleConns(5)
	controlDB.SetConnMaxLifetime(30 * time.Minute)
	controlDB.SetConnMaxIdleTime(5 * time.Minute)
	if err := controlDB.PingContext(context.Background()); err != nil {
		controlDB.Close()
		log.Fatalf("ping control-plane database: error=%s", telemetry.StableErrorCode(err))
	}
	shutdown.OnShutdown("control-plane-database", func(context.Context) error { return controlDB.Close() })
	inboxStore, err := reliable.OpenPostgresStore(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("open reliable message store: error=%s", telemetry.StableErrorCode(err))
	}
	shutdown.OnShutdown("reliable-message-store", func(context.Context) error { return inboxStore.Close() })
	toolCatalog, err := platformtool.NewMCPAdmissionResolver(os.Getenv("MCP_PROFILES"))
	if err != nil {
		log.Fatalf("configure MCP admission catalog: error=%s", telemetry.StableErrorCode(err))
	}
	// The admission registry describes the runtime implementations installed in
	// this deployment. The default composition intentionally exposes only the
	// bundled LLM runtime; future factories must be registered explicitly at
	// both the Admin and Worker composition roots.
	runtimeFactories := worker.NewRuntimeAgentRegistry()
	// Admin and Worker must publish against an immutable runtime composition.
	runtimeFactories.Seal()
	controlService := controlplane.NewService(controlDB, func(
		ctx context.Context,
		tenantID string,
		snapshot *controlplane.VersionSnapshot,
	) error {
		return validateVersionSnapshotWithCatalog(ctx, tenantService, toolCatalog, runtimeFactories, tenantID, snapshot, true)
	})
	// Admin reconciliation shares the same advisory-fencing authority as the
	// strict Worker. Using the compatibility recorder here would let an
	// operator decision race an in-flight Session/Memory operation.
	executionRecorder := controlplane.NewExecutionRecorderWithAdvisoryFencing(controlDB)
	approvalStore := governance.NewPostgresApprovalStore(controlDB)

	// Setup HTTP routes
	mux := http.NewServeMux()

	// Tenant CRUD
	mux.HandleFunc("/api/v1/tenants", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			listTenants(w, r, tenantService)
		case http.MethodPost:
			createTenant(w, r, tenantService)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/v1/tenants/", func(w http.ResponseWriter, r *http.Request) {
		tenantID, ok := tenantIDFromPath(r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}

		switch r.Method {
		case http.MethodGet:
			getTenant(w, r, tenantService, tenantID)
		case http.MethodPut:
			updateTenant(w, r, tenantService, tenantID)
		case http.MethodDelete:
			deleteTenant(w, r, tenantService, tenantID)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
	registerControlPlaneRoutes(mux, controlService, executionRecorder, approvalStore, inboxStore)

	healthChecker := health.New(
		health.WithDatabase(tenantRepo),
		health.WithDrainState(shutdown),
	)

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		body, statusCode := healthChecker.Report(r.Context())

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(statusCode)
		json.NewEncoder(w).Encode(body)
	})
	mux.HandleFunc("/live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	mux.Handle("/metrics", telemetry.MetricsHandlerFromEnv())

	// Only the tenant API is drain-tracked; /health and /metrics must stay
	// answerable for the whole drain.
	handler := http.NewServeMux()
	handler.Handle("/health", mux)
	handler.Handle("/live", mux)
	handler.Handle("/metrics", mux)
	protect := func(name string, permissions map[string]adminauth.Permission) http.Handler {
		tracked := shutdown.Middleware(telemetry.HTTPMiddleware(name, mux))
		return noStoreAdminResponses(adminAuthenticator.Middleware(adminauth.RequireMethods(permissions, tracked)))
	}
	handler.Handle("/api/v1/tenants", protect("admin.tenants", map[string]adminauth.Permission{
		http.MethodGet: adminauth.PermissionTenantRead, http.MethodPost: adminauth.PermissionTenantCreate,
	}))
	handler.Handle("/api/v1/tenants/", protect("admin.tenant", map[string]adminauth.Permission{
		http.MethodGet: adminauth.PermissionTenantRead, http.MethodPut: adminauth.PermissionTenantWrite,
		http.MethodDelete: adminauth.PermissionTenantDelete,
	}))
	handler.Handle("/api/v1/agent-apps", protect("admin.agent-apps", map[string]adminauth.Permission{
		http.MethodPost: adminauth.PermissionAgentWrite,
	}))
	handler.Handle("/api/v1/agent-versions", protect("admin.agent-versions", map[string]adminauth.Permission{
		http.MethodPost: adminauth.PermissionAgentWrite,
	}))
	handler.Handle("/api/v1/agent-versions/", protect("admin.agent-version", map[string]adminauth.Permission{
		http.MethodPost: adminauth.PermissionAgentPublish,
	}))
	handler.Handle("/api/v1/deployments", protect("admin.deployments", map[string]adminauth.Permission{
		http.MethodPost: adminauth.PermissionAgentDeploy,
	}))
	handler.Handle("/api/v1/execution-reconciliations", protect("admin.execution-reconciliations", map[string]adminauth.Permission{
		http.MethodPost: adminauth.PermissionExecutionReconcile,
	}))
	handler.Handle("/api/v1/outbox-replays", protect("admin.outbox-replays", map[string]adminauth.Permission{
		http.MethodPost: adminauth.PermissionOutboxReplay,
	}))
	handler.Handle("/api/v1/tool-approvals/", protect("admin.tool-approvals", map[string]adminauth.Permission{
		http.MethodGet:  adminauth.PermissionToolApprovalRead,
		http.MethodPost: adminauth.PermissionToolApprovalGrant,
	}))
	handler.Handle("/api/v1/tool-approvals", protect("admin.tool-approvals-list", map[string]adminauth.Permission{
		http.MethodGet: adminauth.PermissionToolApprovalRead,
	}))
	registerOperationsConsole(handler, controlDB, adminAuthenticator, shutdown)

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
	if err := health.ServeUntilSignal(server, shutdown, "Admin API"); err != nil {
		log.Fatalf("admin shutdown failed: error=%s", telemetry.StableErrorCode(err))
	}
}
