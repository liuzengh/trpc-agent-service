//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

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
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/auth"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/health"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/migrationruntime"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/platformtool"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/resultcache"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/runtimeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	summarycoord "trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/summary"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
)

var (
	workerCacheSaturation = promauto.NewCounter(prometheus.CounterOpts{
		Name: "agent_worker_cache_saturation_total",
		Help: "Worker requests rejected because every bounded Runner cache slot was active.",
	})
	executionsReconciled = promauto.NewCounter(prometheus.CounterOpts{
		Name: "agent_execution_reconciled_total",
		Help: "Stale RUNNING execution records transitioned to ABANDONED.",
	})
	executionReconcileErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "agent_execution_reconcile_errors_total",
		Help: "Failed stale execution reconciliation passes.",
	})
	errTenantLookup        = errors.New("tenant lookup failed")
	errTenantNotFound      = errors.New("tenant not found")
	errTenantSuspended     = errors.New("tenant is suspended")
	errTenantNotActive     = errors.New("tenant is not active")
	errTenantStorageUnsafe = errors.New("tenant storage is not suitable for multi-node execution")
	errEmptyWorkerResponse = errors.New("worker returned an empty response")
)

// encodeWorkerResponse prevents a broken Processor implementation from
// turning a nil response into a durable JSON null success result.
func encodeWorkerResponse(response *worker.Response) ([]byte, error) {
	if response == nil {
		return nil, errEmptyWorkerResponse
	}
	return json.Marshal(response)
}

type workerTenantReader interface {
	GetTenant(context.Context, string) (*tenant.Tenant, error)
}

func authorizeWorkerRequest(
	ctx context.Context,
	tenants workerTenantReader,
	req worker.Request,
) (*tenant.Tenant, error) {
	t, err := tenants.GetTenant(ctx, req.TenantID)
	if err != nil {
		if errors.Is(err, tenant.ErrTenantNotFound) {
			return nil, fmt.Errorf("%w: %w", errTenantNotFound, err)
		}
		return nil, fmt.Errorf("%w: %w", errTenantLookup, err)
	}
	if t == nil {
		return nil, fmt.Errorf("%w: empty tenant result", errTenantLookup)
	}
	switch t.Status {
	case tenant.TenantStatusActive:
	case tenant.TenantStatusSuspended:
		return nil, fmt.Errorf("%w", errTenantSuspended)
	case tenant.TenantStatusDeleted:
		return nil, fmt.Errorf("%w", errTenantNotFound)
	default:
		return nil, fmt.Errorf("%w: %s", errTenantNotActive, t.Status)
	}
	if err := tenant.ValidateDistributedStorage(t.Storage); err != nil {
		return nil, fmt.Errorf("%w: %v", errTenantStorageUnsafe, err)
	}
	return t, nil
}

func writeWorkerAuthorizationError(w http.ResponseWriter, tenantID string, err error) {
	switch {
	case errors.Is(err, errTenantNotFound):
		log.Printf("tenant lookup found no active record: error=%s", telemetry.StableErrorCode(err))
		http.Error(w, "Tenant not found", http.StatusNotFound)
	case errors.Is(err, errTenantLookup):
		log.Printf("failed to load tenant: error=%s", telemetry.StableErrorCode(err))
		http.Error(w, "Tenant directory unavailable", http.StatusServiceUnavailable)
	case errors.Is(err, errTenantSuspended):
		log.Printf("rejected suspended tenant %s: error=%s", tenantID, telemetry.StableErrorCode(err))
		http.Error(w, "Tenant requires operator reconciliation", http.StatusLocked)
	case errors.Is(err, errTenantNotActive):
		log.Printf("rejected inactive tenant %s: error=%s", tenantID, telemetry.StableErrorCode(err))
		http.Error(w, "Tenant is not active", http.StatusForbidden)
	case errors.Is(err, errTenantStorageUnsafe):
		log.Printf("tenant %s has unsafe production storage: error=%s", tenantID, telemetry.StableErrorCode(err))
		http.Error(w, "Tenant storage is not suitable for multi-node execution", http.StatusServiceUnavailable)
	default:
		log.Printf("worker request authorization failed: error=%s", telemetry.StableErrorCode(err))
		http.Error(w, "Worker request authorization failed", http.StatusServiceUnavailable)
	}
}

func runWorker() {
	log.Println("Starting Agent Worker with Governance, Shared Storage, and Budget Tracking...")
	if err := telemetry.ConfigureMetricLabelsFromEnv(); err != nil {
		log.Fatalf("configure tenant metric labels: error=%s", telemetry.StableErrorCode(err))
	}

	// Load configuration
	dbURL := requireEnv("DATABASE_URL")
	if err := storage.ValidateServicePostgresURL(dbURL, os.Getenv("DATABASE_ALLOW_INSECURE") == "true"); err != nil {
		log.Fatal("invalid DATABASE_URL")
	}
	redisURL := requireEnv("REDIS_URL")
	port := getEnv("PORT", "9090")
	executionTimeout, err := parseExecutionTimeout(os.Getenv("EXECUTION_TIMEOUT"))
	if err != nil {
		log.Fatalf("configure execution timeout: error=%s", telemetry.StableErrorCode(err))
	}
	activeKeyID, masterKeys, err := tenant.LoadKeyRingFromEnv(32)
	if err != nil {
		log.Fatalf("configure tenant encryption key ring: error=%s", telemetry.StableErrorCode(err))
	}
	secretResolver, err := tenant.NewEnvSecretResolver("TRPC_SECRET_")
	if err != nil {
		log.Fatalf("configure tenant secret resolver: error=%s", telemetry.StableErrorCode(err))
	}
	serviceSecret := requireSecret("SERVICE_AUTH_SECRET", 32)
	backendProfiles, err := storage.LoadBackendProfiles(
		requireEnv("STORAGE_BACKEND_PROFILES"),
		os.LookupEnv,
	)
	if err != nil {
		log.Fatalf("configure storage backend profiles: error=%s", telemetry.StableErrorCode(err))
	}
	dataPlaneProfiles, err := runtimeplane.LoadProfiles(
		requireEnv("DATA_PLANE_PROFILES"), os.LookupEnv,
	)
	if err != nil {
		log.Fatalf("configure runtime data-plane profiles: error=%s", telemetry.StableErrorCode(err))
	}
	traceShutdown, err := telemetry.SetupTracing(context.Background(), "agent-worker")
	if err != nil {
		log.Fatalf("configure tracing: error=%s", telemetry.StableErrorCode(err))
	}
	defer traceShutdown(context.Background())
	// A shared key keeps user pseudonyms stable across worker replicas. When it
	// is omitted, telemetry emits a redaction marker instead of the raw IM ID.
	auditIdentityKey := []byte(os.Getenv("AUDIT_IDENTITY_HMAC_KEY"))

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

	databaseBudget, err := controlplane.ParseRuntimeDatabaseBudget(os.Getenv("CONTROL_DB_MAX_CONNECTIONS"), 25)
	if err != nil {
		log.Fatal("invalid CONTROL_DB_MAX_CONNECTIONS: expected 9..300")
	}
	databases, err := controlplane.OpenRuntimeDatabases(context.Background(), dbURL, controlplane.RuntimeDatabaseOptions{
		MaxConnections: databaseBudget, ExecutionFencing: true,
	})
	if err != nil {
		log.Fatalf("failed to open control-plane database: error=%s", telemetry.StableErrorCode(err))
	}
	defer databases.Close()
	controlDB := databases.Control
	migrationRuntime, err := migrationruntime.New(migrationruntime.Options{
		DB: controlDB, GateDB: databases.MigrationGate, StorageProfiles: backendProfiles, DataPlaneProfiles: dataPlaneProfiles,
	})
	if err != nil {
		_ = databases.Close()
		log.Fatalf("initialize online migration runtime: error=%s", telemetry.StableErrorCode(err))
	}
	dataPlaneResolver, err := runtimeplane.NewProfileResolver(dataPlaneProfiles, controlDB, migrationRuntime.DataPlaneOption())
	if err != nil {
		_ = migrationRuntime.Close()
		_ = databases.Close()
		log.Fatalf("initialize runtime data plane: error=%s", telemetry.StableErrorCode(err))
	}
	versionResolver := controlplane.NewPostgresResolver(controlDB)
	executionLeaseTTL := envDuration("EXECUTION_LEASE_TTL", controlplane.DefaultExecutionLeaseTTL)
	executionRecorder, err := controlplane.NewExecutionRecorderWithLeaseTTLAndAdvisoryFencing(databases.Execution, executionLeaseTTL)
	if err != nil {
		log.Fatalf("configure execution lease: error=%s", telemetry.StableErrorCode(err))
	}
	executionHeartbeatInterval := envDuration("EXECUTION_HEARTBEAT_INTERVAL", executionLeaseTTL/3)
	if executionHeartbeatInterval <= 0 || executionHeartbeatInterval >= executionLeaseTTL/2 {
		log.Fatalf("EXECUTION_HEARTBEAT_INTERVAL must be positive and less than half EXECUTION_LEASE_TTL")
	}
	resultStore := resultcache.New(controlDB)
	summaryCheckpoints := summarycoord.NewPostgresSink(controlDB)
	// Dangerous-tool approvals live in the same durable control plane as the
	// tenant and deployment records. A process-local store would let a restart
	// invalidate or, worse, split the one-time capability across replicas.
	approvalStore := governance.NewPostgresApprovalStore(controlDB)
	toolCatalog, err := platformtool.NewMCPRuntimeResolver(os.Getenv("MCP_PROFILES"), secretResolver)
	if err != nil {
		log.Fatalf("configure MCP runtime catalog: error=%s", telemetry.StableErrorCode(err))
	}
	runtimeFactories := worker.NewRuntimeAgentRegistry()
	// Freeze the process composition before accepting work. Published versions
	// must not observe a factory replacement under the same runtime type.
	runtimeFactories.Seal()

	// Initialize Redis for distributed locks and budget tracking
	redisOptions, err := parseRedisOptions(redisURL)
	if err != nil {
		log.Fatalf("invalid REDIS_URL: error=%s", telemetry.StableErrorCode(err))
	}
	redisClient := redis.NewClient(redisOptions)
	// Closed by a shutdown hook so ordering against storage is defined.

	// Test Redis connection
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("Redis is required for leases, budgets and replay protection: error=%s", telemetry.StableErrorCode(err))
	}
	serviceVerifier, err := auth.NewVerifier(serviceSecret, auth.NewRedisNonceStore(redisClient), "consumer")
	if err != nil {
		log.Fatalf("configure service authentication: error=%s", telemetry.StableErrorCode(err))
	}

	// Coordinate graceful shutdown: fail readiness first, drain in-flight
	// processing, then release dependencies in reverse registration order.
	// Redis is registered first so it is closed last: releasing storage leases
	// writes through Redis.
	shutdown := health.NewCoordinator()
	shutdown.OnShutdown("redis", func(context.Context) error { return redisClient.Close() })
	shutdown.OnShutdown("control-plane-databases", func(context.Context) error { return databases.Close() })
	shutdown.OnShutdown("migration-runtime", func(context.Context) error { return migrationRuntime.Close() })
	// MCP sessions are process-owned and shared by immutable Worker runners.
	// Register their cleanup before the Worker cache so reverse-order shutdown
	// first drains every active Runner, then closes remote MCP transports.
	shutdown.OnShutdown("mcp-tool-catalog", func(context.Context) error { return toolCatalog.Close() })
	cleanupCtx, cancelCleanup := context.WithCancel(context.Background())
	cleanupDone := make(chan error, 1)
	go func() { cleanupDone <- resultStore.RunCleanup(cleanupCtx, time.Hour) }()
	shutdown.OnShutdown("result-cache-cleanup", func(ctx context.Context) error {
		cancelCleanup()
		select {
		case err := <-cleanupDone:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	reconcileCtx, cancelReconcile := context.WithCancel(context.Background())
	reconcileDone := make(chan error, 1)
	go func() {
		reconcileDone <- executionRecorder.RunReconcilerWithObserver(
			reconcileCtx,
			envDuration("EXECUTION_RECONCILE_INTERVAL", time.Minute),
			envInt("EXECUTION_RECONCILE_BATCH", 100),
			func(rows int64) { executionsReconciled.Add(float64(rows)) },
			func(err error) {
				executionReconcileErrors.Inc()
				log.Printf("execution reconciliation failed: error=%s", telemetry.StableErrorCode(err))
			},
		)
	}()
	shutdown.OnShutdown("execution-reconciler", func(ctx context.Context) error {
		cancelReconcile()
		select {
		case err := <-reconcileDone:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	// Initialize tenant-routed storage. Worker serializes the complete session
	// invocation; the adapter must not pretend an AppendEvent-only lock protects
	// model/tool execution ordering.
	baseAdapter := storage.NewMultiTenantStorageAdapterImplWithOptions(storage.StorageCacheOptions{
		BackendProfiles:           backendProfiles,
		SessionDecorator:          migrationRuntime.SessionDecorator(),
		WriteFence:                controlplane.NewPostgresSessionFence(databases.Execution),
		ConfiguredTenants:         tenantService.ListTenants,
		RequireBackendHealthProbe: true,
	})

	// The selected shared SessionService owns durable state. Worker holds one
	// renewable distributed lease for the full tenant/user/session invocation,
	// so separate replicas do not interleave model/tool/event lifecycles.
	shutdown.OnShutdown("storage", func(context.Context) error { return baseAdapter.Close() })

	workerCache := worker.NewCache(worker.CacheOptions{
		MaxEntries: envInt("WORKER_CACHE_SIZE", 128),
		IdleTTL:    envDuration("WORKER_CACHE_IDLE_TTL", 10*time.Minute),
	})
	// Cache closes before shared storage/Redis because shutdown hooks run in
	// reverse registration order. Active cache references have already drained
	// through the HTTP shutdown middleware.
	shutdown.OnShutdown("worker-cache", workerCache.Close)
	janitorCtx, cancelJanitor := context.WithCancel(context.Background())
	janitorDone := make(chan error, 1)
	go func() {
		janitorDone <- workerCache.RunJanitor(janitorCtx, envDuration("WORKER_CACHE_SWEEP_INTERVAL", time.Minute))
	}()
	shutdown.OnShutdown("worker-cache-janitor", func(ctx context.Context) error {
		cancelJanitor()
		select {
		case err := <-janitorDone:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	// Initialize health checker with real dependency checks. The drain state is
	// registered so readiness fails as soon as shutdown starts.
	healthChecker := health.New(
		health.WithRedis(redisClient),
		health.WithDatabase(controlDB),
		health.WithDrainState(shutdown),
	)

	// Setup HTTP routes
	mux := http.NewServeMux()
	processHandler := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(worker.ExecutionContractVersionHeader, strconv.Itoa(worker.ExecutionContractVersion))
		w.Header().Set(worker.ExecutionTimeoutHeader, executionTimeout.String())
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req worker.Request
		if !decodeWorkerRequest(w, r, &req) {
			return
		}
		if !validateWorkerExecutionContract(w, &req, executionTimeout) {
			return
		}
		if traceParent := r.Header.Get("traceparent"); traceParent != "" {
			if req.Metadata == nil {
				req.Metadata = make(map[string]interface{})
			}
			req.Metadata["traceparent"] = traceParent
		}
		// Run the same content, metadata and trace boundary used by direct
		// Worker callers before resolving an immutable deployment or writing an
		// execution record. The gateway-specific identity checks remain below.
		if err := worker.ValidateRequest(&req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}
		if err := validateWorkerRequest(req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}
		// Normalize before creating the execution fence. Process performs the
		// same check for in-process callers, but the HTTP token must carry the
		// canonical group Session owner used by the Runner and Session backend.
		if err := worker.NormalizeRequestSessionOwner(&req); err != nil {
			http.Error(w, "Invalid session identity", http.StatusBadRequest)
			return
		}
		// Start one Worker-owned deadline before the first control-plane read.
		// Consumer's process timeout is intentionally larger; resetting the
		// execution budget after tenant/deployment/cache preflight could consume
		// that margin and let the Consumer cancel while Runner is still active.
		executionCtx, cancelExecution := context.WithTimeout(r.Context(), executionTimeout)
		defer cancelExecution()
		t, err := authorizeWorkerRequest(executionCtx, tenantService, req)
		if err != nil {
			writeWorkerAuthorizationError(w, req.TenantID, err)
			return
		}

		options := worker.Options{
			Collector: telemetry.NewCollectorWithAuditSinkAndIdentityKey(
				telemetry.NewDurableAuditSink(telemetry.NewSQLAuditWriter(controlDB), os.Stderr),
				auditIdentityKey,
			),
			ToolResolver:       toolCatalog,
			ApprovalStore:      approvalStore,
			ExecutionTimeout:   executionTimeout,
			SecretResolver:     secretResolver,
			RuntimeFactories:   runtimeFactories,
			SummaryCheckpoints: summaryCheckpoints,
			DataPlaneResolver:  dataPlaneResolver,
		}
		var executionHandle controlplane.ExecutionHandle
		var resolved *controlplane.ResolvedDeployment
		if req.AgentApp != "" {
			var resolveErr error
			resolved, resolveErr = versionResolver.ResolvePinnedWithPayload(
				executionCtx,
				req.TenantID,
				req.AgentApp,
				req.SessionID,
				req.IdempotencyKey,
				req.PayloadHash,
			)
			switch {
			case resolveErr == nil:
				options.Agent = &resolved.Snapshot.Agent
				options.Model = &resolved.Snapshot.Model
				options.ModelCatalogRevision = resolved.Snapshot.ModelCatalogRevision
				options.ModelContextWindow = resolved.Snapshot.ModelContextWindow
				options.AppName = resolved.AgentAppName
				options.AgentAppID = resolved.AgentAppID
				options.VersionID = resolved.VersionID
				options.DeploymentID = resolved.DeploymentID
				options.RuntimeCapabilityFingerprint = resolved.Snapshot.RuntimeCapabilityFingerprint
				req.AgentVersion = resolved.VersionID
				req.DeploymentID = resolved.DeploymentID
				identity := resultIdentity(req, resolved)
				cached, found, cacheErr := resultStore.GetScoped(executionCtx, identity)
				if cacheErr != nil {
					if errors.Is(cacheErr, resultcache.ErrPayloadConflict) ||
						errors.Is(cacheErr, resultcache.ErrRequestIdentityConflict) {
						http.Error(w, "Idempotency key conflicts with an earlier request", http.StatusConflict)
					} else {
						log.Printf("result cache lookup failed for tenant %s: error=%s", req.TenantID, telemetry.StableErrorCode(cacheErr))
						http.Error(w, "Invocation cache unavailable", http.StatusServiceUnavailable)
					}
					return
				}
				if found {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write(cached.Response)
					return
				}
				// A queue retry can arrive while an operator approval is still
				// pending. Inspect the challenge and grant atomically before opening
				// a new execution attempt; otherwise every poll would create a
				// retry-safe execution record and amplify control-plane noise.
				var admitted bool
				executionHandle, admitted = admitHTTPExecutionWithApprovalGate(
					w, executionCtx, &req, approvalStore, func() (controlplane.ExecutionHandle, error) {
						return executionRecorder.StartWithRequest(
							executionCtx, req.TenantID, req.SessionID, req.IdempotencyKey, req.PayloadHash, resolved,
						)
					},
				)
				if !admitted {
					return
				}
			case errors.Is(resolveErr, controlplane.ErrNoActiveDeployment):
				// Production execution is versioned. Falling back to a mutable
				// tenant JSON snapshot would let retries cross configuration
				// boundaries and makes audit replay non-reproducible.
				http.Error(w, "Agent app has no active deployment", http.StatusServiceUnavailable)
				return
			case errors.Is(resolveErr, controlplane.ErrPayloadConflict),
				errors.Is(resolveErr, controlplane.ErrRequestIdentityConflict):
				http.Error(w, "Idempotency key conflicts with an earlier request", http.StatusConflict)
				return
			default:
				log.Printf("failed to resolve agent deployment: error=%s", telemetry.StableErrorCode(resolveErr))
				http.Error(w, "Agent deployment unavailable", http.StatusServiceUnavailable)
				return
			}
		}

		if executionHandle.ID <= 0 || resolved == nil {
			http.Error(w, "A pinned execution is required", http.StatusServiceUnavailable)
			return
		}
		scopedAppName, scopeErr := storage.TenantScopedAppName(t, resolved.AgentAppName)
		if scopeErr != nil {
			log.Printf("failed to derive tenant-scoped app identity: error=%s", telemetry.StableErrorCode(scopeErr))
			http.Error(w, "Agent deployment identity is invalid", http.StatusServiceUnavailable)
			return
		}
		cacheKey := worker.CacheKey{
			TenantID: t.ID, TenantConfigVersion: t.ConfigVersion, AgentApp: req.AgentApp,
			AgentVersionID: resolved.VersionID, DeploymentID: resolved.DeploymentID,
		}
		// Carry the same end-to-end deadline through cache acquisition, dependency
		// initialization, and Process. Worker.Process repeats the bound for direct
		// callers, while the earlier parent deadline prevents this HTTP path from
		// resetting the budget after control-plane preflight.
		executionCtx = fence.WithToken(executionCtx, fence.Token{
			TenantID:       req.TenantID,
			AgentAppID:     resolved.AgentAppID,
			AgentAppName:   resolved.AgentAppName,
			ScopedAppName:  scopedAppName,
			UserID:         req.UserID,
			SessionOwnerID: req.SessionOwnerID,
			SessionID:      req.SessionID,
			ExecutionID:    executionHandle.ID,
			Generation:     executionHandle.Generation,
			Value:          executionHandle.Token,
		})
		heartbeatDone := make(chan error, 1)
		var heartbeatFailed atomic.Bool
		go func() {
			heartbeatErr := executionRecorder.RunHeartbeat(
				executionCtx, executionHandle, executionHeartbeatInterval,
			)
			if heartbeatErr != nil {
				heartbeatFailed.Store(true)
				cancelExecution()
			}
			heartbeatDone <- heartbeatErr
		}()
		var stopHeartbeatOnce sync.Once
		stopHeartbeat := func() {
			stopHeartbeatOnce.Do(func() {
				cancelExecution()
				if heartbeatErr := <-heartbeatDone; heartbeatErr != nil {
					log.Printf("execution heartbeat stopped for record %d: error=%s", executionHandle.ID, telemetry.StableErrorCode(heartbeatErr))
				}
			})
		}
		defer stopHeartbeat()
		// Reuse the exact immutable tenant/version Runner. Reference counting
		// prevents an idle eviction or shutdown from closing an active run.
		workerInstance, releaseWorker, err := workerCache.Acquire(executionCtx, cacheKey, func(ctx context.Context) (worker.Processor, error) {
			return worker.NewProductionWorkerWithOptionsContext(ctx, t, baseAdapter, redisClient, options)
		})
		if err != nil {
			stopHeartbeat()
			if heartbeatFailed.Load() {
				// A lost execution lease makes the attempt's side-effect boundary
				// uncertain, even if cache/backend initialization failed locally.
				// Do not let the Consumer retry it as a transient 503.
				http.Error(w, "Session requires operator reconciliation", http.StatusLocked)
				return
			}
			failure := classifyWorkerInitializationFailure(executionCtx, err)
			if executionHandle.ID != 0 {
				failExecution(r.Context(), executionRecorder, executionHandle, failure.code, failure.safeToRetry)
			}
			log.Printf("failed to create worker: error=%s", telemetry.StableErrorCode(err))
			if errors.Is(err, worker.ErrCacheSaturated) {
				workerCacheSaturation.Inc()
			}
			http.Error(w, failure.message, failure.statusCode)
			return
		}
		defer releaseWorker()

		// Process the message
		resp, err := workerInstance.Process(executionCtx, &req)
		if err != nil {
			stopHeartbeat()
			if heartbeatFailed.Load() {
				// A lease renewal failure invalidates the attempt's ownership
				// regardless of the error returned by Runner. Do not let an
				// approval or cancellation branch turn it back into a retryable
				// response.
				http.Error(w, "Session requires operator reconciliation", http.StatusLocked)
				return
			}
			if writeHTTPApprovalPause(w, err, func() {
				// Approval is an operator-gated pause, not an uncertain external
				// side effect. Mark this attempt retry-safe so the same durable
				// invocation can continue after the grant is consumed.
				if executionHandle.ID != 0 && !heartbeatFailed.Load() {
					failExecution(r.Context(), executionRecorder, executionHandle, "tool_approval_required", true)
				}
			}) {
				return
			}
			if errors.Is(err, worker.ErrApprovalResumeUnsafe) {
				if executionHandle.ID != 0 && !heartbeatFailed.Load() {
					failExecution(r.Context(), executionRecorder, executionHandle, "approval_resume_unsafe", false)
				}
				http.Error(w, "Session requires operator reconciliation", http.StatusLocked)
				return
			}
			failure := classifyWorkerProcessFailure(err)
			if executionHandle.ID != 0 && !heartbeatFailed.Load() {
				failExecution(r.Context(), executionRecorder, executionHandle, failure.code, failure.safeToRetry)
			}
			if failure.statusCode != 0 {
				http.Error(w, failure.message, failure.statusCode)
				return
			}
			log.Printf("failed to process message: error=%s", telemetry.StableErrorCode(err))
			http.Error(w, "Processing failed", http.StatusInternalServerError)
			return
		}
		encodedResponse, err := encodeWorkerResponse(resp)
		if err != nil {
			stopHeartbeat()
			if heartbeatFailed.Load() {
				http.Error(w, "Session requires operator reconciliation", http.StatusLocked)
				return
			}
			if executionHandle.ID != 0 && !heartbeatFailed.Load() {
				failureCode := "response_encoding_failed"
				if errors.Is(err, errEmptyWorkerResponse) {
					failureCode = "empty_worker_response"
				}
				failure := classifyWorkerPostRunnerFailure(failureCode)
				failExecution(r.Context(), executionRecorder, executionHandle, failure.code, failure.safeToRetry)
			}
			// Runner has already completed by this point. A malformed or empty
			// response is therefore not a retry-safe preflight failure: the
			// Session/Memory side effects may have committed even though this
			// process cannot publish a response. Stop the Inbox FIFO and require
			// reconciliation instead of letting a 5xx trigger a duplicate run.
			http.Error(w, "Session requires operator reconciliation", http.StatusLocked)
			return
		}
		stopHeartbeat()
		if heartbeatFailed.Load() {
			// The response may have been produced after the execution fence was
			// lost. It cannot be acknowledged as a successful result or retried
			// automatically; the reconciler owns the durable attempt.
			http.Error(w, "Session requires operator reconciliation", http.StatusLocked)
			return
		}
		persistCtx, cancelPersist := detachedContext(r.Context(), 5*time.Second)
		persistErr := resultStore.CommitSuccess(
			persistCtx,
			resultIdentity(req, resolved),
			executionHandle.ID,
			executionHandle.Token,
			encodedResponse,
		)
		cancelPersist()
		if persistErr != nil {
			log.Printf("result cache persistence failed: error=%s", telemetry.StableErrorCode(persistErr))
			// CommitSuccess is the atomic result/terminal transition. If it
			// cannot commit after Runner returned, neither this response nor the
			// execution outcome is safe to replay automatically. Try to record a
			// durable non-retryable failure when the database is reachable; if the
			// outage also prevents that write, the execution reconciler will
			// abandon the expired lease. In both cases the Consumer must stop the
			// Inbox FIFO rather than retrying a potentially side-effecting run.
			if executionHandle.ID != 0 {
				failure := classifyWorkerPostRunnerFailure("result_persistence_failed")
				failExecution(r.Context(), executionRecorder, executionHandle, failure.code, failure.safeToRetry)
			}
			http.Error(w, "Session requires operator reconciliation", http.StatusLocked)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(encodedResponse)
	}
	mux.HandleFunc("/process", writeLegacyProcessUnavailable)
	mux.HandleFunc(worker.ExecutionContractProcessPath, processHandler)

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		body, statusCode := healthChecker.Report(r.Context())

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set(worker.ExecutionTimeoutHeader, executionTimeout.String())
		w.WriteHeader(statusCode)
		json.NewEncoder(w).Encode(body)
	})
	mux.HandleFunc("/live", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

	mux.Handle("/metrics", telemetry.MetricsHandlerFromEnv())

	// Only process routes are authenticated and drain-tracked. /health must keep answering during the
	// drain so orchestrators can observe the not-ready transition, and /metrics
	// must stay scrapeable until the process exits.
	handler := http.NewServeMux()
	handler.Handle("/health", mux)
	handler.Handle("/live", mux)
	handler.Handle("/metrics", mux)
	protectedProcessHandler := serviceVerifier.Middleware(
		shutdown.Middleware(telemetry.HTTPMiddleware("worker.process", mux)),
	)
	registerWorkerProcessRoutes(handler, protectedProcessHandler)

	// Start HTTP server
	workerHTTPTimeout := executionTimeout + worker.ResponseCompletionGrace
	server := &http.Server{
		Addr:              ":" + port,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       130 * time.Second,
		WriteTimeout:      workerHTTPTimeout,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}

	// Run until SIGTERM/SIGINT, then stop the listener, drain in-flight
	// work, and release dependencies. See health.ServeUntilSignal.
	shutdownTimeout := envDuration("SHUTDOWN_TIMEOUT", workerHTTPTimeout+worker.ResponseCompletionGrace)
	if shutdownTimeout < workerHTTPTimeout {
		log.Fatalf("SHUTDOWN_TIMEOUT must be at least EXECUTION_TIMEOUT plus %s", worker.ResponseCompletionGrace)
	}
	if err := health.ServeUntilSignalWithTimeout(server, shutdown, "Worker", shutdownTimeout); err != nil {
		log.Fatalf("worker shutdown failed: error=%s", telemetry.StableErrorCode(err))
	}
}
