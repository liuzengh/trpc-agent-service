package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	redisstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector/milvus"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector/rebuild"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector/retrieval"
	vectortask "github.com/liuzengh/trpc-agent-service/trpcservice/vector/task"
)

// vectorComposition owns every production vector component. A nil
// *vectorComposition on the runtime means fully disabled: no Milvus client,
// no credential resolution, no Redis connection, no worker goroutines.
type vectorComposition struct {
	worker    *vectortask.Worker
	store     vector.VectorStore
	redis     *redisstore.Backend
	retrieval *retrieval.Service
	memory    *pgstore.MemoryRepository
	rebuild   *rebuild.Coordinator
}

// start runs the bounded VectorStore readiness gate and then starts the
// durable vector worker. It is called only when the composition exists.
func (v *vectorComposition) start(ctx context.Context) error {
	if v == nil {
		return nil
	}
	if v.store != nil {
		readyCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		err := v.store.Ready(readyCtx)
		cancel()
		if err != nil {
			return errors.New("vector store is not ready")
		}
	}
	if v.worker != nil {
		if err := v.worker.StartContext(ctx); err != nil {
			return errors.New("vector worker failed to start")
		}
	}
	return nil
}

// stop is idempotent and bounded. The worker drains first; the derived index
// and the lease backend close after the queue worker stop path completes.
func (v *vectorComposition) stopWorker(ctx context.Context) {
	if v == nil || v.worker == nil {
		return
	}
	_ = v.worker.Stop()
}

func (v *vectorComposition) close() {
	if v == nil {
		return
	}
	if v.store != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = v.store.Close(closeCtx)
		cancel()
	}
	if v.redis != nil {
		_ = v.redis.Close()
	}
}

type vectorCompositionConfig struct {
	backend          string
	workerEnabled    bool
	retrievalEnabled bool
	endpoint         string
	rebuildEnabled   bool
	credentialRef    string
	username         string
	collection       string
	model            string
	modelVersion     string
	schemaVersion    string
	dimension        int
	metric           string
	redisURL         string
	redisPrefix      string
	maxAttempts      int
	concurrency      int
}

func vectorEnv(name string) string { return strings.TrimSpace(os.Getenv(name)) }

// parseTelemetryConfig reads the server-owned telemetry configuration.
// Default is fully disabled (no exporters, no collector connections).
func parseTelemetryConfig() (telemetry.Config, error) {
	config := telemetry.Config{
		Mode:            telemetry.Mode(strings.ToLower(vectorEnv("TELEMETRY_MODE"))),
		Endpoint:        vectorEnv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		Environment:     vectorEnv("TELEMETRY_ENVIRONMENT"),
		ServiceVersion:  vectorEnv("TELEMETRY_SERVICE_VERSION"),
		InsecureLocalOK: vectorEnv("TELEMETRY_INSECURE_LOCAL") == "1",
	}
	if raw := vectorEnv("TELEMETRY_SAMPLE_RATIO"); raw != "" {
		value, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return config, fmt.Errorf("telemetry: invalid sampling ratio")
		}
		config.SampleRatio = value
	}
	return config, nil
}

func vectorEnvBool(name string) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return false, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("vector composition configuration is invalid")
	}
	return value, nil
}

func vectorEnvInt(name string, fallback, minimum, maximum int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("vector composition configuration is invalid")
	}
	return value, nil
}

// parseVectorCompositionConfig reads the server-owned vector configuration.
// Defaults are fully disabled; any enabled component with incomplete or
// out-of-bounds configuration fails closed. Values are never logged.
func parseVectorCompositionConfig() (vectorCompositionConfig, error) {
	config := vectorCompositionConfig{
		backend:       strings.ToLower(vectorEnv("VECTOR_BACKEND")),
		credentialRef: vectorEnv("MILVUS_CREDENTIAL_REF"),
		username:      vectorEnv("MILVUS_USERNAME"),
		collection:    vectorEnv("MILVUS_COLLECTION"),
		model:         vectorEnv("VECTOR_MODEL"),
		modelVersion:  vectorEnv("VECTOR_MODEL_VERSION"),
		schemaVersion: vectorEnv("VECTOR_SCHEMA_VERSION"),
		metric:        strings.ToLower(vectorEnv("VECTOR_METRIC")),
		redisURL:      vectorEnv("VECTOR_REDIS_URL"),
		redisPrefix:   vectorEnv("VECTOR_REDIS_KEY_PREFIX"),
		endpoint:      vectorEnv("MILVUS_ENDPOINT"),
	}
	if config.backend == "" {
		config.backend = vector.BackendNone
	}
	var err error
	if config.workerEnabled, err = vectorEnvBool("VECTOR_WORKER_ENABLED"); err != nil {
		return config, err
	}
	if config.retrievalEnabled, err = vectorEnvBool("RETRIEVAL_ENABLED"); err != nil {
		return config, err
	}
	if config.rebuildEnabled, err = vectorEnvBool("REBUILD_ENABLED"); err != nil {
		return config, err
	}
	if config.dimension, err = vectorEnvInt("VECTOR_DIMENSION", 0, 1, vector.MaxVectorDimension); err != nil {
		return config, err
	}
	if config.maxAttempts, err = vectorEnvInt("VECTOR_TASK_MAX_ATTEMPTS", 5, 1, 100); err != nil {
		return config, err
	}
	if config.concurrency, err = vectorEnvInt("VECTOR_WORKER_CONCURRENCY", 1, 1, 16); err != nil {
		return config, err
	}
	if config.backend != vector.BackendNone && config.backend != vector.BackendMilvus {
		return config, fmt.Errorf("vector composition configuration is invalid")
	}
	if config.workerEnabled || config.retrievalEnabled || config.rebuildEnabled {
		if config.backend != vector.BackendMilvus {
			return config, fmt.Errorf("vector composition configuration is incomplete")
		}
		if config.endpoint == "" || config.collection == "" || config.model == "" ||
			config.modelVersion == "" || config.schemaVersion == "" || config.dimension == 0 {
			return config, fmt.Errorf("vector composition configuration is incomplete")
		}
		if config.metric == "" {
			config.metric = "cosine"
		}
	}
	if (config.workerEnabled || config.rebuildEnabled) && config.redisURL == "" {
		return config, fmt.Errorf("vector composition configuration is incomplete")
	}
	if config.redisPrefix == "" {
		config.redisPrefix = "trpcvector"
	}
	return config, nil
}

// assembleTelemetry composes the process telemetry runtime. Default mode is
// none: no exporters, no collector connections, no goroutines. Failures are
// reported without echoing endpoints or credentials.
func assembleTelemetry(ctx context.Context, dependencies productionAssemblyDependencies) (*telemetry.Runtime, error) {
	if dependencies.Telemetry != nil {
		return dependencies.Telemetry, nil
	}
	config, err := parseTelemetryConfig()
	if err != nil {
		return nil, errors.New("telemetry initialization failed")
	}
	runtime, composeErr := telemetry.Compose(ctx, config, telemetry.NewJSONLogger(os.Stderr))
	if composeErr != nil {
		return nil, errors.New("telemetry initialization failed")
	}
	return runtime, nil
}

type vectorCompositionInput struct {
	pool         *pgxpool.Pool
	ownerID      string
	secret       tenant.SecretResolver
	dependencies *productionAssemblyDependencies
}

// assembleVectorComposition builds the default-disabled production vector
// components. Enabled paths require complete server-owned configuration and a
// production Embedder; missing pieces fail closed instead of falling back to
// deterministic fakes. Partial construction cleans up everything it created.
func assembleVectorComposition(ctx context.Context, input vectorCompositionInput) (*vectorComposition, error) {
	config, err := parseVectorCompositionConfig()
	if err != nil {
		return nil, err
	}
	if config.backend == vector.BackendNone && !config.workerEnabled && !config.retrievalEnabled {
		// Fully disabled: no pool, clients, credentials or goroutines.
		return nil, nil
	}
	if input.pool == nil || input.dependencies == nil {
		return nil, errors.New("vector composition initialization failed")
	}
	composition := &vectorComposition{}
	backendConfig := vector.BackendConfig{
		Mode: config.backend, EndpointRef: "env:MILVUS_ENDPOINT", Collection: config.collection,
		CredentialRef: config.credentialRef, Model: config.model, ModelVersion: config.modelVersion,
		Dimension: config.dimension, SchemaVersion: config.schemaVersion, Metric: config.metric,
		MaxInputBytes: vector.MaxContentBytes, MaxMetadataBytes: vector.MaxMetadataBytes, MaxTopK: vector.MaxTopK,
		OperationTimeout: 8 * time.Second, ReadinessTimeout: 12 * time.Second,
	}
	if err := backendConfig.Validate(); err != nil {
		return nil, errors.New("vector composition initialization failed")
	}
	if input.dependencies.VectorStore != nil {
		composition.store = input.dependencies.VectorStore
	} else {
		store, storeErr := milvus.New(ctx, backendConfig, milvusEndpointResolver{}, milvusCredentialResolver{secret: input.secret, username: config.username})
		if storeErr != nil {
			return nil, errors.New("vector composition initialization failed")
		}
		composition.store = store
	}
	taskRepository, taskErr := vectortask.NewPostgresRepository(input.pool, vectortask.RepositoryConfig{MaxAttempts: config.maxAttempts})
	if taskErr != nil {
		composition.close()
		return nil, errors.New("vector composition initialization failed")
	}
	sourceProjector, projectorErr := vectortask.NewPostgresSourceProjector(input.pool, vectortask.SourceProjectorConfig{
		Model: config.model, ModelVersion: config.modelVersion, Dimension: config.dimension, SchemaVersion: config.schemaVersion,
	})
	if projectorErr != nil {
		composition.close()
		return nil, errors.New("vector composition initialization failed")
	}
	memoryRepository, memoryErr := pgstore.NewMemoryRepository(input.pool, vectorTaskEnqueuerAdapter{tasks: taskRepository}, pgstore.MemoryProjectionConfig{
		Model: config.model, ModelVersion: config.modelVersion, Dimension: config.dimension, SchemaVersion: config.schemaVersion,
	})
	if memoryErr != nil {
		composition.close()
		return nil, errors.New("vector composition initialization failed")
	}
	composition.memory = memoryRepository
	leases := input.dependencies.VectorLeases
	if leases == nil && config.workerEnabled || config.rebuildEnabled && input.dependencies.VectorLeases == nil {
		if config.redisURL == "" {
			composition.close()
			return nil, errors.New("vector composition initialization failed")
		}
		created, redisErr := redisstore.NewBackend(redisstore.Config{URL: config.redisURL, KeyPrefix: config.redisPrefix, SessionLeaseTTL: 30 * time.Second, RenewInterval: 10 * time.Second})
		if redisErr != nil {
			composition.close()
			return nil, errors.New("vector composition initialization failed")
		}
		composition.redis = created
		leases = redisstore.NewStore(created)
	}
	if config.workerEnabled {
		if input.dependencies.VectorEmbedder == nil {
			composition.close()
			return nil, errors.New("vector composition initialization failed")
		}
		if leases == nil {
			composition.close()
			return nil, errors.New("vector composition initialization failed")
		}
		worker, workerErr := vectortask.NewWorker(vectortask.Config{
			WorkerID: input.ownerID + "-vector", Concurrency: config.concurrency,
			LeaseTTL: 30 * time.Second, TaskTimeout: 2 * time.Minute, ShutdownTimeout: 10 * time.Second,
		}, vectortask.Dependencies{
			Repository: taskRepository, Leases: leases, Store: composition.store,
			Embedder: input.dependencies.VectorEmbedder, Source: sourceProjector,
		})
		if workerErr != nil {
			composition.close()
			return nil, errors.New("vector composition initialization failed")
		}
		composition.worker = worker
	}
	if config.rebuildEnabled {
		if input.dependencies.VectorEmbedder == nil {
			composition.close()
			return nil, errors.New("vector composition initialization failed")
		}
		inspector, inspectorOK := composition.store.(vector.IdentityReader)
		if !inspectorOK {
			composition.close()
			return nil, errors.New("vector composition initialization failed")
		}
		runRepository, runRepoErr := rebuild.NewPostgresRunRepository(input.pool)
		if runRepoErr != nil {
			composition.close()
			return nil, errors.New("vector composition initialization failed")
		}
		coordinator, coordinatorErr := rebuild.NewCoordinator(rebuild.Config{Owner: input.ownerID + "-rebuild"}, rebuild.Dependencies{
			Pool: input.pool, Runs: runRepository, Tasks: vectorTaskEnqueuerAdapter{tasks: taskRepository},
			Leases: leases, Reader: inspector, Projection: vector.ProjectionConfig{
				Model: config.model, ModelVersion: config.modelVersion,
				Dimension: config.dimension, SchemaVersion: config.schemaVersion,
			},
		})
		if coordinatorErr != nil {
			composition.close()
			return nil, errors.New("vector composition initialization failed")
		}
		composition.rebuild = coordinator
	}
	if config.retrievalEnabled {
		if input.dependencies.VectorEmbedder == nil {
			composition.close()
			return nil, errors.New("vector composition initialization failed")
		}
		hydrator, hydratorErr := retrieval.NewPostgresHydrator(input.pool, retrieval.HydratorConfig{})
		if hydratorErr != nil {
			composition.close()
			return nil, errors.New("vector composition initialization failed")
		}
		service, serviceErr := retrieval.NewService(retrieval.Config{
			Model: config.model, ModelVersion: config.modelVersion, SchemaVersion: config.schemaVersion, Dimension: config.dimension,
		}, retrieval.Dependencies{Store: composition.store, Embedder: input.dependencies.VectorEmbedder, Hydrator: hydrator})
		if serviceErr != nil {
			composition.close()
			return nil, errors.New("vector composition initialization failed")
		}
		composition.retrieval = service
	}
	return composition, nil
}

// vectorTaskEnqueuerAdapter adapts the durable vector task repository to the
// narrow transaction-aware enqueue interface the Memory write path consumes.
type vectorTaskEnqueuerAdapter struct {
	tasks *vectortask.PostgresRepository
}

// EnqueueTx inserts the projection task inside the caller-owned transaction.
func (a vectorTaskEnqueuerAdapter) EnqueueTx(ctx context.Context, tx pgx.Tx, tc tenant.TenantContext, ref vector.VectorDocumentRef, now time.Time) (bool, error) {
	outcome, err := a.tasks.EnqueueTx(ctx, tx, tc, ref, now)
	if err != nil {
		return false, err
	}
	return outcome.Created, nil
}

// RedriveTx re-drives an already succeeded task of the same logical identity
// inside the caller-owned transaction.
func (a vectorTaskEnqueuerAdapter) RedriveTx(ctx context.Context, tx pgx.Tx, tc tenant.TenantContext, ref vector.VectorDocumentRef, now time.Time) (bool, error) {
	return a.tasks.RedriveTx(ctx, tx, tc, ref, now)
}

type milvusEndpointResolver struct{}

// Resolve serves the server-owned Milvus endpoint from the process
// environment. The endpoint value itself is never logged.
func (milvusEndpointResolver) Resolve(context.Context, string) (string, error) {
	endpoint := vectorEnv("MILVUS_ENDPOINT")
	if endpoint == "" {
		return "", vector.ErrInvalidConfig
	}
	return endpoint, nil
}

type milvusCredentialResolver struct {
	secret   tenant.SecretResolver
	username string
}

// Resolve serves Milvus credentials through the existing SecretRef boundary.
// Plaintext credentials never reach logs or errors.
func (r milvusCredentialResolver) Resolve(ctx context.Context, ref string) (milvus.Credentials, error) {
	if r.secret == nil || ref == "" {
		return milvus.Credentials{}, vector.ErrInvalidConfig
	}
	password, err := r.secret.Resolve(ctx, ref)
	if err != nil || password == "" {
		return milvus.Credentials{}, vector.ErrInvalidConfig
	}
	return milvus.Credentials{Username: r.username, Password: password}, nil
}
