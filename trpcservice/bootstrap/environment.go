package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/migrations"
	"github.com/XnLemon/trpc-agent-service/trpcservice/admin"
	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	runtimestorageredis "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/redis"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	sessionstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/session"
	"github.com/XnLemon/trpc-agent-service/trpcservice/web"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

const (
	envControlPlaneDriver = "TRPC_CONTROL_PLANE_DRIVER"
	envPostgresDSN        = "TRPC_POSTGRES_DSN"
	envMySQLDSN           = "TRPC_MYSQL_DSN"
	envMySQLMigrationDSN  = "TRPC_MYSQL_MIGRATION_DSN"
	// #nosec G101 -- environment variable name, not a credential.
	envAPIToken      = "TRPC_API_TOKEN"
	envAPIIdentities = "TRPC_API_IDENTITIES"
	envTenantID      = "TRPC_TENANT_ID"
	envAppID         = "TRPC_APP_ID"
	// #nosec G101 -- environment variable name, not a credential.
	envAdminToken   = "TRPC_ADMIN_TOKEN"
	envAdminTenants = "TRPC_ADMIN_TENANTS"
	// #nosec G101 -- environment variable name, not a credential.
	envAdminUsername = "TRPC_ADMIN_USERNAME"
	// #nosec G101 -- environment variable name, not a credential.
	envAdminPassword = "TRPC_ADMIN_PASSWORD"
	envSubjectID     = "TRPC_SUBJECT_ID"
	// #nosec G101 -- environment variable name, not a credential.
	envModelAPIKey = "TRPC_MODEL_API_KEY"
	// #nosec G101 -- environment variable name, not a credential.
	envModelAPIKeys      = "TRPC_MODEL_API_KEYS"
	envModelProvider     = "TRPC_MODEL_PROVIDER"
	envModelNames        = "TRPC_MODEL_NAMES"
	envModelEndpointHost = "TRPC_MODEL_ENDPOINT_HOSTS"
	// #nosec G101 -- environment variable name, not a secret.
	envModelSecretRef = "TRPC_MODEL_SECRET_REF"
	envSessionBackend = "TRPC_SESSION_BACKEND"
	envRedisAddr      = "TRPC_REDIS_ADDR"
	// #nosec G101 -- environment variable name, not a credential.
	envRedisPassword  = "TRPC_REDIS_PASSWORD"
	envRedisDB        = "TRPC_REDIS_DB"
	envRedisKeyPrefix = "TRPC_REDIS_KEY_PREFIX"
	// #nosec G101 -- environment variable name, not a secret.
	envRedisSecretRef    = "TRPC_REDIS_SECRET_REF"
	envRedisDialTimeout  = "TRPC_REDIS_DIAL_TIMEOUT"
	envRedisReadTimeout  = "TRPC_REDIS_READ_TIMEOUT"
	envRedisWriteTimeout = "TRPC_REDIS_WRITE_TIMEOUT"
	envRedisPoolSize     = "TRPC_REDIS_POOL_SIZE"
	envS3AccessKeyID     = "TRPC_S3_ACCESS_KEY_ID"
	// #nosec G101 -- environment variable name, not a secret.
	envS3SecretKey = "TRPC_S3_SECRET_KEY"
	// #nosec G101 -- environment variable name, not a secret.
	envS3SecretRef = "TRPC_S3_SECRET_REF"
	envDemoMode    = "TRPC_DEMO_MODE"
	// #nosec G101 -- environment variable name, not a secret.
	envWeComCallbackToken  = "WECOM_CALLBACK_TOKEN"
	envWeComEncodingAESKey = "WECOM_ENCODING_AES_KEY"
	// #nosec G101 -- environment variable name, not a secret.
	envWeComAppSecret = "WECOM_APP_SECRET"
	// #nosec G101 -- environment variable name, not a secret.
	envWeComSecretRef = "WECOM_SECRET_REF"
	// #nosec G101 -- environment variable name, not a credential.
	envWeComAIBotConnections = "WECOM_AIBOT_CONNECTIONS"
	envOTLPEndpoint          = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envOTLPHeaders           = "OTEL_EXPORTER_OTLP_HEADERS"
	envOTLPInsecure          = "OTEL_EXPORTER_OTLP_INSECURE"
	envOTELServiceName       = "OTEL_SERVICE_NAME"

	defaultModelProvider = "openai"
	defaultModelNames    = "gpt-4o-mini"
	defaultEndpointHost  = "api.openai.com"
	demoModelProvider    = "fake"
	demoModelName        = "deterministic"
	// #nosec G101 -- symbolic secret reference, not secret material.
	defaultModelSecretRef = "env/trpc-model-api-key"
	defaultSubjectID      = "service"
	defaultWebRoot        = "/app/web"
	maxRedisDB            = 1 << 15
)

func environmentWebRoot() string {
	if root := strings.TrimSpace(os.Getenv("TRPC_WEB_ROOT")); root != "" {
		return root
	}
	return defaultWebRoot
}

var (
	openEnvironmentDatabase                         = postgres.Open
	openMySQLEnvironmentDatabase                    = mysql.Open
	applyEnvironmentMigrations                      = migrations.Apply
	applyMySQLEnvironmentMigrations                 = migrations.ApplyMySQL
	verifyEnvironmentMigrations                     = migrations.Verify
	verifyMySQLEnvironmentMigrations                = migrations.VerifyMySQL
	newEnvironmentRuntimeStore                      = environmentRuntimeStore
	newEnvironmentRedisRuntimeStore                 = environmentRedisRuntimeStore
	newEnvironmentInMemoryFallback                  = func() environmentStorage { return runtimestorageinmemory.New() }
	newEnvironmentS3Store            s3StoreFactory = newEnvironmentS3StoreFromConfig
	environmentWeComOwnerFunc                       = environmentWeComOwner
	newEnvironmentWeComWorker                       = outbox.New
)

type s3StoreFactory func(context.Context, string, backend.CapabilityBinding, modelprofile.SecretValue) (environmentS3Store, error)

type environmentS3Store interface {
	runtimestorage.ArtifactStore
	runtimestorage.ObjectStore
	Probe(context.Context) error
}

// environmentConfig is intentionally private: it contains the one secret
// handed to the ModelFactory and must not become a serializable application
// configuration object.
type environmentConfig struct {
	driver         ControlPlaneDriver
	dsn            string
	migrationDSN   string
	apiToken       string
	apiIdentities  map[string]gateway.APIIdentity
	adminToken     string
	adminTenants   []string
	adminUsername  string
	adminPassword  string
	tenantID       string
	appID          string
	subjectID      string
	modelAPIKey    string
	modelAPIKeys   map[string]string
	modelProvider  string
	modelNames     []string
	endpointHosts  []string
	secretRef      string
	runtimeStorage string
	redis          runtimestorageredis.Config
	redisEndpoint  string
	redisSecretRef string
	s3AccessKeyID  string
	s3SecretKey    string
	s3SecretRef    string
	demoMode       bool
	wecom          *environmentWeComConfig
	wecomAIBots    []environmentWeComAIBotConfig
	telemetry      observability.Provider
	otlp           observability.OTLPConfig
}

type environmentWeComConfig struct {
	callbackToken  string
	encodingAESKey string
	appSecret      string
	secretRef      string
}

// environmentWeComAIBotConfig is one operator-owned startup connection. Its
// SecretRef must match the immutable Binding before the secret is released.
type environmentWeComAIBotConfig struct {
	BindingID string `json:"binding_id"`
	SecretRef string `json:"secret_ref"`
	BotSecret string `json:"bot_secret"`
}

// environmentRuntimeStores owns process-scoped runtime stores. The primary
// store serves ingress and outbox processing; provider stores serve Backend
// Profile capability materialization.
type environmentRuntimeStores struct {
	primary   environmentStorage
	providers map[string]environmentStorage
	owned     []environmentStorage
}

// environmentStorage is the private composition shape used while Bootstrap
// builds runtime providers. It is deliberately not exported from runtime
// storage: callers receive the narrow capability interfaces they need.
type environmentStorage interface {
	sessionstorage.SessionStateStore
	sessionstorage.EventHistoryStore
	runtimestorage.MessageStore
	runtimestorage.ReplyStore
	Close() error
}

func (stores environmentRuntimeStores) Close() error {
	var errs []error
	for _, store := range stores.owned {
		if store != nil {
			errs = append(errs, store.Close())
		}
	}
	return errors.Join(errs...)
}

func environmentPrimaryRuntimeCapabilities(runtimeStore environmentStorage) (runtimestorage.ReplyBatchEnqueuer, attachment.Reader, runtimestorage.AttachmentStore, error) {
	replyBatchStore, ok := runtimeStore.(runtimestorage.ReplyBatchEnqueuer)
	if !ok {
		return nil, nil, nil, fmt.Errorf("%w: runtime storage does not support atomic reply batches", ErrInvalidConfig)
	}
	attachments, _ := runtimeStore.(attachment.Reader)
	attachmentStore, _ := runtimeStore.(runtimestorage.AttachmentStore)
	return replyBatchStore, attachments, attachmentStore, nil
}

func environmentAdminAuthenticator(config environmentConfig) (admin.Authenticator, error) {
	staticAdmin, err := admin.NewStaticAuthenticator(config.adminToken, config.adminTenants)
	if err != nil {
		return nil, fmt.Errorf("%w: Admin authenticator configuration is invalid", ErrInvalidConfig)
	}
	if config.adminUsername == "" {
		return staticAdmin, nil
	}
	sessionAuthenticator, err := admin.NewSessionAuthenticator(config.adminUsername, config.adminPassword, staticAdmin)
	if err != nil {
		return nil, fmt.Errorf("%w: Admin session configuration is invalid", ErrInvalidConfig)
	}
	return sessionAuthenticator, nil
}

// NewFromEnvironment assembles the production bootstrap graph from explicit
// process configuration. It fails before binding an HTTP server when the
// durable control plane or required credentials are not configured.
//
//nolint:gocyclo // Bootstrap coordinates independent control-plane and runtime dependencies.
func NewFromEnvironment(ctx context.Context) (*Runtime, error) {
	if ctx == nil {
		return nil, ErrInvalidConfig
	}
	config, err := loadEnvironment()
	if err != nil {
		return nil, err
	}
	telemetry, err := observability.NewOTLPProvider(ctx, config.otlp)
	if err != nil {
		return nil, fmt.Errorf("%w: telemetry exporter configuration is invalid", ErrInvalidConfig)
	}
	config.telemetry = telemetry
	telemetryOwned := true
	defer func() {
		if telemetryOwned {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = telemetry.Shutdown(shutdownCtx)
			cancel()
		}
	}()
	modelCatalog, backendCatalog, err := environmentCatalogs(config)
	if err != nil {
		return nil, err
	}
	authenticator, err := gateway.NewStaticAPIAuthenticator(config.apiIdentities)
	if err != nil {
		return nil, fmt.Errorf("%w: API authenticator configuration is invalid", ErrInvalidConfig)
	}
	adminAuthenticator, err := environmentAdminAuthenticator(config)
	if err != nil {
		return nil, err
	}
	db, applyMigrations, verifyMigrations, err := openEnvironmentDatabaseForConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	delegateSessions := inmemory.NewSessionService()
	runtimeStores, err := newEnvironmentRuntimeStoresForConfig(ctx, config, db)
	if err != nil {
		_ = delegateSessions.Close()
		_ = db.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if config.runtimeStorage == "redis" {
			return nil, fmt.Errorf("%w: Redis runtime storage is unavailable", ErrInvalidConfig)
		}
		return nil, err
	}
	runtimeStore := runtimeStores.primary
	replyBatchStore, attachments, attachmentStore, err := environmentPrimaryRuntimeCapabilities(runtimeStore)
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStores.Close()
		_ = db.Close()
		return nil, err
	}
	replyStore, messageStore, deliveryStore := environmentPrimaryDeliveryCapabilities(runtimeStore)
	tenantRepo, appRepo, channelRepo, auditWriter, err := environmentRepositories(config, db)
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStores.Close()
		_ = db.Close()
		return nil, fmt.Errorf("%w: environment repositories: %v", ErrInvalidConfig, err)
	}
	auditWriter = metrics.WrapAuditWriter(auditWriter, config.telemetry)
	wecomFactory, wecomProvider, err := environmentWeComComponents(environmentWeComDependencies{
		config: config, channels: channelRepo, tenants: tenantRepo, apps: appRepo,
		attachments: attachmentStore, auditWriter: auditWriter,
	})
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStores.Close()
		_ = db.Close()
		return nil, fmt.Errorf("%w: wecom components: %v", ErrInvalidConfig, err)
	}
	secretRegistry, modelRegistry, backendRegistry, err := environmentRegistriesForStores(config, delegateSessions, runtimeStores)
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStores.Close()
		_ = db.Close()
		return nil, fmt.Errorf("%w: environment registries: %v", ErrInvalidConfig, err)
	}
	modelRepository := environmentModelRepository(config, db, modelCatalog)
	backendRepository := environmentBackendRepository(config, db, backendCatalog)
	// Tenant runtime is lazy: tenants created through Admin after startup are
	// materialized on their first request and can execute without a restart.
	tenantMaterializer, materializerErr := newEnvironmentTenantMaterializer(environmentTenantRuntimeOptions{
		config: config, delegateSessions: delegateSessions, runtimeStores: runtimeStores,
		secretRegistry: secretRegistry, modelRegistry: modelRegistry, backendRegistry: backendRegistry,
		controlPlane: &environmentTenantRuntimeDependencies{tenants: tenantRepo, apps: appRepo, models: modelRepository, backends: backendRepository, modelCatalog: modelCatalog, backendCatalog: backendCatalog, secrets: secretRegistry},
	})
	if materializerErr != nil {
		return nil, materializerErr
	}
	tenantRuntime, materializerErr := runtime.NewTenantRuntimeRegistry(tenantMaterializer)
	if materializerErr != nil {
		return nil, materializerErr
	}
	aiBotFactories, aiBotBindingIDs, err := environmentWeComAIBotComponents(environmentWeComAIBotDependencies{
		ctx: ctx, config: config, channels: channelRepo, tenants: tenantRepo, apps: appRepo,
	})
	if err != nil {
		_ = tenantRuntime.Close()
		_ = delegateSessions.Close()
		_ = runtimeStores.Close()
		_ = db.Close()
		return nil, fmt.Errorf("%w: wecom ai bot components: %v", ErrInvalidConfig, err)
	}
	workerFactory := environmentOutboxWorkerFactory(environmentOutboxWorkerDependencies{
		config: config, replyStore: replyStore, messageStore: messageStore, deliveryStore: deliveryStore, auditWriter: auditWriter,
		legacy: wecomProvider, aiBotBindings: aiBotBindingIDs, bindings: channelRepo,
	})
	storageFactory, err := storagefactory.NewRegistryStorageFactory(backendRegistry, secretRegistry)
	if err != nil {
		_ = tenantRuntime.Close()
		_ = delegateSessions.Close()
		_ = runtimeStores.Close()
		_ = db.Close()
		return nil, fmt.Errorf("%w: storage factory: %v", ErrInvalidConfig, err)
	}
	graph, err := NewWithDatabase(ctx, db, Config{
		OwnDB:                true,
		ControlPlaneDriver:   config.driver,
		Observability:        config.telemetry,
		Tenants:              tenantRepo,
		Apps:                 appRepo,
		Channels:             channelRepo,
		ModelCatalog:         modelCatalog,
		BackendCatalog:       backendCatalog,
		SecretResolver:       secretRegistry,
		TenantRuntime:        tenantRuntime,
		ModelFactory:         modelRegistry,
		StorageFactory:       storageFactory,
		Sessions:             delegateSessions,
		SessionStore:         runtimeStore,
		EventHistoryStore:    runtimeStore,
		MessageStore:         runtimeStore,
		ReplyBatchStore:      replyBatchStore,
		Attachments:          attachments,
		AttachmentStore:      attachmentStore,
		RuntimeTenantID:      "",
		Authenticator:        authenticator,
		AdminAuthenticator:   adminAuthenticator,
		EnableWebConnections: true,
		HTTP:                 gateway.HTTPConfig{Web: web.NewHandler(environmentWebRoot())},
		WeComHandlerFactory:  wecomFactory,
		WeComAIBotFactories:  aiBotFactories,
		OutboxWorkerFactory:  workerFactory,
		OutboxPollInterval:   time.Second,
		AuditWriter:          auditWriter,
		Ping: func(pingContext context.Context) error {
			pinger, _ := runtimeStore.(interface{ Ping(context.Context) error })
			return environmentPing(pingContext, config.driver, db, pinger)
		},
		Migrate:          applyMigrations,
		VerifyMigrations: verifyMigrations,
		CloseDependencies: func() error {
			return errors.Join(tenantRuntime.Close(), delegateSessions.Close(), runtimeStores.Close())
		},
	})
	if err != nil {
		_ = delegateSessions.Close()
		_ = runtimeStores.Close()
		_ = db.Close()
		return nil, err
	}
	telemetryOwned = false
	return graph, nil
}

func openEnvironmentDatabaseForConfig(ctx context.Context, config environmentConfig) (*sql.DB, func(context.Context, *sql.DB) error, func(context.Context, *sql.DB) error, error) {
	if config.driver != ControlPlaneDriverMySQL {
		db, err := openPostgresEnvironmentDatabaseForConfig(ctx, config)
		if err != nil {
			return nil, nil, nil, err
		}
		return db, nil, nil, nil
	}
	migrationDB, migrationErr := openMySQLEnvironmentDatabase(ctx, config.migrationDSN, mysql.Options{MaxOpenConns: 4, MaxIdleConns: 4})
	var migrationUser, migrationDatabase string
	if migrationErr == nil {
		migrationUser, migrationErr = mysql.CurrentUser(ctx, migrationDB)
	}
	if migrationErr == nil {
		migrationDatabase, migrationErr = mysql.CurrentDatabase(ctx, migrationDB)
	}
	if migrationErr == nil {
		migrationErr = applyMySQLEnvironmentMigrations(ctx, migrationDB)
	}
	if migrationErr == nil {
		migrationErr = verifyMySQLEnvironmentMigrations(ctx, migrationDB)
	}
	if migrationDB != nil {
		if closeErr := migrationDB.Close(); migrationErr == nil {
			migrationErr = closeErr
		}
	}
	if migrationErr != nil {
		if ctx.Err() != nil {
			return nil, nil, nil, ctx.Err()
		}
		return nil, nil, nil, fmt.Errorf("%w: MySQL migrations are not ready", ErrInvalidConfig)
	}
	db, err := openMySQLEnvironmentDatabase(ctx, config.dsn, mysql.Options{MaxOpenConns: 8, MaxIdleConns: 8})
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, nil, ctx.Err()
		}
		return nil, nil, nil, fmt.Errorf("%w: mysql control plane is unavailable", ErrInvalidConfig)
	}
	applicationUser, userErr := mysql.CurrentUser(ctx, db)
	applicationDatabase, databaseErr := mysql.CurrentDatabase(ctx, db)
	if userErr != nil || databaseErr != nil || applicationUser == migrationUser || applicationDatabase != migrationDatabase {
		_ = db.Close()
		if ctx.Err() != nil {
			return nil, nil, nil, ctx.Err()
		}
		return nil, nil, nil, fmt.Errorf("%w: MySQL migration and application accounts/databases are invalid", ErrInvalidConfig)
	}
	// The application account is verification-only during bootstrap; migrations
	// and trigger metadata are handled through the migration account above.
	return db, nil, nil, nil
}

func openPostgresEnvironmentDatabaseForConfig(ctx context.Context, config environmentConfig) (*sql.DB, error) {
	db, err := openEnvironmentDatabase(ctx, config.dsn, postgres.Options{MaxOpenConns: 8, MaxIdleConns: 8})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: %s control plane is unavailable", ErrInvalidConfig, config.driver)
	}
	if err := applyEnvironmentMigrations(ctx, db); err != nil {
		_ = db.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: PostgreSQL migrations are not ready", ErrInvalidConfig)
	}
	if err := verifyEnvironmentMigrations(ctx, db); err != nil {
		_ = db.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: PostgreSQL migrations are not ready", ErrInvalidConfig)
	}
	return db, nil
}
