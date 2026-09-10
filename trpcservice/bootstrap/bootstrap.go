// Package bootstrap assembles the durable control-plane and the real Gateway
// execution spine. It keeps ownership explicit so tests can inject fakes and
// production callers can decide which resources to close.
package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/admin"
	agentrunnerfactory "github.com/XnLemon/trpc-agent-service/trpcservice/agent/runnerfactory"
	agentsessionstore "github.com/XnLemon/trpc-agent-service/trpcservice/agent/sessionstore"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	appmemory "github.com/XnLemon/trpc-agent-service/trpcservice/app/inmemory"
	appmysql "github.com/XnLemon/trpc-agent-service/trpcservice/app/mysql"
	apppostgres "github.com/XnLemon/trpc-agent-service/trpcservice/app/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	backendmysql "github.com/XnLemon/trpc-agent-service/trpcservice/backend/mysql"
	backendpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/backend/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	channelmysql "github.com/XnLemon/trpc-agent-service/trpcservice/channels/mysql"
	channelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/channels/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	modelmysql "github.com/XnLemon/trpc-agent-service/trpcservice/model/mysql"
	modelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/model/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimebudget "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
	runtimebudgetinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget/inmemory"
	runtimebudgetpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget/postgres"
	runtimequeue "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/queue"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	sessionstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/session"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
	tenantmysql "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/mysql"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

var (
	// ErrInvalidConfig is returned when an explicit bootstrap dependency is
	// missing or cannot be assembled.
	ErrInvalidConfig = errors.New("invalid bootstrap configuration")
	// ErrBootstrapNotReady is used by the deliberately unconfigured local
	// server returned by NewUnavailable.
	ErrBootstrapNotReady = errors.New("bootstrap dependencies are not configured")
)

// ControlPlaneDriver selects the durable control-plane SQL adapter. The zero
// value intentionally preserves the historical PostgreSQL default.
type ControlPlaneDriver string

const (
	// ControlPlaneDriverPostgres selects the PostgreSQL control-plane adapter.
	ControlPlaneDriverPostgres ControlPlaneDriver = "postgres"
	// ControlPlaneDriverMySQL selects the MySQL control-plane adapter.
	ControlPlaneDriverMySQL ControlPlaneDriver = "mysql"
)

// Config is the complete, explicit dependency boundary for one process.
// Repositories are optional only when DB is supplied; they are then built for
// the selected ControlPlaneDriver (PostgreSQL by default). All other runtime dependencies are required so
// a successful bootstrap always creates a real Resolver, Registry and HTTP
// handler.
type Config struct {
	// Observability is the process telemetry provider. A nil value selects a no-op provider.
	Observability observability.Provider
	DB            *sql.DB
	OwnDB         bool
	// ControlPlaneDriver selects the repository implementation used when DB is
	// supplied and repositories are not injected. Empty means PostgreSQL.
	ControlPlaneDriver ControlPlaneDriver
	Tenants            tenant.Repository
	Apps               appmodel.Repository
	Models             modelprofile.Repository
	Backends           backend.Repository
	Channels           channels.CandidateConsumer

	ModelCatalog   *modelprofile.ProviderCatalog
	BackendCatalog *backend.ProviderCatalog
	// TenantRuntime lazily materializes tenant-scoped runtime capabilities.
	// When provided, newly created tenants can execute without a restart.
	TenantRuntime  runtime.TenantRuntime
	SecretResolver modelprofile.SecretResolver
	ModelFactory   modelprofile.ModelFactory
	StorageFactory storagefactory.StorageFactory
	Sessions       session.Service
	// ToolRegistry resolves published revision authorizations to installed,
	// context-bound platform tools. A nil value uses the built-in registry.
	ToolRegistry *servicetool.Registry
	// SessionStore is the session-state capability used by durable dispatch.
	SessionStore sessionstorage.SessionStateStore
	// EventHistoryStore is the immutable upstream event history capability used
	// when Bootstrap wraps an upstream Session service for durable recovery.
	EventHistoryStore sessionstorage.EventHistoryStore
	// MessageStore is the inbound message lifecycle capability used by durable
	// dispatch.
	MessageStore runtimestorage.MessageStore
	// ReplyBatchStore is the atomic reply materialization capability used by the
	// outbox materializer.
	ReplyBatchStore runtimestorage.ReplyBatchEnqueuer
	// BudgetStore is the atomic monthly token/cost ledger. A nil value selects
	// the PostgreSQL ledger when DB is configured, otherwise an in-memory
	// ledger for local/single-process deployments.
	BudgetStore runtimebudget.Store
	// Attachments loads verified tenant-owned media during Gateway dispatch.
	// It is kept separate from the session/message/reply capabilities.
	Attachments attachment.Reader
	// AttachmentStore binds and stores verified tenant-owned media.
	AttachmentStore runtimestorage.AttachmentStore
	// RuntimeTenantID fixes the tenant scope when Bootstrap wraps Sessions with
	// the explicitly supplied session persistence capabilities. It must come
	// from trusted config.
	RuntimeTenantID string
	// OutboxWorker is constructed from trusted provider routing configuration.
	// Bootstrap owns its lifecycle but never derives a recipient from HTTP.
	OutboxWorker *outbox.Worker
	// OutboxWorkerFactory creates the owned worker after AI Bot factories have
	// produced their managers. It is mutually exclusive with OutboxWorker.
	OutboxWorkerFactory func([]channels.PollingAdapter) (*outbox.Worker, error)
	OutboxPollInterval  time.Duration
	// ExecutionQueue is an optional generic durable execution worker. The
	// caller constructs its task handler; Bootstrap only owns its lifecycle and
	// closes it before the Runner Registry.
	ExecutionQueue *runtimequeue.Worker
	// AuditWriter receives execution and configured channel delivery facts.
	AuditWriter        audit.Writer
	Authenticator      gateway.APIAuthenticator
	AdminAuthenticator admin.Authenticator
	// EnableWebConnections enables process-owned channel onboarding through Admin.
	EnableWebConnections bool
	AdminHandler         http.Handler
	WeComHandler         http.Handler
	// WeComHandlerFactory is called after Dispatcher construction so a callback
	// handler cannot receive an uninitialized execution dependency.
	WeComHandlerFactory func(gateway.DispatchService) (http.Handler, error)
	// WeComAIBotFactories constructs every configured AI Bot connection after
	// Dispatcher construction. Returned adapters are owned by Runtime.
	WeComAIBotFactories []func(gateway.DispatchService) (channels.PollingAdapter, error)

	Registry          runtimerunner.RunnerRegistryConfig
	HTTP              gateway.HTTPConfig
	DrainTimeout      time.Duration
	ReadyGate         func() bool
	Ping              func(context.Context) error
	Migrate           func(context.Context, *sql.DB) error
	VerifyMigrations  func(context.Context, *sql.DB) error
	CloseDependencies func() error
}

// Runtime is the owned bootstrap graph. Handler.BeginShutdown should happen
// before the HTTP server is drained; Close then closes the Runner Registry and
// only after that resources explicitly owned by this graph.
type Runtime struct {
	Handler          *gateway.HTTPHandler
	Resolver         *gateway.PlanResolver
	Registry         *runtimerunner.RunnerRegistry
	Dispatcher       *gateway.Dispatcher
	OutboxWorker     *outbox.Worker
	ExecutionQueue   *runtimequeue.Worker
	wecomLifecycle   callbackLifecycle
	wecomHandler     http.Handler
	wecomAIBots      []channels.PollingAdapter
	connections      admin.ChannelConnections
	connectionsClose func() error
	aiBotDone        []chan struct{}
	aiBotCancel      context.CancelFunc

	db               *sql.DB
	ownDB            bool
	readyGate        func() bool
	ping             func(context.Context) error
	verifyMigrations func(context.Context, *sql.DB) error
	closeDeps        func() error
	telemetry        observability.Provider
	closing          atomic.Bool
	closeOnce        sync.Once
	closeErr         error
}

type callbackLifecycle interface {
	BeginShutdown()
	Close() error
}

type bootstrapSessionPersistence struct {
	sessionstorage.SessionStateStore
	sessionstorage.EventHistoryStore
}

type pollingHealth interface{ Ready() bool }

// NewWithDatabase is the normal constructor for a real PostgreSQL bootstrap.
// A concrete *sql.DB is accepted here while Config keeps the remainder of the
// dependency graph easy to construct in tests.
func NewWithDatabase(ctx context.Context, db *sql.DB, config Config) (*Runtime, error) {
	config.DB = db
	return New(ctx, config)
}

// NewWithDatabaseDriver is the explicit constructor for a selected SQL
// control-plane adapter. Existing callers should continue using
// NewWithDatabase, which defaults to PostgreSQL.
func NewWithDatabaseDriver(ctx context.Context, db *sql.DB, driver ControlPlaneDriver, config Config) (*Runtime, error) {
	config.DB = db
	config.ControlPlaneDriver = driver
	return New(ctx, config)
}

// New assembles the real PlanResolver, Runtime RunnerRegistry, Dispatcher and
// HTTPHandler from explicit dependencies.
func New(ctx context.Context, config Config) (*Runtime, error) {
	if ctx == nil {
		return nil, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := prepareDatabaseConfig(ctx, &config); err != nil {
		return nil, err
	}
	if err := validateConfig(config); err != nil {
		return nil, err
	}
	if err := prepareRuntimeConfig(&config); err != nil {
		return nil, err
	}
	config.AuditWriter = metrics.WrapAuditWriter(config.AuditWriter, config.Observability)
	runtimeGraph, err := newRuntimeGraph(config)
	if err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = runtimeGraph.Close()
		}
	}()
	if err := configureAdmin(&config, runtimeGraph.Registry, runtimeGraph.connections); err != nil {
		return nil, err
	}
	if err := configureHandler(runtimeGraph, config); err != nil {
		return nil, err
	}
	if err := startOutboxWorker(runtimeGraph, config.OutboxPollInterval); err != nil {
		return nil, err
	}
	if err := startExecutionQueue(runtimeGraph); err != nil {
		return nil, err
	}
	if err := startAIBots(runtimeGraph); err != nil {
		return nil, err
	}
	committed = true
	return runtimeGraph, nil
}

func prepareDatabaseConfig(ctx context.Context, config *Config) error {
	if config.DB == nil {
		return nil
	}
	driver, err := resolveControlPlaneDriver(config)
	if err != nil {
		return ErrInvalidConfig
	}
	if err := config.DB.PingContext(ctx); err != nil {
		if driver == ControlPlaneDriverMySQL {
			return mysql.ErrStorage
		}
		return postgres.ErrStorage
	}
	if driver == ControlPlaneDriverMySQL {
		return prepareMySQLDatabaseConfig(ctx, config)
	}
	return preparePostgresDatabaseConfig(ctx, config)
}

func resolveControlPlaneDriver(config *Config) (ControlPlaneDriver, error) {
	driver := config.ControlPlaneDriver
	if driver == "" {
		driver = ControlPlaneDriverPostgres
		config.ControlPlaneDriver = driver
	}
	if driver != ControlPlaneDriverPostgres && driver != ControlPlaneDriverMySQL {
		return "", ErrInvalidConfig
	}
	return driver, nil
}

func prepareMySQLDatabaseConfig(ctx context.Context, config *Config) error {
	if config.Migrate != nil {
		if err := config.Migrate(ctx, config.DB); err != nil {
			return ErrInvalidConfig
		}
	}
	if config.VerifyMigrations != nil {
		if err := config.VerifyMigrations(ctx, config.DB); err != nil {
			return ErrInvalidConfig
		}
	}
	if err := mysql.VerifyApplicationPrivileges(ctx, config.DB); err != nil {
		return ErrInvalidConfig
	}
	if config.Tenants == nil {
		config.Tenants = tenantmysql.NewRepository(config.DB)
	}
	if config.Apps == nil {
		config.Apps = appmysql.NewAppRepository(config.DB)
	}
	if config.Models == nil {
		config.Models = modelmysql.NewRepository(config.DB, config.ModelCatalog)
	}
	if config.Backends == nil {
		config.Backends = backendmysql.NewRepository(config.DB, config.BackendCatalog)
	}
	if config.Channels == nil {
		config.Channels = channelmysql.NewRepository(config.DB)
	}
	return nil
}

func preparePostgresDatabaseConfig(ctx context.Context, config *Config) error {
	if config.Migrate != nil {
		if err := config.Migrate(ctx, config.DB); err != nil {
			return ErrInvalidConfig
		}
	}
	if config.Tenants == nil {
		config.Tenants = tenantpostgres.NewRepository(config.DB)
	}
	if config.Apps == nil {
		config.Apps = apppostgres.NewAppRepository(config.DB)
	}
	if config.Models == nil {
		config.Models = modelpostgres.NewRepository(config.DB, config.ModelCatalog)
	}
	if config.Backends == nil {
		config.Backends = backendpostgres.NewRepository(config.DB, config.BackendCatalog)
	}
	if config.Channels == nil {
		config.Channels = channelpostgres.NewRepository(config.DB)
	}
	return nil
}

func validateConfig(config Config) error {
	dependencies := []any{
		config.Tenants, config.Apps, config.Models, config.Backends, config.Channels,
		config.ModelCatalog, config.BackendCatalog, config.SecretResolver, config.ModelFactory,
		config.Authenticator,
	}
	for _, dependency := range dependencies {
		if dependency == nil {
			return ErrInvalidConfig
		}
	}
	if config.Sessions == nil && config.StorageFactory == nil {
		return ErrInvalidConfig
	}
	return nil
}

func prepareRuntimeConfig(config *Config) error {
	if err := prepareBudgetConfig(config); err != nil {
		return err
	}
	if config.BudgetStore == nil {
		config.BudgetStore = runtimebudgetinmemory.New()
	}
	if config.SessionStore == nil && config.EventHistoryStore == nil && config.MessageStore == nil && config.ReplyBatchStore == nil {
		store := runtimestorageinmemory.New()
		config.SessionStore = store
		config.EventHistoryStore = store
		config.MessageStore = store
		config.ReplyBatchStore = store
		if config.Attachments == nil {
			config.Attachments = store
		}
		if config.AttachmentStore == nil {
			config.AttachmentStore = store
		}
		previousClose := config.CloseDependencies
		config.CloseDependencies = func() error {
			if previousClose == nil {
				return store.Close()
			}
			return errors.Join(previousClose(), store.Close())
		}
	}
	if config.RuntimeTenantID == "" {
		return nil
	}
	if config.Sessions == nil || config.SessionStore == nil || config.EventHistoryStore == nil {
		return ErrInvalidConfig
	}
	persistence := bootstrapSessionPersistence{SessionStateStore: config.SessionStore, EventHistoryStore: config.EventHistoryStore}
	wrapped, err := agentsessionstore.NewWithObservability(config.RuntimeTenantID, config.Sessions, persistence, config.Observability)
	if err != nil {
		return ErrInvalidConfig
	}
	config.Sessions = wrapped
	return nil
}

func prepareBudgetConfig(config *Config) error {
	if config.BudgetStore != nil || config.DB == nil || config.ControlPlaneDriver != ControlPlaneDriverPostgres {
		return nil
	}
	store, err := runtimebudgetpostgres.New(config.DB)
	if err != nil {
		return ErrInvalidConfig
	}
	config.BudgetStore = store
	return nil
}

func newRuntimeGraph(config Config) (*Runtime, error) {
	resolver, err := gateway.NewPlanResolver(runtime.PlanResolverConfig{
		Tenants: config.Tenants, Apps: config.Apps, Models: config.Models, Backends: config.Backends,
		ModelCatalog: config.ModelCatalog, BackendCatalog: config.BackendCatalog,
		TenantRuntime: config.TenantRuntime,
	})
	if err != nil {
		return nil, ErrInvalidConfig
	}
	registry, err := agentrunnerfactory.NewRuntimeRunnerRegistry(agentrunnerfactory.Config{
		Registry: config.Registry, SecretResolver: config.SecretResolver,
		ModelFactory: config.ModelFactory, Sessions: config.Sessions, StorageFactory: config.StorageFactory,
		Observability: config.Observability, ToolRegistry: config.ToolRegistry, EnableUsageCallbacks: config.BudgetStore != nil,
	})
	if err != nil {
		return nil, ErrInvalidConfig
	}
	dispatcher, err := gateway.NewDispatcher(gateway.DispatchConfig{
		Resolver: resolver, Registry: registry,
		SessionStore: config.SessionStore,
		MessageStore: config.MessageStore, ReplyBatchStore: config.ReplyBatchStore,
		Attachments: config.Attachments, AttachmentStore: config.AttachmentStore,
		DrainTimeout: config.DrainTimeout, AuditWriter: config.AuditWriter, Observability: config.Observability,
		Budget: runtimebudget.NewController(config.BudgetStore),
	})
	if err != nil {
		_ = registry.Close()
		return nil, ErrInvalidConfig
	}
	aiBots, err := configureRuntimeChannels(&config, dispatcher)
	if err != nil {
		_ = registry.Close()
		return nil, ErrInvalidConfig
	}
	readyGate := config.ReadyGate
	if readyGate == nil {
		readyGate = func() bool { return true }
	}
	ping := config.Ping
	if ping == nil && config.DB != nil {
		ping = config.DB.PingContext
	}
	runtimeGraph := &Runtime{
		Resolver: resolver, Registry: registry, Dispatcher: dispatcher,
		OutboxWorker:   config.OutboxWorker,
		ExecutionQueue: config.ExecutionQueue,
		wecomHandler:   config.WeComHandler,
		db:             config.DB, ownDB: config.OwnDB, readyGate: readyGate,
		ping: ping, verifyMigrations: config.VerifyMigrations, closeDeps: config.CloseDependencies,
		telemetry:   config.Observability,
		wecomAIBots: aiBots,
	}
	if config.EnableWebConnections && config.AdminAuthenticator != nil {
		connections, factoryErr := newWebChannelConnections(config, runtimeGraph)
		if factoryErr != nil || connections == nil {
			_ = runtimeGraph.Close()
			return nil, ErrInvalidConfig
		}
		runtimeGraph.connections = connections
		if lifecycle, ok := connections.(interface{ Close() error }); ok {
			runtimeGraph.connectionsClose = lifecycle.Close
		}
	}
	if lifecycle, ok := config.WeComHandler.(callbackLifecycle); ok {
		runtimeGraph.wecomLifecycle = lifecycle
	}
	return runtimeGraph, nil
}

func configureRuntimeChannels(config *Config, dispatcher gateway.DispatchService) ([]channels.PollingAdapter, error) {
	if config.WeComHandler != nil && config.WeComHandlerFactory != nil {
		return nil, ErrInvalidConfig
	}
	if config.WeComHandlerFactory != nil {
		handler, err := config.WeComHandlerFactory(dispatcher)
		if err != nil || handler == nil {
			return nil, ErrInvalidConfig
		}
		config.WeComHandler = handler
	}
	aiBots, err := newWeComAIBots(config.WeComAIBotFactories, dispatcher)
	if err != nil {
		closeCallbackHandler(config.WeComHandler)
		return nil, err
	}
	if config.OutboxWorker != nil && config.OutboxWorkerFactory != nil {
		closePollingAdapters(aiBots)
		closeCallbackHandler(config.WeComHandler)
		return nil, ErrInvalidConfig
	}
	if config.OutboxWorkerFactory == nil {
		return aiBots, nil
	}
	worker, err := config.OutboxWorkerFactory(aiBots)
	if err != nil || worker == nil {
		if worker != nil {
			_ = worker.Close()
		}
		closePollingAdapters(aiBots)
		closeCallbackHandler(config.WeComHandler)
		return nil, ErrInvalidConfig
	}
	config.OutboxWorker = worker
	return aiBots, nil
}

func newWeComAIBots(factories []func(gateway.DispatchService) (channels.PollingAdapter, error), dispatcher gateway.DispatchService) ([]channels.PollingAdapter, error) {
	aiBots := make([]channels.PollingAdapter, 0, len(factories))
	invalid := func(err error) ([]channels.PollingAdapter, error) {
		closePollingAdapters(aiBots)
		return nil, err
	}
	for _, factory := range factories {
		if factory == nil {
			return invalid(ErrInvalidConfig)
		}
		aiBot, err := factory(dispatcher)
		if err != nil || aiBot == nil || aiBot.Channel() != channels.ChannelWeComAIBot {
			return invalid(ErrInvalidConfig)
		}
		if _, ok := aiBot.(pollingHealth); !ok {
			return invalid(ErrInvalidConfig)
		}
		aiBots = append(aiBots, aiBot)
	}
	return aiBots, nil
}

func closePollingAdapters(adapters []channels.PollingAdapter) {
	for _, adapter := range adapters {
		if adapter != nil {
			_ = adapter.Close()
		}
	}
}

func closeCallbackHandler(handler http.Handler) {
	if lifecycle, ok := handler.(callbackLifecycle); ok {
		_ = lifecycle.Close()
	}
}

func startAIBots(runtimeGraph *Runtime) error {
	if runtimeGraph == nil || len(runtimeGraph.wecomAIBots) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	runtimeGraph.aiBotCancel = cancel
	runtimeGraph.aiBotDone = make([]chan struct{}, 0, len(runtimeGraph.wecomAIBots))
	for _, adapter := range runtimeGraph.wecomAIBots {
		done := make(chan struct{})
		runtimeGraph.aiBotDone = append(runtimeGraph.aiBotDone, done)
		go func(adapter channels.PollingAdapter, done chan struct{}) {
			defer close(done)
			if err := adapter.Run(ctx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				logPollingAdapterStopped(adapter, err)
			}
		}(adapter, done)
	}
	return nil
}

func configureAdmin(config *Config, registry *runtimerunner.RunnerRegistry, connections admin.ChannelConnections) error {
	if config.AdminAuthenticator == nil {
		config.HTTP.AdminAuth = nil
		return nil
	}
	bindingRepository, ok := config.Channels.(channels.Repository)
	if !ok {
		return ErrInvalidConfig
	}
	adminHandler, err := admin.NewHandler(admin.Config{
		Tenants: config.Tenants, Apps: config.Apps, Models: config.Models,
		Backends: config.Backends, Bindings: bindingRepository,
		Authenticator: config.AdminAuthenticator,
		Connections:   connections,
		ModelCatalog:  config.ModelCatalog, BackendCatalog: config.BackendCatalog,
		CacheInvalidator: admin.CacheInvalidatorFunc(func(change admin.CacheInvalidation) {
			invalidateRuntimeCacheWithTenant(registry, config.TenantRuntime, change)
		}),
	})
	if err != nil {
		return ErrInvalidConfig
	}
	config.AdminHandler = adminHandler
	if sessionAuthenticator, ok := config.AdminAuthenticator.(*admin.SessionAuthenticator); ok {
		config.HTTP.AdminAuth = sessionAuthenticator
	}
	return nil
}

func invalidateRuntimeCache(registry *runtimerunner.RunnerRegistry, change admin.CacheInvalidation) {
	invalidateRuntimeCacheWithTenant(registry, nil, change)
}

func invalidateRuntimeCacheWithTenant(registry *runtimerunner.RunnerRegistry, tenantRuntime runtime.TenantRuntime, change admin.CacheInvalidation) {
	// A closed registry cannot admit a future execution. Other errors are
	// impossible for Admin-derived non-empty IDs, so a committed control-
	// plane mutation remains successful during shutdown.
	switch change.Kind {
	case admin.CacheInvalidationTenant:
		_ = registry.InvalidateTenant(change.TenantID)
	case admin.CacheInvalidationApp:
		_ = registry.InvalidateApp(change.TenantID, change.AppID)
	case admin.CacheInvalidationModel:
		_ = registry.InvalidateModelProfile(change.TenantID, change.ProfileID)
	case admin.CacheInvalidationBackend:
		_ = registry.InvalidateBackendProfile(change.TenantID, change.ProfileID)
	case admin.CacheInvalidationBinding:
		// Bindings are resolved and verified on every channel request. They
		// do not key a Runner or provider cache in this process.
	}
	if invalidator, ok := tenantRuntime.(runtime.TenantRuntimeInvalidator); ok {
		invalidator.InvalidateTenant(change.TenantID)
	}
}

func configureHandler(runtimeGraph *Runtime, config Config) error {
	adminHandler := config.AdminHandler
	if adminHandler == nil {
		adminHandler = config.HTTP.Admin
	}
	handler, err := gateway.NewHTTPHandler(gateway.HTTPConfig{
		Dispatcher: runtimeGraph.Dispatcher, Authenticator: config.Authenticator, Admin: adminHandler, AdminAuth: config.HTTP.AdminAuth, WeCom: runtimeGraph.wecomHandler,
		Web:   config.HTTP.Web,
		Ready: runtimeGraph.Ready, Limiter: config.HTTP.Limiter, Idempotency: config.HTTP.Idempotency,
		MaxBodyBytes: config.HTTP.MaxBodyBytes, RequestTimeout: config.HTTP.RequestTimeout, Observability: config.Observability,
	})
	if err != nil {
		return ErrInvalidConfig
	}
	runtimeGraph.Handler = handler
	return nil
}

func startOutboxWorker(runtimeGraph *Runtime, pollInterval time.Duration) error {
	if runtimeGraph.OutboxWorker == nil {
		return nil
	}
	if err := runtimeGraph.OutboxWorker.Start(context.Background(), pollInterval); err != nil {
		return ErrInvalidConfig
	}
	return nil
}

func startExecutionQueue(runtimeGraph *Runtime) error {
	if runtimeGraph == nil || runtimeGraph.ExecutionQueue == nil {
		return nil
	}
	if err := runtimeGraph.ExecutionQueue.Start(context.Background()); err != nil {
		return ErrInvalidConfig
	}
	return nil
}

// Ready is the single readiness gate used by HTTPHandler. It checks the
// database on demand (when present) and never returns true after shutdown has
// started.
func (graph *Runtime) Ready() bool {
	if graph == nil || graph.closing.Load() || graph.readyGate == nil || !graph.readyGate() {
		return false
	}
	if graph.ping != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		err := graph.ping(ctx)
		cancel()
		if err != nil {
			return false
		}
	}
	if graph.verifyMigrations != nil && graph.db != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		err := graph.verifyMigrations(ctx, graph.db)
		cancel()
		if err != nil {
			return false
		}
	}
	for _, adapter := range graph.wecomAIBots {
		if !adapter.(pollingHealth).Ready() {
			return false
		}
	}
	return graph.Resolver != nil && graph.Resolver.Ready() && graph.Registry != nil && graph.Registry.Ready() && graph.Dispatcher != nil && graph.Dispatcher.Ready() && graph.Handler != nil
}

// BeginShutdown immediately removes the graph from readiness and stops new
// HTTP admissions. The caller should then drain its net/http.Server.
func (graph *Runtime) BeginShutdown() {
	if graph == nil {
		return
	}
	graph.closing.Store(true)
	if graph.Handler != nil {
		graph.Handler.BeginShutdown()
	}
	if graph.wecomLifecycle != nil {
		graph.wecomLifecycle.BeginShutdown()
	}
	for _, adapter := range graph.wecomAIBots {
		if lifecycle, ok := adapter.(interface{ BeginShutdown() }); ok {
			lifecycle.BeginShutdown()
		}
	}
}

// Close performs bounded Registry shutdown, then closes explicitly owned
// dependencies. Borrowed repositories, sessions and factories are untouched.
func (graph *Runtime) Close() error {
	if graph == nil {
		return nil
	}
	graph.closeOnce.Do(func() {
		graph.BeginShutdown()
		var closeErr error
		if graph.Handler != nil {
			closeErr = errors.Join(closeErr, graph.Handler.Close())
		}
		if graph.wecomLifecycle != nil {
			closeErr = errors.Join(closeErr, graph.wecomLifecycle.Close())
		}
		if graph.connectionsClose != nil {
			closeErr = errors.Join(closeErr, graph.connectionsClose())
		}
		if graph.aiBotCancel != nil {
			graph.aiBotCancel()
		}
		for _, adapter := range graph.wecomAIBots {
			closeErr = errors.Join(closeErr, adapter.Close())
		}
		for _, done := range graph.aiBotDone {
			<-done
		}
		if graph.ExecutionQueue != nil {
			closeErr = errors.Join(closeErr, graph.ExecutionQueue.Close())
		}
		if graph.Registry != nil {
			closeErr = errors.Join(closeErr, graph.Registry.Close())
		}
		if graph.OutboxWorker != nil {
			closeErr = errors.Join(closeErr, graph.OutboxWorker.Close())
		}
		if graph.closeDeps != nil {
			closeErr = errors.Join(closeErr, graph.closeDeps())
		}
		if graph.ownDB && graph.db != nil {
			closeErr = errors.Join(closeErr, graph.db.Close())
		}
		if graph.telemetry != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			closeErr = errors.Join(closeErr, graph.telemetry.Shutdown(ctx))
			cancel()
		}
		graph.closeErr = closeErr
	})
	return graph.closeErr
}

// HandlerValue returns the real HTTP handler assembled by New.
func (graph *Runtime) HandlerValue() *gateway.HTTPHandler {
	if graph == nil {
		return nil
	}
	return graph.Handler
}

// NewUnavailable builds the command's deliberately unconfigured local mode.
// It still constructs the real resolver/registry/dispatcher graph; the
// explicit readiness gate remains false until a caller supplies durable
// control-plane configuration through Config.
func NewUnavailable() (*Runtime, error) {
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider: "unconfigured", Models: []string{"unconfigured"},
		EndpointPolicy: modelprofile.FieldForbidden, SecretRefPolicy: modelprofile.FieldForbidden,
	})
	if err != nil {
		return nil, ErrInvalidConfig
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "unconfigured", Capabilities: []backend.Capability{backend.CapabilitySession},
		EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
		Options: map[string]backend.OptionSpec{},
	})
	if err != nil {
		return nil, ErrInvalidConfig
	}
	authenticator, err := gateway.NewStaticAPIAuthenticator(map[string]gateway.APIIdentity{})
	if err != nil {
		return nil, ErrInvalidConfig
	}
	sessions := inmemory.NewSessionService()
	config := Config{
		Tenants: tenantmemory.NewRepository(), Apps: appmemory.NewRepository(),
		Models: modelmemory.NewRepository(modelCatalog), Backends: backendmemory.NewRepository(backendCatalog),
		Channels: channelmemory.NewRepository(), ModelCatalog: modelCatalog, BackendCatalog: backendCatalog,
		SecretResolver: unavailableSecretResolver{}, ModelFactory: unavailableModelFactory{},
		Sessions: sessions, Authenticator: authenticator,
		ReadyGate: func() bool { return false }, CloseDependencies: sessions.Close,
	}
	return New(context.Background(), config)
}

type unavailableSecretResolver struct{}

func (unavailableSecretResolver) Resolve(context.Context, modelprofile.SecretScope) (modelprofile.SecretValue, error) {
	return modelprofile.SecretValue{}, ErrBootstrapNotReady
}

type unavailableModelFactory struct{}

func (unavailableModelFactory) New(context.Context, modelprofile.ModelFactoryInput, modelprofile.SecretValue) (trpcmodel.Model, error) {
	return nil, ErrBootstrapNotReady
}

// NewHTTPServer is a small explicit server helper for command wiring.
func NewHTTPServer(graph *Runtime, address string, readHeaderTimeout, readTimeout, writeTimeout, idleTimeout time.Duration) (*http.Server, error) {
	if graph == nil || graph.HandlerValue() == nil || address == "" {
		return nil, ErrInvalidConfig
	}
	return &http.Server{Addr: address, Handler: graph.HandlerValue().Handler(), ReadHeaderTimeout: readHeaderTimeout, ReadTimeout: readTimeout, WriteTimeout: writeTimeout, IdleTimeout: idleTimeout}, nil
}
