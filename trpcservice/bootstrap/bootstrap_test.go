package bootstrap

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/admin"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	appmemory "github.com/XnLemon/trpc-agent-service/trpcservice/app/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom_aibot"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	runtimeservice "github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimebudgetmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget/inmemory"
	runtimebudgetpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget/postgres"
	modelruntime "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/model"
	runtimequeue "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/queue"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestNewBuildsRealGraphAndGatesReadiness(t *testing.T) {
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	var gate atomic.Bool
	config.ReadyGate = gate.Load
	config.CloseDependencies = nil
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if graph.Resolver == nil || graph.Registry == nil || graph.Dispatcher == nil || graph.HandlerValue() == nil {
		t.Fatal("bootstrap did not construct the real resolver/registry/dispatcher/handler graph")
	}
	readyRequest := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	readyResponse := httptest.NewRecorder()
	graph.HandlerValue().ServeHTTP(readyResponse, readyRequest)
	if readyResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured readiness status = %d", readyResponse.Code)
	}

	gate.Store(true)
	readyResponse = httptest.NewRecorder()
	graph.HandlerValue().ServeHTTP(readyResponse, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if readyResponse.Code != http.StatusOK {
		t.Fatalf("configured readiness status = %d", readyResponse.Code)
	}

	graph.BeginShutdown()
	if graph.Ready() || graph.HandlerValue().Ready() {
		t.Fatal("shutdown graph remained ready")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBootstrapWiresOptionalWebConnections(t *testing.T) {
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	config.SecretResolver = modelruntime.NewSecretRegistry()
	config.AdminAuthenticator, _ = admin.NewStaticAuthenticator("admin", []string{"*"})
	config.EnableWebConnections = true
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if graph.connections == nil || graph.connectionsClose == nil {
		t.Fatal("optional web connections were not wired into the runtime")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBootstrapServesConcurrentTenantsWithIndependentProviders(t *testing.T) {
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider: "fake", Models: []string{"model-one", "model-two"},
		EndpointPolicy: modelprofile.FieldForbidden, SecretRefPolicy: modelprofile.FieldRequired,
	})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "memory", Capabilities: []backend.Capability{backend.CapabilitySession},
		EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
		Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString, Required: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tenants := tenantmemory.NewRepository()
	apps := appmemory.NewRepository()
	models := modelmemory.NewRepository(modelCatalog)
	backends := backendmemory.NewRepository(backendCatalog)
	identities := make(map[string]gateway.APIIdentity)
	secrets := modelruntime.NewSecretRegistry()
	for _, configured := range []struct {
		token, tenantKey, appKey, modelName, secretRef, secretValue string
	}{
		{token: "token-one", tenantKey: "bootstrap-one", appKey: "app-one", modelName: "model-one", secretRef: "secret/one", secretValue: "value-one"},
		{token: "token-two", tenantKey: "bootstrap-two", appKey: "app-two", modelName: "model-two", secretRef: "secret/two", secretValue: "value-two"},
	} {
		root, app := createBootstrapTenantExecutionState(t, tenants, apps, models, backends, configured.tenantKey, configured.appKey, configured.modelName, configured.secretRef)
		if err := secrets.RegisterValue(modelprofile.SecretScope{TenantID: root.TenantID, SecretRef: configured.secretRef}, configured.secretValue); err != nil {
			t.Fatal(err)
		}
		identities[configured.token] = gateway.APIIdentity{TenantID: root.TenantID, AppID: app.AppID, SubjectID: configured.tenantKey}
	}
	authenticator, err := gateway.NewStaticAPIAuthenticator(identities)
	if err != nil {
		t.Fatal(err)
	}
	modelFactory := &bootstrapRecordingModelFactory{}
	storageFactory := &bootstrapRecordingStorageFactory{}
	graph, err := New(context.Background(), Config{
		Tenants: tenants, Apps: apps, Models: models, Backends: backends, Channels: channelmemory.NewRepository(),
		ModelCatalog: modelCatalog, BackendCatalog: backendCatalog, SecretResolver: secrets, ModelFactory: modelFactory,
		StorageFactory: storageFactory, Authenticator: authenticator,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = graph.Close() }()
	type result struct {
		tenantID string
		err      error
	}
	results := make(chan result, len(identities))
	var group sync.WaitGroup
	for token, identity := range identities {
		group.Add(1)
		go func(token string, identity gateway.APIIdentity) {
			defer group.Done()
			request := httptest.NewRequest(http.MethodGet, "/v1/chat", nil)
			request.Header.Set("Authorization", "Bearer "+token)
			authenticated, authErr := authenticator.Authenticate(context.Background(), request)
			if authErr != nil {
				results <- result{tenantID: identity.TenantID, err: authErr}
				return
			}
			plan, resolveErr := graph.Resolver.ResolveAuthenticatedAPI(context.Background(), authenticated)
			if resolveErr != nil {
				results <- result{tenantID: identity.TenantID, err: resolveErr}
				return
			}
			lease, acquireErr := graph.Registry.Acquire(context.Background(), plan)
			if acquireErr == nil {
				acquireErr = lease.Release()
			}
			results <- result{tenantID: identity.TenantID, err: acquireErr}
		}(token, identity)
	}
	group.Wait()
	close(results)
	for outcome := range results {
		if outcome.err != nil {
			t.Fatalf("tenant %s execution setup = %v", outcome.tenantID, outcome.err)
		}
	}
	for _, identity := range identities {
		if modelFactory.Secret(identity.TenantID) == "" || storageFactory.SessionCount(identity.TenantID) != 1 {
			t.Fatalf("tenant %s did not receive independent provider materialization", identity.TenantID)
		}
	}
	if modelFactory.Secret(identities["token-one"].TenantID) == modelFactory.Secret(identities["token-two"].TenantID) {
		t.Fatal("tenant model secrets crossed provider scope")
	}
}

func TestRuntimeStartsAndStopsConfiguredOutboxWorker(t *testing.T) {
	store := runtimestorageinmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", EventID: "event", BindingID: "binding", ExternalMessageID: "external"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", EventID: "event", ReplyID: "reply", SegmentCount: 1, Payload: "payload"}); err != nil {
		t.Fatal(err)
	}
	provider := &bootstrapBlockingProvider{started: make(chan struct{}), canceled: make(chan struct{})}
	worker, err := outbox.New(outbox.Config{Store: store, MessageStore: store, Provider: provider, TenantID: "tenant-a", Owner: "bootstrap-worker", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	config.SessionStore = store
	config.EventHistoryStore = store
	config.MessageStore = store
	config.ReplyBatchStore = store
	config.Attachments = store
	config.AttachmentStore = store
	config.OutboxWorker = worker
	config.OutboxPollInterval = time.Hour
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("bootstrap did not start the configured outbox worker")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.canceled:
	case <-time.After(time.Second):
		t.Fatal("runtime close did not cancel and join the outbox worker")
	}
}

func TestNewRejectsAlreadyRunningOutboxWorker(t *testing.T) {
	store := runtimestorageinmemory.New()
	worker, err := outbox.New(outbox.Config{
		Store: store, MessageStore: store, Provider: &bootstrapBlockingProvider{started: make(chan struct{}), canceled: make(chan struct{})},
		TenantID: "tenant-a", Owner: "already-running", LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(context.Background(), time.Hour); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = worker.Close() })
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	config.OutboxWorker = worker
	if _, err := New(context.Background(), config); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("already-running worker error = %v", err)
	}
}

func TestStartExecutionQueueRejectsRunningWorker(t *testing.T) {
	store := runtimequeue.NewMemory()
	worker, err := runtimequeue.New(runtimequeue.Config{
		Store: store, Handler: func(context.Context, runtimequeue.Task) error { return nil },
		Owner: "bootstrap-queue-error", LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = worker.Close()
		_ = store.Close()
	})
	if err := startExecutionQueue(&Runtime{ExecutionQueue: worker}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("running queue error = %v", err)
	}
}

func TestConfigureRuntimeChannelsClosesWorkerReturnedWithError(t *testing.T) {
	worker := &outbox.Worker{}
	config := Config{
		OutboxWorkerFactory: func([]channels.PollingAdapter) (*outbox.Worker, error) {
			return worker, errors.New("worker factory failed")
		},
	}
	if _, err := configureRuntimeChannels(&config, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("worker factory error = %v", err)
	}
}

func TestNewRejectsMissingExplicitDependency(t *testing.T) {
	if _, err := New(context.Background(), Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing dependency error = %v", err)
	}
}

func TestBootstrapFailureAndLifecycleBoundaries(t *testing.T) {
	if _, err := New(nilContextForTest(), Config{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil bootstrap context error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := New(canceled, Config{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled bootstrap context error = %v", err)
	}

	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	config, closeDependencies := testConfig(t)
	t.Cleanup(closeDependencies)
	config.DB = db
	mock.ExpectPing().WillReturnError(errors.New("database unavailable"))
	if _, err := New(context.Background(), config); !errors.Is(err, postgres.ErrStorage) {
		t.Fatalf("failed bootstrap ping error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	config, closeDependencies = testConfig(t)
	t.Cleanup(closeDependencies)
	config.Ping = func(context.Context) error { return errors.New("readiness ping failure") }
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if graph.Ready() {
		t.Fatal("graph remained ready after a ping failure")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}

	closeFailure := errors.New("dependency close failure")
	config, closeDependencies = testConfig(t)
	config.CloseDependencies = func() error {
		closeDependencies()
		return closeFailure
	}
	graph, err = New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("close failure = %v", err)
	}
	if err := graph.Close(); !errors.Is(err, closeFailure) {
		t.Fatalf("repeat close failure = %v", err)
	}
	if (&Runtime{}).Ready() {
		t.Fatal("uninitialized runtime reported ready")
	}
}

func TestBootstrapCoversConstructionFailureBoundaries(t *testing.T) {
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	config.RuntimeTenantID = "runtime-tenant"
	config.Sessions = nil
	config.StorageFactory = storagefactory.StorageFactoryFunc(func(context.Context, backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
		return nil, nil
	})
	if _, err := New(context.Background(), config); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid runtime tenant configuration = %v", err)
	}

	config, closeDependencies = testConfig(t)
	config.Registry.Factory = func(context.Context, runtimeservice.ExecutionPlan) (runtimerunner.Runner, error) { return nil, nil }
	config.HTTP.MaxBodyBytes = -1
	if _, err := New(context.Background(), config); !errors.Is(err, ErrInvalidConfig) {
		closeDependencies()
		t.Fatalf("invalid handler configuration = %v", err)
	}
	closeDependencies()

	config, closeDependencies = testConfig(t)
	config.WeComHandler = &bootstrapWeComLifecycle{}
	config.WeComHandlerFactory = func(gateway.DispatchService) (http.Handler, error) { return nil, nil }
	if _, err := New(context.Background(), config); !errors.Is(err, ErrInvalidConfig) {
		closeDependencies()
		t.Fatalf("conflicting callback handlers = %v", err)
	}
	closeDependencies()

	config, closeDependencies = testConfig(t)
	config.AdminAuthenticator, _ = admin.NewStaticAuthenticator("admin", []string{"*"})
	config.Channels = candidateOnly{}
	if _, err := New(context.Background(), config); !errors.Is(err, ErrInvalidConfig) {
		closeDependencies()
		t.Fatalf("non-repository admin channel dependency = %v", err)
	}
	closeDependencies()
}

func TestBootstrapClosesPartiallyConstructedAIBots(t *testing.T) {
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	first := newBootstrapAIBot()
	config.WeComAIBotFactories = []func(gateway.DispatchService) (channels.PollingAdapter, error){
		func(gateway.DispatchService) (channels.PollingAdapter, error) { return first, nil },
		func(gateway.DispatchService) (channels.PollingAdapter, error) {
			return nil, errors.New("second bot failed")
		},
	}
	if _, err := New(context.Background(), config); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("partial AI Bot construction error = %v", err)
	}
	if first.closed.Load() != 1 {
		t.Fatalf("partially constructed AI Bot close count = %d, want 1", first.closed.Load())
	}
}

func TestBootstrapFailureClosesConstructedGraph(t *testing.T) {
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	bot := newBootstrapAIBot()
	config.WeComAIBotFactories = []func(gateway.DispatchService) (channels.PollingAdapter, error){
		func(gateway.DispatchService) (channels.PollingAdapter, error) { return bot, nil },
	}
	config.HTTP.MaxBodyBytes = -1
	if _, err := New(context.Background(), config); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("handler construction error = %v", err)
	}
	if bot.closed.Load() != 1 {
		t.Fatalf("failed bootstrap AI Bot close count = %d, want 1", bot.closed.Load())
	}
}

func TestBootstrapRoutesAdminCacheInvalidationsToRuntimeRegistry(t *testing.T) {
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = graph.Close() }()
	const tenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	for _, change := range []admin.CacheInvalidation{
		{TenantID: tenantID, Kind: admin.CacheInvalidationTenant},
		{TenantID: tenantID, AppID: "app-1", Kind: admin.CacheInvalidationApp},
		{TenantID: tenantID, ProfileID: "model-1", Kind: admin.CacheInvalidationModel},
		{TenantID: tenantID, ProfileID: "backend-1", Kind: admin.CacheInvalidationBackend},
		{TenantID: tenantID, BindingID: "binding-1", Kind: admin.CacheInvalidationBinding},
	} {
		invalidateRuntimeCache(graph.Registry, change)
	}
}

func TestBootstrapRoutesTenantRuntimeInvalidation(t *testing.T) {
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = graph.Close() }()

	const tenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	invalidator := &bootstrapTenantRuntimeInvalidator{}
	invalidateRuntimeCacheWithTenant(graph.Registry, invalidator, admin.CacheInvalidation{TenantID: tenantID, Kind: admin.CacheInvalidationTenant})
	if invalidator.tenantID != tenantID {
		t.Fatalf("invalidated tenant = %q, want %q", invalidator.tenantID, tenantID)
	}
}

func TestNewUnavailableUsesRealGraphButReturns503(t *testing.T) {
	graph, err := NewUnavailable()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = graph.Close() }()
	if graph.Resolver == nil || graph.Registry == nil || graph.Dispatcher == nil {
		t.Fatal("unavailable mode did not construct the real execution graph")
	}
	response := httptest.NewRecorder()
	graph.HandlerValue().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unavailable readiness status = %d", response.Code)
	}
}

func TestUnavailableBootstrapBoundariesAndHTTPServer(t *testing.T) {
	graph, err := NewUnavailable()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = graph.Close() }()
	if _, err := NewHTTPServer(nil, ":8080", 0, 0, 0, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil graph server error = %v", err)
	}
	if _, err := NewHTTPServer(&Runtime{}, ":8080", 0, 0, 0, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("handlerless graph server error = %v", err)
	}
	if _, err := NewHTTPServer(graph, "", 0, 0, 0, 0); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty address server error = %v", err)
	}
	server, err := NewHTTPServer(graph, ":8080", 0, 0, 0, 0)
	if err != nil || server.Handler != graph.HandlerValue().Handler() {
		t.Fatalf("HTTP server = %+v, err=%v", server, err)
	}
	if _, err := (unavailableSecretResolver{}).Resolve(context.Background(), modelprofile.SecretScope{}); !errors.Is(err, ErrBootstrapNotReady) {
		t.Fatalf("unavailable secret resolver error = %v", err)
	}
	if _, err := (unavailableModelFactory{}).New(context.Background(), modelprofile.ModelFactoryInput{}, modelprofile.SecretValue{}); !errors.Is(err, ErrBootstrapNotReady) {
		t.Fatalf("unavailable model factory error = %v", err)
	}
	var nilGraph *Runtime
	if nilGraph.HandlerValue() != nil || nilGraph.Ready() {
		t.Fatal("nil runtime reported a handler or readiness")
	}
}

func TestEnvironmentBootstrapRequiresExplicitConfigurationAndBuildsDependencies(t *testing.T) {
	setEnvironmentBootstrapTestVariables(t)
	config := assertEnvironmentConfigurationAndCatalogs(t)
	assertEnvironmentAuthenticationAndSecret(t, config)
	assertEnvironmentRequiredValues(t)
}

func TestEnvironmentAdminWebCredentialsMustBeConfiguredTogether(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envAdminUsername, "operator")
	t.Setenv(envAdminPassword, "")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("one-sided admin web credentials error = %v", err)
	}
	t.Setenv(envAdminUsername, "")
	t.Setenv(envAdminPassword, "secret")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("password-only admin web credentials error = %v", err)
	}
	t.Setenv(envAdminUsername, "operator")
	t.Setenv(envAdminPassword, "secret")
	config, err := loadEnvironment()
	if err != nil || config.adminUsername != "operator" || config.adminPassword != "secret" {
		t.Fatalf("paired admin web credentials = %+v, err=%v", config, err)
	}
}

func setEnvironmentBootstrapTestVariables(t *testing.T) {
	t.Helper()
	t.Setenv(envPostgresDSN, "postgres://postgres:postgres@127.0.0.1:5432/control_plane")
	t.Setenv(envAPIToken, "api-token")
	t.Setenv(envTenantID, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAppID, "app_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAdminToken, "admin-token")
	t.Setenv(envAdminTenants, "*")
	t.Setenv(envSubjectID, "service")
	t.Setenv(envModelAPIKey, "test-secret")
	t.Setenv(envModelProvider, "openai")
	t.Setenv(envModelNames, "gpt-4o-mini,custom.model")
	t.Setenv(envModelEndpointHost, "api.openai.com,proxy.example")
	t.Setenv(envModelSecretRef, "env/test-key")
	t.Setenv(envSessionBackend, "postgres")
}

func assertEnvironmentConfigurationAndCatalogs(t *testing.T) environmentConfig {
	t.Helper()
	config, err := loadEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if config.modelProvider != "openai" || len(config.modelNames) != 2 || config.secretRef != "env/test-key" {
		t.Fatalf("environment config = %+v", config)
	}
	t.Setenv(envSessionBackend, "inmemory")
	devConfig, err := loadEnvironment()
	if err != nil || devConfig.runtimeStorage != "inmemory" {
		t.Fatalf("documented in-memory backend = %+v, err=%v", devConfig, err)
	}
	t.Setenv(envSessionBackend, "")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing explicit session backend error = %v", err)
	}
	t.Setenv(envSessionBackend, "postgres")
	modelCatalog, backendCatalog, err := environmentCatalogs(config)
	if err != nil || modelCatalog == nil || backendCatalog == nil {
		t.Fatalf("environment catalogs = %v, %v, %v", modelCatalog, backendCatalog, err)
	}
	return config
}

func assertEnvironmentAuthenticationAndSecret(t *testing.T, config environmentConfig) {
	t.Helper()
	authenticator, err := gateway.NewStaticAPIAuthenticator(map[string]gateway.APIIdentity{
		config.apiToken: {TenantID: config.tenantID, AppID: config.appID, SubjectID: config.subjectID},
	})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	request.Header.Set("Authorization", "Bearer "+config.apiToken)
	authenticated, err := authenticator.Authenticate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := authenticated.Identity()
	if err != nil || identity.TenantID != config.tenantID || identity.AppID != config.appID {
		t.Fatalf("environment identity = %+v, err=%v", identity, err)
	}

	resolver := environmentSecretResolver{reference: config.secretRef, value: config.modelAPIKey}
	secret, err := resolver.Resolve(context.Background(), modelprofile.SecretScope{TenantID: config.tenantID, SecretRef: config.secretRef})
	if err != nil || secret.Value() != config.modelAPIKey || secret.String() != "<redacted-secret>" {
		t.Fatalf("environment secret = %s, err=%v", secret, err)
	}
	model, err := (environmentModelFactory{}).New(context.Background(), modelprofile.ModelFactoryInput{Model: "gpt-4o-mini"}, secret)
	if err != nil || model == nil {
		t.Fatalf("environment model = %v, err=%v", model, err)
	}
}

func assertEnvironmentRequiredValues(t *testing.T) {
	t.Helper()
	for _, name := range []string{envPostgresDSN, envAPIToken, envTenantID, envAppID, envAdminToken, envAdminTenants, envModelAPIKey} {
		t.Setenv(name, "")
		if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("missing %s error = %v", name, err)
		}
		t.Setenv(name, "configured")
	}
	if _, err := NewFromEnvironment(nilContextForTest()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil environment bootstrap context error = %v", err)
	}
}

func TestEnvironmentWeComCredentialsMustBeConfiguredTogether(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envWeComCallbackToken, "callback-token")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("partial WeCom configuration error = %v", err)
	}
	t.Setenv(envWeComEncodingAESKey, "encoding-key")
	t.Setenv(envWeComAppSecret, "app-secret")
	t.Setenv(envWeComSecretRef, "env/wecom")
	config, err := loadEnvironment()
	if err != nil || config.wecom == nil {
		t.Fatalf("complete WeCom environment = %+v, %v", config.wecom, err)
	}
	if config.wecom.callbackToken != "callback-token" || config.wecom.secretRef != "env/wecom" {
		t.Fatalf("WeCom environment = %+v", config.wecom)
	}
	resolver := environmentWeComCredentialResolver{tenantID: config.tenantID, config: *config.wecom}
	credentials, err := resolver.Resolve(context.Background(), channels.SecretScope{TenantID: config.tenantID, SecretRef: config.wecom.secretRef})
	if err != nil || credentials.CallbackToken != config.wecom.callbackToken || credentials.AppSecret != config.wecom.appSecret {
		t.Fatalf("WeCom credentials = %+v, %v", credentials, err)
	}
	if _, err := resolver.Resolve(context.Background(), channels.SecretScope{TenantID: config.tenantID, SecretRef: "env/other"}); err == nil {
		t.Fatal("mismatched WeCom secret reference was accepted")
	}
}

func TestEnvironmentWeComAIBotConnectionsAreScopedAndValidated(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	config := environmentConfig{tenantID: tenantID, apiIdentities: map[string]gateway.APIIdentity{"token": {TenantID: tenantID, AppID: "app_00000000000000000000000000", SubjectID: "service"}}}
	t.Setenv(envWeComAIBotConnections, `[{"binding_id":"cb_00000000000000000000000000","secret_ref":"env/wecom-aibot","bot_secret":"test-secret"}]`)
	if err := config.loadWeComAIBots(); err != nil {
		t.Fatal(err)
	}
	if len(config.wecomAIBots) != 1 || config.wecomAIBots[0].BindingID == "" {
		t.Fatalf("AI Bot connections = %+v", config.wecomAIBots)
	}
	resolver := environmentWeComAIBotCredentialResolver{tenantID: tenantID, secrets: map[string]string{"env/wecom-aibot": "test-secret"}}
	credentials, err := resolver.Resolve(context.Background(), channels.SecretScope{TenantID: tenantID, SecretRef: "env/wecom-aibot"})
	if err != nil || credentials.BotSecret != "test-secret" {
		t.Fatalf("AI Bot credentials = %+v %v", credentials, err)
	}
	if _, err := resolver.Resolve(context.Background(), channels.SecretScope{TenantID: tenantID, SecretRef: "other"}); err == nil {
		t.Fatal("unknown AI Bot secret reference was accepted")
	}

	for _, value := range []string{`[]`, `[{"binding_id":"","secret_ref":"env/wecom-aibot","bot_secret":"test-secret"}]`, `[{"binding_id":"one","secret_ref":"env/ref","bot_secret":"first"},{"binding_id":"one","secret_ref":"env/other","bot_secret":"second"}]`} {
		t.Setenv(envWeComAIBotConnections, value)
		candidate := environmentConfig{tenantID: tenantID, apiIdentities: config.apiIdentities}
		if err := candidate.loadWeComAIBots(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("connection configuration %s error = %v", value, err)
		}
	}
}

func TestEnvironmentWeComAIBotComponentsUseTrustedBindings(t *testing.T) {
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider: "fake", Models: []string{"test-model"}, EndpointPolicy: modelprofile.FieldForbidden, SecretRefPolicy: modelprofile.FieldOptional,
	})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "memory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tenants := tenantmemory.NewRepository()
	apps := appmemory.NewRepository()
	models := modelmemory.NewRepository(modelCatalog)
	backends := backendmemory.NewRepository(backendCatalog)
	channelsRepo := channelmemory.NewRepository()
	root, app := createBootstrapTenantExecutionState(t, tenants, apps, models, backends, "aibot-components", "aibot-components", "test-model", "secret/model")
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelWeComAIBot, "aibot-components")
	if err != nil {
		t.Fatal(err)
	}
	binding, _, err := channelsRepo.Create(context.Background(), channels.CreateInput{
		TenantID: root.TenantID, BindingKey: "aibot-components", Channel: channels.ChannelWeComAIBot,
		ProviderAccountID: "aibot-components", PublicRouteKeyDigest: routeDigest, AppID: app.AppID,
		SecretRef: "env/aibot-components", Status: channels.StatusActive,
		Protocol: channels.ProtocolConfiguration{WeComAIBot: &channels.WeComAIBotProtocolConfiguration{BotID: "bot-components"}},
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "bootstrap", Reason: "fixture", CorrelationID: "aibot-components"},
	})
	if err != nil {
		t.Fatal(err)
	}
	environment := environmentConfig{
		tenantID:    root.TenantID,
		wecomAIBots: []environmentWeComAIBotConfig{{BindingID: binding.BindingID, SecretRef: binding.SecretRef, BotSecret: "bot-secret"}},
	}
	factories, bindingIDs, err := environmentWeComAIBotComponents(environmentWeComAIBotDependencies{
		ctx: context.Background(), config: environment, channels: channelsRepo, tenants: tenants, apps: apps,
	})
	if err != nil || len(factories) != 1 || len(bindingIDs) != 1 {
		t.Fatalf("AI Bot components = factories:%d bindings:%d err:%v", len(factories), len(bindingIDs), err)
	}
	manager, err := factories[0](bootstrapNoopDispatcher{})
	if err != nil || manager.Channel() != channels.ChannelWeComAIBot {
		t.Fatalf("AI Bot manager = %v, %v", manager, err)
	}
	runtimeStore := runtimestorageinmemory.New()
	defer func() { _ = runtimeStore.Close() }()
	previousOwner := environmentWeComOwnerFunc
	previousWorker := newEnvironmentWeComWorker
	defer func() { environmentWeComOwnerFunc = previousOwner }()
	defer func() { newEnvironmentWeComWorker = previousWorker }()
	environmentWeComOwnerFunc = func() (string, error) { return "test-owner", nil }
	var workerConfig outbox.Config
	newEnvironmentWeComWorker = func(config outbox.Config) (*outbox.Worker, error) {
		workerConfig = config
		return outbox.New(config)
	}
	workerFactory := environmentOutboxWorkerFactory(environmentOutboxWorkerDependencies{
		config: environment, replyStore: runtimeStore, messageStore: runtimeStore, deliveryStore: runtimeStore, aiBotBindings: bindingIDs,
	})
	if _, err := workerFactory([]channels.PollingAdapter{manager}); err != nil {
		t.Fatalf("AI Bot outbox worker = %v", err)
	}
	if workerConfig.LeaseDuration != wecom_aibot.OutboxLeaseDuration {
		t.Fatalf("AI Bot outbox lease duration = %s, want %s", workerConfig.LeaseDuration, wecom_aibot.OutboxLeaseDuration)
	}
	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}
	if factories, bindingIDs, err := environmentWeComAIBotComponents(environmentWeComAIBotDependencies{
		ctx: context.Background(), config: environmentConfig{tenantID: root.TenantID}, channels: channelsRepo, tenants: tenants, apps: apps,
	}); err != nil || factories != nil || bindingIDs != nil {
		t.Fatalf("empty AI Bot components = %v %v %v", factories, bindingIDs, err)
	}
	duplicate := environment
	duplicate.wecomAIBots = append(duplicate.wecomAIBots, environmentWeComAIBotConfig{BindingID: "other", SecretRef: binding.SecretRef, BotSecret: "other-secret"})
	if _, _, err := environmentWeComAIBotComponents(environmentWeComAIBotDependencies{
		ctx: context.Background(), config: duplicate, channels: channelsRepo, tenants: tenants, apps: apps,
	}); err == nil {
		t.Fatal("duplicate AI Bot secret reference was accepted")
	}
	unavailable := environment
	unavailable.wecomAIBots[0].BindingID = "missing-binding"
	if _, _, err := environmentWeComAIBotComponents(environmentWeComAIBotDependencies{
		ctx: context.Background(), config: unavailable, channels: channelsRepo, tenants: tenants, apps: apps,
	}); err == nil {
		t.Fatal("unavailable AI Bot binding was accepted")
	}
}

func TestEnvironmentOutboxWorkerFactoryRoutesAIBotBindings(t *testing.T) {
	store := runtimestorageinmemory.New()
	defer func() { _ = store.Close() }()
	legacy := bootstrapStaticProvider{receipt: "legacy"}
	aiBot := bootstrapStaticProvider{receipt: "aibot"}
	router := environmentReplyProvider{legacy: legacy, aiBot: aiBot, aiBotBindingIDs: map[string]struct{}{"aibot-binding": {}}}
	for _, test := range []struct {
		binding string
		want    string
	}{{binding: "aibot-binding", want: "aibot"}, {binding: "wecom-binding", want: "legacy"}} {
		receipt, err := router.Deliver(context.Background(), runtimestorage.ReplyOutbox{ReplyTarget: runtimestorage.ReplyTarget{BindingID: test.binding}})
		if err != nil || receipt != test.want {
			t.Fatalf("binding %s receipt = %q, %v", test.binding, receipt, err)
		}
	}
	if status, receipt, err := router.Reconcile(context.Background(), runtimestorage.ReplyOutbox{ReplyTarget: runtimestorage.ReplyTarget{BindingID: "aibot-binding"}}); err != nil || status != outbox.DeliveryAccepted || receipt != "aibot" {
		t.Fatalf("AI Bot reconcile = %q %q %v", status, receipt, err)
	}

	const tenantID = "t_00000000000000000000000000"
	factory := environmentOutboxWorkerFactory(environmentOutboxWorkerDependencies{
		config: environmentConfig{tenantID: tenantID}, replyStore: store, messageStore: store, deliveryStore: store,
		aiBotBindings: map[string]struct{}{"aibot-binding": {}},
	})
	manager := &wecom_aibot.Manager{}
	if worker, err := factory([]channels.PollingAdapter{manager}); err == nil || worker != nil {
		t.Fatal("manager without binding identity was accepted")
	}
}

func TestWeComHandlerFactoryIsWiredAndOwnedByRuntime(t *testing.T) {
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	callback := &bootstrapWeComLifecycle{}
	var factoryCalls int
	config.WeComHandlerFactory = func(dispatcher gateway.DispatchService) (http.Handler, error) {
		if dispatcher == nil {
			t.Fatal("WeCom factory received nil dispatcher")
		}
		factoryCalls++
		return callback, nil
	}
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	graph.HandlerValue().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/wecom/callback/route", nil))
	if response.Code != http.StatusNoContent || factoryCalls != 1 || callback.calls.Load() != 1 {
		t.Fatalf("WeCom callback wiring = status %d factory %d calls %d", response.Code, factoryCalls, callback.calls.Load())
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if callback.beginShutdown.Load() != 1 || callback.closed.Load() != 1 {
		t.Fatalf("WeCom lifecycle = begin %d close %d", callback.beginShutdown.Load(), callback.closed.Load())
	}
}

func TestRuntimeOwnsAllWeComAIBotConnectionsAndReadiness(t *testing.T) {
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	first, second := newBootstrapAIBot(), newBootstrapAIBot()
	config.WeComAIBotFactories = []func(gateway.DispatchService) (channels.PollingAdapter, error){
		func(gateway.DispatchService) (channels.PollingAdapter, error) { return first, nil },
		func(gateway.DispatchService) (channels.PollingAdapter, error) { return second, nil },
	}
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	<-first.started
	<-second.started
	deadline := time.Now().Add(time.Second)
	for !graph.Ready() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !graph.Ready() {
		t.Fatal("runtime never became ready after AI Bot authentication")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if first.beginShutdown.Load() != 1 || first.closed.Load() != 1 || second.beginShutdown.Load() != 1 || second.closed.Load() != 1 {
		t.Fatalf("AI Bot lifecycle was not owned: first=%d/%d second=%d/%d", first.beginShutdown.Load(), first.closed.Load(), second.beginShutdown.Load(), second.closed.Load())
	}
}

func TestBootstrapBuildsRuntimeRegistryFromStorageFactory(t *testing.T) {
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	config.Sessions = nil
	config.StorageFactory = storagefactory.StorageFactoryFunc(func(_ context.Context, input backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
		return storagefactory.NewCapabilitySet(input.TenantID, map[backend.Capability]any{backend.CapabilitySession: inmemory.NewSessionService()})
	})
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if !graph.Ready() {
		t.Fatal("storage-factory bootstrap graph is not ready")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBootstrapOwnsOptionalExecutionQueueLifecycle(t *testing.T) {
	store := runtimequeue.NewMemory()
	defer func() { _ = store.Close() }()
	worker, err := runtimequeue.New(runtimequeue.Config{
		Store: store, Handler: func(context.Context, runtimequeue.Task) error { return nil },
		Owner: "bootstrap-queue", LeaseDuration: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	config.ExecutionQueue = worker
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RunOnce(context.Background()); !errors.Is(err, runtimequeue.ErrClosed) {
		t.Fatalf("queue after Bootstrap close = %v", err)
	}
}

func TestBootstrapPassesExplicitAttachmentCapabilities(t *testing.T) {
	store := runtimestorageinmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	config.SessionStore = store
	config.EventHistoryStore = store
	config.MessageStore = store
	config.ReplyBatchStore = store
	config.Attachments = store
	config.AttachmentStore = store
	if err := prepareRuntimeConfig(&config); err != nil {
		t.Fatal(err)
	}
	if config.Attachments != store || config.AttachmentStore != store {
		t.Fatalf("configured attachment capabilities = reader:%T store:%T", config.Attachments, config.AttachmentStore)
	}
}

func TestPrepareRuntimeConfigOwnsDefaultCapabilities(t *testing.T) {
	var previousClosed atomic.Bool
	config := Config{CloseDependencies: func() error {
		previousClosed.Store(true)
		return nil
	}}
	if err := prepareRuntimeConfig(&config); err != nil {
		t.Fatal(err)
	}
	if config.SessionStore == nil || config.EventHistoryStore == nil || config.MessageStore == nil || config.ReplyBatchStore == nil || config.Attachments == nil || config.AttachmentStore == nil {
		t.Fatalf("default runtime capabilities = session:%T history:%T message:%T reply:%T attachments:%T attachmentStore:%T", config.SessionStore, config.EventHistoryStore, config.MessageStore, config.ReplyBatchStore, config.Attachments, config.AttachmentStore)
	}
	if err := config.CloseDependencies(); err != nil {
		t.Fatal(err)
	}
	if !previousClosed.Load() {
		t.Fatal("bootstrap did not preserve the existing dependency closer")
	}
}

func TestPrepareRuntimeConfigSelectsBudgetStoreByDatabaseDriver(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	postgresConfig := Config{DB: db, ControlPlaneDriver: ControlPlaneDriverPostgres}
	if err := prepareRuntimeConfig(&postgresConfig); err != nil {
		t.Fatal(err)
	}
	if _, ok := postgresConfig.BudgetStore.(*runtimebudgetpostgres.Store); !ok {
		t.Fatalf("PostgreSQL budget store = %T, want *postgres.Store", postgresConfig.BudgetStore)
	}
	if err := postgresConfig.CloseDependencies(); err != nil {
		t.Fatal(err)
	}

	injected := runtimebudgetmemory.New()
	mysqlConfig := Config{DB: db, ControlPlaneDriver: ControlPlaneDriverMySQL, BudgetStore: injected}
	if err := prepareRuntimeConfig(&mysqlConfig); err != nil {
		t.Fatal(err)
	}
	if mysqlConfig.BudgetStore != injected {
		t.Fatalf("injected budget store = %T, want preserved %T", mysqlConfig.BudgetStore, injected)
	}
	if err := mysqlConfig.CloseDependencies(); err != nil {
		t.Fatal(err)
	}

	localConfig := Config{ControlPlaneDriver: ControlPlaneDriverMySQL}
	if err := prepareRuntimeConfig(&localConfig); err != nil {
		t.Fatal(err)
	}
	if _, ok := localConfig.BudgetStore.(*runtimebudgetmemory.Store); !ok {
		t.Fatalf("local budget store = %T, want *inmemory.Store", localConfig.BudgetStore)
	}
	if err := localConfig.CloseDependencies(); err != nil {
		t.Fatal(err)
	}
}

func nilContextForTest() context.Context { return nil }

func setRequiredEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv(envPostgresDSN, "postgres://postgres:postgres@127.0.0.1:5432/control_plane")
	t.Setenv(envMySQLDSN, "")
	t.Setenv(envMySQLMigrationDSN, "")
	t.Setenv(envAPIToken, "api-token")
	t.Setenv(envTenantID, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAppID, "app_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAdminToken, "admin-token")
	t.Setenv(envAdminTenants, "*")
	t.Setenv(envModelAPIKey, "model-secret")
	t.Setenv(envSessionBackend, "inmemory")
}

func TestEnvironmentBootstrapPreservesCancellationAndRejectsBadLists(t *testing.T) {
	t.Setenv(envPostgresDSN, "postgres://postgres:postgres@127.0.0.1:5432/control_plane")
	t.Setenv(envAPIToken, "api-token")
	t.Setenv(envTenantID, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAppID, "app_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAdminToken, "admin-token")
	t.Setenv(envAdminTenants, "*")
	t.Setenv(envModelAPIKey, "test-secret")
	t.Setenv(envSessionBackend, "postgres")
	t.Setenv(envModelNames, "gpt-4o-mini,,custom.model")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty model list item error = %v", err)
	}
	t.Setenv(envModelNames, "gpt-4o-mini")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewFromEnvironment(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled environment bootstrap error = %v", err)
	}
}

func TestEnvironmentDependencyErrorBoundaries(t *testing.T) {
	if _, _, err := environmentCatalogs(environmentConfig{
		modelProvider: "invalid provider", modelNames: []string{"chat"}, endpointHosts: []string{"example.test"},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("invalid model catalog error = %v", err)
	}
	if _, _, err := environmentCatalogs(environmentConfig{
		modelProvider: "anthropic", modelNames: []string{"chat"}, endpointHosts: []string{"example.test"},
	}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unsupported model provider error = %v", err)
	}
	values, err := environmentList("TEST_LIST", "A, B", false)
	if err != nil || len(values) != 2 || values[0] != "A" || values[1] != "B" {
		t.Fatalf("case-preserving environment list = %#v, err=%v", values, err)
	}
	if _, err := environmentList("TEST_LIST", ",", true); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("empty environment list error = %v", err)
	}

	resolver := environmentSecretResolver{reference: "env/test-key", value: "secret"}
	validScope := modelprofile.SecretScope{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", SecretRef: "env/test-key"}
	if _, err := resolver.Resolve(nilContextForTest(), validScope); err == nil {
		t.Fatal("nil resolver context unexpectedly succeeded")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.Resolve(canceled, validScope); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled resolver error = %v", err)
	}
	if _, err := resolver.Resolve(context.Background(), modelprofile.SecretScope{TenantID: "invalid", SecretRef: validScope.SecretRef}); err == nil {
		t.Fatal("invalid secret scope unexpectedly succeeded")
	}
	if _, err := resolver.Resolve(context.Background(), modelprofile.SecretScope{TenantID: validScope.TenantID, SecretRef: "env/other"}); err == nil {
		t.Fatal("mismatched secret reference unexpectedly succeeded")
	}
	if _, err := (environmentSecretResolver{reference: validScope.SecretRef}).Resolve(context.Background(), validScope); err == nil {
		t.Fatal("empty environment secret unexpectedly succeeded")
	}

	factory := environmentModelFactory{}
	secret, err := modelprofile.NewSecretValue("secret")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := factory.New(nilContextForTest(), modelprofile.ModelFactoryInput{Model: "chat"}, secret); err == nil {
		t.Fatal("nil model factory context unexpectedly succeeded")
	}
	canceled, cancel = context.WithCancel(context.Background())
	cancel()
	if _, err := factory.New(canceled, modelprofile.ModelFactoryInput{Model: "chat"}, secret); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled model factory error = %v", err)
	}
	if _, err := factory.New(context.Background(), modelprofile.ModelFactoryInput{Model: "chat"}, modelprofile.SecretValue{}); err == nil {
		t.Fatal("empty model factory secret unexpectedly succeeded")
	}
	if _, err := factory.New(context.Background(), modelprofile.ModelFactoryInput{Model: "chat", Endpoint: "https://api.openai.com/v1"}, secret); err != nil {
		t.Fatalf("endpoint model factory error = %v", err)
	}
	if _, err := factory.New(context.Background(), modelprofile.ModelFactoryInput{Provider: "anthropic", Model: "chat"}, secret); err == nil {
		t.Fatal("unsupported model factory provider unexpectedly succeeded")
	}
}

func TestNewFromEnvironmentRejectsInvalidConfigurationBeforeOpeningDatabase(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T)
	}{
		{
			name:  "catalog",
			setup: func(t *testing.T) { t.Setenv(envModelProvider, "anthropic") },
		},
		{
			name: "api authenticator",
			setup: func(t *testing.T) {
				t.Setenv(envAPIIdentities, "api-token|invalid-tenant|app_01ARZ3NDEKTSV4RRFFQ69G5FAV|service")
			},
		},
		{
			name:  "admin authenticator",
			setup: func(t *testing.T) { t.Setenv(envAdminToken, "admin-token\ninvalid") },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnvironment(t)
			tt.setup(t)
			previousOpen := openEnvironmentDatabase
			var openCalls atomic.Int32
			openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) {
				openCalls.Add(1)
				return nil, errors.New("database should not be opened")
			}
			defer func() { openEnvironmentDatabase = previousOpen }()

			_, err := NewFromEnvironment(context.Background())
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("configuration error = %v", err)
			}
			if openCalls.Load() != 0 {
				t.Fatalf("database opened %d times for preflight error", openCalls.Load())
			}
		})
	}
}

func TestNewFromEnvironmentDatabaseOpenErrorPreservesCancellation(t *testing.T) {
	for _, tt := range []struct {
		name      string
		ctx       func() context.Context
		wantError error
	}{
		{name: "unavailable", ctx: context.Background, wantError: ErrInvalidConfig},
		{name: "canceled", ctx: func() context.Context { ctx, cancel := context.WithCancel(context.Background()); cancel(); return ctx }, wantError: context.Canceled},
	} {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnvironment(t)
			previousOpen := openEnvironmentDatabase
			openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) {
				return nil, errors.New("database unavailable")
			}
			defer func() { openEnvironmentDatabase = previousOpen }()

			_, err := NewFromEnvironment(tt.ctx())
			if !errors.Is(err, tt.wantError) {
				t.Fatalf("database open error = %v, want %v", err, tt.wantError)
			}
		})
	}
}

func TestEnvironmentRuntimeStoreSelection(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if store, err := environmentRuntimeStore("inmemory", db); err != nil || store == nil {
		t.Fatalf("inmemory runtime store = %v, %v", store, err)
	}
	if store, err := environmentRuntimeStore("postgres", db); err != nil || store == nil {
		t.Fatalf("postgres runtime store = %v, %v", store, err)
	}
	if _, err := environmentRuntimeStore("unknown", db); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unknown runtime store = %v", err)
	}
	if _, err := environmentRuntimeStore("postgres", nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("postgres nil db = %v", err)
	}
}

func TestEnvironmentSelectsMySQLControlPlaneAndRejectsPostgresRuntimeStore(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envControlPlaneDriver, "mysql")
	t.Setenv(envPostgresDSN, "")
	t.Setenv(envMySQLDSN, "user:password@tcp(localhost:3306)/control_plane")
	t.Setenv(envMySQLMigrationDSN, "migration:password@tcp(localhost:3306)/control_plane")
	config, err := loadEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	if config.driver != ControlPlaneDriverMySQL || config.dsn != "user:password@tcp(localhost:3306)/control_plane" {
		t.Fatalf("MySQL environment config = %+v", config)
	}
	t.Setenv(envSessionBackend, "postgres")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("MySQL/PostgreSQL runtime combination error = %v", err)
	}
	t.Setenv(envSessionBackend, "inmemory")
	t.Setenv(envMySQLDSN, "")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing MySQL DSN error = %v", err)
	}
	t.Setenv(envMySQLDSN, "user:password@tcp(localhost:3306)/control_plane")
	t.Setenv(envMySQLMigrationDSN, "")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing MySQL migration DSN error = %v", err)
	}
	t.Setenv(envMySQLMigrationDSN, "migration:password@tcp(localhost:3306)/control_plane")
	t.Setenv(envControlPlaneDriver, "sqlite")
	if _, err := loadEnvironment(); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unknown control-plane driver error = %v", err)
	}
}

func TestEnvironmentRuntimeCapabilities(t *testing.T) {
	t.Run("requires atomic reply batches", func(t *testing.T) {
		_, _, _, err := environmentPrimaryRuntimeCapabilities(&environmentRuntimeStoreSpy{})
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("runtime capabilities error = %v", err)
		}
	})

	t.Run("derives optional capabilities", func(t *testing.T) {
		store := runtimestorageinmemory.New()
		t.Cleanup(func() { _ = store.Close() })
		replyBatchStore, attachments, attachmentStore, err := environmentPrimaryRuntimeCapabilities(store)
		if err != nil {
			t.Fatal(err)
		}
		if replyBatchStore == nil {
			t.Fatal("reply batch capability is nil")
		}
		if attachments == nil || attachmentStore == nil {
			t.Fatal("attachment capabilities are nil")
		}
		replyStore, messageStore, deliveryStore := environmentPrimaryDeliveryCapabilities(store)
		if replyStore != store || messageStore != store || deliveryStore != store {
			t.Fatalf("delivery capabilities = reply:%T message:%T delivery:%T", replyStore, messageStore, deliveryStore)
		}
	})
}

func TestEnvironmentAdminAuthenticator(t *testing.T) {
	static, err := environmentAdminAuthenticator(environmentConfig{adminToken: "admin", adminTenants: []string{"*"}})
	if err != nil || static == nil {
		t.Fatalf("static admin authenticator = %v, %v", static, err)
	}

	session, err := environmentAdminAuthenticator(environmentConfig{adminToken: "admin", adminTenants: []string{"*"}, adminUsername: "operator", adminPassword: "secret"})
	if err != nil || session == nil {
		t.Fatalf("session admin authenticator = %v, %v", session, err)
	}

	for _, config := range []environmentConfig{
		{adminToken: "admin\ninvalid", adminTenants: []string{"*"}},
		{adminToken: "admin", adminTenants: []string{"*"}, adminUsername: "operator", adminPassword: "secret\n"},
	} {
		if _, err := environmentAdminAuthenticator(config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("invalid admin authenticator error = %v", err)
		}
	}
}

func TestNewFromEnvironmentBootstrapsMySQLWithSeparateMigrationAccount(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envControlPlaneDriver, "mysql")
	t.Setenv(envPostgresDSN, "")
	t.Setenv(envMySQLDSN, "app:password@tcp(mysql)/control_plane")
	t.Setenv(envMySQLMigrationDSN, "migration:password@tcp(mysql)/control_plane")

	migrationDB, migrationMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	appDB, appMock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		_ = migrationDB.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = migrationDB.Close()
		_ = appDB.Close()
	})
	migrationMock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("migration@%"))
	migrationMock.ExpectQuery("SELECT DATABASE\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow("control_plane"))
	migrationMock.ExpectClose()
	appMock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("app@%"))
	appMock.ExpectQuery("SELECT DATABASE\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow("control_plane"))
	appMock.ExpectPing()
	appMock.ExpectQuery("SELECT DATABASE\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow("control_plane"))
	appMock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("app@%"))
	appMock.ExpectQuery("SELECT COUNT\\(\\*\\)").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	appMock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for app@%"}).AddRow("GRANT USAGE ON *.* TO 'app'@'%'"))
	appMock.ExpectPing()
	appMock.ExpectClose()

	previousOpen := openMySQLEnvironmentDatabase
	previousApply := applyMySQLEnvironmentMigrations
	previousVerify := verifyMySQLEnvironmentMigrations
	openCalls := 0
	var openedDSNs []string
	var applied, verified int
	openMySQLEnvironmentDatabase = func(_ context.Context, dsn string, _ mysql.Options) (*sql.DB, error) {
		openedDSNs = append(openedDSNs, dsn)
		openCalls++
		if openCalls == 1 {
			return migrationDB, nil
		}
		return appDB, nil
	}
	applyMySQLEnvironmentMigrations = func(_ context.Context, db *sql.DB) error {
		if db != migrationDB {
			return errors.New("migrations used application database")
		}
		applied++
		return nil
	}
	verifyMySQLEnvironmentMigrations = func(_ context.Context, db *sql.DB) error {
		if db != migrationDB && db != appDB {
			return errors.New("verification used unknown database")
		}
		verified++
		return nil
	}
	t.Cleanup(func() {
		openMySQLEnvironmentDatabase = previousOpen
		applyMySQLEnvironmentMigrations = previousApply
		verifyMySQLEnvironmentMigrations = previousVerify
	})

	graph, err := NewFromEnvironment(context.Background())
	if err != nil {
		t.Fatalf("%v (opens=%#v applied=%d verified=%d app expectations=%v)", err, openedDSNs, applied, verified, appMock.ExpectationsWereMet())
	}
	if !graph.Ready() {
		_ = graph.Close()
		t.Fatal("MySQL environment graph is not ready")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if applied != 1 || verified != 1 {
		t.Fatalf("MySQL migration/verification calls = applied %d verified %d", applied, verified)
	}
	if len(openedDSNs) != 2 || openedDSNs[0] != "migration:password@tcp(mysql)/control_plane" || openedDSNs[1] != "app:password@tcp(mysql)/control_plane" {
		t.Fatalf("MySQL bootstrap DSNs = %#v", openedDSNs)
	}
	if err := appMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNewFromEnvironmentRejectsSharedMySQLAccount(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envControlPlaneDriver, "mysql")
	t.Setenv(envPostgresDSN, "")
	t.Setenv(envMySQLDSN, "shared:password@tcp(mysql)/control_plane")
	t.Setenv(envMySQLMigrationDSN, "shared:password@tcp(mysql)/control_plane")

	migrationDB, migrationMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	appDB, appMock, err := sqlmock.New()
	if err != nil {
		_ = migrationDB.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = migrationDB.Close()
		_ = appDB.Close()
	})
	migrationMock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("shared@%"))
	migrationMock.ExpectQuery("SELECT DATABASE\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow("control_plane"))
	migrationMock.ExpectClose()
	appMock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("shared@%"))
	appMock.ExpectQuery("SELECT DATABASE\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow("control_plane"))
	appMock.ExpectClose()

	previousOpen := openMySQLEnvironmentDatabase
	previousApply := applyMySQLEnvironmentMigrations
	previousVerify := verifyMySQLEnvironmentMigrations
	openCalls := 0
	openMySQLEnvironmentDatabase = func(_ context.Context, _ string, _ mysql.Options) (*sql.DB, error) {
		openCalls++
		if openCalls == 1 {
			return migrationDB, nil
		}
		return appDB, nil
	}
	applyMySQLEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	verifyMySQLEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	t.Cleanup(func() {
		openMySQLEnvironmentDatabase = previousOpen
		applyMySQLEnvironmentMigrations = previousApply
		verifyMySQLEnvironmentMigrations = previousVerify
	})

	if _, err := NewFromEnvironment(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("shared MySQL account error = %v", err)
	}
	if err := migrationMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if err := appMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNewFromEnvironmentRejectsDifferentMySQLDatabase(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envControlPlaneDriver, "mysql")
	t.Setenv(envPostgresDSN, "")
	t.Setenv(envMySQLDSN, "app:password@tcp(mysql)/application_db")
	t.Setenv(envMySQLMigrationDSN, "migration:password@tcp(mysql)/control_plane")

	migrationDB, migrationMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	appDB, appMock, err := sqlmock.New()
	if err != nil {
		_ = migrationDB.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = migrationDB.Close()
		_ = appDB.Close()
	})
	migrationMock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("migration@%"))
	migrationMock.ExpectQuery("SELECT DATABASE\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow("control_plane"))
	migrationMock.ExpectClose()
	appMock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("app@%"))
	appMock.ExpectQuery("SELECT DATABASE\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow("application_db"))
	appMock.ExpectClose()

	previousOpen := openMySQLEnvironmentDatabase
	previousApply := applyMySQLEnvironmentMigrations
	previousVerify := verifyMySQLEnvironmentMigrations
	openCalls := 0
	openMySQLEnvironmentDatabase = func(_ context.Context, _ string, _ mysql.Options) (*sql.DB, error) {
		openCalls++
		if openCalls == 1 {
			return migrationDB, nil
		}
		return appDB, nil
	}
	applyMySQLEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	verifyMySQLEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	t.Cleanup(func() {
		openMySQLEnvironmentDatabase = previousOpen
		applyMySQLEnvironmentMigrations = previousApply
		verifyMySQLEnvironmentMigrations = previousVerify
	})

	if _, err := NewFromEnvironment(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("different MySQL database error = %v", err)
	}
	if err := migrationMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if err := appMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNewFromEnvironmentRejectsMySQLApplicationOpenFailure(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envControlPlaneDriver, "mysql")
	t.Setenv(envPostgresDSN, "")
	t.Setenv(envMySQLDSN, "app:password@tcp(mysql)/control_plane")
	t.Setenv(envMySQLMigrationDSN, "migration:password@tcp(mysql)/control_plane")

	migrationDB, migrationMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = migrationDB.Close() })
	migrationMock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("migration@%"))
	migrationMock.ExpectClose()

	previousOpen := openMySQLEnvironmentDatabase
	previousApply := applyMySQLEnvironmentMigrations
	previousVerify := verifyMySQLEnvironmentMigrations
	openCalls := 0
	openMySQLEnvironmentDatabase = func(_ context.Context, _ string, _ mysql.Options) (*sql.DB, error) {
		openCalls++
		if openCalls == 1 {
			return migrationDB, nil
		}
		return nil, errors.New("application DSN unavailable")
	}
	applyMySQLEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	verifyMySQLEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	t.Cleanup(func() {
		openMySQLEnvironmentDatabase = previousOpen
		applyMySQLEnvironmentMigrations = previousApply
		verifyMySQLEnvironmentMigrations = previousVerify
	})

	if _, err := NewFromEnvironment(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("application open failure = %v", err)
	}
	if err := migrationMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareDatabaseConfigBuildsMySQLRepositories(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectPing()
	mock.ExpectQuery("SELECT DATABASE\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"DATABASE()"}).AddRow("control_plane"))
	mock.ExpectQuery("SELECT CURRENT_USER\\(\\)").WillReturnRows(sqlmock.NewRows([]string{"CURRENT_USER()"}).AddRow("app@%"))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))
	mock.ExpectQuery("SHOW GRANTS").WillReturnRows(sqlmock.NewRows([]string{"Grants for app@%"}).AddRow("GRANT USAGE ON *.* TO 'app'@'%'"))
	config := Config{DB: db, ControlPlaneDriver: ControlPlaneDriverMySQL}
	if err := prepareDatabaseConfig(context.Background(), &config); err != nil {
		t.Fatal(err)
	}
	if config.Tenants == nil || config.Apps == nil || config.Models == nil || config.Backends == nil || config.Channels == nil {
		t.Fatalf("MySQL repositories were not built: %+v", config)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareDatabaseConfigFailsClosedWhenMigrationVerificationFails(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectPing()
	verified := false
	config := Config{
		DB: db, ControlPlaneDriver: ControlPlaneDriverMySQL,
		VerifyMigrations: func(context.Context, *sql.DB) error {
			verified = true
			return errors.New("schema is incomplete")
		},
	}
	if err := prepareDatabaseConfig(context.Background(), &config); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("verification error = %v", err)
	}
	if !verified {
		t.Fatal("migration verification was not called before repository construction")
	}
	if config.Tenants != nil || config.Apps != nil {
		t.Fatal("repositories were constructed after verification failure")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNewFromEnvironmentBuildsRealGraphWhenDatabaseOpens(t *testing.T) {
	t.Setenv(envPostgresDSN, "postgres://configured")
	t.Setenv(envAPIToken, "api-token")
	t.Setenv(envTenantID, "t_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAppID, "app_01ARZ3NDEKTSV4RRFFQ69G5FAV")
	t.Setenv(envAdminToken, "admin-token")
	t.Setenv(envAdminTenants, "*")
	t.Setenv(envModelAPIKey, "test-secret")
	t.Setenv(envSessionBackend, "postgres")

	registerBootstrapPingDriver.Do(func() {
		sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{})
	})
	db, err := sql.Open("trpc-service-bootstrap-ping", "")
	if err != nil {
		t.Fatal(err)
	}
	previousOpen := openEnvironmentDatabase
	previousApply := applyEnvironmentMigrations
	previousVerify := verifyEnvironmentMigrations
	openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) {
		return db, nil
	}
	applyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	verifyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	defer func() { openEnvironmentDatabase = previousOpen }()
	defer func() { applyEnvironmentMigrations = previousApply; verifyEnvironmentMigrations = previousVerify }()

	graph, err := NewFromEnvironment(context.Background())
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if !graph.Ready() {
		t.Fatal("environment bootstrap graph is not ready")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}

	openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) {
		return nil, errors.New("database open failure")
	}
	if _, err := NewFromEnvironment(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("database open failure = %v", err)
	}
}

func TestNewFromEnvironmentClosesDatabaseWhenMigrationFails(t *testing.T) {
	setRequiredEnvironment(t)
	registerBootstrapPingDriver.Do(func() {
		sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{})
	})
	db, err := sql.Open("trpc-service-bootstrap-ping", "")
	if err != nil {
		t.Fatal(err)
	}
	previousOpen := openEnvironmentDatabase
	previousApply := applyEnvironmentMigrations
	openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) { return db, nil }
	applyEnvironmentMigrations = func(context.Context, *sql.DB) error { return errors.New("migration failed") }
	defer func() {
		openEnvironmentDatabase = previousOpen
		applyEnvironmentMigrations = previousApply
	}()

	if _, err := NewFromEnvironment(context.Background()); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("migration error = %v", err)
	}
	if err := db.Ping(); err == nil {
		t.Fatal("database remained open after migration failure")
	}
}

func TestNewFromEnvironmentInstallsWeComCallbackAndOutboxWorker(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envWeComCallbackToken, "callback-token")
	t.Setenv(envWeComEncodingAESKey, base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)))
	t.Setenv(envWeComAppSecret, "app-secret")
	t.Setenv(envWeComSecretRef, "env/wecom")

	registerBootstrapPingDriver.Do(func() {
		sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{})
	})
	db, err := sql.Open("trpc-service-bootstrap-ping", "")
	if err != nil {
		t.Fatal(err)
	}
	previousOpen := openEnvironmentDatabase
	previousApply := applyEnvironmentMigrations
	previousVerify := verifyEnvironmentMigrations
	previousWorker := newEnvironmentWeComWorker
	var workerConfig outbox.Config
	openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) { return db, nil }
	applyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	verifyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
	newEnvironmentWeComWorker = func(config outbox.Config) (*outbox.Worker, error) {
		workerConfig = config
		return outbox.New(config)
	}
	defer func() { openEnvironmentDatabase = previousOpen }()
	defer func() { applyEnvironmentMigrations = previousApply }()
	defer func() { verifyEnvironmentMigrations = previousVerify }()
	defer func() { newEnvironmentWeComWorker = previousWorker }()

	graph, err := NewFromEnvironment(context.Background())
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if graph.OutboxWorker == nil || graph.wecomLifecycle == nil {
		_ = graph.Close()
		t.Fatal("WeCom environment did not install callback and outbox components")
	}
	if workerConfig.AuditWriter == nil {
		_ = graph.Close()
		t.Fatal("WeCom environment outbox worker did not receive an audit writer")
	}
	callback := httptest.NewRecorder()
	graph.HandlerValue().ServeHTTP(callback, httptest.NewRequest(http.MethodPost, "/wecom/callback/environment-route", nil))
	if callback.Code != http.StatusForbidden {
		_ = graph.Close()
		t.Fatalf("WeCom environment callback status = %d", callback.Code)
	}
	if err := graph.OutboxWorker.Start(context.Background(), time.Second); !errors.Is(err, outbox.ErrAlreadyRunning) {
		_ = graph.Close()
		t.Fatalf("environment outbox worker was not started: %v", err)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNewFromEnvironmentCleansUpWhenWeComWorkerSetupFails(t *testing.T) {
	setRequiredEnvironment(t)
	t.Setenv(envWeComCallbackToken, "callback-token")
	t.Setenv(envWeComEncodingAESKey, base64.RawStdEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)))
	t.Setenv(envWeComAppSecret, "app-secret")
	t.Setenv(envWeComSecretRef, "env/wecom")

	registerBootstrapPingDriver.Do(func() {
		sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{})
	})
	previousOpen := openEnvironmentDatabase
	previousApply := applyEnvironmentMigrations
	previousVerify := verifyEnvironmentMigrations
	previousOwner := environmentWeComOwnerFunc
	previousWorker := newEnvironmentWeComWorker
	defer func() {
		openEnvironmentDatabase = previousOpen
		applyEnvironmentMigrations = previousApply
		verifyEnvironmentMigrations = previousVerify
		environmentWeComOwnerFunc = previousOwner
		newEnvironmentWeComWorker = previousWorker
	}()

	tests := []struct {
		name      string
		ownerErr  error
		workerErr error
	}{
		{name: "owner", ownerErr: errors.New("owner unavailable")},
		{name: "worker", workerErr: errors.New("worker unavailable")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := sql.Open("trpc-service-bootstrap-ping", "")
			if err != nil {
				t.Fatal(err)
			}
			openEnvironmentDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) { return db, nil }
			applyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
			verifyEnvironmentMigrations = func(context.Context, *sql.DB) error { return nil }
			if tt.ownerErr != nil {
				environmentWeComOwnerFunc = func() (string, error) { return "", tt.ownerErr }
			} else {
				environmentWeComOwnerFunc = func() (string, error) { return "test-owner", nil }
			}
			if tt.workerErr != nil {
				newEnvironmentWeComWorker = func(outbox.Config) (*outbox.Worker, error) { return nil, tt.workerErr }
			} else {
				newEnvironmentWeComWorker = outbox.New
			}

			if _, err := NewFromEnvironment(context.Background()); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("setup error = %v", err)
			}
			if err := db.Ping(); err == nil {
				t.Fatal("database was not closed after WeCom setup failure")
			}
		})
	}
}

var registerBootstrapPingDriver sync.Once

func TestNewUsesDatabasePingAndBuildsPostgreSQLRepositories(t *testing.T) {
	registerBootstrapPingDriver.Do(func() {
		sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{})
	})
	db, err := sql.Open("trpc-service-bootstrap-ping", "")
	if err != nil {
		t.Fatal(err)
	}
	config, closeDependencies := testConfig(t)
	config.DB, config.OwnDB = db, true
	config.CloseDependencies = func() error {
		closeDependencies()
		return nil
	}
	config.Tenants, config.Apps, config.Models, config.Backends, config.Channels = nil, nil, nil, nil, nil
	graph, err := New(context.Background(), config)
	if err != nil {
		_ = db.Close()
		closeDependencies()
		t.Fatal(err)
	}
	if !graph.Ready() {
		t.Fatal("database-backed bootstrap graph is not ready")
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
}

type bootstrapPingDriver struct{}

func (bootstrapPingDriver) Open(string) (driver.Conn, error) { return bootstrapPingConn{}, nil }

type bootstrapPingConn struct{}

func (bootstrapPingConn) Prepare(string) (driver.Stmt, error) { return nil, driver.ErrSkip }
func (bootstrapPingConn) Close() error                        { return nil }
func (bootstrapPingConn) Begin() (driver.Tx, error)           { return nil, driver.ErrSkip }
func (bootstrapPingConn) Ping(context.Context) error          { return nil }

type trackingRuntimeStore struct {
	environmentStorage
	closed atomic.Bool
}

func (store *trackingRuntimeStore) Close() error {
	store.closed.Store(true)
	return nil
}

func testConfig(t *testing.T) (Config, func()) {
	t.Helper()
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider: "test", Models: []string{"test-model"},
		EndpointPolicy: modelprofile.FieldForbidden, SecretRefPolicy: modelprofile.FieldForbidden,
	})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "test", Capabilities: []backend.Capability{backend.CapabilitySession},
		EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
		Options: map[string]backend.OptionSpec{},
	})
	if err != nil {
		t.Fatal(err)
	}
	authenticator, err := gateway.NewStaticAPIAuthenticator(map[string]gateway.APIIdentity{})
	if err != nil {
		t.Fatal(err)
	}
	sessions := inmemory.NewSessionService()
	config := Config{
		Tenants: tenantmemory.NewRepository(), Apps: appmemory.NewRepository(),
		Models: modelmemory.NewRepository(modelCatalog), Backends: backendmemory.NewRepository(backendCatalog),
		Channels: channelmemory.NewRepository(), ModelCatalog: modelCatalog, BackendCatalog: backendCatalog,
		SecretResolver: testSecretResolver{}, ModelFactory: testModelFactory{}, Sessions: sessions,
		Authenticator: authenticator,
	}
	return config, func() { _ = sessions.Close() }
}

type testSecretResolver struct{}

func (testSecretResolver) Resolve(context.Context, modelprofile.SecretScope) (modelprofile.SecretValue, error) {
	return modelprofile.SecretValue{}, errors.New("test resolver failure")
}

type testModelFactory struct{}

func (testModelFactory) New(context.Context, modelprofile.ModelFactoryInput, modelprofile.SecretValue) (trpcmodel.Model, error) {
	return nil, errors.New("test factory failure")
}

type bootstrapNoopDispatcher struct{}

func (bootstrapNoopDispatcher) Dispatch(context.Context, gateway.DispatchRequest) (<-chan gateway.DispatchEvent, error) {
	events := make(chan gateway.DispatchEvent)
	close(events)
	return events, nil
}

type bootstrapBlockingProvider struct {
	started  chan struct{}
	canceled chan struct{}
	once     sync.Once
}

type bootstrapStaticProvider struct{ receipt string }

func (p bootstrapStaticProvider) Deliver(context.Context, runtimestorage.ReplyOutbox) (string, error) {
	return p.receipt, nil
}

func (p bootstrapStaticProvider) Reconcile(context.Context, runtimestorage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	return outbox.DeliveryAccepted, p.receipt, nil
}

type candidateOnly struct{ channels.CandidateConsumer }

type bootstrapTenantRuntimeInvalidator struct {
	tenantID string
}

func (invalidator *bootstrapTenantRuntimeInvalidator) Ensure(context.Context, string) error {
	return nil
}

func (invalidator *bootstrapTenantRuntimeInvalidator) InvalidateTenant(tenantID string) {
	invalidator.tenantID = tenantID
}

func createBootstrapTenantExecutionState(
	t *testing.T,
	tenants *tenantmemory.InMemoryRepository,
	apps *appmemory.InMemoryRepository,
	models *modelmemory.InMemoryRepository,
	backends *backendmemory.InMemoryRepository,
	tenantKey, appKey, modelName, secretRef string,
) (*tenant.Tenant, *appmodel.App) {
	t.Helper()
	root, err := tenants.Create(context.Background(), tenant.CreateInput{TenantKey: tenantKey, DisplayName: tenantKey, AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	profile, _, err := models.Create(context.Background(), modelprofile.CreateInput{
		TenantID: root.TenantID, ProfileKey: "model-" + tenantKey, DisplayName: "Model " + tenantKey,
		Configuration: modelprofile.Configuration{Provider: "fake", Model: modelName, SecretRef: secretRef},
		Metadata:      modelprofile.ChangeMetadata{ActorType: "test", ActorID: "bootstrap", Reason: "fixture", CorrelationID: tenantKey},
	})
	if err != nil {
		t.Fatal(err)
	}
	profileBackend, _, err := backends.Create(context.Background(), backend.CreateInput{
		TenantID: root.TenantID, ProfileKey: "backend-" + tenantKey, DisplayName: "Backend " + tenantKey,
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "memory", Options: map[string]string{"namespace": tenantKey}}},
		Metadata: backend.ChangeMetadata{ActorType: "test", ActorID: "bootstrap", Reason: "fixture", CorrelationID: tenantKey},
	})
	if err != nil {
		t.Fatal(err)
	}
	app, err := apps.Create(context.Background(), appmodel.CreateInput{TenantID: root.TenantID, AppKey: appKey, DisplayName: appKey})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := apps.CreateDraft(context.Background(), appmodel.CreateDraftInput{
		TenantID: root.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version,
		Configuration: appmodel.DraftConfiguration{Instruction: "answer", ModelProfileID: profile.ProfileID, Runtime: appmodel.DefaultRuntimePolicy()},
	})
	if err != nil {
		t.Fatal(err)
	}
	published, _, _, err := apps.Publish(context.Background(), appmodel.PublishInput{
		TenantID: root.TenantID, AppID: app.AppID, Revision: draft.Revision, ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion, TenantActive: true,
		Metadata: appmodel.ChangeMetadata{ActorType: "test", ActorID: "bootstrap", Reason: "fixture", CorrelationID: tenantKey},
	})
	if err != nil {
		t.Fatal(err)
	}
	appID, backendID := published.AppID, profileBackend.ProfileID
	updated, err := tenants.UpdateConfiguration(context.Background(), tenant.UpdateConfigurationInput{
		TenantID: root.TenantID, ExpectedVersion: root.Version, DisplayName: root.DisplayName, AuditRetentionDays: root.AuditRetentionDays,
		LogMaskingLevel: root.LogMaskingLevel, TraceSamplingRate: root.TraceSamplingRate, DefaultAgentAppID: &appID, DefaultBackendProfileID: &backendID,
	})
	if err != nil {
		t.Fatal(err)
	}
	return updated, published
}

type bootstrapRecordingModelFactory struct {
	mu      sync.Mutex
	secrets map[string]string
}

func (factory *bootstrapRecordingModelFactory) New(_ context.Context, input modelprofile.ModelFactoryInput, secret modelprofile.SecretValue) (trpcmodel.Model, error) {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	if factory.secrets == nil {
		factory.secrets = make(map[string]string)
	}
	factory.secrets[input.TenantID] = secret.Value()
	return bootstrapTestModel{}, nil
}

func (factory *bootstrapRecordingModelFactory) Secret(tenantID string) string {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	return factory.secrets[tenantID]
}

type bootstrapTestModel struct{}

func (bootstrapTestModel) Info() trpcmodel.Info { return trpcmodel.Info{Name: "bootstrap-test"} }
func (bootstrapTestModel) GenerateContent(context.Context, *trpcmodel.Request) (<-chan *trpcmodel.Response, error) {
	return nil, errors.New("unused bootstrap test model")
}

type bootstrapRecordingStorageFactory struct {
	mu       sync.Mutex
	sessions map[string]int
}

func (factory *bootstrapRecordingStorageFactory) New(_ context.Context, input backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
	factory.mu.Lock()
	if factory.sessions == nil {
		factory.sessions = make(map[string]int)
	}
	factory.sessions[input.TenantID]++
	factory.mu.Unlock()
	return storagefactory.NewCapabilitySet(input.TenantID, map[backend.Capability]any{backend.CapabilitySession: inmemory.NewSessionService()})
}

func (factory *bootstrapRecordingStorageFactory) SessionCount(tenantID string) int {
	factory.mu.Lock()
	defer factory.mu.Unlock()
	return factory.sessions[tenantID]
}

type bootstrapWeComLifecycle struct {
	calls         atomic.Int32
	beginShutdown atomic.Int32
	closed        atomic.Int32
}

type bootstrapAIBot struct {
	started       chan struct{}
	startOnce     sync.Once
	ready         atomic.Bool
	beginShutdown atomic.Int32
	closed        atomic.Int32
}

func newBootstrapAIBot() *bootstrapAIBot          { return &bootstrapAIBot{started: make(chan struct{})} }
func (*bootstrapAIBot) Channel() channels.Channel { return channels.ChannelWeComAIBot }
func (bot *bootstrapAIBot) Ready() bool           { return bot.ready.Load() }
func (bot *bootstrapAIBot) Run(ctx context.Context) error {
	bot.ready.Store(true)
	bot.startOnce.Do(func() { close(bot.started) })
	<-ctx.Done()
	bot.ready.Store(false)
	return ctx.Err()
}
func (bot *bootstrapAIBot) BeginShutdown() { bot.beginShutdown.Add(1) }
func (bot *bootstrapAIBot) Close() error   { bot.closed.Add(1); return nil }

func (handler *bootstrapWeComLifecycle) ServeHTTP(writer http.ResponseWriter, _ *http.Request) {
	handler.calls.Add(1)
	writer.WriteHeader(http.StatusNoContent)
}
func (handler *bootstrapWeComLifecycle) BeginShutdown() { handler.beginShutdown.Add(1) }
func (handler *bootstrapWeComLifecycle) Close() error {
	handler.closed.Add(1)
	return nil
}

func (p *bootstrapBlockingProvider) Deliver(ctx context.Context, _ runtimestorage.ReplyOutbox) (string, error) {
	p.once.Do(func() { close(p.started) })
	<-ctx.Done()
	close(p.canceled)
	return "", ctx.Err()
}

func (*bootstrapBlockingProvider) Reconcile(context.Context, runtimestorage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	return outbox.DeliveryUnknown, "", nil
}

var (
	_ tenant.Repository          = (*tenantmemory.InMemoryRepository)(nil)
	_ appmodel.Repository        = (*appmemory.InMemoryRepository)(nil)
	_ channels.CandidateConsumer = (*channelmemory.InMemoryRepository)(nil)
	_ session.Service            = (*inmemory.SessionService)(nil)
)
