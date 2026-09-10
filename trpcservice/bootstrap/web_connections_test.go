package bootstrap

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/admin"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	appmemory "github.com/XnLemon/trpc-agent-service/trpcservice/app/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom_aibot"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	modelruntime "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/model"
	storage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
)

func TestNewWebChannelConnectionsValidatesDependencies(t *testing.T) {
	if _, err := newWebChannelConnections(Config{}, nil); !errors.Is(err, admin.ErrConnectionUnavailable) {
		t.Fatalf("empty web connection config error = %v", err)
	}

	config, closeDependencies := testConfig(t)
	defer closeDependencies()
	config.SecretResolver = modelruntime.NewSecretRegistry()
	graph, err := New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = graph.Close() }()

	connections, err := newWebChannelConnections(config, graph)
	if err != nil {
		t.Fatal(err)
	}
	if connections == nil {
		t.Fatal("newWebChannelConnections returned nil")
	}
	graph.Resolver = nil
	if _, err := newWebChannelConnections(config, graph); err == nil {
		t.Fatal("invalid dispatcher dependencies were accepted")
	}
}

//nolint:gocyclo // Covers the complete connection replacement and shutdown lifecycle.
func TestWebChannelConnectionsConnectListReplaceAndDisconnect(t *testing.T) {
	connections, root, secrets := newWebConnectionsFixture(t)
	var adapters []*webConnectionAdapter
	connections.adapterFactory = func(_ context.Context, target channels.RoutingTarget, secret string) (channels.PollingAdapter, error) {
		if err := target.Validate(); err != nil {
			t.Fatalf("adapter target validation = %v", err)
		}
		if secret == "" {
			t.Fatal("adapter received an empty secret")
		}
		adapter := newWebConnectionAdapter(channels.ChannelTelegram, "web-bot")
		adapters = append(adapters, adapter)
		return adapter, nil
	}
	metadata := channels.ChangeMetadata{ActorType: "admin", ActorID: "operator", Reason: "connect", CorrelationID: "request-1"}
	first, err := connections.Connect(context.Background(), root.TenantID, admin.ConnectInput{Channel: channels.ChannelTelegram, BotID: "12345", Secret: "first-secret"}, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if first.BindingID == "" || first.BotID != "12345" || first.URL != "https://t.me/web-bot" || !first.Ready {
		t.Fatalf("first connection = %+v", first)
	}
	if len(adapters) != 1 {
		t.Fatalf("adapter count after first connect = %d", len(adapters))
	}
	if _, err := secrets.Resolve(context.Background(), modelruntime.SecretScope{TenantID: root.TenantID, SecretRef: bindingSecretRef(t, connections, root.TenantID, first.BindingID)}); err != nil {
		t.Fatalf("registered connection secret was not resolvable: %v", err)
	}

	listed, err := connections.List(context.Background(), root.TenantID)
	if err != nil || len(listed) != 1 || listed[0].BindingID != first.BindingID {
		t.Fatalf("listed connections = %+v, err=%v", listed, err)
	}
	otherTenant, err := connections.List(context.Background(), "another-tenant")
	if err != nil || len(otherTenant) != 0 {
		t.Fatalf("foreign tenant connections = %+v, err=%v", otherTenant, err)
	}

	second, err := connections.Connect(context.Background(), root.TenantID, admin.ConnectInput{Channel: channels.ChannelTelegram, BotID: "67890", Secret: "second-secret"}, metadata)
	if err != nil {
		t.Fatal(err)
	}
	if second.BindingID == first.BindingID || second.BotID != "67890" {
		t.Fatalf("replacement connection = %+v", second)
	}
	if adapters[0].closed.Load() != 1 {
		t.Fatalf("replaced adapter close count = %d", adapters[0].closed.Load())
	}
	oldBinding, err := connections.bindings.Get(context.Background(), root.TenantID, first.BindingID)
	if err != nil {
		t.Fatal(err)
	}
	if oldBinding.Status != channels.StatusDisabled {
		t.Fatalf("replaced binding status = %s, want disabled", oldBinding.Status)
	}
	if _, err := secrets.Resolve(context.Background(), modelruntime.SecretScope{TenantID: root.TenantID, SecretRef: oldBinding.SecretRef}); !errors.Is(err, modelruntime.ErrSecretUnavailable) {
		t.Fatalf("replaced connection secret error = %v", err)
	}

	if err := connections.Disconnect(context.Background(), root.TenantID, second.BindingID, metadata); err != nil {
		t.Fatal(err)
	}
	if adapters[1].closed.Load() != 1 {
		t.Fatalf("disconnected adapter close count = %d", adapters[1].closed.Load())
	}
	listed, err = connections.List(context.Background(), root.TenantID)
	if err != nil || len(listed) != 0 {
		t.Fatalf("connections after disconnect = %+v, err=%v", listed, err)
	}
	if err := connections.Disconnect(context.Background(), root.TenantID, second.BindingID, metadata); !errors.Is(err, admin.ErrConnectionFailed) {
		t.Fatalf("repeated disconnect error = %v", err)
	}
	if err := connections.Close(); err != nil {
		t.Fatal(err)
	}
	if err := connections.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWebChannelConnectionsRejectsInvalidAndFailedConnections(t *testing.T) {
	connections, root, _ := newWebConnectionsFixture(t)
	for name, input := range map[string]admin.ConnectInput{
		"blank bot id":              {Channel: channels.ChannelTelegram, BotID: " ", Secret: "secret"},
		"blank secret":              {Channel: channels.ChannelTelegram, BotID: "123", Secret: " "},
		"invalid channel":           {Channel: channels.ChannelWeCom, BotID: "123", Secret: "secret"},
		"non canonical telegram id": {Channel: channels.ChannelTelegram, BotID: "00123", Secret: "secret"},
		"negative telegram id":      {Channel: channels.ChannelTelegram, BotID: "-1", Secret: "secret"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := connections.Connect(context.Background(), root.TenantID, input, channels.ChangeMetadata{}); !errors.Is(err, admin.ErrConnectionFailed) {
				t.Fatalf("Connect() error = %v", err)
			}
		})
	}
	var nilContext context.Context
	if _, err := connections.Connect(nilContext, root.TenantID, admin.ConnectInput{Channel: channels.ChannelTelegram, BotID: "123", Secret: "secret"}, channels.ChangeMetadata{}); !errors.Is(err, admin.ErrConnectionFailed) {
		t.Fatalf("nil context Connect() error = %v", err)
	}
	if _, err := connections.List(nil, root.TenantID); !errors.Is(err, admin.ErrConnectionUnavailable) {
		t.Fatalf("nil context List() error = %v", err)
	}
	if err := connections.Disconnect(nil, root.TenantID, "binding", channels.ChangeMetadata{}); !errors.Is(err, admin.ErrConnectionUnavailable) {
		t.Fatalf("nil context Disconnect() error = %v", err)
	}
	var nilConnections *webChannelConnections
	if _, err := nilConnections.List(context.Background(), root.TenantID); !errors.Is(err, admin.ErrConnectionUnavailable) {
		t.Fatalf("nil connections List() error = %v", err)
	}
	if err := nilConnections.Disconnect(context.Background(), root.TenantID, "binding", channels.ChangeMetadata{}); !errors.Is(err, admin.ErrConnectionUnavailable) {
		t.Fatalf("nil connections Disconnect() error = %v", err)
	}
	if err := nilConnections.Close(); err != nil {
		t.Fatal(err)
	}

	connections.closed = true
	if _, err := connections.Connect(context.Background(), root.TenantID, admin.ConnectInput{Channel: channels.ChannelTelegram, BotID: "123", Secret: "secret"}, channels.ChangeMetadata{}); !errors.Is(err, admin.ErrConnectionUnavailable) {
		t.Fatalf("closed Connect() error = %v", err)
	}
	connections.closed = false
	metadata := channels.ChangeMetadata{ActorType: "admin", ActorID: "operator", Reason: "adapter failure", CorrelationID: "adapter-failure-1"}
	connections.adapterFactory = func(context.Context, channels.RoutingTarget, string) (channels.PollingAdapter, error) {
		return nil, errors.New("adapter construction failed")
	}
	if _, err := connections.Connect(context.Background(), root.TenantID, admin.ConnectInput{Channel: channels.ChannelTelegram, BotID: "123", Secret: "secret"}, metadata); !errors.Is(err, admin.ErrConnectionFailed) {
		t.Fatalf("adapter failure error = %v", err)
	}
	if values, err := connections.List(context.Background(), root.TenantID); err != nil || len(values) != 0 {
		t.Fatalf("failed connection remained active: %+v, %v", values, err)
	}

	connections.adapterFactory = func(context.Context, channels.RoutingTarget, string) (channels.PollingAdapter, error) {
		return newWebConnectionAdapter(channels.ChannelTelegram, ""), nil
	}
	if _, err := connections.Connect(context.Background(), root.TenantID, admin.ConnectInput{Channel: channels.ChannelTelegram, BotID: "456", Secret: "secret"}, metadata); !errors.Is(err, admin.ErrConnectionFailed) {
		t.Fatalf("not-ready adapter error = %v", err)
	}
}

func TestWebChannelConnectionsCoversSetupFailureCleanup(t *testing.T) {
	testCases := []struct {
		name  string
		setup func(*webChannelConnections, *tenant.Tenant)
		want  error
	}{
		{name: "tenant missing", setup: func(connections *webChannelConnections, _ *tenant.Tenant) {
			connections.tenants = webTenantRepository{Repository: connections.tenants}
		}, want: admin.ErrAgentNotReady},
		{name: "tenant invalid", setup: func(connections *webChannelConnections, root *tenant.Tenant) {
			invalid := root.Clone()
			invalid.TraceSamplingRate = -1
			connections.tenants = webTenantRepository{Repository: connections.tenants, value: &invalid}
		}, want: admin.ErrAgentNotReady},
		{name: "agent unavailable", setup: func(connections *webChannelConnections, _ *tenant.Tenant) {
		}, want: admin.ErrAgentNotReady},
		{name: "binding create failure", setup: func(connections *webChannelConnections, _ *tenant.Tenant) {
			connections.bindings = webChannelRepository{Repository: connections.bindings, createErr: errors.New("create failed")}
		}, want: admin.ErrConnectionFailed},
		{name: "secret registration failure", setup: func(connections *webChannelConnections, _ *tenant.Tenant) {
			_ = connections.secrets.Close()
		}, want: admin.ErrConnectionFailed},
		{name: "binding activation failure", setup: func(connections *webChannelConnections, _ *tenant.Tenant) {
			connections.bindings = webChannelRepository{Repository: connections.bindings, activateErr: errors.New("activate failed")}
		}, want: admin.ErrConnectionFailed},
		{name: "target resolution failure", setup: func(connections *webChannelConnections, _ *tenant.Tenant) {
			connections.candidates = webCandidateRepository{CandidateConsumer: connections.candidates}
		}, want: admin.ErrAgentNotReady},
		{name: "nil adapter", setup: func(connections *webChannelConnections, _ *tenant.Tenant) {
			connections.adapterFactory = func(context.Context, channels.RoutingTarget, string) (channels.PollingAdapter, error) {
				return nil, nil
			}
		}, want: admin.ErrConnectionFailed},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			connections, root, _ := newWebConnectionsFixture(t)
			if test.name == "agent unavailable" {
				app, err := connections.apps.Get(context.Background(), root.TenantID, *root.DefaultAgentAppID)
				if err != nil {
					t.Fatal(err)
				}
				app.Status = appmodel.StatusSuspended
				connections.apps = webAppRepository{Repository: connections.apps, value: app}
				test.setup = func(*webChannelConnections, *tenant.Tenant) {}
			}
			test.setup(connections, root)
			metadata := channels.ChangeMetadata{ActorType: "admin", ActorID: "operator", Reason: "setup failure", CorrelationID: "setup-failure-1"}
			_, err := connections.Connect(context.Background(), root.TenantID, admin.ConnectInput{Channel: channels.ChannelTelegram, BotID: "123", Secret: "secret"}, metadata)
			if !errors.Is(err, test.want) {
				t.Fatalf("Connect() error = %v, want %v", err, test.want)
			}
		})
	}

}

func TestWebChannelConnectionsReturnsFailureWhenReplacingConnectionCannotStop(t *testing.T) {
	connections, root, _ := newWebConnectionsFixture(t)
	metadata := channels.ChangeMetadata{ActorType: "admin", ActorID: "operator", Reason: "replace", CorrelationID: "replace-1"}
	var first *webConnectionAdapter
	connections.adapterFactory = func(context.Context, channels.RoutingTarget, string) (channels.PollingAdapter, error) {
		first = newWebConnectionAdapter(channels.ChannelTelegram, "first")
		return first, nil
	}
	if _, err := connections.Connect(context.Background(), root.TenantID, admin.ConnectInput{Channel: channels.ChannelTelegram, BotID: "123", Secret: "first"}, metadata); err != nil {
		t.Fatalf("first connection failed: adapter=%#v ready=%v err=%T %v", first, first != nil && first.Ready(), err, err)
	}
	first.closeErr = errors.New("close failed")
	if _, err := connections.Connect(context.Background(), root.TenantID, admin.ConnectInput{Channel: channels.ChannelTelegram, BotID: "456", Secret: "second"}, metadata); !errors.Is(err, admin.ErrConnectionFailed) {
		t.Fatalf("replacement stop error = %v", err)
	}
}

func TestWebChannelConnectionHelpersAndCredentialScope(t *testing.T) {
	const credentialTenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV" // #nosec G101 -- deterministic tenant ID for secret-scope isolation, not credential material.
	ready := newWebConnectionAdapter(channels.ChannelTelegram, "helper")
	if !adapterReady(ready) || conversationURL(ready) != "https://t.me/helper" {
		t.Fatalf("adapter helper values: ready=%v url=%q", adapterReady(ready), conversationURL(ready))
	}
	noUsername := newWebConnectionAdapter(channels.ChannelTelegram, " ")
	if conversationURL(noUsername) != "" {
		t.Fatalf("blank username URL = %q", conversationURL(noUsername))
	}
	if adapterReady(noReadyAdapter{}) != true {
		t.Fatal("adapter without Ready should be ready")
	}
	done := make(chan struct{})
	close(done)
	snapshot := (webChannelConnection{binding: &channels.Binding{BindingID: "binding", Channel: channels.ChannelTelegram, ProviderAccountID: "123"}, adapter: ready, url: "https://t.me/helper", done: done}).snapshot()
	if snapshot.Ready {
		t.Fatal("completed adapter was reported ready")
	}

	secrets := modelruntime.NewSecretRegistry()
	if _, err := (webAIBotCredentialResolver{}).Resolve(context.Background(), channels.SecretScope{TenantID: credentialTenantID, SecretRef: "secret"}); err == nil {
		t.Fatal("nil secret registry was accepted")
	}
	if err := secrets.RegisterValue(modelruntime.SecretScope{TenantID: credentialTenantID, SecretRef: "secret"}, "bot-secret"); err != nil {
		t.Fatal(err)
	}
	resolver := webAIBotCredentialResolver{secrets: secrets}
	credentials, err := resolver.Resolve(context.Background(), channels.SecretScope{TenantID: credentialTenantID, SecretRef: "secret"})
	if err != nil || credentials.BotSecret != "bot-secret" {
		t.Fatalf("credentials = %+v, err=%v", credentials, err)
	}
	if _, err := resolver.Resolve(context.Background(), channels.SecretScope{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", SecretRef: "secret"}); !errors.Is(err, modelruntime.ErrSecretUnavailable) {
		t.Fatalf("foreign secret error = %v", err)
	}

	connections, root, registry := newWebConnectionsFixture(t)
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelWeComAIBot, "new-adapter")
	if err != nil {
		t.Fatal(err)
	}
	appID := *root.DefaultAgentAppID
	binding, _, err := connections.bindings.Create(context.Background(), channels.CreateInput{
		TenantID: root.TenantID, BindingKey: "new-adapter", Channel: channels.ChannelWeComAIBot,
		ProviderAccountID: "bot-new-adapter", PublicRouteKeyDigest: routeDigest, AppID: appID,
		SecretRef: "web/new-adapter", Status: channels.StatusActive,
		Protocol: channels.ProtocolConfiguration{WeComAIBot: &channels.WeComAIBotProtocolConfiguration{BotID: "bot-new-adapter"}},
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "test", Reason: "fixture", CorrelationID: "new-adapter"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterValue(modelruntime.SecretScope{TenantID: root.TenantID, SecretRef: binding.SecretRef}, "bot-secret"); err != nil {
		t.Fatal(err)
	}
	target, err := channels.ResolveConfiguredRoutingTarget(context.Background(), connections.candidates, connections.tenants, connections.apps, root.TenantID, binding.BindingID)
	if err != nil {
		t.Fatal(err)
	}
	connections.dispatcher = bootstrapNoopDispatcher{}
	adapter, err := connections.newAdapter(context.Background(), target, "ignored-secret")
	if err != nil {
		t.Fatal(err)
	}
	if adapter.Channel() != channels.ChannelWeComAIBot {
		t.Fatalf("new WeCom adapter channel = %s", adapter.Channel())
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWebConnectionWaitAndScopedReplyStore(t *testing.T) {
	ready := newWebConnectionAdapter(channels.ChannelTelegram, "ready")
	if err := waitWebConnection(context.Background(), ready, make(chan struct{})); err != nil {
		t.Fatalf("ready adapter wait = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitWebConnection(canceled, newWebConnectionAdapter(channels.ChannelTelegram, ""), make(chan struct{})); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled adapter wait = %v", err)
	}
	done := make(chan struct{})
	close(done)
	if err := waitWebConnection(context.Background(), newWebConnectionAdapter(channels.ChannelTelegram, ""), done); !errors.Is(err, admin.ErrConnectionFailed) {
		t.Fatalf("completed adapter wait = %v", err)
	}

	accepted := storage.ReplyOutbox{ReplyTarget: storage.ReplyTarget{BindingID: "binding-a"}}
	rejected := storage.ReplyOutbox{ReplyTarget: storage.ReplyTarget{BindingID: "binding-b"}}
	store := webReplyStoreStub{candidates: []storage.ReplyOutbox{accepted, rejected}}
	scoped := scopedReplyStore{ReplyStore: store, ReplyCorrelationStore: store, accepts: func(_ context.Context, value storage.ReplyOutbox) (bool, error) {
		return value.ReplyTarget.BindingID == "binding-a", nil
	}}
	values, err := scoped.ListReplyCandidates(context.Background(), "tenant")
	if err != nil || len(values) != 1 || values[0].ReplyTarget.BindingID != "binding-a" {
		t.Fatalf("scoped candidates = %+v, err=%v", values, err)
	}
	scoped.accepts = func(context.Context, storage.ReplyOutbox) (bool, error) { return false, errors.New("filter failed") }
	if _, err := scoped.ListReplyCandidates(context.Background(), "tenant"); err == nil {
		t.Fatal("scoped candidate filter failure was ignored")
	}

	runtimeStore := runtimestorageinmemory.New()
	t.Cleanup(func() { _ = runtimeStore.Close() })
	connections := &webChannelConnections{config: Config{ReplyBatchStore: runtimeStore, MessageStore: runtimeStore}}
	binding := &channels.Binding{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAV", BindingID: "binding", SecretRef: "secret"}
	if _, err := connections.newReplyWorker(binding, &wecom_aibot.Manager{}); err != nil {
		t.Fatal(err)
	}
	if _, err := (&webChannelConnections{}).newReplyWorker(binding, &wecom_aibot.Manager{}); !errors.Is(err, admin.ErrConnectionUnavailable) {
		t.Fatalf("missing reply worker dependencies = %v", err)
	}
}

func newWebConnectionsFixture(t *testing.T) (*webChannelConnections, *tenant.Tenant, *modelruntime.SecretRegistry) {
	t.Helper()
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider: "fake", Models: []string{"web-model"}, EndpointPolicy: modelprofile.FieldForbidden, SecretRefPolicy: modelprofile.FieldRequired,
	})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "memory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden,
		Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tenants := tenantmemory.NewRepository()
	apps := appmemory.NewRepository()
	models := modelmemory.NewRepository(modelCatalog)
	backends := backendmemory.NewRepository(backendCatalog)
	root, _ := createBootstrapTenantExecutionState(t, tenants, apps, models, backends, "web-connections", "web-connections", "web-model", "model/web")
	bindings := channelmemory.NewRepository()
	secrets := modelruntime.NewSecretRegistry()
	connections := &webChannelConnections{
		config:             Config{},
		tenants:            tenants,
		apps:               apps,
		bindings:           bindings,
		candidates:         bindings,
		secrets:            secrets,
		dispatcher:         bootstrapNoopDispatcher{},
		telegramDispatcher: bootstrapNoopDispatcher{},
		active:             make(map[string]webChannelConnection),
	}
	return connections, root, secrets
}

func bindingSecretRef(t *testing.T, connections *webChannelConnections, tenantID, bindingID string) string {
	t.Helper()
	binding, err := connections.bindings.Get(context.Background(), tenantID, bindingID)
	if err != nil {
		t.Fatal(err)
	}
	return binding.SecretRef
}

type webConnectionAdapter struct {
	channel   channels.Channel
	username  string
	ready     atomic.Bool
	started   chan struct{}
	startOnce sync.Once
	closed    atomic.Int32
	closeErr  error
}

func newWebConnectionAdapter(channel channels.Channel, username string) *webConnectionAdapter {
	adapter := &webConnectionAdapter{channel: channel, username: username, started: make(chan struct{})}
	if username != "" {
		adapter.ready.Store(true)
	}
	return adapter
}

func (adapter *webConnectionAdapter) Channel() channels.Channel { return adapter.channel }

func (adapter *webConnectionAdapter) Run(ctx context.Context) error {
	adapter.startOnce.Do(func() { close(adapter.started) })
	if !adapter.ready.Load() {
		return errors.New("adapter is not ready")
	}
	<-ctx.Done()
	return ctx.Err()
}

func (adapter *webConnectionAdapter) Close() error {
	adapter.closed.Add(1)
	return adapter.closeErr
}

func (adapter *webConnectionAdapter) Ready() bool      { return adapter.ready.Load() }
func (adapter *webConnectionAdapter) Username() string { return adapter.username }

type noReadyAdapter struct{}

func (noReadyAdapter) Channel() channels.Channel { return channels.ChannelTelegram }
func (noReadyAdapter) Run(context.Context) error { return nil }
func (noReadyAdapter) Close() error              { return nil }

type webReplyStoreStub struct {
	storage.ReplyStore
	storage.ReplyCorrelationStore
	candidates []storage.ReplyOutbox
}

func (store webReplyStoreStub) ListReplyCandidates(context.Context, string) ([]storage.ReplyOutbox, error) {
	return append([]storage.ReplyOutbox(nil), store.candidates...), nil
}

var _ channels.PollingAdapter = (*webConnectionAdapter)(nil)

type webTenantRepository struct {
	tenant.Repository
	value *tenant.Tenant
}

func (repository webTenantRepository) Get(context.Context, string) (*tenant.Tenant, error) {
	if repository.value == nil {
		return nil, nil
	}
	value := repository.value.Clone()
	return &value, nil
}

type webAppRepository struct {
	appmodel.Repository
	value *appmodel.App
}

func (repository webAppRepository) Get(context.Context, string, string) (*appmodel.App, error) {
	if repository.value == nil {
		return nil, nil
	}
	value := repository.value.Clone()
	return &value, nil
}

type webChannelRepository struct {
	channels.Repository
	createErr   error
	activateErr error
}

type webCandidateRepository struct {
	channels.CandidateConsumer
}

func (repository webCandidateRepository) Get(context.Context, string, string) (*channels.Binding, error) {
	return nil, nil
}

func (repository webChannelRepository) Create(ctx context.Context, input channels.CreateInput) (*channels.Binding, channels.ChangeEvent, error) {
	if repository.createErr != nil {
		return nil, channels.ChangeEvent{}, repository.createErr
	}
	return repository.Repository.Create(ctx, input)
}

func (repository webChannelRepository) Activate(ctx context.Context, input channels.TransitionStatusInput) (*channels.Binding, channels.ChangeEvent, error) {
	if repository.activateErr != nil {
		return nil, channels.ChangeEvent{}, repository.activateErr
	}
	return repository.Repository.Activate(ctx, input)
}
