package bootstrap

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/admin"
	"github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom_aibot"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	runtimebudget "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
	modelruntime "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/model"
	storage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

type webChannelConnections struct {
	opMu               sync.Mutex
	mu                 sync.Mutex
	closed             bool
	config             Config
	telegramDispatcher gateway.DispatchService
	adapterFactory     func(context.Context, channels.RoutingTarget, string) (channels.PollingAdapter, error)
	tenants            tenant.Repository
	apps               app.Repository
	bindings           channels.Repository
	candidates         channels.CandidateConsumer
	secrets            *modelruntime.SecretRegistry
	dispatcher         gateway.DispatchService
	active             map[string]webChannelConnection
}

type webChannelConnection struct {
	binding *channels.Binding
	adapter channels.PollingAdapter
	cancel  context.CancelFunc
	url     string
	done    chan struct{}
	worker  *outbox.Worker
}

func newWebChannelConnections(config Config, graph *Runtime) (admin.ChannelConnections, error) {
	candidates, ok := config.Channels.(channels.CandidateConsumer)
	bindings, okRepo := config.Channels.(channels.Repository)
	secrets, okSecrets := config.SecretResolver.(*modelruntime.SecretRegistry)
	if !ok || !okRepo || !okSecrets || config.Tenants == nil || config.Apps == nil || graph == nil || graph.Dispatcher == nil {
		return nil, admin.ErrConnectionUnavailable
	}
	telegramDispatcher, err := gateway.NewDispatcher(gateway.DispatchConfig{
		Resolver: graph.Resolver, Registry: graph.Registry, DrainTimeout: config.DrainTimeout,
		Attachments: config.Attachments, AttachmentStore: config.AttachmentStore,
		AuditWriter: config.AuditWriter, Observability: config.Observability,
		Budget: runtimebudget.NewController(config.BudgetStore),
	})
	if err != nil {
		return nil, err
	}
	return &webChannelConnections{config: config, telegramDispatcher: telegramDispatcher, tenants: config.Tenants, apps: config.Apps, bindings: bindings, candidates: candidates, secrets: secrets, dispatcher: graph.Dispatcher, active: make(map[string]webChannelConnection)}, nil
}

//nolint:gocyclo // Connection setup owns the ordered binding, secret, adapter, and worker lifecycle.
func (c *webChannelConnections) Connect(ctx context.Context, tenantID string, input admin.ConnectInput, metadata channels.ChangeMetadata) (admin.Connection, error) {
	if ctx == nil || c == nil || strings.TrimSpace(input.BotID) == "" || strings.TrimSpace(input.Secret) == "" || (input.Channel != channels.ChannelTelegram && input.Channel != channels.ChannelWeComAIBot) {
		return admin.Connection{}, admin.ErrConnectionFailed
	}
	c.opMu.Lock()
	defer c.opMu.Unlock()
	if c.closed {
		return admin.Connection{}, admin.ErrConnectionUnavailable
	}
	ctx, cancelConnect := context.WithTimeout(ctx, 20*time.Second)
	defer cancelConnect()
	root, err := c.tenants.Get(ctx, tenantID)
	if err != nil || root == nil || root.DefaultAgentAppID == nil || root.Status != tenant.StatusActive {
		return admin.Connection{}, admin.ErrAgentNotReady
	}
	if err := root.Validate(); err != nil {
		return admin.Connection{}, admin.ErrAgentNotReady
	}
	agent, err := c.apps.Get(ctx, tenantID, *root.DefaultAgentAppID)
	if err != nil || agent == nil || !agent.CanAcceptExecution() {
		return admin.Connection{}, admin.ErrAgentNotReady
	}
	account := strings.TrimSpace(input.BotID)
	if input.Channel == channels.ChannelTelegram {
		if id, err := strconv.ParseInt(account, 10, 64); err != nil || id <= 0 || strconv.FormatInt(id, 10) != account {
			return admin.Connection{}, admin.ErrConnectionFailed
		}
	}
	keyScope := tenantID + "\x00" + string(input.Channel)
	c.mu.Lock()
	previous, exists := c.active[keyScope]
	delete(c.active, keyScope)
	c.mu.Unlock()
	if exists {
		if err := c.stop(previous, metadata); err != nil {
			return admin.Connection{}, admin.ErrConnectionFailed
		}
	}
	channelKey := strings.ReplaceAll(string(input.Channel), "_", "-")
	key := "web-" + channelKey + "-" + uuid.NewString()[:8]
	digest, err := channels.DigestPublicRouteKey(input.Channel, key)
	if err != nil {
		return admin.Connection{}, admin.ErrConnectionFailed
	}
	protocol := channels.ProtocolConfiguration{}
	if input.Channel == channels.ChannelTelegram {
		protocol.Telegram = &channels.TelegramProtocolConfiguration{}
	} else {
		protocol.WeComAIBot = &channels.WeComAIBotProtocolConfiguration{BotID: account}
	}
	secretRef := "web/" + uuid.NewString()
	binding, _, err := c.bindings.Create(ctx, channels.CreateInput{TenantID: tenantID, BindingKey: key, Channel: input.Channel, ProviderAccountID: account, PublicRouteKeyDigest: digest, AppID: *root.DefaultAgentAppID, SecretRef: secretRef, Protocol: protocol, Metadata: metadata})
	if err != nil || binding == nil {
		packageLog.Warn("web channel binding creation failed", zap.String("channel", string(input.Channel)), zap.String("tenant_id", tenantID), zap.String("error_type", "binding_create"))
		return admin.Connection{}, admin.ErrConnectionFailed
	}
	committed := false
	var connection webChannelConnection
	connection.binding = binding
	defer func() {
		if !committed {
			_ = c.stop(connection, metadata)
		}
	}()
	if err := c.secrets.RegisterValue(modelruntime.SecretScope{TenantID: tenantID, SecretRef: secretRef}, input.Secret); err != nil {
		packageLog.Warn("web channel secret registration failed", zap.String("channel", string(input.Channel)), zap.String("tenant_id", tenantID), zap.String("error_type", "secret_register"))
		return admin.Connection{}, admin.ErrConnectionFailed
	}
	active, _, err := c.bindings.Activate(ctx, channels.TransitionStatusInput{TenantID: tenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version, Metadata: metadata})
	if err != nil || active == nil {
		packageLog.Warn("web channel binding activation failed", zap.String("channel", string(input.Channel)), zap.String("tenant_id", tenantID), zap.String("error_type", "binding_activate"))
		return admin.Connection{}, admin.ErrConnectionFailed
	}
	connection.binding = active
	target, err := channels.ResolveConfiguredRoutingTarget(ctx, c.candidates, c.tenants, c.apps, tenantID, active.BindingID)
	if err != nil {
		packageLog.Warn("web channel target resolution failed", zap.String("channel", string(input.Channel)), zap.String("tenant_id", tenantID), zap.String("error_type", "target_resolve"))
		return admin.Connection{}, admin.ErrAgentNotReady
	}
	factory := c.adapterFactory
	if factory == nil {
		factory = c.newAdapter
	}
	adapter, err := factory(ctx, target, input.Secret)
	if err != nil || adapter == nil {
		packageLog.Warn("web channel adapter creation failed", zap.String("channel", string(input.Channel)), zap.String("tenant_id", tenantID), zap.String("error_type", "adapter_create"))
		return admin.Connection{}, admin.ErrConnectionFailed
	}
	connection.adapter = adapter
	connection.url = conversationURL(adapter)
	runCtx, cancel := context.WithCancel(context.Background())
	connection.cancel = cancel
	connection.done = make(chan struct{})
	done := connection.done
	go func() { defer close(done); _ = adapter.Run(runCtx) }()
	if err := waitWebConnection(ctx, adapter, done); err != nil {
		packageLog.Warn("web channel readiness failed", zap.String("channel", string(input.Channel)), zap.String("tenant_id", tenantID), zap.String("error_type", "connection_ready"))
		return admin.Connection{}, admin.ErrConnectionFailed
	}
	if manager, ok := adapter.(*wecom_aibot.Manager); ok {
		worker, err := c.newReplyWorker(active, manager)
		if err != nil {
			return admin.Connection{}, admin.ErrConnectionUnavailable
		}
		connection.worker = worker
		if err := worker.Start(runCtx, 250*time.Millisecond); err != nil {
			return admin.Connection{}, admin.ErrConnectionFailed
		}
	}
	c.mu.Lock()
	c.active[keyScope] = connection
	c.mu.Unlock()
	committed = true
	return connection.snapshot(), nil
}

func waitWebConnection(ctx context.Context, adapter channels.PollingAdapter, done <-chan struct{}) error {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return admin.ErrConnectionFailed
		default:
		}
		if adapterReady(adapter) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return admin.ErrConnectionFailed
		case <-ticker.C:
		}
	}
}

func (value webChannelConnection) snapshot() admin.Connection {
	ready := adapterReady(value.adapter)
	select {
	case <-value.done:
		ready = false
	default:
	}
	return admin.Connection{BindingID: value.binding.BindingID, Channel: value.binding.Channel, BotID: value.binding.ProviderAccountID, URL: value.url, Ready: ready}
}

func (c *webChannelConnections) newAdapter(ctx context.Context, target channels.RoutingTarget, secret string) (channels.PollingAdapter, error) {
	if target.Channel == channels.ChannelTelegram {
		return telegram.New(ctx, telegram.Config{BotToken: secret, Target: target, Dispatcher: c.telegramDispatcher})
	}
	return wecom_aibot.NewForBinding(ctx, wecom_aibot.BindingConfig{Target: target, Bindings: c.bindings, Credentials: webAIBotCredentialResolver{secrets: c.secrets}, Dispatcher: c.dispatcher})
}

func (c *webChannelConnections) List(ctx context.Context, tenantID string) ([]admin.Connection, error) {
	if ctx == nil || c == nil {
		return nil, admin.ErrConnectionUnavailable
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make([]admin.Connection, 0, len(c.active))
	for _, value := range c.active {
		if value.binding != nil && value.binding.TenantID == tenantID {
			result = append(result, value.snapshot())
		}
	}
	return result, nil
}

func (c *webChannelConnections) Disconnect(ctx context.Context, tenantID, bindingID string, metadata channels.ChangeMetadata) error {
	if c == nil || ctx == nil {
		return admin.ErrConnectionUnavailable
	}
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.mu.Lock()
	var found *webChannelConnection
	var scope string
	for key, value := range c.active {
		if value.binding != nil && value.binding.TenantID == tenantID && value.binding.BindingID == bindingID {
			copy := value
			found, scope = &copy, key
			break
		}
	}
	if found != nil {
		delete(c.active, scope)
	}
	c.mu.Unlock()
	if found == nil {
		return admin.ErrConnectionFailed
	}
	return c.stop(*found, metadata)
}

// stop joins the adapter before removing its credential and disabling its route.
func (c *webChannelConnections) stop(value webChannelConnection, metadata channels.ChangeMetadata) error {
	if value.cancel != nil {
		value.cancel()
	}
	var err error
	if value.worker != nil {
		err = errors.Join(err, value.worker.Close())
	}
	if value.adapter != nil {
		err = errors.Join(err, value.adapter.Close())
	}
	if value.done != nil {
		<-value.done
	}
	if value.binding != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _, disableErr := c.bindings.Disable(ctx, channels.TransitionStatusInput{TenantID: value.binding.TenantID, BindingID: value.binding.BindingID, ExpectedVersion: value.binding.Version, Metadata: metadata})
		err = errors.Join(err, disableErr, c.secrets.Remove(modelruntime.SecretScope{TenantID: value.binding.TenantID, SecretRef: value.binding.SecretRef}))
	}
	return err
}

func (c *webChannelConnections) Close() error {
	if c == nil {
		return nil
	}
	c.opMu.Lock()
	defer c.opMu.Unlock()
	c.closed = true
	c.mu.Lock()
	values := c.active
	c.active = make(map[string]webChannelConnection)
	c.mu.Unlock()
	var err error
	for _, value := range values {
		err = errors.Join(err, c.stop(value, channels.ChangeMetadata{ActorType: "service", ActorID: "web-connections", Reason: "service shutdown", CorrelationID: "web-shutdown"}))
	}
	return err
}

func (c *webChannelConnections) newReplyWorker(binding *channels.Binding, manager *wecom_aibot.Manager) (*outbox.Worker, error) {
	replies, ok := c.config.ReplyBatchStore.(storage.ReplyStore)
	delivery, hasDelivery := c.config.ReplyBatchStore.(wecom_aibot.DeliveryStore)
	if !ok || !hasDelivery || c.config.MessageStore == nil {
		return nil, admin.ErrConnectionUnavailable
	}
	provider, err := wecom_aibot.NewProvider(manager, delivery)
	if err != nil {
		return nil, err
	}
	store := scopedReplyStore{ReplyStore: replies, ReplyCorrelationStore: delivery, accepts: func(_ context.Context, value storage.ReplyOutbox) (bool, error) {
		return value.ReplyTarget.BindingID == binding.BindingID, nil
	}}
	return outbox.New(outbox.Config{Store: store, MessageStore: c.config.MessageStore, Provider: provider, TenantID: binding.TenantID, Owner: "web-" + uuid.NewString(), LeaseDuration: wecom_aibot.OutboxLeaseDuration, Channel: "wecom_aibot", ProviderName: "wecom_aibot", AuditWriter: c.config.AuditWriter, Observability: c.config.Observability})
}

// scopedReplyStore filters candidates before claiming, so independent channel
// workers cannot claim and dead-letter another connection's replies.
type scopedReplyStore struct {
	storage.ReplyStore
	storage.ReplyCorrelationStore
	accepts func(context.Context, storage.ReplyOutbox) (bool, error)
}

func (s scopedReplyStore) ListReplyCandidates(ctx context.Context, tenantID string) ([]storage.ReplyOutbox, error) {
	candidates, err := s.ReplyStore.ListReplyCandidates(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	result := make([]storage.ReplyOutbox, 0, len(candidates))
	for _, candidate := range candidates {
		accepted, err := s.accepts(ctx, candidate)
		if err != nil {
			return nil, err
		}
		if accepted {
			result = append(result, candidate)
		}
	}
	return result, nil
}

type webAIBotCredentialResolver struct{ secrets *modelruntime.SecretRegistry }

func (r webAIBotCredentialResolver) Resolve(ctx context.Context, scope channels.SecretScope) (wecom_aibot.Credentials, error) {
	if r.secrets == nil {
		return wecom_aibot.Credentials{}, errors.New("secret unavailable")
	}
	value, err := r.secrets.Resolve(ctx, modelruntime.SecretScope{TenantID: scope.TenantID, SecretRef: scope.SecretRef})
	if err != nil {
		return wecom_aibot.Credentials{}, err
	}
	return wecom_aibot.Credentials{BotSecret: value.Value()}, nil
}
func adapterReady(adapter channels.PollingAdapter) bool {
	if value, ok := adapter.(interface{ Ready() bool }); ok {
		return value.Ready()
	}
	return true
}
func conversationURL(adapter channels.PollingAdapter) string {
	if value, ok := adapter.(interface{ Username() string }); ok {
		if username := strings.TrimSpace(value.Username()); username != "" {
			return "https://t.me/" + username
		}
	}
	return ""
}

var _ admin.ChannelConnections = (*webChannelConnections)(nil)
