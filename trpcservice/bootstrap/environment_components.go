package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	appmysql "github.com/XnLemon/trpc-agent-service/trpcservice/app/mysql"
	apppostgres "github.com/XnLemon/trpc-agent-service/trpcservice/app/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	auditpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/audit/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmysql "github.com/XnLemon/trpc-agent-service/trpcservice/backend/mysql"
	backendpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/backend/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelmysql "github.com/XnLemon/trpc-agent-service/trpcservice/channels/mysql"
	channelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/channels/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom_aibot"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmysql "github.com/XnLemon/trpc-agent-service/trpcservice/model/mysql"
	modelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/model/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	modelruntime "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/model"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmysql "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/mysql"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type environmentTenantRuntimeOptions struct {
	config           environmentConfig
	delegateSessions session.Service
	runtimeStores    environmentRuntimeStores
	secretRegistry   *modelruntime.SecretRegistry
	modelRegistry    *modelruntime.ModelProviderRegistry
	backendRegistry  *storagefactory.ProviderRegistry
	controlPlane     *environmentTenantRuntimeDependencies
}

type environmentTenantRuntimeDependencies struct {
	tenants        tenant.Repository
	apps           appmodel.Repository
	models         modelprofile.Repository
	backends       backend.Repository
	modelCatalog   *modelprofile.ProviderCatalog
	backendCatalog *backend.ProviderCatalog
	secrets        modelprofile.SecretResolver
}

func environmentModelRepository(config environmentConfig, db *sql.DB, catalog *modelprofile.ProviderCatalog) modelprofile.Repository {
	if config.driver == ControlPlaneDriverMySQL {
		return modelmysql.NewRepository(db, catalog)
	}
	return modelpostgres.NewRepository(db, catalog)
}

func environmentBackendRepository(config environmentConfig, db *sql.DB, catalog *backend.ProviderCatalog) backend.Repository {
	if config.driver == ControlPlaneDriverMySQL {
		return backendmysql.NewRepository(db, catalog)
	}
	return backendpostgres.NewRepository(db, catalog)
}

//nolint:gocyclo // Materialization validates and wires all tenant-scoped providers atomically.
func newEnvironmentTenantMaterializer(o environmentTenantRuntimeOptions) (runtime.TenantRuntimeMaterializer, error) {
	providers, err := environmentRuntimeProviders(o.config, o.runtimeStores)
	if err != nil {
		return nil, err
	}
	if o.secretRegistry == nil || o.modelRegistry == nil || o.backendRegistry == nil {
		return nil, ErrInvalidConfig
	}
	if o.controlPlane != nil {
		cp := o.controlPlane
		if cp.tenants == nil || cp.apps == nil || cp.models == nil || cp.backends == nil || cp.secrets == nil {
			return nil, ErrInvalidConfig
		}
		return func(ctx context.Context, tenantID string) error {
			root, err := cp.tenants.Get(ctx, tenantID)
			if err != nil || root == nil || !root.CanAcceptExecution() || root.DefaultAgentAppID == nil || root.DefaultBackendProfileID == nil {
				return ErrInvalidConfig
			}
			app, err := cp.apps.Get(ctx, tenantID, *root.DefaultAgentAppID)
			if err != nil || app == nil || !app.CanAcceptExecution() || app.CurrentRevision == nil {
				return ErrInvalidConfig
			}
			revision, err := cp.apps.GetRevision(ctx, tenantID, app.AppID, *app.CurrentRevision)
			if err != nil || revision == nil {
				return ErrInvalidConfig
			}
			model, err := cp.models.Get(ctx, tenantID, revision.ModelProfileID)
			if err != nil || model == nil {
				return ErrInvalidConfig
			}
			backendProfile, err := cp.backends.Get(ctx, tenantID, *root.DefaultBackendProfileID)
			if err != nil || backendProfile == nil {
				return ErrInvalidConfig
			}
			secretRef := model.Configuration.SecretRef
			if secretRef != "" {
				scope := modelprofile.SecretScope{TenantID: tenantID, SecretRef: secretRef}
				secret, resolveErr := cp.secrets.Resolve(ctx, scope)
				if resolveErr == nil {
					if err := o.secretRegistry.Register(scope, secret); err != nil {
						return err
					}
				} else {
					key := o.config.modelAPIKeys[tenantID]
					if key == "" {
						key = o.config.modelAPIKey
					}
					if secretRef != o.config.secretRef || key == "" {
						return resolveErr
					}
					if err := o.secretRegistry.RegisterValue(scope, key); err != nil {
						return err
					}
				}
			}
			if err := o.modelRegistry.Register(tenantID, model.Configuration.Provider, environmentModelFactory{}); err != nil {
				return err
			}
			for _, binding := range backendProfile.Bindings {
				provider, ok := environmentRuntimeProvider(providers, strings.ToLower(binding.Provider))
				if !ok {
					return ErrInvalidConfig
				}
				factory := environmentRuntimeCapabilityProvider{capability: binding.Capability, delegate: o.delegateSessions, store: provider.store, telemetry: o.config.telemetry, backend: provider.name}
				if err := o.backendRegistry.Register(tenantID, binding.Capability, binding.Provider, factory); err != nil {
					return err
				}
			}
			return nil
		}, nil
	}
	return func(ctx context.Context, tenantID string) error {
		if ctx == nil || strings.TrimSpace(tenantID) == "" {
			return ErrInvalidConfig
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if o.config.demoMode {
			if err := o.modelRegistry.Register(tenantID, demoModelProvider, environmentModelFactory{}); err != nil {
				return err
			}
		} else {
			key := o.config.modelAPIKey
			if mapped, ok := o.config.modelAPIKeys[tenantID]; ok && mapped != "" {
				key = mapped
			}
			if key == "" {
				return ErrInvalidConfig
			}
			if err := o.secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: tenantID, SecretRef: o.config.secretRef}, key); err != nil {
				return err
			}
			if err := o.modelRegistry.Register(tenantID, o.config.modelProvider, environmentModelFactory{}); err != nil {
				return err
			}
		}
		return registerEnvironmentRuntimeProviders(o.backendRegistry, tenantID, o.delegateSessions, o.config, providers)
	}, nil
}

func environmentRuntimeProvider(providers []environmentRuntimeProviderSpec, name string) (environmentRuntimeProviderSpec, bool) {
	for _, provider := range providers {
		if provider.name == name {
			return provider, true
		}
	}
	return environmentRuntimeProviderSpec{}, false
}

func environmentRepositories(config environmentConfig, db *sql.DB) (tenant.Repository, appmodel.Repository, channels.CandidateConsumer, audit.Writer, error) {
	if config.driver == ControlPlaneDriverMySQL {
		return tenantmysql.NewRepository(db), appmysql.NewAppRepository(db), channelmysql.NewRepository(db), nil, nil
	}
	tenantRepo := tenantpostgres.NewRepository(db)
	appRepo := apppostgres.NewAppRepository(db)
	channelRepo := channelpostgres.NewRepository(db)
	// Channel bindings and tenants can be added dynamically after startup, even
	// when the HTTP API has only one configured identity. Always route audit
	// events by their event tenant so dynamic Telegram/WeCom tenants cannot be
	// rejected by a startup tenant scope.
	return tenantRepo, appRepo, channelRepo, auditpostgres.NewMultiTenant(db), nil
}

type environmentWeComDependencies struct {
	config      environmentConfig
	channels    channels.CandidateConsumer
	tenants     tenant.Repository
	apps        appmodel.Repository
	attachments runtimestorage.AttachmentStore
	auditWriter audit.Writer
}

func environmentWeComComponents(dependencies environmentWeComDependencies) (func(gateway.DispatchService) (http.Handler, error), outbox.Provider, error) {
	config := dependencies.config
	if config.wecom == nil {
		return nil, nil, nil
	}
	credentials := environmentWeComCredentialResolver{tenantID: config.tenantID, config: *config.wecom}
	var mediaDownloader wecom.MediaDownloader
	if dependencies.attachments != nil {
		mediaDownloader = &wecom.HTTPMediaDownloader{}
	}
	factory := func(dispatcher gateway.DispatchService) (http.Handler, error) {
		return wecom.New(wecom.Config{Candidates: dependencies.channels, Tenants: dependencies.tenants, Apps: dependencies.apps, Credentials: credentials, Dispatcher: dispatcher, Attachments: dependencies.attachments, MediaDownloader: mediaDownloader, AuditWriter: dependencies.auditWriter, Observability: config.telemetry})
	}
	return factory, &wecom.BindingProvider{Bindings: dependencies.channels, Credentials: credentials}, nil
}

type environmentWeComAIBotDependencies struct {
	ctx      context.Context
	config   environmentConfig
	channels channels.CandidateConsumer
	tenants  tenant.Repository
	apps     appmodel.Repository
}

func environmentWeComAIBotComponents(dependencies environmentWeComAIBotDependencies) ([]func(gateway.DispatchService) (channels.PollingAdapter, error), map[string]struct{}, error) {
	config := dependencies.config
	if len(config.wecomAIBots) == 0 {
		return nil, nil, nil
	}
	secrets := make(map[string]string, len(config.wecomAIBots))
	targets := make([]channels.RoutingTarget, 0, len(config.wecomAIBots))
	for _, value := range config.wecomAIBots {
		if _, exists := secrets[value.SecretRef]; exists {
			return nil, nil, errors.New("wecom ai bot secret reference is duplicated")
		}
		secrets[value.SecretRef] = value.BotSecret
		target, err := channels.ResolveConfiguredRoutingTarget(dependencies.ctx, dependencies.channels, dependencies.tenants, dependencies.apps, config.tenantID, value.BindingID)
		if err != nil || target.Channel != channels.ChannelWeComAIBot {
			return nil, nil, errors.New("wecom ai bot binding is unavailable")
		}
		targets = append(targets, target)
	}
	credentials := environmentWeComAIBotCredentialResolver{tenantID: config.tenantID, secrets: secrets}
	factories := make([]func(gateway.DispatchService) (channels.PollingAdapter, error), 0, len(targets))
	bindingIDs := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		target := target
		bindingIDs[target.BindingID] = struct{}{}
		factories = append(factories, func(dispatcher gateway.DispatchService) (channels.PollingAdapter, error) {
			return wecom_aibot.NewForBinding(dependencies.ctx, wecom_aibot.BindingConfig{Target: target, Bindings: dependencies.channels, Credentials: credentials, Dispatcher: dispatcher})
		})
	}
	return factories, bindingIDs, nil
}

type environmentOutboxWorkerDependencies struct {
	config        environmentConfig
	replyStore    runtimestorage.ReplyStore
	messageStore  runtimestorage.MessageStore
	deliveryStore wecom_aibot.DeliveryStore
	auditWriter   audit.Writer
	legacy        outbox.Provider
	aiBotBindings map[string]struct{}
	bindings      channels.CandidateConsumer
}

func environmentOutboxWorkerFactory(dependencies environmentOutboxWorkerDependencies) func([]channels.PollingAdapter) (*outbox.Worker, error) {
	config := dependencies.config
	replyStore := dependencies.replyStore
	messageStore := dependencies.messageStore
	deliveryStore := dependencies.deliveryStore
	auditWriter := dependencies.auditWriter
	legacy := dependencies.legacy
	aiBotBindingIDs := dependencies.aiBotBindings
	if legacy == nil && len(aiBotBindingIDs) == 0 {
		return nil
	}
	return func(adapters []channels.PollingAdapter) (*outbox.Worker, error) {
		provider := legacy
		channel, providerName := "wecom", "wecom"
		leaseDuration := 30 * time.Second
		if len(aiBotBindingIDs) > 0 {
			leaseDuration = wecom_aibot.OutboxLeaseDuration
			if deliveryStore == nil {
				return nil, errors.New("runtime store does not support durable reply acknowledgements")
			}
			managers := make([]*wecom_aibot.Manager, 0, len(aiBotBindingIDs))
			for _, adapter := range adapters {
				manager, ok := adapter.(*wecom_aibot.Manager)
				if !ok {
					return nil, errors.New("wecom ai bot adapter has an invalid type")
				}
				managers = append(managers, manager)
			}
			if len(managers) != len(aiBotBindingIDs) {
				return nil, errors.New("wecom ai bot manager count is invalid")
			}
			aiBotProvider, err := wecom_aibot.NewBindingProvider(deliveryStore, managers...)
			if err != nil {
				return nil, err
			}
			if provider == nil {
				provider, channel, providerName = aiBotProvider, "wecom_aibot", "wecom_aibot"
			} else {
				provider = environmentReplyProvider{legacy: provider, aiBot: aiBotProvider, aiBotBindingIDs: aiBotBindingIDs}
			}
		}
		owner, err := environmentWeComOwnerFunc()
		if err != nil {
			return nil, err
		}
		workerStore := replyStore
		if dependencies.bindings != nil {
			workerStore = scopedReplyStore{ReplyStore: replyStore, ReplyCorrelationStore: deliveryStore, accepts: func(ctx context.Context, value runtimestorage.ReplyOutbox) (bool, error) {
				if _, ok := aiBotBindingIDs[value.ReplyTarget.BindingID]; ok {
					return true, nil
				}
				if legacy == nil {
					return false, nil
				}
				binding, err := dependencies.bindings.Get(ctx, value.TenantID, value.ReplyTarget.BindingID)
				if err != nil {
					return false, err
				}
				return binding != nil && binding.Channel == channels.ChannelWeCom, nil
			}}
		}
		return newEnvironmentWeComWorker(outbox.Config{Store: workerStore, MessageStore: messageStore, Provider: provider, Channel: channel, ProviderName: providerName, TenantID: config.tenantID, Owner: owner, LeaseDuration: leaseDuration, AuditWriter: auditWriter, Observability: config.telemetry})
	}
}

func environmentPrimaryDeliveryCapabilities(runtimeStore environmentStorage) (runtimestorage.ReplyStore, runtimestorage.MessageStore, wecom_aibot.DeliveryStore) {
	deliveryStore, _ := runtimeStore.(wecom_aibot.DeliveryStore)
	return runtimeStore, runtimeStore, deliveryStore
}

type environmentReplyProvider struct {
	legacy          outbox.Provider
	aiBot           outbox.Provider
	aiBotBindingIDs map[string]struct{}
}

func (p environmentReplyProvider) Deliver(ctx context.Context, value runtimestorage.ReplyOutbox) (string, error) {
	if _, ok := p.aiBotBindingIDs[value.ReplyTarget.BindingID]; ok {
		return p.aiBot.Deliver(ctx, value)
	}
	return p.legacy.Deliver(ctx, value)
}

func (p environmentReplyProvider) Reconcile(ctx context.Context, value runtimestorage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	if _, ok := p.aiBotBindingIDs[value.ReplyTarget.BindingID]; ok {
		return p.aiBot.Reconcile(ctx, value)
	}
	return p.legacy.Reconcile(ctx, value)
}

func environmentRegistries(config environmentConfig, delegateSessions session.Service, runtimeStore environmentStorage) (*modelruntime.SecretRegistry, *modelruntime.ModelProviderRegistry, *storagefactory.ProviderRegistry, error) {
	providerName := environmentRuntimeProviderName(config.runtimeStorage)
	return environmentRegistriesForStores(config, delegateSessions, environmentRuntimeStores{
		primary:   runtimeStore,
		providers: map[string]environmentStorage{providerName: runtimeStore},
	})
}

type environmentRuntimeProviderSpec struct {
	name         string
	capabilities []backend.Capability
	store        environmentStorage
}

func environmentRegistriesForStores(config environmentConfig, delegateSessions session.Service, runtimeStores environmentRuntimeStores) (*modelruntime.SecretRegistry, *modelruntime.ModelProviderRegistry, *storagefactory.ProviderRegistry, error) {
	secretRegistry := modelruntime.NewSecretRegistry()
	modelRegistry := modelruntime.NewModelProviderRegistry()
	backendRegistry := storagefactory.NewProviderRegistry()
	runtimeProviders, err := environmentRuntimeProviders(config, runtimeStores)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, identity := range config.apiIdentities {
		if config.demoMode {
			if err := modelRegistry.Register(identity.TenantID, demoModelProvider, environmentModelFactory{}); err != nil {
				return nil, nil, nil, err
			}
			if err := registerEnvironmentRuntimeProviders(backendRegistry, identity.TenantID, delegateSessions, config, runtimeProviders); err != nil {
				return nil, nil, nil, err
			}
			continue
		}
		modelAPIKey := config.modelAPIKey
		if len(config.modelAPIKeys) != 0 {
			modelAPIKey = config.modelAPIKeys[identity.TenantID]
		}
		if modelAPIKey == "" {
			return nil, nil, nil, ErrInvalidConfig
		}
		if err := secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: identity.TenantID, SecretRef: config.secretRef}, modelAPIKey); err != nil {
			return nil, nil, nil, err
		}
		if err := modelRegistry.Register(identity.TenantID, config.modelProvider, environmentModelFactory{}); err != nil {
			return nil, nil, nil, err
		}
		if config.runtimeStorage == "redis" && config.redis.Password != "" {
			if err := secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: identity.TenantID, SecretRef: config.redisSecretRef}, config.redis.Password); err != nil {
				return nil, nil, nil, err
			}
		}
		if config.s3AccessKeyID != "" {
			if err := secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: identity.TenantID, SecretRef: config.s3SecretRef}, config.s3AccessKeyID+":"+config.s3SecretKey); err != nil {
				return nil, nil, nil, err
			}
		}
		if err := registerEnvironmentRuntimeProviders(backendRegistry, identity.TenantID, delegateSessions, config, runtimeProviders); err != nil {
			return nil, nil, nil, err
		}
	}
	return secretRegistry, modelRegistry, backendRegistry, nil
}

func environmentRuntimeProviders(config environmentConfig, stores environmentRuntimeStores) ([]environmentRuntimeProviderSpec, error) {
	providerName := environmentRuntimeProviderName(config.runtimeStorage)
	primary := stores.providers[providerName]
	if primary == nil {
		return nil, fmt.Errorf("%w: primary runtime provider is unavailable", ErrInvalidConfig)
	}
	providers := []environmentRuntimeProviderSpec{{name: providerName, capabilities: environmentRuntimeCapabilities(config.runtimeStorage), store: primary}}
	if config.runtimeStorage != "redis" {
		return providers, nil
	}
	fallback := stores.providers["inmemory"]
	if fallback == nil {
		return nil, fmt.Errorf("%w: in-memory runtime provider is unavailable", ErrInvalidConfig)
	}
	return append(providers, environmentRuntimeProviderSpec{name: "inmemory", capabilities: environmentRuntimeCapabilities("inmemory"), store: fallback}), nil
}

func registerEnvironmentRuntimeProviders(registry *storagefactory.ProviderRegistry, tenantID string, delegateSessions session.Service, config environmentConfig, runtimeProviders []environmentRuntimeProviderSpec) error {
	for _, runtimeProvider := range runtimeProviders {
		for _, capability := range runtimeProvider.capabilities {
			provider := environmentRuntimeCapabilityProvider{capability: capability, delegate: delegateSessions, store: runtimeProvider.store, telemetry: config.telemetry, backend: runtimeProvider.name}
			if runtimeProvider.name == "redis" {
				provider.redisEndpoint = config.redisEndpoint
				provider.redisSecretRef = config.redisSecretRef
				provider.redisPasswordRequired = config.redis.Password != ""
			}
			if err := registry.Register(tenantID, capability, runtimeProvider.name, provider); err != nil {
				return err
			}
		}
	}
	if err := registry.Register(tenantID, backend.CapabilityArtifact, "s3", environmentS3CapabilityProvider{tenantID: tenantID, secretRef: config.s3SecretRef}); err != nil {
		return err
	}
	return nil
}

func environmentRuntimeProviderName(runtimeStorage string) string {
	if runtimeStorage == "redis" {
		return "redis"
	}
	return "inmemory"
}

func environmentRuntimeCapabilities(runtimeStorage string) []backend.Capability {
	if runtimeStorage == "redis" {
		return []backend.Capability{backend.CapabilitySession, backend.CapabilityMemory}
	}
	return []backend.Capability{backend.CapabilitySession, backend.CapabilityMemory, backend.CapabilitySummary, backend.CapabilityKnowledge, backend.CapabilityArtifact, backend.CapabilityAudit}
}
