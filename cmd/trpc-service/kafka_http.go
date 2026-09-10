package main

import (
	"context"
	"fmt"
	"io/fs"
	"net/http"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/node"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

// KafkaHTTPDependencies are the Gateway HTTP dependencies. External IM ingress
// is owned by long-lived channel receivers; HTTP only serves the authenticated
// console plus health/readiness endpoints.
type KafkaHTTPDependencies struct {
	Configurations          tenant.Repository
	Observer                metrics.HTTPObserver
	Producer                messaging.Producer
	ExecutionManifests      *messaging.ExecutionManifestCodec
	StateStore              storage.StateStore
	ExecutionDedup          storage.ExecutionDedupStore
	RetryTracker            storage.RetryTracker
	ToolExecutions          platformtool.ExecutionLister
	WebIdempotency          storage.IdempotencyStore
	KnowledgeIngest         storage.KnowledgeIngestQueue
	KnowledgeMigrationStore storage.KnowledgeMigrationStore
	KnowledgeMigrator       web.KnowledgeMigrationManager
	BackendProfiles         storage.BackendProfileStore
	ConsoleSystem           web.SystemInfo
	ConsoleProbes           map[string]web.DependencyProbe
	Nodes                   node.Lister
	ChannelStatuses         channels.BindingStatusLister
	ConsoleFS               fs.FS
	WebReplySubscriber      web.WebReplySubscriber
	Knowledge               web.KnowledgeAdmin
	KnowledgeSourcePolicy   web.KnowledgeSourcePolicy
	AgentMemory             web.AgentMemoryProvider
	ArtifactServices        web.ArtifactServiceProvider
	AgentSessions           web.AgentSessionProvider
	SessionMigrationStore   storage.SessionMigrationStore
	SessionMigrator         web.SessionMigrationManager
	ApplicationValidator    interface {
		Validate(config.TenantConfig) error
	}
	ToolCatalog  []web.ToolInfo
	ModelSyncer  func(context.Context) ([]web.ModelProviderInfo, error)
	ModelRemover func(context.Context, string, string) error
	// AuthHandler owns /api/v1/auth/* and applies its own protection to private routes.
	AuthHandler http.Handler
	// Sessions backs the login middleware protecting the console API.
	Sessions   identity.SessionStore
	Identities identity.IdentityStore
	// Audits receives login-session lifecycle events from the middleware.
	Audits identity.AuditRecorder
}

// NewKafkaHTTPHandler builds routes that do not execute model work inline.
func NewKafkaHTTPHandler(dependencies KafkaHTTPDependencies) (http.Handler, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	})
	if dependencies.Configurations == nil {
		return nil, fmt.Errorf("tenant configuration repository is required")
	}
	if dependencies.Producer == nil {
		return nil, fmt.Errorf("Kafka producer is required")
	}
	var sessions storage.SessionLister
	var replyStore storage.OutboxDeliveryStore
	var claims storage.ClaimLister
	if dependencies.StateStore != nil {
		replyStore, _ = dependencies.StateStore.(storage.OutboxDeliveryStore)
		sessions, _ = dependencies.StateStore.(storage.SessionLister)
	}
	if dependencies.ExecutionDedup != nil {
		claims, _ = dependencies.ExecutionDedup.(storage.ClaimLister)
	}
	if dependencies.ConsoleFS != nil {
		if dependencies.AuthHandler == nil {
			return nil, fmt.Errorf("login auth handler is required for the console")
		}
		if dependencies.Sessions == nil {
			return nil, fmt.Errorf("login session store is required for the console")
		}
		var attempts storage.AttemptLister
		if tracker, ok := dependencies.RetryTracker.(storage.AttemptLister); ok {
			attempts = tracker
		}
		console, err := web.NewConsoleHandler(web.ConsoleDependencies{
			Configurations:     dependencies.Configurations,
			ExecutionManifests: dependencies.ExecutionManifests,
			Identities:         dependencies.Identities,
			LoginSessions:      dependencies.Sessions,
			Producer:           dependencies.Producer,
			Sessions:           sessions,
			AgentSessions:      dependencies.AgentSessions,
			SessionManager: func() storage.SessionManager {
				manager, _ := dependencies.StateStore.(storage.SessionManager)
				return manager
			}(),
			Claims:                  claims,
			State:                   dependencies.StateStore,
			ToolExecutions:          dependencies.ToolExecutions,
			Replies:                 replyStore,
			ReplySubscriber:         dependencies.WebReplySubscriber,
			Knowledge:               dependencies.Knowledge,
			KnowledgeSourcePolicy:   dependencies.KnowledgeSourcePolicy,
			AgentMemory:             dependencies.AgentMemory,
			ArtifactServices:        dependencies.ArtifactServices,
			SessionMigrationStore:   dependencies.SessionMigrationStore,
			SessionMigrator:         dependencies.SessionMigrator,
			ApplicationValidator:    dependencies.ApplicationValidator,
			WebIdempotency:          dependencies.WebIdempotency,
			KnowledgeIngest:         dependencies.KnowledgeIngest,
			KnowledgeMigrationStore: dependencies.KnowledgeMigrationStore,
			KnowledgeMigrator:       dependencies.KnowledgeMigrator,
			BackendProfiles:         dependencies.BackendProfiles,
			Attempts:                attempts,
			System:                  dependencies.ConsoleSystem,
			Probes:                  dependencies.ConsoleProbes,
			Nodes:                   dependencies.Nodes,
			ChannelStatuses:         dependencies.ChannelStatuses,
			ToolCatalog:             dependencies.ToolCatalog,
			ModelSyncer:             dependencies.ModelSyncer,
			ModelRemover:            dependencies.ModelRemover,
		})
		if err != nil {
			return nil, fmt.Errorf("construct platform console: %w", err)
		}
		mux.Handle("/api/v1/", identity.CSRFMiddleware(identity.SessionMiddleware(dependencies.Sessions, dependencies.Identities, dependencies.Audits, identity.PasswordChangeMiddleware(console))))
		mux.Handle("/api/v1/auth/", dependencies.AuthHandler)
		spa, err := web.NewConsoleSPAHandler(dependencies.ConsoleFS)
		if err != nil {
			return nil, fmt.Errorf("construct console assets: %w", err)
		}
		mux.Handle("/console/", spa)
	}
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		for name, probe := range dependencies.ConsoleProbes {
			probeContext, cancel := context.WithTimeout(request.Context(), 2*time.Second)
			err := probe(probeContext)
			cancel()
			if err != nil {
				http.Error(writer, "dependency not ready: "+name, http.StatusServiceUnavailable)
				return
			}
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	if dependencies.Observer != nil {
		return dependencies.Observer.WrapHTTP(mux), nil
	}
	return mux, nil
}

// NewWorkerHTTPHandler exposes only process health for a Worker role. Worker
// pods never mount the Console, authentication routes, webhook ingress, or any
// other Gateway surface.
func NewWorkerHTTPHandler(observer metrics.HTTPObserver, probes map[string]web.DependencyProbe) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("/readyz", func(writer http.ResponseWriter, request *http.Request) {
		for name, probe := range probes {
			probeContext, cancel := context.WithTimeout(request.Context(), 2*time.Second)
			err := probe(probeContext)
			cancel()
			if err != nil {
				http.Error(writer, "dependency not ready: "+name, http.StatusServiceUnavailable)
				return
			}
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	if observer != nil {
		return observer.WrapHTTP(mux)
	}
	return mux
}
