// Console API: the JSON + SSE endpoints behind the platform development
// console. Mutating routes publish control-plane or Kafka work; browser chat
// uses the same asynchronous Worker/Runtime path as IM messages. All routes are
// protected by the composition root with login-session + CSRF middleware.
package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/node"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
)

const (
	consoleListLimit        = 100
	defaultSessionListLimit = 10
)

// DependencyProbe reports whether one external dependency answers within the
// probe timeout.
type DependencyProbe func(context.Context) error

// SystemInfo is the static, non-secret runtime summary rendered by the system
// page. Secrets never enter this struct.
type SystemInfo struct {
	Version               string              `json:"version"`
	ListenAddress         string              `json:"listen_address"`
	KafkaTopic            string              `json:"kafka_topic"`
	KafkaBrokers          string              `json:"kafka_brokers"`
	RedisAddress          string              `json:"redis_address"`
	ModelProviders        []ModelProviderInfo `json:"model_providers"`
	ChannelCredentialRefs []string            `json:"channel_credential_refs,omitempty"`
	ToolCredentialRefs    []string            `json:"tool_credential_refs,omitempty"`
}

type ModelProviderInfo struct {
	ID             string      `json:"id"`
	Type           string      `json:"type,omitempty"`
	BaseURL        string      `json:"base_url,omitempty"`
	CredentialRefs []string    `json:"credential_refs,omitempty"`
	Configured     bool        `json:"configured"`
	Models         []ModelInfo `json:"models,omitempty"`
	SyncError      string      `json:"sync_error,omitempty"`
}

type ModelInfo struct {
	Name         string                    `json:"name"`
	Capabilities *config.ModelCapabilities `json:"capabilities,omitempty"`
	Pricing      *ModelPricingInfo         `json:"pricing,omitempty"`
	Source       string                    `json:"source,omitempty"`
}

type ModelPricingInfo struct {
	PromptMicros       int64  `json:"prompt_micros_per_million_tokens"`
	CachedPromptMicros *int64 `json:"cached_prompt_micros_per_million_tokens,omitempty"`
	CompletionMicros   int64  `json:"completion_micros_per_million_tokens"`
}

// ToolInfo describes one tenant-selectable tool exposed by the platform.
// Framework-injected tools are intentionally not part of this catalog.
type ToolInfo struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// TenantCatalog is the platform-owned configuration surface exposed to
// tenant administrators. It intentionally excludes service topology, probe
// status, provider endpoints and other platform-operator details.
type TenantCatalog struct {
	ModelProviders        []TenantModelProviderInfo      `json:"model_providers"`
	BackendProfiles       []storage.TenantBackendProfile `json:"backend_profiles"`
	ChannelCredentialRefs []string                       `json:"channel_credential_refs"`
	ToolCredentialRefs    []string                       `json:"tool_credential_refs"`
	Tools                 []ToolInfo                     `json:"tools"`
}

type TenantModelProviderInfo struct {
	ID     string      `json:"id"`
	Type   string      `json:"type,omitempty"`
	Models []ModelInfo `json:"models"`
}

// KnowledgeAdmin is the console-facing seam for knowledge source lifecycle.
// Runtime retrieval stays inside the framework-backed agent path.
type KnowledgeAdmin interface {
	ListKnowledgeDocuments(context.Context, string, string) ([]storage.KnowledgeDocument, error)
	DeleteKnowledgeDocument(context.Context, config.TenantConfig, string) error
}

// KnowledgeSourcePolicy validates remote Source inputs before durable work is
// queued. The worker applies the same policy again before network access.
type KnowledgeSourcePolicy interface {
	ValidateRemoteSource(context.Context, string, string) error
}

type AgentSessionProvider interface {
	Session(context.Context, config.TenantConfig) (agentsession.Service, error)
}

type AgentMemoryProvider interface {
	MemoryReader(context.Context, config.TenantConfig) (agentmemory.Reader, error)
	MemoryService(context.Context, config.TenantConfig) (agentmemory.Service, error)
}

type SessionMigrationManager interface {
	StartSessionMigration(context.Context, string, string, string) (storage.SessionMigrationStatus, error)
	AdvanceSessionMigration(context.Context, string, string, string, uint64) (storage.SessionMigrationStatus, error)
	RollbackSessionMigration(context.Context, string, string, string, uint64) (storage.SessionMigrationStatus, error)
}

type KnowledgeMigrationManager interface {
	StartKnowledgeMigration(context.Context, string, string, string) (storage.KnowledgeMigrationStatus, error)
	AdvanceKnowledgeMigration(context.Context, string, string, string, uint64) (storage.KnowledgeMigrationStatus, error)
	RollbackKnowledgeMigration(context.Context, string, string, string, uint64) (storage.KnowledgeMigrationStatus, error)
}

// ConsoleDependencies are the read-mostly data sources for the console API.
type ConsoleDependencies struct {
	Configurations          tenant.Repository
	Identities              identity.ConsoleIdentityStore
	InboundIdentities       messaging.ChannelIdentityResolver
	Producer                messaging.Producer
	ExecutionManifests      *messaging.ExecutionManifestCodec
	Sessions                storage.SessionLister
	LoginSessions           identity.SessionStore
	AgentSessions           AgentSessionProvider
	SessionMigrationStore   storage.SessionMigrationStore
	SessionMigrator         SessionMigrationManager
	SessionManager          storage.SessionManager
	Claims                  storage.ClaimLister
	State                   storage.StateStore
	Attempts                storage.AttemptLister
	ToolExecutions          platformtool.ExecutionLister
	WebIdempotency          storage.IdempotencyStore
	KnowledgeIngest         storage.KnowledgeIngestQueue
	KnowledgeMigrationStore storage.KnowledgeMigrationStore
	KnowledgeMigrator       KnowledgeMigrationManager
	BackendProfiles         storage.BackendProfileStore
	System                  SystemInfo
	Probes                  map[string]DependencyProbe
	Nodes                   node.Lister
	ChannelStatuses         channels.BindingStatusLister
	Replies                 storage.OutboxDeliveryStore
	ReplySubscriber         WebReplySubscriber
	Knowledge               KnowledgeAdmin
	KnowledgeSourcePolicy   KnowledgeSourcePolicy
	AgentMemory             AgentMemoryProvider
	ArtifactServices        ArtifactServiceProvider
	ApplicationValidator    interface {
		Validate(config.TenantConfig) error
	}
	ToolCatalog  []ToolInfo
	ModelSyncer  func(context.Context) ([]ModelProviderInfo, error)
	ModelRemover func(context.Context, string, string) error
}

func (c *consoleAPI) agentSessionService(ctx context.Context, tenantID, appCode string) (agentsession.Service, error) {
	if c.dependencies.AgentSessions == nil {
		return nil, errors.New("agent Session backend is not configured")
	}
	snapshot, err := c.dependencies.Configurations.GetActive(ctx, tenantID, appCode)
	if err != nil {
		return nil, fmt.Errorf("resolve Session backend configuration: %w", err)
	}
	service, err := c.dependencies.AgentSessions.Session(ctx, snapshot.Config)
	if err != nil {
		return nil, fmt.Errorf("resolve Session backend: %w", err)
	}
	return service, nil
}

func (c *consoleAPI) agentMemoryReader(ctx context.Context, tenantID, appCode string) (agentmemory.Reader, error) {
	if c.dependencies.AgentMemory == nil {
		return nil, errors.New("agent Memory backend is not configured")
	}
	snapshot, err := c.dependencies.Configurations.GetActive(ctx, tenantID, appCode)
	if err != nil {
		return nil, fmt.Errorf("resolve Memory backend configuration: %w", err)
	}
	reader, err := c.dependencies.AgentMemory.MemoryReader(ctx, snapshot.Config)
	if err != nil {
		return nil, fmt.Errorf("resolve Memory backend: %w", err)
	}
	return reader, nil
}

func (c *consoleAPI) agentMemoryService(ctx context.Context, tenantID, appCode string) (agentmemory.Service, error) {
	if c.dependencies.AgentMemory == nil {
		return nil, errors.New("agent Memory backend is not configured")
	}
	snapshot, err := c.dependencies.Configurations.GetActive(ctx, tenantID, appCode)
	if err != nil {
		return nil, fmt.Errorf("resolve Memory backend configuration: %w", err)
	}
	service, err := c.dependencies.AgentMemory.MemoryService(ctx, snapshot.Config)
	if err != nil {
		return nil, fmt.Errorf("resolve Memory backend service: %w", err)
	}
	return service, nil
}

// NewConsoleHandler builds the authenticated console API routes.
func NewConsoleHandler(dependencies ConsoleDependencies) (http.Handler, error) {
	if dependencies.Configurations == nil {
		return nil, fmt.Errorf("console tenant repository is required")
	}
	if dependencies.Producer == nil {
		return nil, fmt.Errorf("console Kafka producer is required")
	}
	if dependencies.ExecutionManifests == nil {
		return nil, fmt.Errorf("console execution manifest codec is required")
	}
	if dependencies.ApplicationValidator == nil {
		return nil, fmt.Errorf("console application policy validator is required")
	}
	if dependencies.InboundIdentities == nil {
		return nil, fmt.Errorf("console inbound identity resolver is required")
	}

	console := &consoleAPI{dependencies: dependencies}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/apps", console.applications)
	mux.HandleFunc("/api/v1/apps/", console.updateApplication)
	mux.HandleFunc("/api/v1/tenants", console.tenants)
	mux.HandleFunc("/api/v1/tenants/", console.updateTenant)
	mux.HandleFunc("/api/v1/users", console.users)
	mux.HandleFunc("/api/v1/users/local", console.localUsers)
	mux.HandleFunc("/api/v1/users/local/reset", console.resetLocalPassword)
	mux.HandleFunc("/api/v1/tenant-members", console.tenantMembers)
	mux.HandleFunc("/api/v1/tenant-model-policy", console.tenantModelPolicy)
	mux.HandleFunc("/api/v1/tenant-tool-policy", console.tenantToolPolicy)
	mux.HandleFunc("/api/v1/account/profile", console.accountProfile)
	mux.HandleFunc("/api/v1/sessions", console.listTenantSessions)
	mux.HandleFunc("/api/v1/sessions/mine", console.listMySessions)
	mux.HandleFunc("/api/v1/sessions/tenant", console.listTenantSessions)
	mux.HandleFunc("/api/v1/sessions/messages", console.getSessionMessages)
	mux.HandleFunc("/api/v1/channels/status", console.channelStatuses)
	mux.HandleFunc("/api/v1/sessions/archive", console.archiveSession)
	mux.HandleFunc("/api/v1/storage/session-migrations", console.sessionMigrations)
	mux.HandleFunc("/api/v1/memory", console.memory)
	mux.HandleFunc("/api/v1/knowledge", console.knowledge)
	mux.HandleFunc("/api/v1/knowledge/documents", console.knowledgeDocuments)
	mux.HandleFunc("/api/v1/storage/knowledge-migrations", console.knowledgeMigrations)
	mux.HandleFunc("/api/v1/artifacts", console.artifacts)
	mux.HandleFunc("/api/v1/claims", console.listClaims)
	mux.HandleFunc("/api/v1/audit", console.listAudit)
	mux.HandleFunc("/api/v1/outbox", console.listOutbox)
	mux.HandleFunc("/api/v1/execution", console.executionTrace)
	mux.HandleFunc("/api/v1/system", console.systemStatus)
	mux.HandleFunc("/api/v1/catalog", console.tenantCatalog)
	mux.HandleFunc("/api/v1/models", console.models)
	mux.HandleFunc("/api/v1/models/sync", console.syncModels)
	mux.HandleFunc("/api/v1/backend-drivers", console.backendDrivers)
	mux.HandleFunc("/api/v1/backend-profiles", console.backendProfiles)
	mux.HandleFunc("/api/v1/backend-profiles/", console.backendProfile)
	mux.HandleFunc("/api/v1/tenant-backend-policy", console.tenantBackendPolicy)
	mux.HandleFunc("/api/v1/chat", console.enqueueChat)
	mux.HandleFunc("/api/v1/chat/stream", console.chatStream)
	return mux, nil
}

func (c *consoleAPI) tenantCatalog(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	user, ok := sessionUser(request)
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	tenantID := resolveTenantParam(request)
	if tenantID == "" {
		badRequest(writer, "tenant query parameter or X-Active-Tenant header is required")
		return
	}
	if !user.IsSystemAdmin && !requireTenantWrite(writer, request, tenantID) {
		return
	}

	providers, err := c.tenantModelProviders(request.Context(), tenantID)
	if err != nil {
		c.writeTenantModelPolicyError(writer, err)
		return
	}
	if c.dependencies.BackendProfiles == nil {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": "backend profile store is not configured"})
		return
	}
	backendProfiles, err := c.dependencies.BackendProfiles.ListTenantBackendProfiles(request.Context(), tenantID)
	if err != nil {
		serverError(writer, "list tenant backend profiles", err)
		return
	}
	tools, err := c.tenantTools(request.Context(), tenantID)
	if err != nil {
		c.writeTenantToolPolicyError(writer, err)
		return
	}

	writeJSON(writer, http.StatusOK, TenantCatalog{
		ModelProviders:        providers,
		BackendProfiles:       backendProfiles,
		ChannelCredentialRefs: append([]string{}, c.dependencies.System.ChannelCredentialRefs...),
		ToolCredentialRefs:    append([]string{}, c.dependencies.System.ToolCredentialRefs...),
		Tools:                 tools,
	})
}

type consoleAPI struct {
	dependencies ConsoleDependencies
	modelMu      sync.Mutex
}

type chatRequest struct {
	TenantID       string `json:"tenant_id"`
	AppCode        string `json:"app_code"`
	ConversationID string `json:"conversation_id"`
	Text           string `json:"text"`
	RequestID      string `json:"request_id"`
}

// buildWebInbound resolves one authenticated browser request into the same
// channel-neutral inbound contract used by external connectors. Browser callers
// never choose a transport or sender identity; both are derived server-side.
func (c *consoleAPI) buildWebInbound(ctx context.Context, body chatRequest, userID string) (tenant.Snapshot, string, channels.InboundMessage, error) {
	if strings.TrimSpace(body.TenantID) == "" || strings.TrimSpace(body.AppCode) == "" {
		return tenant.Snapshot{}, "", channels.InboundMessage{}, errors.New("tenant_id and app_code are required")
	}
	snapshot, err := c.dependencies.Configurations.GetActive(ctx, body.TenantID, body.AppCode)
	if err != nil {
		return tenant.Snapshot{}, "", channels.InboundMessage{}, fmt.Errorf("resolve tenant application: %w", err)
	}
	if snapshot.Config.Status != config.AgentActive {
		return tenant.Snapshot{}, "", channels.InboundMessage{}, fmt.Errorf("agent %q is not active", snapshot.Config.AppName())
	}
	var messageID uuid.UUID
	if strings.TrimSpace(body.RequestID) == "" {
		messageID, err = uuid.NewRandom()
		if err != nil {
			return tenant.Snapshot{}, "", channels.InboundMessage{}, fmt.Errorf("generate message ID: %w", err)
		}
	} else {
		messageID, err = uuid.Parse(strings.TrimSpace(body.RequestID))
		if err != nil {
			return tenant.Snapshot{}, "", channels.InboundMessage{}, errors.New("request_id must be a UUID")
		}
	}
	inbound := channels.InboundMessage{
		MessageID:      messageID.String(),
		Channel:        channels.Web,
		ConversationID: strings.TrimSpace(body.ConversationID),
		SenderID:       strings.TrimSpace(userID),
		WebOwnerID:     strings.TrimSpace(userID),
		TriggerType:    channels.TriggerDirect,
		Text:           strings.TrimSpace(body.Text),
		ReceivedAt:     time.Now().UTC(),
	}
	if err := inbound.Validate(); err != nil {
		return tenant.Snapshot{}, "", channels.InboundMessage{}, fmt.Errorf("validate inbound message: %w", err)
	}
	if c.dependencies.State == nil {
		return tenant.Snapshot{}, "", channels.InboundMessage{}, errors.New("session routing state store is required")
	}
	sessionKey, inbound, err := messaging.ResolveInboundSession(ctx, c.dependencies.State, c.dependencies.InboundIdentities, snapshot, "web-console", inbound)
	if err != nil {
		return tenant.Snapshot{}, "", channels.InboundMessage{}, fmt.Errorf("resolve session: %w", err)
	}
	selection, err := tenant.ResolveRelease(ctx, c.dependencies.Configurations, snapshot, tenant.ReleaseTarget{
		SessionKey: sessionKey, PlatformUserID: inbound.OwnerPlatformUserID,
		Ingress: tenant.ReleaseIngress(string(channels.Web), "web-console"), Scope: string(inbound.ConversationScope),
	})
	if err != nil {
		return tenant.Snapshot{}, "", channels.InboundMessage{}, fmt.Errorf("resolve application release: %w", err)
	}
	snapshot = selection.Snapshot
	return snapshot, sessionKey, inbound, nil
}
