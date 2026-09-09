package platform

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"trpc.group/trpc-go/trpc-agent-go/session/summary"
)

type Role string

const (
	RolePlatformAdmin Role = "platform_admin"
	RoleTenantAdmin   Role = "tenant_admin"
	RoleOperator      Role = "operator"
	RoleViewer        Role = "viewer"
)

type TenantAssignment struct {
	TenantID   string `json:"tenant_id"`
	TenantName string `json:"tenant_name"`
	Role       Role   `json:"role"`
}

type DevelopmentIdentity struct {
	ID          string             `json:"id"`
	Name        string             `json:"name"`
	Assignments []TenantAssignment `json:"assignments"`
}

type identityResponse struct {
	ID             string             `json:"id"`
	Name           string             `json:"name"`
	ActiveTenantID string             `json:"active_tenant_id"`
	ActiveRole     Role               `json:"active_role"`
	Assignments    []TenantAssignment `json:"assignments"`
	AuthMode       string             `json:"auth_mode"`
}

// SnapshotControlPlane applies Control Plane domain rules to an in-memory snapshot.
// Durable constructors attach SQLite or PostgreSQL persistence; the bare
// constructor remains the test-only implementation.
type SnapshotControlPlane struct {
	mu                  sync.RWMutex
	tenants             map[string]Tenant
	apps                map[string]AgentApp
	deployments         map[string]Deployment
	versions            map[string][]DeploymentVersion
	versionCreations    map[versionCreationKey]versionCreation
	channelBindings     map[string]ChannelBinding
	providerRoutes      map[string]BotRoute
	backendSelections   map[string]backendSelection
	governancePolicies  map[string]TenantPolicy
	persistence         controlPlanePersistence
	persistenceErr      error
	persistenceRevision int64
}

// ControlPlaneStore is the authoritative Gateway resource boundary. The
// operations stay package-private so persistence cannot bypass domain rules.
type ControlPlaneStore interface {
	seedTenant(context.Context, TenantAssignment) error
	createTenant(context.Context, Tenant) (bool, error)
	tenant(context.Context, string) (Tenant, bool, error)
	updateTenantAuditPolicy(context.Context, string, AuditPolicy) (Tenant, error)
	DeploymentVersion(context.Context, DeploymentVersionRef) (DeploymentVersion, bool, error)
	listTenants(context.Context) ([]Tenant, error)
	listTenantsFor(context.Context, TenantContext) ([]Tenant, error)
	createApp(context.Context, AgentApp) (bool, error)
	app(context.Context, string, string) (AgentApp, bool, error)
	listApps(context.Context, string) ([]AgentApp, error)
	createDeployment(context.Context, Deployment) (bool, error)
	deployment(context.Context, string, string) (Deployment, bool, error)
	listDeployments(context.Context, string) ([]Deployment, error)
	createVersion(context.Context, Deployment, string, map[string]any) (DeploymentVersion, string, bool, error)
	listVersions(context.Context, string, string) ([]DeploymentVersion, error)
	transition(context.Context, Deployment, DeploymentStatus, string) (Deployment, string, bool, error)
	startRollout(context.Context, Deployment, string, int) (Deployment, string, bool, error)
	rollbackPreview(context.Context, Deployment) (DeploymentRollbackPreview, string, bool, error)
	rollback(context.Context, Deployment) (Deployment, string, bool, error)
	activeDeployment(context.Context, string, string) (Deployment, bool, error)
	routeDeployment(context.Context, string, string, string) (Deployment, bool, error)
	loadChannelBindings(context.Context) (map[string]ChannelBinding, error)
	mutateChannelBindings(context.Context, func(map[string]ChannelBinding) error) (map[string]ChannelBinding, error)
	loadProviderRoutes(context.Context) (map[string]BotRoute, error)
	mutateProviderRoutes(context.Context, func(map[string]BotRoute) error) (map[string]BotRoute, error)
	loadBackendSelections(context.Context) (map[string]backendSelection, error)
	saveBackendSelection(context.Context, string, backendSelection) error
	loadGovernancePolicies(context.Context) (map[string]TenantPolicy, error)
	saveGovernancePolicy(context.Context, TenantPolicy) (TenantPolicy, error)
	Close() error
}

var errControlPlaneUnavailable = errors.New("control_plane_unavailable")

func controlPlanePersistenceError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", errControlPlaneUnavailable, err)
}

type versionCreationKey struct {
	tenantID       string
	deploymentID   string
	idempotencyKey string
}

type versionCreation struct {
	config  string
	version DeploymentVersion
}

func NewInMemoryControlPlane() *SnapshotControlPlane {
	return &SnapshotControlPlane{
		tenants: make(map[string]Tenant), apps: make(map[string]AgentApp),
		deployments: make(map[string]Deployment), versions: make(map[string][]DeploymentVersion),
		versionCreations: make(map[versionCreationKey]versionCreation), channelBindings: make(map[string]ChannelBinding), providerRoutes: make(map[string]BotRoute),
		backendSelections: make(map[string]backendSelection), governancePolicies: make(map[string]TenantPolicy),
	}
}

func (p *SnapshotControlPlane) loadBackendSelections(ctx context.Context) (map[string]backendSelection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return nil, p.persistenceErr
	}
	return copyBackendSelections(p.backendSelections), nil
}

func (p *SnapshotControlPlane) saveBackendSelection(ctx context.Context, tenantID string, selection backendSelection) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return p.persistenceErr
	}
	previous, existed := p.backendSelections[tenantID]
	if previous.MigrationID != "" {
		return ErrTenantMigrating
	}
	p.backendSelections[tenantID] = selection
	if p.persistLockedContext(ctx) {
		return nil
	}
	if existed {
		p.backendSelections[tenantID] = previous
	} else {
		delete(p.backendSelections, tenantID)
	}
	return p.persistenceErr
}

func (p *SnapshotControlPlane) loadGovernancePolicies(ctx context.Context) (map[string]TenantPolicy, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return nil, p.persistenceErr
	}
	policies := make(map[string]TenantPolicy, len(p.governancePolicies))
	for key, policy := range p.governancePolicies {
		policies[key] = clonePolicy(policy)
	}
	return policies, nil
}

func (p *SnapshotControlPlane) saveGovernancePolicy(ctx context.Context, policy TenantPolicy) (TenantPolicy, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return TenantPolicy{}, p.persistenceErr
	}
	key := governanceKey(policy.TenantID, policy.AgentAppID)
	previous, existed := p.governancePolicies[key]
	policy.Revision = previous.Revision + 1
	policy.UpdatedAt = time.Now().UTC()
	p.governancePolicies[key] = clonePolicy(policy)
	if p.persistLockedContext(ctx) {
		return clonePolicy(policy), nil
	}
	if existed {
		p.governancePolicies[key] = previous
	} else {
		delete(p.governancePolicies, key)
	}
	return TenantPolicy{}, p.persistenceErr
}

func (p *SnapshotControlPlane) loadChannelBindings(ctx context.Context) (map[string]ChannelBinding, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return nil, p.persistenceErr
	}
	items := make(map[string]ChannelBinding, len(p.channelBindings))
	for key, binding := range p.channelBindings {
		items[key] = binding
	}
	return items, nil
}

func (p *SnapshotControlPlane) mutateChannelBindings(ctx context.Context, mutate func(map[string]ChannelBinding) error) (map[string]ChannelBinding, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return nil, p.persistenceErr
	}
	previous := p.channelBindings
	bindings := make(map[string]ChannelBinding, len(previous))
	for key, binding := range previous {
		bindings[key] = binding
	}
	if err := mutate(bindings); err != nil {
		return nil, err
	}
	p.channelBindings = bindings
	if !p.persistLockedContext(ctx) {
		p.channelBindings = previous
		return nil, p.persistenceErr
	}
	result := make(map[string]ChannelBinding, len(bindings))
	for key, binding := range bindings {
		result[key] = binding
	}
	return result, nil
}

func (p *SnapshotControlPlane) loadProviderRoutes(ctx context.Context) (map[string]BotRoute, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return nil, p.persistenceErr
	}
	return copyBotRoutes(p.providerRoutes), nil
}

func (p *SnapshotControlPlane) mutateProviderRoutes(ctx context.Context, mutate func(map[string]BotRoute) error) (map[string]BotRoute, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return nil, p.persistenceErr
	}
	previous := p.providerRoutes
	routes := copyBotRoutes(previous)
	if err := mutate(routes); err != nil {
		return nil, err
	}
	p.providerRoutes = routes
	if !p.persistLockedContext(ctx) {
		p.providerRoutes = previous
		return nil, p.persistenceErr
	}
	return copyBotRoutes(routes), nil
}

func (p *SnapshotControlPlane) persistLockedContext(ctx context.Context) bool {
	if p.persistence == nil {
		return true
	}
	operationCtx, cancel := controlPlaneOperationContext(ctx)
	defer cancel()
	revision, err := p.persistence.Save(operationCtx, controlPlaneSnapshotFrom(p), p.persistenceRevision)
	p.persistenceErr = controlPlanePersistenceError(err)
	if err == nil {
		p.persistenceRevision = revision
	}
	return p.persistenceErr == nil
}

func (p *SnapshotControlPlane) refreshLockedContext(ctx context.Context) bool {
	if p.persistence == nil {
		return true
	}
	operationCtx, cancel := controlPlaneOperationContext(ctx)
	defer cancel()
	snapshot, revision, err := p.persistence.Load(operationCtx)
	p.persistenceErr = controlPlanePersistenceError(err)
	if err != nil {
		return false
	}
	if revision > p.persistenceRevision {
		applyControlPlaneSnapshot(p, snapshot)
		p.persistenceRevision = revision
	}
	return true
}

func (p *SnapshotControlPlane) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.persistence == nil {
		return nil
	}
	err := p.persistence.Close()
	p.persistence = nil
	return err
}

func (p *SnapshotControlPlane) seedTenant(ctx context.Context, assignment TenantAssignment) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return p.persistenceErr
	}
	if _, exists := p.tenants[assignment.TenantID]; !exists {
		p.tenants[assignment.TenantID] = Tenant{ID: assignment.TenantID, Name: assignment.TenantName, CreatedAt: time.Now().UTC(), AuditPolicy: DefaultAuditPolicy()}
		if !p.persistLockedContext(ctx) {
			return p.persistenceErr
		}
	}
	return nil
}

func (p *SnapshotControlPlane) createTenant(ctx context.Context, tenant Tenant) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return false, p.persistenceErr
	}
	if _, exists := p.tenants[tenant.ID]; exists {
		return false, nil
	}
	tenant.AuditPolicy = normalizedAuditPolicy(tenant.AuditPolicy)
	p.tenants[tenant.ID] = tenant
	ok := p.persistLockedContext(ctx)
	return ok, p.persistenceErr
}

func (p *SnapshotControlPlane) tenant(ctx context.Context, id string) (Tenant, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return Tenant{}, false, p.persistenceErr
	}
	tenant, ok := p.tenants[id]
	if ok {
		tenant.AuditPolicy = normalizedAuditPolicy(tenant.AuditPolicy)
		p.tenants[id] = tenant
	}
	return tenant, ok, nil
}

func (p *SnapshotControlPlane) updateTenantAuditPolicy(ctx context.Context, id string, policy AuditPolicy) (Tenant, error) {
	normalized, err := policy.Normalize()
	if err != nil {
		return Tenant{}, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return Tenant{}, p.persistenceErr
	}
	tenant, ok := p.tenants[id]
	if !ok {
		return Tenant{}, ErrNotFound
	}
	previous := tenant
	tenant.AuditPolicy = normalized
	p.tenants[id] = tenant
	if !p.persistLockedContext(ctx) {
		p.tenants[id] = previous
		return Tenant{}, p.persistenceErr
	}
	return tenant, nil
}

func (p *SnapshotControlPlane) DeploymentVersion(ctx context.Context, ref DeploymentVersionRef) (DeploymentVersion, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return DeploymentVersion{}, false, p.persistenceErr
	}
	if ref.TenantID == "" || ref.VersionID == "" {
		return DeploymentVersion{}, false, nil
	}
	for _, version := range p.versionsByTenant(ref.TenantID) {
		if version.ID != ref.VersionID {
			continue
		}
		if deployment, ok := p.deployments[resourceKey(version.TenantID, version.DeploymentID)]; ok {
			version.Active = deployment.Status == DeploymentActive && deployment.VersionID == version.ID ||
				deployment.Status == DeploymentActive && deployment.GrayPercentage > 0 && deployment.TargetVersionID == version.ID
		}
		return version, true, nil
	}
	return DeploymentVersion{}, false, nil
}

func (p *SnapshotControlPlane) versionsByTenant(tenantID string) []DeploymentVersion {
	var versions []DeploymentVersion
	for key, items := range p.versions {
		if strings.HasPrefix(key, tenantID+"\x00") {
			versions = append(versions, items...)
		}
	}
	return versions
}

func (p *SnapshotControlPlane) listTenants(ctx context.Context) ([]Tenant, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return nil, p.persistenceErr
	}
	items := make([]Tenant, 0, len(p.tenants))
	for _, tenant := range p.tenants {
		tenant.AuditPolicy = normalizedAuditPolicy(tenant.AuditPolicy)
		items = append(items, tenant)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items, nil
}

func (p *SnapshotControlPlane) listTenantsFor(ctx context.Context, tenant TenantContext) ([]Tenant, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.refreshLockedContext(ctx) {
		return nil, p.persistenceErr
	}
	items := make([]Tenant, 0, len(tenant.Assignments))
	for _, candidate := range p.tenants {
		candidate.AuditPolicy = normalizedAuditPolicy(candidate.AuditPolicy)
		if tenantCanSee(tenant, candidate.ID) {
			items = append(items, candidate)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items, nil
}

type developmentSession struct {
	activeTenantID string
	lastSeen       time.Time
}

type productionSession struct {
	identity identityResponse
	lastSeen time.Time
}

const maxDevelopmentSessions = 256

type AdminHandler struct {
	summarizer               summary.SessionSummarizer
	platform                 ControlPlaneStore
	identity                 DevelopmentIdentity
	identityProvider         IdentityProvider
	productionSessions       map[string]*productionSession
	governance               *GovernanceCenter
	mu                       sync.Mutex
	sessions                 map[string]*developmentSession
	runtime                  *Runtime
	backends                 *backendRegistry
	migrations               map[string]migrationResult
	backendSelectionPath     string
	backendCatalog           map[string]backendSelection
	migrationSourceAddress   string
	migrationDestinationPath string
	migrationCheckpointPath  string
	migrationCtx             context.Context
	migrationCancel          context.CancelFunc
	migrationWG              sync.WaitGroup
	migrationRunning         bool
	closing                  bool
	failureCtx               context.Context
	failureCancel            context.CancelFunc
	channels                 *ChannelCoordinator
	providers                *ProviderRuntime
	runCoordinator           RunCoordinator
	chatMu                   sync.Mutex
	activeRuns               map[string]activeChatRun
	chatCtx                  context.Context
	chatCancel               context.CancelFunc
	chatWG                   sync.WaitGroup
	capacityCtx              context.Context
	capacityCancel           context.CancelFunc
	capacityWG               sync.WaitGroup
	capacityMu               sync.Mutex
	capacityRuns             map[string]*capacityRun
	life                     RuntimeLifecycle
	drainState               DrainState
	drainStartedAt           time.Time
	drainCompletedAt         time.Time
	drainError               string
	faultInjectionEnabled    bool
	internalGovernanceToken  string
}

// ConfigureIdentityProvider enables production identity mode. Development
// sessions are not consulted while a production provider is configured.
func (h *AdminHandler) ConfigureIdentityProvider(provider IdentityProvider) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.faultInjectionEnabled = false
	h.identityProvider = provider
	h.sessions = make(map[string]*developmentSession)
	h.productionSessions = make(map[string]*productionSession)
	if source, ok := provider.(interface{ Assignments() []TenantAssignment }); ok {
		for _, assignment := range source.Assignments() {
			_ = h.platform.seedTenant(context.Background(), assignment)
		}
	}
}

func (h *AdminHandler) ConfigureGovernance(center *GovernanceCenter) {
	if center == nil {
		return
	}
	h.governance = center
	center.configurePolicyStore(h.platform)
	if tenants, err := h.platform.listTenants(context.Background()); err == nil {
		for _, tenant := range tenants {
			_ = center.SetAuditPolicy(tenant.ID, tenant.AuditPolicy)
		}
	}
	if governed, ok := h.runtime.worker.runner.(interface{ SetGovernance(*GovernanceCenter) }); ok {
		governed.SetGovernance(center)
	}
}

func NewAdminHandler(platform ControlPlaneStore, identity DevelopmentIdentity) *AdminHandler {
	if platform == nil {
		platform = NewInMemoryControlPlane()
	}
	for _, assignment := range identity.Assignments {
		_ = platform.seedTenant(context.Background(), assignment)
	}
	migrationCtx, migrationCancel := context.WithCancel(context.Background())
	failureCtx, failureCancel := context.WithCancel(context.Background())
	chatCtx, chatCancel := context.WithCancel(context.Background())
	capacityCtx, capacityCancel := context.WithCancel(context.Background())
	channels := NewChannelCoordinator(NewMockChannel())
	channels.configurePersistence(platform.loadChannelBindings, platform.mutateChannelBindings)
	h := &AdminHandler{
		platform: platform, identity: identity, sessions: make(map[string]*developmentSession),
		governance: NewGovernanceCenter(), productionSessions: make(map[string]*productionSession),
		runtime: NewRuntime(platform, EchoRunner{}, nil), backends: newBackendRegistry(NewInMemoryStore(), nil),
		migrations: make(map[string]migrationResult), backendCatalog: map[string]backendSelection{"inmemory": {Backend: "inmemory"}},
		migrationCtx: migrationCtx, migrationCancel: migrationCancel, failureCtx: failureCtx, failureCancel: failureCancel,
		channels: channels, activeRuns: make(map[string]activeChatRun), runCoordinator: NewInMemoryRunCoordinator(),
		chatCtx: chatCtx, chatCancel: chatCancel, capacityCtx: capacityCtx, capacityCancel: capacityCancel,
		capacityRuns: make(map[string]*capacityRun), drainState: DrainIdle, faultInjectionEnabled: true,
	}
	for _, assignment := range identity.Assignments {
		if tenant, ok, err := platform.tenant(context.Background(), assignment.TenantID); err == nil && ok {
			_ = h.governance.SetAuditPolicy(tenant.ID, tenant.AuditPolicy)
		}
	}
	return h
}

func (h *AdminHandler) ConfigureFaultInjection(enabled bool) {
	h.mu.Lock()
	h.faultInjectionEnabled = enabled
	h.mu.Unlock()
}

// ConfigureDataStore replaces the Stage 2 data backend. It is safe to call
// during process composition before serving requests.
func (h *AdminHandler) ConfigureDataStore(store DataStore) {
	if store == nil {
		return
	}
	h.backends.replaceDefault(store)
}

type backendSelection struct {
	ProfileID      string               `json:"profile_id,omitempty"`
	MigrationID    string               `json:"migration_id,omitempty"`
	ExternalMemory ExternalMemoryConfig `json:"external_memory,omitempty"`
	Backend        string               `json:"backend"`
	Address        string               `json:"address"`
	Memory         BackendEndpoint      `json:"memory,omitempty"`
	Artifact       BackendEndpoint      `json:"artifact,omitempty"`
	Knowledge      BackendEndpoint      `json:"knowledge,omitempty"`
	Object         ObjectBackendConfig  `json:"object,omitempty"`
	Vector         VectorBackendConfig  `json:"vector,omitempty"`
}

func (h *AdminHandler) ConfigureBackendSelections(path string) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.backendSelectionPath = path
	selections, err := h.platform.loadBackendSelections(context.Background())
	if err != nil {
		return err
	}
	if len(selections) > 0 || path == "" {
		h.backends.setSelections(selections)
		return nil
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var importedSelections map[string]backendSelection
	if err := json.Unmarshal(data, &importedSelections); err != nil {
		return err
	}
	h.backends.setSelections(importedSelections)
	for tenantID, selection := range importedSelections {
		if err := h.platform.saveBackendSelection(context.Background(), tenantID, selection); err != nil {
			return err
		}
	}
	return nil
}
func (h *AdminHandler) ConfigureMigration(sourceAddress, destinationPath, checkpointPath string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.migrationSourceAddress = sourceAddress
	h.migrationDestinationPath = destinationPath
	h.migrationCheckpointPath = checkpointPath
}
func (h *AdminHandler) ConfigureBackendCatalog(redisAddress, sqlitePath string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if redisAddress != "" {
		h.backendCatalog["redis"] = backendSelection{Backend: "redis", Address: redisAddress}
	}
	if sqlitePath != "" {
		h.backendCatalog["sqlite"] = backendSelection{Backend: "sqlite", Address: sqlitePath}
	}
}

func (h *AdminHandler) ConfigurePostgresBackend(dsn string) {
	if dsn == "" {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.backendCatalog["postgres"] = backendSelection{Backend: "postgres", Address: dsn}
}

func (h *AdminHandler) selectBackend(ctx context.Context, tenantID string, selection backendSelection, store DataStore) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.platform.saveBackendSelection(ctx, tenantID, selection); err != nil {
		return err
	}
	next := h.backends.selectionSnapshot()
	next[tenantID] = selection
	if h.backendSelectionPath != "" {
		data, err := json.Marshal(next)
		if err != nil {
			return err
		}
		if err := os.WriteFile(h.backendSelectionPath, data, 0600); err != nil {
			return err
		}
	}
	h.backends.replace(tenantID, selection, store)
	return nil
}

func (h *AdminHandler) Close() error {
	h.mu.Lock()
	if h.closing {
		h.mu.Unlock()
		return nil
	}
	h.closing = true
	h.mu.Unlock()
	h.chatCancel()
	h.capacityCancel()
	if h.providers != nil {
		h.providers.Close()
	}
	h.backends.beginClose()
	h.migrationCancel()
	h.migrationWG.Wait()
	h.chatWG.Wait()
	h.capacityWG.Wait()
	h.failureCancel()
	if h.runCoordinator != nil {
		_ = h.runCoordinator.Close()
	}
	runtimeErr := h.runtime.Close()
	storeErr := h.backends.close()
	if runtimeErr != nil {
		return runtimeErr
	}
	if storeErr != nil {
		return storeErr
	}
	return h.platform.Close()
}

func (h *AdminHandler) isClosing() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closing
}

func (h *AdminHandler) ConfigureProviderRuntime(providers *ProviderRuntime) {
	if providers != nil {
		_ = providers.routes.configurePersistence(h.platform.loadProviderRoutes, h.platform.mutateProviderRoutes)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.providers = providers
	if providers != nil {
		h.channels.RegisterAdapter(ChannelTelegram, TelegramChannel{Sender: providers.sendTelegram})
		h.channels.RegisterAdapter(ChannelEnterpriseWeChat, EnterpriseWeChatChannel{Sender: providers.sendWeCom})
	}
}

func (h *AdminHandler) acquireStore(ctx context.Context, tenantID string) (DataStore, func(), error) {
	selections, err := h.platform.loadBackendSelections(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", errControlPlaneUnavailable, err)
	}
	if selections[tenantID].MigrationID != "" {
		return nil, nil, ErrTenantMigrating
	}
	h.backends.syncSelections(selections)
	return h.backends.acquire(tenantID)
}

// ConfigureRuntime replaces the default fake runtime and attaches lifecycle
// admission. It is intended for process composition and deterministic tests.
func (h *AdminHandler) ConfigureRuntime(runner RunnerAdapter, life RuntimeLifecycle) {
	_ = h.runtime.Close()
	if governed, ok := runner.(interface{ SetGovernance(*GovernanceCenter) }); ok {
		governed.SetGovernance(h.governance)
	}
	h.runtime = NewRuntime(h.platform, runner, life)
	h.mu.Lock()
	h.life = life
	h.mu.Unlock()
}

func (h *AdminHandler) ConfigureSessionLeases(manager SessionLeaseManager) {
	h.runtime.SetSessionLeaseManager(manager)
}

// ConfigureRunCoordinator installs the shared execution coordinator used by
// chat submission and cancellation. A nil value restores the single-process
// in-memory adapter.
func (h *AdminHandler) ConfigureRunCoordinator(coordinator RunCoordinator) {
	h.mu.Lock()
	previous := h.runCoordinator
	if coordinator == nil {
		coordinator = NewInMemoryRunCoordinator()
	}
	h.runCoordinator = coordinator
	h.mu.Unlock()
	if previous != nil && previous != coordinator {
		_ = previous.Close()
	}
}

func (h *AdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/internal/metrics" {
		h.handleInternalMetrics(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/internal/governance/tool/") {
		h.handleInternalGovernance(w, r)
		return
	}
	auditWriter := &auditResponseWriter{ResponseWriter: w, buffered: r.Method != http.MethodGet || r.URL.Path == "/api/v1/auth/me"}
	w = auditWriter
	started := time.Now()
	defer func() {
		auditCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := h.auditHTTPRequest(auditCtx, r, auditWriter, started); err != nil && auditWriter.buffered && (auditWriter.status == 0 || auditWriter.status < http.StatusBadRequest) {
			auditWriter.auditUnavailable()
		}
		auditWriter.commit()
	}()
	if isDataPath(r.URL.Path) {
		trusted := h.trustedRequest(w, r)
		if trusted == nil {
			return
		}
		h.handleDataResource(w, trusted, strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/admin/"), "/"), "/"))
		return
	}
	switch r.URL.Path {
	case "/api/v1/auth/login":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
			return
		}
		h.handleProductionLogin(w, r)
	case "/api/v1/auth/logout":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
			return
		}
		h.handleProductionLogout(w, r)
	case "/api/v1/auth/me":
		if r.Method != http.MethodGet {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET")
			return
		}
		h.handleIdentity(w, r)
	case "/api/v1/auth/switch-tenant":
		if r.Method != http.MethodPost {
			writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
			return
		}
		h.handleSwitchTenant(w, r)
	case "/api/v1/admin/tenants":
		trusted := h.trustedRequest(w, r)
		if trusted != nil {
			h.handleTenants(w, trusted)
		}
	case "/api/v1/chat/bindings":
		trusted := h.trustedRequest(w, r)
		if trusted != nil {
			h.handleChannelBindings(w, trusted)
		}
	case "/api/v1/chat/channels/mock/callback":
		trusted := h.trustedRequest(w, r)
		if trusted != nil {
			h.handleMockChannelCallback(w, trusted)
		}
	case "/api/v1/chat/mock/faults":
		trusted := h.trustedRequest(w, r)
		if trusted != nil {
			h.handleMockFaults(w, trusted)
		}
	case "/api/v1/admin/providers/status":
		trusted := h.trustedRequest(w, r)
		if trusted != nil {
			h.handleProviderStatus(w, trusted)
		}
	case "/api/v1/admin/providers/deliveries":
		trusted := h.trustedRequest(w, r)
		if trusted != nil {
			h.handleProviderDeliveries(w, trusted)
		}
	case "/api/v1/admin/providers/routes":
		trusted := h.trustedRequest(w, r)
		if trusted != nil {
			h.handleProviderRoutes(w, trusted)
		}
	case "/api/v1/admin/providers/replay":
		trusted := h.trustedRequest(w, r)
		if trusted != nil {
			h.handleProviderReplay(w, trusted)
		}
	case "/api/v1/chat/sessions":
		trusted := h.trustedRequest(w, r)
		if trusted != nil {
			h.handleChatSessionResource(w, trusted, []string{})
		}
	default:
		if strings.HasPrefix(r.URL.Path, "/api/v1/admin/governance/") {
			trusted := h.trustedRequest(w, r)
			if trusted != nil {
				h.handleGovernance(w, trusted)
			}
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/v1/chat/bindings/") {
			trusted := h.trustedRequest(w, r)
			if trusted != nil {
				h.handleChannelBindingResource(w, trusted, strings.TrimPrefix(r.URL.Path, "/api/v1/chat/bindings/"))
			}
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/v1/chat/sessions/") {
			trusted := h.trustedRequest(w, r)
			if trusted != nil {
				h.handleChatSessionResource(w, trusted, strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1/chat/sessions/"), "/"), "/"))
			}
			return
		}
		if h.handleAdminResource(w, r) {
			return
		}
		if strings.HasPrefix(r.URL.Path, "/api/v1/admin/tenants/") {
			trusted := h.trustedRequest(w, r)
			if trusted != nil {
				h.handleTenant(w, trusted, strings.TrimPrefix(r.URL.Path, "/api/v1/admin/tenants/"))
			}
			return
		}
		http.NotFound(w, r)
	}
}

func (h *AdminHandler) trustedRequest(w http.ResponseWriter, r *http.Request) *http.Request {
	h.mu.Lock()
	provider := h.identityProvider
	h.mu.Unlock()
	if provider != nil {
		identity, err := h.productionIdentity(r, "")
		if err != nil {
			status, code, message := identityPublicError(err)
			writeError(w, status, code, message)
			return nil
		}
		tenant := TenantContext{TenantID: identity.ActiveTenantID, UserID: identity.ID, Role: identity.ActiveRole, Assignments: append([]TenantAssignment(nil), identity.Assignments...)}
		ctx := WithTenantContext(r.Context(), tenant)
		markAuditIdentity(w, tenant)
		return r.WithContext(ctx)
	}
	_, session, ok := h.session(w, r)
	if !ok {
		return r
	}
	assignment, ok := h.assignment(session.activeTenantID)
	if !ok {
		return r
	}
	identity := h.identitySnapshot()
	tenant := TenantContext{TenantID: assignment.TenantID, UserID: identity.ID, Role: assignment.Role, Assignments: append([]TenantAssignment(nil), identity.Assignments...)}
	markAuditIdentity(w, tenant)
	ctx := WithTenantContext(r.Context(), tenant)
	return r.WithContext(ctx)
}

func (h *AdminHandler) handleTenants(w http.ResponseWriter, r *http.Request) {
	trusted, ok := TenantContextFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
		return
	}
	switch r.Method {
	case http.MethodGet:
		items, err := h.platform.listTenantsFor(r.Context(), trusted)
		if writeControlPlaneError(w, err) {
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		if trusted.Role != RolePlatformAdmin {
			writeError(w, http.StatusForbidden, "forbidden", "platform administrator role is required")
			return
		}
		var request struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil || !validResourceID(request.ID) || !validDisplayName(request.Name) {
			writeError(w, http.StatusBadRequest, "invalid_tenant", "id and name must be valid")
			return
		}
		tenant := Tenant{ID: request.ID, Name: strings.TrimSpace(request.Name), CreatedAt: time.Now().UTC(), AuditPolicy: DefaultAuditPolicy()}
		created, err := h.platform.createTenant(r.Context(), tenant)
		if writeControlPlaneError(w, err) {
			return
		}
		if !created {
			writeError(w, http.StatusConflict, "tenant_exists", "tenant identifier already exists")
			return
		}
		h.mu.Lock()
		h.identity.Assignments = append(h.identity.Assignments, TenantAssignment{TenantID: tenant.ID, TenantName: tenant.Name, Role: RolePlatformAdmin})
		h.mu.Unlock()
		writeJSON(w, http.StatusCreated, tenant)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET or POST")
	}
}

func (h *AdminHandler) handleTenant(w http.ResponseWriter, r *http.Request, id string) {
	trusted, ok := TenantContextFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
		return
	}
	if !tenantCanSee(trusted, id) {
		writeError(w, http.StatusNotFound, "tenant_not_found", "tenant was not found")
		return
	}
	tenant, exists, err := h.platform.tenant(r.Context(), id)
	if writeControlPlaneError(w, err) {
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "tenant_not_found", "tenant was not found")
		return
	}
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, tenant)
		return
	}
	if r.Method != http.MethodPatch && r.Method != http.MethodPut {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET, PATCH, or PUT")
		return
	}
	assignment, _ := trusted.AssignmentFor(id)
	if trusted.Role != RolePlatformAdmin && assignment.Role != RolePlatformAdmin && (trusted.Role != RoleTenantAdmin || trusted.TenantID != id) {
		writeError(w, http.StatusForbidden, "forbidden", "tenant administrator role is required")
		return
	}
	var request struct {
		AuditPolicy AuditPolicy `json:"audit_policy"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_audit_policy", "audit_policy is required")
		return
	}
	updated, err := h.platform.updateTenantAuditPolicy(r.Context(), id, request.AuditPolicy)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, "tenant_not_found", "tenant was not found")
			return
		}
		if errors.Is(err, errControlPlaneUnavailable) {
			writeControlPlaneError(w, err)
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_audit_policy", err.Error())
		return
	}
	h.governance.SetAuditPolicy(id, updated.AuditPolicy)
	writeJSON(w, http.StatusOK, updated)
}

func writeControlPlaneError(w http.ResponseWriter, err error) bool {
	if !errors.Is(err, errControlPlaneUnavailable) {
		return false
	}
	writeError(w, http.StatusServiceUnavailable, "control_plane_unavailable", "Control Plane Store is unavailable")
	return true
}

var resourceIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{2,62}$`)

func validResourceID(id string) bool { return resourceIDPattern.MatchString(id) }

func validDisplayName(name string) bool {
	length := len([]rune(strings.TrimSpace(name)))
	return length >= 2 && length <= 80
}

func (h *AdminHandler) handleIdentity(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	provider := h.identityProvider
	h.mu.Unlock()
	if provider != nil {
		identity, err := h.productionIdentity(r, "")
		if err != nil {
			status, code, message := identityPublicError(err)
			writeError(w, status, code, message)
			return
		}
		markAuditIdentity(w, TenantContext{TenantID: identity.ActiveTenantID, UserID: identity.ID, Role: identity.ActiveRole, Assignments: append([]TenantAssignment(nil), identity.Assignments...)})
		writeJSON(w, http.StatusOK, identity)
		return
	}
	_, session, ok := h.session(w, r)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "development_identity_unavailable", "development identity has no tenant assignments")
		return
	}
	assignment, _ := h.assignment(session.activeTenantID)
	identity := h.identitySnapshot()
	markAuditIdentity(w, TenantContext{TenantID: assignment.TenantID, UserID: identity.ID, Role: assignment.Role, Assignments: append([]TenantAssignment(nil), identity.Assignments...)})
	writeJSON(w, http.StatusOK, identityResponse{
		ID: identity.ID, Name: identity.Name, ActiveTenantID: assignment.TenantID,
		ActiveRole: assignment.Role, Assignments: identity.Assignments, AuthMode: "development",
	})
}

func (h *AdminHandler) handleSwitchTenant(w http.ResponseWriter, r *http.Request) {
	var request struct {
		TenantID string `json:"tenant_id"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || request.TenantID == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "tenant_id is required")
		return
	}
	h.mu.Lock()
	provider := h.identityProvider
	h.mu.Unlock()
	if provider != nil {
		identity, err := h.productionIdentity(r, request.TenantID)
		if err != nil {
			status, code, message := identityPublicError(err)
			writeError(w, status, code, message)
			return
		}
		markAuditIdentity(w, TenantContext{TenantID: identity.ActiveTenantID, UserID: identity.ID, Role: identity.ActiveRole, Assignments: append([]TenantAssignment(nil), identity.Assignments...)})
		writeJSON(w, http.StatusOK, identity)
		return
	}
	assignment, approved := h.assignment(request.TenantID)
	if !approved {
		writeError(w, http.StatusForbidden, "tenant_not_assigned", "tenant is not assigned to the development identity")
		return
	}
	token, _, ok := h.session(w, r)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, "development_identity_unavailable", "development identity has no tenant assignments")
		return
	}
	h.mu.Lock()
	if session := h.sessions[token]; session != nil {
		session.activeTenantID = request.TenantID
		session.lastSeen = time.Now().UTC()
	}
	h.mu.Unlock()
	identity := h.identitySnapshot()
	markAuditIdentity(w, TenantContext{TenantID: assignment.TenantID, UserID: identity.ID, Role: assignment.Role, Assignments: append([]TenantAssignment(nil), identity.Assignments...)})
	writeJSON(w, http.StatusOK, identityResponse{
		ID: identity.ID, Name: identity.Name, ActiveTenantID: assignment.TenantID,
		ActiveRole: assignment.Role, Assignments: identity.Assignments, AuthMode: "development",
	})
}

func (h *AdminHandler) assignment(tenantID string) (TenantAssignment, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.assignmentLocked(tenantID)
}

func (h *AdminHandler) assignmentLocked(tenantID string) (TenantAssignment, bool) {
	for _, assignment := range h.identity.Assignments {
		if assignment.TenantID == tenantID {
			return assignment, true
		}
	}
	return TenantAssignment{}, false
}

func (h *AdminHandler) identitySnapshot() DevelopmentIdentity {
	h.mu.Lock()
	defer h.mu.Unlock()
	identity := h.identity
	identity.Assignments = append([]TenantAssignment(nil), h.identity.Assignments...)
	return identity
}

func (h *AdminHandler) session(w http.ResponseWriter, r *http.Request) (string, developmentSession, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cookie, err := r.Cookie("trpc_dev_session"); err == nil {
		if session := h.sessions[cookie.Value]; session != nil {
			session.lastSeen = time.Now().UTC()
			return cookie.Value, *session, true
		}
	}
	if len(h.identity.Assignments) == 0 {
		return "", developmentSession{}, false
	}
	tokenBytes := make([]byte, 32)
	if _, err := rand.Read(tokenBytes); err != nil {
		return "", developmentSession{}, false
	}
	if len(h.sessions) >= maxDevelopmentSessions {
		var oldestToken string
		var oldest time.Time
		for candidate, session := range h.sessions {
			if oldestToken == "" || session.lastSeen.Before(oldest) {
				oldestToken, oldest = candidate, session.lastSeen
			}
		}
		delete(h.sessions, oldestToken)
	}
	token := hex.EncodeToString(tokenBytes)
	session := &developmentSession{activeTenantID: h.identity.Assignments[0].TenantID, lastSeen: time.Now().UTC()}
	h.sessions[token] = session
	http.SetCookie(w, &http.Cookie{
		Name: "trpc_dev_session", Value: token, Path: "/", HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})
	return token, *session, true
}
