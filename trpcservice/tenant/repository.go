package tenant

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Repository is the control-plane source used by publishers and workers.
type Repository interface {
	CreateTenant(context.Context, Tenant) (Tenant, error)
	GetTenant(context.Context, string) (Tenant, error)
	CreateAgentApp(context.Context, TenantContext, AgentApp) (AgentApp, error)
	GetAgentApp(context.Context, TenantContext, string) (AgentApp, error)
	CreateRevision(context.Context, TenantContext, AgentRevision) (AgentRevision, error)
	GetRevision(context.Context, TenantContext, string, string) (AgentRevision, error)
	PublishRevision(context.Context, TenantContext, string, string) (AgentApp, AgentRevision, error)
	ResolveRevision(context.Context, TenantContext, string, string) (AgentRevision, error)
}

type appKey struct {
	tenantID string
	appID    string
}

type revisionKey struct {
	tenantID   string
	appID      string
	revisionID string
}

type revisionNumberKey struct {
	tenantID   string
	appID      string
	revisionNo uint64
}

var _ Repository = (*MemoryRepository)(nil)

// MemoryRepository is a concurrency-safe reference implementation for local
// development. Returned values are deep copies so revisions stay immutable.
type MemoryRepository struct {
	mu              sync.RWMutex
	tenants         map[string]Tenant
	tenantSlugs     map[string]string
	apps            map[appKey]AgentApp
	revisions       map[revisionKey]AgentRevision
	revisionNumbers map[revisionNumberKey]string
	now             func() time.Time
}

func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		tenants:         make(map[string]Tenant),
		tenantSlugs:     make(map[string]string),
		apps:            make(map[appKey]AgentApp),
		revisions:       make(map[revisionKey]AgentRevision),
		revisionNumbers: make(map[revisionNumberKey]string),
		now:             time.Now,
	}
}

func (r *MemoryRepository) CreateTenant(ctx context.Context, item Tenant) (Tenant, error) {
	if err := contextError(ctx); err != nil {
		return Tenant{}, err
	}
	now := r.now().UTC()
	if item.Status == "" {
		item.Status = StatusActive
	}
	item.CreatedAt = now
	item.UpdatedAt = now
	if err := item.Validate(); err != nil {
		return Tenant{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.tenants[item.ID]; ok {
		return Tenant{}, fmt.Errorf("%w: tenant %q", ErrAlreadyExists, item.ID)
	}
	if _, ok := r.tenantSlugs[item.Slug]; ok {
		return Tenant{}, fmt.Errorf("%w: tenant slug %q", ErrAlreadyExists, item.Slug)
	}
	r.tenants[item.ID] = item
	r.tenantSlugs[item.Slug] = item.ID
	return cloneTenant(item), nil
}

func (r *MemoryRepository) GetTenant(ctx context.Context, tenantID string) (Tenant, error) {
	if err := contextError(ctx); err != nil {
		return Tenant{}, err
	}
	if err := ValidateResourceID("tenant id", tenantID); err != nil {
		return Tenant{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	item, ok := r.tenants[tenantID]
	if !ok {
		return Tenant{}, fmt.Errorf("%w: tenant %q", ErrNotFound, tenantID)
	}
	return cloneTenant(item), nil
}

func (r *MemoryRepository) CreateAgentApp(
	ctx context.Context,
	scope TenantContext,
	item AgentApp,
) (AgentApp, error) {
	if err := contextError(ctx); err != nil {
		return AgentApp{}, err
	}
	if item.Status == "" {
		item.Status = AppStatusActive
	}
	now := r.now().UTC()
	item.CreatedAt = now
	item.UpdatedAt = now
	item.RoutingVersion = 0
	item.RoutingPolicy = RoutingPolicy{}
	if err := item.Validate(scope); err != nil {
		return AgentApp{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.requireActiveTenantLocked(scope.TenantID); err != nil {
		return AgentApp{}, err
	}
	key := appKey{tenantID: scope.TenantID, appID: item.ID}
	if _, ok := r.apps[key]; ok {
		return AgentApp{}, fmt.Errorf("%w: app %q", ErrAlreadyExists, item.ID)
	}
	item.RoutingPolicy = cloneRoutingPolicy(item.RoutingPolicy)
	r.apps[key] = item
	return cloneAgentApp(item), nil
}

func (r *MemoryRepository) GetAgentApp(
	ctx context.Context,
	scope TenantContext,
	appID string,
) (AgentApp, error) {
	if err := contextError(ctx); err != nil {
		return AgentApp{}, err
	}
	if err := scope.Validate(); err != nil {
		return AgentApp{}, err
	}
	if err := ValidateResourceID("app id", appID); err != nil {
		return AgentApp{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	item, ok := r.apps[appKey{tenantID: scope.TenantID, appID: appID}]
	if !ok {
		return AgentApp{}, fmt.Errorf("%w: app %q", ErrNotFound, appID)
	}
	return cloneAgentApp(item), nil
}

func (r *MemoryRepository) CreateRevision(
	ctx context.Context,
	scope TenantContext,
	item AgentRevision,
) (AgentRevision, error) {
	if err := contextError(ctx); err != nil {
		return AgentRevision{}, err
	}
	if item.Status == "" {
		item.Status = RevisionStatusDraft
	}
	item.CreatedAt = r.now().UTC()
	item.PublishedAt = nil
	if err := item.ValidateForCreate(scope); err != nil {
		return AgentRevision{}, err
	}
	digest, err := item.Config.Digest()
	if err != nil {
		return AgentRevision{}, err
	}
	item.ConfigDigest = digest
	item.Config = cloneRevisionConfig(item.Config)

	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.requireActiveTenantLocked(scope.TenantID); err != nil {
		return AgentRevision{}, err
	}
	appMapKey := appKey{tenantID: scope.TenantID, appID: item.AgentAppID}
	if _, ok := r.apps[appMapKey]; !ok {
		return AgentRevision{}, fmt.Errorf("%w: app %q", ErrNotFound, item.AgentAppID)
	}
	key := revisionKey{tenantID: scope.TenantID, appID: item.AgentAppID, revisionID: item.ID}
	if _, ok := r.revisions[key]; ok {
		return AgentRevision{}, fmt.Errorf("%w: revision %q", ErrAlreadyExists, item.ID)
	}
	numberKey := revisionNumberKey{
		tenantID:   scope.TenantID,
		appID:      item.AgentAppID,
		revisionNo: item.RevisionNo,
	}
	if _, ok := r.revisionNumbers[numberKey]; ok {
		return AgentRevision{}, fmt.Errorf("%w: revision number %d", ErrAlreadyExists, item.RevisionNo)
	}
	r.revisions[key] = item
	r.revisionNumbers[numberKey] = item.ID
	return cloneAgentRevision(item), nil
}

func (r *MemoryRepository) GetRevision(
	ctx context.Context,
	scope TenantContext,
	appID string,
	revisionID string,
) (AgentRevision, error) {
	if err := contextError(ctx); err != nil {
		return AgentRevision{}, err
	}
	if err := scope.Validate(); err != nil {
		return AgentRevision{}, err
	}
	if err := ValidateResourceID("app id", appID); err != nil {
		return AgentRevision{}, err
	}
	if err := ValidateResourceID("revision id", revisionID); err != nil {
		return AgentRevision{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	item, ok := r.revisions[revisionKey{
		tenantID:   scope.TenantID,
		appID:      appID,
		revisionID: revisionID,
	}]
	if !ok {
		return AgentRevision{}, fmt.Errorf("%w: revision %q", ErrNotFound, revisionID)
	}
	return cloneAgentRevision(item), nil
}

func (r *MemoryRepository) PublishRevision(
	ctx context.Context,
	scope TenantContext,
	appID string,
	revisionID string,
) (AgentApp, AgentRevision, error) {
	if err := contextError(ctx); err != nil {
		return AgentApp{}, AgentRevision{}, err
	}
	if err := scope.Validate(); err != nil {
		return AgentApp{}, AgentRevision{}, err
	}
	if err := ValidateResourceID("app id", appID); err != nil {
		return AgentApp{}, AgentRevision{}, err
	}
	if err := ValidateResourceID("revision id", revisionID); err != nil {
		return AgentApp{}, AgentRevision{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.requireActiveTenantLocked(scope.TenantID); err != nil {
		return AgentApp{}, AgentRevision{}, err
	}
	appMapKey := appKey{tenantID: scope.TenantID, appID: appID}
	app, ok := r.apps[appMapKey]
	if !ok {
		return AgentApp{}, AgentRevision{}, fmt.Errorf("%w: app %q", ErrNotFound, appID)
	}
	revMapKey := revisionKey{tenantID: scope.TenantID, appID: appID, revisionID: revisionID}
	revision, ok := r.revisions[revMapKey]
	if !ok {
		return AgentApp{}, AgentRevision{}, fmt.Errorf("%w: revision %q", ErrNotFound, revisionID)
	}
	if revision.Status == RevisionStatusPublished {
		if app.RoutingPolicy.DefaultRevisionID == revisionID {
			return cloneAgentApp(app), cloneAgentRevision(revision), nil
		}
		now := r.now().UTC()
		app.RoutingVersion++
		app.RoutingPolicy = RoutingPolicy{
			DefaultRevisionID: revision.ID,
			Routes:            []RevisionRoute{{RevisionID: revision.ID, Weight: 10000}},
		}
		app.UpdatedAt = now
		r.apps[appMapKey] = app
		return cloneAgentApp(app), cloneAgentRevision(revision), nil
	}
	if revision.Status != RevisionStatusDraft {
		return AgentApp{}, AgentRevision{}, fmt.Errorf("%w: cannot publish status %q", ErrInvalidArgument, revision.Status)
	}
	// A mismatch here means the stored config was changed after it was created,
	// which no request can cause and no request can fix. See ErrConfigIntegrity.
	digest, err := revision.Config.Digest()
	if err != nil || digest != revision.ConfigDigest {
		return AgentApp{}, AgentRevision{}, fmt.Errorf("%w: revision %q", ErrConfigIntegrity, revisionID)
	}

	now := r.now().UTC()
	revision.Status = RevisionStatusPublished
	revision.PublishedAt = &now
	app.RoutingVersion++
	app.RoutingPolicy = RoutingPolicy{
		DefaultRevisionID: revision.ID,
		Routes:            []RevisionRoute{{RevisionID: revision.ID, Weight: 10000}},
	}
	app.UpdatedAt = now
	r.revisions[revMapKey] = revision
	r.apps[appMapKey] = app
	return cloneAgentApp(app), cloneAgentRevision(revision), nil
}

// ResolveRevision returns a published pinned revision, or the app default.
func (r *MemoryRepository) ResolveRevision(
	ctx context.Context,
	scope TenantContext,
	appID string,
	pinnedRevisionID string,
) (AgentRevision, error) {
	if err := contextError(ctx); err != nil {
		return AgentRevision{}, err
	}
	if err := scope.Validate(); err != nil {
		return AgentRevision{}, err
	}
	if err := ValidateResourceID("app id", appID); err != nil {
		return AgentRevision{}, err
	}
	if pinnedRevisionID != "" {
		if err := ValidateResourceID("revision id", pinnedRevisionID); err != nil {
			return AgentRevision{}, err
		}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.requireActiveTenantLocked(scope.TenantID); err != nil {
		return AgentRevision{}, err
	}
	app, ok := r.apps[appKey{tenantID: scope.TenantID, appID: appID}]
	if !ok || app.Status != AppStatusActive {
		return AgentRevision{}, fmt.Errorf("%w: active app %q", ErrNotFound, appID)
	}
	revisionID := pinnedRevisionID
	if revisionID == "" {
		revisionID = app.RoutingPolicy.DefaultRevisionID
	}
	if revisionID == "" {
		return AgentRevision{}, fmt.Errorf("%w: app %q", ErrNoPublishedRevision, appID)
	}
	revision, ok := r.revisions[revisionKey{
		tenantID:   scope.TenantID,
		appID:      appID,
		revisionID: revisionID,
	}]
	if !ok {
		return AgentRevision{}, fmt.Errorf("%w: revision %q", ErrNotFound, revisionID)
	}
	if revision.Status != RevisionStatusPublished {
		return AgentRevision{}, fmt.Errorf("%w: revision %q", ErrRevisionNotPublished, revisionID)
	}
	return cloneAgentRevision(revision), nil
}

func (r *MemoryRepository) requireActiveTenantLocked(tenantID string) error {
	tenantItem, ok := r.tenants[tenantID]
	if !ok {
		return fmt.Errorf("%w: tenant %q", ErrNotFound, tenantID)
	}
	if tenantItem.Status != StatusActive {
		return fmt.Errorf("%w: tenant %q has status %q", ErrTenantInactive, tenantID, tenantItem.Status)
	}
	return nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalidArgument)
	}
	return ctx.Err()
}

func cloneTenant(item Tenant) Tenant {
	return item
}

func cloneAgentApp(item AgentApp) AgentApp {
	item.RoutingPolicy = cloneRoutingPolicy(item.RoutingPolicy)
	return item
}

func cloneRoutingPolicy(policy RoutingPolicy) RoutingPolicy {
	policy.Routes = append([]RevisionRoute(nil), policy.Routes...)
	return policy
}

func cloneAgentRevision(item AgentRevision) AgentRevision {
	item.Config = cloneRevisionConfig(item.Config)
	if item.PublishedAt != nil {
		publishedAt := *item.PublishedAt
		item.PublishedAt = &publishedAt
	}
	return item
}

func cloneRevisionConfig(config RevisionConfig) RevisionConfig {
	config.ToolRefs = append([]string(nil), config.ToolRefs...)
	config.SkillRefs = append([]string(nil), config.SkillRefs...)
	config.KnowledgeRefs = append([]string(nil), config.KnowledgeRefs...)
	config.PolicyRefs = append([]string(nil), config.PolicyRefs...)
	if config.Model.Temperature != nil {
		temperature := *config.Model.Temperature
		config.Model.Temperature = &temperature
	}
	return config
}
