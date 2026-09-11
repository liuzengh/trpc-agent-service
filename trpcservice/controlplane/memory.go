package controlplane

import (
	"context"
	"errors"
	"sync"
	"time"
)

// MemoryRepository is an in-process control plane for tutorials and tests.
type MemoryRepository struct {
	resourceMemory  resourceMemory
	knowledgeLock   chan struct{}
	knowledgeStates map[string][]byte
	mu              sync.RWMutex
	closed          bool
	tenants         map[string]Tenant
	apps            map[string]AgentApp
	revisions       map[string]AgentRevision
	channels        map[string]ChannelBinding
	channelIDs      map[string]ChannelBinding
	backends        []BackendBinding
	migrations      map[string]BackendMigration
}

// NewMemoryRepository builds a validated in-process snapshot.
func NewMemoryRepository(data BootstrapData) *MemoryRepository {
	repository := &MemoryRepository{
		knowledgeLock: make(chan struct{}, 1), knowledgeStates: map[string][]byte{},
		tenants:    make(map[string]Tenant),
		apps:       make(map[string]AgentApp),
		revisions:  make(map[string]AgentRevision),
		channels:   make(map[string]ChannelBinding),
		channelIDs: make(map[string]ChannelBinding),
		backends:   append([]BackendBinding(nil), data.BackendBindings...),
		migrations: make(map[string]BackendMigration),
	}
	for _, tenant := range data.Tenants {
		repository.tenants[tenant.ID] = cloneTenant(tenant)
	}
	for _, app := range data.Apps {
		repository.apps[scopedKey(app.TenantID, app.ID)] = cloneAgentApp(app)
	}
	for _, revision := range data.Revisions {
		repository.revisions[scopedKey(revision.TenantID, revision.ID)] = cloneRevision(revision)
	}
	for _, binding := range data.ChannelBindings {
		repository.channels[binding.CallbackKey] = cloneChannelBinding(binding)
		repository.channelIDs[scopedKey(binding.TenantID, binding.ID)] = cloneChannelBinding(binding)
	}
	return repository
}

func (r *MemoryRepository) GetChannelBinding(
	ctx context.Context,
	tenantID string,
	bindingID string,
) (ChannelBinding, error) {
	if err := r.check(ctx); err != nil {
		return ChannelBinding{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	binding, ok := r.channelIDs[scopedKey(tenantID, bindingID)]
	if !ok {
		return ChannelBinding{}, ErrNotFound
	}
	return cloneChannelBinding(binding), nil
}

func (r *MemoryRepository) GetTenant(ctx context.Context, tenantID string) (Tenant, error) {
	if err := r.check(ctx); err != nil {
		return Tenant{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	tenant, ok := r.tenants[tenantID]
	if !ok {
		return Tenant{}, ErrNotFound
	}
	return cloneTenant(tenant), nil
}

func (r *MemoryRepository) GetAgentApp(
	ctx context.Context,
	tenantID string,
	appID string,
) (AgentApp, error) {
	if err := r.check(ctx); err != nil {
		return AgentApp{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	app, ok := r.apps[scopedKey(tenantID, appID)]
	if !ok {
		return AgentApp{}, ErrNotFound
	}
	return cloneAgentApp(app), nil
}

func (r *MemoryRepository) GetRevision(
	ctx context.Context,
	tenantID string,
	revisionID string,
) (AgentRevision, error) {
	if err := r.check(ctx); err != nil {
		return AgentRevision{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	revision, ok := r.revisions[scopedKey(tenantID, revisionID)]
	if !ok {
		return AgentRevision{}, ErrNotFound
	}
	return cloneRevision(revision), nil
}

func (r *MemoryRepository) GetStableRevision(
	ctx context.Context,
	tenantID string,
	appID string,
) (AgentRevision, error) {
	app, err := r.GetAgentApp(ctx, tenantID, appID)
	if err != nil {
		return AgentRevision{}, err
	}
	if app.StableRevisionID == "" {
		return AgentRevision{}, ErrNotFound
	}
	return r.GetRevision(ctx, tenantID, app.StableRevisionID)
}

func (r *MemoryRepository) GetChannelBindingByCallbackKey(
	ctx context.Context,
	callbackKey string,
) (ChannelBinding, error) {
	if err := r.check(ctx); err != nil {
		return ChannelBinding{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	binding, ok := r.channels[callbackKey]
	if !ok {
		return ChannelBinding{}, ErrNotFound
	}
	return cloneChannelBinding(binding), nil
}

func (r *MemoryRepository) ListBackendBindings(
	ctx context.Context,
	tenantID string,
	appID string,
) ([]BackendBinding, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]BackendBinding, 0)
	for _, binding := range r.backends {
		if binding.TenantID != tenantID || (binding.AppID != "" && binding.AppID != appID) {
			continue
		}
		result = append(result, cloneBackendBinding(binding))
	}
	return result, nil
}

func (r *MemoryRepository) GetBackendBinding(
	ctx context.Context,
	tenantID string,
	bindingID string,
) (BackendBinding, error) {
	if err := r.check(ctx); err != nil {
		return BackendBinding{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, binding := range r.backends {
		if binding.TenantID == tenantID && binding.ID == bindingID {
			return cloneBackendBinding(binding), nil
		}
	}
	return BackendBinding{}, ErrNotFound
}

func (r *MemoryRepository) GetBackendMigration(
	ctx context.Context,
	tenantID string,
	migrationID string,
) (BackendMigration, error) {
	if err := r.check(ctx); err != nil {
		return BackendMigration{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	migration, ok := r.migrations[scopedKey(tenantID, migrationID)]
	if !ok {
		return BackendMigration{}, ErrNotFound
	}
	return cloneBackendMigration(migration), nil
}

func (r *MemoryRepository) GetActiveBackendMigration(
	ctx context.Context,
	tenantID string,
	appID string,
	resourceType string,
) (BackendMigration, error) {
	if err := r.check(ctx); err != nil {
		return BackendMigration{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, migration := range r.migrations {
		if migration.TenantID == tenantID && migration.AppID == appID &&
			migration.ResourceType == resourceType && migrationActive(migration.State) {
			return cloneBackendMigration(migration), nil
		}
	}
	return BackendMigration{}, ErrNotFound
}

func (r *MemoryRepository) AdjustBackendMigrationRepair(
	_ context.Context,
	tenantID string,
	migrationID string,
	delta int64,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := scopedKey(tenantID, migrationID)
	migration, ok := r.migrations[key]
	if !ok {
		return ErrNotFound
	}
	migration.RepairBacklog += delta
	if migration.RepairBacklog < 0 {
		migration.RepairBacklog = 0
	}
	migration.UpdatedAt = time.Now().UTC()
	r.migrations[key] = migration
	return nil
}

func (r *MemoryRepository) Ready(ctx context.Context) error {
	return r.check(ctx)
}

func (r *MemoryRepository) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	return nil
}

func (r *MemoryRepository) CreateTenant(_ context.Context, tenant Tenant) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRepositoryClosed
	}
	if _, exists := r.tenants[tenant.ID]; exists {
		return ErrConflict
	}
	r.tenants[tenant.ID] = cloneTenant(tenant)
	return nil
}

func (r *MemoryRepository) CreateAgentApp(_ context.Context, app AgentApp) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.tenants[app.TenantID]; !exists {
		return ErrNotFound
	}
	key := scopedKey(app.TenantID, app.ID)
	if _, exists := r.apps[key]; exists {
		return ErrConflict
	}
	r.apps[key] = cloneAgentApp(app)
	return nil
}

func (r *MemoryRepository) CreateRevision(_ context.Context, revision AgentRevision) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.apps[scopedKey(revision.TenantID, revision.AppID)]; !exists {
		return ErrNotFound
	}
	key := scopedKey(revision.TenantID, revision.ID)
	if _, exists := r.revisions[key]; exists {
		return ErrConflict
	}
	r.revisions[key] = cloneRevision(revision)
	return nil
}

func (r *MemoryRepository) PublishRevision(
	_ context.Context,
	tenantID string,
	appID string,
	revisionID string,
	expectedVersion int64,
) (AgentApp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := scopedKey(tenantID, appID)
	app, exists := r.apps[key]
	if !exists {
		return AgentApp{}, ErrNotFound
	}
	revision, exists := r.revisions[scopedKey(tenantID, revisionID)]
	if !exists || revision.AppID != appID {
		return AgentApp{}, ErrNotFound
	}
	if app.Version != expectedVersion {
		return AgentApp{}, ErrConflict
	}
	app.StableRevisionID = revisionID
	app.Version++
	app.UpdatedAt = time.Now().UTC()
	r.apps[key] = app
	return cloneAgentApp(app), nil
}

func (r *MemoryRepository) UpdateRolloutPolicy(
	_ context.Context,
	tenantID string,
	appID string,
	rolloutPolicy []byte,
	expectedVersion int64,
) (AgentApp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := scopedKey(tenantID, appID)
	app, ok := r.apps[key]
	if !ok {
		return AgentApp{}, ErrNotFound
	}
	if app.Version != expectedVersion {
		return AgentApp{}, ErrConflict
	}
	app.RolloutPolicy = cloneJSON(rolloutPolicy)
	app.Version++
	app.UpdatedAt = time.Now().UTC()
	r.apps[key] = app
	return cloneAgentApp(app), nil
}

func (r *MemoryRepository) CreateChannelBinding(
	_ context.Context,
	binding ChannelBinding,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.apps[scopedKey(binding.TenantID, binding.AppID)]; !exists {
		return ErrNotFound
	}
	if _, exists := r.channels[binding.CallbackKey]; exists {
		return ErrConflict
	}
	if _, exists := r.channelIDs[scopedKey(binding.TenantID, binding.ID)]; exists {
		return ErrConflict
	}
	r.channels[binding.CallbackKey] = cloneChannelBinding(binding)
	r.channelIDs[scopedKey(binding.TenantID, binding.ID)] = cloneChannelBinding(binding)
	return nil
}

func (r *MemoryRepository) UpdateChannelBinding(
	_ context.Context,
	tenantID string,
	bindingID string,
	config []byte,
	status string,
	expectedVersion int64,
) (ChannelBinding, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := scopedKey(tenantID, bindingID)
	binding, exists := r.channelIDs[key]
	if !exists {
		return ChannelBinding{}, ErrNotFound
	}
	if binding.Version != expectedVersion {
		return ChannelBinding{}, ErrConflict
	}
	binding.Config = cloneJSON(config)
	binding.Status = status
	binding.Version++
	binding.UpdatedAt = time.Now().UTC()
	r.channelIDs[key] = cloneChannelBinding(binding)
	r.channels[binding.CallbackKey] = cloneChannelBinding(binding)
	return cloneChannelBinding(binding), nil
}

func (r *MemoryRepository) CreateBackendBinding(
	_ context.Context,
	binding BackendBinding,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if binding.AppID != "" {
		if _, exists := r.apps[scopedKey(binding.TenantID, binding.AppID)]; !exists {
			return ErrNotFound
		}
	}
	for _, existing := range r.backends {
		if existing.ID == binding.ID {
			return ErrConflict
		}
	}
	r.backends = append(r.backends, cloneBackendBinding(binding))
	return nil
}

func (r *MemoryRepository) CreateBackendMigration(
	_ context.Context,
	migration BackendMigration,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrRepositoryClosed
	}
	key := scopedKey(migration.TenantID, migration.ID)
	if _, exists := r.migrations[key]; exists {
		return ErrConflict
	}
	for _, existing := range r.migrations {
		if existing.TenantID == migration.TenantID && existing.AppID == migration.AppID &&
			existing.ResourceType == migration.ResourceType && migrationActive(existing.State) {
			return ErrConflict
		}
	}
	r.migrations[key] = cloneBackendMigration(migration)
	return nil
}

func (r *MemoryRepository) TransitionBackendMigration(
	ctx context.Context,
	tenantID string,
	migrationID string,
	nextState string,
	expectedVersion int64,
	checkpoint []byte,
	verification []byte,
) (BackendMigration, error) {
	current, err := r.GetBackendMigration(ctx, tenantID, migrationID)
	if err != nil {
		return BackendMigration{}, err
	}
	if validResource(current.ResourceType) {
		if !ResourceAccessHeld(ctx, tenantID, current.AppID, current.ResourceType, "") {
			var out BackendMigration
			err := r.WithResourceSync(ctx, tenantID, current.AppID, current.ResourceType, func(ctx context.Context, s *ResourceSync, _ func() error) error {
				if (nextState == MigrationCutover || nextState == MigrationCompleted) && !validResourceProof(*s, current) {
					return errors.New("migration requires current complete server verification")
				}
				var err error
				out, err = r.TransitionBackendMigration(ctx, tenantID, migrationID, nextState, expectedVersion, checkpoint, verification)
				return err
			})
			return out, err
		}
	}
	select {
	case r.knowledgeLock <- struct{}{}:
	case <-ctx.Done():
		return BackendMigration{}, context.Cause(ctx)
	}
	defer func() { <-r.knowledgeLock }()
	r.mu.Lock()
	defer r.mu.Unlock()
	key := scopedKey(tenantID, migrationID)
	migration, ok := r.migrations[key]
	if !ok {
		return BackendMigration{}, ErrNotFound
	}
	if migration.Version != expectedVersion {
		return BackendMigration{}, ErrConflict
	}
	if migration.ResourceType == "knowledge" && (nextState == MigrationCutover || nextState == MigrationCompleted) && !validKnowledgeProof(r.knowledgeStates[scopedKey(tenantID, migration.AppID)], migration) {
		return BackendMigration{}, errors.New("knowledge migration requires current server verification")
	}
	migration.State = nextState
	migration.Version++
	migration.UpdatedAt = time.Now().UTC()
	if len(checkpoint) > 0 {
		migration.Checkpoint = cloneJSON(checkpoint)
	}
	if len(verification) > 0 {
		migration.Verification = cloneJSON(verification)
	}
	if nextState == MigrationCompleted || nextState == MigrationRolledBack {
		for index := range r.backends {
			binding := &r.backends[index]
			switch binding.ID {
			case migration.SourceBindingID:
				if nextState == MigrationCompleted {
					binding.MigrationState = "retired"
				} else {
					binding.MigrationState = "active"
				}
				binding.Version++
			case migration.TargetBindingID:
				if nextState == MigrationCompleted {
					binding.MigrationState = "active"
				} else {
					binding.MigrationState = "migration_target"
				}
				binding.Version++
			}
		}
	}
	r.migrations[key] = migration
	return cloneBackendMigration(migration), nil
}

func (r *MemoryRepository) check(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return ErrRepositoryClosed
	}
	return nil
}

var ErrRepositoryClosed = errors.New("control-plane repository is closed")

func scopedKey(first string, second string) string { return first + "\x00" + second }

func cloneJSON(value []byte) []byte { return append([]byte(nil), value...) }

func cloneTenant(value Tenant) Tenant {
	value.QuotaConfig = cloneJSON(value.QuotaConfig)
	value.AuditPolicy = cloneJSON(value.AuditPolicy)
	return value
}

func cloneAgentApp(value AgentApp) AgentApp {
	value.RolloutPolicy = cloneJSON(value.RolloutPolicy)
	return value
}

func cloneRevision(value AgentRevision) AgentRevision {
	value.AgentConfig = cloneJSON(value.AgentConfig)
	value.ModelConfig = cloneJSON(value.ModelConfig)
	value.ToolPolicy = cloneJSON(value.ToolPolicy)
	value.KnowledgeConfig = cloneJSON(value.KnowledgeConfig)
	value.MemoryConfig = cloneJSON(value.MemoryConfig)
	value.GuardrailConfig = cloneJSON(value.GuardrailConfig)
	return value
}

func cloneChannelBinding(value ChannelBinding) ChannelBinding {
	value.Config = cloneJSON(value.Config)
	return value
}

func cloneBackendBinding(value BackendBinding) BackendBinding {
	value.Config = cloneJSON(value.Config)
	return value
}

func cloneBackendMigration(value BackendMigration) BackendMigration {
	value.Checkpoint = cloneJSON(value.Checkpoint)
	value.Verification = cloneJSON(value.Verification)
	return value
}

func migrationActive(state string) bool {
	return state != MigrationCompleted && state != MigrationRolledBack && state != MigrationFailed
}

var _ Repository = (*MemoryRepository)(nil)
var _ MutableRepository = (*MemoryRepository)(nil)
