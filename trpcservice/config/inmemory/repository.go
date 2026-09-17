// Package inmemory implements immutable ConfigSnapshot publication for local
// contract tests.
package inmemory

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type Repository struct {
	mu        sync.RWMutex
	tenants   tenant.Repository
	apps      agentapp.Repository
	snapshots map[string]map[int64]config.Snapshot
	next      map[string]int64
	releases  map[string]config.Release
}

func New(tenants tenant.Repository, apps agentapp.Repository) *Repository {
	return &Repository{tenants: tenants, apps: apps, snapshots: make(map[string]map[int64]config.Snapshot), next: make(map[string]int64), releases: make(map[string]config.Release)}
}

func (r *Repository) Validate(ctx context.Context, in config.ValidateInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if in.TenantID == "" || in.Payload.SchemaVersion != config.CurrentSchemaVersion || in.Payload.PolicyVersion < 1 || in.Payload.DefaultAgentAppID == "" {
		return config.ErrInvalid
	}
	if err := r.validateApp(ctx, in.TenantID, in.Payload.DefaultAgentAppID); err != nil {
		return err
	}
	seenChannels := map[string]struct{}{}
	for _, binding := range in.Payload.ChannelBindings {
		if binding.BindingID == "" || binding.Channel == "" || binding.ExternalAccountID == "" || binding.AgentAppID == "" || binding.SecretRef.Ref == "" || binding.SecretRef.Version < 1 || !config.ValidSendSecret(binding) {
			return config.ErrInvalid
		}
		key := binding.Channel + "\x00" + binding.ExternalAccountID
		if _, ok := seenChannels[key]; ok {
			return config.ErrInvalid
		}
		seenChannels[key] = struct{}{}
		if err := r.validateApp(ctx, in.TenantID, binding.AgentAppID); err != nil {
			return err
		}
	}
	seenDomains := map[string]struct{}{}
	for _, binding := range in.Payload.BackendBindings {
		if binding.Domain == "" || binding.BackendProfileID == "" || binding.BackendVersion < 1 {
			return config.ErrInvalid
		}
		if _, ok := seenDomains[binding.Domain]; ok {
			return config.ErrInvalid
		}
		seenDomains[binding.Domain] = struct{}{}
	}
	if err := config.ValidateBackendTopology(in.Payload.BackendBindings); err != nil {
		return err
	}
	return nil
}
func (r *Repository) validateApp(ctx context.Context, tenantID, appID string) error {
	app, err := r.apps.Get(ctx, tenantID, appID)
	if err != nil {
		return err
	}
	if app.TenantID != tenantID || app.Status != agentapp.StatusActive || app.CurrentRevision < 1 {
		return config.ErrInvalid
	}
	rev, err := r.apps.GetRevision(ctx, tenantID, appID, app.CurrentRevision)
	if err != nil {
		return err
	}
	if rev.State != agentapp.RevisionPublished || rev.ContentDigest == "" {
		return config.ErrInvalid
	}
	return nil
}
func (r *Repository) Publish(ctx context.Context, in config.PublishInput) (config.PublishResult, error) {
	if err := r.Validate(ctx, config.ValidateInput{TenantID: in.TenantID, Payload: in.Payload}); err != nil {
		return config.PublishResult{}, err
	}
	current, err := r.tenants.Get(ctx, in.TenantID)
	if err != nil {
		return config.PublishResult{}, err
	}
	if current.Version != in.ExpectedTenantVersion {
		return config.PublishResult{}, tenant.ErrVersionConflict
	}
	if current.Status == tenant.StatusDisabled {
		return config.PublishResult{}, tenant.ErrStatusConflict
	}
	snapshot, err := r.stage(in.TenantID, in.Payload)
	if err != nil {
		return config.PublishResult{}, err
	}
	nextTenant := current
	nextTenant.DefaultAgentAppID = in.Payload.DefaultAgentAppID
	nextTenant.ActiveConfigVersion = snapshot.ConfigVersion
	changed, err := r.tenants.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{Tenant: nextTenant, ExpectedVersion: in.ExpectedTenantVersion, ChangeMetadata: in.Metadata})
	if err != nil {
		r.removeStaged(in.TenantID, snapshot.ConfigVersion)
		return config.PublishResult{}, err
	}
	r.activate(in.TenantID, snapshot.ConfigVersion)
	snapshot, _ = r.Get(ctx, in.TenantID, snapshot.ConfigVersion)
	return config.PublishResult{Snapshot: snapshot, Tenant: changed.Tenant}, nil
}

// Stage persists a validated, immutable candidate without moving the tenant's
// active pointer. It is safe to inspect and attach to a release, but cannot
// affect admissions until a release selects it.
func (r *Repository) Stage(ctx context.Context, in config.StageInput) (config.Snapshot, error) {
	if err := in.Metadata.Validate(); err != nil {
		return config.Snapshot{}, err
	}
	if err := r.Validate(ctx, config.ValidateInput{TenantID: in.TenantID, Payload: in.Payload}); err != nil {
		return config.Snapshot{}, err
	}
	current, err := r.tenants.Get(ctx, in.TenantID)
	if err != nil {
		return config.Snapshot{}, err
	}
	if current.Version != in.ExpectedTenantVersion {
		return config.Snapshot{}, tenant.ErrVersionConflict
	}
	if current.Status != tenant.StatusActive {
		return config.Snapshot{}, tenant.ErrStatusConflict
	}
	snapshot, err := r.stage(in.TenantID, in.Payload)
	if err != nil {
		return config.Snapshot{}, err
	}
	r.activate(in.TenantID, snapshot.ConfigVersion)
	return r.Get(ctx, in.TenantID, snapshot.ConfigVersion)
}
func (r *Repository) stage(tenantID string, payload config.ConfigV1) (config.Snapshot, error) {
	normalized := config.NormalizeV1(payload)
	digest, _, err := config.ContentDigest(normalized)
	if err != nil {
		return config.Snapshot{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	version := r.next[tenantID] + 1
	r.next[tenantID] = version
	if r.snapshots[tenantID] == nil {
		r.snapshots[tenantID] = make(map[int64]config.Snapshot)
	}
	snapshot := config.Snapshot{TenantID: tenantID, ConfigVersion: version, SchemaVersion: config.CurrentSchemaVersion, Payload: normalized, ContentDigest: digest, State: config.StateStaged, CreatedAt: time.Now().UTC()}
	r.snapshots[tenantID][version] = snapshot
	return cloneSnapshot(snapshot), nil
}
func (r *Repository) activate(tenantID string, version int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	snapshot := r.snapshots[tenantID][version]
	snapshot.State = config.StatePublished
	snapshot.PublishedAt = time.Now().UTC()
	r.snapshots[tenantID][version] = snapshot
}
func (r *Repository) removeStaged(tenantID string, version int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if snapshot, ok := r.snapshots[tenantID][version]; ok && snapshot.State == config.StateStaged {
		delete(r.snapshots[tenantID], version)
	}
}
func (r *Repository) Get(ctx context.Context, tenantID string, version int64) (config.Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return config.Snapshot{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshot, ok := r.snapshots[tenantID][version]
	if !ok || snapshot.State != config.StatePublished {
		return config.Snapshot{}, config.ErrNotFound
	}
	return cloneSnapshot(snapshot), nil
}
func (r *Repository) GetCurrent(ctx context.Context, tenantID string) (config.Snapshot, error) {
	current, err := r.tenants.Get(ctx, tenantID)
	if err != nil {
		return config.Snapshot{}, err
	}
	if current.ActiveConfigVersion < 1 {
		return config.Snapshot{}, config.ErrNotFound
	}
	return r.Get(ctx, tenantID, current.ActiveConfigVersion)
}
func (r *Repository) Rollback(ctx context.Context, in config.RollbackInput) (config.PublishResult, error) {
	target, err := r.Get(ctx, in.TenantID, in.TargetVersion)
	if err != nil {
		return config.PublishResult{}, err
	}
	return r.Publish(ctx, config.PublishInput{TenantID: in.TenantID, ExpectedTenantVersion: in.ExpectedTenantVersion, Payload: target.Payload, Metadata: in.Metadata})
}

func (r *Repository) CreateRelease(ctx context.Context, in config.ReleaseCreateInput) (config.Release, error) {
	if err := validReleaseCreate(in); err != nil {
		return config.Release{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.releases[in.ReleaseID]; exists {
		return config.Release{}, config.ErrReleaseConflict
	}
	for _, target := range in.Targets {
		current, err := r.tenants.Get(ctx, target.TenantID)
		if err != nil || current.Status != tenant.StatusActive || current.ActiveConfigVersion != target.BaselineConfigVersion {
			return config.Release{}, config.ErrReleaseConflict
		}
		if snapshot, ok := r.snapshots[target.TenantID][target.CandidateConfigVersion]; !ok || snapshot.State != config.StatePublished {
			return config.Release{}, config.ErrNotFound
		}
		for _, existing := range r.releases {
			if existing.State != config.ReleaseActive {
				continue
			}
			for _, other := range existing.Targets {
				if other.TenantID == target.TenantID {
					return config.Release{}, config.ErrReleaseConflict
				}
			}
		}
	}
	now := time.Now().UTC()
	value := config.Release{ReleaseID: in.ReleaseID, State: config.ReleaseActive, Percentage: in.Percentage, Salt: in.Salt,
		Version: 1, Targets: append([]config.ReleaseTarget(nil), in.Targets...), CreatedAt: now, UpdatedAt: now}
	r.releases[value.ReleaseID] = value
	return cloneRelease(value), nil
}

func (r *Repository) GetRelease(ctx context.Context, releaseID string) (config.Release, error) {
	if err := ctx.Err(); err != nil {
		return config.Release{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	value, ok := r.releases[releaseID]
	if !ok {
		return config.Release{}, config.ErrReleaseNotFound
	}
	return cloneRelease(value), nil
}

func (r *Repository) UpdateRelease(ctx context.Context, in config.ReleaseUpdateInput) (config.Release, error) {
	if err := in.Metadata.Validate(); err != nil || in.ReleaseID == "" || in.ExpectedVersion < 1 || in.Percentage < 0 || in.Percentage > 100 {
		return config.Release{}, config.ErrInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	value, ok := r.releases[in.ReleaseID]
	if !ok {
		return config.Release{}, config.ErrReleaseNotFound
	}
	if value.State != config.ReleaseActive || value.Version != in.ExpectedVersion {
		return config.Release{}, config.ErrReleaseConflict
	}
	value.Percentage, value.Version, value.UpdatedAt = in.Percentage, value.Version+1, time.Now().UTC()
	r.releases[in.ReleaseID] = value
	return cloneRelease(value), nil
}

func (r *Repository) RollbackRelease(ctx context.Context, in config.ReleaseRollbackInput) (config.Release, error) {
	if err := in.Metadata.Validate(); err != nil || in.ReleaseID == "" || in.ExpectedVersion < 1 {
		return config.Release{}, config.ErrInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	value, ok := r.releases[in.ReleaseID]
	if !ok {
		return config.Release{}, config.ErrReleaseNotFound
	}
	if value.State != config.ReleaseActive || value.Version != in.ExpectedVersion {
		return config.Release{}, config.ErrReleaseConflict
	}
	value.State, value.Version, value.UpdatedAt = config.ReleaseRolledBack, value.Version+1, time.Now().UTC()
	r.releases[in.ReleaseID] = value
	return cloneRelease(value), nil
}

func (r *Repository) SelectEffective(ctx context.Context, tenantID string, baseline int64) (config.Snapshot, error) {
	current, err := r.tenants.Get(ctx, tenantID)
	if err != nil {
		return config.Snapshot{}, err
	}
	if baseline == 0 {
		baseline = current.ActiveConfigVersion
	}
	if baseline < 1 || current.ActiveConfigVersion != baseline {
		return config.Snapshot{}, config.ErrReleaseConflict
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, release := range r.releases {
		if release.State != config.ReleaseActive {
			continue
		}
		for _, target := range release.Targets {
			if target.TenantID == tenantID && target.BaselineConfigVersion == baseline && selected(release, target) {
				return cloneSnapshot(r.snapshots[tenantID][target.CandidateConfigVersion]), nil
			}
		}
	}
	return cloneSnapshot(r.snapshots[tenantID][baseline]), nil
}
func (r *Repository) ResolveExecutionBinding(ctx context.Context, tc tenant.Context) (tenant.ExecutionBinding, error) {
	if err := tc.Validate(); err != nil {
		return tenant.ExecutionBinding{}, err
	}
	current, err := r.tenants.Get(ctx, tc.TenantID)
	if err != nil {
		return tenant.ExecutionBinding{}, err
	}
	if current.Version != tc.TenantVersion || current.Status != tenant.StatusActive {
		return tenant.ExecutionBinding{}, runtime.ErrVersionMismatch
	}
	snapshot, err := r.SelectEffective(ctx, tc.TenantID, current.ActiveConfigVersion)
	return r.resolveExecutionBinding(ctx, tc, current, snapshot, err)
}

func (r *Repository) ResolveExecutionBindingAt(ctx context.Context, tc tenant.Context, version int64) (tenant.ExecutionBinding, error) {
	if err := tc.Validate(); err != nil || version < 1 {
		return tenant.ExecutionBinding{}, config.ErrInvalid
	}
	current, err := r.tenants.Get(ctx, tc.TenantID)
	if err != nil {
		return tenant.ExecutionBinding{}, err
	}
	snapshot, getErr := r.Get(ctx, tc.TenantID, version)
	return r.resolveExecutionBinding(ctx, tc, current, snapshot, getErr)
}

func (r *Repository) resolveExecutionBinding(ctx context.Context, tc tenant.Context, current tenant.Tenant, snapshot config.Snapshot, getErr error) (tenant.ExecutionBinding, error) {
	if err := tc.Validate(); err != nil {
		return tenant.ExecutionBinding{}, err
	}
	if current.Version != tc.TenantVersion || current.Status != tenant.StatusActive {
		return tenant.ExecutionBinding{}, runtime.ErrVersionMismatch
	}
	if getErr != nil {
		return tenant.ExecutionBinding{}, getErr
	}
	allowed := tc.AgentAppID == snapshot.Payload.DefaultAgentAppID && tc.TrustedSource == "authenticated_api"
	for _, binding := range snapshot.Payload.ChannelBindings {
		if binding.AgentAppID == tc.AgentAppID && binding.Channel == tc.Channel && tc.TrustedSource == "channel_binding:"+binding.BindingID {
			allowed = true
			break
		}
	}
	if !allowed {
		return tenant.ExecutionBinding{}, config.ErrTenantScope
	}
	app, err := r.apps.Get(ctx, tc.TenantID, tc.AgentAppID)
	if err != nil {
		return tenant.ExecutionBinding{}, err
	}
	if app.Status != agentapp.StatusActive || app.CurrentRevision < 1 {
		return tenant.ExecutionBinding{}, config.ErrInvalid
	}
	revision, err := r.apps.GetRevision(ctx, tc.TenantID, tc.AgentAppID, app.CurrentRevision)
	if err != nil {
		return tenant.ExecutionBinding{}, err
	}
	result := tenant.ExecutionBinding{AgentAppVersion: app.Version, AgentAppRevision: revision.Revision, AgentContentDigest: revision.ContentDigest, ConfigVersion: snapshot.ConfigVersion, PolicyVersion: snapshot.Payload.PolicyVersion,
		ExecutionBudget: runtime.ExecutionBudget{MaxLLMCalls: revision.ExecutionBudget.MaxLLMCalls, MaxToolCalls: revision.ExecutionBudget.MaxToolCalls, MaxParallelTools: revision.ExecutionBudget.MaxParallelTools, ExecutionTimeoutSeconds: revision.ExecutionBudget.ExecutionTimeoutSeconds}}
	return result, result.Validate()
}

func validReleaseCreate(in config.ReleaseCreateInput) error {
	if err := in.Metadata.Validate(); err != nil || in.ReleaseID == "" || in.Salt == "" || in.Percentage < 0 || in.Percentage > 100 || len(in.Targets) == 0 {
		return config.ErrInvalid
	}
	seen := map[string]struct{}{}
	for _, target := range in.Targets {
		if target.TenantID == "" || target.BaselineConfigVersion < 1 || target.CandidateConfigVersion < 1 || target.BaselineConfigVersion == target.CandidateConfigVersion {
			return config.ErrInvalid
		}
		if _, ok := seen[target.TenantID]; ok {
			return config.ErrInvalid
		}
		seen[target.TenantID] = struct{}{}
	}
	return nil
}

func selected(release config.Release, target config.ReleaseTarget) bool {
	if target.Allowlisted {
		return true
	}
	sum := sha256.Sum256([]byte(release.ReleaseID + "\x00" + target.TenantID + "\x00" + release.Salt))
	return int(binary.BigEndian.Uint64(sum[:8])%100) < release.Percentage
}
func clonePayload(in config.ConfigV1) config.ConfigV1 {
	return config.NormalizeV1(in)
}
func cloneSnapshot(in config.Snapshot) config.Snapshot {
	out := in
	out.Payload = clonePayload(in.Payload)
	return out
}

func cloneRelease(in config.Release) config.Release {
	out := in
	out.Targets = append([]config.ReleaseTarget(nil), in.Targets...)
	return out
}

var _ config.Repository = (*Repository)(nil)
