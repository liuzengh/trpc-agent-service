// Package inmemory provides a single-process Agent App repository for contract
// tests and local development. It is not a distributed authority.
package inmemory

import (
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
)

type appKey struct{ tenantID, appID string }
type revisionKey struct {
	appKey
	revision int64
}

type Repository struct {
	mu        sync.RWMutex
	apps      map[appKey]agentapp.AgentApp
	appKeys   map[string]appKey
	revisions map[revisionKey]agentapp.Revision
}

func New() *Repository {
	return &Repository{apps: make(map[appKey]agentapp.AgentApp), appKeys: make(map[string]appKey), revisions: make(map[revisionKey]agentapp.Revision)}
}

func (r *Repository) Create(ctx context.Context, in agentapp.CreateInput) (agentapp.AgentApp, error) {
	if err := ctx.Err(); err != nil {
		return agentapp.AgentApp{}, err
	}
	if err := in.ChangeMetadata.Validate(); err != nil {
		return agentapp.AgentApp{}, err
	}
	a := in.App
	if a.TenantID == "" || a.AgentAppID == "" || a.AgentAppKey == "" || a.DisplayName == "" {
		return agentapp.AgentApp{}, agentapp.ErrInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	k := appKey{a.TenantID, a.AgentAppID}
	if _, ok := r.apps[k]; ok {
		return agentapp.AgentApp{}, agentapp.ErrVersionConflict
	}
	keyScope := a.TenantID + "\x00" + a.AgentAppKey
	if _, ok := r.appKeys[keyScope]; ok {
		return agentapp.AgentApp{}, agentapp.ErrVersionConflict
	}
	now := time.Now().UTC()
	a.Status = agentapp.StatusDraft
	a.Version = 1
	a.NextRevision = 1
	a.CreatedAt = now
	a.UpdatedAt = now
	r.apps[k] = a
	r.appKeys[keyScope] = k
	return a, nil
}

func (r *Repository) Get(ctx context.Context, tenantID, appID string) (agentapp.AgentApp, error) {
	if err := ctx.Err(); err != nil {
		return agentapp.AgentApp{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.apps[appKey{tenantID, appID}]
	if !ok {
		return agentapp.AgentApp{}, agentapp.ErrNotFound
	}
	return a, nil
}

func (r *Repository) CreateDraft(ctx context.Context, in agentapp.CreateDraftInput) (agentapp.Revision, error) {
	if err := ctx.Err(); err != nil {
		return agentapp.Revision{}, err
	}
	if err := in.ChangeMetadata.Validate(); err != nil {
		return agentapp.Revision{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	ak := appKey{in.TenantID, in.AgentAppID}
	a, ok := r.apps[ak]
	if !ok {
		return agentapp.Revision{}, agentapp.ErrNotFound
	}
	if a.Version != in.ExpectedAppVersion {
		return agentapp.Revision{}, agentapp.ErrVersionConflict
	}
	if a.Status == agentapp.StatusDisabled {
		return agentapp.Revision{}, agentapp.ErrStatusConflict
	}
	rev := agentapp.NormalizeRevision(in.Revision)
	rev.TenantID = in.TenantID
	rev.AgentAppID = in.AgentAppID
	rev.Revision = a.NextRevision
	rev.State = agentapp.RevisionDraft
	rev.DraftVersion = 1
	rev.SchemaVersion = 1
	if err := rev.ValidateDraft(); err != nil {
		return agentapp.Revision{}, err
	}
	if err := r.validateChildRefsLocked(rev); err != nil {
		return agentapp.Revision{}, err
	}
	now := time.Now().UTC()
	rev.CreatedAt = now
	rev.UpdatedAt = now
	a.NextRevision++
	a.Version++
	a.UpdatedAt = now
	r.apps[ak] = a
	r.revisions[revisionKey{ak, rev.Revision}] = rev
	return copyRevision(rev), nil
}

func (r *Repository) UpdateDraft(ctx context.Context, in agentapp.UpdateDraftInput) (agentapp.Revision, error) {
	if err := ctx.Err(); err != nil {
		return agentapp.Revision{}, err
	}
	if err := in.ChangeMetadata.Validate(); err != nil {
		return agentapp.Revision{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rev := agentapp.NormalizeRevision(in.Revision)
	k := revisionKey{appKey{rev.TenantID, rev.AgentAppID}, rev.Revision}
	old, ok := r.revisions[k]
	if !ok {
		return agentapp.Revision{}, agentapp.ErrNotFound
	}
	if old.State != agentapp.RevisionDraft {
		return agentapp.Revision{}, agentapp.ErrImmutable
	}
	if old.DraftVersion != in.ExpectedDraftVersion {
		return agentapp.Revision{}, agentapp.ErrVersionConflict
	}
	rev.State = agentapp.RevisionDraft
	rev.DraftVersion = old.DraftVersion + 1
	rev.SchemaVersion = old.SchemaVersion
	rev.CreatedAt = old.CreatedAt
	rev.UpdatedAt = time.Now().UTC()
	if err := rev.ValidateDraft(); err != nil {
		return agentapp.Revision{}, err
	}
	if err := r.validateChildRefsLocked(rev); err != nil {
		return agentapp.Revision{}, err
	}
	r.revisions[k] = rev
	return copyRevision(rev), nil
}

func (r *Repository) validateChildRefsLocked(rev agentapp.Revision) error {
	source := revisionKey{appKey{rev.TenantID, rev.AgentAppID}, rev.Revision}
	for _, node := range rev.AgentSpec.Nodes {
		targetKey := revisionKey{appKey{rev.TenantID, node.AgentRef.AgentAppID}, node.AgentRef.Revision}
		target, ok := r.revisions[targetKey]
		if !ok || target.State != agentapp.RevisionPublished || target.ContentDigest != node.AgentRef.ContentDigest {
			return agentapp.ErrInvalid
		}
		if targetKey == source || r.reachesLocked(targetKey, source, make(map[revisionKey]bool)) {
			return agentapp.ErrInvalid
		}
	}
	return nil
}

func (r *Repository) reachesLocked(current, target revisionKey, seen map[revisionKey]bool) bool {
	if current == target {
		return true
	}
	if seen[current] {
		return false
	}
	seen[current] = true
	revision, ok := r.revisions[current]
	if !ok {
		return false
	}
	for _, node := range revision.AgentSpec.Nodes {
		next := revisionKey{appKey{revision.TenantID, node.AgentRef.AgentAppID}, node.AgentRef.Revision}
		if r.reachesLocked(next, target, seen) {
			return true
		}
	}
	return false
}

func (r *Repository) GetRevision(ctx context.Context, tenantID, appID string, revision int64) (agentapp.Revision, error) {
	if err := ctx.Err(); err != nil {
		return agentapp.Revision{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	rev, ok := r.revisions[revisionKey{appKey{tenantID, appID}, revision}]
	if !ok {
		return agentapp.Revision{}, agentapp.ErrNotFound
	}
	return copyRevision(rev), nil
}

func (r *Repository) Publish(ctx context.Context, in agentapp.PublishInput) (agentapp.PublishResult, error) {
	if err := ctx.Err(); err != nil {
		return agentapp.PublishResult{}, err
	}
	if err := in.ChangeMetadata.Validate(); err != nil {
		return agentapp.PublishResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	ak := appKey{in.TenantID, in.AgentAppID}
	a, ok := r.apps[ak]
	if !ok {
		return agentapp.PublishResult{}, agentapp.ErrNotFound
	}
	if a.Version != in.ExpectedAppVersion {
		return agentapp.PublishResult{}, agentapp.ErrVersionConflict
	}
	rk := revisionKey{ak, in.Revision}
	rev, ok := r.revisions[rk]
	if !ok {
		return agentapp.PublishResult{}, agentapp.ErrNotFound
	}
	if rev.State != agentapp.RevisionDraft {
		return agentapp.PublishResult{}, agentapp.ErrImmutable
	}
	if rev.DraftVersion != in.ExpectedDraftVersion {
		return agentapp.PublishResult{}, agentapp.ErrVersionConflict
	}
	if err := rev.ValidateDraft(); err != nil {
		return agentapp.PublishResult{}, err
	}
	digest, err := rev.ComputeContentDigest()
	if err != nil {
		return agentapp.PublishResult{}, err
	}
	now := time.Now().UTC()
	rev.State = agentapp.RevisionPublished
	rev.ContentDigest = digest
	rev.PublishedAt = &now
	rev.UpdatedAt = now
	a.CurrentRevision = rev.Revision
	a.Version++
	a.UpdatedAt = now
	if a.Status == agentapp.StatusDraft {
		a.Status = agentapp.StatusActive
	}
	r.revisions[rk] = rev
	r.apps[ak] = a
	return agentapp.PublishResult{App: a, Revision: copyRevision(rev)}, nil
}

func (r *Repository) Rollback(ctx context.Context, in agentapp.RollbackInput) (agentapp.RollbackResult, error) {
	if err := ctx.Err(); err != nil {
		return agentapp.RollbackResult{}, err
	}
	if err := in.ChangeMetadata.Validate(); err != nil {
		return agentapp.RollbackResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	ak := appKey{in.TenantID, in.AgentAppID}
	a, ok := r.apps[ak]
	if !ok {
		return agentapp.RollbackResult{}, agentapp.ErrNotFound
	}
	if a.Version != in.ExpectedAppVersion {
		return agentapp.RollbackResult{}, agentapp.ErrVersionConflict
	}
	rev, ok := r.revisions[revisionKey{ak, in.TargetRevision}]
	if !ok || rev.State != agentapp.RevisionPublished {
		return agentapp.RollbackResult{}, agentapp.ErrStatusConflict
	}
	a.CurrentRevision = in.TargetRevision
	a.Version++
	a.UpdatedAt = time.Now().UTC()
	r.apps[ak] = a
	return agentapp.RollbackResult{App: a}, nil
}

func (r *Repository) TransitionStatus(ctx context.Context, in agentapp.TransitionStatusInput) (agentapp.ChangeResult, error) {
	if err := ctx.Err(); err != nil {
		return agentapp.ChangeResult{}, err
	}
	if err := in.ChangeMetadata.Validate(); err != nil {
		return agentapp.ChangeResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	ak := appKey{in.TenantID, in.AgentAppID}
	a, ok := r.apps[ak]
	if !ok {
		return agentapp.ChangeResult{}, agentapp.ErrNotFound
	}
	if a.Version != in.ExpectedAppVersion {
		return agentapp.ChangeResult{}, agentapp.ErrVersionConflict
	}
	if !legalTransition(a.Status, in.NextStatus) {
		return agentapp.ChangeResult{}, agentapp.ErrStatusConflict
	}
	a.Status = in.NextStatus
	a.Version++
	a.UpdatedAt = time.Now().UTC()
	r.apps[ak] = a
	return agentapp.ChangeResult{App: a}, nil
}

func legalTransition(from, to agentapp.Status) bool {
	return (from == agentapp.StatusActive && (to == agentapp.StatusSuspended || to == agentapp.StatusDisabled)) ||
		(from == agentapp.StatusSuspended && (to == agentapp.StatusActive || to == agentapp.StatusDisabled)) ||
		(from == agentapp.StatusDraft && to == agentapp.StatusDisabled)
}

// JSON round-tripping gives callers defensive copies of maps and slices.
func copyRevision(in agentapp.Revision) agentapp.Revision {
	b, _ := jsonMarshal(in)
	var out agentapp.Revision
	_ = jsonUnmarshal(b, &out)
	return out
}
