package configcontrol

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
)

type memoryRevision struct {
	revision  Revision
	canonical []byte
}

type memoryRelease struct {
	release Release
	nodes   map[string]ReleaseNode
}

// MemoryStore is a strict, mutex-protected control plane for local/demo and
// unit-test use. It is deliberately not used as a fallback for production
// PostgreSQL mode.
type MemoryStore struct {
	mu           sync.RWMutex
	revisions    map[string]map[string]memoryRevision
	states       map[string]TenantState
	releases     map[string]*memoryRelease
	heartbeats   map[string]NodeHeartbeat
	heartbeatTTL time.Duration
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		revisions:    make(map[string]map[string]memoryRevision),
		states:       make(map[string]TenantState),
		releases:     make(map[string]*memoryRelease),
		heartbeats:   make(map[string]NodeHeartbeat),
		heartbeatTTL: 30 * time.Second,
	}
}

func (s *MemoryStore) SetHeartbeatTTL(ttl time.Duration) {
	if ttl > 0 {
		s.mu.Lock()
		s.heartbeatTTL = ttl
		s.mu.Unlock()
	}
}

func (s *MemoryStore) Close() {}

func (s *MemoryStore) Bootstrap(_ context.Context, tenants []config.TenantConfig, actor, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.states) != 0 {
		return nil
	}
	if len(tenants) == 0 {
		return errors.New("config control: bootstrap requires at least one tenant")
	}
	now := time.Now().UTC()
	for _, tenant := range tenants {
		if tenant.TenantID == "" || tenant.Version == "" {
			return errors.New("config control: bootstrap tenant requires tenant_id and version")
		}
		if s.revisions[tenant.TenantID] == nil {
			s.revisions[tenant.TenantID] = make(map[string]memoryRevision)
		}
		rev, err := makeRevision(RevisionInput{
			TenantID: tenant.TenantID, Revision: tenant.Version, Tenant: tenant,
			CreatedBy: actor, ChangeReason: reason,
		}, now)
		if err != nil {
			return err
		}
		s.revisions[tenant.TenantID][tenant.Version] = memoryRevision{revision: rev, canonical: canonicalConfig(rev.Tenant)}
		s.states[tenant.TenantID] = TenantState{
			TenantID: tenant.TenantID, ActiveRevision: tenant.Version,
			Generation: 1, UpdatedBy: actor, UpdatedAt: now,
		}
	}
	return nil
}

func (s *MemoryStore) EnsureTenant(_ context.Context, tenant config.TenantConfig, actor, reason string) (TenantState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state, ok := s.states[tenant.TenantID]; ok {
		return state, nil
	}
	now := time.Now().UTC()
	revision, err := s.putRevisionLocked(RevisionInput{TenantID: tenant.TenantID, Revision: tenant.Version, Tenant: tenant, CreatedBy: actor, ChangeReason: reason}, now)
	if err != nil {
		return TenantState{}, err
	}
	state := TenantState{TenantID: revision.TenantID, ActiveRevision: revision.Revision, Generation: 1, UpdatedBy: actor, UpdatedAt: now}
	s.states[tenant.TenantID] = state
	return state, nil
}

func (s *MemoryStore) PutRevision(_ context.Context, input RevisionInput) (Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	return s.putRevisionLocked(input, now)
}

func (s *MemoryStore) putRevisionLocked(input RevisionInput, now time.Time) (Revision, error) {
	if input.TenantID == "" {
		input.TenantID = input.Tenant.TenantID
	}
	if input.Revision == "" {
		input.Revision = input.Tenant.Version
	}
	if input.TenantID == "" || input.Revision == "" {
		return Revision{}, errors.New("config control: tenant_id and revision are required")
	}
	input.Tenant.TenantID = input.TenantID
	input.Tenant.Version = input.Revision
	rev, err := makeRevision(input, now)
	if err != nil {
		return Revision{}, err
	}
	if s.revisions[input.TenantID] == nil {
		s.revisions[input.TenantID] = make(map[string]memoryRevision)
	}
	if existing, ok := s.revisions[input.TenantID][input.Revision]; ok {
		if existing.revision.ConfigSHA256 != rev.ConfigSHA256 {
			return Revision{}, fmt.Errorf("%w: tenant %s revision %s", ErrRevisionConflict, input.TenantID, input.Revision)
		}
		return cloneRevision(existing.revision), nil
	}
	s.revisions[input.TenantID][input.Revision] = memoryRevision{revision: rev, canonical: canonicalConfig(rev.Tenant)}
	return cloneRevision(rev), nil
}

func (s *MemoryStore) GetRevision(_ context.Context, tenantID, revision string) (Revision, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.getRevisionLocked(tenantID, revision)
}

func (s *MemoryStore) getRevisionLocked(tenantID, revision string) (Revision, error) {
	byVersion := s.revisions[tenantID]
	if byVersion == nil {
		return Revision{}, ErrRevisionNotFound
	}
	rev, ok := byVersion[revision]
	if !ok {
		return Revision{}, ErrRevisionNotFound
	}
	return cloneRevision(rev.revision), nil
}

func (s *MemoryStore) ListRevisions(_ context.Context, tenantID string) ([]RevisionSummary, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	byVersion := s.revisions[tenantID]
	if byVersion == nil {
		return nil, ErrTenantNotFound
	}
	result := make([]RevisionSummary, 0, len(byVersion))
	for _, item := range byVersion {
		r := item.revision
		result = append(result, RevisionSummary{
			TenantID: r.TenantID, Revision: r.Revision, ConfigSHA256: r.ConfigSHA256,
			CreatedBy: r.CreatedBy, ChangeReason: r.ChangeReason, CreatedAt: r.CreatedAt,
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result, nil
}

func (s *MemoryStore) GetState(_ context.Context, tenantID string) (TenantState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.states[tenantID]
	if !ok {
		return TenantState{}, ErrTenantNotFound
	}
	return state, nil
}

func (s *MemoryStore) ListStates(_ context.Context) ([]TenantState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]TenantState, 0, len(s.states))
	for _, state := range s.states {
		result = append(result, state)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TenantID < result[j].TenantID })
	return result, nil
}

func (s *MemoryStore) CreateRelease(_ context.Context, request CreateReleaseRequest) (Release, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.createReleaseLocked(request, time.Now().UTC())
}

func (s *MemoryStore) createReleaseLocked(request CreateReleaseRequest, now time.Time) (Release, error) {
	state, ok := s.states[request.TenantID]
	if !ok {
		return Release{}, ErrTenantNotFound
	}
	if request.ExpectedGeneration > 0 && request.ExpectedGeneration != state.Generation {
		return Release{}, fmt.Errorf("%w: expected %d, current %d", ErrGenerationConflict, request.ExpectedGeneration, state.Generation)
	}
	if request.ExpectedActive != "" && request.ExpectedActive != state.ActiveRevision {
		return Release{}, ErrGenerationConflict
	}
	if request.ExpectedCanary != "" && request.ExpectedCanary != state.CanaryRevision {
		return Release{}, ErrGenerationConflict
	}
	if request.TargetRevision == "" {
		return Release{}, errors.New("config control: target revision is required")
	}
	if _, err := s.getRevisionLocked(request.TenantID, request.TargetRevision); err != nil {
		return Release{}, err
	}
	switch request.Kind {
	case ReleaseFull, ReleaseCanary, ReleasePromote, ReleaseRollback:
	default:
		return Release{}, fmt.Errorf("config control: unsupported release kind %q", request.Kind)
	}
	if request.Kind == ReleasePromote && (state.CanaryRevision == "" || request.TargetRevision != state.CanaryRevision) {
		return Release{}, ErrInvalidTransition
	}
	if request.TargetRolloutPercent < 0 || request.TargetRolloutPercent > 100 {
		return Release{}, errors.New("config control: rollout percent must be between 0 and 100")
	}
	for _, existing := range s.releases {
		if existing.release.TenantID == request.TenantID &&
			(existing.release.Status == ReleasePreparing || existing.release.Status == ReleaseActivePending) {
			return Release{}, fmt.Errorf("%w: release %s is still pending", ErrReleaseConflict, existing.release.ReleaseID)
		}
	}
	release := Release{
		ReleaseID: uuid.NewString(), TenantID: request.TenantID, Kind: request.Kind,
		SourceActiveRevision: state.ActiveRevision, SourceCanaryRevision: state.CanaryRevision,
		TargetRevision: request.TargetRevision, TargetRolloutPercent: request.TargetRolloutPercent,
		ExpectedGeneration: state.Generation, RequestedBy: request.RequestedBy,
		ChangeReason: request.ChangeReason, Status: ReleasePreparing, CreatedAt: now,
	}
	mr := &memoryRelease{release: release, nodes: make(map[string]ReleaseNode)}
	for key, heartbeat := range s.heartbeats {
		if heartbeat.LastSeenAt.Add(s.heartbeatTTL).Before(now) {
			continue
		}
		mr.nodes[key] = ReleaseNode{ReleaseID: release.ReleaseID, NodeID: heartbeat.NodeID, BootID: heartbeat.BootID, Status: NodePending, UpdatedAt: now}
	}
	if len(mr.nodes) == 0 {
		// A release without a live node is intentionally pending. The controller
		// may retry after a heartbeat arrives, but must never claim verified.
	}
	s.releases[release.ReleaseID] = mr
	return cloneRelease(release, mr.nodes), nil
}

func (s *MemoryStore) GetRelease(_ context.Context, releaseID string) (Release, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	mr, ok := s.releases[releaseID]
	if !ok {
		return Release{}, ErrReleaseNotFound
	}
	return cloneRelease(mr.release, mr.nodes), nil
}

func (s *MemoryStore) ListReleases(_ context.Context, tenantID string) ([]Release, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Release, 0)
	for _, item := range s.releases {
		if item.release.TenantID == tenantID {
			result = append(result, cloneRelease(item.release, item.nodes))
		}
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].CreatedAt.Equal(result[j].CreatedAt) {
			return result[i].ReleaseID < result[j].ReleaseID
		}
		return result[i].CreatedAt.Before(result[j].CreatedAt)
	})
	return result, nil
}

func (s *MemoryStore) ListPendingReleases(_ context.Context) ([]Release, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Release, 0)
	for _, item := range s.releases {
		if item.release.Status == ReleasePreparing || item.release.Status == ReleaseActivePending {
			result = append(result, cloneRelease(item.release, item.nodes))
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].CreatedAt.Before(result[j].CreatedAt) })
	return result, nil
}

func (s *MemoryStore) AckPrepared(_ context.Context, releaseID string, node NodeIdentity, loadedRevision string, loadedGeneration int64, ackErr error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	mr, ok := s.releases[releaseID]
	if !ok {
		return ErrReleaseNotFound
	}
	key := nodeKey(node.NodeID, node.BootID)
	record, ok := mr.nodes[key]
	if !ok {
		return ErrNodeNotRegistered
	}
	now := time.Now().UTC()
	if ackErr != nil {
		record.Status = NodeFailed
		record.ErrorMessage = safeError(ackErr)
		record.UpdatedAt = now
		mr.nodes[key] = record
		mr.release.Status = ReleaseFailed
		mr.release.ErrorMessage = record.ErrorMessage
		failed := now
		mr.release.FailedAt = &failed
		return nil
	}
	if mr.release.Status != ReleasePreparing {
		if record.Status == NodePrepared || record.Status == NodeApplied {
			return nil
		}
		return ErrInvalidTransition
	}
	if record.Status == NodePrepared || record.Status == NodeApplied {
		return nil
	}
	record.Status = NodePrepared
	record.LoadedRevision = loadedRevision
	record.LoadedGeneration = loadedGeneration
	record.PreparedAt = &now
	record.UpdatedAt = now
	mr.nodes[key] = record
	return nil
}

func (s *MemoryStore) ActivateIfReady(_ context.Context, releaseID string) (Release, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mr, ok := s.releases[releaseID]
	if !ok {
		return Release{}, ErrReleaseNotFound
	}
	if mr.release.Status == ReleaseActivePending || mr.release.Status == ReleaseVerified {
		return cloneRelease(mr.release, mr.nodes), nil
	}
	if mr.release.Status != ReleasePreparing {
		return Release{}, ErrInvalidTransition
	}
	if len(mr.nodes) == 0 {
		return cloneRelease(mr.release, mr.nodes), nil
	}
	for _, node := range mr.nodes {
		if node.Status != NodePrepared {
			return cloneRelease(mr.release, mr.nodes), nil
		}
	}
	state, ok := s.states[mr.release.TenantID]
	if !ok {
		return Release{}, ErrTenantNotFound
	}
	if state.Generation != mr.release.ExpectedGeneration || state.ActiveRevision != mr.release.SourceActiveRevision || state.CanaryRevision != mr.release.SourceCanaryRevision {
		mr.release.Status = ReleaseFailed
		mr.release.ErrorMessage = ErrGenerationConflict.Error()
		now := time.Now().UTC()
		mr.release.FailedAt = &now
		return cloneRelease(mr.release, mr.nodes), ErrGenerationConflict
	}
	switch mr.release.Kind {
	case ReleaseCanary:
		state.CanaryRevision = mr.release.TargetRevision
		state.RolloutPercent = mr.release.TargetRolloutPercent
	case ReleasePromote:
		state.ActiveRevision = mr.release.TargetRevision
		state.CanaryRevision = ""
		state.RolloutPercent = 0
	case ReleaseFull, ReleaseRollback:
		state.ActiveRevision = mr.release.TargetRevision
		state.CanaryRevision = ""
		state.RolloutPercent = 0
	}
	state.Generation++
	state.UpdatedBy = mr.release.RequestedBy
	state.UpdatedAt = time.Now().UTC()
	s.states[state.TenantID] = state
	mr.release.ResultingGeneration = state.Generation
	mr.release.Status = ReleaseActivePending
	now := time.Now().UTC()
	mr.release.ActivatedAt = &now
	return cloneRelease(mr.release, mr.nodes), nil
}

func (s *MemoryStore) AckApplied(_ context.Context, releaseID string, node NodeIdentity, loadedRevision string, loadedGeneration int64, ackErr error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	mr, ok := s.releases[releaseID]
	if !ok {
		return ErrReleaseNotFound
	}
	key := nodeKey(node.NodeID, node.BootID)
	record, ok := mr.nodes[key]
	if !ok {
		return ErrNodeNotRegistered
	}
	now := time.Now().UTC()
	if ackErr != nil {
		record.Status = NodeFailed
		record.ErrorMessage = safeError(ackErr)
		record.UpdatedAt = now
		mr.nodes[key] = record
		if mr.release.Status == ReleaseActivePending {
			mr.release.Status = ReleaseFailed
			mr.release.ErrorMessage = record.ErrorMessage
			failed := now
			mr.release.FailedAt = &failed
		}
		return nil
	}
	if mr.release.Status == ReleaseVerified {
		if record.Status == NodeApplied {
			return nil
		}
		return ErrInvalidTransition
	}
	if mr.release.Status != ReleaseActivePending || loadedGeneration != mr.release.ResultingGeneration {
		return ErrInvalidTransition
	}
	if record.Status == NodeApplied {
		return nil
	}
	record.Status = NodeApplied
	record.LoadedRevision = loadedRevision
	record.LoadedGeneration = loadedGeneration
	record.AppliedAt = &now
	record.UpdatedAt = now
	mr.nodes[key] = record
	for _, node := range mr.nodes {
		if node.Status != NodeApplied {
			return nil
		}
	}
	mr.release.Status = ReleaseVerified
	verified := now
	mr.release.VerifiedAt = &verified
	return nil
}

func (s *MemoryStore) FailRelease(_ context.Context, releaseID, message string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	mr, ok := s.releases[releaseID]
	if !ok {
		return ErrReleaseNotFound
	}
	if mr.release.Status == ReleaseVerified {
		return ErrInvalidTransition
	}
	mr.release.Status = ReleaseFailed
	mr.release.ErrorMessage = message
	now := time.Now().UTC()
	mr.release.FailedAt = &now
	return nil
}

func (s *MemoryStore) Heartbeat(_ context.Context, heartbeat NodeHeartbeat) error {
	if heartbeat.NodeID == "" || heartbeat.BootID == "" {
		return errors.New("config control: node_id and boot_id are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	heartbeat.LastSeenAt = now
	heartbeat.UpdatedAt = now
	s.heartbeats[nodeKey(heartbeat.NodeID, heartbeat.BootID)] = heartbeat
	return nil
}

func (s *MemoryStore) ListHeartbeats(_ context.Context) ([]NodeHeartbeat, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now().UTC()
	result := make([]NodeHeartbeat, 0, len(s.heartbeats))
	for _, heartbeat := range s.heartbeats {
		if heartbeat.LastSeenAt.Add(s.heartbeatTTL).Before(now) {
			continue
		}
		result = append(result, heartbeat)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].NodeID == result[j].NodeID {
			return result[i].BootID < result[j].BootID
		}
		return result[i].NodeID < result[j].NodeID
	})
	return result, nil
}

func makeRevision(input RevisionInput, now time.Time) (Revision, error) {
	input.Tenant.TenantID = input.TenantID
	input.Tenant.Version = input.Revision
	input.Tenant = cloneTenantConfig(input.Tenant)
	canonical, err := json.Marshal(input.Tenant)
	if err != nil {
		return Revision{}, fmt.Errorf("config control: encode revision: %w", err)
	}
	digest := sha256.Sum256(canonical)
	return Revision{
		TenantID: input.TenantID, Revision: input.Revision, Tenant: input.Tenant,
		ConfigSHA256: hex.EncodeToString(digest[:]), CreatedBy: input.CreatedBy,
		ChangeReason: input.ChangeReason, CreatedAt: now,
	}, nil
}

func canonicalConfig(t config.TenantConfig) []byte {
	data, _ := json.Marshal(t)
	return data
}

func cloneRevision(revision Revision) Revision {
	revision.Tenant = cloneTenantConfig(revision.Tenant)
	return revision
}

func cloneTenantConfig(tenant config.TenantConfig) config.TenantConfig {
	tenant.Skills.Allow = append([]string(nil), tenant.Skills.Allow...)
	tenant.Channels = append([]config.ChannelConfig(nil), tenant.Channels...)
	for i := range tenant.Channels {
		tenant.Channels[i].AllowedUsers = append([]string(nil), tenant.Channels[i].AllowedUsers...)
	}
	tenant.Tools.Allow = append([]string(nil), tenant.Tools.Allow...)
	tenant.Tools.Deny = append([]string(nil), tenant.Tools.Deny...)
	tenant.Tools.RequireConfirm = append([]string(nil), tenant.Tools.RequireConfirm...)
	tenant.Tools.SideEffects = append([]string(nil), tenant.Tools.SideEffects...)
	tenant.Audit.RedactPatterns = append([]string(nil), tenant.Audit.RedactPatterns...)
	return tenant
}

func cloneRelease(release Release, nodes map[string]ReleaseNode) Release {
	release.Nodes = make([]ReleaseNode, 0, len(nodes))
	for _, node := range nodes {
		release.Nodes = append(release.Nodes, node)
	}
	sort.Slice(release.Nodes, func(i, j int) bool {
		if release.Nodes[i].NodeID == release.Nodes[j].NodeID {
			return release.Nodes[i].BootID < release.Nodes[j].BootID
		}
		return release.Nodes[i].NodeID < release.Nodes[j].NodeID
	})
	return release
}

func nodeKey(nodeID, bootID string) string { return nodeID + "\x1f" + bootID }

func safeError(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	if len(message) > 512 {
		message = message[:512]
	}
	return message
}
