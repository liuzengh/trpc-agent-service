// Package tenant defines the multi-tenant control-plane configuration contract.
package tenant

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

var (
	// ErrNotFound indicates that a requested configuration snapshot or binding
	// does not exist in the control plane.
	ErrNotFound = errors.New("tenant configuration not found")
	// ErrVersionConflict indicates a configuration publication that does not
	// advance the immutable version sequence for one tenant application.
	ErrVersionConflict = errors.New("tenant configuration version conflict")
	// ErrBindingConflict indicates that an active external channel binding is
	// already owned by another tenant application.
	ErrBindingConflict   = errors.New("channel binding conflict")
	ErrCandidateNotFound = errors.New("application candidate not found")
	ErrRolloutInProgress = errors.New("application rollout is in progress")
)

// Snapshot is one immutable, checksummed tenant configuration version.
type Snapshot struct {
	Config      config.TenantConfig
	Checksum    string
	PublishedAt time.Time
}

// Repository exposes the control-plane queries required by request routing.
// Implementations must return independent snapshot copies so callers cannot
// mutate a version that is already published.
type Repository interface {
	GetActive(ctx context.Context, tenantID, appCode string) (Snapshot, error)
	GetCandidate(ctx context.Context, tenantID, appCode string) (Snapshot, error)
	GetVersion(ctx context.Context, tenantID, appCode string, version uint64) (Snapshot, error)
	ListVersions(ctx context.Context, tenantID, appCode string, limit int) ([]Snapshot, error)
	ResolveBinding(ctx context.Context, channel channels.Channel, bindingID string) (Snapshot, error)
	ListApplications(ctx context.Context, tenantID string) ([]Snapshot, error)
	Publish(ctx context.Context, tenantConfig config.TenantConfig) (Snapshot, error)
	Stage(ctx context.Context, tenantConfig config.TenantConfig) (Snapshot, error)
	GetRollout(ctx context.Context, tenantID, appCode string) (RolloutPolicy, error)
	SetRollout(ctx context.Context, tenantID, appCode string, update RolloutUpdate) (RolloutPolicy, error)
	StopRollout(ctx context.Context, tenantID, appCode string, expectedGeneration uint64) error
	DiscardCandidate(ctx context.Context, tenantID, appCode string, expectedVersion uint64) error
	PromoteCandidate(ctx context.Context, tenantID, appCode string, expectedVersion, expectedGeneration uint64) (Snapshot, error)
}

// MemoryRepository is a process-local repository for deterministic tests and
// local development. It is deliberately injected, never global, and must not
// be used as the durable production control plane.
type MemoryRepository struct {
	mu sync.RWMutex

	activeByApplication    map[string]uint64
	candidateByApplication map[string]uint64
	latestByApplication    map[string]uint64
	snapshots              map[string]Snapshot
	applicationBindings    map[string]map[string]struct{}
	bindingOwners          map[string]string
	rollouts               map[string]RolloutPolicy
	rolloutGenerations     map[string]uint64
}

// NewMemoryRepository constructs an empty isolated repository instance.
func NewMemoryRepository() *MemoryRepository {
	return &MemoryRepository{
		activeByApplication:    make(map[string]uint64),
		candidateByApplication: make(map[string]uint64),
		latestByApplication:    make(map[string]uint64),
		snapshots:              make(map[string]Snapshot),
		applicationBindings:    make(map[string]map[string]struct{}),
		bindingOwners:          make(map[string]string),
		rollouts:               make(map[string]RolloutPolicy),
		rolloutGenerations:     make(map[string]uint64),
	}
}

// GetActive returns the active immutable snapshot for one tenant application.
func (r *MemoryRepository) GetActive(ctx context.Context, tenantID, appCode string) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	applicationKey, err := applicationKey(tenantID, appCode)
	if err != nil {
		return Snapshot{}, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	version, ok := r.activeByApplication[applicationKey]
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	snapshot, ok := r.snapshots[snapshotKey(applicationKey, version)]
	if !ok {
		return Snapshot{}, fmt.Errorf("active configuration snapshot missing for %q: %w", applicationKey, ErrNotFound)
	}
	return cloneSnapshot(snapshot)
}

func (r *MemoryRepository) GetCandidate(ctx context.Context, tenantID, appCode string) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	key, err := applicationKey(tenantID, appCode)
	if err != nil {
		return Snapshot{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	version := r.candidateByApplication[key]
	if version == 0 {
		return Snapshot{}, ErrCandidateNotFound
	}
	snapshot, ok := r.snapshots[snapshotKey(key, version)]
	if !ok {
		return Snapshot{}, fmt.Errorf("candidate configuration snapshot missing for %q: %w", key, ErrCandidateNotFound)
	}
	return cloneSnapshot(snapshot)
}

// GetVersion returns a specific immutable configuration version.
func (r *MemoryRepository) GetVersion(ctx context.Context, tenantID, appCode string, version uint64) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	applicationKey, err := applicationKey(tenantID, appCode)
	if err != nil {
		return Snapshot{}, err
	}
	if version == 0 {
		return Snapshot{}, fmt.Errorf("configuration version must be positive")
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshot, ok := r.snapshots[snapshotKey(applicationKey, version)]
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	return cloneSnapshot(snapshot)
}

func (r *MemoryRepository) ListVersions(ctx context.Context, tenantID, appCode string, limit int) ([]Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	applicationKey, err := applicationKey(tenantID, appCode)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 20
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	latest := r.latestByApplication[applicationKey]
	if latest == 0 {
		return nil, ErrNotFound
	}
	versions := make([]Snapshot, 0, limit)
	for version := latest; version > 0 && len(versions) < limit; version-- {
		if snapshot, ok := r.snapshots[snapshotKey(applicationKey, version)]; ok {
			cloned, cloneErr := cloneSnapshot(snapshot)
			if cloneErr != nil {
				return nil, cloneErr
			}
			versions = append(versions, cloned)
		}
	}
	return versions, nil
}

// ResolveBinding returns the active configuration that owns one external
// channel binding. The lookup key includes the channel to prevent accidental
// reuse of a Telegram identifier as a different IM identifier.
func (r *MemoryRepository) ResolveBinding(ctx context.Context, channel channels.Channel, bindingID string) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if !channel.Supported() {
		return Snapshot{}, fmt.Errorf("unsupported channel %q", channel)
	}
	bindingKey, err := externalBindingKey(string(channel), bindingID)
	if err != nil {
		return Snapshot{}, err
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	applicationKey, ok := r.bindingOwners[bindingKey]
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	version, ok := r.activeByApplication[applicationKey]
	if !ok {
		return Snapshot{}, fmt.Errorf("binding owner %q has no active configuration: %w", applicationKey, ErrNotFound)
	}
	snapshot, ok := r.snapshots[snapshotKey(applicationKey, version)]
	if !ok {
		return Snapshot{}, fmt.Errorf("binding owner %q has no active snapshot: %w", applicationKey, ErrNotFound)
	}
	if snapshot.Config.Status != config.AgentActive {
		return Snapshot{}, ErrNotFound
	}
	return cloneSnapshot(snapshot)
}

// Publish validates and atomically activates a new immutable configuration
// version. A version must strictly increase for its tenant application.
func (r *MemoryRepository) Publish(ctx context.Context, tenantConfig config.TenantConfig) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if err := validateTenantConfig(tenantConfig); err != nil {
		return Snapshot{}, err
	}

	applicationKey := tenantConfig.AppName()
	bindingKeys, err := bindingKeys(tenantConfig)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot, err := newSnapshot(tenantConfig, time.Now().UTC())
	if err != nil {
		return Snapshot{}, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.rollouts[applicationKey]; exists {
		return Snapshot{}, ErrRolloutInProgress
	}

	if latestVersion := r.latestByApplication[applicationKey]; tenantConfig.ConfigVersion <= latestVersion {
		return Snapshot{}, fmt.Errorf("%w: latest=%d requested=%d", ErrVersionConflict, latestVersion, tenantConfig.ConfigVersion)
	}
	for _, bindingKey := range bindingKeys {
		if owner, exists := r.bindingOwners[bindingKey]; exists && owner != applicationKey {
			return Snapshot{}, fmt.Errorf("%w: binding=%q owner=%q", ErrBindingConflict, bindingKey, owner)
		}
	}

	for previousBinding := range r.applicationBindings[applicationKey] {
		delete(r.bindingOwners, previousBinding)
	}
	nextBindings := make(map[string]struct{}, len(bindingKeys))
	for _, bindingKey := range bindingKeys {
		r.bindingOwners[bindingKey] = applicationKey
		nextBindings[bindingKey] = struct{}{}
	}
	r.applicationBindings[applicationKey] = nextBindings
	r.activeByApplication[applicationKey] = tenantConfig.ConfigVersion
	delete(r.candidateByApplication, applicationKey)
	r.latestByApplication[applicationKey] = tenantConfig.ConfigVersion
	r.snapshots[snapshotKey(applicationKey, tenantConfig.ConfigVersion)] = snapshot
	delete(r.rollouts, applicationKey)

	return cloneSnapshot(snapshot)
}

// Stage stores an immutable candidate without changing the active version or
// external bindings. It is the only path used to create a gray-release target.
func (r *MemoryRepository) Stage(ctx context.Context, tenantConfig config.TenantConfig) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	if err := validateTenantConfig(tenantConfig); err != nil {
		return Snapshot{}, err
	}
	applicationKey := tenantConfig.AppName()
	snapshot, err := newSnapshot(tenantConfig, time.Now().UTC())
	if err != nil {
		return Snapshot{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.rollouts[applicationKey]; exists {
		return Snapshot{}, ErrRolloutInProgress
	}
	if r.activeByApplication[applicationKey] == 0 {
		return Snapshot{}, ErrNotFound
	}
	latest := r.latestByApplication[applicationKey]
	if tenantConfig.ConfigVersion != latest+1 {
		return Snapshot{}, fmt.Errorf("%w: latest=%d requested=%d", ErrVersionConflict, latest, tenantConfig.ConfigVersion)
	}
	r.latestByApplication[applicationKey] = tenantConfig.ConfigVersion
	r.snapshots[snapshotKey(applicationKey, tenantConfig.ConfigVersion)] = snapshot
	r.candidateByApplication[applicationKey] = tenantConfig.ConfigVersion
	return cloneSnapshot(snapshot)
}

func (r *MemoryRepository) GetRollout(ctx context.Context, tenantID, appCode string) (RolloutPolicy, error) {
	if err := ctx.Err(); err != nil {
		return RolloutPolicy{}, err
	}
	key, err := applicationKey(tenantID, appCode)
	if err != nil {
		return RolloutPolicy{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	rollout, ok := r.rollouts[key]
	if !ok {
		return RolloutPolicy{}, ErrRolloutNotFound
	}
	rollout.TestUserIDs = append([]string(nil), rollout.TestUserIDs...)
	rollout.Ingresses = append([]string(nil), rollout.Ingresses...)
	return rollout, nil
}

func (r *MemoryRepository) SetRollout(ctx context.Context, tenantID, appCode string, update RolloutUpdate) (RolloutPolicy, error) {
	if err := ctx.Err(); err != nil {
		return RolloutPolicy{}, err
	}
	update, err := normalizeRolloutUpdate(update)
	if err != nil {
		return RolloutPolicy{}, err
	}
	key, err := applicationKey(tenantID, appCode)
	if err != nil {
		return RolloutPolicy{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	stableVersion := r.activeByApplication[key]
	if stableVersion == 0 {
		return RolloutPolicy{}, ErrNotFound
	}
	candidateVersion := r.candidateByApplication[key]
	if candidateVersion == 0 {
		return RolloutPolicy{}, ErrCandidateNotFound
	}
	stable, stableOK := r.snapshots[snapshotKey(key, stableVersion)]
	candidate, candidateOK := r.snapshots[snapshotKey(key, candidateVersion)]
	if !stableOK || !candidateOK {
		return RolloutPolicy{}, ErrNotFound
	}
	if candidate.Config.Status != config.AgentActive {
		return RolloutPolicy{}, errors.New("rollout candidate must be active")
	}
	if candidateVersion == stableVersion {
		return RolloutPolicy{}, errors.New("rollout candidate must differ from the stable version")
	}
	if !sameChannelTopology(stable.Config, candidate.Config) {
		return RolloutPolicy{}, errors.New("gray release cannot change channel bindings or credentials")
	}
	if err := validateRolloutIngresses(stable.Config, update.Ingresses); err != nil {
		return RolloutPolicy{}, err
	}
	current, exists := r.rollouts[key]
	if !exists {
		if update.ExpectedGeneration != 0 {
			return RolloutPolicy{}, ErrRolloutConflict
		}
		current.Generation = 0
	} else if current.Generation != update.ExpectedGeneration {
		return RolloutPolicy{}, ErrRolloutConflict
	}
	nextGeneration := r.rolloutGenerations[key] + 1
	if nextGeneration <= current.Generation {
		nextGeneration = current.Generation + 1
	}
	rollout := RolloutPolicy{
		TenantID: tenantID, AppCode: appCode, Generation: nextGeneration,
		StableVersion: stableVersion, CandidateVersion: candidateVersion, BasisPoints: update.BasisPoints,
		TestUserIDs: update.TestUserIDs, Ingresses: update.Ingresses, UpdatedAt: time.Now().UTC(),
	}
	r.rollouts[key] = rollout
	r.rolloutGenerations[key] = nextGeneration
	return rollout, nil
}

func (r *MemoryRepository) StopRollout(ctx context.Context, tenantID, appCode string, expectedGeneration uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := applicationKey(tenantID, appCode)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.rollouts[key]
	if !ok {
		return ErrRolloutNotFound
	}
	if current.Generation != expectedGeneration {
		return ErrRolloutConflict
	}
	delete(r.rollouts, key)
	return nil
}

func (r *MemoryRepository) DiscardCandidate(ctx context.Context, tenantID, appCode string, expectedVersion uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	key, err := applicationKey(tenantID, appCode)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.rollouts[key]; exists {
		return ErrRolloutInProgress
	}
	version := r.candidateByApplication[key]
	if version == 0 {
		return ErrCandidateNotFound
	}
	if expectedVersion == 0 || expectedVersion != version {
		return ErrVersionConflict
	}
	delete(r.candidateByApplication, key)
	return nil
}

func (r *MemoryRepository) PromoteCandidate(ctx context.Context, tenantID, appCode string, expectedVersion, expectedGeneration uint64) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	key, err := applicationKey(tenantID, appCode)
	if err != nil {
		return Snapshot{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	candidateVersion := r.candidateByApplication[key]
	if candidateVersion == 0 {
		return Snapshot{}, ErrCandidateNotFound
	}
	if expectedVersion == 0 || expectedVersion != candidateVersion {
		return Snapshot{}, ErrVersionConflict
	}
	rollout, hasRollout := r.rollouts[key]
	if hasRollout {
		if rollout.Generation != expectedGeneration || rollout.CandidateVersion != candidateVersion || r.activeByApplication[key] != rollout.StableVersion {
			return Snapshot{}, ErrRolloutConflict
		}
	} else if expectedGeneration != 0 {
		return Snapshot{}, ErrRolloutConflict
	}
	candidate, ok := r.snapshots[snapshotKey(key, candidateVersion)]
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	for previousBinding := range r.applicationBindings[key] {
		delete(r.bindingOwners, previousBinding)
	}
	bindingKeys, err := bindingKeys(candidate.Config)
	if err != nil {
		return Snapshot{}, err
	}
	nextBindings := make(map[string]struct{}, len(bindingKeys))
	for _, bindingKey := range bindingKeys {
		r.bindingOwners[bindingKey] = key
		nextBindings[bindingKey] = struct{}{}
	}
	r.applicationBindings[key] = nextBindings
	r.activeByApplication[key] = candidateVersion
	delete(r.candidateByApplication, key)
	delete(r.rollouts, key)
	return cloneSnapshot(candidate)
}

func validateTenantConfig(tenantConfig config.TenantConfig) error {
	return config.ValidateTenantStructure(tenantConfig)
}

func newSnapshot(tenantConfig config.TenantConfig, publishedAt time.Time) (Snapshot, error) {
	// Normalize empty channel lists to [] so the checksum always covers the
	// array form and API consumers never see a JSON null where an array belongs.
	if tenantConfig.Channels == nil {
		tenantConfig.Channels = []config.ChannelBinding{}
	}
	encoded, err := json.Marshal(tenantConfig)
	if err != nil {
		return Snapshot{}, fmt.Errorf("marshal tenant configuration: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return Snapshot{
		Config:      cloneTenantConfig(tenantConfig),
		Checksum:    hex.EncodeToString(sum[:]),
		PublishedAt: publishedAt,
	}, nil
}

func cloneSnapshot(snapshot Snapshot) (Snapshot, error) {
	cloned, err := newSnapshot(snapshot.Config, snapshot.PublishedAt)
	if err != nil {
		return Snapshot{}, err
	}
	if cloned.Checksum != snapshot.Checksum {
		return Snapshot{}, fmt.Errorf("configuration checksum mismatch")
	}
	return cloned, nil
}

func cloneTenantConfig(tenantConfig config.TenantConfig) config.TenantConfig {
	cloned := tenantConfig
	// A non-nil base keeps empty channel lists marshaling as [] instead of
	// JSON null, so API consumers never see a null where an array belongs.
	cloned.Channels = append([]config.ChannelBinding{}, tenantConfig.Channels...)
	return cloned
}

func applicationKey(tenantID, appCode string) (string, error) {
	_, err := channels.BuildSessionKey(tenantID, appCode, "validation")
	if err != nil {
		return "", fmt.Errorf("invalid tenant application: %w", err)
	}
	return tenantID + "/" + appCode, nil
}

func snapshotKey(applicationKey string, version uint64) string {
	return fmt.Sprintf("%s@%d", applicationKey, version)
}

func bindingKeys(tenantConfig config.TenantConfig) ([]string, error) {
	keys := make([]string, 0, len(tenantConfig.Channels))
	for _, binding := range tenantConfig.Channels {
		key, err := externalBindingKey(binding.Type, binding.BindingID)
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func externalBindingKey(channelType, bindingID string) (string, error) {
	channel := channels.Channel(channelType)
	if !channel.Supported() {
		return "", fmt.Errorf("unsupported channel %q", channelType)
	}
	if bindingID == "" {
		return "", fmt.Errorf("external binding ID is required")
	}
	return channelType + "/" + bindingID, nil
}

// ListApplications returns the active application snapshots ordered by tenant
// and app code. An empty tenantID lists every tenant.
func (r *MemoryRepository) ListApplications(ctx context.Context, tenantID string) ([]Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	applications := make([]Snapshot, 0)
	for applicationKey, version := range r.activeByApplication {
		tenant, _, found := strings.Cut(applicationKey, "/")
		if !found {
			return nil, fmt.Errorf("application key %q has no tenant segment", applicationKey)
		}
		if tenantID != "" && tenant != tenantID {
			continue
		}
		snapshot, ok := r.snapshots[snapshotKey(applicationKey, version)]
		if !ok {
			return nil, fmt.Errorf("active snapshot missing for %q", applicationKey)
		}
		cloned, err := cloneSnapshot(snapshot)
		if err != nil {
			return nil, err
		}
		applications = append(applications, cloned)
	}
	sort.Slice(applications, func(left, right int) bool {
		if applications[left].Config.TenantID != applications[right].Config.TenantID {
			return applications[left].Config.TenantID < applications[right].Config.TenantID
		}
		return applications[left].Config.AppCode < applications[right].Config.AppCode
	})
	return applications, nil
}
