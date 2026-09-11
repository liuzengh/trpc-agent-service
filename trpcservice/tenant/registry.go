package tenant

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/configcontrol"
)

var (
	ErrTenantNotFound        = errors.New("tenant not found")
	ErrBindingNotFound       = errors.New("channel binding not found")
	ErrBindingTenantMismatch = errors.New("channel binding belongs to another tenant")
	ErrNoRollback            = errors.New("no previous tenant revision")
	ErrBindingConflict       = errors.New("channel binding conflict")
	ErrVersionImmutable      = errors.New("tenant version is immutable")
)

type Binding struct {
	Tenant  config.TenantConfig
	Channel config.ChannelConfig
}

type snapshot struct {
	tenants  map[string]config.TenantConfig
	bindings map[string]Binding
}

type controlSnapshot struct {
	state  configcontrol.TenantState
	active config.TenantConfig
	canary *config.TenantConfig
}

// Registry publishes immutable configuration snapshots and retains one prior
// revision per tenant for an immediate operational rollback.
type Registry struct {
	mu            sync.RWMutex
	current       snapshot
	history       map[string][]config.TenantConfig
	revisions     map[string]map[string][sha256.Size]byte
	control       map[string]controlSnapshot
	ready         atomic.Bool
	beforePublish func([]config.TenantConfig) error
}

func NewRegistry(cfg *config.Config) (*Registry, error) {
	r := &Registry{
		history:   make(map[string][]config.TenantConfig),
		revisions: make(map[string]map[string][sha256.Size]byte),
		control:   make(map[string]controlSnapshot),
	}
	if err := r.Apply(cfg); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *Registry) Apply(cfg *config.Config) error {
	if cfg == nil {
		return errors.New("tenant registry: nil config")
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	next := snapshot{tenants: make(map[string]config.TenantConfig), bindings: make(map[string]Binding)}
	for _, tenant := range cfg.Tenants {
		next.tenants[tenant.TenantID] = tenant
		if !tenant.Enabled {
			continue
		}
		for _, ch := range tenant.Channels {
			if ch.Enabled {
				next.bindings[bindingKey(ch.Type, ch.BindingID)] = Binding{Tenant: tenant, Channel: ch}
			}
		}
	}

	digests := make(map[string][sha256.Size]byte, len(next.tenants))
	for id, tenantConfig := range next.tenants {
		digest, err := revisionDigest(tenantConfig)
		if err != nil {
			return fmt.Errorf("tenant %s revision digest: %w", id, err)
		}
		digests[id] = digest
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	for id, newTenant := range next.tenants {
		if known, exists := r.revisions[id][newTenant.Version]; exists && known != digests[id] {
			return fmt.Errorf("%w: tenant %s changed without incrementing version %s", ErrVersionImmutable, id, newTenant.Version)
		}
	}
	if err := r.preparePublishLocked(next.tenants); err != nil {
		return err
	}
	for id, old := range r.current.tenants {
		newTenant, exists := next.tenants[id]
		if !exists || newTenant.Version != old.Version {
			r.history[id] = appendBounded(r.history[id], old, 10)
		}
	}
	for id, tenantConfig := range next.tenants {
		if r.revisions[id] == nil {
			r.revisions[id] = make(map[string][sha256.Size]byte)
		}
		r.revisions[id][tenantConfig.Version] = digests[id]
	}
	r.current = next
	r.control = make(map[string]controlSnapshot, len(next.tenants))
	for id, tenantConfig := range next.tenants {
		r.control[id] = controlSnapshot{
			state:  configcontrol.TenantState{TenantID: id, ActiveRevision: tenantConfig.Version, Generation: 1},
			active: tenantConfig,
		}
	}
	r.ready.Store(true)
	return nil
}

// PublishControlState installs the immutable revision selected by the
// persistent control plane. The old snapshot remains available in the
// revision store and any already-created Task keeps its own full Tenant
// snapshot, so refreshing this registry cannot mutate in-flight work.
func (r *Registry) PublishControlState(state configcontrol.TenantState, active config.TenantConfig, canary *config.TenantConfig) error {
	if active.TenantID == "" || active.TenantID != state.TenantID || active.Version != state.ActiveRevision {
		return fmt.Errorf("tenant registry: active revision does not match control state")
	}
	if canary != nil {
		if canary.TenantID != state.TenantID || canary.Version != state.CanaryRevision || canary.App.Name != active.App.Name {
			return fmt.Errorf("tenant registry: canary revision is incompatible with active app identity")
		}
		if state.RolloutPercent < 0 || state.RolloutPercent > 100 {
			return fmt.Errorf("tenant registry: invalid rollout percent")
		}
	}
	activeBindings, err := buildBindingsForTenant(active)
	if err != nil {
		return err
	}
	if canary != nil {
		canaryBindings, err := buildBindingsForTenant(*canary)
		if err != nil {
			return err
		}
		for key, canaryBinding := range canaryBindings {
			if activeBinding, ok := activeBindings[key]; ok && (activeBinding.Channel.Type != canaryBinding.Channel.Type || activeBinding.Channel.BindingID != canaryBinding.Channel.BindingID) {
				return fmt.Errorf("tenant registry: canary binding %s is incompatible", key)
			}
		}
		for key := range activeBindings {
			if _, ok := canaryBindings[key]; !ok {
				return fmt.Errorf("tenant registry: canary is missing active binding %s", key)
			}
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.control == nil {
		r.control = make(map[string]controlSnapshot)
	}
	previous, hadPrevious := r.control[state.TenantID]
	if hadPrevious && state.Generation < previous.state.Generation {
		return fmt.Errorf("tenant registry: stale generation %d", state.Generation)
	}
	nextTenants := make(map[string]config.TenantConfig, len(r.current.tenants)+1)
	for id, tenantConfig := range r.current.tenants {
		nextTenants[id] = tenantConfig
	}
	nextTenants[state.TenantID] = active
	nextBindings, err := buildBindings(nextTenants)
	if err != nil {
		return err
	}

	digest, err := revisionDigest(active)
	if err != nil {
		return err
	}
	var canaryDigest [sha256.Size]byte
	if canary != nil {
		canaryDigest, err = revisionDigest(*canary)
		if err != nil {
			return err
		}
		if !sameBotTransports(active, *canary) {
			return errors.New("tenant registry: canary cannot change intelligent bot transports; use a full release")
		}
	}
	if err := r.preparePublishLocked(nextTenants); err != nil {
		return err
	}
	if r.revisions[state.TenantID] == nil {
		r.revisions[state.TenantID] = make(map[string][sha256.Size]byte)
	}
	r.revisions[state.TenantID][active.Version] = digest
	if canary != nil {
		r.revisions[state.TenantID][canary.Version] = canaryDigest
	}
	if hadPrevious && previous.active.Version != active.Version {
		r.history[state.TenantID] = appendBounded(r.history[state.TenantID], previous.active, 10)
	}
	copyCanary := cloneTenant(canary)
	r.current = snapshot{tenants: nextTenants, bindings: nextBindings}
	r.control[state.TenantID] = controlSnapshot{state: state, active: active, canary: copyCanary}
	return nil
}

func (r *Registry) SetConfigReady(ready bool) { r.ready.Store(ready) }

func (r *Registry) ConfigReady() bool { return r.ready.Load() }

// ResolveForSession deterministically chooses active or canary for one
// tenant/app/session tuple. It returns a value copy, making it safe for the
// caller to persist as an in-flight task snapshot.
func (r *Registry) ResolveForSession(tenantID, sessionID string) (config.TenantConfig, int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	selected, generation, err := r.resolveForSessionLocked(tenantID, sessionID)
	return selected, generation, err
}

func (r *Registry) resolveForSessionLocked(tenantID, sessionID string) (config.TenantConfig, int64, error) {
	if state, ok := r.control[tenantID]; ok {
		if state.canary != nil && state.state.RolloutPercent > 0 {
			key := []byte(tenantID + "\x1f" + state.active.App.Name + "\x1f" + sessionID)
			digest := sha256.Sum256(key)
			bucket := binary.BigEndian.Uint64(digest[:8]) % 100
			if int(bucket) < state.state.RolloutPercent {
				return *cloneTenant(state.canary), state.state.Generation, nil
			}
		}
		return state.active, state.state.Generation, nil
	}
	tenantConfig, ok := r.current.tenants[tenantID]
	if !ok || !tenantConfig.Enabled {
		return config.TenantConfig{}, 0, ErrTenantNotFound
	}
	return tenantConfig, 0, nil
}

// ResolveBindingForSession keeps signature verification on the active binding
// while selecting the immutable canary task snapshot after the provider event
// has been authenticated.
func (r *Registry) ResolveBindingForSession(tenantID, sessionID, channelType, bindingID string) (Binding, int64, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	selected, generation, err := r.resolveForSessionLocked(tenantID, sessionID)
	if err != nil {
		return Binding{}, 0, err
	}
	for _, channel := range selected.Channels {
		if channel.Enabled && channel.Type == channelType && channel.BindingID == bindingID {
			return Binding{Tenant: selected, Channel: channel}, generation, nil
		}
	}
	return Binding{}, 0, ErrBindingNotFound
}

func (r *Registry) ControlState(tenantID string) (configcontrol.TenantState, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	state, ok := r.control[tenantID]
	if !ok {
		return configcontrol.TenantState{}, false
	}
	return state.state, true
}

func (r *Registry) ControlStates() []configcontrol.TenantState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]configcontrol.TenantState, 0, len(r.control))
	for _, state := range r.control {
		result = append(result, state.state)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TenantID < result[j].TenantID })
	return result
}

func (r *Registry) Tenant(id string) (config.TenantConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.current.tenants[id]
	if !ok || !t.Enabled {
		return config.TenantConfig{}, ErrTenantNotFound
	}
	return t, nil
}

func (r *Registry) ResolveBinding(channelType, bindingID string) (Binding, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.current.bindings[bindingKey(channelType, bindingID)]
	if !ok {
		return Binding{}, ErrBindingNotFound
	}
	return b, nil
}

// ResolveBindingForTenant fences delayed Outbox rows against binding-key
// reassignment. A live binding is usable only while it still belongs to the
// tenant that produced the reply.
func (r *Registry) ResolveBindingForTenant(tenantID, channelType, bindingID string) (Binding, error) {
	binding, err := r.ResolveBinding(channelType, bindingID)
	if err != nil {
		return Binding{}, err
	}
	if binding.Tenant.TenantID != tenantID {
		return Binding{}, ErrBindingTenantMismatch
	}
	return binding, nil
}

func (r *Registry) List() []config.TenantConfig {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]config.TenantConfig, 0, len(r.current.tenants))
	for _, t := range r.current.tenants {
		result = append(result, t)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].TenantID < result[j].TenantID })
	return result
}

func (r *Registry) Rollback(tenantID string) (config.TenantConfig, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	versions := r.history[tenantID]
	if len(versions) == 0 {
		return config.TenantConfig{}, ErrNoRollback
	}
	previous := versions[len(versions)-1]
	candidateTenants := make(map[string]config.TenantConfig, len(r.current.tenants)+1)
	for id, tenantConfig := range r.current.tenants {
		candidateTenants[id] = tenantConfig
	}
	candidateTenants[tenantID] = previous
	candidateBindings, err := buildBindings(candidateTenants)
	if err != nil {
		// Keep both the active snapshot and rollback history untouched. A stale
		// revision may contain a binding that another tenant legitimately owns
		// now; publishing it would route verified traffic across tenants.
		return config.TenantConfig{}, err
	}

	if err := r.preparePublishLocked(candidateTenants); err != nil {
		return config.TenantConfig{}, err
	}
	r.history[tenantID] = versions[:len(versions)-1]
	current, exists := r.current.tenants[tenantID]
	if exists {
		r.history[tenantID] = appendBounded(r.history[tenantID], current, 10)
	}
	r.current = snapshot{tenants: candidateTenants, bindings: candidateBindings}
	return previous, nil
}

func buildBindings(tenants map[string]config.TenantConfig) (map[string]Binding, error) {
	bindings := make(map[string]Binding)
	for _, tenantConfig := range tenants {
		if !tenantConfig.Enabled {
			continue
		}
		for _, ch := range tenantConfig.Channels {
			if !ch.Enabled {
				continue
			}
			key := bindingKey(ch.Type, ch.BindingID)
			if existing, ok := bindings[key]; ok && existing.Tenant.TenantID != tenantConfig.TenantID {
				return nil, fmt.Errorf("%w: %s is owned by tenants %s and %s", ErrBindingConflict, key, existing.Tenant.TenantID, tenantConfig.TenantID)
			}
			bindings[key] = Binding{Tenant: tenantConfig, Channel: ch}
		}
	}
	return bindings, nil
}

func buildBindingsForTenant(tenantConfig config.TenantConfig) (map[string]Binding, error) {
	if !tenantConfig.Enabled {
		return map[string]Binding{}, nil
	}
	result := make(map[string]Binding)
	for _, channel := range tenantConfig.Channels {
		if !channel.Enabled {
			continue
		}
		key := bindingKey(channel.Type, channel.BindingID)
		if existing, ok := result[key]; ok && existing.Tenant.TenantID != tenantConfig.TenantID {
			return nil, fmt.Errorf("%w: %s", ErrBindingConflict, key)
		}
		result[key] = Binding{Tenant: tenantConfig, Channel: channel}
	}
	return result, nil
}

func cloneTenant(tenantConfig *config.TenantConfig) *config.TenantConfig {
	if tenantConfig == nil {
		return nil
	}
	clone := *tenantConfig
	clone.Channels = append([]config.ChannelConfig(nil), tenantConfig.Channels...)
	return &clone
}

func bindingKey(channelType, bindingID string) string {
	return fmt.Sprintf("%s/%s", channelType, bindingID)
}

func appendBounded(items []config.TenantConfig, item config.TenantConfig, limit int) []config.TenantConfig {
	items = append(items, item)
	if len(items) > limit {
		items = append([]config.TenantConfig(nil), items[len(items)-limit:]...)
	}
	return items
}

func revisionDigest(tenantConfig config.TenantConfig) ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(tenantConfig)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

// SetBeforePublish installs a transport preparation hook and reconciles the
// current snapshot atomically. The hook must not call back into Registry.
func (r *Registry) SetBeforePublish(hook func([]config.TenantConfig) error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if hook != nil {
		if err := hook(tenantValues(r.current.tenants)); err != nil {
			return err
		}
	}
	r.beforePublish = hook
	return nil
}
func (r *Registry) preparePublishLocked(tenants map[string]config.TenantConfig) error {
	if r.beforePublish == nil {
		return nil
	}
	return r.beforePublish(tenantValues(tenants))
}
func tenantValues(tenants map[string]config.TenantConfig) []config.TenantConfig {
	values := make([]config.TenantConfig, 0, len(tenants))
	for _, value := range tenants {
		values = append(values, *cloneTenant(&value))
	}
	sort.Slice(values, func(i, j int) bool { return values[i].TenantID < values[j].TenantID })
	return values
}

// One binding has one live transport, so active/canary cannot select different
// bot identities. Model/tool policy can still be canaried independently.
func sameBotTransports(a, b config.TenantConfig) bool {
	transports := func(t config.TenantConfig) string {
		values := make(map[string][]string)
		if t.Enabled {
			for _, ch := range t.Channels {
				if ch.Enabled && ch.Type == "wecom-aibot" {
					values[ch.BindingID] = []string{ch.BotIDEnv, ch.BotSecretEnv, ch.WebSocketURL}
				}
			}
		}
		encoded, _ := json.Marshal(values)
		return string(encoded)
	}
	return transports(a) == transports(b)
}
