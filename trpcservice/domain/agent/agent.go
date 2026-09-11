// Package agent hosts tenant-specific agents built on tRPC-Agent-Go
// (llmagent, graph, chain/parallel/cycle) and runner.Runner.
package agent

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"

	fwagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	fwtool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/asset"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/llm"
)

// Status values for an agent.
const (
	StatusDraft     = "draft"
	StatusPublished = "published"
	StatusDisabled  = "disabled"
)

// ErrNotFound is returned when an agent does not exist.
var ErrNotFound = errors.New("agent: not found")

// RuntimeProfile is the frozen, immutable configuration of an agent version.
type RuntimeProfile struct {
	SystemPrompt string   `json:"system_prompt"`
	EndpointID   string   `json:"endpoint_id"`
	ToolIDs      []string `json:"tool_ids,omitempty"`
	KnowledgeIDs []string `json:"kb_ids,omitempty"`                // knowledge bases mounted as search tools
	SkillIDs     []string `json:"skill_ids,omitempty"`             // skills mounted; SKILL.md injected as instruction
	ApprovalToolIDs []string `json:"approval_tool_ids,omitempty"`  // tool ids whose calls need human approval
}

// Agent is a tenant-scoped agent definition (a team assistant, not a per-user
// assistant; per-user state is keyed by user id + session id at runtime).
type Agent struct {
	ID             string `json:"id"`
	TenantID       string `json:"tenant_id"`
	Name           string `json:"name"`
	Description    string `json:"description,omitempty"`
	Status         string `json:"status"`
	CurrentVersion int    `json:"current_version"`
	// CreatedBy is the member that authored the agent; Visibility decides
	// whether the rest of the tenant may see it (see domain/asset).
	CreatedBy  string `json:"created_by,omitempty"`
	Visibility string `json:"visibility,omitempty"`
	// Gray optionally routes part of the traffic to another published version.
	Gray *GrayRelease `json:"gray,omitempty"`
}

// GrayRelease is a canary (gray) release: a share of the sessions runs a
// different published version of the same agent so a change can be observed on
// real traffic before it is promoted. Sessions are bucketed by their id, so one
// conversation never flips between versions mid-flight, and rollback is
// immediate (clear the release, or promote the version).
type GrayRelease struct {
	// Version is the alternate published version (1-based).
	Version int `json:"version"`
	// Percent is the share of sessions routed to Version, 1..100.
	Percent int `json:"percent"`
}

// Validate reports whether the release is usable.
func (g *GrayRelease) Validate() error {
	if g == nil {
		return nil
	}
	if g.Version < 1 {
		return fmt.Errorf("agent: gray version must be >= 1")
	}
	if g.Percent < 1 || g.Percent > 100 {
		return fmt.Errorf("agent: gray percent must be between 1 and 100")
	}
	return nil
}

// GrayBucket maps a session key to a stable bucket in [0,100), the unit the
// release percentage is compared against. Deterministic hashing (not random)
// keeps a conversation on one version for its whole life.
func GrayBucket(sessionKey string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(sessionKey))
	return int(h.Sum32() % 100)
}

// VersionInfo describes a published version for the rollback UI.
type VersionInfo struct {
	Version int    `json:"version"`
	Status  string `json:"status"`
}

// Store is the persistence contract behind Manager. The in-memory store keeps
// the service runnable without MySQL; MySQL and other backend stores are
// implemented in the infra/storage package against this interface.
type Store interface {
	Create(ctx context.Context, a Agent) error
	Get(ctx context.Context, id string) (*Agent, error)
	List(ctx context.Context, tenantID string) ([]Agent, error)
	Update(ctx context.Context, a Agent) error
	Delete(ctx context.Context, id string) error
	Publish(ctx context.Context, id string, p RuntimeProfile) (int, error)
	Rollback(ctx context.Context, id string, version int) error
	Resolve(ctx context.Context, id string) (RuntimeProfile, error)
	Versions(ctx context.Context, id string) ([]VersionInfo, error)
	// ResolveVersion returns one published version's profile (gray releases
	// need the profile of a version that is not the current one).
	ResolveVersion(ctx context.Context, id string, version int) (RuntimeProfile, error)
	// SetGray stores or clears (nil) the agent's canary release.
	SetGray(ctx context.Context, id string, g *GrayRelease) error
}

// Manager owns tenant-scoped agent definitions and their immutable versions
// behind a swappable store, and builds live agents from the model registry.
type Manager struct {
	store  Store
	llmReg *llm.Registry
	// memoryPreload is the framework's long-term-memory preload budget applied
	// to every agent this manager builds. 0 keeps long-term memory off.
	memoryPreload int
}

// NewManager returns an in-memory manager backed by the given model registry.
func NewManager(llmReg *llm.Registry) *Manager {
	return &Manager{store: newMemStore(), llmReg: llmReg}
}

// NewManagerWithStore returns a manager over the given store implementation,
// used by infra/storage to back a Manager with MySQL.
func NewManagerWithStore(s Store, llmReg *llm.Registry) *Manager {
	return &Manager{store: s, llmReg: llmReg}
}

// SetPreloadMemory sets the framework-side long-term-memory preload budget for
// every agent built from a profile. The value is the platform-wide
// config.MemoryConfig.PreloadCount (0 = off, -1 = all, N > 0 = adaptive budget);
// it is applied at startup, before any turn runs, so it needs no locking.
func (m *Manager) SetPreloadMemory(n int) { m.memoryPreload = n }

// MemoryPreload reports the configured preload budget. The worker reads it to
// decide whether to resolve the tenant's memory service and expose the memory
// tools at all, so the switch has exactly one owner.
func (m *Manager) MemoryPreload() int { return m.memoryPreload }

// Validate returns an error if the agent's identity fields are not set.
func Validate(a Agent) error {
	if a.ID == "" || a.TenantID == "" || a.Name == "" {
		return fmt.Errorf("agent: id, tenant and name are required")
	}
	return nil
}

// Create registers a new agent in draft status.
func (m *Manager) Create(ctx context.Context, a Agent) error {
	if err := Validate(a); err != nil {
		return err
	}
	a.Status = StatusDraft
	a.CurrentVersion = 0
	// A new agent is private to its author until the author shares it.
	a.Visibility = asset.VisibilityOrDefault(a.Visibility)
	return m.store.Create(ctx, a)
}

// Get returns an agent by id.
func (m *Manager) Get(ctx context.Context, id string) (*Agent, error) {
	return m.store.Get(ctx, id)
}

// List returns the agents in a tenant (empty tenantID returns all).
func (m *Manager) List(ctx context.Context, tenantID string) ([]Agent, error) {
	return m.store.List(ctx, tenantID)
}

// Update changes an agent's editable fields (name/description/status).
func (m *Manager) Update(ctx context.Context, a Agent) error {
	if err := Validate(a); err != nil {
		return err
	}
	a.Visibility = asset.VisibilityOrDefault(a.Visibility)
	return m.store.Update(ctx, a)
}

// Delete removes an agent.
func (m *Manager) Delete(ctx context.Context, id string) error {
	return m.store.Delete(ctx, id)
}

// Publish freezes a new immutable version and marks it current.
func (m *Manager) Publish(ctx context.Context, id string, p RuntimeProfile) (int, error) {
	return m.store.Publish(ctx, id, p)
}

// Rollback switches the current version back to the given one.
func (m *Manager) Rollback(ctx context.Context, id string, version int) error {
	return m.store.Rollback(ctx, id, version)
}

// Resolve returns the current runtime profile.
func (m *Manager) Resolve(ctx context.Context, id string) (RuntimeProfile, error) {
	return m.store.Resolve(ctx, id)
}

// Versions lists the published versions of an agent.
func (m *Manager) Versions(ctx context.Context, id string) ([]VersionInfo, error) {
	return m.store.Versions(ctx, id)
}

// ResolveVersion returns one published version's profile.
func (m *Manager) ResolveVersion(ctx context.Context, id string, version int) (RuntimeProfile, error) {
	return m.store.ResolveVersion(ctx, id, version)
}

// SetGray installs or clears (nil) the agent's canary release. The target must
// be an already published version: a gray release is a traffic decision, never
// a way to publish something unreviewed.
func (m *Manager) SetGray(ctx context.Context, id string, g *GrayRelease) error {
	if err := g.Validate(); err != nil {
		return err
	}
	if g == nil {
		return m.store.SetGray(ctx, id, nil)
	}
	ag, err := m.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if g.Version == ag.CurrentVersion {
		// Same version on both sides: the release would be a no-op, and a
		// silent no-op is worse than a clear rejection.
		return fmt.Errorf("agent: gray version %d is already the current version", g.Version)
	}
	if _, err := m.store.ResolveVersion(ctx, id, g.Version); err != nil {
		return err
	}
	return m.store.SetGray(ctx, id, g)
}

// ClearGray removes the canary release (all traffic returns to the current
// version). It is the fast rollback: no publish, no restart.
func (m *Manager) ClearGray(ctx context.Context, id string) error {
	return m.store.SetGray(ctx, id, nil)
}

// ResolveForSession picks the runtime profile for one session, applying the
// agent's canary release. It returns the profile and the version it came from
// so the caller can record which behaviour actually served the turn.
//
// Bucketing is by session id, so a conversation stays on one version end to
// end; a gray version that vanished (deleted agent row, hand-edited database)
// falls back to the current version instead of failing the user's turn.
func (m *Manager) ResolveForSession(ctx context.Context, id, sessionKey string) (RuntimeProfile, int, error) {
	ag, err := m.store.Get(ctx, id)
	if err != nil {
		return RuntimeProfile{}, 0, err
	}
	g := ag.Gray
	if g == nil || g.Percent <= 0 {
		p, err := m.store.Resolve(ctx, id)
		return p, ag.CurrentVersion, err
	}
	if GrayBucket(sessionKey) >= g.Percent {
		p, err := m.store.Resolve(ctx, id)
		return p, ag.CurrentVersion, err
	}
	p, err := m.store.ResolveVersion(ctx, id, g.Version)
	if err != nil {
		slog.Warn("agent: gray version unavailable, falling back to the current version",
			"agent", id, "gray_version", g.Version, "err", err)
		p, err = m.store.Resolve(ctx, id)
		return p, ag.CurrentVersion, err
	}
	return p, g.Version, nil
}

// BuildAgent assembles a tRPC-Agent-Go agent from the current profile.
func (m *Manager) BuildAgent(ctx context.Context, agentID string, tools []fwtool.Tool) (fwagent.Agent, error) {
	p, err := m.Resolve(ctx, agentID)
	if err != nil {
		return nil, err
	}
	return m.BuildFromProfile(ctx, agentID, p, tools, "")
}

// BuildAgentWithInstruction is BuildAgent plus an extra instruction fragment
// appended to the profile system prompt. The worker uses it to splice in the
// text of the skills mounted on the agent (skill SKILL.md + prompt template).
func (m *Manager) BuildAgentWithInstruction(ctx context.Context, agentID string, tools []fwtool.Tool, extraInstruction string) (fwagent.Agent, error) {
	p, err := m.Resolve(ctx, agentID)
	if err != nil {
		return nil, err
	}
	return m.BuildFromProfile(ctx, agentID, p, tools, extraInstruction)
}

// BuildFromProfile assembles a live agent from an already-resolved profile
// (avoids re-reading the store when the caller has the profile in hand).
func (m *Manager) BuildFromProfile(ctx context.Context, agentID string, p RuntimeProfile, tools []fwtool.Tool, extraInstruction string) (fwagent.Agent, error) {
	mdl, err := m.llmReg.Resolve(ctx, p.EndpointID)
	if err != nil {
		return nil, err
	}
	instruction := p.SystemPrompt
	if extraInstruction != "" {
		if instruction != "" {
			instruction += "\n\n"
		}
		instruction += extraInstruction
	}
	opts := []llmagent.Option{
		llmagent.WithModel(mdl),
		llmagent.WithInstruction(instruction),
		llmagent.WithTools(tools),
	}
	if m.memoryPreload != 0 {
		// Framework-side memory preload: the LLM agent's request processor
		// injects the user's stored memories into the system prompt, so the
		// model sees long-term context without calling a tool first.
		opts = append(opts, llmagent.WithPreloadMemory(m.memoryPreload))
	}
	return llmagent.New(agentID, opts...), nil
}

// memStore keeps agents and their versioned runtime profiles in maps; the
// zero-dependency dev/test backend.
type memStore struct {
	mu       sync.RWMutex
	agents   map[string]Agent
	versions map[string][]RuntimeProfile
}

func newMemStore() *memStore {
	return &memStore{
		agents:   make(map[string]Agent),
		versions: make(map[string][]RuntimeProfile),
	}
}

func (s *memStore) Create(_ context.Context, a Agent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.agents[a.ID]; ok {
		return fmt.Errorf("agent: %q already exists", a.ID)
	}
	s.agents[a.ID] = a
	return nil
}

func (s *memStore) Get(_ context.Context, id string) (*Agent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.agents[id]
	if !ok {
		return nil, ErrNotFound
	}
	cp := a
	cp.Visibility = asset.VisibilityOrDefault(cp.Visibility)
	return &cp, nil
}

func (s *memStore) List(_ context.Context, tenantID string) ([]Agent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Agent, 0)
	for _, a := range s.agents {
		if tenantID == "" || a.TenantID == tenantID {
			a.Visibility = asset.VisibilityOrDefault(a.Visibility)
			out = append(out, a)
		}
	}
	return out, nil
}

func (s *memStore) Update(_ context.Context, a Agent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.agents[a.ID]
	if !ok {
		return ErrNotFound
	}
	// version is managed by publish/rollback; status may be edited (enable/disable).
	a.CurrentVersion = cur.CurrentVersion
	// Ownership is fixed at creation: a write may publish/unpublish but never
	// reassign the author.
	a.CreatedBy = cur.CreatedBy
	a.Visibility = asset.VisibilityOrDefault(a.Visibility)
	if a.Status == "" {
		a.Status = cur.Status
	}
	s.agents[a.ID] = a
	return nil
}

func (s *memStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.agents[id]; !ok {
		return ErrNotFound
	}
	delete(s.agents, id)
	delete(s.versions, id)
	return nil
}

func (s *memStore) Publish(_ context.Context, id string, p RuntimeProfile) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.agents[id]; !ok {
		return 0, ErrNotFound
	}
	s.versions[id] = append(s.versions[id], p)
	v := len(s.versions[id])
	a := s.agents[id]
	a.Status = StatusPublished
	a.CurrentVersion = v
	s.agents[id] = a
	return v, nil
}

func (s *memStore) Rollback(_ context.Context, id string, version int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.agents[id]; !ok {
		return ErrNotFound
	}
	vs := s.versions[id]
	if version < 1 || version > len(vs) {
		return ErrNotFound
	}
	a := s.agents[id]
	a.CurrentVersion = version
	s.agents[id] = a
	return nil
}

func (s *memStore) Resolve(_ context.Context, id string) (RuntimeProfile, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.agents[id]
	if !ok {
		return RuntimeProfile{}, ErrNotFound
	}
	vs := s.versions[id]
	if a.CurrentVersion < 1 || a.CurrentVersion > len(vs) {
		return RuntimeProfile{}, ErrNotFound
	}
	return vs[a.CurrentVersion-1], nil
}

func (s *memStore) Versions(_ context.Context, id string) ([]VersionInfo, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.agents[id]; !ok {
		return nil, ErrNotFound
	}
	out := make([]VersionInfo, 0, len(s.versions[id]))
	for i := range s.versions[id] {
		out = append(out, VersionInfo{Version: i + 1, Status: StatusPublished})
	}
	return out, nil
}

func (s *memStore) ResolveVersion(_ context.Context, id string, version int) (RuntimeProfile, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, ok := s.agents[id]; !ok {
		return RuntimeProfile{}, ErrNotFound
	}
	vs := s.versions[id]
	if version < 1 || version > len(vs) {
		return RuntimeProfile{}, ErrNotFound
	}
	return vs[version-1], nil
}

func (s *memStore) SetGray(_ context.Context, id string, g *GrayRelease) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[id]
	if !ok {
		return ErrNotFound
	}
	a.Gray = g
	s.agents[id] = a
	return nil
}
