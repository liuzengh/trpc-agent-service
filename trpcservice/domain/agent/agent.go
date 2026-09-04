// Package agent hosts tenant-specific agents built on tRPC-Agent-Go
// (llmagent, graph, chain/parallel/cycle) and runner.Runner.
package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"

	fwagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	fwtool "trpc.group/trpc-go/trpc-agent-go/tool"

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
}

// Manager owns tenant-scoped agent definitions and their immutable versions
// behind a swappable store, and builds live agents from the model registry.
type Manager struct {
	store  Store
	llmReg *llm.Registry
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
	return llmagent.New(agentID,
		llmagent.WithModel(mdl),
		llmagent.WithInstruction(instruction),
		llmagent.WithTools(tools),
	), nil
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
	return &cp, nil
}

func (s *memStore) List(_ context.Context, tenantID string) ([]Agent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Agent, 0)
	for _, a := range s.agents {
		if tenantID == "" || a.TenantID == tenantID {
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
