// Package skill implements the three-level skill asset model (Skill /
// SkillVersion / AgentSkill binding with a version lock). A skill is stored as
// rows and loaded into the model context as text at runtime; the storage form
// is invisible to the model.
//
// Scripts & references (the former fourth-level assets) are not persisted:
// the runtime consumes ContentMD and PromptTemplate from SkillVersion only.
// If executable scripts/knowledge files are ever needed they extend
// SkillVersion as a future migration (stage 33 decision, see
// deployments/mysql/init/005_skills.sql).
package skill

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/asset"
)

// Sentinel errors.
var (
	ErrSkillNotFound   = errors.New("skill: not found")
	ErrVersionNotFound = errors.New("skill: version not found")
	ErrSkillCodeExists = errors.New("skill: code already exists")
	ErrVersionExists   = errors.New("skill: version already exists")
	ErrVersionNotDraft = errors.New("skill: version is not in draft state")
	ErrInvalidSkill    = errors.New("skill: missing required field")
)

// Lifecycle states.
const (
	StatusDraft     = "draft"
	StatusPublished = "published"
	StatusDisabled  = "disabled"
)

// Scopes.
const (
	ScopeGlobal = "global"
	ScopeTenant = "tenant"
)

// Skill is the stable identity of a reusable capability: code, scope,
// name and the current_version pointer that the runtime resolves.
type Skill struct {
	SkillID        string    `json:"skill_id"`
	Scope          string    `json:"scope"`
	OwnerTenantID  *string   `json:"owner_tenant_id,omitempty"` // nil when scope=global
	Code           string    `json:"code"`
	Name           string    `json:"name"`
	Description    string    `json:"description,omitempty"`
	CurrentVersion int       `json:"current_version"`
	Status         string    `json:"status"`
	// CreatedBy is the member that authored the skill; Visibility decides
	// whether the rest of the tenant may see it (see domain/asset).
	CreatedBy  string    `json:"created_by,omitempty"`
	Visibility string    `json:"visibility,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// SkillVersion is one immutable snapshot of a skill's SKILL.md body and
// execution config. Versions are append-only.
type SkillVersion struct {
	SkillID        string    `json:"skill_id"`
	Version        int       `json:"version"`
	ContentMD      string    `json:"content_md"`
	Checksum       string    `json:"checksum"`
	PromptTemplate string    `json:"prompt_template,omitempty"`
	ExecutorType   string    `json:"executor_type"`
	TimeoutSeconds int       `json:"timeout_seconds"`
	Status         string    `json:"status"`
	PublishedAt    time.Time `json:"published_at"`
}

// AgentSkill is the (agent, skill) binding with a locked version and sort
// order. Re-binding the same (agent, skill) updates version + sort in place.
type AgentSkill struct {
	AgentID   string    `json:"agent_id"`
	SkillID   string    `json:"skill_id"`
	Version   int       `json:"version"`
	SortOrder int       `json:"sort_order"`
	CreatedAt time.Time `json:"created_at"`
}

// LoadedSkill is what the runtime consumes for an agent: code for routing,
// content_md and prompt_template to splice into the system prompt, and the
// frozen version so running sessions are not affected by future updates.
type LoadedSkill struct {
	SkillID        string `json:"skill_id"`
	Code           string `json:"code"`
	Name           string `json:"name"`
	Version        int    `json:"version"`
	SortOrder      int    `json:"sort_order"`
	ContentMD      string `json:"content_md"`
	PromptTemplate string `json:"prompt_template,omitempty"`
}

// Manager owns the skill catalog and agent bindings. It is safe for
// concurrent use; the embedded memStore uses a single mutex. MySQL and other
// backend stores are implemented in the infra/storage package against the
// exported Store contract below.
type Manager struct {
	store Store
}

// NewManager returns an in-memory manager (dev / test mode).
func NewManager() *Manager {
	return &Manager{store: newMemStore()}
}

// NewManagerWithStore returns a manager over the given store implementation,
// used by infra/storage to back a Manager with MySQL.
func NewManagerWithStore(s Store) *Manager {
	return &Manager{store: s}
}

// ----- Skill CRUD -----

// Create registers a new skill. SkillID is assigned when empty; the skill
// starts at status=draft with current_version=0.
func (m *Manager) Create(ctx context.Context, s *Skill) error {
	if s.Code == "" || s.Name == "" || s.Scope == "" {
		return ErrInvalidSkill
	}
	if s.Scope != ScopeGlobal && s.OwnerTenantID == nil {
		return ErrInvalidSkill
	}
	if s.SkillID == "" {
		s.SkillID = uuid.NewString()
	}
	// A new skill is private to its author until the author shares it.
	s.Visibility = asset.VisibilityOrDefault(s.Visibility)
	return m.store.Create(ctx, s)
}

// Update mutates mutable fields (name, description, status, visibility). Code
// and Scope are immutable to keep references stable.
func (m *Manager) Update(ctx context.Context, s *Skill) error {
	s.Visibility = asset.VisibilityOrDefault(s.Visibility)
	return m.store.Update(ctx, s)
}

// Delete soft-deletes the skill (is_deleted=1) and unbinds it from all
// agents. Versions and scripts remain in storage for auditability.
func (m *Manager) Delete(ctx context.Context, id string) error {
	return m.store.Delete(ctx, id)
}

// Get returns the skill by id, or ErrSkillNotFound.
func (m *Manager) Get(ctx context.Context, id string) (*Skill, error) {
	return m.store.Get(ctx, id)
}

// List returns skills visible to tenantID. An empty tenantID returns every
// skill across the platform (admin view); otherwise global skills + the
// tenant's own skills.
func (m *Manager) List(ctx context.Context, tenantID string) ([]*Skill, error) {
	return m.store.List(ctx, tenantID)
}

// ----- Version lifecycle -----

// CreateVersion appends a new immutable version. The caller picks the version
// number; checksums are recomputed server-side from content_md.
func (m *Manager) CreateVersion(ctx context.Context, v *SkillVersion) error {
	if v.SkillID == "" || v.Version <= 0 {
		return ErrInvalidSkill
	}
	v.Checksum = checksum(v.ContentMD)
	if v.ExecutorType == "" {
		v.ExecutorType = "inline"
	}
	if v.TimeoutSeconds == 0 {
		v.TimeoutSeconds = 30
	}
	return m.store.CreateVersion(ctx, v)
}

// PublishVersion atomically marks the version as published and updates the
// skill's current_version pointer. Re-publishing the same version is a no-op.
func (m *Manager) PublishVersion(ctx context.Context, skillID string, version int) error {
	return m.store.PublishVersion(ctx, skillID, version)
}

// GetVersion returns a specific version of a skill.
func (m *Manager) GetVersion(ctx context.Context, skillID string, version int) (*SkillVersion, error) {
	return m.store.GetVersion(ctx, skillID, version)
}

// ListVersions lists all versions of a skill, newest first.
func (m *Manager) ListVersions(ctx context.Context, skillID string) ([]*SkillVersion, error) {
	return m.store.ListVersions(ctx, skillID)
}

// ----- Agent binding -----

// BindAgentSkill attaches a locked version of a skill to an agent with a
// sort order. Re-binding the same (agent, skill) updates version + sort in
// place. sort_order controls the in-context concatenation order.
func (m *Manager) BindAgentSkill(ctx context.Context, agentID, skillID string, version, sortOrder int) error {
	return m.store.BindAgentSkill(ctx, agentID, skillID, version, sortOrder)
}

// UnbindAgentSkill removes the (agent, skill) binding.
func (m *Manager) UnbindAgentSkill(ctx context.Context, agentID, skillID string) error {
	return m.store.UnbindAgentSkill(ctx, agentID, skillID)
}

// ListAgentSkills returns the bindings of an agent, ordered by sort_order.
func (m *Manager) ListAgentSkills(ctx context.Context, agentID string) ([]*AgentSkill, error) {
	return m.store.ListAgentSkills(ctx, agentID)
}

// LoadForAgent returns the runtime material the worker splices into the
// system prompt, ordered by sort_order. Only the locked version is loaded so
// a future re-publish of the skill does not affect in-flight sessions.
func (m *Manager) LoadForAgent(ctx context.Context, agentID string) ([]LoadedSkill, error) {
	bindings, err := m.store.ListAgentSkills(ctx, agentID)
	if err != nil {
		return nil, err
	}
	// Defensive sort in case storage returned out of order.
	sort.SliceStable(bindings, func(i, j int) bool { return bindings[i].SortOrder < bindings[j].SortOrder })

	out := make([]LoadedSkill, 0, len(bindings))
	for _, b := range bindings {
		sk, err := m.store.Get(ctx, b.SkillID)
		if err != nil {
			continue // soft-deleted skill — skip silently
		}
		v, err := m.store.GetVersion(ctx, b.SkillID, b.Version)
		if err != nil {
			continue
		}
		out = append(out, LoadedSkill{
			SkillID:        sk.SkillID,
			Code:           sk.Code,
			Name:           sk.Name,
			Version:        v.Version,
			SortOrder:      b.SortOrder,
			ContentMD:      v.ContentMD,
			PromptTemplate: v.PromptTemplate,
		})
	}
	return out, nil
}

// LoadByIDs returns the current published content of each skill id, in the
// given order, skipping unpublished/deleted skills. The worker calls this to
// splice mounted skills into an agent's system prompt.
func (m *Manager) LoadByIDs(ctx context.Context, skillIDs []string) ([]LoadedSkill, error) {
	out := make([]LoadedSkill, 0, len(skillIDs))
	for _, id := range skillIDs {
		sk, err := m.store.Get(ctx, id)
		if err != nil || sk.Status != StatusPublished || sk.CurrentVersion == 0 {
			continue // unpublished or deleted — skip
		}
		v, err := m.store.GetVersion(ctx, id, sk.CurrentVersion)
		if err != nil {
			continue
		}
		out = append(out, LoadedSkill{
			SkillID:        sk.SkillID,
			Code:           sk.Code,
			Name:           sk.Name,
			Version:        v.Version,
			SortOrder:      len(out),
			ContentMD:      v.ContentMD,
			PromptTemplate: v.PromptTemplate,
		})
	}
	return out, nil
}

func checksum(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// =============================================================================
// store contract
// =============================================================================

// Store is the persistence contract behind Manager: the three-level skill
// model (Skill / SkillVersion / AgentSkill binding) plus the version
// lifecycle and M:N agent bindings. The in-memory store keeps the service
// runnable without MySQL; MySQL and other backend stores are implemented in
// the infra/storage package against this interface.
type Store interface {
	Create(ctx context.Context, s *Skill) error
	Update(ctx context.Context, s *Skill) error
	Delete(ctx context.Context, id string) error
	Get(ctx context.Context, id string) (*Skill, error)
	List(ctx context.Context, tenantID string) ([]*Skill, error)

	CreateVersion(ctx context.Context, v *SkillVersion) error
	PublishVersion(ctx context.Context, skillID string, version int) error
	GetVersion(ctx context.Context, skillID string, version int) (*SkillVersion, error)
	ListVersions(ctx context.Context, skillID string) ([]*SkillVersion, error)

	BindAgentSkill(ctx context.Context, agentID, skillID string, version, sortOrder int) error
	UnbindAgentSkill(ctx context.Context, agentID, skillID string) error
	ListAgentSkills(ctx context.Context, agentID string) ([]*AgentSkill, error)
}

// =============================================================================
// memStore — in-memory implementation
// =============================================================================

type memStore struct {
	mu            sync.RWMutex
	skills        map[string]*Skill
	byCode        map[string]string // code -> skill_id
	versions      map[string]map[int]*SkillVersion
	agentBindings map[string][]*AgentSkill
}

func newMemStore() *memStore {
	return &memStore{
		skills:        make(map[string]*Skill),
		byCode:        make(map[string]string),
		versions:      make(map[string]map[int]*SkillVersion),
		agentBindings: make(map[string][]*AgentSkill),
	}
}

func (s *memStore) Create(_ context.Context, sk *Skill) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.byCode[sk.Code]; exists {
		return ErrSkillCodeExists
	}
	if sk.SkillID == "" {
		sk.SkillID = uuid.NewString()
	}
	now := time.Now()
	sk.CreatedAt = now
	sk.UpdatedAt = now
	sk.CurrentVersion = 0
	sk.Status = StatusDraft
	cp := *sk
	s.skills[sk.SkillID] = &cp
	s.byCode[sk.Code] = sk.SkillID
	*sk = cp // return assigned values to caller
	return nil
}

func (s *memStore) Update(_ context.Context, sk *Skill) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.skills[sk.SkillID]
	if !ok {
		return ErrSkillNotFound
	}
	cur.Name = sk.Name
	cur.Description = sk.Description
	cur.Status = sk.Status
	cur.Visibility = asset.VisibilityOrDefault(sk.Visibility)
	cur.UpdatedAt = time.Now()
	*sk = *cur
	return nil
}

func (s *memStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.skills[id]
	if !ok {
		return ErrSkillNotFound
	}
	delete(s.byCode, cur.Code)
	delete(s.skills, id)
	delete(s.versions, id)
	// Unbind from all agents
	for agentID, bindings := range s.agentBindings {
		kept := bindings[:0]
		for _, b := range bindings {
			if b.SkillID != id {
				kept = append(kept, b)
			}
		}
		s.agentBindings[agentID] = kept
	}
	return nil
}

func (s *memStore) Get(_ context.Context, id string) (*Skill, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cur, ok := s.skills[id]
	if !ok {
		return nil, ErrSkillNotFound
	}
	cp := *cur
	return &cp, nil
}

func (s *memStore) List(_ context.Context, tenantID string) ([]*Skill, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Skill, 0, len(s.skills))
	for _, sk := range s.skills {
		if sk.Scope == ScopeGlobal {
			out = append(out, copySkill(sk))
			continue
		}
		if tenantID == "" || (sk.OwnerTenantID != nil && *sk.OwnerTenantID == tenantID) {
			out = append(out, copySkill(sk))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Code < out[j].Code })
	return out, nil
}

func (s *memStore) CreateVersion(_ context.Context, v *SkillVersion) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.skills[v.SkillID]; !ok {
		return ErrSkillNotFound
	}
	versions, ok := s.versions[v.SkillID]
	if !ok {
		versions = make(map[int]*SkillVersion)
		s.versions[v.SkillID] = versions
	}
	if _, exists := versions[v.Version]; exists {
		return ErrVersionExists
	}
	v.Status = StatusDraft
	v.PublishedAt = time.Time{}
	cp := *v
	versions[v.Version] = &cp
	*v = cp
	return nil
}

func (s *memStore) PublishVersion(_ context.Context, skillID string, version int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sk, ok := s.skills[skillID]
	if !ok {
		return ErrSkillNotFound
	}
	versions, ok := s.versions[skillID]
	if !ok {
		return ErrVersionNotFound
	}
	v, ok := versions[version]
	if !ok {
		return ErrVersionNotFound
	}
	if v.Status == StatusPublished {
		// Idempotent re-publish.
		return nil
	}
	if v.Status == StatusDisabled {
		return ErrVersionNotDraft
	}
	v.Status = StatusPublished
	v.PublishedAt = time.Now()
	sk.CurrentVersion = version
	if sk.Status != StatusDisabled {
		sk.Status = StatusPublished
	}
	sk.UpdatedAt = time.Now()
	return nil
}

func (s *memStore) GetVersion(_ context.Context, skillID string, version int) (*SkillVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	versions, ok := s.versions[skillID]
	if !ok {
		return nil, ErrVersionNotFound
	}
	v, ok := versions[version]
	if !ok {
		return nil, ErrVersionNotFound
	}
	cp := *v
	return &cp, nil
}

func (s *memStore) ListVersions(_ context.Context, skillID string) ([]*SkillVersion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	versions, ok := s.versions[skillID]
	if !ok {
		return nil, nil
	}
	out := make([]*SkillVersion, 0, len(versions))
	for _, v := range versions {
		cp := *v
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version > out[j].Version })
	return out, nil
}

func (s *memStore) BindAgentSkill(_ context.Context, agentID, skillID string, version, sortOrder int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.skills[skillID]; !ok {
		return ErrSkillNotFound
	}
	bindings := s.agentBindings[agentID]
	for _, b := range bindings {
		if b.SkillID == skillID {
			b.Version = version
			b.SortOrder = sortOrder
			return nil
		}
	}
	s.agentBindings[agentID] = append(bindings, &AgentSkill{
		AgentID:   agentID,
		SkillID:   skillID,
		Version:   version,
		SortOrder: sortOrder,
		CreatedAt: time.Now(),
	})
	return nil
}

func (s *memStore) UnbindAgentSkill(_ context.Context, agentID, skillID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	bindings := s.agentBindings[agentID]
	kept := bindings[:0]
	for _, b := range bindings {
		if b.SkillID != skillID {
			kept = append(kept, b)
		}
	}
	s.agentBindings[agentID] = kept
	return nil
}

func (s *memStore) ListAgentSkills(_ context.Context, agentID string) ([]*AgentSkill, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	bindings := s.agentBindings[agentID]
	out := make([]*AgentSkill, 0, len(bindings))
	for _, b := range bindings {
		cp := *b
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SortOrder < out[j].SortOrder })
	return out, nil
}

func copySkill(s *Skill) *Skill {
	cp := *s
	cp.Visibility = asset.VisibilityOrDefault(cp.Visibility)
	return &cp
}
