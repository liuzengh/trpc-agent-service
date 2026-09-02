// Package tool registers platform tools and tenant-scoped MCP / function tools.
package tool

import (
	"context"
	"errors"
	"sort"
	"sync"

	fwtool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

// Risk levels used by governance (high-risk tools require approval).
const (
	RiskLow    = "low"
	RiskMedium = "medium"
	RiskHigh   = "high"
)

// Scope values for a tool definition. An empty scope is treated as global.
const (
	ScopeGlobal = "global"
	ScopeTenant = "tenant"
)

// ErrNotFound is returned when a tool does not exist.
var ErrNotFound = errors.New("tool: not found")

// Definition describes a registered tool's metadata. Scope and TenantID
// implement tenant isolation: an empty scope means a global tool visible to
// every tenant; tenant-scoped tools are visible only to their owning tenant.
type Definition struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	RiskLevel   string `json:"risk_level"`
	Scope       string `json:"scope,omitempty"`
	TenantID    string `json:"tenant_id,omitempty"`
}

// store is the persistence contract behind Registry. The in-memory store
// keeps the service runnable without MySQL; the MySQL store (tool_mysql.go)
// is the production path.
type store interface {
	Register(ctx context.Context, d Definition) error
	Get(ctx context.Context, id string) (Definition, error)
	List(ctx context.Context, tenantID string) ([]Definition, error)
	Grant(ctx context.Context, agentID, toolID string) error
	Revoke(ctx context.Context, agentID, toolID string) error
	IsAllowed(ctx context.Context, agentID, toolID string) (bool, error)
	Allowed(ctx context.Context, agentID string) ([]string, error)
	GrantedAgents(ctx context.Context, toolID string) ([]string, error)
}

// Registry stores tool definitions and per-agent grants (RBAC) behind a
// swappable store.
type Registry struct {
	store store
}

// NewRegistry returns an in-memory tool registry.
func NewRegistry() *Registry {
	return &Registry{store: newMemStore()}
}

// Register adds or replaces a tool definition.
func (r *Registry) Register(ctx context.Context, d Definition) error {
	return r.store.Register(ctx, d)
}

// Get returns a tool definition by id, or ErrNotFound.
func (r *Registry) Get(ctx context.Context, id string) (Definition, error) {
	return r.store.Get(ctx, id)
}

// List returns the tools visible to a tenant (global + its own); an empty
// tenantID returns all.
func (r *Registry) List(ctx context.Context, tenantID string) ([]Definition, error) {
	return r.store.List(ctx, tenantID)
}

// Grant allows an agent to use a tool.
func (r *Registry) Grant(ctx context.Context, agentID, toolID string) error {
	return r.store.Grant(ctx, agentID, toolID)
}

// Revoke removes an agent's access to a tool.
func (r *Registry) Revoke(ctx context.Context, agentID, toolID string) error {
	return r.store.Revoke(ctx, agentID, toolID)
}

// IsAllowed reports whether the agent may use the tool.
func (r *Registry) IsAllowed(ctx context.Context, agentID, toolID string) (bool, error) {
	return r.store.IsAllowed(ctx, agentID, toolID)
}

// Allowed returns the tool ids granted to an agent.
func (r *Registry) Allowed(ctx context.Context, agentID string) ([]string, error) {
	return r.store.Allowed(ctx, agentID)
}

// GrantedAgents returns the agent ids a tool is granted to (RBAC admin view).
func (r *Registry) GrantedAgents(ctx context.Context, toolID string) ([]string, error) {
	return r.store.GrantedAgents(ctx, toolID)
}

// memStore keeps tools and grants in maps; the zero-dependency dev/test
// backend.
type memStore struct {
	mu     sync.RWMutex
	tools  map[string]Definition
	grants map[string]map[string]bool // agentID -> toolID -> granted
}

func newMemStore() *memStore {
	return &memStore{
		tools:  make(map[string]Definition),
		grants: make(map[string]map[string]bool),
	}
}

func (s *memStore) Register(_ context.Context, d Definition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tools[d.ID] = d
	return nil
}

func (s *memStore) Get(_ context.Context, id string) (Definition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.tools[id]
	if !ok {
		return Definition{}, ErrNotFound
	}
	return d, nil
}

func (s *memStore) List(_ context.Context, tenantID string) ([]Definition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Definition, 0, len(s.tools))
	for _, d := range s.tools {
		if tenantID == "" || d.Scope == "" || d.Scope == ScopeGlobal || d.TenantID == tenantID {
			out = append(out, d)
		}
	}
	return out, nil
}

func (s *memStore) Grant(_ context.Context, agentID, toolID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.grants[agentID] == nil {
		s.grants[agentID] = make(map[string]bool)
	}
	s.grants[agentID][toolID] = true
	return nil
}

func (s *memStore) Revoke(_ context.Context, agentID, toolID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if g := s.grants[agentID]; g != nil {
		delete(g, toolID)
	}
	return nil
}

func (s *memStore) IsAllowed(_ context.Context, agentID, toolID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.grants[agentID][toolID], nil
}

func (s *memStore) Allowed(_ context.Context, agentID string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.grants[agentID]))
	for id := range s.grants[agentID] {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

func (s *memStore) GrantedAgents(_ context.Context, toolID string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []string{}
	for agentID, granted := range s.grants {
		if granted[toolID] {
			out = append(out, agentID)
		}
	}
	sort.Strings(out)
	return out, nil
}

// EchoTool returns a built-in FunctionTool that echoes its input, used as the
// reference implementation for wrapping platform tools with FunctionTool.
func EchoTool() fwtool.Tool {
	return function.NewFunctionTool(
		func(_ context.Context, in struct {
			Text string `json:"text"`
		}) (struct {
			Text string `json:"text"`
		}, error) {
			return in, nil
		},
		function.WithName("echo"),
		function.WithDescription("Returns the input text unchanged."),
	)
}
