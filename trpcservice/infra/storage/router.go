package storage

import (
	"context"
	"errors"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tenant"
)

// Router resolves per-tenant backend implementations from the tenant's
// data_backend selection, so an operator can swap a tenant's session or memory
// backend by editing tenant data — no code changes, no restart.
//
// One instance per backend id is cached and shared across tenants: isolation
// comes from the tenant id flowing through each call (framework AppName), not
// from instance separation.
type Router struct {
	tenants *tenant.Manager
	sessCfg SessionConfig
	memCfg  MemoryConfig

	mu       sync.Mutex
	sessions map[string]*Sessions
	memories map[string]*Memories
}

// NewRouter returns a router using the given default backend configs. A tenant
// without an explicit data_backend selection uses these defaults.
func NewRouter(tenants *tenant.Manager, sessCfg SessionConfig, memCfg MemoryConfig) *Router {
	return &Router{
		tenants:  tenants,
		sessCfg:  sessCfg,
		memCfg:   memCfg,
		sessions: make(map[string]*Sessions),
		memories: make(map[string]*Memories),
	}
}

// Sessions returns the session store selected by the tenant.
func (r *Router) Sessions(ctx context.Context, tenantID string) (*Sessions, error) {
	backend, err := r.backendFor(ctx, tenantID, tenant.DomainSession, string(r.sessCfg.Backend))
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.sessions[backend]; ok {
		return s, nil
	}
	cfg := r.sessCfg
	cfg.Backend = Backend(backend)
	s, err := NewSessions(cfg)
	if err != nil {
		return nil, err
	}
	r.sessions[backend] = s
	return s, nil
}

// Memories returns the memory store selected by the tenant.
func (r *Router) Memories(ctx context.Context, tenantID string) (*Memories, error) {
	backend, err := r.backendFor(ctx, tenantID, tenant.DomainMemory, string(r.memCfg.Backend))
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if m, ok := r.memories[backend]; ok {
		return m, nil
	}
	cfg := r.memCfg
	cfg.Backend = Backend(backend)
	m, err := NewMemories(cfg)
	if err != nil {
		return nil, err
	}
	r.memories[backend] = m
	return m, nil
}

// backendFor resolves the backend id for a domain: the tenant's explicit
// data_backend selection wins, otherwise the configured default. A missing
// tenant falls back to the default (sessions may be created before the tenant
// is registered; isolation still holds because the tenant id is the key).
func (r *Router) backendFor(ctx context.Context, tenantID, domain, def string) (string, error) {
	if tenantID == "" {
		return def, nil
	}
	t, err := r.tenants.Get(ctx, tenantID)
	if err != nil {
		if errors.Is(err, tenant.ErrNotFound) {
			return def, nil
		}
		return "", err
	}
	if v, ok := t.DataBackend[domain]; ok && v != "" {
		return v, nil
	}
	return def, nil
}
