package storage

// This file resolves one tenant's session backend. It is where
// backend_profiles.session_backend stops being a ledger entry and becomes a
// runtime choice: the legacy gateway path builds (and caches) one session
// service per tenant with that profile's key prefix and TTL, over the
// deployment's Redis endpoint. Reliable mode does not use this router — its
// session authority is MySQL — see coordination.SessionProjection for what a
// tenant's "redis" choice means there.

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	redisession "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

// BackendSetting is one tenant's session-backend choice: the parts of a
// backend profile that matter at runtime. The Redis endpoint itself is
// deployment-wide (the platform has one; a tenant picks the backend, not the
// host).
type BackendSetting struct {
	Backend    string // "memory" or "redis"; "" falls back to the default
	KeyPrefix  string // redis key namespace; empty gets a per-tenant default
	SessionTTL time.Duration
}

// Router serves session services per tenant. The default service serves
// tenants without a profile; a profile builds a per-tenant service on first
// use and caches it until the next ApplyProfiles.
type Router struct {
	defaultSvc session.Service
	redisURL   string

	mu       sync.Mutex
	settings map[string]BackendSetting
	built    map[string]session.Service
}

// NewRouter wires the router over the deployment's default session service
// and Redis endpoint (the endpoint is only used by tenants whose profile
// selects redis).
func NewRouter(defaultSvc session.Service, redisURL string) *Router {
	return &Router{
		defaultSvc: defaultSvc,
		redisURL:   redisURL,
		settings:   map[string]BackendSetting{},
		built:      map[string]session.Service{},
	}
}

// ApplyProfiles replaces the tenant table wholesale and drops every built
// service; the old ones are closed after the swap, so a request racing the
// reload finishes on the old service while the next one lands on the new.
// An empty map means "no tenant has a profile" and everything falls back to
// the default service.
func (r *Router) ApplyProfiles(settings map[string]BackendSetting) {
	r.mu.Lock()
	old := r.built
	r.settings = settings
	r.built = make(map[string]session.Service, len(settings))
	r.mu.Unlock()
	for _, svc := range old {
		if svc != r.defaultSvc {
			_ = svc.Close()
		}
	}
}

// For returns the session service of one tenant. A backend that cannot be
// built (an unknown name, an unreachable-shaped URL) logs and falls back to
// the default: this is the message hot path and refusing to answer is worse
// than serving on the platform default — the misconfiguration is visible in
// the log and in the tenant's next restart.
func (r *Router) For(tenantID string) session.Service {
	r.mu.Lock()
	defer r.mu.Unlock()
	if svc, ok := r.built[tenantID]; ok {
		return svc
	}
	setting, ok := r.settings[tenantID]
	if !ok || setting.Backend == "" {
		r.built[tenantID] = r.defaultSvc
		return r.defaultSvc
	}
	svc, err := r.build(tenantID, setting)
	if err != nil {
		slog.Error("session router: falling back to the default backend",
			"tenant", tenantID, "backend", setting.Backend, "err", err)
		svc = r.defaultSvc
	}
	r.built[tenantID] = svc
	return svc
}

// build constructs the tenant's service. Redis uses the tenant prefix as its
// namespace; an empty prefix gets "tas:<tenant>:" so a tenant-level backend
// can never share the framework's default keyspace with another tenant.
func (r *Router) build(tenantID string, s BackendSetting) (session.Service, error) {
	switch strings.ToLower(s.Backend) {
	case "memory":
		return inmemory.NewSessionService(), nil
	case "redis":
		prefix := s.KeyPrefix
		if prefix == "" {
			prefix = "tas:" + tenantID + ":"
		}
		return redisession.NewService(
			redisession.WithRedisClientURL(r.redisURL),
			redisession.WithKeyPrefix(prefix),
			redisession.WithSessionTTL(s.SessionTTL),
		)
	default:
		return nil, fmt.Errorf("unknown session backend %q (want memory or redis)", s.Backend)
	}
}

// Close releases every built service except the default (which the caller
// owns) and makes further For calls return the default.
func (r *Router) Close() error {
	r.mu.Lock()
	old := r.built
	r.built = map[string]session.Service{}
	r.mu.Unlock()
	for _, svc := range old {
		if svc != r.defaultSvc {
			_ = svc.Close()
		}
	}
	return nil
}
