package postgres

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
	sessionpostgres "trpc.group/trpc-go/trpc-agent-go/session/postgres"
)

// BackendProfileReader supplies immutable, versioned Backend Profile facts.
// It is deliberately narrower than the provider repository so this resolver
// cannot mutate control-plane state.
type BackendProfileReader interface {
	GetBackend(context.Context, string, string, int64) (provider.BackendProfileSnapshot, error)
}

// ProfileServiceResolver maps a published session Backend Profile to an
// official trpc-agent-go PostgreSQL Session service. Profile configuration
// carries only connection_id; the connection material stays in Connections,
// which is injected by the deployment rather than stored per tenant.
type ProfileServiceResolver struct {
	Profiles    BackendProfileReader
	Connections map[string]string

	mu       sync.Mutex
	services map[string]*sessionpostgres.Service
}

// NewProfileServiceResolver validates a deployment-scoped connection registry.
// Services are opened lazily, so adding a connection does not create a network
// dependency until a published tenant binding actually selects it.
func NewProfileServiceResolver(profiles BackendProfileReader, connections map[string]string) (*ProfileServiceResolver, error) {
	if profiles == nil || len(connections) == 0 {
		return nil, runtime.ErrCapabilityUnsupported
	}
	copy := make(map[string]string, len(connections))
	for connectionID, dsn := range connections {
		if !validConnectionID(connectionID) || !validDSN(dsn) {
			return nil, runtime.ErrInvariantViolation
		}
		copy[connectionID] = strings.TrimSpace(dsn)
	}
	return &ProfileServiceResolver{Profiles: profiles, Connections: copy, services: make(map[string]*sessionpostgres.Service)}, nil
}

func (r *ProfileServiceResolver) Resolve(ctx context.Context, snapshot profile.ExecutionProfileSnapshot) (agentsession.Service, error) {
	if r == nil || r.Profiles == nil || snapshot.Key.TenantID == "" || snapshot.Key.ConfigVersion < 1 {
		return nil, runtime.ErrCapabilityUnsupported
	}
	binding, ok := snapshot.BackendBindingFor("session")
	if !ok || !requiresCapability(binding.Required, "atomic_turn_commit") {
		return nil, runtime.ErrCapabilityUnsupported
	}
	connectionID, dsn, err := r.ResolvePostgresDSN(ctx, snapshot.Key.TenantID, binding.BackendProfileID, binding.BackendVersion)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.services == nil {
		return nil, runtime.ErrBackendUnavailable
	}
	if service := r.services[connectionID]; service != nil {
		return service, nil
	}
	service, err := NewOfficialSessionService(dsn)
	if err != nil {
		return nil, err
	}
	r.services[connectionID] = service
	return service, nil
}

// ResolvePostgresDSN is for trusted process composition roots such as the
// Session migration job. It resolves an exact immutable Backend Profile to a
// deployment-injected data-plane DSN; callers must never log or persist the
// returned value.
func (r *ProfileServiceResolver) ResolvePostgresDSN(ctx context.Context, tenantID, profileID string, version int64) (string, string, error) {
	if r == nil || r.Profiles == nil || tenantID == "" || profileID == "" || version < 1 {
		return "", "", runtime.ErrCapabilityUnsupported
	}
	backend, err := r.Profiles.GetBackend(ctx, tenantID, profileID, version)
	if err != nil {
		return "", "", err
	}
	if backend.TenantID != tenantID || backend.ProfileID != profileID || backend.Version != version {
		return "", "", runtime.ErrTenantScope
	}
	if backend.Status != "active" || backend.Provider != "postgres" || (backend.SchemaVersion != 1 && backend.SchemaVersion != 2) ||
		backend.CredentialRef.Ref != "" || backend.CredentialRef.Version != 0 || !backend.Capabilities["atomic_turn_commit"] {
		return "", "", runtime.ErrCapabilityUnsupported
	}
	connectionID := "default"
	if backend.SchemaVersion == 2 {
		connectionID = backend.Configuration["connection_id"]
	}
	if !validConnectionID(connectionID) {
		return "", "", runtime.ErrInvariantViolation
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.services == nil {
		return "", "", runtime.ErrBackendUnavailable
	}
	dsn, ok := r.Connections[connectionID]
	if !ok {
		return "", "", fmt.Errorf("%w: session postgres connection %q is not deployed", runtime.ErrCapabilityUnsupported, connectionID)
	}
	return connectionID, dsn, nil
}

// Close releases every lazily constructed framework service. It is safe to
// call after a partial startup failure and intentionally leaves the resolver
// unusable rather than letting a drained Worker reopen data-plane clients.
func (r *ProfileServiceResolver) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	services := r.services
	r.services = nil
	r.mu.Unlock()
	var first error
	for _, service := range services {
		if err := service.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func validConnectionID(value string) bool {
	if len(value) == 0 || len(value) > 128 || strings.TrimSpace(value) != value {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '_' || char == '-') {
			return false
		}
	}
	return true
}

func validDSN(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && !strings.ContainsAny(value, "\x00\r\n")
}

func requiresCapability(required []string, capability string) bool {
	for _, value := range required {
		if value == capability {
			return true
		}
	}
	return false
}

var _ profile.SessionServiceResolver = (*ProfileServiceResolver)(nil)
