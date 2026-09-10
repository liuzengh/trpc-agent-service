// Package inmemory wires the development-only in-memory Session provider.
package inmemory

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
	frameworkinmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

const providerName = "inmemory"

// Resolver owns one official in-memory Session service per immutable backend
// scope. It is intentionally not wired into the production multi-worker
// runtime: the provider has no persistence or cross-process visibility.
type Resolver struct {
	options []frameworkinmemory.ServiceOpt

	mu       sync.Mutex
	closed   bool
	services map[string]frameworksession.Service
}

// NewResolver creates a local/test-only in-memory Session resolver.
func NewResolver(options ...frameworkinmemory.ServiceOpt) (*Resolver, error) {
	for i, option := range options {
		if option == nil {
			return nil, fmt.Errorf("in-memory session option %d is nil", i)
		}
	}
	return &Resolver{
		options:  append([]frameworkinmemory.ServiceOpt(nil), options...),
		services: make(map[string]frameworksession.Service),
	}, nil
}

// NewSessionResolver is an explicit alias matching the persistent provider
// constructors.
func NewSessionResolver(options ...frameworkinmemory.ServiceOpt) (*Resolver, error) {
	return NewResolver(options...)
}

// ResolveSession returns the official in-memory service selected by exec's
// pinned configuration. The cache key includes the full tenant/app/config and
// backend identity, including backend options.
func (r *Resolver) ResolveSession(ctx context.Context, exec worker.Execution) (frameworksession.Service, error) {
	if r == nil {
		return nil, errors.New("in-memory session resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ref := exec.Config.BackendConfig.Session
	if err := ValidateBackend(ref); err != nil {
		return nil, err
	}
	key, err := sessionCacheKey(exec)
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("in-memory session resolver is closed")
	}
	if service := r.services[key]; service != nil {
		return service, nil
	}
	service := frameworkinmemory.NewSessionService(r.options...)
	r.services[key] = service
	return service, nil
}

// Close closes every cached in-memory Session service. Repeated calls are
// safe and no service can be resolved after the first call.
func (r *Resolver) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	services := r.services
	r.services = nil
	r.mu.Unlock()

	var result error
	for _, service := range services {
		result = errors.Join(result, service.Close())
	}
	return result
}

// ValidateBackend checks that ref selects the development-only provider.
func ValidateBackend(ref tenant.BackendRef) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	if ref.Kind != tenant.BackendInMemory {
		return fmt.Errorf("in-memory session backend kind %q is invalid", ref.Kind)
	}
	if ref.Provider != providerName {
		return fmt.Errorf("unsupported session provider %q", ref.Provider)
	}
	if ref.SecretRef != (tenant.SecretRef{}) {
		return errors.New("in-memory session backend must not use secret_ref")
	}
	return nil
}

func sessionCacheKey(exec worker.Execution) (string, error) {
	ref := exec.Config.BackendConfig.Session
	parts := []string{
		exec.Tenant.ConfigVersion,
		exec.Config.BackendConfig.Name,
		string(ref.Kind),
		ref.Provider,
		ref.Name,
	}
	optionKeys := make([]string, 0, len(ref.Options))
	for key := range ref.Options {
		optionKeys = append(optionKeys, key)
	}
	sort.Strings(optionKeys)
	for _, key := range optionKeys {
		// The combined part remains non-empty even when an option value is
		// intentionally empty, while preserving distinct option maps.
		parts = append(parts, "option", key+"\x00"+ref.Options[key])
	}
	return exec.Tenant.Scope().Key("session-service", parts...)
}

var _ interface {
	ResolveSession(context.Context, worker.Execution) (frameworksession.Service, error)
	Close() error
} = (*Resolver)(nil)
