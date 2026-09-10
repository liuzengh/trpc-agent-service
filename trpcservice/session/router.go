// Package session contains platform Session provider assembly.
package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
)

// Resolver resolves a framework Session service for a prepared execution.
type Resolver interface {
	ResolveSession(context.Context, worker.Execution) (frameworksession.Service, error)
	Close() error
}

type sessionKeyLister interface {
	ListSessionKeys(context.Context, worker.Execution) ([]frameworksession.Key, error)
}

// Router selects a Session resolver by backend provider.
type Router struct {
	postgres Resolver
	redis    Resolver
	inmemory Resolver
}

const (
	postgresProvider = "postgres"
	redisProvider    = "redis"
	inmemoryProvider = "inmemory"
)

// NewRouter creates the built-in Session routes. PostgreSQL and Redis are
// required for the production router; when the optional in-memory route is
// supplied it may also be used to build a local/test-only router without the
// persistent providers.
func NewRouter(postgres Resolver, redis Resolver, inMemory ...Resolver) (*Router, error) {
	if len(inMemory) > 1 {
		return nil, errors.New("only one in-memory session provider is supported")
	}
	if len(inMemory) == 1 && inMemory[0] == nil {
		return nil, errors.New("in-memory session provider must not be nil")
	}
	if len(inMemory) == 0 && (postgres == nil || redis == nil) {
		return nil, errors.New("postgres and redis session providers are required")
	}
	router := &Router{postgres: postgres, redis: redis}
	if len(inMemory) == 1 {
		router.inmemory = inMemory[0]
	}
	return router, nil
}

// ValidateBackend checks whether the platform's built-in Session provider
// providers can resolve ref.
func ValidateBackend(ref tenant.BackendRef) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	switch ref.Provider {
	case postgresProvider:
		if ref.Kind != tenant.BackendSQL {
			return fmt.Errorf("session backend kind %q does not match provider %q", ref.Kind, ref.Provider)
		}
	case redisProvider:
		if ref.Kind != tenant.BackendRedis {
			return fmt.Errorf("session backend kind %q does not match provider %q", ref.Kind, ref.Provider)
		}
	case inmemoryProvider:
		if ref.Kind != tenant.BackendInMemory {
			return fmt.Errorf("session backend kind %q does not match provider %q", ref.Kind, ref.Provider)
		}
		if ref.SecretRef != (tenant.SecretRef{}) {
			return errors.New("in-memory session backend must not use secret_ref")
		}
	default:
		return fmt.Errorf("session provider %q is not supported", ref.Provider)
	}
	return nil
}

// ResolveSession selects the provider registered for the execution Session backend.
func (r *Router) ResolveSession(ctx context.Context, exec worker.Execution) (frameworksession.Service, error) {
	if r == nil {
		return nil, errors.New("session router is not initialized")
	}
	provider := exec.Config.BackendConfig.Session.Provider
	var resolver Resolver
	switch provider {
	case postgresProvider:
		resolver = r.postgres
	case redisProvider:
		resolver = r.redis
	case inmemoryProvider:
		resolver = r.inmemory
	default:
		return nil, fmt.Errorf("session provider %q is not supported", provider)
	}
	if resolver == nil {
		return nil, fmt.Errorf("session provider %q is not configured", provider)
	}
	return resolver.ResolveSession(ctx, exec)
}

// ListSessionKeys returns the backend-owned inventory needed by migration.
// Only Redis has a backend key inventory; SQL inventory remains in PostgreSQL.
func (r *Router) ListSessionKeys(ctx context.Context, exec worker.Execution) ([]frameworksession.Key, error) {
	if r == nil {
		return nil, errors.New("session router is not initialized")
	}
	if exec.Config.BackendConfig.Session.Provider != redisProvider {
		return nil, fmt.Errorf("session provider %q does not expose redis inventory", exec.Config.BackendConfig.Session.Provider)
	}
	lister, ok := r.redis.(sessionKeyLister)
	if !ok {
		return nil, errors.New("redis session provider does not support inventory")
	}
	return lister.ListSessionKeys(ctx, exec)
}

// Close closes all configured providers owned by Router.
func (r *Router) Close() error {
	if r == nil {
		return nil
	}
	var errs []error
	if r.postgres != nil {
		errs = append(errs, r.postgres.Close())
	}
	if r.redis != nil {
		errs = append(errs, r.redis.Close())
	}
	if r.inmemory != nil {
		errs = append(errs, r.inmemory.Close())
	}
	return errors.Join(errs...)
}
