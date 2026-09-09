package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"

	frameworkmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	frameworkrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	ErrRegistryConfiguration = errors.New("runner registry configuration unavailable")
	ErrRegistryDependency    = errors.New("runner registry dependency unavailable")
)

type ConfiguredRunnerFactory func(
	context.Context,
	CacheKey,
	tenant.ConfigVersion,
	string,
	session.Service,
	frameworkmemory.Service,
) (frameworkrunner.Runner, error)

type RunnerRegistry struct {
	repository   tenant.Repository
	backends     *storage.BackendProvider
	credentials  config.CredentialResolver
	cache        *RunnerCache
	generationMu sync.Mutex
	generations  map[CacheKey]uint64
}

func NewRunnerRegistry(
	repository tenant.Repository,
	backends *storage.BackendProvider,
	credentials config.CredentialResolver,
	cacheConfig CacheConfig,
	factory ConfiguredRunnerFactory,
) (*RunnerRegistry, error) {
	if repository == nil {
		return nil, errors.New("tenant repository is required")
	}
	if backends == nil {
		return nil, errors.New("backend provider is required")
	}
	if credentials == nil {
		return nil, errors.New("credential resolver is required")
	}
	if factory == nil {
		return nil, errors.New("configured runner factory is required")
	}
	registry := &RunnerRegistry{
		repository:  repository,
		backends:    backends,
		credentials: credentials,
		generations: make(map[CacheKey]uint64),
	}
	cache, err := NewRunnerCache(cacheConfig, func(ctx context.Context, key CacheKey) (frameworkrunner.Runner, error) {
		configVersion, backend, err := registry.resolve(ctx, key)
		if err != nil {
			return nil, err
		}
		apiKey, err := registry.credentials.Resolve(configVersion.Model.CredentialRef)
		if err != nil {
			return nil, fmt.Errorf("%w: model credential unavailable", ErrRegistryConfiguration)
		}
		sessions, memories := backend.Session(), backend.Memory()
		if sessions == nil || memories == nil {
			return nil, fmt.Errorf("%w: storage services are not ready", ErrRegistryDependency)
		}
		return factory(ctx, key, configVersion, apiKey, sessions, memories)
	})
	if err != nil {
		return nil, err
	}
	registry.cache = cache
	return registry, nil
}

type RegistryLease struct {
	Runner  frameworkrunner.Runner
	Backend storage.Backend
	lease   *Lease
}

func (l *RegistryLease) Release() {
	if l == nil || l.lease == nil {
		return
	}
	l.lease.Release()
}

func (r *RunnerRegistry) Acquire(ctx context.Context, key CacheKey) (*RegistryLease, error) {
	_, backend, err := r.resolve(ctx, key)
	if err != nil {
		return nil, err
	}
	generation := uint64(0)
	if source, ok := backend.(interface{ ResourceGeneration() uint64 }); ok {
		generation = source.ResourceGeneration()
	}
	lease, err := r.acquireGeneration(ctx, key, generation)
	if err != nil {
		return nil, err
	}
	return &RegistryLease{Runner: lease.Runner, Backend: backend, lease: lease}, nil
}

func (r *RunnerRegistry) acquireGeneration(ctx context.Context, key CacheKey, generation uint64) (*Lease, error) {
	if generation == 0 {
		return r.cache.Acquire(ctx, key)
	}
	r.generationMu.Lock()
	defer r.generationMu.Unlock()
	if previous, ok := r.generations[key]; ok && previous != generation {
		if err := r.cache.Drain(ctx, key); err != nil {
			return nil, fmt.Errorf("%w: refresh stale storage runner: %v", ErrRegistryDependency, err)
		}
	}
	lease, err := r.cache.Acquire(ctx, key)
	if err == nil {
		r.generations[key] = generation
	}
	return lease, err
}

func (r *RunnerRegistry) resolve(ctx context.Context, key CacheKey) (tenant.ConfigVersion, storage.Backend, error) {
	configVersion, err := r.repository.GetConfigVersion(ctx, key.TenantID, key.AgentAppID, key.ConfigVersion)
	if err != nil {
		return tenant.ConfigVersion{}, nil, fmt.Errorf("%w: config version lookup failed", ErrRegistryConfiguration)
	}
	profile, err := r.repository.GetStorageProfile(ctx, key.TenantID, configVersion.StorageProfileID)
	if err != nil {
		return tenant.ConfigVersion{}, nil, fmt.Errorf("%w: storage profile lookup failed", ErrRegistryConfiguration)
	}
	backend, err := r.backends.BackendFor(ctx, profile)
	if err != nil {
		return tenant.ConfigVersion{}, nil, fmt.Errorf("%w: selected storage profile unavailable", ErrRegistryDependency)
	}
	return configVersion, backend, nil
}

func (r *RunnerRegistry) Ready(ctx context.Context) error {
	if err := r.cache.Available(); err != nil {
		return fmt.Errorf("runner cache: %w", err)
	}
	if err := r.backends.Ready(ctx); err != nil {
		return fmt.Errorf("storage backends: %w", err)
	}
	return nil
}

func (r *RunnerRegistry) Drain(ctx context.Context, key CacheKey) error {
	r.generationMu.Lock()
	defer r.generationMu.Unlock()
	err := r.cache.Drain(ctx, key)
	delete(r.generations, key)
	return err
}

func (r *RunnerRegistry) Close() error {
	return r.cache.Close()
}
