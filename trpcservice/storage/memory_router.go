package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"golang.org/x/sync/singleflight"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	memorypostgres "trpc.group/trpc-go/trpc-agent-go/memory/postgres"
	memoryredis "trpc.group/trpc-go/trpc-agent-go/memory/redis"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

const memoryResourceType = "memory"

type MemoryRouter struct {
	repository controlplane.Repository
	secrets    secret.Store
	mu         sync.RWMutex
	closed     bool
	services   map[string]memory.Service
	group      singleflight.Group
}

func NewMemoryRouter(
	repository controlplane.Repository,
	secretStore secret.Store,
) (*MemoryRouter, error) {
	if repository == nil || secretStore == nil {
		return nil, fmt.Errorf("memory router repository and secret store are required")
	}
	return &MemoryRouter{
		repository: repository,
		secrets:    secretStore,
		services:   make(map[string]memory.Service),
	}, nil
}

func (r *MemoryRouter) AddMemory(
	ctx context.Context,
	key memory.UserKey,
	value string,
	topics []string,
	opts ...memory.AddOption,
) error {
	service, err := r.serviceFor(ctx, key.AppName)
	if err != nil {
		return err
	}
	return service.AddMemory(ctx, key, value, topics, opts...)
}

func (r *MemoryRouter) UpdateMemory(
	ctx context.Context,
	key memory.Key,
	value string,
	topics []string,
	opts ...memory.UpdateOption,
) error {
	service, err := r.serviceFor(ctx, key.AppName)
	if err != nil {
		return err
	}
	return service.UpdateMemory(ctx, key, value, topics, opts...)
}

func (r *MemoryRouter) DeleteMemory(ctx context.Context, key memory.Key) error {
	service, err := r.serviceFor(ctx, key.AppName)
	if err != nil {
		return err
	}
	return service.DeleteMemory(ctx, key)
}

func (r *MemoryRouter) ClearMemories(ctx context.Context, key memory.UserKey) error {
	service, err := r.serviceFor(ctx, key.AppName)
	if err != nil {
		return err
	}
	return service.ClearMemories(ctx, key)
}

func (r *MemoryRouter) ReadMemories(
	ctx context.Context,
	key memory.UserKey,
	limit int,
) ([]*memory.Entry, error) {
	service, err := r.serviceFor(ctx, key.AppName)
	if err != nil {
		return nil, err
	}
	return service.ReadMemories(ctx, key, limit)
}

func (r *MemoryRouter) SearchMemories(
	ctx context.Context,
	key memory.UserKey,
	query string,
	opts ...memory.SearchOption,
) ([]*memory.Entry, error) {
	service, err := r.serviceFor(ctx, key.AppName)
	if err != nil {
		return nil, err
	}
	return service.SearchMemories(ctx, key, query, opts...)
}

// Tools are registered by the platform catalog so their permission policy is
// tenant scoped. Returning backend tools here would expose a global union.
func (r *MemoryRouter) Tools() []tool.Tool { return nil }

func (r *MemoryRouter) EnqueueAutoMemoryJob(ctx context.Context, sess *session.Session) error {
	if sess == nil {
		return nil
	}
	service, err := r.serviceFor(ctx, sess.AppName)
	if err != nil {
		return err
	}
	return service.EnqueueAutoMemoryJob(ctx, sess)
}

func (r *MemoryRouter) Ready(ctx context.Context) error {
	if r == nil || r.repository == nil {
		return errors.New("memory router is not initialized")
	}
	return r.repository.Ready(ctx)
}

func (r *MemoryRouter) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	services := make([]memory.Service, 0, len(r.services))
	for _, service := range r.services {
		services = append(services, service)
	}
	r.mu.Unlock()
	var closeErr error
	for _, service := range services {
		closeErr = errors.Join(closeErr, service.Close())
	}
	return closeErr
}

func (r *MemoryRouter) serviceFor(ctx context.Context, appName string) (memory.Service, error) {
	tenantID, appID, err := runtimecontext.ParseStorageScope(appName)
	if err != nil {
		return nil, err
	}
	binding, err := resolveBackendBinding(ctx, r.repository, tenantID, appID, memoryResourceType)
	if err != nil {
		return nil, err
	}
	cacheKey := binding.ID + "\x00" + fmt.Sprint(binding.Version)
	r.mu.RLock()
	service := r.services[cacheKey]
	closed := r.closed
	r.mu.RUnlock()
	if closed {
		return nil, errors.New("memory router is closed")
	}
	if service != nil {
		return service, nil
	}
	value, err, _ := r.group.Do(cacheKey, func() (any, error) {
		r.mu.RLock()
		cached := r.services[cacheKey]
		closed := r.closed
		r.mu.RUnlock()
		if closed {
			return nil, errors.New("memory router is closed")
		}
		if cached != nil {
			return cached, nil
		}
		built, err := r.build(ctx, binding)
		if err != nil {
			return nil, err
		}
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			_ = built.Close()
			return nil, errors.New("memory router is closed")
		}
		r.services[cacheKey] = built
		r.mu.Unlock()
		return built, nil
	})
	if err != nil {
		return nil, err
	}
	return value.(memory.Service), nil
}

type memoryBackendConfig struct {
	URL         string `json:"url"`
	KeyPrefix   string `json:"key_prefix"`
	DSN         string `json:"dsn"`
	TableName   string `json:"table_name"`
	Schema      string `json:"schema"`
	MemoryLimit int    `json:"memory_limit"`
}

func (r *MemoryRouter) build(
	ctx context.Context,
	binding controlplane.BackendBinding,
) (memory.Service, error) {
	var cfg memoryBackendConfig
	if err := decodeStorageConfig(binding.Config, &cfg); err != nil {
		return nil, fmt.Errorf("decode memory binding %q: %w", binding.ID, err)
	}
	if cfg.MemoryLimit < 0 {
		return nil, fmt.Errorf("memory binding %q has a negative limit", binding.ID)
	}
	switch strings.ToLower(binding.BackendType) {
	case "inmemory":
		options := []memoryinmemory.ServiceOpt{}
		if cfg.MemoryLimit > 0 {
			options = append(options, memoryinmemory.WithMemoryLimit(cfg.MemoryLimit))
		}
		return memoryinmemory.NewMemoryService(options...), nil
	case "redis":
		url, err := r.endpoint(ctx, cfg.URL, binding.SecretRef)
		if err != nil {
			return nil, err
		}
		options := []memoryredis.ServiceOpt{memoryredis.WithRedisClientURL(url)}
		if cfg.KeyPrefix != "" {
			options = append(options, memoryredis.WithKeyPrefix(cfg.KeyPrefix))
		}
		if cfg.MemoryLimit > 0 {
			options = append(options, memoryredis.WithMemoryLimit(cfg.MemoryLimit))
		}
		return memoryredis.NewService(options...)
	case "postgres":
		dsn, err := r.endpoint(ctx, cfg.DSN, binding.SecretRef)
		if err != nil {
			return nil, err
		}
		options := []memorypostgres.ServiceOpt{memorypostgres.WithPostgresClientDSN(dsn)}
		if cfg.TableName != "" {
			options = append(options, memorypostgres.WithTableName(cfg.TableName))
		}
		if cfg.Schema != "" {
			options = append(options, memorypostgres.WithSchema(cfg.Schema))
		}
		if cfg.MemoryLimit > 0 {
			options = append(options, memorypostgres.WithMemoryLimit(cfg.MemoryLimit))
		}
		return memorypostgres.NewService(options...)
	default:
		return nil, fmt.Errorf("unsupported memory backend %q", binding.BackendType)
	}
}

func (r *MemoryRouter) endpoint(
	ctx context.Context,
	configured string,
	secretRef string,
) (string, error) {
	if secretRef != "" {
		value, err := r.secrets.Resolve(ctx, secretRef)
		if err != nil {
			return "", err
		}
		return value, nil
	}
	if strings.TrimSpace(configured) == "" {
		return "", errors.New("storage endpoint or secret_ref is required")
	}
	return configured, nil
}

func resolveBackendBinding(
	ctx context.Context,
	repository controlplane.Repository,
	tenantID string,
	appID string,
	resourceType string,
) (controlplane.BackendBinding, error) {
	bindings, err := repository.ListBackendBindings(ctx, tenantID, appID)
	if err != nil {
		return controlplane.BackendBinding{}, err
	}
	var tenantDefault *controlplane.BackendBinding
	for index := range bindings {
		binding := bindings[index]
		if binding.ResourceType != resourceType || binding.MigrationState != "active" {
			continue
		}
		if binding.AppID == appID {
			return binding, nil
		}
		if binding.AppID == "" && tenantDefault == nil {
			copy := binding
			tenantDefault = &copy
		}
	}
	if tenantDefault != nil {
		return *tenantDefault, nil
	}
	return controlplane.BackendBinding{}, fmt.Errorf(
		"active %s backend binding not found for tenant %q app %q",
		resourceType, tenantID, appID,
	)
}

func decodeStorageConfig(raw json.RawMessage, target any) error {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return errors.New("configuration must contain one JSON value")
	}
	return nil
}

var _ memory.Service = (*MemoryRouter)(nil)
