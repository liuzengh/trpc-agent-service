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

type MemoryMigrationVerification struct {
	MigrationID string   `json:"migration_id"`
	UserID      string   `json:"user_id"`
	SourceCount int      `json:"source_count"`
	TargetCount int      `json:"target_count"`
	Missing     []string `json:"missing,omitempty"`
	Mismatched  []string `json:"mismatched,omitempty"`
	Passed      bool     `json:"passed"`
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
	if !resourceHeld(ctx, key.AppName, "memory") {
		return resourceDo(ctx, r.repository, key.AppName, "memory", resourceSubject(key.UserID, ""), true, func(ctx context.Context) error { return r.AddMemory(ctx, key, value, topics, opts...) })
	}
	ctx, span := startStorageSpan(ctx, "memory.add", key.AppName)
	defer span.End()
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
	if !resourceHeld(ctx, key.AppName, "memory") {
		return resourceDo(ctx, r.repository, key.AppName, "memory", resourceSubject(key.UserID, ""), true, func(ctx context.Context) error { return r.UpdateMemory(ctx, key, value, topics, opts...) })
	}
	ctx, span := startStorageSpan(ctx, "memory.update", key.AppName)
	defer span.End()
	service, err := r.serviceFor(ctx, key.AppName)
	if err != nil {
		return err
	}
	return service.UpdateMemory(ctx, key, value, topics, opts...)
}

func (r *MemoryRouter) DeleteMemory(ctx context.Context, key memory.Key) error {
	if !resourceHeld(ctx, key.AppName, "memory") {
		return resourceDo(ctx, r.repository, key.AppName, "memory", resourceSubject(key.UserID, ""), true, func(ctx context.Context) error { return r.DeleteMemory(ctx, key) })
	}
	ctx, span := startStorageSpan(ctx, "memory.delete", key.AppName)
	defer span.End()
	service, err := r.serviceFor(ctx, key.AppName)
	if err != nil {
		return err
	}
	return service.DeleteMemory(ctx, key)
}

func (r *MemoryRouter) ClearMemories(ctx context.Context, key memory.UserKey) error {
	if !resourceHeld(ctx, key.AppName, "memory") {
		return resourceDo(ctx, r.repository, key.AppName, "memory", resourceSubject(key.UserID, ""), true, func(ctx context.Context) error { return r.ClearMemories(ctx, key) })
	}
	ctx, span := startStorageSpan(ctx, "memory.clear", key.AppName)
	defer span.End()
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
	if !resourceHeld(ctx, key.AppName, "memory") {
		return resourceValue(ctx, r.repository, key.AppName, "memory", resourceSubject(key.UserID, ""), false, func(ctx context.Context) ([]*memory.Entry, error) { return r.ReadMemories(ctx, key, limit) })
	}
	ctx, span := startStorageSpan(ctx, "memory.read", key.AppName)
	defer span.End()
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
	if !resourceHeld(ctx, key.AppName, "memory") {
		return resourceValue(ctx, r.repository, key.AppName, "memory", resourceSubject(key.UserID, ""), false, func(ctx context.Context) ([]*memory.Entry, error) { return r.SearchMemories(ctx, key, query, opts...) })
	}
	ctx, span := startStorageSpan(ctx, "memory.search", key.AppName)
	defer span.End()
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

func (r *MemoryRouter) BackfillUser(
	ctx context.Context,
	tenantID string,
	migrationID string,
	userID string,
) (MemoryMigrationVerification, error) {
	m, err := r.repository.GetBackendMigration(ctx, tenantID, migrationID)
	if err != nil {
		return MemoryMigrationVerification{}, err
	}
	app := "t/" + tenantID + "/a/" + m.AppID
	if !resourceHeld(ctx, app, "memory") {
		return resourceValue(ctx, r.repository, app, "memory", resourceSubject(userID, ""), false, func(ctx context.Context) (MemoryMigrationVerification, error) {
			return r.BackfillUser(ctx, tenantID, migrationID, userID)
		})
	}
	migration, source, target, err := r.migrationServices(ctx, tenantID, migrationID)
	if err != nil {
		return MemoryMigrationVerification{}, err
	}
	appName := "t/" + migration.TenantID + "/a/" + migration.AppID
	key := memory.UserKey{AppName: appName, UserID: userID}
	entries, err := source.ReadMemories(ctx, key, 100001)
	if err != nil {
		return MemoryMigrationVerification{}, err
	}
	if len(entries) > 100000 {
		return MemoryMigrationVerification{}, errors.New("Memory migration exceeds scan bound")
	}
	targets, err := target.ReadMemories(ctx, key, 100001)
	if err != nil {
		return MemoryMigrationVerification{}, err
	}
	if len(targets) > 100000 {
		return MemoryMigrationVerification{}, errors.New("Memory migration exceeds scan bound")
	}
	wanted := map[string]bool{}
	for _, e := range entries {
		if e != nil {
			wanted[e.ID] = true
		}
	}
	for _, e := range targets {
		if e != nil && !wanted[e.ID] {
			if err = target.DeleteMemory(ctx, memory.Key{AppName: key.AppName, UserID: key.UserID, MemoryID: e.ID}); err != nil {
				return MemoryMigrationVerification{}, err
			}
		}
	}
	for _, entry := range entries {
		if entry == nil || entry.Memory == nil {
			continue
		}
		metadata := &memory.Metadata{
			Kind: entry.Memory.Kind, EventTime: entry.Memory.EventTime,
			Participants: append([]string(nil), entry.Memory.Participants...),
			Location:     entry.Memory.Location,
		}
		if err := target.AddMemory(
			ctx, key, entry.Memory.Memory, append([]string(nil), entry.Memory.Topics...),
			memory.WithMetadata(metadata),
		); err != nil {
			return MemoryMigrationVerification{}, fmt.Errorf("backfill memory %q: %w", entry.ID, err)
		}
	}
	return verifyMemoryServices(ctx, migration.ID, key, source, target)
}

func (r *MemoryRouter) VerifyUser(
	ctx context.Context,
	tenantID string,
	migrationID string,
	userID string,
) (MemoryMigrationVerification, error) {
	m, err := r.repository.GetBackendMigration(ctx, tenantID, migrationID)
	if err != nil {
		return MemoryMigrationVerification{}, err
	}
	app := "t/" + tenantID + "/a/" + m.AppID
	if !resourceHeld(ctx, app, "memory") {
		return resourceValue(ctx, r.repository, app, "memory", resourceSubject(userID, ""), false, func(ctx context.Context) (MemoryMigrationVerification, error) {
			return r.VerifyUser(ctx, tenantID, migrationID, userID)
		})
	}
	migration, source, target, err := r.migrationServices(ctx, tenantID, migrationID)
	if err != nil {
		return MemoryMigrationVerification{}, err
	}
	key := memory.UserKey{
		AppName: "t/" + migration.TenantID + "/a/" + migration.AppID,
		UserID:  userID,
	}
	result, err := verifyMemoryServices(ctx, migration.ID, key, source, target)
	if err == nil && result.Passed {
		s, save, e := resourceState(ctx, app, "memory")
		if e != nil {
			return result, e
		}
		controlplane.RecordResourceProof(s, migration, resourceSubject(userID, ""), dataDigest(result))
		err = save()
	}
	return result, err
}

func (r *MemoryRouter) migrationServices(
	ctx context.Context,
	tenantID string,
	migrationID string,
) (controlplane.BackendMigration, memory.Service, memory.Service, error) {
	migration, err := r.repository.GetBackendMigration(ctx, tenantID, migrationID)
	if err != nil {
		return controlplane.BackendMigration{}, nil, nil, err
	}
	if migration.ResourceType != memoryResourceType {
		return controlplane.BackendMigration{}, nil, nil, errors.New("migration is not for memory")
	}
	sourceBinding, err := r.repository.GetBackendBinding(ctx, tenantID, migration.SourceBindingID)
	if err != nil {
		return controlplane.BackendMigration{}, nil, nil, err
	}
	targetBinding, err := r.repository.GetBackendBinding(ctx, tenantID, migration.TargetBindingID)
	if err != nil {
		return controlplane.BackendMigration{}, nil, nil, err
	}
	source, err := r.cachedService(ctx, sourceBinding)
	if err != nil {
		return controlplane.BackendMigration{}, nil, nil, err
	}
	target, err := r.cachedService(ctx, targetBinding)
	return migration, source, target, err
}

func verifyMemoryServices(
	ctx context.Context,
	migrationID string,
	key memory.UserKey,
	source memory.Service,
	target memory.Service,
) (MemoryMigrationVerification, error) {
	sourceEntries, err := source.ReadMemories(ctx, key, 100001)
	if err != nil {
		return MemoryMigrationVerification{}, err
	}
	targetEntries, err := target.ReadMemories(ctx, key, 100001)
	if err != nil {
		return MemoryMigrationVerification{}, err
	}
	if len(sourceEntries) > 100000 || len(targetEntries) > 100000 {
		return MemoryMigrationVerification{}, errors.New("Memory verification exceeds scan bound")
	}
	targetByID := make(map[string]*memory.Entry, len(targetEntries))
	for _, entry := range targetEntries {
		if entry != nil {
			targetByID[entry.ID] = entry
		}
	}
	result := MemoryMigrationVerification{
		MigrationID: migrationID, UserID: key.UserID,
		SourceCount: len(sourceEntries), TargetCount: len(targetEntries),
	}
	for _, sourceEntry := range sourceEntries {
		if sourceEntry == nil {
			continue
		}
		targetEntry := targetByID[sourceEntry.ID]
		if targetEntry == nil {
			result.Missing = append(result.Missing, sourceEntry.ID)
			continue
		}
		if sourceEntry.Memory == nil || targetEntry.Memory == nil || sourceEntry.AppName != key.AppName || targetEntry.AppName != key.AppName || sourceEntry.UserID != key.UserID || targetEntry.UserID != key.UserID ||
			memoryDigest(sourceEntry.Memory) != memoryDigest(targetEntry.Memory) {
			result.Mismatched = append(result.Mismatched, sourceEntry.ID)
		}
	}
	result.Passed = result.SourceCount == result.TargetCount &&
		len(result.Missing) == 0 && len(result.Mismatched) == 0
	return result, nil
}

func memoryDigest(m *memory.Memory) string {
	if m == nil {
		return ""
	}
	copy := *m
	copy.LastUpdated = nil
	return dataDigest(copy)
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
	migration, migrationErr := r.repository.GetActiveBackendMigration(
		ctx, tenantID, appID, memoryResourceType,
	)
	if migrationErr == nil {
		return r.migrationService(ctx, migration)
	}
	if !errors.Is(migrationErr, controlplane.ErrNotFound) {
		return nil, migrationErr
	}
	binding, err := resolveBackendBinding(ctx, r.repository, tenantID, appID, memoryResourceType)
	if err != nil {
		return nil, err
	}
	return r.cachedService(ctx, binding)
}

func (r *MemoryRouter) migrationService(
	ctx context.Context,
	migration controlplane.BackendMigration,
) (memory.Service, error) {
	sourceBinding, err := r.repository.GetBackendBinding(
		ctx, migration.TenantID, migration.SourceBindingID,
	)
	if err != nil {
		return nil, err
	}
	targetBinding, err := r.repository.GetBackendBinding(
		ctx, migration.TenantID, migration.TargetBindingID,
	)
	if err != nil {
		return nil, err
	}
	if sourceBinding.ResourceType != memoryResourceType ||
		targetBinding.ResourceType != memoryResourceType ||
		sourceBinding.AppID != migration.AppID || targetBinding.AppID != migration.AppID {
		return nil, errors.New("memory migration binding scope mismatch")
	}
	source, err := r.cachedService(ctx, sourceBinding)
	if err != nil {
		return nil, err
	}
	if migration.State == controlplane.MigrationPlanned ||
		migration.State == controlplane.MigrationRollback {
		return source, nil
	}
	target, err := r.cachedService(ctx, targetBinding)
	if err != nil {
		return nil, err
	}
	switch migration.State {
	case controlplane.MigrationDualWrite, controlplane.MigrationBackfill,
		controlplane.MigrationVerify:
		return &dualMemoryService{
			primary: source, secondary: target,
			onSecondaryError: r.repairRecorder(migration),
		}, nil
	case controlplane.MigrationCutover:
		return &dualMemoryService{
			primary: target, secondary: source,
			onSecondaryError: r.repairRecorder(migration),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported active memory migration state %q", migration.State)
	}
}

func (r *MemoryRouter) repairRecorder(
	migration controlplane.BackendMigration,
) func(context.Context, error) {
	return func(ctx context.Context, _ error) {
		_ = r.repository.AdjustBackendMigrationRepair(
			ctx, migration.TenantID, migration.ID, 1,
		)
	}
}

func (r *MemoryRouter) cachedService(
	ctx context.Context,
	binding controlplane.BackendBinding,
) (memory.Service, error) {
	cacheKey := binding.TenantID + "\x00" + binding.ID + "\x00" + dataDigest(binding.Config)
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
		url, err := r.endpoint(ctx, binding.TenantID, cfg.URL, binding.SecretRef)
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
		dsn, err := r.endpoint(ctx, binding.TenantID, cfg.DSN, binding.SecretRef)
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
	tenantID string,
	configured string,
	secretRef string,
) (string, error) {
	if secretRef != "" {
		value, err := r.secrets.Resolve(ctx, tenantID, secret.Memory, secretRef)
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
