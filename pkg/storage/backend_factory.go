//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package storage

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	mempostgres "trpc.group/trpc-go/trpc-agent-go/memory/postgres"
	memredis "trpc.group/trpc-go/trpc-agent-go/memory/redis"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmem "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	sessionpostgres "trpc.group/trpc-go/trpc-agent-go/session/postgres"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

// BackendFactory creates storage backend instances from operator-owned profiles.
type BackendFactory struct {
	profiles BackendProfileResolver
}

// NewBackendFactory creates a new backend factory.
func NewBackendFactory() *BackendFactory {
	return NewBackendFactoryWithProfiles(rejectingBackendProfileResolver{})
}

// NewBackendFactoryWithProfiles creates a backend factory using the supplied
// operator-owned profile resolver.
func NewBackendFactoryWithProfiles(profiles BackendProfileResolver) *BackendFactory {
	if isNilStorageService(profiles) {
		profiles = rejectingBackendProfileResolver{}
	}
	return &BackendFactory{profiles: profiles}
}

// CreateSessionServiceForTenant creates an undecorated official service for
// a migration operator. The caller owns its lifecycle and must establish the
// tenant scope and migration authority before accessing it.
func (f *BackendFactory) CreateSessionServiceForTenant(tenantID string, config *tenant.StorageConfig) (session.Service, error) {
	if f == nil || config == nil {
		return nil, ErrBackendOptionInvalid
	}
	if err := tenant.ValidateTenantID(tenantID); err != nil {
		return nil, ErrBackendOptionInvalid
	}
	return f.createSessionService(tenantID, config.SessionBackend, config.SessionProfile, config.SessionConfig)
}

// CreateBackend creates a backend instance from configuration.
func (f *BackendFactory) CreateBackend(config *tenant.StorageConfig) (*backendInstance, error) {
	return f.CreateBackendForTenant("", config)
}

// CreateBackendForTenant creates both services while applying any operator
// tenant allowlist attached to the referenced backend profiles.
func (f *BackendFactory) CreateBackendForTenant(tenantID string, config *tenant.StorageConfig) (*backendInstance, error) {
	if config == nil {
		return nil, fmt.Errorf("%w: storage configuration is required", ErrBackendOptionInvalid)
	}
	sessionService, err := f.createSessionService(
		tenantID,
		config.SessionBackend,
		config.SessionProfile,
		config.SessionConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("create session backend: %w", err)
	}

	memoryService, err := f.createMemoryService(
		tenantID,
		config.MemoryBackend,
		config.MemoryProfile,
		config.MemoryConfig,
	)
	if err != nil {
		_ = sessionService.Close()
		return nil, fmt.Errorf("create memory backend: %w", err)
	}

	return &backendInstance{
		sessionService: sessionService,
		memoryService:  memoryService,
		createdAt:      time.Now().Unix(),
	}, nil
}

// createSessionService creates a session service based on backend type.
func (f *BackendFactory) createSessionService(
	tenantID,
	backendType,
	profileID string,
	config map[string]string,
) (session.Service, error) {
	if err := validateBackendOptions("session", backendType, profileID, config); err != nil {
		return nil, err
	}
	switch backendType {
	case "inmemory", "":
		return f.createInMemorySessionService(config)
	case "redis":
		return f.createRedisSessionService(tenantID, profileID, config)
	case "postgres":
		return f.createPostgresSessionService(tenantID, profileID, config)
	default:
		return nil, fmt.Errorf("%w: unsupported session backend", ErrBackendOptionInvalid)
	}
}

// createInMemorySessionService creates an in-memory session service.
func (f *BackendFactory) createInMemorySessionService(config map[string]string) (session.Service, error) {
	opts := []sessioninmem.ServiceOpt{}

	if ttl, ok := config["session_ttl"]; ok {
		if d, err := time.ParseDuration(ttl); err == nil {
			opts = append(opts, sessioninmem.WithSessionTTL(d))
		}
	}

	return sessioninmem.NewSessionService(opts...), nil
}

// createRedisSessionService creates a Redis-based session service.
func (f *BackendFactory) createRedisSessionService(tenantID, profileID string, config map[string]string) (session.Service, error) {
	profile, err := f.resolveProfileForTenant(tenantID, profileID, "redis")
	if err != nil {
		return nil, err
	}

	opts := []sessionredis.ServiceOpt{
		sessionredis.WithRedisClientURL(profile.ConnectionString),
	}

	if ttl, ok := config["session_ttl"]; ok {
		if d, err := time.ParseDuration(ttl); err == nil {
			opts = append(opts, sessionredis.WithSessionTTL(d))
		}
	}

	service, err := sessionredis.NewService(opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: session redis", ErrBackendInitialization)
	}
	return service, nil
}

// createPostgresSessionService creates a PostgreSQL-based session service.
func (f *BackendFactory) createPostgresSessionService(tenantID, profileID string, config map[string]string) (session.Service, error) {
	profile, err := f.resolveProfileForTenant(tenantID, profileID, "postgres")
	if err != nil {
		return nil, err
	}

	opts := []sessionpostgres.ServiceOpt{
		sessionpostgres.WithPostgresClientDSN(profile.ConnectionString),
	}

	if ttl, ok := config["session_ttl"]; ok {
		if d, err := time.ParseDuration(ttl); err == nil {
			opts = append(opts, sessionpostgres.WithSessionTTL(d))
		}
	}

	service, err := sessionpostgres.NewService(opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: session postgres", ErrBackendInitialization)
	}
	return service, nil
}

// createMemoryService creates a memory service based on backend type.
func (f *BackendFactory) createMemoryService(
	tenantID,
	backendType,
	profileID string,
	config map[string]string,
) (memory.Service, error) {
	if err := validateBackendOptions("memory", backendType, profileID, config); err != nil {
		return nil, err
	}
	switch backendType {
	case "inmemory", "":
		return f.createInMemoryMemoryService(config)
	case "redis":
		return f.createRedisMemoryService(tenantID, profileID, config)
	case "postgres":
		return f.createPostgresMemoryService(tenantID, profileID, config)
	default:
		return nil, fmt.Errorf("%w: unsupported memory backend", ErrBackendOptionInvalid)
	}
}

// createInMemoryMemoryService creates an in-memory memory service.
func (f *BackendFactory) createInMemoryMemoryService(config map[string]string) (memory.Service, error) {
	opts := []inmemory.ServiceOpt{}

	// memory/inmemory is multi-tenant and doesn't take appName as a constructor option
	// it handles appName per-operation via UserKey
	if limit, ok := config["memory_limit"]; ok {
		if n, err := strconv.Atoi(limit); err == nil && n > 0 {
			opts = append(opts, inmemory.WithMemoryLimit(n))
		}
	}

	return inmemory.NewMemoryService(opts...), nil
}

// createRedisMemoryService creates a Redis-based memory service.
func (f *BackendFactory) createRedisMemoryService(tenantID, profileID string, config map[string]string) (memory.Service, error) {
	profile, err := f.resolveProfileForTenant(tenantID, profileID, "redis")
	if err != nil {
		return nil, err
	}

	opts := []memredis.ServiceOpt{
		memredis.WithRedisClientURL(profile.ConnectionString),
	}

	if limit, ok := config["memory_limit"]; ok {
		if n, err := strconv.Atoi(limit); err == nil && n > 0 {
			opts = append(opts, memredis.WithMemoryLimit(n))
		}
	}

	service, err := memredis.NewService(opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: memory redis", ErrBackendInitialization)
	}
	return service, nil
}

// createPostgresMemoryService creates a PostgreSQL-based memory service.
func (f *BackendFactory) createPostgresMemoryService(tenantID, profileID string, config map[string]string) (memory.Service, error) {
	profile, err := f.resolveProfileForTenant(tenantID, profileID, "postgres")
	if err != nil {
		return nil, err
	}

	opts := []mempostgres.ServiceOpt{
		mempostgres.WithPostgresClientDSN(profile.ConnectionString),
	}

	if limit, ok := config["memory_limit"]; ok {
		if n, err := strconv.Atoi(limit); err == nil && n > 0 {
			opts = append(opts, mempostgres.WithMemoryLimit(n))
		}
	}

	service, err := mempostgres.NewService(opts...)
	if err != nil {
		return nil, fmt.Errorf("%w: memory postgres", ErrBackendInitialization)
	}
	return service, nil
}

func (f *BackendFactory) resolveProfileForTenant(tenantID, profileID, backend string) (BackendProfile, error) {
	var (
		profile BackendProfile
		err     error
	)
	if scoped, ok := f.profiles.(TenantBackendProfileResolver); ok && tenantID != "" {
		profile, err = scoped.ResolveBackendProfileForTenant(tenantID, profileID)
	} else {
		profile, err = f.profiles.ResolveBackendProfile(profileID)
	}
	if err != nil {
		if errors.Is(err, ErrBackendProfileNotFound) {
			return BackendProfile{}, fmt.Errorf("%w: profile %q", ErrBackendProfileNotFound, profileID)
		}
		return BackendProfile{}, fmt.Errorf("%w: profile %q", ErrBackendProfileUnavailable, profileID)
	}
	if profile.Backend != backend {
		return BackendProfile{}, fmt.Errorf("%w: profile %q", ErrBackendProfileTypeMismatch, profileID)
	}
	if err := validateBackendEndpoint(profile); err != nil {
		return BackendProfile{}, fmt.Errorf("%w: profile %q", ErrBackendEndpointInvalid, profileID)
	}
	return profile, nil
}

func validateBackendOptions(domain, backend, profileID string, config map[string]string) error {
	switch backend {
	case "", "inmemory":
		if profileID != "" {
			return fmt.Errorf("%w: in-memory backend cannot use a profile", ErrBackendOptionInvalid)
		}
	case "redis", "postgres":
		if !profileIDPattern.MatchString(profileID) {
			return fmt.Errorf("%w: operator profile is required", ErrBackendOptionInvalid)
		}
	default:
		return fmt.Errorf("%w: unsupported backend", ErrBackendOptionInvalid)
	}

	for key, value := range config {
		switch domain + ":" + key {
		case "session:session_ttl":
			ttl, err := time.ParseDuration(value)
			if err != nil || ttl <= 0 || ttl > 365*24*time.Hour {
				return fmt.Errorf("%w: session_ttl", ErrBackendOptionInvalid)
			}
		case "memory:memory_limit":
			limit, err := strconv.Atoi(value)
			if err != nil || limit <= 0 || limit > 1_000_000 {
				return fmt.Errorf("%w: memory_limit", ErrBackendOptionInvalid)
			}
		default:
			return fmt.Errorf("%w: option %q is not allowed", ErrBackendOptionInvalid, key)
		}
	}
	return nil
}
