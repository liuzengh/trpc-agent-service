//
// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

package storage

import (
	"context"
	"errors"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// StorageAdapter provides unified tenant-routed Session and Memory access.
// MultiTenantStorageAdapterImpl is the single production implementation.
type StorageAdapter interface {
	// SessionService returns the tenant-selected shared service used by Runner.
	// Exposing the real service prevents Worker from silently falling back to
	// process-local sessions. Deprecated: production callers must use
	// ServiceLeaseAdapter.AcquireServices so cache eviction cannot close a
	// backend while a Runner still owns it.
	SessionService(ctx context.Context, t *tenant.Tenant) (session.Service, error)
	// MemoryService returns the tenant-selected shared service used by Runner
	// for native recall and asynchronous auto-memory extraction.
	// Deprecated: production callers must use ServiceLeaseAdapter.AcquireServices.
	MemoryService(ctx context.Context, t *tenant.Tenant) (memory.Service, error)

	CreateSession(ctx context.Context, t *tenant.Tenant, key session.Key, state session.StateMap, opts ...session.Option) (*session.Session, error)
	GetSession(ctx context.Context, t *tenant.Tenant, key session.Key, opts ...session.Option) (*session.Session, error)
	ListSessions(ctx context.Context, t *tenant.Tenant, userKey session.UserKey, opts ...session.Option) ([]*session.Session, error)
	AppendEvent(ctx context.Context, t *tenant.Tenant, sess *session.Session, evt *event.Event, opts ...session.Option) error
	DeleteSession(ctx context.Context, t *tenant.Tenant, key session.Key, opts ...session.Option) error

	AddMemory(ctx context.Context, t *tenant.Tenant, userKey memory.UserKey, memoryText string, topics []string, opts ...memory.AddOption) error
	SearchMemories(ctx context.Context, t *tenant.Tenant, userKey memory.UserKey, query string, opts ...memory.SearchOption) ([]memory.Memory, error)
	DeleteMemory(ctx context.Context, t *tenant.Tenant, memoryKey memory.Key) error

	HealthCheck(ctx context.Context) error
	Close() error
}

// ServiceLeaseAdapter is implemented by storage adapters that can tie a
// backend reference to the lifetime of a Worker. It prevents a configuration
// rollout from closing a Session/Memory client while an in-flight Runner still
// owns it. Older adapters can continue using StorageAdapter's two service
// accessors and are treated as process-owned resources.
type ServiceLeaseAdapter interface {
	AcquireServices(ctx context.Context, t *tenant.Tenant) (session.Service, memory.Service, func(), error)
}

// AtomicFenceAdapter identifies a storage adapter whose services reject stale
// execution generations through a backend-native authorizer. A Redis lease or
// context cancellation alone does not satisfy this contract.
type AtomicFenceAdapter interface {
	ServiceLeaseAdapter
	AtomicWriteFenceEnabled() bool
}

// StorageCacheOptions bounds the number of immutable tenant storage
// configurations retained by a process. A new configuration is rejected when
// all slots are still referenced; silently closing an active client would be a
// data-loss/availability bug during a rolling control-plane update.
type StorageCacheOptions struct {
	MaxEntries      int
	IdleTTL         time.Duration
	Clock           func() time.Time
	BackendProfiles BackendProfileResolver
	// SessionDecorator wraps the real service before the strict execution
	// fence is installed. The returned service owns and closes its input.
	SessionDecorator SessionServiceDecorator
	// WriteFence is required for the strict production composition. When set,
	// every Session/Memory service exposed by the adapter is wrapped with it.
	WriteFence fence.Authorizer
	// ConfiguredTenants makes readiness probe cold-start backends instead of
	// reporting healthy merely because no tenant has received traffic yet.
	// The callback must return only active tenants visible to this process.
	ConfiguredTenants func(context.Context) ([]*tenant.Tenant, error)
	// ReadinessProbeTTL coalesces repeated health requests. A zero value uses a
	// conservative default; a negative value disables caching.
	ReadinessProbeTTL time.Duration
	// RequireBackendHealthProbe makes readiness fail closed for remote profiles
	// when neither the wrapped service nor the profile resolver can perform a
	// live probe. In-memory services remain locally healthy without a probe.
	RequireBackendHealthProbe bool
}

type SessionServiceDecorator func(*tenant.Tenant, session.Service) (session.Service, error)

var (
	ErrBackendCacheSaturated      = errors.New("storage backend cache has no idle capacity")
	ErrBackendCacheClosed         = errors.New("storage backend cache is closed")
	ErrBackendHealthProbeRequired = errors.New("remote storage backend health probe is required")
	// ErrBackendClosePanic identifies a third-party backend that panicked while
	// being released. The panic value is deliberately not retained.
	ErrBackendClosePanic = errors.New("storage backend close panicked")
	// ErrBackendCloseFailed identifies a third-party backend that rejected
	// cleanup. The provider error is deliberately not retained because driver
	// messages can contain DSNs, usernames, hostnames or other credentials.
	ErrBackendCloseFailed = errors.New("storage backend close failed")
	// ErrBackendHealthProbePanic identifies a backend or profile resolver that
	// panicked while being probed. The panic value is deliberately not retained.
	ErrBackendHealthProbePanic = errors.New("storage backend health probe panicked")
)

type backendInstance struct {
	sessionService session.Service
	memoryService  memory.Service
	sessionProfile string
	memoryProfile  string
	createdAt      int64
	tenantID       string
	lastUsed       time.Time
	refs           int
	retiring       bool
	closeTracked   bool
}

type tenantIDKey struct{}

func withTenantID(ctx context.Context, tenantID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, tenantIDKey{}, tenantID)
}
