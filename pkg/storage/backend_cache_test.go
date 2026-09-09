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
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmem "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestStorageBackendCacheDoesNotEvictReferencedBackend(t *testing.T) {
	now := time.Unix(1000, 0)
	adapter := NewMultiTenantStorageAdapterImplWithOptions(StorageCacheOptions{
		MaxEntries: 1,
		IdleTTL:    time.Minute,
		Clock:      func() time.Time { return now },
	})
	defer adapter.Close()

	tenantA := testStorageTenant("tenant-a", "inmemory")
	tenantB := testStorageTenant("tenant-b", "inmemory")
	_, _, release, err := adapter.AcquireServices(context.Background(), tenantA)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := adapter.AcquireServices(context.Background(), tenantB); !errors.Is(err, ErrBackendCacheSaturated) {
		t.Fatalf("referenced backend was evicted: err=%v", err)
	}
	if got := adapter.backendCount(); got != 1 {
		t.Fatalf("backend count=%d, want 1", got)
	}
	release()
	now = now.Add(time.Minute)
	if _, _, releaseB, err := adapter.AcquireServices(context.Background(), tenantB); err != nil {
		t.Fatal(err)
	} else {
		releaseB()
	}
	if got := adapter.backendCount(); got != 1 {
		t.Fatalf("backend count after idle eviction=%d, want 1", got)
	}
}

func TestStorageBackendCacheRejectsInvalidOptions(t *testing.T) {
	adapter := NewMultiTenantStorageAdapterImplWithOptions(StorageCacheOptions{MaxEntries: -1, IdleTTL: -time.Second})
	defer adapter.Close()
	if adapter.maxEntries <= 0 || adapter.idleTTL <= 0 {
		t.Fatalf("invalid cache options were not normalized: max=%d ttl=%s", adapter.maxEntries, adapter.idleTTL)
	}
}

func TestStorageDirectOperationPinsBackendDuringEviction(t *testing.T) {
	now := time.Unix(2000, 0)
	adapter := NewMultiTenantStorageAdapterImplWithOptions(StorageCacheOptions{
		MaxEntries: 1,
		IdleTTL:    time.Minute,
		Clock:      func() time.Time { return now },
	})

	tenantA := testStorageTenant("tenant-a", "inmemory")
	tenantB := testStorageTenant("tenant-b", "inmemory")
	started := make(chan struct{})
	proceed := make(chan struct{})
	closed := make(chan struct{})
	blocking := &blockingSessionService{
		Service: sessioninmem.NewSessionService(),
		started: started,
		proceed: proceed,
		closed:  closed,
	}
	backend := &backendInstance{
		sessionService: blocking,
		memoryService:  inmemory.NewMemoryService(),
		tenantID:       tenantA.ID,
		lastUsed:       now.Add(-time.Hour),
	}
	cacheKey, err := backendCacheKey(tenantA)
	if err != nil {
		t.Fatal(err)
	}
	adapter.tenantBackends[cacheKey] = backend

	done := make(chan error, 1)
	go func() {
		_, err := adapter.CreateSession(context.Background(), tenantA,
			session.Key{AppName: "app", UserID: "user", SessionID: "session"}, nil)
		done <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("direct operation did not enter backend")
	}
	if _, _, _, err := adapter.AcquireServices(context.Background(), tenantB); !errors.Is(err, ErrBackendCacheSaturated) {
		t.Fatalf("active direct operation was evicted: err=%v", err)
	}
	select {
	case <-closed:
		t.Fatal("backend was closed while direct operation was active")
	default:
	}
	close(proceed)
	if err := <-done; err != nil {
		t.Fatalf("direct operation failed: %v", err)
	}
	if err := adapter.Close(); err != nil {
		t.Fatalf("close adapter: %v", err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("backend was not closed during adapter shutdown")
	}
}

func TestStorageBackendInitializationIsSingleFlightAndCloseWaits(t *testing.T) {
	adapter := NewMultiTenantStorageAdapterImplWithOptions(StorageCacheOptions{MaxEntries: 2})
	tenantA := testStorageTenant("tenant-singleflight", "inmemory")
	started := make(chan struct{})
	releaseBuild := make(chan struct{})
	var builds atomic.Int32
	adapter.buildBackend = func(t *tenant.Tenant, now time.Time) (*backendInstance, error) {
		builds.Add(1)
		close(started)
		<-releaseBuild
		return &backendInstance{
			sessionService: sessioninmem.NewSessionService(),
			memoryService:  inmemory.NewMemoryService(),
			tenantID:       t.ID,
			createdAt:      now.Unix(),
			lastUsed:       now,
		}, nil
	}

	acquired := make(chan struct{})
	go func() {
		_, _, release, err := adapter.AcquireServices(context.Background(), tenantA)
		if !errors.Is(err, ErrBackendCacheClosed) {
			t.Errorf("AcquireServices error=%v, want ErrBackendCacheClosed after concurrent Close", err)
		}
		if err == nil {
			release()
		}
		close(acquired)
	}()
	<-started

	closeDone := make(chan error, 1)
	go func() { closeDone <- adapter.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned while backend initialization was blocked: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseBuild)
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
	}
	<-acquired
	if got := builds.Load(); got != 1 {
		t.Fatalf("backend builds=%d, want single-flight build", got)
	}
}

func TestStorageBackendCloseContainsProviderPanics(t *testing.T) {
	adapter := NewMultiTenantStorageAdapterImplWithOptions(StorageCacheOptions{MaxEntries: 1})
	backend := &backendInstance{
		sessionService: panicCloseSessionService{Service: sessioninmem.NewSessionService()},
		memoryService:  panicCloseMemoryService{Service: inmemory.NewMemoryService()},
	}
	adapter.tenantBackends["panic"] = backend

	err := adapter.Close()
	if !errors.Is(err, ErrBackendClosePanic) {
		t.Fatalf("Close error=%v, want ErrBackendClosePanic", err)
	}
}

func TestStorageCloseWaitsForRetiringBackendClose(t *testing.T) {
	adapter := NewMultiTenantStorageAdapterImpl()
	defer adapter.Close()
	tenantA := testStorageTenant("tenant-blocking-close", "inmemory")
	started := make(chan struct{})
	releaseClose := make(chan struct{})
	backend := &backendInstance{
		sessionService: &blockingCloseSessionService{
			Service:  sessioninmem.NewSessionService(),
			started:  started,
			release:  releaseClose,
			closeErr: errors.New("postgres://user:secret@example.invalid/db: backend close failed"),
		},
		memoryService: inmemory.NewMemoryService(),
		tenantID:      tenantA.ID,
		lastUsed:      time.Now().UTC(),
	}
	cacheKey, err := backendCacheKey(tenantA)
	if err != nil {
		t.Fatal(err)
	}
	adapter.tenantBackends[cacheKey] = backend
	_, _, release, err := adapter.AcquireServices(context.Background(), tenantA)
	if err != nil {
		t.Fatalf("AcquireServices: %v", err)
	}

	releaseDone := make(chan struct{})
	go func() {
		release()
		close(releaseDone)
	}()
	// Close must race with the release while the provider is still blocked.
	// The release owns the last reference, so adapter.Close must wait for the
	// provider Close call before returning.
	closeDone := make(chan error, 1)
	go func() { closeDone <- adapter.Close() }()
	<-started

	select {
	case err := <-closeDone:
		t.Fatalf("adapter Close returned before backend Close: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	close(releaseClose)
	<-releaseDone
	if err := <-closeDone; !errors.Is(err, ErrBackendCloseFailed) {
		t.Fatalf("adapter Close error=%v, want ErrBackendCloseFailed", err)
	} else if strings.Contains(err.Error(), "secret@example.invalid") {
		t.Fatalf("adapter Close leaked provider details: %v", err)
	}
}

func TestStorageCloseWaitsForIdleEvictionClose(t *testing.T) {
	now := time.Unix(5000, 0)
	adapter := NewMultiTenantStorageAdapterImplWithOptions(StorageCacheOptions{
		MaxEntries: 1,
		IdleTTL:    time.Minute,
		Clock:      func() time.Time { return now },
	})
	defer adapter.Close()

	started := make(chan struct{})
	releaseClose := make(chan struct{})
	backend := &backendInstance{
		sessionService: &blockingCloseSessionService{
			Service:  sessioninmem.NewSessionService(),
			started:  started,
			release:  releaseClose,
			closeErr: nil,
		},
		memoryService: inmemory.NewMemoryService(),
		lastUsed:      now.Add(-time.Hour),
	}
	cacheKey, err := backendCacheKey(testStorageTenant("tenant-idle-close", "inmemory"))
	if err != nil {
		t.Fatal(err)
	}
	adapter.tenantBackends[cacheKey] = backend

	acquireDone := make(chan error, 1)
	go func() {
		_, _, release, err := adapter.AcquireServices(context.Background(), testStorageTenant("tenant-next", "inmemory"))
		if release != nil {
			release()
		}
		acquireDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("idle eviction did not enter backend close")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- adapter.Close() }()
	select {
	case err := <-closeDone:
		t.Fatalf("adapter Close returned while idle eviction close was blocked: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	close(releaseClose)
	if err := <-closeDone; err != nil {
		t.Fatalf("adapter Close: %v", err)
	}
	if err := <-acquireDone; !errors.Is(err, ErrBackendCacheClosed) {
		t.Fatalf("AcquireServices error=%v, want ErrBackendCacheClosed", err)
	}
}

func TestStorageBackendBuilderPanicBecomesInitializationError(t *testing.T) {
	adapter := NewMultiTenantStorageAdapterImpl()
	defer adapter.Close()
	adapter.buildBackend = func(*tenant.Tenant, time.Time) (*backendInstance, error) {
		panic("backend credential")
	}
	_, _, _, err := adapter.AcquireServices(context.Background(), testStorageTenant("panic-builder", "inmemory"))
	if !errors.Is(err, ErrBackendInitialization) {
		t.Fatalf("AcquireServices error=%v, want ErrBackendInitialization", err)
	}
}

func TestStorageHealthCheckProbesConfiguredColdTenants(t *testing.T) {
	now := time.Unix(3000, 0)
	configured := testStorageTenant("tenant-cold", "inmemory")
	configured.Status = tenant.TenantStatusActive
	var builds atomic.Int32
	adapter := NewMultiTenantStorageAdapterImplWithOptions(StorageCacheOptions{
		Clock: nowFunc(now),
		ConfiguredTenants: func(context.Context) ([]*tenant.Tenant, error) {
			return []*tenant.Tenant{configured}, nil
		},
	})
	defer adapter.Close()
	adapter.buildBackend = func(t *tenant.Tenant, created time.Time) (*backendInstance, error) {
		builds.Add(1)
		return &backendInstance{
			sessionService: sessioninmem.NewSessionService(),
			memoryService:  inmemory.NewMemoryService(),
			tenantID:       t.ID,
			createdAt:      created.Unix(),
			lastUsed:       created,
		}, nil
	}
	if err := adapter.HealthCheck(context.Background()); err != nil {
		t.Fatalf("cold configured tenant health check: %v", err)
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("cold tenant backend builds=%d, want 1", got)
	}
	if err := adapter.HealthCheck(context.Background()); err != nil {
		t.Fatalf("cached configured tenant health check: %v", err)
	}
	if got := builds.Load(); got != 1 {
		t.Fatalf("readiness probe rebuilt backend inside TTL: builds=%d", got)
	}
}

func TestStorageHealthCheckDoesNotFanOutBeyondCacheCapacity(t *testing.T) {
	tenants := make([]*tenant.Tenant, 129)
	for i := range tenants {
		tenants[i] = testStorageTenant(fmt.Sprintf("tenant-%03d", i), "inmemory")
		tenants[i].Status = tenant.TenantStatusActive
	}
	adapter := NewMultiTenantStorageAdapterImplWithOptions(StorageCacheOptions{
		ConfiguredTenants: func(context.Context) ([]*tenant.Tenant, error) { return tenants, nil },
	})
	defer adapter.Close()
	if err := adapter.HealthCheck(context.Background()); err != nil {
		t.Fatalf("large tenant set poisoned node readiness: %v", err)
	}
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()
	if len(adapter.tenantBackends) != 0 {
		t.Fatalf("readiness fan-out initialized %d tenant backends", len(adapter.tenantBackends))
	}
}

func TestStorageHealthCheckPropagatesConfiguredTenantSourceFailure(t *testing.T) {
	adapter := NewMultiTenantStorageAdapterImplWithOptions(StorageCacheOptions{
		ConfiguredTenants: func(context.Context) ([]*tenant.Tenant, error) {
			return nil, errors.New("control plane unavailable")
		},
	})
	defer adapter.Close()
	if err := adapter.HealthCheck(context.Background()); err == nil {
		t.Fatal("configured tenant source failure was reported healthy")
	}
}

func TestStorageHealthCheckDoesNotCacheCallerCancellation(t *testing.T) {
	now := time.Unix(3500, 0)
	configured := testStorageTenant("tenant-cancel", "inmemory")
	configured.Status = tenant.TenantStatusActive
	var calls atomic.Int32
	adapter := NewMultiTenantStorageAdapterImplWithOptions(StorageCacheOptions{
		Clock: nowFunc(now),
		ConfiguredTenants: func(ctx context.Context) ([]*tenant.Tenant, error) {
			calls.Add(1)
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return []*tenant.Tenant{configured}, nil
		},
	})
	defer adapter.Close()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := adapter.HealthCheck(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("first health check error=%v, want context.Canceled", err)
	}
	if err := adapter.HealthCheck(context.Background()); err != nil {
		t.Fatalf("second health check retained caller cancellation: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("configured tenant calls=%d, want 2 after uncached cancellation", got)
	}
}

func TestStorageHealthCheckWaiterHonorsContext(t *testing.T) {
	configured := testStorageTenant("tenant-blocked-readiness", "inmemory")
	configured.Status = tenant.TenantStatusActive
	started := make(chan struct{})
	release := make(chan struct{})
	adapter := NewMultiTenantStorageAdapterImplWithOptions(StorageCacheOptions{
		ConfiguredTenants: func(context.Context) ([]*tenant.Tenant, error) {
			select {
			case <-started:
			default:
				close(started)
			}
			<-release
			return []*tenant.Tenant{configured}, nil
		},
	})
	defer adapter.Close()
	ownerDone := make(chan error, 1)
	go func() { ownerDone <- adapter.HealthCheck(context.Background()) }()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := adapter.HealthCheck(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting HealthCheck error=%v, want deadline", err)
	}
	close(release)
	if err := <-ownerDone; err != nil {
		t.Fatalf("owner HealthCheck: %v", err)
	}
}

func TestStorageHealthCheckConfiguredTenantPanicDoesNotStrandWaiters(t *testing.T) {
	adapter := NewMultiTenantStorageAdapterImplWithOptions(StorageCacheOptions{
		ConfiguredTenants: func(context.Context) ([]*tenant.Tenant, error) {
			panic("control plane secret")
		},
	})
	defer adapter.Close()
	if err := adapter.HealthCheck(context.Background()); !errors.Is(err, ErrBackendHealthProbePanic) {
		t.Fatalf("HealthCheck error=%v, want ErrBackendHealthProbePanic", err)
	}
	if err := adapter.HealthCheck(context.Background()); !errors.Is(err, ErrBackendHealthProbePanic) {
		t.Fatalf("second HealthCheck error=%v, want ErrBackendHealthProbePanic", err)
	}
}

func nowFunc(value time.Time) func() time.Time {
	return func() time.Time { return value }
}

type blockingSessionService struct {
	session.Service
	started   chan<- struct{}
	proceed   <-chan struct{}
	closed    chan<- struct{}
	startOnce sync.Once
	closeOnce sync.Once
}

type panicCloseSessionService struct{ session.Service }

func (panicCloseSessionService) Close() error { panic("session backend secret") }

type panicCloseMemoryService struct{ memory.Service }

func (panicCloseMemoryService) Close() error { panic("memory backend secret") }

type blockingCloseSessionService struct {
	session.Service
	started  chan<- struct{}
	release  <-chan struct{}
	closeErr error
	once     sync.Once
}

func (s *blockingCloseSessionService) Close() error {
	s.once.Do(func() { close(s.started) })
	<-s.release
	return s.closeErr
}

func (s *blockingSessionService) CreateSession(ctx context.Context, key session.Key, state session.StateMap, opts ...session.Option) (*session.Session, error) {
	s.startOnce.Do(func() { close(s.started) })
	<-s.proceed
	return s.Service.CreateSession(ctx, key, state, opts...)
}

func (s *blockingSessionService) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return s.Service.Close()
}

func testStorageTenant(id, backend string) *tenant.Tenant {
	return &tenant.Tenant{ID: id, Storage: tenant.StorageConfig{
		SessionBackend: backend,
		MemoryBackend:  backend,
	}}
}

func (m *MultiTenantStorageAdapterImpl) backendCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.tenantBackends)
}
