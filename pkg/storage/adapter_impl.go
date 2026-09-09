//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// MultiTenantStorageAdapterImpl implements StorageAdapter with real backend integration.
type MultiTenantStorageAdapterImpl struct {
	factory                   *BackendFactory
	tenantBackends            map[string]*backendInstance
	initializing              map[string]*backendInitialization
	mu                        sync.RWMutex
	initWG                    sync.WaitGroup
	maxEntries                int
	idleTTL                   time.Duration
	clock                     func() time.Time
	writeFence                fence.Authorizer
	buildBackend              func(*tenant.Tenant, time.Time) (*backendInstance, error)
	closeErrs                 []error
	closed                    bool
	closeDone                 chan struct{}
	configuredTenants         func(context.Context) ([]*tenant.Tenant, error)
	readinessProbeTTL         time.Duration
	requireBackendHealthProbe bool
	backendProfiles           BackendProfileResolver
	sessionDecorator          SessionServiceDecorator
	readinessMu               sync.Mutex
	readinessInFlight         chan struct{}
	lastReadinessProbe        time.Time
	lastReadinessErr          error
	activeRefs                int
	refsDone                  chan struct{}
	backendCloseInFlight      int
	backendCloseDone          chan struct{}
}

type backendInitialization struct {
	done    chan struct{}
	backend *backendInstance
	err     error
}

// NewMultiTenantStorageAdapterImpl creates a new multi-tenant storage adapter with real backends.
func NewMultiTenantStorageAdapterImpl() *MultiTenantStorageAdapterImpl {
	return NewMultiTenantStorageAdapterImplWithOptions(StorageCacheOptions{})
}

// NewMultiTenantStorageAdapterImplWithOptions creates a storage adapter with
// explicit backend cache bounds. The defaults are deliberately conservative
// enough for a multi-tenant worker while still allowing a normal canary and
// stable configuration to coexist.
func NewMultiTenantStorageAdapterImplWithOptions(options StorageCacheOptions) *MultiTenantStorageAdapterImpl {
	if options.MaxEntries <= 0 {
		options.MaxEntries = 128
	}
	if options.IdleTTL <= 0 {
		options.IdleTTL = 10 * time.Minute
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.ReadinessProbeTTL == 0 {
		options.ReadinessProbeTTL = 10 * time.Second
	}
	value := &MultiTenantStorageAdapterImpl{
		factory:                   NewBackendFactoryWithProfiles(options.BackendProfiles),
		tenantBackends:            make(map[string]*backendInstance),
		initializing:              make(map[string]*backendInitialization),
		maxEntries:                options.MaxEntries,
		idleTTL:                   options.IdleTTL,
		clock:                     options.Clock,
		writeFence:                options.WriteFence,
		closeDone:                 make(chan struct{}),
		configuredTenants:         options.ConfiguredTenants,
		readinessProbeTTL:         options.ReadinessProbeTTL,
		requireBackendHealthProbe: options.RequireBackendHealthProbe,
		backendProfiles:           options.BackendProfiles,
		sessionDecorator:          options.SessionDecorator,
		refsDone:                  closedSignal(),
		backendCloseDone:          closedSignal(),
	}
	value.buildBackend = value.createBackend
	return value
}

// AcquireServices returns the tenant-selected services and a release function
// tied to the caller's Worker lifetime. Production workers use this seam so
// old backends remain open until every Runner using them has drained.
func (m *MultiTenantStorageAdapterImpl) AcquireServices(ctx context.Context, t *tenant.Tenant) (session.Service, memory.Service, func(), error) {
	backend, release, err := m.acquireBackend(ctx, t)
	if err != nil {
		return nil, nil, nil, err
	}
	return backend.sessionService, backend.memoryService, release, nil
}

// AtomicWriteFenceEnabled reports whether the adapter was composed with a
// backend-native execution fence. It is deliberately a runtime capability,
// not inferred from the selected backend name.
func (m *MultiTenantStorageAdapterImpl) AtomicWriteFenceEnabled() bool {
	return m != nil && m.writeFence != nil
}

// SessionService returns the real tenant-selected service for Runner.
func (m *MultiTenantStorageAdapterImpl) SessionService(ctx context.Context, t *tenant.Tenant) (session.Service, error) {
	backend, err := m.getOrInitBackend(ctx, t)
	if err != nil {
		return nil, err
	}
	m.touchBackend(backend)
	return backend.sessionService, nil
}

// MemoryService returns the real tenant-selected service for Runner. Runner
// borrows this service; the adapter remains its lifecycle owner.
func (m *MultiTenantStorageAdapterImpl) MemoryService(ctx context.Context, t *tenant.Tenant) (memory.Service, error) {
	backend, err := m.getOrInitBackend(ctx, t)
	if err != nil {
		return nil, err
	}
	m.touchBackend(backend)
	return backend.memoryService, nil
}

// CreateSession creates a new session for the tenant.
func (m *MultiTenantStorageAdapterImpl) CreateSession(ctx context.Context, t *tenant.Tenant, key session.Key, state session.StateMap, opts ...session.Option) (*session.Session, error) {
	var err error
	key, err = scopeSessionKey(t, key)
	if err != nil {
		return nil, err
	}
	backend, release, err := m.acquireBackend(ctx, t)
	if err != nil {
		return nil, err
	}
	defer release()
	ctx = withTenantID(ctx, t.ID)
	return backend.sessionService.CreateSession(ctx, key, state, opts...)
}

// GetSession retrieves a session for the tenant.
func (m *MultiTenantStorageAdapterImpl) GetSession(ctx context.Context, t *tenant.Tenant, key session.Key, opts ...session.Option) (*session.Session, error) {
	var err error
	key, err = scopeSessionKey(t, key)
	if err != nil {
		return nil, err
	}
	backend, release, err := m.acquireBackend(ctx, t)
	if err != nil {
		return nil, err
	}
	defer release()
	ctx = withTenantID(ctx, t.ID)
	return backend.sessionService.GetSession(ctx, key, opts...)
}

// ListSessions lists sessions for the tenant.
func (m *MultiTenantStorageAdapterImpl) ListSessions(ctx context.Context, t *tenant.Tenant, userKey session.UserKey, opts ...session.Option) ([]*session.Session, error) {
	var err error
	userKey, err = scopeSessionUserKey(t, userKey)
	if err != nil {
		return nil, err
	}
	backend, release, err := m.acquireBackend(ctx, t)
	if err != nil {
		return nil, err
	}
	defer release()
	ctx = withTenantID(ctx, t.ID)
	return backend.sessionService.ListSessions(ctx, userKey, opts...)
}

// AppendEvent delegates to the tenant-selected backend. Production Runner
// serializes the whole invocation with a session lease; locking only this
// method would still allow two model/tool invocations to interleave.
func (m *MultiTenantStorageAdapterImpl) AppendEvent(ctx context.Context, t *tenant.Tenant, sess *session.Session, evt *event.Event, opts ...session.Option) error {
	var err error
	sess, err = scopeSession(t, sess)
	if err != nil {
		return err
	}
	backend, release, err := m.acquireBackend(ctx, t)
	if err != nil {
		return err
	}
	defer release()
	ctx = withTenantID(ctx, t.ID)
	return backend.sessionService.AppendEvent(ctx, sess, evt, opts...)
}

// DeleteSession deletes a session.
func (m *MultiTenantStorageAdapterImpl) DeleteSession(ctx context.Context, t *tenant.Tenant, key session.Key, opts ...session.Option) error {
	var err error
	key, err = scopeSessionKey(t, key)
	if err != nil {
		return err
	}
	backend, release, err := m.acquireBackend(ctx, t)
	if err != nil {
		return err
	}
	defer release()
	ctx = withTenantID(ctx, t.ID)
	return backend.sessionService.DeleteSession(ctx, key, opts...)
}

// AddMemory adds a memory for the tenant.
func (m *MultiTenantStorageAdapterImpl) AddMemory(ctx context.Context, t *tenant.Tenant, userKey memory.UserKey, memoryText string, topics []string, opts ...memory.AddOption) error {
	var err error
	userKey, err = scopeMemoryUserKey(t, userKey)
	if err != nil {
		return err
	}
	backend, release, err := m.acquireBackend(ctx, t)
	if err != nil {
		return err
	}
	defer release()
	ctx = withTenantID(ctx, t.ID)
	return backend.memoryService.AddMemory(ctx, userKey, memoryText, topics, opts...)
}

// DeleteMemory deletes a memory.
func (m *MultiTenantStorageAdapterImpl) DeleteMemory(ctx context.Context, t *tenant.Tenant, memoryKey memory.Key) error {
	var err error
	memoryKey, err = scopeMemoryKey(t, memoryKey)
	if err != nil {
		return err
	}
	backend, release, err := m.acquireBackend(ctx, t)
	if err != nil {
		return err
	}
	defer release()
	ctx = withTenantID(ctx, t.ID)
	return backend.memoryService.DeleteMemory(ctx, memoryKey)
}

// HealthCheck performs health check on all backends.
func (m *MultiTenantStorageAdapterImpl) HealthCheck(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if m.configuredTenants != nil {
		return m.healthCheckConfiguredTenants(ctx)
	}
	m.mu.Lock()
	// Snapshot immutable service references and pin them while probing. A slow
	// backend health implementation must not block every tenant operation, but
	// Close must also not close a service underneath the probe.
	backends := make([]*backendInstance, 0, len(m.tenantBackends))
	for _, backend := range m.tenantBackends {
		backend.refs++
		if m.activeRefs == 0 {
			m.refsDone = make(chan struct{})
		}
		m.activeRefs++
		backends = append(backends, backend)
	}
	m.mu.Unlock()
	defer func() {
		for _, backend := range backends {
			m.releaseBackend(backend)
		}
	}()

	for _, backend := range backends {
		tenantID := backend.tenantID
		if err := m.checkBackendHealth(ctx, backend.tenantID, backend.sessionProfile, backend.sessionService); err != nil {
			return fmt.Errorf("tenant %s session backend unhealthy: %w", tenantID, err)
		}
		if err := m.checkBackendHealth(ctx, backend.tenantID, backend.memoryProfile, backend.memoryService); err != nil {
			return fmt.Errorf("tenant %s memory backend unhealthy: %w", tenantID, err)
		}
	}

	return nil
}

// healthCheckConfiguredTenants probes the configured tenant set on cold start
// and after a short cache window. Without this callback a worker with an empty
// backend cache would be healthy before any real tenant storage was tested.
func (m *MultiTenantStorageAdapterImpl) healthCheckConfiguredTenants(ctx context.Context) error {
	for {
		m.readinessMu.Lock()
		now := m.clock().UTC()
		if m.readinessProbeTTL >= 0 && !m.lastReadinessProbe.IsZero() &&
			now.Before(m.lastReadinessProbe.Add(m.readinessProbeTTL)) {
			err := m.lastReadinessErr
			m.readinessMu.Unlock()
			return err
		}
		if pending := m.readinessInFlight; pending != nil {
			m.readinessMu.Unlock()
			select {
			case <-pending:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		m.readinessInFlight = make(chan struct{})
		m.readinessMu.Unlock()

		err := m.probeConfiguredTenantsSafely(ctx)
		m.readinessMu.Lock()
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// A caller-side timeout is not evidence that the dependency is
			// unhealthy. Do not poison the readiness cache for the next probe.
			m.lastReadinessProbe = time.Time{}
			m.lastReadinessErr = nil
		} else {
			m.lastReadinessProbe = now
			m.lastReadinessErr = err
		}
		close(m.readinessInFlight)
		m.readinessInFlight = nil
		m.readinessMu.Unlock()
		return err
	}
}

func (m *MultiTenantStorageAdapterImpl) probeConfiguredTenants(ctx context.Context) error {
	tenants, err := m.configuredTenants(ctx)
	if err == nil {
		// Readiness is a node/dependency signal, not a synchronous fan-out over
		// every tenant. Once the active set exceeds the bounded backend cache,
		// probing all tenants would deterministically saturate the cache and
		// make healthy workers fail readiness. Tenant-specific health is checked
		// on acquisition and exposed through routing/circuit-breaker state.
		active := 0
		for _, configured := range tenants {
			if configured != nil && configured.Status == tenant.TenantStatusActive {
				active++
			}
		}
		if active > m.maxEntries {
			return nil
		}
		for _, configured := range tenants {
			if configured == nil || configured.Status != tenant.TenantStatusActive {
				continue
			}
			backend, release, acquireErr := m.acquireBackend(ctx, configured)
			if acquireErr != nil {
				err = fmt.Errorf("tenant %s backend unavailable: %w", configured.ID, acquireErr)
				break
			}
			if healthErr := m.checkBackendHealth(ctx, backend.tenantID, backend.sessionProfile, backend.sessionService); healthErr != nil {
				err = fmt.Errorf("tenant %s session backend unhealthy: %w", configured.ID, healthErr)
				release()
				break
			}
			if healthErr := m.checkBackendHealth(ctx, backend.tenantID, backend.memoryProfile, backend.memoryService); healthErr != nil {
				err = fmt.Errorf("tenant %s memory backend unhealthy: %w", configured.ID, healthErr)
				release()
				break
			}
			release()
		}
	}
	return err
}

func (m *MultiTenantStorageAdapterImpl) probeConfiguredTenantsSafely(ctx context.Context) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrBackendHealthProbePanic
		}
	}()
	return m.probeConfiguredTenants(ctx)
}

func (m *MultiTenantStorageAdapterImpl) checkBackendHealth(ctx context.Context, tenantID, profileID string, service interface{}) error {
	supported, err := probeBackendHealth(ctx, service)
	if err != nil || supported || !m.requireBackendHealthProbe || profileID == "" {
		return err
	}
	checker, ok := m.backendProfiles.(BackendProfileHealthChecker)
	if !ok {
		return fmt.Errorf("%w: profile %q", ErrBackendHealthProbeRequired, profileID)
	}
	return checkBackendProfileHealthSafely(checker, ctx, tenantID, profileID)
}

func checkBackendProfileHealthSafely(checker BackendProfileHealthChecker, ctx context.Context, tenantID, profileID string) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrBackendHealthProbePanic
		}
	}()
	return checker.HealthCheckBackendProfile(ctx, tenantID, profileID)
}

// Backends in tRPC-Agent-Go do not yet share a health interface. Support the
// two conventional contracts without coupling the platform to concrete
// implementations. Services with neither contract are still exercised by the
// Worker request path; the command-level health endpoint separately verifies
// PostgreSQL and Redis coordination dependencies.
func checkBackendHealth(ctx context.Context, service interface{}) error {
	_, err := probeBackendHealth(ctx, service)
	return err
}

func probeBackendHealth(ctx context.Context, service interface{}) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	return probeBackendHealthSafely(ctx, service)
}

func probeBackendHealthSafely(ctx context.Context, service interface{}) (supported bool, err error) {
	defer func() {
		if recover() != nil {
			supported = true
			err = ErrBackendHealthProbePanic
		}
	}()
	if checker, ok := service.(interface{ HealthCheck(context.Context) error }); ok {
		if err := checker.HealthCheck(ctx); !errors.Is(err, ErrHealthCheckUnsupported) {
			return true, err
		}
	}
	if pinger, ok := service.(interface{ PingContext(context.Context) error }); ok {
		if err := pinger.PingContext(ctx); !errors.Is(err, ErrHealthCheckUnsupported) {
			return true, err
		}
	}
	return false, nil
}

// Close closes all backend connections.
func (m *MultiTenantStorageAdapterImpl) Close() error {
	m.mu.Lock()
	if m.closedLocked() {
		done := m.closeDone
		m.mu.Unlock()
		<-done
		m.mu.RLock()
		err := errors.Join(m.closeErrs...)
		m.mu.RUnlock()
		return err
	}
	m.closed = true
	var evicted, closing []*backendInstance
	for key, backend := range m.tenantBackends {
		backend.retiring = true
		delete(m.tenantBackends, key)
		closing = append(closing, backend)
		if backend.refs == 0 {
			evicted = append(evicted, backend)
		}
	}
	m.trackBackendCloseLocked(closing)
	m.mu.Unlock()
	m.closeBackendsTracked(evicted)
	// Initializers reserve capacity before leaving the mutex. Once closed is
	// visible they cannot publish a backend; each one closes its partial result
	// and records any cleanup error before signalling Done.
	m.initWG.Wait()
	m.mu.RLock()
	refsDone := m.refsDone
	m.mu.RUnlock()
	<-refsDone
	m.mu.RLock()
	backendCloseDone := m.backendCloseDone
	m.mu.RUnlock()
	<-backendCloseDone
	m.mu.RLock()
	err := errors.Join(m.closeErrs...)
	m.mu.RUnlock()
	close(m.closeDone)
	return err
}

// getOrInitBackend retrieves or initializes backend for a tenant.
func (m *MultiTenantStorageAdapterImpl) getOrInitBackend(ctx context.Context, t *tenant.Tenant) (*backendInstance, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	cacheKey, err := backendCacheKey(t)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	if m.closedLocked() {
		m.mu.Unlock()
		return nil, ErrBackendCacheClosed
	}
	if backend := m.tenantBackends[cacheKey]; backend != nil {
		backend.lastUsed = m.clock().UTC()
		m.mu.Unlock()
		return backend, nil
	}
	if pending := m.initializing[cacheKey]; pending != nil {
		done := pending.done
		m.mu.Unlock()
		select {
		case <-done:
			if pending.err != nil {
				return nil, pending.err
			}
			if pending.backend == nil {
				return nil, ErrBackendInitialization
			}
			return pending.backend, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	now := m.clock().UTC()
	var evicted []*backendInstance
	if len(m.tenantBackends)+len(m.initializing) >= m.maxEntries {
		for key, candidate := range m.tenantBackends {
			if candidate.refs == 0 {
				candidate.retiring = true
				delete(m.tenantBackends, key)
				evicted = append(evicted, candidate)
				if len(m.tenantBackends)+len(m.initializing) < m.maxEntries {
					break
				}
			}
		}
		if len(m.tenantBackends)+len(m.initializing) >= m.maxEntries {
			m.trackBackendCloseLocked(evicted)
			m.mu.Unlock()
			m.closeBackendsTracked(evicted)
			return nil, ErrBackendCacheSaturated
		}
	}
	m.trackBackendCloseLocked(evicted)

	pending := &backendInitialization{done: make(chan struct{})}
	m.initializing[cacheKey] = pending
	m.initWG.Add(1)
	builder := m.buildBackend
	m.mu.Unlock()
	m.closeBackendsTracked(evicted)

	backend, buildErr := buildBackendSafely(builder, t, now)
	m.mu.Lock()
	closed := m.closedLocked()
	switch {
	case closed:
		pending.err = ErrBackendCacheClosed
	case buildErr != nil:
		pending.err = buildErr
	case backend == nil:
		pending.err = ErrBackendInitialization
	default:
		// A storage config change gets a new immutable backend instance.
		// The reservation above ensures publication cannot exceed the
		// configured cache bound.
		m.tenantBackends[cacheKey] = backend
		pending.backend = backend
	}
	delete(m.initializing, cacheKey)
	close(pending.done)
	m.mu.Unlock()

	if (closed || buildErr != nil) && backend != nil {
		if closeErr := closeBackends([]*backendInstance{backend}); closeErr != nil {
			m.recordCloseError(closeErr)
		}
	}
	m.initWG.Done()
	if pending.err != nil {
		return nil, pending.err
	}
	return pending.backend, nil
}

func buildBackendSafely(builder func(*tenant.Tenant, time.Time) (*backendInstance, error), t *tenant.Tenant, now time.Time) (backend *backendInstance, err error) {
	defer func() {
		if recover() != nil {
			backend = nil
			err = fmt.Errorf("%w: backend builder panicked", ErrBackendInitialization)
		}
	}()
	if builder == nil {
		return nil, ErrBackendInitialization
	}
	return builder(t, now)
}

func (m *MultiTenantStorageAdapterImpl) createBackend(t *tenant.Tenant, now time.Time) (*backendInstance, error) {
	backend, err := m.factory.CreateBackendForTenant(t.ID, &t.Storage)
	if err != nil {
		return nil, err
	}
	if m.sessionDecorator != nil {
		decorated, wrapErr := m.sessionDecorator(t, backend.sessionService)
		if wrapErr != nil || isNilStorageService(decorated) {
			_ = closeBackends([]*backendInstance{backend})
			return nil, fmt.Errorf("configure session decorator: %w", ErrBackendInitialization)
		}
		backend.sessionService = decorated
	}
	if m.writeFence != nil {
		strictSession, wrapErr := NewStrictFencedSessionService(backend.sessionService, m.writeFence, t.ID)
		if wrapErr != nil {
			_ = closeBackends([]*backendInstance{backend})
			return nil, fmt.Errorf("configure fenced session service: %w", wrapErr)
		}
		strictMemory, wrapErr := NewStrictFencedMemoryService(backend.memoryService, m.writeFence, t.ID)
		if wrapErr != nil {
			_ = closeBackends([]*backendInstance{backend})
			return nil, fmt.Errorf("configure fenced memory service: %w", wrapErr)
		}
		backend.sessionService = strictSession
		backend.memoryService = strictMemory
	}
	backend.createdAt = now.Unix()
	backend.tenantID = t.ID
	backend.sessionProfile = t.Storage.SessionProfile
	backend.memoryProfile = t.Storage.MemoryProfile
	backend.lastUsed = now
	return backend, nil
}

// acquireBackend borrows an immutable backend instance for one adapter
// operation. The reference is published only while the exact instance is
// still authoritative in the cache; this closes the race where a config
// rollout could otherwise evict and close a service between lookup and use.
func (m *MultiTenantStorageAdapterImpl) acquireBackend(ctx context.Context, t *tenant.Tenant) (*backendInstance, func(), error) {
	cacheKey, err := backendCacheKey(t)
	if err != nil {
		return nil, nil, err
	}
	for {
		backend, err := m.getOrInitBackend(ctx, t)
		if err != nil {
			return nil, nil, err
		}
		m.mu.Lock()
		if m.closedLocked() {
			m.mu.Unlock()
			return nil, nil, ErrBackendCacheClosed
		}
		if current := m.tenantBackends[cacheKey]; current != backend {
			m.mu.Unlock()
			continue
		}
		backend.refs++
		if m.activeRefs == 0 {
			m.refsDone = make(chan struct{})
		}
		m.activeRefs++
		backend.lastUsed = m.clock().UTC()
		m.mu.Unlock()

		var once sync.Once
		release := func() {
			once.Do(func() { m.releaseBackend(backend) })
		}
		return backend, release, nil
	}
}

func (m *MultiTenantStorageAdapterImpl) touchBackend(backend *backendInstance) {
	if backend == nil {
		return
	}
	m.mu.Lock()
	if !m.closedLocked() {
		backend.lastUsed = m.clock().UTC()
	}
	m.mu.Unlock()
}

func (m *MultiTenantStorageAdapterImpl) releaseBackend(backend *backendInstance) {
	if backend == nil {
		return
	}
	var closeTarget *backendInstance
	var refsDone chan struct{}
	m.mu.Lock()
	if backend.refs > 0 {
		backend.refs--
		m.activeRefs--
		if m.activeRefs == 0 {
			// Capture this generation's signal before unlocking. A new acquire
			// may install the next generation's channel while a retiring
			// backend is still closing; closing m.refsDone later would then
			// wake waiters for the wrong generation.
			refsDone = m.refsDone
		}
	}
	backend.lastUsed = m.clock().UTC()
	if backend.refs == 0 && backend.retiring {
		closeTarget = backend
		m.trackBackendCloseLocked([]*backendInstance{backend})
	}
	m.mu.Unlock()
	if closeTarget != nil {
		m.closeBackendsTracked([]*backendInstance{closeTarget})
	}
	if refsDone != nil {
		close(refsDone)
	}
}

func closedSignal() chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func (m *MultiTenantStorageAdapterImpl) closedLocked() bool { return m.closed }

func closeBackends(backends []*backendInstance) error {
	var errs []error
	for _, backend := range backends {
		if backend == nil {
			continue
		}
		if backend.sessionService != nil {
			if err := closeBackendService(backend.sessionService.Close); err != nil {
				errs = append(errs, err)
			}
		}
		if backend.memoryService != nil {
			if err := closeBackendService(backend.memoryService.Close); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

// closeBackendService isolates provider cleanup. Backend implementations are
// outside this package and a panic from one must not prevent the other service
// or another tenant backend from being released.
func closeBackendService(closeFn func() error) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrBackendClosePanic
		}
	}()
	if closeFn == nil {
		return ErrBackendCloseFailed
	}
	if err := closeFn(); err != nil {
		return ErrBackendCloseFailed
	}
	return nil
}

func (m *MultiTenantStorageAdapterImpl) recordCloseError(err error) {
	if err == nil {
		return
	}
	m.mu.Lock()
	m.closeErrs = append(m.closeErrs, err)
	m.mu.Unlock()
}

func (m *MultiTenantStorageAdapterImpl) trackBackendCloseLocked(backends []*backendInstance) {
	count := 0
	for _, backend := range backends {
		if backend == nil || backend.closeTracked {
			continue
		}
		backend.closeTracked = true
		count++
	}
	if count == 0 {
		return
	}
	if m.backendCloseInFlight == 0 {
		m.backendCloseDone = make(chan struct{})
	}
	m.backendCloseInFlight += count
}

func (m *MultiTenantStorageAdapterImpl) finishBackendClose() {
	m.mu.Lock()
	if m.backendCloseInFlight > 0 {
		m.backendCloseInFlight--
		if m.backendCloseInFlight == 0 {
			close(m.backendCloseDone)
		}
	}
	m.mu.Unlock()
}

func (m *MultiTenantStorageAdapterImpl) closeBackendsTracked(backends []*backendInstance) {
	if len(backends) == 0 {
		return
	}
	if err := closeBackends(backends); err != nil {
		m.recordCloseError(err)
	}
	for range backends {
		m.finishBackendClose()
	}
}

func backendCacheKey(t *tenant.Tenant) (string, error) {
	if t == nil || t.ID == "" {
		return "", fmt.Errorf("tenant and tenant ID are required")
	}
	data, err := json.Marshal(t.Storage)
	if err != nil {
		return "", fmt.Errorf("encode tenant storage config: %w", err)
	}
	digest := sha256.Sum256(data)
	return t.ID + ":" + hex.EncodeToString(digest[:8]), nil
}

// SearchMemories searches memories for the tenant.
func (m *MultiTenantStorageAdapterImpl) SearchMemories(ctx context.Context, t *tenant.Tenant, userKey memory.UserKey, query string, opts ...memory.SearchOption) ([]memory.Memory, error) {
	var err error
	userKey, err = scopeMemoryUserKey(t, userKey)
	if err != nil {
		return nil, err
	}
	backend, release, err := m.acquireBackend(ctx, t)
	if err != nil {
		return nil, err
	}
	defer release()
	ctx = withTenantID(ctx, t.ID)
	entries, err := backend.memoryService.SearchMemories(ctx, userKey, query, opts...)
	if err != nil {
		return nil, err
	}

	// Convert []*memory.Entry to []memory.Memory
	memories := make([]memory.Memory, len(entries))
	for i, entry := range entries {
		if entry.Memory != nil {
			memories[i] = *entry.Memory
			memories[i].LastUpdated = &entry.UpdatedAt
		}
	}
	return memories, nil
}

func tenantPrefix(t *tenant.Tenant) (string, error) {
	if t == nil || t.ID == "" {
		return "", fmt.Errorf("tenant and tenant ID are required")
	}
	if err := tenant.ValidateTenantID(t.ID); err != nil {
		return "", err
	}
	return fmt.Sprintf("tsa1:%d:%s:", len(t.ID), t.ID), nil
}

// TenantScopedAppName converts a logical app name into the canonical key used
// by shared Session and Memory backends. The tenant byte length makes the
// encoding injective even when IDs or app names contain separators.
func TenantScopedAppName(t *tenant.Tenant, appName string) (string, error) {
	prefix, err := tenantPrefix(t)
	if err != nil {
		return "", err
	}
	if err := tenant.ValidateAgentAppName(appName); err != nil {
		return "", err
	}
	scoped := prefix + appName
	// tRPC-Agent's PostgreSQL Session backend stores app_name in VARCHAR(255).
	// Enforce the narrowest installed backend contract before any I/O.
	if len(scoped) > 255 {
		return "", fmt.Errorf("tenant-scoped app name exceeds backend limit")
	}
	return scoped, nil
}

func scopeSessionKey(t *tenant.Tenant, key session.Key) (session.Key, error) {
	var err error
	key.AppName, err = TenantScopedAppName(t, key.AppName)
	if err != nil {
		return session.Key{}, err
	}
	return key, nil
}

func scopeSessionUserKey(t *tenant.Tenant, key session.UserKey) (session.UserKey, error) {
	var err error
	key.AppName, err = TenantScopedAppName(t, key.AppName)
	if err != nil {
		return session.UserKey{}, err
	}
	return key, nil
}

func scopeMemoryUserKey(t *tenant.Tenant, key memory.UserKey) (memory.UserKey, error) {
	var err error
	key.AppName, err = TenantScopedAppName(t, key.AppName)
	if err != nil {
		return memory.UserKey{}, err
	}
	return key, nil
}

func scopeMemoryKey(t *tenant.Tenant, key memory.Key) (memory.Key, error) {
	var err error
	key.AppName, err = TenantScopedAppName(t, key.AppName)
	if err != nil {
		return memory.Key{}, err
	}
	return key, nil
}

func scopeSession(t *tenant.Tenant, sess *session.Session) (*session.Session, error) {
	if sess == nil {
		return nil, fmt.Errorf("session is required")
	}
	scoped, err := TenantScopedAppName(t, sess.AppName)
	if err != nil {
		return nil, err
	}
	// Session contains internal mutexes. Use the framework's lock-aware Clone
	// instead of copying the value, which would duplicate those mutexes and can
	// race with an in-flight Runner invocation.
	copy := sess.Clone()
	copy.AppName = scoped
	return copy, nil
}
