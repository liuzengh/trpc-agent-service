package inmemory

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type BundleFactory func(context.Context, profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error)

type BundleManagerPolicy struct {
	FailureBackoff time.Duration
	CloseTimeout   time.Duration
}

func DefaultBundleManagerPolicy() BundleManagerPolicy {
	return BundleManagerPolicy{FailureBackoff: 250 * time.Millisecond, CloseTimeout: 5 * time.Second}
}

type bundleEntry struct {
	bundle   profile.RuntimeBundle
	close    func(context.Context) error
	refs     int
	retired  bool
	building bool
	ready    chan struct{}
	cancel   context.CancelFunc
}

type buildFailure struct {
	err        error
	retryAfter time.Time
}

type BundleManager struct {
	mu           sync.Mutex
	factory      BundleFactory
	policy       BundleManagerPolicy
	entries      map[profile.ExecutionProfileKey]*bundleEntry
	failures     map[profile.ExecutionProfileKey]buildFailure
	retired      map[profile.ExecutionProfileKey]struct{}
	closed       bool
	closing      int
	closeErr     error
	stateChanged chan struct{}
}

func NewBundleManager(factory BundleFactory) *BundleManager {
	return NewBundleManagerWithPolicy(factory, DefaultBundleManagerPolicy())
}

func NewBundleManagerWithPolicy(factory BundleFactory, policy BundleManagerPolicy) *BundleManager {
	defaults := DefaultBundleManagerPolicy()
	if policy.FailureBackoff <= 0 {
		policy.FailureBackoff = defaults.FailureBackoff
	}
	if policy.CloseTimeout <= 0 {
		policy.CloseTimeout = defaults.CloseTimeout
	}
	return &BundleManager{factory: factory, policy: policy, entries: make(map[profile.ExecutionProfileKey]*bundleEntry),
		failures: make(map[profile.ExecutionProfileKey]buildFailure), retired: make(map[profile.ExecutionProfileKey]struct{}),
		stateChanged: make(chan struct{})}
}

func (m *BundleManager) Acquire(ctx context.Context, key profile.ExecutionProfileKey) (profile.BundleLease, error) {
	if ctx == nil {
		return nil, runtime.ErrInvariantViolation
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if m.factory == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	for {
		m.mu.Lock()
		if m.closed {
			m.mu.Unlock()
			return nil, runtime.ErrBackendUnavailable
		}
		if _, retired := m.retired[key]; retired {
			m.mu.Unlock()
			return nil, runtime.ErrVersionMismatch
		}
		if failure, ok := m.failures[key]; ok {
			if time.Now().Before(failure.retryAfter) {
				m.mu.Unlock()
				return nil, failure.err
			}
			delete(m.failures, key)
		}
		if entry, ok := m.entries[key]; ok {
			if entry.retired {
				m.mu.Unlock()
				return nil, runtime.ErrVersionMismatch
			}
			if entry.building {
				ready := entry.ready
				m.mu.Unlock()
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-ready:
				}
				continue
			}
			entry.refs++
			m.signalLocked()
			result := &lease{manager: m, key: key, bundle: entry.bundle}
			m.mu.Unlock()
			return result, nil
		}
		buildCtx, cancelBuild := context.WithCancel(ctx)
		entry := &bundleEntry{building: true, ready: make(chan struct{}), cancel: cancelBuild}
		m.entries[key] = entry
		m.signalLocked()
		m.mu.Unlock()

		bundle, closeFn, buildErr := m.build(buildCtx, key)
		cancelBuild()
		m.mu.Lock()
		entry.building = false
		entry.cancel = nil
		if buildErr != nil {
			delete(m.entries, key)
			if ctx.Err() == nil {
				m.failures[key] = buildFailure{err: buildErr, retryAfter: time.Now().Add(m.policy.FailureBackoff)}
			}
			close(entry.ready)
			if closeFn != nil {
				m.reserveCloseLocked()
			} else {
				m.signalLocked()
			}
			m.mu.Unlock()
			m.closeOwnedReserved(closeFn)
			return nil, buildErr
		}
		if bundle == nil {
			delete(m.entries, key)
			failure := runtime.ErrInvariantViolation
			m.failures[key] = buildFailure{err: failure, retryAfter: time.Now().Add(m.policy.FailureBackoff)}
			close(entry.ready)
			if closeFn != nil {
				m.reserveCloseLocked()
			} else {
				m.signalLocked()
			}
			m.mu.Unlock()
			m.closeOwnedReserved(closeFn)
			return nil, failure
		}
		entry.bundle, entry.close = bundle, closeFn
		if m.closed || entry.retired {
			delete(m.entries, key)
			close(entry.ready)
			if closeFn != nil {
				m.reserveCloseLocked()
			} else {
				m.signalLocked()
			}
			m.mu.Unlock()
			m.closeOwnedReserved(closeFn)
			if m.closed {
				return nil, runtime.ErrBackendUnavailable
			}
			return nil, runtime.ErrVersionMismatch
		}
		entry.refs = 1
		close(entry.ready)
		m.signalLocked()
		result := &lease{manager: m, key: key, bundle: bundle}
		m.mu.Unlock()
		return result, nil
	}
}

func (m *BundleManager) Retire(key profile.ExecutionProfileKey) {
	m.mu.Lock()
	m.retired[key] = struct{}{}
	delete(m.failures, key)
	entry := m.entries[key]
	if entry == nil {
		m.mu.Unlock()
		return
	}
	entry.retired = true
	if entry.refs > 0 || entry.building {
		m.signalLocked()
		m.mu.Unlock()
		return
	}
	delete(m.entries, key)
	if entry.close != nil {
		m.reserveCloseLocked()
	} else {
		m.signalLocked()
	}
	closeFn := entry.close
	m.mu.Unlock()
	m.closeOwnedReserved(closeFn)
}

// RetireTenant marks every cached bundle for one tenant retired. Active
// leases retain their immutable resources; later Acquire calls fail closed.
func (m *BundleManager) RetireTenant(tenantID string) {
	if tenantID == "" {
		return
	}
	m.mu.Lock()
	keys := make([]profile.ExecutionProfileKey, 0)
	for key := range m.entries {
		if key.TenantID == tenantID {
			keys = append(keys, key)
		}
	}
	for key := range m.retired {
		if key.TenantID == tenantID {
			keys = append(keys, key)
		}
	}
	m.mu.Unlock()
	for _, key := range keys {
		m.Retire(key)
	}
}

func (m *BundleManager) Close(ctx context.Context) error {
	if ctx == nil {
		return runtime.ErrInvariantViolation
	}
	for {
		m.mu.Lock()
		m.closed = true
		m.failures = make(map[profile.ExecutionProfileKey]buildFailure)
		var closeFns []func(context.Context) error
		for key, entry := range m.entries {
			entry.retired = true
			if entry.building && entry.cancel != nil {
				entry.cancel()
			}
			if entry.refs == 0 && !entry.building {
				delete(m.entries, key)
				if entry.close != nil {
					closeFns = append(closeFns, entry.close)
				}
			}
		}
		if len(closeFns) > 0 {
			m.closing += len(closeFns)
		}
		m.signalLocked()
		done := len(m.entries) == 0 && m.closing == 0
		result := m.closeErr
		changed := m.stateChanged
		m.mu.Unlock()

		for _, closeFn := range closeFns {
			m.startClose(closeFn, ctx)
		}
		if done && len(closeFns) == 0 {
			return result
		}
		select {
		case <-ctx.Done():
			return errors.Join(result, ctx.Err())
		case <-changed:
		}
	}
}

type lease struct {
	once    sync.Once
	manager *BundleManager
	key     profile.ExecutionProfileKey
	bundle  profile.RuntimeBundle
}

func (l *lease) Bundle() profile.RuntimeBundle { return l.bundle }
func (l *lease) Release()                      { l.once.Do(func() { l.manager.release(l.key) }) }

func (m *BundleManager) release(key profile.ExecutionProfileKey) {
	m.mu.Lock()
	entry := m.entries[key]
	if entry == nil || entry.refs < 1 {
		m.mu.Unlock()
		return
	}
	entry.refs--
	if entry.refs > 0 || !entry.retired {
		m.signalLocked()
		m.mu.Unlock()
		return
	}
	delete(m.entries, key)
	if entry.close != nil {
		m.reserveCloseLocked()
	} else {
		m.signalLocked()
	}
	closeFn := entry.close
	m.mu.Unlock()
	m.closeOwnedReserved(closeFn)
}

func (m *BundleManager) closeOwned(closeFn func(context.Context) error) {
	if closeFn == nil {
		return
	}
	m.mu.Lock()
	m.reserveCloseLocked()
	m.mu.Unlock()
	m.closeOwnedReserved(closeFn)
}

func (m *BundleManager) closeOwnedReserved(closeFn func(context.Context) error) {
	if closeFn == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), m.policy.CloseTimeout)
	done := make(chan error, 1)
	go func() { done <- closeFn(ctx) }()
	select {
	case err := <-done:
		cancel()
		m.finishClose(err)
	case <-ctx.Done():
		// The owner has a bounded wait, but the close operation may still be
		// unwinding. Keep the closing count until it actually returns.
		go func() {
			err := <-done
			cancel()
			m.finishClose(err)
		}()
	}
}

func (m *BundleManager) reserveCloseLocked() {
	m.closing++
	m.signalLocked()
}

func (m *BundleManager) startClose(closeFn func(context.Context) error, ctx context.Context) {
	go func() {
		err := closeFn(ctx)
		m.finishClose(err)
	}()
}

type buildResult struct {
	bundle profile.RuntimeBundle
	close  func(context.Context) error
	err    error
}

// build prevents a factory that ignores cancellation from blocking Acquire.
// A late-built bundle is still closed when the factory eventually returns.
func (m *BundleManager) build(ctx context.Context, key profile.ExecutionProfileKey) (profile.RuntimeBundle, func(context.Context) error, error) {
	resultCh := make(chan buildResult, 1)
	go func() {
		bundle, closeFn, err := m.factory(ctx, key)
		resultCh <- buildResult{bundle: bundle, close: closeFn, err: err}
	}()
	select {
	case result := <-resultCh:
		return result.bundle, result.close, result.err
	case <-ctx.Done():
		go func() {
			result := <-resultCh
			if result.close == nil {
				return
			}
			closeCtx, cancel := context.WithTimeout(context.Background(), m.policy.CloseTimeout)
			defer cancel()
			_ = result.close(closeCtx)
		}()
		return nil, nil, ctx.Err()
	}
}

func (m *BundleManager) finishClose(err error) {
	m.mu.Lock()
	m.closing--
	m.closeErr = errors.Join(m.closeErr, err)
	m.signalLocked()
	m.mu.Unlock()
}

func (m *BundleManager) signalLocked() {
	close(m.stateChanged)
	m.stateChanged = make(chan struct{})
}
