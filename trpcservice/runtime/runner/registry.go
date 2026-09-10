// Package runner owns service-side Runner caching, leasing, invalidation, and
// bounded lifecycle management.
package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	serviceagent "github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
)

var (
	// ErrInvalid reports invalid Runner registry configuration or input.
	ErrInvalid = errors.New("invalid runtime runner")
	// ErrNotReady reports that the Runner registry is nil or unavailable.
	ErrNotReady = errors.New("runtime Runner registry is not ready")
	// ErrClosed reports that the Runner registry has stopped accepting work.
	ErrClosed = errors.New("runtime Runner registry is closed")
	// ErrRunnerCapacity reports that every registry slot is still borrowed.
	ErrRunnerCapacity = errors.New("runner registry capacity exhausted")
	// ErrRunnerUnavailable is the redacted result of a failed Runner factory.
	ErrRunnerUnavailable = errors.New("runner unavailable")
	// ErrRunnerClose reports a Runner close failure without exposing provider
	// or Secret details.
	ErrRunnerClose = errors.New("runner close failed")
	// ErrRegistryCloseTimeout reports that borrowed Runners did not release
	// before the bounded registry close deadline.
	ErrRegistryCloseTimeout = errors.New("runner registry close timed out")
	// errRunnerInvalidated is internal: callers retry the invalidated build.
	errRunnerInvalidated = errors.New("runner build invalidated")
)

const (
	defaultRunnerRegistryMaxEntries = 128
	defaultRunnerRegistryIdleTTL    = 10 * time.Minute
	defaultRunnerRegistryCloseWait  = 5 * time.Second
)

// Runner is the external Agent Runner lifecycle contract cached by the
// service runtime registry. The registry never closes borrowed
// Session/Secret/Model dependencies captured by a Runner factory.
type Runner = serviceagent.Runner

// RunnerFactory builds one Runner from one immutable, validated plan.
type RunnerFactory func(context.Context, runtime.ExecutionPlan) (Runner, error)

// RunnerRegistryConfig defines the in-process Runner cache and its ownership
// policy. A zero value uses safe defaults for limits and timeouts.
type RunnerRegistryConfig struct {
	Factory      RunnerFactory
	MaxEntries   int
	IdleTTL      time.Duration
	CloseTimeout time.Duration
	Now          func() time.Time
}

// RunnerRegistry owns Runners keyed by the complete ExecutionPlan CacheKey.
// Entries are reference-counted so eviction and invalidation cannot close a
// Runner while a Dispatch lease is still using it.
type RunnerRegistry struct {
	mu           sync.Mutex
	factory      RunnerFactory
	maxEntries   int
	idleTTL      time.Duration
	closeTimeout time.Duration
	now          func() time.Time
	entries      map[runtime.CacheKey]*runnerEntry
	pending      map[runtime.CacheKey]*runnerBuild
	closed       bool
	closeStarted bool
	closeDone    chan struct{}
	closeErr     error
}

type runnerEntry struct {
	runner      Runner
	refs        int
	lastUsed    time.Time
	invalidated bool
	zero        chan struct{}
	closeOnce   sync.Once
	closeErr    error
}

type runnerBuild struct {
	done        chan struct{}
	invalidated bool
}

// NewRunnerRegistry validates and creates an empty in-process registry.
func NewRunnerRegistry(config RunnerRegistryConfig) (*RunnerRegistry, error) {
	if config.Factory == nil {
		return nil, fmt.Errorf("%w: runner factory is required", ErrInvalid)
	}
	if config.MaxEntries == 0 {
		config.MaxEntries = defaultRunnerRegistryMaxEntries
	}
	if config.MaxEntries < 1 {
		return nil, fmt.Errorf("%w: runner registry capacity must be positive", ErrInvalid)
	}
	if config.IdleTTL == 0 {
		config.IdleTTL = defaultRunnerRegistryIdleTTL
	}
	if config.IdleTTL < 0 {
		return nil, fmt.Errorf("%w: runner registry idle TTL cannot be negative", ErrInvalid)
	}
	if config.CloseTimeout == 0 {
		config.CloseTimeout = defaultRunnerRegistryCloseWait
	}
	if config.CloseTimeout < 0 {
		return nil, fmt.Errorf("%w: runner registry close timeout cannot be negative", ErrInvalid)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &RunnerRegistry{
		factory: config.Factory, maxEntries: config.MaxEntries, idleTTL: config.IdleTTL,
		closeTimeout: config.CloseTimeout, now: config.Now,
		entries: make(map[runtime.CacheKey]*runnerEntry), pending: make(map[runtime.CacheKey]*runnerBuild),
	}, nil
}

// Ready reports whether the registry can accept a new lease.
func (registry *RunnerRegistry) Ready() bool {
	if registry == nil {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return !registry.closed
}

// Acquire returns a reference-counted lease for plan. Concurrent construction
// of one CacheKey is merged into a single factory call.
func (registry *RunnerRegistry) Acquire(ctx context.Context, plan runtime.ExecutionPlan) (*RunnerLease, error) {
	if registry == nil {
		return nil, ErrNotReady
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	key, err := plan.CacheKey()
	if err != nil {
		return nil, fmt.Errorf("%w: execution plan is invalid", ErrInvalid)
	}

	for {
		lease, retry, err := registry.acquireOnce(ctx, plan, key)
		if err != nil {
			return nil, err
		}
		if !retry {
			return lease, nil
		}
	}
}

func (registry *RunnerRegistry) acquireOnce(ctx context.Context, plan runtime.ExecutionPlan, key runtime.CacheKey) (*RunnerLease, bool, error) {
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return nil, false, ErrClosed
	}
	if entry := registry.entries[key]; entry != nil && !entry.invalidated {
		registry.retainLocked(entry)
		registry.mu.Unlock()
		return &RunnerLease{registry: registry, entry: entry}, false, nil
	}
	if build := registry.pending[key]; build != nil {
		done := build.done
		registry.mu.Unlock()
		retry, err := waitForRunnerBuild(ctx, done)
		return nil, retry, err
	}

	toClose := registry.evictIdleLocked(registry.now())
	if registry.slotCountLocked() >= registry.maxEntries {
		toClose = append(toClose, registry.evictCapacityLocked()...)
	}
	if registry.slotCountLocked() >= registry.maxEntries {
		registry.mu.Unlock()
		closeEntries(toClose)
		return nil, false, ErrRunnerCapacity
	}
	build := &runnerBuild{done: make(chan struct{})}
	registry.pending[key] = build
	factory := registry.factory
	registry.mu.Unlock()
	closeEntries(toClose)
	return registry.finishRunnerBuild(ctx, plan, key, build, factory)
}

func waitForRunnerBuild(ctx context.Context, done <-chan struct{}) (bool, error) {
	select {
	case <-done:
		if err := ctx.Err(); err != nil {
			return false, err
		}
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (registry *RunnerRegistry) finishRunnerBuild(ctx context.Context, plan runtime.ExecutionPlan, key runtime.CacheKey, build *runnerBuild, factory RunnerFactory) (*RunnerLease, bool, error) {
	runnerValue, factoryErr := factory(ctx, plan)
	factoryErr = normalizeRunnerFactoryError(factoryErr)
	if runnerValue == nil && factoryErr == nil {
		factoryErr = ErrRunnerUnavailable
	}

	registry.mu.Lock()
	delete(registry.pending, key)
	invalidated := build.invalidated
	closed := registry.closed
	var entry *runnerEntry
	if factoryErr == nil {
		if err := ctx.Err(); err != nil {
			factoryErr = err
		}
	}
	if factoryErr == nil && (closed || invalidated) {
		if closed {
			factoryErr = ErrClosed
		} else {
			factoryErr = errRunnerInvalidated
		}
	}
	if factoryErr == nil {
		entry = &runnerEntry{runner: runnerValue, refs: 1, lastUsed: registry.now(), zero: make(chan struct{})}
		registry.entries[key] = entry
	}
	close(build.done)
	registry.mu.Unlock()

	if factoryErr != nil {
		if runnerValue != nil {
			closeEntries([]*runnerEntry{{runner: runnerValue}})
		}
		if errors.Is(factoryErr, errRunnerInvalidated) {
			if err := ctx.Err(); err != nil {
				return nil, false, err
			}
			return nil, true, nil
		}
		return nil, false, factoryErr
	}
	return &RunnerLease{registry: registry, entry: entry}, false, nil
}

// Invalidate prevents new leases from using key. A borrowed old Runner is
// closed only after its last lease is released.
func (registry *RunnerRegistry) Invalidate(key runtime.CacheKey) error {
	if registry == nil {
		return ErrNotReady
	}
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return ErrClosed
	}
	if build := registry.pending[key]; build != nil {
		build.invalidated = true
	}
	entry := registry.entries[key]
	if entry == nil {
		registry.mu.Unlock()
		return nil
	}
	delete(registry.entries, key)
	entry.invalidated = true
	closeNow := entry.refs == 0
	registry.mu.Unlock()
	if closeNow {
		return entry.close()
	}
	return nil
}

// InvalidateMatching invalidates only entries whose complete cache key matches
// predicate. Borrowed leases keep their frozen Runner until Release.
func (registry *RunnerRegistry) InvalidateMatching(predicate func(runtime.CacheKey) bool) error {
	if registry == nil {
		return ErrNotReady
	}
	if predicate == nil {
		return fmt.Errorf("%w: invalidation predicate is required", ErrInvalid)
	}
	registry.mu.Lock()
	if registry.closed {
		registry.mu.Unlock()
		return ErrClosed
	}
	toClose := make([]*runnerEntry, 0)
	for key, build := range registry.pending {
		if predicate(key) {
			build.invalidated = true
		}
	}
	for key, entry := range registry.entries {
		if !predicate(key) {
			continue
		}
		delete(registry.entries, key)
		entry.invalidated = true
		if entry.refs == 0 {
			toClose = append(toClose, entry)
		}
	}
	registry.mu.Unlock()
	for _, entry := range toClose {
		if err := entry.close(); err != nil {
			return err
		}
	}
	return nil
}

// InvalidateTenant removes future runners for one tenant.
func (registry *RunnerRegistry) InvalidateTenant(tenantID string) error {
	return registry.InvalidateMatching(func(key runtime.CacheKey) bool { return key.TenantID == tenantID })
}

// InvalidateApp removes future runners for one tenant/app pair.
func (registry *RunnerRegistry) InvalidateApp(tenantID, appID string) error {
	return registry.InvalidateMatching(func(key runtime.CacheKey) bool { return key.TenantID == tenantID && key.AppID == appID })
}

// InvalidateModelProfile removes future runners using one model profile.
func (registry *RunnerRegistry) InvalidateModelProfile(tenantID, profileID string) error {
	return registry.InvalidateMatching(func(key runtime.CacheKey) bool { return key.TenantID == tenantID && key.ModelProfileID == profileID })
}

// InvalidateBackendProfile removes future runners using one backend profile.
func (registry *RunnerRegistry) InvalidateBackendProfile(tenantID, profileID string) error {
	return registry.InvalidateMatching(func(key runtime.CacheKey) bool { return key.TenantID == tenantID && key.BackendProfileID == profileID })
}

// Close stops new acquisitions and waits a bounded time for borrowed leases.
// If a caller fails to release a lease, the owned Runner remains available to
// that lease and is closed by its eventual Release; the registry returns a
// timeout instead of waiting forever.
func (registry *RunnerRegistry) Close() error {
	if registry == nil {
		return nil
	}
	registry.mu.Lock()
	if registry.closeStarted {
		done := registry.closeDone
		registry.mu.Unlock()
		<-done
		return registry.closeErr
	}
	registry.closeStarted = true
	registry.closeDone = make(chan struct{})
	registry.closed = true
	waitEntries := make([]*runnerEntry, 0, len(registry.entries))
	closeImmediately := make([]*runnerEntry, 0, len(registry.entries))
	for key, entry := range registry.entries {
		delete(registry.entries, key)
		entry.invalidated = true
		if entry.refs > 0 {
			waitEntries = append(waitEntries, entry)
		} else {
			closeImmediately = append(closeImmediately, entry)
		}
	}
	pending := make([]*runnerBuild, 0, len(registry.pending))
	for _, build := range registry.pending {
		build.invalidated = true
		pending = append(pending, build)
	}
	timeout := registry.closeTimeout
	registry.mu.Unlock()

	var runnerCloseErr error
	for _, entry := range closeImmediately {
		runnerCloseErr = joinRunnerCloseError(runnerCloseErr, entry.close())
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	timedOut := false
	for _, build := range pending {
		select {
		case <-build.done:
		case <-deadline.C:
			timedOut = true
		}
		if timedOut {
			break
		}
	}
	if !timedOut {
		for _, entry := range waitEntries {
			select {
			case <-entry.zero:
				runnerCloseErr = joinRunnerCloseError(runnerCloseErr, entry.close())
			case <-deadline.C:
				timedOut = true
			}
			if timedOut {
				break
			}
		}
	}

	registry.mu.Lock()
	if timedOut {
		registry.closeErr = errors.Join(ErrRegistryCloseTimeout, runnerCloseErr)
	} else {
		registry.closeErr = runnerCloseErr
	}
	close(registry.closeDone)
	err := registry.closeErr
	registry.mu.Unlock()
	return err
}

func (registry *RunnerRegistry) slotCountLocked() int {
	return len(registry.entries) + len(registry.pending)
}

func (registry *RunnerRegistry) retainLocked(entry *runnerEntry) {
	if entry.refs == 0 {
		entry.zero = make(chan struct{})
	}
	entry.refs++
	entry.lastUsed = registry.now()
}

func (registry *RunnerRegistry) evictIdleLocked(now time.Time) []*runnerEntry {
	if registry.idleTTL <= 0 {
		return nil
	}
	var toClose []*runnerEntry
	for key, entry := range registry.entries {
		if entry.refs == 0 && !now.Before(entry.lastUsed.Add(registry.idleTTL)) {
			delete(registry.entries, key)
			entry.invalidated = true
			toClose = append(toClose, entry)
		}
	}
	return toClose
}

func (registry *RunnerRegistry) evictCapacityLocked() []*runnerEntry {
	var oldestKey runtime.CacheKey
	var oldest *runnerEntry
	for key, entry := range registry.entries {
		if entry.refs != 0 {
			continue
		}
		if oldest == nil || entry.lastUsed.Before(oldest.lastUsed) {
			oldestKey, oldest = key, entry
		}
	}
	if oldest == nil {
		return nil
	}
	delete(registry.entries, oldestKey)
	oldest.invalidated = true
	return []*runnerEntry{oldest}
}

func normalizeRunnerFactoryError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrRunnerUnavailable
}

func closeEntries(entries []*runnerEntry) {
	for _, entry := range entries {
		if entry != nil {
			_ = entry.close()
		}
	}
}

func joinRunnerCloseError(current, next error) error {
	if next == nil {
		return current
	}
	return errors.Join(current, next)
}

func (entry *runnerEntry) close() error {
	entry.closeOnce.Do(func() {
		if entry.runner == nil {
			return
		}
		if err := entry.runner.Close(); err != nil {
			entry.closeErr = ErrRunnerClose
		}
	})
	return entry.closeErr
}

// RunnerLease is a single-use reference to one Registry-owned Runner.
type RunnerLease struct {
	registry *RunnerRegistry
	entry    *runnerEntry
	once     sync.Once
	err      error
}

// Runner returns the leased Runner. It remains valid until Release returns.
func (lease *RunnerLease) Runner() Runner {
	if lease == nil || lease.entry == nil {
		return nil
	}
	return lease.entry.runner
}

// Release returns the lease exactly once. Repeated calls are safe.
func (lease *RunnerLease) Release() error {
	if lease == nil || lease.registry == nil || lease.entry == nil {
		return nil
	}
	lease.once.Do(func() {
		lease.err = lease.release()
	})
	return lease.err
}

func (lease *RunnerLease) release() error {
	registry := lease.registry
	entry := lease.entry
	registry.mu.Lock()
	if entry.refs < 1 {
		registry.mu.Unlock()
		return nil
	}
	entry.refs--
	if entry.refs != 0 {
		registry.mu.Unlock()
		return nil
	}
	entry.lastUsed = registry.now()
	shouldClose := entry.invalidated || registry.closed
	if shouldClose {
		close(entry.zero)
	}
	registry.mu.Unlock()
	if shouldClose {
		return entry.close()
	}
	return nil
}
