package storagebundle

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"golang.org/x/sync/singleflight"
)

// Options configures a Router.
type Options struct {
	// Default is the Bundle the empty profile id resolves to: the store this
	// process was started with. It is borrowed, not transferred — whoever
	// built it still closes it, and the Router never does. Transferring it is
	// not expressible here anyway: the startup path has to be able to release
	// it on a partial failure, at a point where no Router exists yet.
	Default Bundle

	// Source resolves dynamic profiles. Required; a process with none passes
	// NoProfiles().
	Source ProfileSource

	// Factory builds what Source describes. Required.
	Factory Factory
}

// Router resolves a revision's backend profile id to a Bundle, building each
// (tenant, profile id) at most once while it stays cached.
//
// Its lifecycle mirrors agent.RuntimeResolver, deliberately and line for line:
// the two sit one above the other on the shutdown path, and a Router that
// stopped waiting for its holders in a different way would be a second set of
// shutdown rules to reason about. Close refuses new work, cancels the build
// context, waits for in-flight builds, waits for every lease — the default
// one included — and only then closes what it owns.
//
// Nothing this type owns is evicted before Close. A profile that was built
// stays built, which bounds nothing but is honest: the alternative, closing a
// store while a Runtime may still be assembling around it, is the failure this
// package exists to prevent.
type Router struct {
	defaultBundle Bundle
	source        ProfileSource
	factory       Factory

	mu           sync.Mutex
	cache        map[profileKey]*bundleEntry
	owned        []*bundleEntry
	activeLeases int
	leasesDone   *sync.Cond
	closed       bool
	builds       singleflight.Group
	buildsDone   sync.WaitGroup
	lifecycleCtx context.Context
	cancel       context.CancelFunc
	closeOnce    sync.Once
	closeDone    chan struct{}
	closeErr     error
}

// bundleEntry is one cached Bundle and everything needed to decide whether it
// still answers the profile that asked for it.
//
// There is deliberately no per-entry reference count. Nothing is evicted before
// Close, so no decision anywhere depends on how many holders one entry has — the
// only question ever asked is whether the Router as a whole still has holders,
// and activeLeases answers it. A per-entry count would be a number that is
// maintained, locked and never read, which is the kind of field a later change
// mistakes for a working eviction check.
type bundleEntry struct {
	bundle      Bundle
	close       func() error
	fingerprint string
}

func NewRouter(options Options) (*Router, error) {
	if options.Source == nil {
		return nil, errors.New("storagebundle: profile source is required")
	}
	if options.Factory == nil {
		return nil, errors.New("storagebundle: bundle factory is required")
	}
	// A Router with no default cannot serve the revisions that name no
	// profile, which is nearly all of them. Refusing here makes that a
	// startup failure instead of a per-request one.
	if err := options.Default.Validate(); err != nil {
		return nil, fmt.Errorf("storagebundle: default bundle: %w", err)
	}
	lifecycleCtx, cancel := context.WithCancel(context.Background())
	router := &Router{
		defaultBundle: options.Default,
		source:        options.Source,
		factory:       options.Factory,
		cache:         make(map[profileKey]*bundleEntry),
		lifecycleCtx:  lifecycleCtx,
		cancel:        cancel,
		closeDone:     make(chan struct{}),
	}
	router.leasesDone = sync.NewCond(&router.mu)
	return router, nil
}

// Resolve returns a lease on the Bundle this profile id names, within scope.
//
// The empty id is this process's default store, borrowed. Every other id is
// resolved from the Source on every call — not once per cache entry — and its
// fingerprint is compared with the one its Bundle was built from, so a profile
// whose content moved under a stable id is reported instead of served.
func (r *Router) Resolve(
	ctx context.Context,
	scope tenant.TenantContext,
	profileID string,
) (Lease, error) {
	if r == nil {
		return nil, errors.New("storagebundle: router is nil")
	}
	if ctx == nil {
		return nil, errors.New("storagebundle: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if r.isClosed() {
		return nil, ErrRouterClosed
	}
	if profileID == "" {
		return r.acquireDefault()
	}
	// Before the Source is asked: an id that could never name a profile is a
	// bad request, and it must not become a lookup, a cache key or a
	// singleflight key.
	if err := tenant.ValidateResourceID("backend profile id", profileID); err != nil {
		return nil, err
	}
	profile, err := r.source.ResolveProfile(ctx, scope, profileID)
	if err != nil {
		return nil, fmt.Errorf("storagebundle: resolve profile %q: %w", profileID, err)
	}
	if profile.TenantID != scope.TenantID {
		return nil, fmt.Errorf(
			"%w: backend profile %q resolved as tenant %q",
			tenant.ErrTenantScope, profileID, profile.TenantID,
		)
	}
	if profile.ID != profileID {
		// A source that answers with a different profile than the one asked
		// for is broken, and the safe reading of a broken answer is that the
		// requested profile was not found.
		return nil, fmt.Errorf(
			"%w: source resolved %q for request %q", ErrProfileNotFound, profile.ID, profileID)
	}
	fingerprint, err := profile.Fingerprint()
	if err != nil {
		return nil, fmt.Errorf("storagebundle: profile %q: %w", profileID, err)
	}
	key := profileKey{tenantID: scope.TenantID, profileID: profileID}
	lease, err := r.acquire(key, fingerprint)
	if err != nil || lease != nil {
		return lease, err
	}

	resultChannel := r.builds.DoChan(key.String(), func() (any, error) {
		return r.build(key, fingerprint, profile)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultChannel:
		if result.Err != nil {
			return nil, result.Err
		}
		lease, err := r.acquire(key, fingerprint)
		if err != nil {
			return nil, err
		}
		if lease == nil {
			// Built, then closed by a Close that overtook this caller.
			return nil, ErrRouterClosed
		}
		return lease, nil
	}
}

// CacheSize reports how many Bundles this Router currently owns.
func (r *Router) CacheSize() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.cache)
}

// Close releases every Bundle this Router built, after every holder has let go.
//
// It is idempotent and every caller waits for the same result. Ordering it
// wrong — closing the Router while a Runtime still holds a lease — blocks here
// rather than pulling a store out from under a live conversation; that is the
// same failure mode as agent.RuntimeResolver.Close and the same deliberate
// trade.
func (r *Router) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		r.closeErr = r.closeAll()
		close(r.closeDone)
	})
	<-r.closeDone
	return r.closeErr
}

func (r *Router) closeAll() error {
	r.mu.Lock()
	r.closed = true
	r.cancel()
	r.mu.Unlock()

	r.buildsDone.Wait()

	r.mu.Lock()
	for r.activeLeases > 0 {
		r.leasesDone.Wait()
	}
	entries := r.owned
	r.owned = nil
	r.cache = make(map[profileKey]*bundleEntry)
	r.mu.Unlock()

	// Outside the lock, and in reverse order of construction. The Factory's
	// close is somebody else's code: running it under r.mu would put an
	// unknown call inside this package's critical section, and a Lease.Release
	// arriving from another goroutine would deadlock against it.
	//
	// The default Bundle is not here and never will be. It belongs to whoever
	// built the process's storage, and it outlives this Router by design.
	var closeErr error
	for i := len(entries) - 1; i >= 0; i-- {
		closeErr = errors.Join(closeErr, entries[i].close())
	}
	return closeErr
}

// build is the singleflight body: one construction per key at a time, under the
// Router's lifecycle context rather than any caller's.
func (r *Router) build(
	key profileKey,
	fingerprint string,
	profile Profile,
) (*bundleEntry, error) {
	if existing := r.cached(key); existing != nil {
		return existing, nil
	}
	if !r.beginBuild() {
		return nil, ErrRouterClosed
	}
	defer r.buildsDone.Done()
	bundle, closeBundle, err := r.factory.Build(r.lifecycleCtx, profile)
	if err != nil {
		return nil, fmt.Errorf("storagebundle: build profile %q: %w", key.profileID, err)
	}
	if closeBundle == nil {
		// A Bundle that arrived without its close cannot be handed back to the
		// Factory, so the only release left is the one Own performs. It is not
		// cached either way: a Factory that breaks this half of its contract
		// has not earned the Router's trust for the other half.
		return nil, errors.Join(
			fmt.Errorf(
				"storagebundle: factory returned no close for profile %q", key.profileID),
			Own(bundle).Release(),
		)
	}
	if err := bundle.Validate(); err != nil {
		return nil, errors.Join(
			fmt.Errorf("storagebundle: factory built profile %q: %w", key.profileID, err),
			closeBundle(),
		)
	}
	return r.storeBuilt(key, fingerprint, bundle, closeBundle)
}

// storeBuilt publishes a freshly built Bundle, or closes it if it turns out
// nobody can use it.
func (r *Router) storeBuilt(
	key profileKey,
	fingerprint string,
	bundle Bundle,
	closeBundle func() error,
) (*bundleEntry, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		// Close has already passed the point where it waits for builds, so
		// this Bundle is this goroutine's to release.
		return nil, errors.Join(ErrRouterClosed, closeBundle())
	}
	if existing := r.cache[key]; existing != nil {
		r.mu.Unlock()
		if err := closeBundle(); err != nil {
			return nil, fmt.Errorf("storagebundle: close duplicate bundle: %w", err)
		}
		return existing, nil
	}
	entry := &bundleEntry{bundle: bundle, close: closeBundle, fingerprint: fingerprint}
	r.cache[key] = entry
	r.owned = append(r.owned, entry)
	r.mu.Unlock()
	return entry, nil
}

// acquire takes a reference on a cached Bundle. A nil lease and a nil error
// mean "not cached yet"; a closed Router is ErrRouterClosed.
func (r *Router) acquire(key profileKey, fingerprint string) (Lease, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrRouterClosed
	}
	entry := r.cache[key]
	if entry == nil {
		return nil, nil
	}
	if entry.fingerprint != fingerprint {
		// Not a cache miss and not a reason to rebuild. The id is the version,
		// so this Bundle may already be the wrong storage for what the profile
		// says now, and building a second one under the same key would make
		// two answers to one question. The cache is left exactly as it was.
		return nil, fmt.Errorf(
			"%w: backend profile %q of tenant %q", ErrProfileChanged, key.profileID, key.tenantID)
	}
	r.activeLeases++
	return &routerLease{bundle: entry.bundle, release: r.releaseLease}, nil
}

// acquireDefault takes a reference on the borrowed process default.
//
// It counts. The Router closes nothing here, but Close must still wait for it:
// the holder is a Runtime, and the store it is holding is closed by the
// storageStack immediately after this Router returns from Close. Not counting
// it would let shutdown walk past a live Runtime and close the session service
// underneath it.
func (r *Router) acquireDefault() (Lease, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrRouterClosed
	}
	r.activeLeases++
	return &routerLease{bundle: r.defaultBundle, release: r.releaseLease}, nil
}

// releaseLease records one lease going away, dynamic or default alike. Which
// Bundle it pointed at does not matter here: Close waits on the total, and the
// stores themselves are released afterwards from r.owned.
func (r *Router) releaseLease() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.activeLeases--
	if r.activeLeases == 0 {
		r.leasesDone.Broadcast()
	}
}

func (r *Router) cached(key profileKey) *bundleEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.cache[key]
}

func (r *Router) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

func (r *Router) beginBuild() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.buildsDone.Add(1)
	return true
}

// routerLease is a reference count, not an owner. Release returns nil because
// there is nothing to fail: the Bundle it points at is closed by the Router, on
// the Router's own schedule.
type routerLease struct {
	bundle  Bundle
	once    sync.Once
	release func()
}

func (l *routerLease) Bundle() Bundle {
	return l.bundle
}

func (l *routerLease) Release() error {
	if l == nil {
		return nil
	}
	l.once.Do(l.release)
	return nil
}
