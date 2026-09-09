package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"golang.org/x/sync/singleflight"
)

var ErrResolverClosed = errors.New("agent: runtime resolver is closed")

// RevisionSource is the minimum control-plane contract needed by a Worker.
type RevisionSource interface {
	ResolveRevision(context.Context, tenant.TenantContext, string, string) (tenant.AgentRevision, error)
}

type RuntimeBuildFunc func(context.Context, tenant.AgentRevision) (*Runtime, error)

type runtimeKey struct {
	tenantID   string
	appID      string
	revisionID string
}

type runtimeEntry struct {
	runtime    *Runtime
	references int
}

type runtimeLease struct {
	once    sync.Once
	release func()
}

// ResolvedRuntime is a lease on a cached runtime. Release must be called only
// after the Runner event channel has been fully consumed.
type ResolvedRuntime struct {
	Runtime  *Runtime
	Revision tenant.AgentRevision
	lease    *runtimeLease
}

// Release allows shutdown to close the Runtime after all active Runs finish.
func (r ResolvedRuntime) Release() {
	if r.lease == nil {
		return
	}
	r.lease.once.Do(r.lease.release)
}

// RuntimeResolver resolves the published revision and builds each immutable
// tenant/app/revision runtime at most once while it remains cached.
type RuntimeResolver struct {
	source RevisionSource
	build  RuntimeBuildFunc

	mu           sync.Mutex
	cache        map[runtimeKey]*runtimeEntry
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

func NewRuntimeResolver(source RevisionSource, build RuntimeBuildFunc) (*RuntimeResolver, error) {
	if source == nil {
		return nil, fmt.Errorf("agent: revision source is required")
	}
	if build == nil {
		return nil, fmt.Errorf("agent: runtime builder is required")
	}
	lifecycleCtx, cancel := context.WithCancel(context.Background())
	resolver := &RuntimeResolver{
		source:       source,
		build:        build,
		cache:        make(map[runtimeKey]*runtimeEntry),
		lifecycleCtx: lifecycleCtx,
		cancel:       cancel,
		closeDone:    make(chan struct{}),
	}
	resolver.leasesDone = sync.NewCond(&resolver.mu)
	return resolver, nil
}

func (r *RuntimeResolver) Resolve(
	ctx context.Context,
	scope tenant.TenantContext,
	appID string,
	pinnedRevisionID string,
) (ResolvedRuntime, error) {
	if ctx == nil {
		return ResolvedRuntime{}, fmt.Errorf("agent: context is required")
	}
	if err := ctx.Err(); err != nil {
		return ResolvedRuntime{}, err
	}
	if r == nil {
		return ResolvedRuntime{}, fmt.Errorf("agent: runtime resolver is nil")
	}
	if r.isClosed() {
		return ResolvedRuntime{}, ErrResolverClosed
	}
	revision, err := r.source.ResolveRevision(ctx, scope, appID, pinnedRevisionID)
	if err != nil {
		return ResolvedRuntime{}, fmt.Errorf("agent: resolve revision: %w", err)
	}
	if revision.TenantID != scope.TenantID || revision.AgentAppID != appID {
		return ResolvedRuntime{}, fmt.Errorf(
			"%w: revision %q resolved as tenant %q app %q",
			tenant.ErrTenantScope,
			revision.ID,
			revision.TenantID,
			revision.AgentAppID,
		)
	}
	key := runtimeKey{
		tenantID:   revision.TenantID,
		appID:      revision.AgentAppID,
		revisionID: revision.ID,
	}
	if resolved, ok := r.acquire(key, revision); ok {
		return resolved, nil
	}

	resultChannel := r.builds.DoChan(key.String(), func() (any, error) {
		if runtime := r.cached(key); runtime != nil {
			return runtime, nil
		}
		if !r.beginBuild() {
			return nil, ErrResolverClosed
		}
		defer r.buildsDone.Done()
		runtime, buildErr := r.build(r.lifecycleCtx, revision)
		if buildErr != nil {
			return nil, fmt.Errorf("agent: build revision %q: %w", revision.ID, buildErr)
		}
		if runtime == nil {
			return nil, fmt.Errorf("agent: builder returned nil runtime for revision %q", revision.ID)
		}
		return r.storeBuilt(key, revision, runtime)
	})

	select {
	case <-ctx.Done():
		return ResolvedRuntime{}, ctx.Err()
	case result := <-resultChannel:
		if result.Err != nil {
			return ResolvedRuntime{}, result.Err
		}
		if _, ok := result.Val.(*Runtime); !ok {
			return ResolvedRuntime{}, fmt.Errorf("agent: runtime builder returned unexpected type")
		}
		resolved, ok := r.acquire(key, revision)
		if !ok {
			return ResolvedRuntime{}, ErrResolverClosed
		}
		return resolved, nil
	}
}

func (r *RuntimeResolver) CacheSize() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.cache)
}

func (r *RuntimeResolver) Close() error {
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

func (r *RuntimeResolver) closeAll() error {
	r.mu.Lock()
	r.closed = true
	r.cancel()
	r.mu.Unlock()

	r.buildsDone.Wait()

	r.mu.Lock()
	for r.activeLeases > 0 {
		r.leasesDone.Wait()
	}
	runtimes := make([]*Runtime, 0, len(r.cache))
	for key, entry := range r.cache {
		runtimes = append(runtimes, entry.runtime)
		delete(r.cache, key)
	}
	r.mu.Unlock()

	var closeErr error
	for _, runtime := range runtimes {
		closeErr = errors.Join(closeErr, runtime.Close())
	}
	return closeErr
}

func (r *RuntimeResolver) acquire(
	key runtimeKey,
	revision tenant.AgentRevision,
) (ResolvedRuntime, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ResolvedRuntime{}, false
	}
	entry := r.cache[key]
	if entry == nil {
		return ResolvedRuntime{}, false
	}
	entry.references++
	r.activeLeases++
	return ResolvedRuntime{
		Runtime:  entry.runtime,
		Revision: revision,
		lease: &runtimeLease{release: func() {
			r.release(entry)
		}},
	}, true
}

func (r *RuntimeResolver) release(entry *runtimeEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry.references--
	r.activeLeases--
	if r.activeLeases == 0 {
		r.leasesDone.Broadcast()
	}
}

func (r *RuntimeResolver) cached(key runtimeKey) *Runtime {
	r.mu.Lock()
	defer r.mu.Unlock()
	if entry := r.cache[key]; entry != nil {
		return entry.runtime
	}
	return nil
}

func (r *RuntimeResolver) isClosed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.closed
}

func (r *RuntimeResolver) beginBuild() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	r.buildsDone.Add(1)
	return true
}

func (r *RuntimeResolver) storeBuilt(
	key runtimeKey,
	revision tenant.AgentRevision,
	runtime *Runtime,
) (*Runtime, error) {
	if err := runtime.validateFor(revision); err != nil {
		closeErr := runtime.Close()
		return nil, errors.Join(
			fmt.Errorf("agent: invalid runtime for revision %q: %w", revision.ID, err),
			closeErr,
		)
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		closeErr := runtime.Close()
		return nil, errors.Join(ErrResolverClosed, closeErr)
	}
	if existing := r.cache[key]; existing != nil {
		r.mu.Unlock()
		closeErr := runtime.Close()
		if closeErr != nil {
			return nil, fmt.Errorf("agent: close duplicate runtime: %w", closeErr)
		}
		return existing.runtime, nil
	}
	r.cache[key] = &runtimeEntry{runtime: runtime}
	r.mu.Unlock()
	return runtime, nil
}

func (k runtimeKey) String() string {
	return k.tenantID + "\x00" + k.appID + "\x00" + k.revisionID
}
