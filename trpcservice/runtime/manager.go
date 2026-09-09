package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ErrStaleSnapshot indicates that an older configuration tried to replace the published head.
var ErrStaleSnapshot = errors.New("runtime: stale configuration snapshot")

// Runtime is the lifecycle surface cached by Manager.
type Runtime interface {
	Run(context.Context, RunInput) (RunResult, error)
	Close() error
}

// Factory constructs a Runtime from one immutable snapshot.
type Factory func(config.RuntimeSnapshot) (Runtime, error)

type key struct {
	tenantID, appID string
	version         tenant.ConfigVersion
}
type scope struct{ tenantID, appID string }
type entry struct {
	runtime Runtime
	refs    int
	retired bool
}
type buildState struct {
	done chan struct{}
	err  error
}

// Manager caches one Runtime per tenant/app/version and drains superseded versions.
type Manager struct {
	mu       sync.Mutex
	factory  Factory
	entries  map[key]*entry
	heads    map[scope]tenant.ConfigVersion
	building map[key]*buildState
	closed   bool
	changed  chan struct{}
}

// Lease pins a Runtime until Release.
type Lease struct {
	Runtime Runtime
	manager *Manager
	key     key
	once    sync.Once
}

// NewManager creates an empty Manager.
func NewManager(factory Factory) (*Manager, error) {
	if factory == nil {
		return nil, errors.New("runtime: nil factory")
	}
	return &Manager{factory: factory, entries: make(map[key]*entry), heads: make(map[scope]tenant.ConfigVersion), building: make(map[key]*buildState), changed: make(chan struct{}, 1)}, nil
}

// Acquire returns a reference-counted Runtime and retires older scope versions.
func (manager *Manager) Acquire(snapshot config.RuntimeSnapshot) (*Lease, error) {
	wanted := key{snapshot.TenantID(), snapshot.AppID(), snapshot.Version()}
	wantedScope := scope{wanted.tenantID, wanted.appID}
	for {
		manager.mu.Lock()
		if manager.closed {
			manager.mu.Unlock()
			return nil, errors.New("runtime: manager closed")
		}
		if head := manager.heads[wantedScope]; wanted.version < head {
			manager.mu.Unlock()
			return nil, fmt.Errorf("%w: have %d, head is %d", ErrStaleSnapshot, wanted.version, head)
		}
		if current := manager.entries[wanted]; current != nil {
			current.refs++
			manager.mu.Unlock()
			return &Lease{Runtime: current.runtime, manager: manager, key: wanted}, nil
		}
		if pending := manager.building[wanted]; pending != nil {
			manager.mu.Unlock()
			<-pending.done
			if pending.err != nil {
				return nil, pending.err
			}
			continue
		}
		pending := &buildState{done: make(chan struct{})}
		manager.building[wanted] = pending
		manager.mu.Unlock()

		built, err := manager.factory(snapshot)
		manager.mu.Lock()
		delete(manager.building, wanted)
		if err != nil {
			buildErr := fmt.Errorf("runtime: build bundle: %w", err)
			// Keep the previous Bundle cached for requests that are explicitly
			// pinned to its version, but never execute a newer request on it.
			// The caller retries the exact immutable version after the transient
			// build failure clears.
			pending.err = buildErr
			close(pending.done)
			manager.notifyChanged()
			manager.mu.Unlock()
			return nil, pending.err
		}
		if manager.closed || wanted.version < manager.heads[wantedScope] {
			if manager.closed {
				pending.err = errors.New("runtime: manager closed")
			} else {
				pending.err = fmt.Errorf("%w: have %d, head is %d", ErrStaleSnapshot, wanted.version, manager.heads[wantedScope])
			}
			close(pending.done)
			manager.notifyChanged()
			manager.mu.Unlock()
			_ = built.Close()
			return nil, pending.err
		}
		manager.entries[wanted] = &entry{runtime: built, refs: 1}
		manager.heads[wantedScope] = wanted.version
		close(pending.done)
		manager.notifyChanged()
		var closeNow []Runtime
		for candidate, value := range manager.entries {
			if candidate != wanted && candidate.tenantID == wanted.tenantID && candidate.appID == wanted.appID {
				value.retired = true
				if value.refs == 0 {
					delete(manager.entries, candidate)
					closeNow = append(closeNow, value.runtime)
				}
			}
		}
		manager.mu.Unlock()
		for _, item := range closeNow {
			_ = item.Close()
		}
		return &Lease{Runtime: built, manager: manager, key: wanted}, nil
	}
}

// Release decrements the in-flight count and closes a retired Runtime at zero.
func (lease *Lease) Release() {
	if lease == nil || lease.manager == nil {
		return
	}
	lease.once.Do(func() { lease.manager.release(lease.key) })
}

func (manager *Manager) release(released key) {
	manager.mu.Lock()
	current := manager.entries[released]
	var closeNow Runtime
	if current != nil {
		current.refs--
		if current.refs == 0 && current.retired {
			delete(manager.entries, released)
			closeNow = current.runtime
		}
	}
	manager.notifyChanged()
	manager.mu.Unlock()
	if closeNow != nil {
		_ = closeNow.Close()
	}
}

func (manager *Manager) notifyChanged() {
	select {
	case manager.changed <- struct{}{}:
	default:
	}
}

// Close rejects new acquisitions, waits for leases, and closes every Runtime.
func (manager *Manager) Close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("runtime: nil close context")
	}
	manager.mu.Lock()
	manager.closed = true
	manager.mu.Unlock()
	for {
		manager.mu.Lock()
		active := 0
		for _, value := range manager.entries {
			active += value.refs
		}
		active += len(manager.building)
		if active == 0 {
			runtimes := make([]Runtime, 0, len(manager.entries))
			for _, value := range manager.entries {
				runtimes = append(runtimes, value.runtime)
			}
			manager.entries = make(map[key]*entry)
			manager.mu.Unlock()
			var errs []error
			for _, item := range runtimes {
				errs = append(errs, item.Close())
			}
			return errors.Join(errs...)
		}
		manager.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-manager.changed:
		}
	}
}
