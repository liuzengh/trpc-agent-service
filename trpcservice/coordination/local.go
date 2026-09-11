package coordination

import (
	"context"
	"sync"
	"sync/atomic"
)

type localEntry struct {
	semaphore  chan struct{}
	references int
}

// LocalCoordinator serializes the same Session inside one process while
// allowing unrelated Sessions to execute concurrently.
type LocalCoordinator struct {
	mu          sync.Mutex
	entries     map[Key]*localEntry
	closed      bool
	closeCtx    context.Context
	closeCancel context.CancelCauseFunc
	nextToken   atomic.Int64
}

// NewLocalCoordinator creates a process-local keyed coordinator.
func NewLocalCoordinator() *LocalCoordinator {
	closeCtx, closeCancel := context.WithCancelCause(context.Background())
	return &LocalCoordinator{
		entries:     make(map[Key]*localEntry),
		closeCtx:    closeCtx,
		closeCancel: closeCancel,
	}
}

// Acquire waits until no other turn in this process owns key.
func (c *LocalCoordinator) Acquire(ctx context.Context, key Key) (Lease, error) {
	if err := key.Validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	entry, err := c.retainEntry(key)
	if err != nil {
		return nil, err
	}

	select {
	case <-entry.semaphore:
		if err := ctx.Err(); err != nil {
			entry.semaphore <- struct{}{}
			c.releaseEntry(key, entry)
			return nil, err
		}
		if cause := context.Cause(c.closeCtx); cause != nil {
			entry.semaphore <- struct{}{}
			c.releaseEntry(key, entry)
			return nil, cause
		}
	case <-ctx.Done():
		c.releaseEntry(key, entry)
		return nil, context.Cause(ctx)
	case <-c.closeCtx.Done():
		c.releaseEntry(key, entry)
		return nil, context.Cause(c.closeCtx)
	}

	leaseCtx, leaseCancel := context.WithCancelCause(ctx)
	stopCloseCallback := context.AfterFunc(c.closeCtx, func() {
		leaseCancel(ErrCoordinatorClosed)
	})
	return &localLease{
		ctx:               leaseCtx,
		cancel:            leaseCancel,
		stopCloseCallback: stopCloseCallback,
		coordinator:       c,
		key:               key,
		entry:             entry,
		token:             c.nextToken.Add(1),
	}, nil
}

func (c *LocalCoordinator) retainEntry(key Key) (*localEntry, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, ErrCoordinatorClosed
	}
	entry := c.entries[key]
	if entry == nil {
		entry = &localEntry{semaphore: make(chan struct{}, 1)}
		entry.semaphore <- struct{}{}
		c.entries[key] = entry
	}
	entry.references++
	return entry, nil
}

func (c *LocalCoordinator) releaseEntry(key Key, entry *localEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry.references--
	if entry.references == 0 && c.entries[key] == entry {
		delete(c.entries, key)
	}
}

// Ready reports whether the local coordinator is still open.
func (c *LocalCoordinator) Ready(ctx context.Context) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrCoordinatorClosed
	}
	return nil
}

// Close rejects new leases and cancels active lease contexts.
func (c *LocalCoordinator) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	c.closeCancel(ErrCoordinatorClosed)
	return nil
}

type localLease struct {
	ctx               context.Context
	cancel            context.CancelCauseFunc
	stopCloseCallback func() bool
	coordinator       *LocalCoordinator
	key               Key
	entry             *localEntry
	token             int64
	once              sync.Once
}

func (l *localLease) Context() context.Context {
	return l.ctx
}

func (l *localLease) FencingToken() int64 {
	return l.token
}

func (l *localLease) Release(context.Context) error {
	if l == nil {
		return nil
	}
	l.once.Do(func() {
		if l.stopCloseCallback != nil {
			l.stopCloseCallback()
		}
		l.cancel(nil)
		l.entry.semaphore <- struct{}{}
		l.coordinator.releaseEntry(l.key, l.entry)
	})
	return nil
}

var _ Coordinator = (*LocalCoordinator)(nil)
var _ Lease = (*localLease)(nil)

func coordinatorCause(ctx context.Context) error {
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return ErrCoordinatorClosed
}
