package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	frameworkrunner "trpc.group/trpc-go/trpc-agent-go/runner"
)

var (
	ErrCacheClosed = errors.New("runner cache is closed")
	ErrCacheFull   = errors.New("runner cache is full")
	ErrRunnerDrain = errors.New("runner is draining")
)

type CacheKey struct {
	TenantID      string
	AgentAppID    string
	ConfigVersion string
}

type CacheConfig struct {
	MaxEntries    int
	IdleTTL       time.Duration
	CreateTimeout time.Duration
	DrainTimeout  time.Duration
	CloseTimeout  time.Duration
}

func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		MaxEntries:    32,
		IdleTTL:       30 * time.Minute,
		CreateTimeout: 10 * time.Second,
		DrainTimeout:  30 * time.Second,
		CloseTimeout:  5 * time.Second,
	}
}

type RunnerFactory func(context.Context, CacheKey) (frameworkrunner.Runner, error)

type cacheEntry struct {
	key       CacheKey
	runner    frameworkrunner.Runner
	refs      int
	lastUsed  time.Time
	draining  bool
	close     sync.Once
	closeDone chan struct{}
	closeErr  error
}

func (e *cacheEntry) closeRunner(timeout time.Duration) error {
	e.close.Do(func() {
		if e.closeDone == nil {
			e.closeDone = make(chan struct{})
		}
		go func() {
			e.closeErr = e.runner.Close()
			close(e.closeDone)
		}()
	})
	if timeout <= 0 {
		<-e.closeDone
		return e.closeErr
	}
	select {
	case <-e.closeDone:
		return e.closeErr
	case <-time.After(timeout):
		return fmt.Errorf("close runner %s timed out after %s", e.key.AgentAppID, timeout)
	}
}

type creation struct {
	done chan struct{}
	err  error
}

type RunnerCache struct {
	mu       sync.Mutex
	entries  map[CacheKey]*cacheEntry
	creating map[CacheKey]*creation
	closed   bool
	factory  RunnerFactory
	config   CacheConfig
}

func NewRunnerCache(config CacheConfig, factory RunnerFactory) (*RunnerCache, error) {
	if factory == nil {
		return nil, errors.New("runner factory is required")
	}
	if config.MaxEntries <= 0 || config.IdleTTL <= 0 || config.CreateTimeout <= 0 || config.DrainTimeout <= 0 || config.CloseTimeout <= 0 {
		return nil, errors.New("runner cache configuration must be positive")
	}
	return &RunnerCache{
		entries:  make(map[CacheKey]*cacheEntry),
		creating: make(map[CacheKey]*creation),
		factory:  factory,
		config:   config,
	}, nil
}

type Lease struct {
	Runner frameworkrunner.Runner
	cache  *RunnerCache
	entry  *cacheEntry
	once   sync.Once
}

func (l *Lease) Release() {
	if l == nil || l.cache == nil || l.entry == nil {
		return
	}
	l.once.Do(func() {
		l.cache.release(l.entry)
	})
}

func (c *RunnerCache) Acquire(ctx context.Context, key CacheKey) (*Lease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil, ErrCacheClosed
		}
		if entry, ok := c.entries[key]; ok {
			if entry.draining {
				c.mu.Unlock()
				return nil, ErrRunnerDrain
			}
			entry.refs++
			entry.lastUsed = time.Now()
			c.mu.Unlock()
			return &Lease{Runner: entry.runner, cache: c, entry: entry}, nil
		}
		if pending, ok := c.creating[key]; ok {
			done := pending.done
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-done:
				if pending.err != nil {
					return nil, pending.err
				}
				continue
			}
		}

		victims := c.evictExpiredLocked(time.Now())
		if len(c.entries)+len(c.creating) >= c.config.MaxEntries {
			if victim := c.evictLRULocked(); victim != nil {
				victims = append(victims, victim)
			} else {
				c.mu.Unlock()
				c.closeEntries(victims)
				return nil, ErrCacheFull
			}
		}
		pending := &creation{done: make(chan struct{})}
		c.creating[key] = pending
		c.mu.Unlock()
		c.closeEntries(victims)

		createCtx, cancel := context.WithTimeout(ctx, c.config.CreateTimeout)
		runner, err := c.factory(createCtx, key)
		if err == nil {
			err = createCtx.Err()
		}
		cancel()

		c.mu.Lock()
		delete(c.creating, key)
		pending.err = err
		if err == nil && runner == nil {
			err = errors.New("runner factory returned nil runner")
			pending.err = err
		}
		if err == nil && !c.closed {
			entry := &cacheEntry{key: key, runner: runner, refs: 1, lastUsed: time.Now(), closeDone: make(chan struct{})}
			c.entries[key] = entry
			close(pending.done)
			c.mu.Unlock()
			return &Lease{Runner: runner, cache: c, entry: entry}, nil
		}
		closed := c.closed
		close(pending.done)
		c.mu.Unlock()
		if runner != nil {
			_ = (&cacheEntry{key: key, runner: runner, closeDone: make(chan struct{})}).closeRunner(c.config.CloseTimeout)
		}
		if err == nil && closed {
			return nil, ErrCacheClosed
		}
		if err != nil {
			return nil, err
		}
	}
}

func (c *RunnerCache) evictLRULocked() *cacheEntry {
	var victim *cacheEntry
	for _, entry := range c.entries {
		if entry.refs != 0 || entry.draining {
			continue
		}
		if victim == nil || entry.lastUsed.Before(victim.lastUsed) {
			victim = entry
		}
	}
	if victim != nil {
		delete(c.entries, victim.key)
	}
	return victim
}

// Ready reports whether a specific key may accept new runs.
func (c *RunnerCache) Ready(key CacheKey) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrCacheClosed
	}
	if entry, ok := c.entries[key]; ok && entry.draining {
		return ErrRunnerDrain
	}
	return nil
}

func (c *RunnerCache) Available() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrCacheClosed
	}
	return nil
}

func (c *RunnerCache) release(entry *cacheEntry) {
	c.mu.Lock()
	if entry.refs > 0 {
		entry.refs--
	}
	entry.lastUsed = time.Now()
	c.mu.Unlock()
}

func (c *RunnerCache) evictExpiredLocked(now time.Time) []*cacheEntry {
	if c.config.IdleTTL <= 0 {
		return nil
	}
	var victims []*cacheEntry
	for key, entry := range c.entries {
		if entry.refs == 0 && !entry.draining && now.Sub(entry.lastUsed) >= c.config.IdleTTL {
			delete(c.entries, key)
			victims = append(victims, entry)
		}
	}
	return victims
}

func (c *RunnerCache) closeEntries(entries []*cacheEntry) {
	for _, entry := range entries {
		_ = entry.closeRunner(c.config.CloseTimeout)
	}
}

func (c *RunnerCache) Drain(ctx context.Context, key CacheKey) error {
	if ctx == nil {
		ctx = context.Background()
	}
	drainCtx, cancel := context.WithTimeout(ctx, c.config.DrainTimeout)
	defer cancel()
	c.mu.Lock()
	entry, ok := c.entries[key]
	if !ok {
		c.mu.Unlock()
		return nil
	}
	entry.draining = true
	c.mu.Unlock()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		c.mu.Lock()
		refs := entry.refs
		c.mu.Unlock()
		if refs == 0 {
			return c.removeAndClose(entry)
		}
		select {
		case <-drainCtx.Done():
			closeErr := c.removeAndClose(entry)
			return errors.Join(fmt.Errorf("drain runner %s: %w", key.AgentAppID, drainCtx.Err()), closeErr)
		case <-ticker.C:
		}
	}
}

func (c *RunnerCache) removeAndClose(entry *cacheEntry) error {
	c.mu.Lock()
	if current, ok := c.entries[entry.key]; ok && current == entry {
		delete(c.entries, entry.key)
	}
	c.mu.Unlock()
	return entry.closeRunner(c.config.CloseTimeout)
}

func (c *RunnerCache) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	entries := make([]*cacheEntry, 0, len(c.entries))
	for key, entry := range c.entries {
		entries = append(entries, entry)
		delete(c.entries, key)
	}
	c.mu.Unlock()

	var errs []error
	for _, entry := range entries {
		if err := entry.closeRunner(c.config.CloseTimeout); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
