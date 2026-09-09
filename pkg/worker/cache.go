package worker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

var (
	ErrCacheClosed    = errors.New("worker cache is closed")
	ErrCacheSaturated = errors.New("worker cache has no idle capacity")
	// ErrProcessorCloseFailed is returned when a provider processor rejects
	// cleanup. Provider errors are deliberately not retained because they may
	// contain model endpoints, credentials or other connection material.
	ErrProcessorCloseFailed = errors.New("worker processor close failed")
	// ErrProcessorClosePanic identifies a provider processor that panicked
	// during cleanup. The panic value is deliberately discarded.
	ErrProcessorClosePanic = errors.New("worker processor close panicked")
	// ErrProcessorBuildPanic identifies a factory that panicked while creating
	// a processor. The panic value is deliberately discarded.
	ErrProcessorBuildPanic = errors.New("worker processor build panicked")
)

// Processor is the immutable, version-bound execution object held by Cache.
// Worker implements this interface; the abstraction keeps cache lifecycle
// tests independent from model providers.
type Processor interface {
	Process(ctx context.Context, request *Request) (*Response, error)
	Close() error
}

// CacheKey contains every mutable control-plane dimension that can change a
// Worker's model, policy, storage namespace or tool set. A retry pinned to the
// same immutable version therefore reuses the same Runner, while any tenant or
// deployment change creates a separate entry.
type CacheKey struct {
	TenantID            string
	TenantConfigVersion int64
	AgentApp            string
	AgentVersionID      string
	DeploymentID        string
}

func (k CacheKey) validate() error {
	if k.TenantID == "" || k.TenantConfigVersion <= 0 || k.AgentApp == "" ||
		k.AgentVersionID == "" || k.DeploymentID == "" {
		return fmt.Errorf("worker cache key requires tenant, positive config version, app, version and deployment")
	}
	return nil
}

// CacheOptions bounds resident model/Runner instances.
type CacheOptions struct {
	MaxEntries int
	IdleTTL    time.Duration
	Clock      func() time.Time
}

type cacheEntry struct {
	ready     chan struct{}
	processor Processor
	buildErr  error
	refs      int
	lastUsed  time.Time
	retiring  bool
}

// Cache is a bounded, reference-counted cache. Entries in active use are never
// evicted or closed. If every slot is busy, Acquire fails fast rather than
// exceeding the configured model-memory bound.
type Cache struct {
	mu            sync.Mutex
	entries       map[CacheKey]*cacheEntry
	maxEntries    int
	idleTTL       time.Duration
	clock         func() time.Time
	closed        bool
	notify        chan struct{}
	closeErrs     []error
	closeInFlight int
}

func NewCache(options CacheOptions) *Cache {
	if options.MaxEntries <= 0 {
		options.MaxEntries = 128
	}
	if options.IdleTTL <= 0 {
		options.IdleTTL = 10 * time.Minute
	}
	if options.Clock == nil {
		options.Clock = time.Now
	}
	return &Cache{
		entries: make(map[CacheKey]*cacheEntry), maxEntries: options.MaxEntries,
		idleTTL: options.IdleTTL, clock: options.Clock, notify: make(chan struct{}, 1),
	}
}

// Acquire returns one immutable processor and an idempotent release function.
// Concurrent first-use calls for the same key share one factory invocation.
func (c *Cache) Acquire(ctx context.Context, key CacheKey, factory func(context.Context) (Processor, error)) (Processor, func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := key.validate(); err != nil {
		return nil, nil, err
	}
	if factory == nil {
		return nil, nil, fmt.Errorf("worker cache factory is required")
	}
	for {
		processor, release, wait, evicted, err := c.acquireOrReserve(key)
		// The new key may already have a reserved build slot. Retirement
		// failures are retained for Close(), while replacement construction
		// must continue so that slot and its waiters are not stranded.
		_ = c.closeProcessorsTracked(evicted)
		if err != nil || !isNilProcessor(processor) {
			return processor, release, err
		}
		if wait != nil {
			select {
			case <-ctx.Done():
				return nil, nil, ctx.Err()
			case <-wait:
				continue
			}
		}

		created, buildErr := safeBuildProcessor(factory, ctx)
		return c.publishBuild(key, created, buildErr)
	}
}

func safeBuildProcessor(factory func(context.Context) (Processor, error), ctx context.Context) (processor Processor, err error) {
	defer func() {
		if recover() != nil {
			processor = nil
			err = ErrProcessorBuildPanic
		}
	}()
	return factory(ctx)
}

func (c *Cache) acquireOrReserve(key CacheKey) (Processor, func(), <-chan struct{}, []Processor, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, nil, nil, nil, ErrCacheClosed
	}
	if entry, ok := c.entries[key]; ok {
		select {
		case <-entry.ready:
			if entry.buildErr != nil {
				return nil, nil, nil, nil, entry.buildErr
			}
			entry.refs++
			return entry.processor, c.releaseFunc(key, entry), nil, nil, nil
		default:
			return nil, nil, entry.ready, nil, nil
		}
	}

	evicted := c.evictExpiredLocked()
	if len(c.entries) >= c.maxEntries {
		oldestKey, oldest, found := c.oldestIdleLocked()
		if !found {
			c.trackCloseLocked(evicted)
			return nil, nil, nil, evicted, ErrCacheSaturated
		}
		delete(c.entries, oldestKey)
		oldest.retiring = true
		evicted = append(evicted, oldest.processor)
	}
	c.trackCloseLocked(evicted)
	c.entries[key] = &cacheEntry{ready: make(chan struct{}), lastUsed: c.clock().UTC()}
	return nil, nil, nil, evicted, nil
}

func (c *Cache) publishBuild(key CacheKey, processor Processor, buildErr error) (Processor, func(), error) {
	c.mu.Lock()
	entry, ok := c.entries[key]
	if !ok {
		if !isNilProcessor(processor) {
			c.trackCloseLocked([]Processor{processor})
		}
		c.mu.Unlock()
		if !isNilProcessor(processor) {
			_ = c.closeProcessorsTracked([]Processor{processor})
		}
		if buildErr != nil {
			return nil, nil, buildErr
		}
		return nil, nil, ErrCacheClosed
	}
	if buildErr == nil && isNilProcessor(processor) {
		buildErr = fmt.Errorf("worker cache factory returned a nil processor")
	}
	entry.processor, entry.buildErr = processor, buildErr
	if buildErr == nil {
		entry.refs = 1
		entry.lastUsed = c.clock().UTC()
	} else {
		delete(c.entries, key)
	}
	close(entry.ready)
	c.signalLocked()
	closed := c.closed
	if closed && buildErr == nil {
		delete(c.entries, key)
		entry.retiring = true
		c.trackCloseLocked([]Processor{processor})
	}
	if buildErr != nil && !isNilProcessor(processor) {
		c.trackCloseLocked([]Processor{processor})
	}
	c.mu.Unlock()
	if closed && buildErr == nil {
		_ = c.closeProcessorsTracked([]Processor{processor})
		return nil, nil, ErrCacheClosed
	}
	if buildErr != nil {
		if !isNilProcessor(processor) {
			closeErr := c.closeProcessorsTracked([]Processor{processor})
			return nil, nil, errors.Join(buildErr, closeErr)
		}
		return nil, nil, buildErr
	}
	return processor, c.releaseFunc(key, entry), nil
}

func (c *Cache) releaseFunc(key CacheKey, entry *cacheEntry) func() {
	var once sync.Once
	return func() {
		once.Do(func() { c.release(key, entry) })
	}
}

func (c *Cache) release(key CacheKey, entry *cacheEntry) {
	var closeTarget Processor
	c.mu.Lock()
	current, ok := c.entries[key]
	if ok && current == entry && entry.refs > 0 {
		entry.refs--
		entry.lastUsed = c.clock().UTC()
		if entry.refs == 0 && (entry.retiring || c.closed) {
			delete(c.entries, key)
			closeTarget = entry.processor
			c.trackCloseLocked([]Processor{closeTarget})
		}
	}
	c.signalLocked()
	c.mu.Unlock()
	if closeTarget != nil {
		_ = c.closeProcessorsTracked([]Processor{closeTarget})
	}
}

// Sweep closes idle entries older than IdleTTL. Active entries are untouched.
func (c *Cache) Sweep() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrCacheClosed
	}
	evicted := c.evictExpiredLocked()
	c.trackCloseLocked(evicted)
	c.mu.Unlock()
	return c.closeProcessorsTracked(evicted)
}

// RunJanitor periodically enforces IdleTTL until ctx is cancelled.
func (c *Cache) RunJanitor(ctx context.Context, interval time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if interval <= 0 {
		interval = c.idleTTL / 2
		if interval <= 0 {
			interval = time.Minute
		}
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := c.Sweep(); err != nil && !errors.Is(err, ErrCacheClosed) {
				return err
			}
		}
	}
}

// Close rejects new acquisitions, closes idle entries immediately and waits
// for active references to release. Context expiry lets in-flight cleanup
// finish and leaves active processors to their eventual release function.
func (c *Cache) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.mu.Lock()
	if !c.closed {
		c.closed = true
	}
	var idle []Processor
	for key, entry := range c.entries {
		select {
		case <-entry.ready:
		default:
			entry.retiring = true
			continue
		}
		entry.retiring = true
		if entry.refs == 0 {
			delete(c.entries, key)
			idle = append(idle, entry.processor)
		}
	}
	c.signalLocked()
	c.trackCloseLocked(idle)
	c.mu.Unlock()
	if len(idle) > 0 {
		// Provider cleanup has no context; keep the caller's deadline effective.
		go func() { _ = c.closeProcessorsTracked(idle) }()
	}

	for {
		c.mu.Lock()
		remaining := len(c.entries)
		closing := c.closeInFlight
		errs := errors.Join(c.closeErrs...)
		c.mu.Unlock()
		if remaining == 0 && closing == 0 {
			return errs
		}
		select {
		case <-ctx.Done():
			return errors.Join(errs, ctx.Err())
		case <-c.notify:
		}
	}
}

func (c *Cache) evictExpiredLocked() []Processor {
	cutoff := c.clock().UTC().Add(-c.idleTTL)
	var evicted []Processor
	for key, entry := range c.entries {
		select {
		case <-entry.ready:
		default:
			continue
		}
		if entry.refs == 0 && !entry.lastUsed.After(cutoff) {
			delete(c.entries, key)
			entry.retiring = true
			evicted = append(evicted, entry.processor)
		}
	}
	return evicted
}

func (c *Cache) oldestIdleLocked() (CacheKey, *cacheEntry, bool) {
	var selectedKey CacheKey
	var selected *cacheEntry
	for key, entry := range c.entries {
		select {
		case <-entry.ready:
		default:
			continue
		}
		if entry.refs != 0 || isNilProcessor(entry.processor) {
			continue
		}
		if selected == nil || entry.lastUsed.Before(selected.lastUsed) {
			selectedKey, selected = key, entry
		}
	}
	return selectedKey, selected, selected != nil
}

func (c *Cache) signalLocked() {
	select {
	case c.notify <- struct{}{}:
	default:
	}
}

func (c *Cache) trackCloseLocked(processors []Processor) {
	count := 0
	for _, processor := range processors {
		if !isNilProcessor(processor) {
			count++
		}
	}
	if count > 0 {
		c.closeInFlight += count
	}
}

func (c *Cache) finishClose(count int, err error) {
	if count <= 0 {
		return
	}
	c.mu.Lock()
	// Publish the sanitized error before a waiter can observe all closes done.
	if err != nil {
		c.closeErrs = append(c.closeErrs, err)
	}
	if count > c.closeInFlight {
		count = c.closeInFlight
	}
	c.closeInFlight -= count
	c.signalLocked()
	c.mu.Unlock()
}

func (c *Cache) closeProcessorsTracked(processors []Processor) error {
	count := 0
	for _, processor := range processors {
		if !isNilProcessor(processor) {
			count++
		}
	}
	if count == 0 {
		return nil
	}
	err := closeProcessors(processors)
	c.finishClose(count, err)
	return err
}

func closeProcessors(processors []Processor) error {
	var errs []error
	for _, processor := range processors {
		if !isNilProcessor(processor) {
			if err := closeProcessor(processor); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func closeProcessor(processor Processor) (err error) {
	defer func() {
		if recover() != nil {
			err = ErrProcessorClosePanic
		}
	}()
	if isNilProcessor(processor) {
		return nil
	}
	if err := processor.Close(); err != nil {
		return ErrProcessorCloseFailed
	}
	return nil
}

func isNilProcessor(processor Processor) bool {
	if processor == nil {
		return true
	}
	value := reflect.ValueOf(processor)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
