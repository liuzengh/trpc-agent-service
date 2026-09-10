package gateway

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	defaultTenantMaxConcurrent = 8
	defaultTenantMaxRequests   = 60
	defaultTenantWindow        = time.Minute
)

// TenantLimiterConfig defines the in-process admission budget for one tenant.
// It is intentionally small and replaceable; it is not a distributed quota.
type TenantLimiterConfig struct {
	MaxConcurrent int
	MaxRequests   int
	Window        time.Duration
	Now           func() time.Time
}

type tenantLimitEntry struct {
	windowStart time.Time
	requests    int
	active      int
}

// TenantLimiter limits both the number of in-flight executions and the number
// of admissions in one fixed window per tenant.
type TenantLimiter struct {
	mu            sync.Mutex
	maxConcurrent int
	maxRequests   int
	window        time.Duration
	now           func() time.Time
	entries       map[string]*tenantLimitEntry
	closed        bool
}

// TenantLimitLease releases one in-flight tenant slot exactly once.
type TenantLimitLease struct {
	limiter *TenantLimiter
	tenant  string
	once    sync.Once
}

// NewTenantLimiter creates a safe-default in-process limiter.
func NewTenantLimiter(config TenantLimiterConfig) (*TenantLimiter, error) {
	if config.MaxConcurrent == 0 {
		config.MaxConcurrent = defaultTenantMaxConcurrent
	}
	if config.MaxConcurrent < 1 {
		return nil, fmt.Errorf("%w: tenant concurrent limit must be positive", ErrInvalid)
	}
	if config.MaxRequests == 0 {
		config.MaxRequests = defaultTenantMaxRequests
	}
	if config.MaxRequests < 1 {
		return nil, fmt.Errorf("%w: tenant request limit must be positive", ErrInvalid)
	}
	if config.Window == 0 {
		config.Window = defaultTenantWindow
	}
	if config.Window < 0 {
		return nil, fmt.Errorf("%w: tenant limit window cannot be negative", ErrInvalid)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &TenantLimiter{
		maxConcurrent: config.MaxConcurrent,
		maxRequests:   config.MaxRequests,
		window:        config.Window,
		now:           config.Now,
		entries:       make(map[string]*tenantLimitEntry),
	}, nil
}

// Ready reports whether the limiter can admit new requests.
func (limiter *TenantLimiter) Ready() bool {
	if limiter == nil {
		return false
	}
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	return !limiter.closed
}

// Acquire reserves one request slot for tenantID.
func (limiter *TenantLimiter) Acquire(ctx context.Context, tenantID string) (*TenantLimitLease, error) {
	if limiter == nil {
		return nil, ErrNotReady
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := validateScopedID(tenantID, "t_", "tenant"); err != nil {
		return nil, err
	}
	now := limiter.now().UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}

	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	if limiter.closed {
		return nil, ErrClosed
	}
	entry := limiter.entries[tenantID]
	if entry == nil {
		entry = &tenantLimitEntry{windowStart: now}
		limiter.entries[tenantID] = entry
	} else if !now.Before(entry.windowStart.Add(limiter.window)) {
		// Preserve active leases that crossed the fixed-window boundary. Only
		// the admission counter belongs to the expiring window. A timestamp
		// that moves backwards can be observed when callers sample the clock
		// before waiting for this lock, so it must not reset the request budget.
		entry.windowStart = now
		entry.requests = 0
	}
	if entry.requests >= limiter.maxRequests || entry.active >= limiter.maxConcurrent {
		return nil, ErrRateLimited
	}
	entry.requests++
	entry.active++
	return &TenantLimitLease{limiter: limiter, tenant: tenantID}, nil
}

// Release returns the in-flight slot. Releasing twice is harmless.
func (lease *TenantLimitLease) Release() error {
	if lease == nil || lease.limiter == nil {
		return nil
	}
	lease.once.Do(func() {
		lease.limiter.mu.Lock()
		defer lease.limiter.mu.Unlock()
		if entry := lease.limiter.entries[lease.tenant]; entry != nil && entry.active > 0 {
			entry.active--
		}
	})
	return nil
}

// Close stops new admissions and releases the limiter's retained state.
func (limiter *TenantLimiter) Close() error {
	if limiter == nil {
		return nil
	}
	limiter.mu.Lock()
	defer limiter.mu.Unlock()
	limiter.closed = true
	limiter.entries = make(map[string]*tenantLimitEntry)
	return nil
}
