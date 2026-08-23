package storage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// LeaseRenewalRunner renews one already-acquired lease until its context is
// cancelled or the LeaseStore rejects the lease. It never acquires a lease.
type LeaseRenewalRunner struct {
	store    LeaseStore
	tenant   tenant.TenantContext
	lease    Lease
	ttl      time.Duration
	interval time.Duration
	onError  func(error)

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	errCh  chan error

	startOnce sync.Once
	stopOnce  sync.Once
	mu        sync.RWMutex
	started   bool
	latest    Lease
	result    error
}

// NewLeaseRenewalRunner creates a runner for an acquired lease. interval must
// be positive and shorter than ttl; a zero interval uses ttl/3.
func NewLeaseRenewalRunner(parent context.Context, store LeaseStore, tc tenant.TenantContext, lease Lease, ttl, interval time.Duration, onError func(error)) (*LeaseRenewalRunner, error) {
	if parent == nil || store == nil || lease.OwnerID == "" || lease.FenceToken == 0 || lease.Epoch == 0 || ttl <= 0 {
		return nil, fmt.Errorf("%w: invalid lease renewal runner configuration", ErrInvalidArgument)
	}
	if interval == 0 {
		interval = ttl / 3
	}
	if interval <= 0 || interval >= ttl {
		return nil, fmt.Errorf("%w: renewal interval must be shorter than ttl", ErrInvalidArgument)
	}
	ctx, cancel := context.WithCancel(parent)
	return &LeaseRenewalRunner{
		store: store, tenant: tc, lease: lease, latest: lease, ttl: ttl,
		interval: interval, onError: onError, ctx: ctx, cancel: cancel,
		done: make(chan struct{}), errCh: make(chan error, 1),
	}, nil
}

// Start starts the single renewal goroutine. A runner cannot be restarted.
func (r *LeaseRenewalRunner) Start() error {
	var startErr error
	r.startOnce.Do(func() {
		r.mu.Lock()
		r.started = true
		r.mu.Unlock()
		go r.run()
	})
	r.mu.RLock()
	started := r.started
	r.mu.RUnlock()
	if !started {
		startErr = fmt.Errorf("%w: runner start failed", ErrInvalidArgument)
	}
	return startErr
}

// Run starts the runner and waits for its terminal result.
func (r *LeaseRenewalRunner) Run() error {
	if err := r.Start(); err != nil {
		return err
	}
	return r.Wait()
}

// Stop is idempotent and waits until the renewal goroutine has exited.
func (r *LeaseRenewalRunner) Stop() error {
	if err := r.Start(); err != nil {
		return err
	}
	r.stopOnce.Do(r.cancel)
	return r.Wait()
}

// Wait waits for the runner to stop. It is safe to call more than once.
func (r *LeaseRenewalRunner) Wait() error {
	<-r.done
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.result
}

// Done returns a channel closed exactly once when the runner exits.
func (r *LeaseRenewalRunner) Done() <-chan struct{} { return r.done }

// Errors returns a channel receiving at most one renew error. Context
// cancellation is returned by Wait but is not reported as a renew failure.
func (r *LeaseRenewalRunner) Errors() <-chan error { return r.errCh }

// Lease returns the latest lease, including the most recent expiry.
func (r *LeaseRenewalRunner) Lease() Lease {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.latest
}

func (r *LeaseRenewalRunner) run() {
	defer close(r.done)
	defer close(r.errCh)
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			r.setResult(r.ctx.Err())
			return
		case <-ticker.C:
			updated, err := r.store.Renew(r.ctx, r.tenant, r.Lease(), r.ttl)
			if err == nil {
				r.mu.Lock()
				r.latest = updated
				r.mu.Unlock()
				continue
			}
			if r.ctx.Err() != nil {
				r.setResult(r.ctx.Err())
				return
			}
			r.mu.Lock()
			if r.result == nil {
				r.result = err
			}
			r.mu.Unlock()
			r.errCh <- err
			if r.onError != nil {
				r.onError(err)
			}
			r.cancel()
			return
		}
	}
}

func (r *LeaseRenewalRunner) setResult(err error) {
	r.mu.Lock()
	r.result = err
	r.mu.Unlock()
}
