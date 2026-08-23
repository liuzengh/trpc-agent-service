package redis

import (
	"context"
	"errors"
	"sync"
	"time"
)

type LeaseRunner struct {
	Lease  Lease
	Ctx    context.Context
	Cancel context.CancelFunc
	Done   <-chan struct{}
	Err    <-chan error
}

func (b *Backend) RunWithLease(parent context.Context, lease Lease, fn func(context.Context) error) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	interval := b.leaseTTL / 3
	if interval <= 0 {
		interval = time.Second
	}
	errs := make(chan error, 1)
	done := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := b.Renew(ctx, lease, b.leaseTTL); err != nil {
					once.Do(func() { errs <- err; cancel() })
					return
				}
			}
		}
	}()
	runErr := fn(ctx)
	cancel()
	<-done
	var leaseErr error
	select {
	case leaseErr = <-errs:
	default:
	}
	if leaseErr != nil {
		return errors.Join(leaseErr, runErr)
	}
	if runErr != nil {
		return runErr
	}
	return b.Release(context.Background(), lease)
}
