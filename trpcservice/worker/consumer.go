package worker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/broker"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	sessionstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/session"
)

type LeaseExecutor interface {
	ExecuteWithLease(context.Context, runtime.ExecutionEnvelope, uint64, func(context.Context) error) error
}

type CancellationExecutor interface {
	CancelWithLease(context.Context, runtime.ExecutionEnvelope, uint64, func(context.Context) error) error
}

type Consumer struct {
	WorkerID           string
	Shards             []broker.Shard
	Broker             broker.Broker
	Leases             coordination.LeaseManager
	Sessions           sessionstore.AtomicSessionStore
	Parker             gateway.InputParker
	Statuses           gateway.ExecutionReader
	Executor           LeaseExecutor
	LeaseTTL           time.Duration
	RenewInterval      time.Duration
	RetryWait          time.Duration
	ReclaimInterval    time.Duration
	ReclaimLimit       int
	CancelPollInterval time.Duration
	CancelHints        CancelHintSource
	Drain              <-chan struct{}
	DrainTimeout       time.Duration
	Lifecycle          *Lifecycle
	OnDeliveryError    func(context.Context, broker.Delivery, error)
}

func (w Consumer) Run(ctx context.Context) error {
	if w.WorkerID == "" || len(w.Shards) == 0 || w.Broker == nil || w.Leases == nil || w.Sessions == nil || w.Statuses == nil || w.Executor == nil {
		return runtime.ErrInvariantViolation
	}
	if _, ok := w.Executor.(CancellationExecutor); !ok {
		return runtime.ErrCapabilityUnsupported
	}
	if w.LeaseTTL <= 0 {
		w.LeaseTTL = 5 * time.Second
	}
	if w.RetryWait <= 0 {
		w.RetryWait = 10 * time.Millisecond
	}
	if w.RenewInterval <= 0 {
		w.RenewInterval = w.LeaseTTL / 3
	}
	if w.ReclaimInterval <= 0 {
		w.ReclaimInterval = time.Second
	}
	if w.ReclaimLimit <= 0 {
		w.ReclaimLimit = 100
	}
	if w.DrainTimeout <= 0 {
		w.DrainTimeout = 30 * time.Second
	}
	drainSignal := w.Drain
	if w.Lifecycle != nil {
		if drainSignal != nil {
			return runtime.ErrInvariantViolation
		}
		if err := w.Lifecycle.MarkReady(); err != nil {
			return err
		}
		defer w.Lifecycle.MarkStopped()
		drainSignal = w.Lifecycle.Drain()
	}
	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
	consumeCtx, cancelConsume := context.WithCancel(workCtx)
	defer cancelConsume()
	var draining atomic.Bool
	runFinished := make(chan struct{})
	drainWatchDone := make(chan struct{})
	go func() {
		defer close(drainWatchDone)
		if drainSignal == nil {
			<-runFinished
			return
		}
		select {
		case <-runFinished:
			return
		case <-workCtx.Done():
			return
		case <-drainSignal:
			draining.Store(true)
			cancelConsume()
		}
		timer := time.NewTimer(w.DrainTimeout)
		defer timer.Stop()
		select {
		case <-runFinished:
		case <-workCtx.Done():
		case <-timer.C:
			cancelWork()
		}
	}()
	process := func(deliveryCtx context.Context, delivery broker.Delivery) error {
		for {
			err := w.handle(deliveryCtx, delivery)
			if !errors.Is(err, runtime.ErrCommitConflict) && !errors.Is(err, runtime.ErrVersionConflict) {
				return err
			}
			if err := wait(deliveryCtx, w.RetryWait); err != nil {
				return err
			}
		}
	}
	reclaimDone := make(chan struct{})
	go func() {
		defer close(reclaimDone)
		ticker := time.NewTicker(w.ReclaimInterval)
		defer ticker.Stop()
		for {
			select {
			case <-consumeCtx.Done():
				return
			case <-ticker.C:
				deliveries, err := w.Broker.Reclaim(consumeCtx, broker.ReclaimOptions{ConsumerID: w.WorkerID, Limit: w.ReclaimLimit})
				if err != nil {
					w.report(consumeCtx, broker.Delivery{}, err)
					continue
				}
				for _, delivery := range deliveries {
					if consumeCtx.Err() != nil || draining.Load() {
						return
					}
					if err := process(workCtx, delivery); err != nil && workCtx.Err() == nil {
						w.report(workCtx, delivery, err)
					}
				}
			}
		}
	}()
	err := w.Broker.Consume(consumeCtx, broker.ConsumerOptions{ConsumerID: w.WorkerID, Shards: w.Shards}, func(_ context.Context, delivery broker.Delivery) error {
		if consumeCtx.Err() != nil || draining.Load() {
			return context.Canceled
		}
		if err := process(workCtx, delivery); err != nil {
			if workCtx.Err() != nil {
				return workCtx.Err()
			}
			w.report(workCtx, delivery, err)
		}
		// Execution failures deliberately leave the delivery pending. Returning
		// nil keeps this Worker alive so an idle delivery can be reclaimed.
		return nil
	})
	cancelConsume()
	<-reclaimDone
	close(runFinished)
	<-drainWatchDone
	if draining.Load() && ctx.Err() == nil {
		return nil
	}
	return err
}

func (w Consumer) report(ctx context.Context, delivery broker.Delivery, err error) {
	if err != nil && w.OnDeliveryError != nil {
		w.OnDeliveryError(ctx, delivery, err)
	}
}

func (w Consumer) handle(ctx context.Context, delivery broker.Delivery) error {
	key := coordination.SessionKey{
		TenantID: delivery.Envelope.TenantID, AgentAppID: delivery.Envelope.AgentAppID, SessionID: delivery.Envelope.SessionID,
	}
	persistedFence, err := w.Sessions.ReadLastFence(ctx, sessionstore.SessionKey(key))
	if err != nil {
		return err
	}
	if err := w.Leases.EnsureFenceAtLeast(ctx, key, persistedFence); err != nil {
		return err
	}
	lease, err := w.acquire(ctx, key)
	if err != nil {
		return err
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = w.Leases.Release(releaseCtx, lease)
	}()
	executionCtx, cancelExecution := context.WithCancel(ctx)
	defer cancelExecution()
	var cancelRequested atomic.Bool
	watchDone := make(chan error, 1)
	watchCtx, stopWatch := context.WithCancel(executionCtx)
	go w.watchCancellation(watchCtx, delivery.Envelope, &cancelRequested, cancelExecution, watchDone)
	stopRenewal := make(chan struct{})
	renewalDone := make(chan struct{})
	leaseLost := make(chan struct{})
	var leaseLostOnce sync.Once
	markLeaseLost := func() {
		leaseLostOnce.Do(func() {
			close(leaseLost)
			cancelExecution()
		})
	}
	go func() {
		defer close(renewalDone)
		ticker := time.NewTicker(w.RenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopRenewal:
				return
			case <-executionCtx.Done():
				return
			case <-ticker.C:
				if _, renewErr := w.Leases.Renew(executionCtx, lease, w.LeaseTTL); renewErr != nil {
					markLeaseLost()
					return
				}
			}
		}
	}()
	beforeCommit := func(commitCtx context.Context) error {
		select {
		case <-leaseLost:
			return runtime.ErrLeaseLost
		default:
		}
		if _, renewErr := w.Leases.Renew(commitCtx, lease, w.LeaseTTL); renewErr != nil {
			markLeaseLost()
			return runtime.ErrLeaseLost
		}
		return nil
	}
	var executeErr error
	for {
		executeErr = w.Executor.ExecuteWithLease(executionCtx, delivery.Envelope, lease.Fence, beforeCommit)
		if !errors.Is(executeErr, runtime.ErrInputNotReady) {
			break
		}
		if w.Parker == nil {
			executeErr = runtime.ErrCapabilityUnsupported
			break
		}
		parked, parkErr := w.Parker.ParkInput(executionCtx, gateway.ParkRequest{
			TenantID: delivery.Envelope.TenantID, RequestID: delivery.Envelope.RequestID,
			InputSeq: delivery.Envelope.InputSeq,
		})
		if parkErr != nil {
			executeErr = parkErr
			break
		}
		switch parked.Disposition {
		case gateway.ParkInputReady:
			continue
		case gateway.ParkedInput, gateway.ParkInputTerminal:
			executeErr = nil
		case gateway.ParkInputBlocked:
			w.report(executionCtx, delivery, runtime.ErrInputBlocked)
			executeErr = nil
		default:
			executeErr = runtime.ErrInvariantViolation
		}
		break
	}
	stopWatch()
	watchErr := <-watchDone
	if watchErr != nil && !errors.Is(watchErr, context.Canceled) {
		executeErr = watchErr
	}
	if cancelRequested.Load() || errors.Is(executeErr, runtime.ErrCancelRequested) {
		canceller, ok := w.Executor.(CancellationExecutor)
		if !ok {
			executeErr = runtime.ErrCapabilityUnsupported
		} else {
			commitCtx, cancelCommit := context.WithTimeout(context.WithoutCancel(ctx), w.LeaseTTL)
			executeErr = canceller.CancelWithLease(commitCtx, delivery.Envelope, lease.Fence, beforeCommit)
			cancelCommit()
		}
	}
	close(stopRenewal)
	<-renewalDone
	select {
	case <-leaseLost:
		return runtime.ErrLeaseLost
	default:
	}
	if executeErr != nil {
		return executeErr
	}
	return w.Broker.Ack(ctx, delivery)
}

func (w Consumer) watchCancellation(ctx context.Context, envelope runtime.ExecutionEnvelope, requested *atomic.Bool, cancel context.CancelFunc, done chan<- error) {
	interval := w.CancelPollInterval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	check := func() (bool, error) {
		status, err := w.Statuses.GetExecution(ctx, gateway.ExecutionKey{TenantID: envelope.TenantID, RequestID: envelope.RequestID})
		if err != nil {
			return false, err
		}
		if status.Envelope.TenantID != envelope.TenantID || status.Envelope.RequestID != envelope.RequestID {
			return false, runtime.ErrTenantScope
		}
		return status.CancelRequested, nil
	}
	var hints <-chan gateway.ExecutionKey
	if w.CancelHints != nil {
		hints = w.CancelHints.SubscribeCancellation(ctx)
	}
	for {
		cancelled, err := check()
		if err != nil {
			done <- err
			cancel()
			return
		}
		if cancelled {
			requested.Store(true)
			done <- nil
			cancel()
			return
		}
		timer := time.NewTimer(interval)
		select {
		case key, ok := <-hints:
			if !ok {
				hints = nil
				continue
			}
			if key.TenantID == envelope.TenantID && key.RequestID == envelope.RequestID {
				requested.Store(true)
				done <- nil
				cancel()
				return
			}
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			done <- nil
			return
		case <-timer.C:
		}
	}
}

func (w Consumer) acquire(ctx context.Context, key coordination.SessionKey) (coordination.Lease, error) {
	for {
		lease, err := w.Leases.Acquire(ctx, key, w.WorkerID, w.LeaseTTL)
		if err == nil {
			return lease, nil
		}
		if !errors.Is(err, runtime.ErrVersionConflict) {
			return coordination.Lease{}, err
		}
		if err := wait(ctx, w.RetryWait); err != nil {
			return coordination.Lease{}, err
		}
	}
}

func wait(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
