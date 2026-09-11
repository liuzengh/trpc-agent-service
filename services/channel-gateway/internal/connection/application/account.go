package application

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

type ownedClient struct {
	lease  *localLease
	client Client
	cancel context.CancelFunc
	done   chan struct{}
}

func (r *accountRecord) run(ctx context.Context) {
	s := r.supervisor
	for ctx.Err() == nil {
		a, reason, allowed := r.desired()
		if !allowed {
			phase := PhaseBlocked
			if reason == ReasonMissing {
				phase = PhaseStopped
			}
			r.set(a, domain.OwnerGrant{}, phase, reason, false, false)
			if !r.pause(ctx) {
				return
			}
			continue
		}
		if !r.restartDue(a) {
			if !r.pause(ctx) {
				return
			}
			continue
		}
		r.set(a, domain.OwnerGrant{}, PhaseAcquiring, ReasonNone, false, false)
		op, cancel := context.WithTimeout(ctx, s.options.OperationTimeout)
		started := time.Now()
		grant, err := s.store.ApplyAndAcquire(op, a, s.options.InstanceID, s.options.LeaseTTL)
		cancel()
		if err != nil {
			r.acquireFailure(a, err)
			if !r.pause(ctx) {
				return
			}
			continue
		}
		lease, err := s.newLease(a, grant, started)
		if err != nil {
			r.set(a, grant, PhaseFailed, ReasonLost, false, false)
			if !r.pause(ctx) {
				return
			}
			continue
		}
		s.mu.Lock()
		r.lease = lease
		s.mu.Unlock()
		owned, reason := r.construct(ctx, a, lease)
		if reason != ReasonNone {
			if owned == nil {
				owned = &ownedClient{lease: lease}
			}
			r.finish(a, owned, reason, false)
			if !r.pause(ctx) {
				return
			}
			continue
		}
		reason, normal := r.serve(ctx, a, owned)
		r.finish(a, owned, reason, normal)
		if ctx.Err() != nil {
			return
		}
		if reason == ReasonRetryable {
			r.scheduleRestart(a)
		}
		if !r.pause(ctx) {
			return
		}
	}
}
func (r *accountRecord) acquireFailure(a domain.Account, err error) {
	phase, reason, ready := PhaseFailed, ReasonAcquire, false
	switch {
	case errors.Is(err, domain.ErrHeld):
		phase, reason, ready = PhaseStandby, ReasonHeld, true
	case errors.Is(err, domain.ErrDisabled):
		phase, reason, ready = PhaseDisabled, ReasonDisabled, !a.Enabled
	case errors.Is(err, domain.ErrReplaced):
		phase, reason = PhaseBlocked, ReasonReplaced
		r.block(a.Revision, reason)
	case errors.Is(err, domain.ErrStaleRevision):
		phase, reason = PhaseBlocked, ReasonStale
		r.block(a.Revision, reason)
	case errors.Is(err, domain.ErrConflict), errors.Is(err, domain.ErrInvalid):
		phase, reason = PhaseBlocked, ReasonConflict
		r.block(a.Revision, reason)
	}
	r.set(a, domain.OwnerGrant{}, phase, reason, false, ready)
}
func (r *accountRecord) setupContext(ctx context.Context, l *localLease) (context.Context, context.CancelFunc) {
	op, cancel := context.WithTimeout(l.ctx, r.supervisor.options.OperationTimeout)
	stop := context.AfterFunc(ctx, cancel)
	return op, func() { stop(); cancel() }
}

// credentialContext has its own budget and observes source/config revocation
// without waiting for the account actor to return from credential network I/O.
func (r *accountRecord) credentialContext(ctx context.Context, l *localLease, a domain.Account) (context.Context, context.CancelFunc) {
	op, cancel := context.WithTimeout(l.ctx, r.supervisor.options.CredentialResolveTimeout)
	stop := context.AfterFunc(ctx, cancel)
	s := r.supervisor
	s.mu.Lock()
	r.preparingID++
	id := r.preparingID
	r.preparingAccount = a
	r.preparingCancel = cancel
	if !r.present || r.sourceReason != "" || r.account != a || r.blockedRevision >= a.Revision {
		cancel()
	}
	s.mu.Unlock()
	return op, func() {
		stop()
		cancel()
		s.mu.Lock()
		if r.preparingID == id {
			r.preparingCancel = nil
		}
		s.mu.Unlock()
	}
}

func (r *accountRecord) construct(ctx context.Context, a domain.Account, l *localLease) (*ownedClient, Reason) {
	s := r.supervisor
	g, _ := l.snapshot()
	r.set(a, g, PhaseResolving, ReasonNone, true, false)
	if ctx.Err() != nil || !r.matches(a) {
		return nil, ReasonShutdown
	}
	op, cancel := r.setupContext(ctx, l)
	err := s.store.Check(op, g)
	cancel()
	if err != nil {
		l.lose()
		return nil, ReasonLost
	}
	// Resolver receives an independent budget while lease renewal continues.
	op, cancel = r.credentialContext(ctx, l, a)
	var material CredentialMaterial
	if owned, ok := s.resolver.(OwnedCredentialResolver); ok {
		material, err = owned.ResolveOwned(op, a, g)
	} else {
		material, err = s.resolver.Resolve(op, a)
	}
	resolveContextErr := op.Err()
	cancel()
	if err != nil || resolveContextErr != nil || material.Secret == "" {
		return nil, ReasonCredential
	}
	if ctx.Err() != nil || !r.matches(a) {
		return nil, ReasonShutdown
	}
	op, cancel = r.setupContext(ctx, l)
	err = s.store.Check(op, g)
	cancel()
	if err != nil {
		l.lose()
		return nil, ReasonLost
	}
	op, cancel = r.setupContext(ctx, l)
	client, err := s.factory.New(op, a, g, material)
	material.Secret = ""
	cancel()
	if err != nil || client == nil {
		if client != nil {
			cleanup, stop := context.WithTimeout(context.Background(), s.options.OperationTimeout)
			_ = client.Close(cleanup)
			stop()
		}
		return nil, ReasonFactory
	}
	runCtx, runCancel := context.WithCancel(context.Background())
	owned := &ownedClient{lease: l, client: client, cancel: runCancel, done: make(chan struct{})}
	if ctx.Err() != nil || !r.matches(a) || !l.attach(client, runCancel) {
		runCancel()
		close(owned.done)
		return owned, ReasonLost
	}
	r.set(a, g, PhaseStarting, ReasonNone, true, false)
	go func() { defer close(owned.done); _ = client.Run(runCtx) }()
	return owned, ReasonNone
}
func (r *accountRecord) serve(ctx context.Context, a domain.Account, o *ownedClient) (Reason, bool) {
	ticker := time.NewTicker(r.supervisor.options.PollInterval)
	defer ticker.Stop()
	for {
		status := o.client.Status()
		g, _ := o.lease.snapshot()
		if status.Replaced {
			r.block(a.Revision, ReasonReplaced)
			return ReasonReplaced, false
		}
		select {
		case <-o.lease.lost:
			return ReasonLost, false
		default:
		}
		if status.Terminal {
			return r.terminalReason(a, status), false
		}
		phase := PhaseStarting
		if status.Ready {
			phase = PhaseReady
		}
		r.set(a, g, phase, ReasonNone, true, status.Ready)
		select {
		case <-ctx.Done():
			return ReasonShutdown, true
		case <-o.lease.lost:
			return ReasonLost, false
		case <-o.done:
			if o.client.Status().Replaced {
				r.block(a.Revision, ReasonReplaced)
				return ReasonReplaced, false
			}
			select {
			case <-o.lease.lost:
				return ReasonLost, false
			default:
			}
			return r.terminalReason(a, o.client.Status()), false
		case <-r.wake:
			if !r.matches(a) {
				_, reason, _ := r.desired()
				if reason == "" {
					reason = ReasonConflict
				}
				return reason, false
			}
		case <-ticker.C:
			if !r.matches(a) {
				_, reason, _ := r.desired()
				if reason == "" {
					reason = ReasonConflict
				}
				return reason, false
			}
		}
	}
}

// finish keeps renewal independent of normal shutdown until Client.Close and Run
// have both completed. Loss fences immediately; cleanup always uses fresh ctx.
func (r *accountRecord) finish(a domain.Account, o *ownedClient, reason Reason, normal bool) {
	s := r.supervisor
	g, _ := o.lease.snapshot()
	r.set(a, g, PhaseDraining, reason, true, false)
	defer func() {
		s.mu.Lock()
		if r.lease == o.lease {
			r.lease = nil
		}
		s.mu.Unlock()
	}()
	keeperChecked, keeperStopped, replacementAttempted := false, false, false
	stopKeeper := func() bool {
		if !keeperChecked {
			keeperStopped = o.lease.stop()
			keeperChecked = true
		}
		return keeperStopped
	}
	markReplacement := func() {
		if replacementAttempted {
			return
		}
		r.block(a.Revision, ReasonReplaced)
		// Cancel/quiesce first, then persist the known replacement even when
		// Client.Close will fail. Close success is not a persistence prerequisite.
		stopKeeper()
		g, _ = o.lease.snapshot()
		op, cancel := context.WithTimeout(context.Background(), s.options.OperationTimeout)
		err := s.store.MarkReplaced(op, g)
		cancel()
		r.recordIsolation(a.Revision, err == nil)
		replacementAttempted = true
	}
	if o.client != nil {
		o.client.Quiesce()
		if normal {
			r.set(a, g, PhaseDraining, ReasonShutdown, true, false)
			drainCtx, cancel := context.WithTimeout(context.Background(), s.options.DrainTimeout)
			result := make(chan error, 1)
			go func() { result <- o.client.Drain(drainCtx) }()
			select {
			case err := <-result:
				if err != nil {
					reason = ReasonDrain
				}
			case <-drainCtx.Done():
				reason = ReasonDrain
			case <-o.lease.lost:
				reason = ReasonLost
			case <-o.done:
				if o.client.Status().Replaced {
					reason = ReasonReplaced
					r.block(a.Revision, reason)
				} else {
					reason = ReasonClient
				}
			}
			cancel()
		}
		o.cancel()
		if reason == ReasonReplaced || o.client.Status().Replaced {
			reason = ReasonReplaced
			markReplacement()
		}
		cleanup, cancel := context.WithTimeout(context.Background(), s.options.OperationTimeout)
		closeErr := o.client.Close(cleanup)
		if closeErr == nil {
			select {
			case <-o.done:
			case <-cleanup.Done():
				closeErr = ErrCleanup
			}
		}
		cancel()
		if o.client.Status().Replaced {
			reason = ReasonReplaced
			markReplacement()
		}
		if closeErr != nil {
			stopKeeper()
			r.cleanupFailure()
			r.set(a, g, PhaseFailed, ReasonClose, false, false)
			return
		}
	}
	// No keeper may renew after release/replacement. The latest returned grant is
	// used, while persistence remains responsible for epoch fencing and DB time.
	if !stopKeeper() {
		r.cleanupFailure()
		r.set(a, g, PhaseFailed, ReasonClose, false, false)
		return
	}
	g, _ = o.lease.snapshot()
	if reason == ReasonReplaced {
		markReplacement()
		// On a failed MarkReplaced, do not Release: keep the residual lease until
		// expiry instead of actively accelerating another instance's reconnect.
		r.set(a, g, PhaseBlocked, ReasonReplaced, false, false)
		return
	}
	op, cancel := context.WithTimeout(context.Background(), s.options.OperationTimeout)
	err := s.store.Release(op, g)
	cancel()
	if err != nil && !errors.Is(err, domain.ErrLost) {
		reason = ReasonRelease
	}
	phase := PhaseStopped
	if reason == ReasonLost || reason == ReasonCredential || reason == ReasonFactory || reason == ReasonRelease || reason == ReasonDrain {
		phase = PhaseFailed
	}
	if reason == ReasonClient {
		phase = PhaseBlocked
	}
	r.set(a, g, phase, reason, false, false)
}
