package application

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ReserveFinal locates exactly the acquired local lease/client, never another
// owner's socket. MarkCalling's database guard remains the durable send gate.
func (s *Supervisor) ReserveFinal(ctx context.Context, o SenderOrigin) (ReservedSender, error) {
	if ctx == nil || o.Validate() != nil {
		return nil, ErrInvalidSender
	}
	if ctx.Err() != nil {
		return nil, ErrSenderUnavailable
	}
	client, lease, err := s.senderClient(o, nil)
	if err != nil {
		return nil, err
	}
	capable, ok := client.(FinalClient)
	if !ok {
		return nil, ErrSenderUnavailable
	}
	reserved, err := capable.ReserveFinal(ctx, ReplyTarget{RequestID: o.RequestID, SocketGeneration: o.SocketGeneration})
	if err != nil {
		return nil, err
	}
	if reserved == nil {
		return nil, ErrSenderUnavailable
	}
	if _, _, err = s.senderClient(o, lease); err != nil {
		reserved.Release()
		return nil, err
	}
	return &guardedSender{supervisor: s, lease: lease, origin: o, inner: reserved}, nil
}
func (s *Supervisor) senderClient(o SenderOrigin, expected *localLease) (Client, *localLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.records[o.AccountID]
	if r == nil || r.lease == nil || (expected != nil && r.lease != expected) {
		return nil, nil, ErrStaleOrigin
	}
	l := r.lease
	l.mu.Lock()
	defer l.mu.Unlock()
	g := l.grant
	if g.AccountID != o.AccountID || g.InstanceID != o.InstanceID || g.Epoch != o.Epoch || g.Revision != o.Revision || r.account.Revision != o.Revision {
		return nil, nil, ErrStaleOrigin
	}
	if l.expired || l.stopped || !time.Now().Before(l.deadline) {
		return nil, nil, ErrStaleOrigin
	}
	if !s.running || s.runContext == nil || s.runContext.Err() != nil || !s.healthy || !r.present || !r.account.Enabled || r.sourceReason != "" || r.blockedRevision >= r.account.Revision || r.status.Phase == PhaseDraining {
		return nil, nil, ErrSenderUnavailable
	}
	if l.client == nil {
		return nil, nil, ErrSenderUnavailable
	}
	status := l.client.Status()
	if !status.Ready || status.Terminal || status.Replaced {
		return nil, nil, ErrSenderUnavailable
	}
	return l.client, l, nil
}

type guardedSender struct {
	supervisor *Supervisor
	lease      *localLease
	origin     SenderOrigin
	inner      ReservedSender
	used       atomic.Bool
	released   atomic.Bool
	release    sync.Once
}

func (r *guardedSender) SendFinal(ctx context.Context, c FinalCommand) SendResult {
	if r.released.Load() {
		return SendResult{Certainty: NotSent, Code: SendReleased}
	}
	if !r.used.CompareAndSwap(false, true) {
		return SendResult{Certainty: NotSent, Code: SendUsed}
	}
	if ctx == nil {
		return SendResult{Certainty: NotSent, Code: SendInvalid}
	}
	if ctx.Err() != nil {
		return SendResult{Certainty: NotSent, Code: SendCanceled}
	}
	if _, _, err := r.supervisor.senderClient(r.origin, r.lease); err != nil {
		code := SendUnavailable
		if errors.Is(err, ErrStaleOrigin) {
			code = SendStale
		}
		return SendResult{Certainty: NotSent, Code: code}
	}
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.lease.ctx, cancel)
	defer func() { stop(); cancel() }()
	// A local loss can occur after lookup. It cancels this call and the SDK Run;
	// after entering Write only the protocol adapter determines result certainty.
	if r.lease.ctx.Err() != nil {
		cancel()
	}
	return r.inner.SendFinal(call, c)
}
func (r *guardedSender) Release() { r.release.Do(func() { r.released.Store(true); r.inner.Release() }) }
