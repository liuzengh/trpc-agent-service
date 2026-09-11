package application

import (
	"context"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

// localLease keeps database clock values out of wall-clock authorization. The
// returned DB interval is anchored at local call START, never response receipt.
// Its expiry timer fences a Client even while a renew operation is waiting.
type localLease struct {
	supervisor *Supervisor
	account    domain.Account
	mu         sync.Mutex
	grant      domain.OwnerGrant
	deadline   time.Time
	ctx        context.Context
	cancel     context.CancelFunc
	lost       chan struct{}
	done       chan struct{}
	expired    bool
	stopped    bool
	timer      *time.Timer
	client     Client
	cancelRun  context.CancelFunc
}

func (s *Supervisor) newLease(a domain.Account, g domain.OwnerGrant, started time.Time) (*localLease, error) {
	deadline, ok := s.grantDeadline(a, g, started)
	if !ok {
		return nil, domain.ErrLost
	}
	ctx, cancel := context.WithCancel(context.Background())
	l := &localLease{supervisor: s, account: a, grant: g, deadline: deadline, ctx: ctx, cancel: cancel, lost: make(chan struct{}), done: make(chan struct{})}
	l.timer = time.AfterFunc(time.Until(deadline), l.expire)
	go l.renew()
	return l, nil
}
func (s *Supervisor) grantDeadline(a domain.Account, g domain.OwnerGrant, started time.Time) (time.Time, bool) {
	delta := g.LeaseUntil.Sub(g.ObservedAt)
	deadline := started.Add(delta)
	return deadline, g.Validate() == nil && g.AccountID == a.ID && g.BotID == a.BotID && g.InstanceID == s.options.InstanceID && g.Revision == a.Revision && delta > 0 && delta <= s.options.LeaseTTL && deadline.After(time.Now())
}
func (l *localLease) snapshot() (domain.OwnerGrant, time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.grant, l.deadline
}
func (l *localLease) expire() { l.markLost(true) }
func (l *localLease) lose()   { l.markLost(false) }
func (l *localLease) markLost(onlyExpired bool) {
	l.mu.Lock()
	if l.stopped || l.expired || (onlyExpired && time.Now().Before(l.deadline)) {
		l.mu.Unlock()
		return
	}
	l.expired = true
	close(l.lost)
	client, cancel := l.client, l.cancelRun
	l.mu.Unlock()
	if client != nil {
		client.Quiesce()
	}
	if cancel != nil {
		cancel()
	}
	l.cancel()
}
func (l *localLease) attach(client Client, cancel context.CancelFunc) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped || l.expired || !time.Now().Before(l.deadline) {
		return false
	}
	l.client = client
	l.cancelRun = cancel
	return true
}
func (l *localLease) renew() {
	defer close(l.done)
	for {
		_, deadline := l.snapshot()
		delay := min(l.supervisor.options.LeaseTTL/3, time.Until(deadline)/3)
		if delay <= 0 {
			l.lose()
			return
		}
		timer := time.NewTimer(delay)
		select {
		case <-l.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
		previous, deadline := l.snapshot()
		op, cancel := context.WithDeadline(l.ctx, minTime(time.Now().Add(l.supervisor.options.OperationTimeout), deadline))
		started := time.Now()
		grant, err := l.supervisor.store.Renew(op, previous, l.supervisor.options.LeaseTTL)
		cancel()
		next, valid := l.supervisor.grantDeadline(l.account, grant, started)
		if err != nil || !valid || grant.Epoch != previous.Epoch {
			l.lose()
			return
		}
		l.mu.Lock()
		if l.expired || l.stopped {
			l.mu.Unlock()
			return
		}
		l.grant = grant
		l.deadline = next
		l.timer.Reset(time.Until(next))
		l.mu.Unlock()
	}
}
func (l *localLease) stop() bool {
	l.mu.Lock()
	l.stopped = true
	l.timer.Stop()
	l.mu.Unlock()
	l.cancel()
	timer := time.NewTimer(l.supervisor.options.OperationTimeout)
	defer timer.Stop()
	select {
	case <-l.done:
		return true
	case <-timer.C:
		return false
	}
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func (l *localLease) valid() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return !l.expired && !l.stopped && time.Now().Before(l.deadline)
}
