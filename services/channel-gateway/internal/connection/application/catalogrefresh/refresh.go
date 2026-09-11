// Package catalogrefresh owns the trusted account-source lifecycle independently
// of the WeCom connection actor. A snapshot is not applied until the replica has
// persisted it; failure and freshness expiration revoke local account contexts.
package catalogrefresh

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"

	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
	c "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain/accountcatalog"
)

type Source interface {
	Fetch(context.Context) (c.Snapshot, error)
}
type Replica interface {
	BeginPoll(context.Context) (c.Poll, error)
	Apply(context.Context, c.Poll, c.Snapshot) (c.Classification, c.Qualification, error)
	Invalidate(context.Context) error
}
type record struct {
	account c.Account
	ctx     context.Context
	cancel  context.CancelFunc
}
type View struct {
	Account       c.Account
	Poll          c.Poll
	Qualification c.Qualification
	Context       context.Context
}
type Service struct {
	source                   Source
	replica                  Replica
	pollMu                   sync.Mutex
	mu                       sync.Mutex
	records                  map[string]record
	poll                     c.Poll
	qualification            c.Qualification
	deadline                 time.Time
	timer                    *time.Timer
	healthy, closed, started bool
	generation               uint64
}

func New(source Source, replica Replica) (*Service, error) {
	if source == nil || replica == nil {
		return nil, c.ErrInvalid
	}
	return &Service{source: source, replica: replica, records: map[string]record{}}, nil
}

// Refresh is serialized for both manual initialization and Run. SUPERSEDED does
// not renew the watchdog and never publishes a mixed or partially applied view.
func (s *Service) Refresh(ctx context.Context) (c.Classification, error) {
	if ctx == nil {
		return "", c.ErrInvalid
	}
	s.pollMu.Lock()
	defer s.pollMu.Unlock()
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return "", c.ErrUnavailable
	}
	op, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	p, err := s.replica.BeginPoll(op)
	if err != nil {
		return "", s.fail(err)
	}
	snap, err := s.source.Fetch(op)
	if err != nil {
		return "", s.fail(err)
	}
	if err = snap.Validate(); err != nil {
		return "", s.fail(err)
	}
	class, q, err := s.replica.Apply(op, p, snap)
	if err != nil {
		return "", s.fail(err)
	}
	if class == c.Superseded {
		return class, nil
	}
	if class != c.Same && class != c.Advance {
		return "", s.fail(c.ErrIntegrity)
	}
	if op.Err() != nil || !time.Now().Before(p.LocalStarted.Add(30*time.Second)) || q.Generation < 1 || q.Revision != snap.Revision {
		return "", s.fail(c.ErrExpired)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", c.ErrUnavailable
	}
	s.generation++
	generation := s.generation
	if s.timer != nil {
		s.timer.Stop()
	}
	next := make(map[string]record, len(snap.Accounts))
	for _, a := range snap.Accounts {
		old, ok := s.records[a.ID]
		// Metadata/floor-only changes preserve the SDK lifetime context. A new
		// process qualification after revocation cannot revive a canceled context.
		if ok && old.account.ConnectionRevision == a.ConnectionRevision && old.ctx.Err() == nil && s.qualification.Generation == q.Generation {
			old.account = clone(a)
			next[a.ID] = old
		} else {
			if ok {
				old.cancel()
			}
			lifetime, stop := context.WithCancel(context.Background())
			next[a.ID] = record{clone(a), lifetime, stop}
			if !a.Enabled {
				stop()
			}
		}
	}
	for id, r := range s.records {
		if _, ok := next[id]; !ok {
			r.cancel()
		}
	}
	s.records = next
	s.poll = p
	s.qualification = q
	s.deadline = p.LocalStarted.Add(30 * time.Second)
	s.healthy = true
	s.timer = time.AfterFunc(time.Until(s.deadline), func() { s.expire(generation) })
	return class, nil
}
func (s *Service) expire(generation uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if generation != s.generation || time.Now().Before(s.deadline) {
		return
	}
	s.revokeLocked()
}
func (s *Service) revokeLocked() {
	s.healthy = false
	for _, r := range s.records {
		r.cancel()
	}
	if s.timer != nil {
		s.timer.Stop()
	}
}
func (s *Service) fail(err error) error {
	s.mu.Lock()
	s.revokeLocked()
	s.mu.Unlock()
	// Local cancellation happens BEFORE this bounded best-effort durable revoke.
	// DB qualification expiration remains independent of a stuck HTTP request.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.replica.Invalidate(ctx)
	return err
}
func (s *Service) Close() { s.mu.Lock(); s.closed = true; s.revokeLocked(); s.mu.Unlock() }
func (s *Service) Ready() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closed && s.healthy && time.Now().Before(s.deadline)
}
func (s *Service) Lookup(ctx context.Context, id string) (View, error) {
	if ctx == nil || ctx.Err() != nil {
		return View{}, c.ErrUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.healthy || !time.Now().Before(s.deadline) {
		return View{}, c.ErrUnavailable
	}
	r, ok := s.records[id]
	if !ok || !r.account.Enabled || r.ctx.Err() != nil {
		return View{}, c.ErrUnauthorized
	}
	return View{clone(r.account), s.poll, s.qualification, r.ctx}, nil
}

// List is the existing WeCom AccountSource seam. Telegram is never sent into
// the stateful Supervisor. Revision projects connection_revision only.
func (s *Service) List(ctx context.Context) ([]d.Account, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, c.ErrUnavailable
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || !s.healthy || !time.Now().Before(s.deadline) {
		return nil, c.ErrUnavailable
	}
	out := []d.Account{}
	for _, r := range s.records {
		a := r.account
		if a.Provider != "wecom" {
			continue
		}
		out = append(out, d.Account{ID: a.ID, BotID: a.ProviderAccountID, CredentialRef: a.Credentials[0].ID, Revision: a.ConnectionRevision, Enabled: a.Enabled})
	}
	return out, nil
}
func (s *Service) Run(ctx context.Context) error {
	if ctx == nil {
		return c.ErrInvalid
	}
	s.mu.Lock()
	if s.started || s.closed {
		s.mu.Unlock()
		return c.ErrInvalid
	}
	s.started = true
	s.mu.Unlock()
	defer s.Close()
	stop := context.AfterFunc(ctx, s.Close)
	defer stop()
	for ctx.Err() == nil {
		class, _ := s.Refresh(ctx)
		// Avoid an unbounded hot loop during sustained concurrent supersession.
		delay := time.Duration(float64(10*time.Second) * (0.8 + rand.Float64()*0.4))
		if class == c.Superseded {
			delay = 100 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

// Accounts is a bounded copy of the current diagnostic catalog, including
// disabled records; it is not a grant and consumers must still call Lookup.
func (s *Service) Accounts() []c.Account {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]c.Account, 0, len(s.records))
	for _, r := range s.records {
		out = append(out, clone(r.account))
	}
	c.SortAccounts(out)
	return out
}
func clone(a c.Account) c.Account {
	a.Credentials = append([]c.Credential(nil), a.Credentials...)
	return a
}

var _ interface {
	List(context.Context) ([]d.Account, error)
} = (*Service)(nil)
