package application_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

type source struct {
	mu       sync.Mutex
	accounts []domain.Account
	err      error
}

func (s *source) List(context.Context) ([]domain.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.Account(nil), s.accounts...), s.err
}
func (s *source) set(accounts []domain.Account, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.accounts = accounts
	s.err = err
}

type leaseStore struct {
	mu         sync.Mutex
	epoch      int64
	lost       atomic.Bool
	acquires   atomic.Int64
	renews     atomic.Int64
	releases   atomic.Int64
	marked     atomic.Int64
	acquireErr error
}

func (s *leaseStore) ApplyAndAcquire(ctx context.Context, a domain.Account, id string, ttl time.Duration) (domain.OwnerGrant, error) {
	s.acquires.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.acquireErr != nil {
		return domain.OwnerGrant{}, s.acquireErr
	}
	if !a.Enabled {
		return domain.OwnerGrant{}, domain.ErrDisabled
	}
	s.epoch++
	now := time.Now()
	return domain.OwnerGrant{AccountID: a.ID, BotID: a.BotID, InstanceID: id, Epoch: s.epoch, Revision: a.Revision, ObservedAt: now, LeaseUntil: now.Add(ttl)}, nil
}
func (s *leaseStore) Renew(ctx context.Context, g domain.OwnerGrant, ttl time.Duration) (domain.OwnerGrant, error) {
	s.renews.Add(1)
	if s.lost.Load() {
		return domain.OwnerGrant{}, domain.ErrLost
	}
	now := time.Now()
	g.ObservedAt = now
	g.LeaseUntil = now.Add(ttl)
	return g, nil
}
func (s *leaseStore) Release(context.Context, domain.OwnerGrant) error { s.releases.Add(1); return nil }
func (s *leaseStore) Check(context.Context, domain.OwnerGrant) error {
	if s.lost.Load() {
		return domain.ErrLost
	}
	return nil
}
func (s *leaseStore) MarkReplaced(context.Context, domain.OwnerGrant) error {
	s.marked.Add(1)
	return nil
}

type resolver struct{ calls atomic.Int64 }

func (r *resolver) Resolve(context.Context, domain.Account) (app.CredentialMaterial, error) {
	r.calls.Add(1)
	return app.CredentialMaterial{Secret: "synthetic-secret"}, nil
}

type client struct {
	ready    atomic.Bool
	replaced atomic.Bool
	terminal atomic.Bool
	quiesced atomic.Bool
	runs     atomic.Int64
	closed   atomic.Int64
	started  chan struct{}
	stopped  chan struct{}
	drain    func(context.Context) error
}

func newClient() *client { return &client{started: make(chan struct{}), stopped: make(chan struct{})} }
func (c *client) Run(ctx context.Context) error {
	c.runs.Add(1)
	c.ready.Store(true)
	close(c.started)
	<-ctx.Done()
	c.terminal.Store(true)
	close(c.stopped)
	return ctx.Err()
}
func (c *client) Quiesce() { c.quiesced.Store(true) }
func (c *client) Drain(ctx context.Context) error {
	if c.drain != nil {
		return c.drain(ctx)
	}
	return nil
}
func (c *client) Close(context.Context) error { c.closed.Add(1); return nil }
func (c *client) Status() app.ClientStatus {
	return app.ClientStatus{Ready: c.ready.Load(), Replaced: c.replaced.Load(), Terminal: c.terminal.Load()}
}

type factory struct {
	mu      sync.Mutex
	clients []*client
	created chan *client
}

func (f *factory) New(ctx context.Context, a domain.Account, g domain.OwnerGrant, m app.CredentialMaterial) (app.Client, error) {
	if m.Secret != "synthetic-secret" {
		return nil, errors.New("invalid synthetic credential")
	}
	c := newClient()
	f.mu.Lock()
	f.clients = append(f.clients, c)
	f.mu.Unlock()
	if f.created != nil {
		f.created <- c
	}
	return c, nil
}
func account(revision int64) domain.Account {
	return domain.Account{ID: "account", BotID: "bot", CredentialRef: "ENV:TEST", Revision: revision, Enabled: true}
}
func options() app.Options {
	return app.Options{InstanceID: "instance", LeaseTTL: 300 * time.Millisecond, PollInterval: 10 * time.Millisecond, OperationTimeout: 20 * time.Millisecond, DrainTimeout: 100 * time.Millisecond, MaxAccounts: 5}
}
func eventually(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
func wait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
		var zero T
		return zero
	}
}
func TestLeaseCredentialClientReadyAndLostCancellation(t *testing.T) {
	store := &leaseStore{}
	src := &source{accounts: []domain.Account{account(1)}}
	res := &resolver{}
	fac := &factory{created: make(chan *client, 10)}
	supervisor, err := app.NewSupervisor(store, src, res, fac, options())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()
	c := wait(t, fac.created)
	wait(t, c.started)
	eventually(t, func() bool { st, _ := supervisor.Status("account"); return st.Ready })
	status, ok := supervisor.Status("account")
	if !ok || !status.Owned || !status.Ready || status.Revision != 1 || status.Epoch != 1 {
		t.Fatal(status)
	}
	if res.calls.Load() != 1 || c.runs.Load() != 1 {
		t.Fatal("lifecycle did not execute exactly once")
	}
	store.lost.Store(true)
	wait(t, c.stopped)
	if !c.quiesced.Load() {
		t.Fatal("loss canceled without quiesce")
	}
	eventually(t, func() bool { return c.closed.Load() > 0 })
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}
