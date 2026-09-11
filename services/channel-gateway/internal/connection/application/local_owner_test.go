package application_test

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

func TestLocalOwnerReturnsOriginalGrantCopyWithoutAcquiring(t *testing.T) {
	opts := options()
	opts.LeaseTTL = time.Second
	// Database clock is deliberately unrelated to the machine clock. The real
	// local lease anchors this valid returned interval at the operation start.
	observed := time.Date(2001, 2, 3, 4, 5, 6, 123456000, time.UTC)
	want := domain.OwnerGrant{AccountID: "account", BotID: "bot", InstanceID: "instance", Epoch: 73, Revision: 1, ObservedAt: observed, LeaseUntil: observed.Add(opts.LeaseTTL)}
	base := &leaseStore{}
	store := &customStore{leaseStore: base}
	store.acquire = func(context.Context, domain.Account, string, time.Duration) (domain.OwnerGrant, error) {
		base.acquires.Add(1)
		return want, nil
	}
	store.renew = func(context.Context, domain.OwnerGrant, time.Duration) (domain.OwnerGrant, error) {
		return want, nil
	}
	src := &source{accounts: []domain.Account{account(1)}}
	fac := &factory{created: make(chan *client, 2)}
	s, err := app.NewSupervisor(store, src, &resolver{}, fac, opts)
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		if got, ok := s.LocalOwner("account"); ok || got != (domain.OwnerGrant{}) {
			t.Fatalf("owner before Run: %+v %v", got, ok)
		}
	}
	if base.acquires.Load() != 0 {
		t.Fatal("read-only eligibility acquired a lease")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := wait(t, done); err != nil {
			t.Error(err)
		}
	})
	c := wait(t, fac.created)
	wait(t, c.started)
	for range 100 {
		got, ok := s.LocalOwner("account")
		if !ok || got != want {
			t.Fatalf("actual grant not returned: got=%+v ok=%v want=%+v", got, ok, want)
		}
		got.AccountID, got.BotID, got.InstanceID = "other", "other", "other"
		got.Epoch, got.Revision = 999, 999
		got.ObservedAt, got.LeaseUntil = time.Time{}, time.Time{}
	}
	if got, ok := s.LocalOwner("missing"); ok || got != (domain.OwnerGrant{}) {
		t.Fatalf("missing account fabricated owner: %+v %v", got, ok)
	}
	if base.acquires.Load() != 1 {
		t.Fatal("eligibility lookup acquired another lease")
	}
}

func localOwnerReady(t *testing.T, s *app.Supervisor, revision int64) domain.OwnerGrant {
	t.Helper()
	var grant domain.OwnerGrant
	eventually(t, func() bool {
		var ok bool
		grant, ok = s.LocalOwner("account")
		return ok && grant.Revision == revision
	})
	return grant
}

func localOwnerAbsent(t *testing.T, s *app.Supervisor) {
	t.Helper()
	if grant, ok := s.LocalOwner("account"); ok || grant != (domain.OwnerGrant{}) {
		t.Fatalf("unavailable account exposed owner: %+v %v", grant, ok)
	}
}

func TestLocalOwnerRejectsUnavailableSourceAndAccount(t *testing.T) {
	for _, reason := range []string{"removed", "disabled", "source-error", "incomplete-source", "same-revision-conflict", "stale-revision"} {
		t.Run(reason, func(t *testing.T) {
			initial := account(2)
			store := &leaseStore{}
			src := &source{accounts: []domain.Account{initial}}
			fac := &factory{created: make(chan *client, 10)}
			s, cancel, done := start(t, store, src, &resolver{}, fac, options())
			t.Cleanup(func() {
				cancel()
				if err := wait(t, done); err != nil {
					t.Error(err)
				}
			})
			c := wait(t, fac.created)
			wait(t, c.started)
			localOwnerReady(t, s, initial.Revision)
			switch reason {
			case "removed":
				src.set(nil, nil)
			case "disabled":
				next := account(3)
				next.Enabled = false
				src.set([]domain.Account{next}, nil)
			case "source-error":
				// A partial result accompanying an error is not a complete source.
				src.set([]domain.Account{initial}, errors.New("synthetic source unavailable"))
			case "incomplete-source":
				src.set([]domain.Account{initial, initial}, nil)
			case "same-revision-conflict":
				next := initial
				next.CredentialRef = "ENV:DIFFERENT"
				src.set([]domain.Account{next}, nil)
			case "stale-revision":
				src.set([]domain.Account{account(1)}, nil)
			}
			wait(t, c.stopped)
			localOwnerAbsent(t, s)
			if reason == "source-error" || reason == "incomplete-source" {
				if s.Ready() {
					t.Fatal("unhealthy source retained supervisor readiness")
				}
			}
		})
	}
}

func TestLocalOwnerUsesLiveClientFlagsRatherThanCachedStatus(t *testing.T) {
	for _, flag := range []string{"not-ready", "terminal", "replaced"} {
		t.Run(flag, func(t *testing.T) {
			src := &source{accounts: []domain.Account{account(1)}}
			fac := &factory{created: make(chan *client, 10)}
			s, cancel, done := start(t, &leaseStore{}, src, &resolver{}, fac, options())
			t.Cleanup(func() {
				cancel()
				if err := wait(t, done); err != nil {
					t.Error(err)
				}
			})
			c := wait(t, fac.created)
			wait(t, c.started)
			localOwnerReady(t, s, 1)
			switch flag {
			case "not-ready":
				c.ready.Store(false)
			case "terminal":
				c.terminal.Store(true)
			case "replaced":
				c.replaced.Store(true)
			}
			// No waiting for the Supervisor's periodic Status refresh is allowed.
			localOwnerAbsent(t, s)
			if flag == "not-ready" {
				c.ready.Store(true)
				localOwnerReady(t, s, 1)
			}
		})
	}
}

func TestLocalOwnerRejectsLostLeaseWithoutAcquiringReplacement(t *testing.T) {
	store := &leaseStore{}
	src := &source{accounts: []domain.Account{account(1)}}
	fac := &factory{created: make(chan *client, 10)}
	s, cancel, done := start(t, store, src, &resolver{}, fac, options())
	t.Cleanup(func() {
		cancel()
		if err := wait(t, done); err != nil {
			t.Error(err)
		}
	})
	c := wait(t, fac.created)
	wait(t, c.started)
	localOwnerReady(t, s, 1)
	store.lost.Store(true)
	wait(t, c.stopped)
	for range 100 {
		localOwnerAbsent(t, s)
	}
	if !c.quiesced.Load() {
		t.Fatal("lost lease did not quiesce the Client")
	}
}

func TestLocalOwnerExpiresWhileRenewIsStillBlocked(t *testing.T) {
	base := &leaseStore{}
	store := &customStore{leaseStore: base}
	renewStarted := make(chan struct{})
	unblock := make(chan struct{})
	var first sync.Once
	store.renew = func(ctx context.Context, _ domain.OwnerGrant, _ time.Duration) (domain.OwnerGrant, error) {
		first.Do(func() { close(renewStarted) })
		// Deliberately violate the port deadline to exercise the independent local
		// expiry timer. Cleanup below unblocks the synthetic stalled dependency.
		<-unblock
		return domain.OwnerGrant{}, ctx.Err()
	}
	store.acquire = func(ctx context.Context, a domain.Account, id string, ttl time.Duration) (domain.OwnerGrant, error) {
		if base.acquires.Load() > 0 {
			return domain.OwnerGrant{}, domain.ErrHeld
		}
		g, err := base.ApplyAndAcquire(ctx, a, id, ttl)
		// A fast DB clock must not extend the conservative local expiry.
		g.ObservedAt = time.Date(2090, 1, 1, 0, 0, 0, 0, time.UTC)
		g.LeaseUntil = g.ObservedAt.Add(ttl)
		return g, err
	}
	opts := options()
	opts.LeaseTTL = 120 * time.Millisecond
	src := &source{accounts: []domain.Account{account(1)}}
	fac := &factory{created: make(chan *client, 10)}
	s, cancel, done := start(t, store, src, &resolver{}, fac, opts)
	var release sync.Once
	t.Cleanup(func() {
		release.Do(func() { close(unblock) })
		cancel()
		if err := wait(t, done); err != nil {
			t.Error(err)
		}
	})
	c := wait(t, fac.created)
	wait(t, c.started)
	localOwnerReady(t, s, 1)
	wait(t, renewStarted)
	wait(t, c.stopped)
	localOwnerAbsent(t, s)
	release.Do(func() { close(unblock) })
}

func TestLocalOwnerExcludesNormalDrainWhileLeaseStillRenews(t *testing.T) {
	store := &leaseStore{}
	src := &source{accounts: []domain.Account{account(1)}}
	created := make(chan *client, 1)
	draining, releaseDrain := make(chan struct{}), make(chan struct{})
	fac := factoryFunc(func(context.Context, domain.Account, domain.OwnerGrant, app.CredentialMaterial) (app.Client, error) {
		c := newClient()
		c.drain = func(ctx context.Context) error {
			close(draining)
			select {
			case <-releaseDrain:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		created <- c
		return c, nil
	})
	opts := options()
	opts.DrainTimeout = time.Second
	s, cancel, done := start(t, store, src, &resolver{}, fac, opts)
	var release sync.Once
	t.Cleanup(func() {
		release.Do(func() { close(releaseDrain) })
		cancel()
		if err := wait(t, done); err != nil {
			t.Error(err)
		}
	})
	c := wait(t, created)
	wait(t, c.started)
	localOwnerReady(t, s, 1)
	before := store.renews.Load()
	cancel()
	localOwnerAbsent(t, s)
	wait(t, draining)
	eventually(t, func() bool { return store.renews.Load() > before })
	if !c.Status().Ready || c.Status().Terminal {
		t.Fatal("test did not retain healthy Client through normal drain")
	}
	localOwnerAbsent(t, s)
	release.Do(func() { close(releaseDrain) })
}

func TestLocalOwnerRotationExcludesOldAndUnstartedClient(t *testing.T) {
	store := &leaseStore{}
	src := &source{accounts: []domain.Account{account(1)}}
	created := make(chan *client, 4)
	constructing, releaseFactory := make(chan struct{}), make(chan struct{})
	fac := factoryFunc(func(ctx context.Context, a domain.Account, _ domain.OwnerGrant, _ app.CredentialMaterial) (app.Client, error) {
		if a.Revision == 2 {
			close(constructing)
			select {
			case <-releaseFactory:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		c := newClient()
		created <- c
		return c, nil
	})
	opts := options()
	opts.OperationTimeout = 80 * time.Millisecond
	s, cancel, done := start(t, store, src, &resolver{}, fac, opts)
	var release sync.Once
	t.Cleanup(func() {
		release.Do(func() { close(releaseFactory) })
		cancel()
		if err := wait(t, done); err != nil {
			t.Error(err)
		}
	})
	old := wait(t, created)
	wait(t, old.started)
	first := localOwnerReady(t, s, 1)
	next := account(2)
	next.CredentialRef = "ENV:ROTATED"
	src.set([]domain.Account{next}, nil)
	wait(t, constructing)
	localOwnerAbsent(t, s)
	release.Do(func() { close(releaseFactory) })
	current := wait(t, created)
	wait(t, current.started)
	second := localOwnerReady(t, s, 2)
	if second.Epoch <= first.Epoch || second.Revision != 2 || !old.quiesced.Load() || old.closed.Load() == 0 {
		t.Fatalf("rotation returned stale grant: first=%+v second=%+v", first, second)
	}
}

func TestLocalOwnerConcurrentReadsDuringRenewalAndConfigurationChanges(t *testing.T) {
	src := &source{accounts: []domain.Account{account(1)}}
	store := &leaseStore{}
	fac := &factory{created: make(chan *client, 20)}
	s, cancel, done := start(t, store, src, &resolver{}, fac, options())
	t.Cleanup(func() {
		cancel()
		if err := wait(t, done); err != nil {
			t.Error(err)
		}
	})
	localOwnerReady(t, s, 1)
	stop := make(chan struct{})
	var readers sync.WaitGroup
	var successful atomic.Int64
	for range 8 {
		readers.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				g, ok := s.LocalOwner("account")
				if ok {
					successful.Add(1)
					if g.Validate() != nil || g.AccountID != "account" || g.BotID != "bot" || g.InstanceID != "instance" || !g.LeaseUntil.After(g.ObservedAt) {
						t.Errorf("incoherent concurrent grant: %+v", g)
						return
					}
					g.InstanceID, g.Epoch = "changed-copy", -1
				} else if g != (domain.OwnerGrant{}) {
					t.Errorf("failed lookup returned authority: %+v", g)
					return
				}
				runtime.Gosched()
			}
		})
	}
	defer func() { close(stop); readers.Wait() }()
	eventually(t, func() bool { return store.renews.Load() > 0 })
	for revision := int64(2); revision <= 6; revision++ {
		src.set([]domain.Account{account(revision)}, nil)
		localOwnerReady(t, s, revision)
	}
	cancel()
	for range 100 {
		localOwnerAbsent(t, s)
	}
	if successful.Load() == 0 {
		t.Fatal("concurrent readers never observed an eligible owner")
	}
}

func TestLocalOwnerReturnsLatestRenewedGrant(t *testing.T) {
	base := &leaseStore{}
	store := &customStore{leaseStore: base}
	clock := time.Date(2080, 3, 4, 5, 6, 7, 765432000, time.UTC)
	store.renew = func(_ context.Context, g domain.OwnerGrant, ttl time.Duration) (domain.OwnerGrant, error) {
		g.ObservedAt, g.LeaseUntil = clock, clock.Add(ttl)
		return g, nil
	}
	src := &source{accounts: []domain.Account{account(1)}}
	fac := &factory{created: make(chan *client, 2)}
	s, cancel, done := start(t, store, src, &resolver{}, fac, options())
	t.Cleanup(func() {
		cancel()
		if err := wait(t, done); err != nil {
			t.Error(err)
		}
	})
	c := wait(t, fac.created)
	wait(t, c.started)
	first := localOwnerReady(t, s, 1)
	eventually(t, func() bool {
		current, ok := s.LocalOwner("account")
		return ok && current.ObservedAt == clock
	})
	current, ok := s.LocalOwner("account")
	if !ok || current.AccountID != first.AccountID || current.BotID != first.BotID || current.InstanceID != first.InstanceID || current.Epoch != first.Epoch || current.Revision != first.Revision || current.ObservedAt != clock || current.LeaseUntil != clock.Add(options().LeaseTTL) {
		t.Fatalf("renewed authority not copied intact: first=%+v current=%+v ok=%v", first, current, ok)
	}
}
