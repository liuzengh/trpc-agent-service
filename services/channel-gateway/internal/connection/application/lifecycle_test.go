package application_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

type resolverFunc func(context.Context, domain.Account) (app.CredentialMaterial, error)

func (f resolverFunc) Resolve(ctx context.Context, a domain.Account) (app.CredentialMaterial, error) {
	return f(ctx, a)
}

type factoryFunc func(context.Context, domain.Account, domain.OwnerGrant, app.CredentialMaterial) (app.Client, error)

func (f factoryFunc) New(ctx context.Context, a domain.Account, g domain.OwnerGrant, m app.CredentialMaterial) (app.Client, error) {
	return f(ctx, a, g, m)
}

type customStore struct {
	*leaseStore
	acquire func(context.Context, domain.Account, string, time.Duration) (domain.OwnerGrant, error)
	renew   func(context.Context, domain.OwnerGrant, time.Duration) (domain.OwnerGrant, error)
	release func(context.Context, domain.OwnerGrant) error
	mark    func(context.Context, domain.OwnerGrant) error
}

func (s *customStore) ApplyAndAcquire(ctx context.Context, a domain.Account, id string, ttl time.Duration) (domain.OwnerGrant, error) {
	if s.acquire != nil {
		return s.acquire(ctx, a, id, ttl)
	}
	return s.leaseStore.ApplyAndAcquire(ctx, a, id, ttl)
}
func (s *customStore) Renew(ctx context.Context, g domain.OwnerGrant, ttl time.Duration) (domain.OwnerGrant, error) {
	if s.renew != nil {
		return s.renew(ctx, g, ttl)
	}
	return s.leaseStore.Renew(ctx, g, ttl)
}
func (s *customStore) Release(ctx context.Context, g domain.OwnerGrant) error {
	if s.release != nil {
		return s.release(ctx, g)
	}
	return s.leaseStore.Release(ctx, g)
}
func (s *customStore) MarkReplaced(ctx context.Context, g domain.OwnerGrant) error {
	if s.mark != nil {
		return s.mark(ctx, g)
	}
	return s.leaseStore.MarkReplaced(ctx, g)
}
func start(t *testing.T, store app.LeaseStore, src app.AccountSource, res app.CredentialResolver, fac app.ClientFactory, opts app.Options) (*app.Supervisor, context.CancelFunc, <-chan error) {
	t.Helper()
	supervisor, err := app.NewSupervisor(store, src, res, fac, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- supervisor.Run(ctx) }()
	t.Cleanup(cancel)
	return supervisor, cancel, done
}
func noEvent[T any](t *testing.T, ch <-chan T, d time.Duration) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("unexpected additional lifecycle event")
	case <-time.After(d):
	}
}

func TestEmptyAndStandbyAreReadyWithoutResolvingCredentials(t *testing.T) {
	for _, empty := range []bool{true, false} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			store := &leaseStore{acquireErr: domain.ErrHeld}
			src := &source{}
			if !empty {
				src.accounts = []domain.Account{account(1)}
			}
			res := &resolver{}
			fac := &factory{}
			supervisor, cancel, done := start(t, store, src, res, fac, options())
			eventually(t, supervisor.Ready)
			if res.calls.Load() != 0 {
				t.Fatal("standby resolved credential")
			}
			cancel()
			if err := wait(t, done); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestNormalShutdownRenewsThroughDrainBeforeCancelAndRelease(t *testing.T) {
	base := &leaseStore{}
	store := &customStore{leaseStore: base}
	src := &source{accounts: []domain.Account{account(1)}}
	draining := make(chan struct{})
	drained := make(chan struct{})
	created := make(chan *client, 1)
	store.release = func(ctx context.Context, g domain.OwnerGrant) error {
		if ctx.Err() != nil {
			t.Error("release inherited canceled Run context")
		}
		select {
		case <-drained:
		default:
			t.Error("lease released before drain")
		}
		base.releases.Add(1)
		return nil
	}
	fac := factoryFunc(func(context.Context, domain.Account, domain.OwnerGrant, app.CredentialMaterial) (app.Client, error) {
		c := newClient()
		c.drain = func(ctx context.Context) error {
			if !c.quiesced.Load() {
				t.Error("drain before quiesce")
			}
			if ctx.Err() != nil {
				t.Error("drain reused canceled context")
			}
			close(draining)
			timer := time.NewTimer(150 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-c.stopped:
				t.Error("Run canceled before drain finished")
				return errors.New("early cancel")
			case <-timer.C:
				close(drained)
				return nil
			}
		}
		created <- c
		return c, nil
	})
	opts := options()
	opts.LeaseTTL = 150 * time.Millisecond
	opts.DrainTimeout = 250 * time.Millisecond
	supervisor, cancel, done := start(t, store, src, &resolver{}, fac, opts)
	c := wait(t, created)
	wait(t, c.started)
	eventually(t, supervisor.Ready)
	before := base.renews.Load()
	cancel()
	wait(t, draining)
	if supervisor.Ready() {
		t.Fatal("shutdown retained readiness")
	}
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
	if base.renews.Load()-before < 2 {
		t.Fatal("lease not renewed throughout drain")
	}
	if base.releases.Load() != 1 {
		t.Fatal(base.releases.Load())
	}
	wait(t, c.stopped)
}
func TestRenewLossInterruptsDrain(t *testing.T) {
	store := &leaseStore{}
	src := &source{accounts: []domain.Account{account(1)}}
	draining := make(chan struct{})
	created := make(chan *client, 1)
	fac := factoryFunc(func(context.Context, domain.Account, domain.OwnerGrant, app.CredentialMaterial) (app.Client, error) {
		c := newClient()
		c.drain = func(ctx context.Context) error { close(draining); <-ctx.Done(); return ctx.Err() }
		created <- c
		return c, nil
	})
	opts := options()
	opts.DrainTimeout = time.Second
	supervisor, cancel, done := start(t, store, src, &resolver{}, fac, opts)
	c := wait(t, created)
	wait(t, c.started)
	eventually(t, supervisor.Ready)
	cancel()
	wait(t, draining)
	store.lost.Store(true)
	begin := time.Now()
	wait(t, c.stopped)
	if time.Since(begin) > 400*time.Millisecond {
		t.Fatal("lost lease waited for drain timeout")
	}
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}
func TestRevisionRotationStopsOldBeforeNewFactory(t *testing.T) {
	store := &leaseStore{}
	src := &source{accounts: []domain.Account{account(1)}}
	created := make(chan *client, 4)
	var previous *client
	fac := factoryFunc(func(ctx context.Context, a domain.Account, g domain.OwnerGrant, m app.CredentialMaterial) (app.Client, error) {
		if previous != nil {
			select {
			case <-previous.stopped:
			default:
				t.Error("new factory created before old Run stopped")
			}
			if !previous.quiesced.Load() || previous.closed.Load() == 0 {
				t.Error("old client not quiesced/closed")
			}
		}
		c := newClient()
		previous = c
		created <- c
		return c, nil
	})
	supervisor, cancel, done := start(t, store, src, &resolver{}, fac, options())
	first := wait(t, created)
	wait(t, first.started)
	eventually(t, supervisor.Ready)
	next := account(2)
	next.CredentialRef = "ENV:ROTATED"
	src.set([]domain.Account{next}, nil)
	second := wait(t, created)
	wait(t, second.started)
	eventually(t, func() bool { st, _ := supervisor.Status("account"); return st.Ready && st.Revision == 2 })
	state, _ := supervisor.Status("account")
	if state.Revision != 2 || state.Epoch != 2 {
		t.Fatal(state)
	}
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}
func TestDisabledRevisionStopsClientAndIsAppliedWithoutCredential(t *testing.T) {
	store := &leaseStore{}
	src := &source{accounts: []domain.Account{account(1)}}
	res := &resolver{}
	fac := &factory{created: make(chan *client, 4)}
	supervisor, cancel, done := start(t, store, src, res, fac, options())
	c := wait(t, fac.created)
	wait(t, c.started)
	disabled := account(2)
	disabled.Enabled = false
	src.set([]domain.Account{disabled}, nil)
	wait(t, c.stopped)
	eventually(t, func() bool { status, _ := supervisor.Status("account"); return status.Phase == app.PhaseDisabled })
	if res.calls.Load() != 1 {
		t.Fatal("disabled config resolved credential")
	}
	if !supervisor.Ready() {
		t.Fatal("disabled-only config not ready")
	}
	noEvent(t, fac.created, 30*time.Millisecond)
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}
func TestStaleAndConflictingSourcesBlockUntilHigherRevision(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint(conflict), func(t *testing.T) {
			store := &leaseStore{}
			src := &source{accounts: []domain.Account{account(2)}}
			fac := &factory{created: make(chan *client, 5)}
			supervisor, cancel, done := start(t, store, src, &resolver{}, fac, options())
			first := wait(t, fac.created)
			wait(t, first.started)
			bad := account(1)
			if conflict {
				bad = account(2)
				bad.CredentialRef = "ENV:CONFLICT"
			}
			src.set([]domain.Account{bad}, nil)
			wait(t, first.stopped)
			eventually(t, func() bool { st, _ := supervisor.Status("account"); return st.Phase == app.PhaseBlocked })
			src.set([]domain.Account{account(2)}, nil)
			noEvent(t, fac.created, 50*time.Millisecond)
			if st, _ := supervisor.Status("account"); st.Ready {
				t.Fatal("blocked account reported ready")
			}
			if !supervisor.Ready() {
				t.Fatal("single blocked account removed shared readiness")
			}
			src.set([]domain.Account{account(3)}, nil)
			second := wait(t, fac.created)
			wait(t, second.started)
			eventually(t, supervisor.Ready)
			cancel()
			if err := wait(t, done); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestReplacementBlocksSameRevisionAndDoesNotReleaseOnMarkFailure(t *testing.T) {
	base := &leaseStore{}
	store := &customStore{leaseStore: base, mark: func(context.Context, domain.OwnerGrant) error {
		base.marked.Add(1)
		return errors.New("database secret error")
	}}
	src := &source{accounts: []domain.Account{account(1)}}
	fac := &factory{created: make(chan *client, 5)}
	supervisor, cancel, done := start(t, store, src, &resolver{}, fac, options())
	c := wait(t, fac.created)
	wait(t, c.started)
	c.replaced.Store(true)
	wait(t, c.stopped)
	eventually(t, func() bool { return base.marked.Load() == 1 })
	noEvent(t, fac.created, 50*time.Millisecond)
	if base.releases.Load() != 0 {
		t.Fatal("failed MarkReplaced accelerated takeover with Release")
	}
	st, _ := supervisor.Status("account")
	if st.Reason != app.ReasonReplaced || !st.IsolationError || st.IsolationPersisted || strings.Contains(fmt.Sprint(st), "secret") {
		t.Fatal(st)
	}
	src.set([]domain.Account{account(2)}, nil)
	next := wait(t, fac.created)
	wait(t, next.started)
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}
func TestPersistentReplacedAndClientTerminalDoNotLoopSameCredentials(t *testing.T) {
	for _, heldReplaced := range []bool{true, false} {
		t.Run(fmt.Sprint(heldReplaced), func(t *testing.T) {
			store := &leaseStore{}
			if heldReplaced {
				store.acquireErr = domain.ErrReplaced
			}
			src := &source{accounts: []domain.Account{account(1)}}
			fac := &factory{created: make(chan *client, 4)}
			supervisor, cancel, done := start(t, store, src, &resolver{}, fac, options())
			if !heldReplaced {
				c := wait(t, fac.created)
				wait(t, c.started)
				c.terminal.Store(true)
				wait(t, c.stopped)
			}
			eventually(t, func() bool { st, _ := supervisor.Status("account"); return st.Phase == app.PhaseBlocked })
			before := store.acquires.Load()
			noEvent(t, fac.created, 60*time.Millisecond)
			if store.acquires.Load() != before {
				t.Fatal("terminal same revision retried acquire")
			}
			cancel()
			if err := wait(t, done); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestSlowAccountDoesNotBlockIndependentAccount(t *testing.T) {
	first, second := account(1), account(1)
	second.ID = "second"
	second.BotID = "second-bot"
	src := &source{accounts: []domain.Account{first, second}}
	store := &leaseStore{}
	fac := &factory{created: make(chan *client, 8)}
	res := resolverFunc(func(ctx context.Context, a domain.Account) (app.CredentialMaterial, error) {
		if a.ID == "account" {
			<-ctx.Done()
			return app.CredentialMaterial{}, ctx.Err()
		}
		return app.CredentialMaterial{Secret: "synthetic-secret"}, nil
	})
	supervisor, cancel, done := start(t, store, src, res, fac, options())
	c := wait(t, fac.created)
	wait(t, c.started)
	eventually(t, func() bool { st, _ := supervisor.Status("second"); return st.Ready })
	if st, _ := supervisor.Status("account"); st.Ready {
		t.Fatal("failed owned account reported ready")
	}
	if !supervisor.Ready() {
		t.Fatal("one failed account blocked an independent healthy account")
	}
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}
func TestSourceFailureStopsAndCanRecoverWithoutLeakingError(t *testing.T) {
	src := &source{accounts: []domain.Account{account(1)}}
	fac := &factory{created: make(chan *client, 5)}
	supervisor, cancel, done := start(t, &leaseStore{}, src, &resolver{}, fac, options())
	first := wait(t, fac.created)
	wait(t, first.started)
	src.set(nil, errors.New("database secret URL"))
	wait(t, first.stopped)
	eventually(t, func() bool { st, _ := supervisor.Status("account"); return st.Reason == app.ReasonSource })
	if supervisor.Ready() {
		t.Fatal("source failure retained ready")
	}
	st, _ := supervisor.Status("account")
	if strings.Contains(fmt.Sprint(st), "secret") {
		t.Fatal(st)
	}
	src.set([]domain.Account{account(1)}, nil)
	second := wait(t, fac.created)
	wait(t, second.started)
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}
func TestDatabaseIntervalAnchoredAtCallStartExpiresPromptly(t *testing.T) {
	base := &leaseStore{}
	store := &customStore{leaseStore: base}
	store.acquire = func(ctx context.Context, a domain.Account, id string, ttl time.Duration) (domain.OwnerGrant, error) {
		time.Sleep(100 * time.Millisecond)
		now := time.Now().Add(24 * time.Hour)
		return domain.OwnerGrant{AccountID: a.ID, BotID: a.BotID, InstanceID: id, Epoch: 1, Revision: a.Revision, ObservedAt: now, LeaseUntil: now.Add(160 * time.Millisecond)}, nil
	}
	store.renew = func(ctx context.Context, g domain.OwnerGrant, ttl time.Duration) (domain.OwnerGrant, error) {
		<-ctx.Done()
		return domain.OwnerGrant{}, ctx.Err()
	}
	opts := options()
	opts.LeaseTTL = 600 * time.Millisecond
	opts.OperationTimeout = 180 * time.Millisecond
	src := &source{accounts: []domain.Account{account(1)}}
	fac := &factory{created: make(chan *client, 5)}
	_, cancel, done := start(t, store, src, &resolver{}, fac, opts)
	c := wait(t, fac.created)
	wait(t, c.started)
	begin := time.Now()
	wait(t, c.stopped)
	if time.Since(begin) > 120*time.Millisecond {
		t.Fatal("deadline was anchored at response or absolute DB wall time")
	}
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}
func TestRunOnceAndOptionValidation(t *testing.T) {
	src := &source{}
	supervisor, cancel, done := start(t, &leaseStore{}, src, &resolver{}, &factory{}, options())
	eventually(t, supervisor.Ready)
	if err := supervisor.Run(context.Background()); !errors.Is(err, app.ErrAlreadyRun) {
		t.Fatal(err)
	}
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Run(context.Background()); !errors.Is(err, app.ErrAlreadyRun) {
		t.Fatal(err)
	}
	for _, modify := range []func(*app.Options){func(o *app.Options) { o.InstanceID = "" }, func(o *app.Options) { o.OperationTimeout = o.LeaseTTL / 3 }, func(o *app.Options) { o.DrainTimeout = -1 }, func(o *app.Options) { o.MaxAccounts = -1 }, func(o *app.Options) { o.PollInterval = -1 }} {
		opts := options()
		modify(&opts)
		if _, err := app.NewSupervisor(&leaseStore{}, src, &resolver{}, &factory{}, opts); !errors.Is(err, app.ErrInvalid) {
			t.Fatal(err)
		}
	}
}
