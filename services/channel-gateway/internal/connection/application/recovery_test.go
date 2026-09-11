package application_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

type recoveryClient struct {
	*client
	retryable atomic.Bool
}

func (c *recoveryClient) Status() app.ClientStatus {
	status := c.client.Status()
	status.Retryable = c.retryable.Load()
	return status
}
func TestRetryableTerminalRecoversSameRevisionAfterBackoff(t *testing.T) {
	created := make(chan *recoveryClient, 4)
	fac := factoryFunc(func(context.Context, domain.Account, domain.OwnerGrant, app.CredentialMaterial) (app.Client, error) {
		c := &recoveryClient{client: newClient()}
		created <- c
		return c, nil
	})
	opts := options()
	opts.RestartBackoff = 40 * time.Millisecond
	opts.RestartCooldown = 200 * time.Millisecond
	opts.MaxRestarts = 1
	supervisor, cancel, done := start(t, &leaseStore{}, &source{accounts: []domain.Account{account(1)}}, &resolver{}, fac, opts)
	first := wait(t, created)
	wait(t, first.started)
	first.retryable.Store(true)
	first.terminal.Store(true)
	wait(t, first.stopped)
	noEvent(t, created, 25*time.Millisecond)
	second := wait(t, created)
	wait(t, second.started)
	eventually(t, func() bool { status, _ := supervisor.Status("account"); return status.Ready })
	status, _ := supervisor.Status("account")
	if status.Revision != 1 || status.RestartAttempts != 1 {
		t.Fatal(status)
	}
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestAccountFailureDoesNotRemoveSupervisorReadiness(t *testing.T) {
	created := make(chan *client, 1)
	fac := factoryFunc(func(context.Context, domain.Account, domain.OwnerGrant, app.CredentialMaterial) (app.Client, error) {
		c := newClient()
		created <- c
		return c, nil
	})
	supervisor, cancel, done := start(t, &leaseStore{}, &source{accounts: []domain.Account{account(1)}}, &resolver{}, fac, options())
	c := wait(t, created)
	wait(t, c.started)
	c.terminal.Store(true)
	wait(t, c.stopped)
	eventually(t, func() bool { status, _ := supervisor.Status("account"); return status.Phase == app.PhaseBlocked })
	if !supervisor.Ready() {
		t.Fatal("single account terminal failure removed whole Supervisor readiness")
	}
	status, _ := supervisor.Status("account")
	if status.Ready {
		t.Fatal("blocked account itself reported ready")
	}
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestReplacementIsolationOutcomeSurvivesCloseFailure(t *testing.T) {
	for _, persistFail := range []bool{false, true} {
		t.Run(map[bool]string{false: "persisted", true: "isolation_failed"}[persistFail], func(t *testing.T) {
			base := &leaseStore{}
			store := &customStore{leaseStore: base}
			if persistFail {
				store.mark = func(context.Context, domain.OwnerGrant) error { return context.DeadlineExceeded }
			}
			created := make(chan *client, 1)
			fac := factoryFunc(func(context.Context, domain.Account, domain.OwnerGrant, app.CredentialMaterial) (app.Client, error) {
				c := newClient()
				created <- c
				return &closeClient{client: c, closeFn: func(context.Context) error { return context.DeadlineExceeded }}, nil
			})
			supervisor, cancel, done := start(t, store, &source{accounts: []domain.Account{account(1)}}, &resolver{}, fac, options())
			c := wait(t, created)
			wait(t, c.started)
			c.replaced.Store(true)
			wait(t, c.stopped)
			eventually(t, func() bool { status, _ := supervisor.Status("account"); return status.Reason == app.ReasonClose })
			status, _ := supervisor.Status("account")
			if status.IsolationPersisted == persistFail || status.IsolationError != persistFail {
				t.Fatalf("Close failure obscured independent isolation result: %#v", status)
			}
			if base.releases.Load() != 0 {
				t.Fatal("replacement path released grant")
			}
			cancel()
			if err := wait(t, done); err != app.ErrCleanup {
				t.Fatal(err)
			}
		})
	}
}

func transientFailure(t *testing.T, supervisor *app.Supervisor, c *recoveryClient) {
	t.Helper()
	wait(t, c.started)
	eventually(t, func() bool { status, _ := supervisor.Status("account"); return status.Ready })
	c.retryable.Store(true)
	c.terminal.Store(true)
	wait(t, c.stopped)
}
func recoveryFactory(created chan<- *recoveryClient) factoryFunc {
	return func(context.Context, domain.Account, domain.OwnerGrant, app.CredentialMaterial) (app.Client, error) {
		c := &recoveryClient{client: newClient()}
		created <- c
		return c, nil
	}
}
func TestRetryBudgetCooldownAndHalfOpenDoNotResetOnReady(t *testing.T) {
	created := make(chan *recoveryClient, 8)
	store := &leaseStore{}
	opts := options()
	opts.PollInterval = 2 * time.Millisecond
	opts.RestartBackoff = 20 * time.Millisecond
	opts.RestartCooldown = 140 * time.Millisecond
	opts.MaxRestarts = 2
	supervisor, cancel, done := start(t, store, &source{accounts: []domain.Account{account(1)}}, &resolver{}, recoveryFactory(created), opts)
	first := wait(t, created)
	transientFailure(t, supervisor, first)
	noEvent(t, created, 12*time.Millisecond)
	second := wait(t, created)
	transientFailure(t, supervisor, second)
	noEvent(t, created, 28*time.Millisecond)
	third := wait(t, created)
	transientFailure(t, supervisor, third)
	eventually(t, func() bool { status, _ := supervisor.Status("account"); return status.Phase == app.PhaseCooldown })
	status, _ := supervisor.Status("account")
	if status.RestartAttempts != 2 || status.NextRetryAt.IsZero() {
		t.Fatal(status)
	}
	before := store.acquires.Load()
	noEvent(t, created, 80*time.Millisecond)
	if store.acquires.Load() != before {
		t.Fatal("source poll bypassed cooldown with lease acquisition")
	}
	fourth := wait(t, created)
	transientFailure(t, supervisor, fourth)
	eventually(t, func() bool { status, _ := supervisor.Status("account"); return status.Phase == app.PhaseCooldown })
	noEvent(t, created, 90*time.Millisecond)
	fifth := wait(t, created)
	wait(t, fifth.started)
	eventually(t, func() bool { status, _ := supervisor.Status("account"); return status.Ready })
	status, _ = supervisor.Status("account")
	if status.RestartAttempts != 2 {
		t.Fatal("short READY reset exhausted budget")
	}
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}
func TestHigherRevisionResetsCooldownAndBudget(t *testing.T) {
	created := make(chan *recoveryClient, 6)
	src := &source{accounts: []domain.Account{account(1)}}
	opts := options()
	opts.RestartBackoff = 10 * time.Millisecond
	opts.RestartCooldown = 500 * time.Millisecond
	opts.MaxRestarts = 1
	supervisor, cancel, done := start(t, &leaseStore{}, src, &resolver{}, recoveryFactory(created), opts)
	transientFailure(t, supervisor, wait(t, created))
	transientFailure(t, supervisor, wait(t, created))
	eventually(t, func() bool { status, _ := supervisor.Status("account"); return status.Phase == app.PhaseCooldown })
	begin := time.Now()
	src.set([]domain.Account{account(2)}, nil)
	third := wait(t, created)
	wait(t, third.started)
	if time.Since(begin) > 250*time.Millisecond {
		t.Fatal("new revision inherited old cooldown")
	}
	eventually(t, func() bool { status, _ := supervisor.Status("account"); return status.Ready && status.Revision == 2 })
	status, _ := supervisor.Status("account")
	if status.RestartAttempts != 0 || !status.NextRetryAt.IsZero() || status.IsolationPersisted || status.IsolationError {
		t.Fatal(status)
	}
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}
func TestCancelDuringCooldownDoesNotWaitOrReconnect(t *testing.T) {
	created := make(chan *recoveryClient, 5)
	opts := options()
	opts.RestartBackoff = 10 * time.Millisecond
	opts.RestartCooldown = time.Second
	opts.MaxRestarts = 1
	supervisor, cancel, done := start(t, &leaseStore{}, &source{accounts: []domain.Account{account(1)}}, &resolver{}, recoveryFactory(created), opts)
	transientFailure(t, supervisor, wait(t, created))
	transientFailure(t, supervisor, wait(t, created))
	eventually(t, func() bool { status, _ := supervisor.Status("account"); return status.Phase == app.PhaseCooldown })
	begin := time.Now()
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
	if time.Since(begin) > 100*time.Millisecond {
		t.Fatal("shutdown waited for cooldown")
	}
	noEvent(t, created, 30*time.Millisecond)
}
func TestReplacedOverridesRetryableAndPersistsIsolation(t *testing.T) {
	created := make(chan *recoveryClient, 3)
	store := &leaseStore{}
	opts := options()
	opts.RestartBackoff = 5 * time.Millisecond
	opts.RestartCooldown = 40 * time.Millisecond
	opts.MaxRestarts = 1
	supervisor, cancel, done := start(t, store, &source{accounts: []domain.Account{account(1)}}, &resolver{}, recoveryFactory(created), opts)
	first := wait(t, created)
	wait(t, first.started)
	first.retryable.Store(true)
	first.replaced.Store(true)
	first.terminal.Store(true)
	wait(t, first.stopped)
	eventually(t, func() bool { status, _ := supervisor.Status("account"); return status.IsolationPersisted })
	noEvent(t, created, 80*time.Millisecond)
	status, _ := supervisor.Status("account")
	if status.Phase != app.PhaseBlocked || status.RestartAttempts != 0 || status.IsolationError {
		t.Fatal(status)
	}
	if store.marked.Load() != 1 {
		t.Fatal(store.marked.Load())
	}
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}
func TestRetryableFlagDoesNotRestartAnActiveClient(t *testing.T) {
	created := make(chan *recoveryClient, 3)
	opts := options()
	opts.RestartBackoff = 5 * time.Millisecond
	opts.RestartCooldown = 30 * time.Millisecond
	supervisor, cancel, done := start(t, &leaseStore{}, &source{accounts: []domain.Account{account(1)}}, &resolver{}, recoveryFactory(created), opts)
	first := wait(t, created)
	wait(t, first.started)
	first.retryable.Store(true)
	noEvent(t, created, 50*time.Millisecond)
	eventually(t, func() bool { status, _ := supervisor.Status("account"); return status.Ready })
	status, _ := supervisor.Status("account")
	if status.RestartAttempts != 0 {
		t.Fatal(status)
	}
	first.retryable.Store(false)
	first.terminal.Store(true)
	wait(t, first.stopped)
	noEvent(t, created, 50*time.Millisecond)
	status, _ = supervisor.Status("account")
	if status.Phase != app.PhaseBlocked {
		t.Fatal(status)
	}
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}
func TestDefaultRestartSlotsAndCooldownAreExplicit(t *testing.T) {
	created := make(chan *recoveryClient, 6)
	opts := options()
	opts.RestartBackoff = 5 * time.Millisecond // Keep default three slots and 60-second cooldown.
	supervisor, cancel, done := start(t, &leaseStore{}, &source{accounts: []domain.Account{account(1)}}, &resolver{}, recoveryFactory(created), opts)
	for range 4 {
		transientFailure(t, supervisor, wait(t, created))
	}
	eventually(t, func() bool { status, _ := supervisor.Status("account"); return status.Phase == app.PhaseCooldown })
	status, _ := supervisor.Status("account")
	remaining := time.Until(status.NextRetryAt)
	if status.RestartAttempts != 3 || remaining < 59*time.Second || remaining > 60*time.Second {
		t.Fatal(status, remaining)
	}
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}
func TestDefaultRestartBackoffAndRecoveryOptionValidation(t *testing.T) {
	created := make(chan *recoveryClient, 2)
	supervisor, cancel, done := start(t, &leaseStore{}, &source{accounts: []domain.Account{account(1)}}, &resolver{}, recoveryFactory(created), options())
	transientFailure(t, supervisor, wait(t, created))
	eventually(t, func() bool { status, _ := supervisor.Status("account"); return status.Phase == app.PhaseBackoff })
	status, _ := supervisor.Status("account")
	remaining := time.Until(status.NextRetryAt)
	if remaining < 800*time.Millisecond || remaining > time.Second {
		t.Fatal(remaining)
	}
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*app.Options){func(o *app.Options) { o.RestartBackoff = -1 }, func(o *app.Options) { o.RestartCooldown = -1 }, func(o *app.Options) { o.RestartBackoff = time.Second; o.RestartCooldown = time.Millisecond }, func(o *app.Options) { o.RestartCooldown = 2 * time.Hour }, func(o *app.Options) { o.MaxRestarts = -1 }, func(o *app.Options) { o.MaxRestarts = 11 }} {
		o := options()
		edit(&o)
		if _, err := app.NewSupervisor(&leaseStore{}, &source{}, &resolver{}, &factory{}, o); err != app.ErrInvalid {
			t.Fatal(err)
		}
	}
}
func TestReadinessRequiresCompleteSourceButNotBotAuthentication(t *testing.T) {
	src := &source{err: context.DeadlineExceeded}
	created := make(chan *client, 2)
	fac := factoryFunc(func(context.Context, domain.Account, domain.OwnerGrant, app.CredentialMaterial) (app.Client, error) {
		c := newClient()
		created <- c
		return &unauthenticatedClient{client: c}, nil
	})
	supervisor, cancel, done := start(t, &leaseStore{}, src, &resolver{}, fac, options())
	time.Sleep(25 * time.Millisecond)
	if supervisor.Ready() {
		t.Fatal("missing initial source snapshot reported ready")
	}
	src.set([]domain.Account{account(1)}, nil)
	c := wait(t, created)
	wait(t, c.started)
	eventually(t, supervisor.Ready)
	status, _ := supervisor.Status("account")
	if status.Ready {
		t.Fatal("unauthenticated account reported ready")
	}
	src.set(nil, context.DeadlineExceeded)
	eventually(t, func() bool { return !supervisor.Ready() })
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}

type unauthenticatedClient struct {
	*client
}

func (c *unauthenticatedClient) Status() app.ClientStatus {
	return app.ClientStatus{Terminal: c.terminal.Load()}
}

type recoveryCloseClient struct {
	*recoveryClient
	closeFn func(context.Context) error
}

func (c *recoveryCloseClient) Close(ctx context.Context) error { return c.closeFn(ctx) }
func TestSourceOutageCannotEraseAnAlreadyClassifiedRecoveryBudget(t *testing.T) {
	created := make(chan *recoveryClient, 3)
	closeEntered := make(chan struct{})
	releaseClose := make(chan struct{})
	src := &source{accounts: []domain.Account{account(1)}}
	var factories atomic.Int64
	fac := factoryFunc(func(context.Context, domain.Account, domain.OwnerGrant, app.CredentialMaterial) (app.Client, error) {
		c := &recoveryClient{client: newClient()}
		created <- c
		if factories.Add(1) == 1 {
			return &recoveryCloseClient{recoveryClient: c, closeFn: func(ctx context.Context) error {
				close(closeEntered)
				select {
				case <-releaseClose:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}}, nil
		}
		return c, nil
	})
	opts := options()
	opts.LeaseTTL = 600 * time.Millisecond
	opts.OperationTimeout = 150 * time.Millisecond
	opts.PollInterval = 2 * time.Millisecond
	opts.RestartBackoff = 120 * time.Millisecond
	opts.RestartCooldown = 300 * time.Millisecond
	supervisor, cancel, done := start(t, &leaseStore{}, src, &resolver{}, fac, opts)
	first := wait(t, created)
	transientFailure(t, supervisor, first)
	wait(t, closeEntered)
	src.set(nil, context.DeadlineExceeded)
	eventually(t, func() bool { return !supervisor.Ready() })
	close(releaseClose)
	eventually(t, func() bool {
		status, _ := supervisor.Status("account")
		return status.Phase == app.PhaseBlocked && status.Reason == app.ReasonSource
	})
	src.set([]domain.Account{account(1)}, nil)
	noEvent(t, created, 75*time.Millisecond)
	second := wait(t, created)
	wait(t, second.started)
	status, _ := supervisor.Status("account")
	if status.RestartAttempts != 1 {
		t.Fatal(status)
	}
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}
