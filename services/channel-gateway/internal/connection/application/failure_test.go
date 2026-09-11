package application_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/application"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/connection/domain"
)

type closeClient struct {
	*client
	closeFn func(context.Context) error
}

func (c *closeClient) Close(ctx context.Context) error { return c.closeFn(ctx) }
func TestCloseFailureKeepsLeaseAndPreventsReplacementClient(t *testing.T) {
	base := &leaseStore{}
	src := &source{accounts: []domain.Account{account(1)}}
	created := make(chan *client, 4)
	fac := factoryFunc(func(context.Context, domain.Account, domain.OwnerGrant, app.CredentialMaterial) (app.Client, error) {
		c := newClient()
		created <- c
		return &closeClient{client: c, closeFn: func(ctx context.Context) error {
			if ctx.Err() != nil {
				t.Error("cleanup inherited canceled context")
			}
			<-ctx.Done()
			return ctx.Err()
		}}, nil
	})
	supervisor, cancel, done := start(t, base, src, &resolver{}, fac, options())
	c := wait(t, created)
	wait(t, c.started)
	cancel()
	if err := wait(t, done); !errors.Is(err, app.ErrCleanup) {
		t.Fatal(err)
	}
	if base.releases.Load() != 0 {
		t.Fatal("failed Close released lease early")
	}
	st, _ := supervisor.Status("account")
	if st.Reason != app.ReasonClose {
		t.Fatal(st)
	}
	wait(t, c.stopped)
}
func TestRenewContinuesDuringClientClose(t *testing.T) {
	base := &leaseStore{}
	created := make(chan *client, 1)
	closing := make(chan struct{})
	var renewAtClose atomic.Int64
	fac := factoryFunc(func(context.Context, domain.Account, domain.OwnerGrant, app.CredentialMaterial) (app.Client, error) {
		c := newClient()
		created <- c
		return &closeClient{client: c, closeFn: func(ctx context.Context) error {
			renewAtClose.Store(base.renews.Load())
			close(closing)
			timer := time.NewTimer(120 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return nil
			}
		}}, nil
	})
	opts := options()
	opts.LeaseTTL = 600 * time.Millisecond
	opts.OperationTimeout = 150 * time.Millisecond
	supervisor, cancel, done := start(t, base, &source{accounts: []domain.Account{account(1)}}, &resolver{}, fac, opts)
	c := wait(t, created)
	wait(t, c.started)
	eventually(t, supervisor.Ready)
	// Close straddles the first renewal at approximately TTL/3.
	time.Sleep(150 * time.Millisecond)
	cancel()
	wait(t, closing)
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
	if base.renews.Load() <= renewAtClose.Load() {
		t.Fatal("renew stopped before Client.Close completed")
	}
}
func TestStaleFactoryResultNeverRunsAndIsCleanedExactlyOnce(t *testing.T) {
	base := &leaseStore{}
	src := &source{accounts: []domain.Account{account(1)}}
	firstFactory := make(chan *client, 1)
	release := make(chan struct{})
	secondCreated := make(chan *client, 1)
	fac := factoryFunc(func(ctx context.Context, a domain.Account, g domain.OwnerGrant, m app.CredentialMaterial) (app.Client, error) {
		c := newClient()
		if a.Revision == 1 {
			firstFactory <- c
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		} else {
			secondCreated <- c
		}
		return c, nil
	})
	opts := options()
	opts.LeaseTTL = time.Second
	opts.OperationTimeout = 200 * time.Millisecond
	supervisor, cancel, done := start(t, base, src, &resolver{}, fac, opts)
	first := wait(t, firstFactory)
	src.set([]domain.Account{account(2)}, nil)
	eventually(t, func() bool { st, _ := supervisor.Status("account"); return !st.Ready })
	time.Sleep(25 * time.Millisecond)
	close(release)
	second := wait(t, secondCreated)
	wait(t, second.started)
	if first.runs.Load() != 0 || first.closed.Load() != 1 || base.releases.Load() != 1 {
		t.Fatalf("runs=%d closes=%d releases=%d", first.runs.Load(), first.closed.Load(), base.releases.Load())
	}
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}
func TestCredentialAndFactoryFailuresDoNotExposeSecretsOrStartClient(t *testing.T) {
	for _, failure := range []string{"resolve", "empty", "factory", "nil_factory"} {
		t.Run(failure, func(t *testing.T) {
			base := &leaseStore{}
			res := resolverFunc(func(ctx context.Context, a domain.Account) (app.CredentialMaterial, error) {
				if failure == "resolve" {
					return app.CredentialMaterial{}, errors.New("secret text")
				}
				if failure == "empty" {
					return app.CredentialMaterial{}, nil
				}
				return app.CredentialMaterial{Secret: "synthetic-secret"}, nil
			})
			var calls atomic.Int64
			fac := factoryFunc(func(context.Context, domain.Account, domain.OwnerGrant, app.CredentialMaterial) (app.Client, error) {
				calls.Add(1)
				if failure == "nil_factory" {
					return nil, nil
				}
				return nil, errors.New("factory secret text")
			})
			supervisor, cancel, done := start(t, base, &source{accounts: []domain.Account{account(1)}}, res, fac, options())
			eventually(t, func() bool { st, _ := supervisor.Status("account"); return st.Phase == app.PhaseFailed })
			if st, _ := supervisor.Status("account"); st.Ready {
				t.Fatal("failed account ready")
			}
			if !supervisor.Ready() {
				t.Fatal("single account failure removed shared readiness")
			}
			if (failure == "resolve" || failure == "empty") && calls.Load() != 0 {
				t.Fatal("factory called without credential")
			}
			cancel()
			if err := wait(t, done); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestInvalidSourceSnapshotFailsClosed(t *testing.T) {
	for _, kind := range []string{"duplicate_id", "duplicate_bot", "invalid", "over_limit", "removed"} {
		t.Run(kind, func(t *testing.T) {
			src := &source{accounts: []domain.Account{account(1)}}
			fac := &factory{created: make(chan *client, 5)}
			opts := options()
			opts.MaxAccounts = 2
			supervisor, cancel, done := start(t, &leaseStore{}, src, &resolver{}, fac, opts)
			first := wait(t, fac.created)
			wait(t, first.started)
			var accounts []domain.Account
			switch kind {
			case "duplicate_id":
				accounts = []domain.Account{account(1), account(1)}
			case "duplicate_bot":
				other := account(1)
				other.ID = "other"
				accounts = []domain.Account{account(1), other}
			case "invalid":
				other := account(1)
				other.Revision = 0
				accounts = []domain.Account{other}
			case "over_limit":
				for i, id := range []string{"a", "b", "c"} {
					other := account(int64(i + 1))
					other.ID = id
					other.BotID = id
					accounts = append(accounts, other)
				}
			}
			src.set(accounts, nil)
			wait(t, first.stopped)
			noEvent(t, fac.created, 30*time.Millisecond)
			if kind == "removed" {
				eventually(t, supervisor.Ready)
			} else if supervisor.Ready() {
				t.Fatal("invalid source retained ready")
			}
			cancel()
			if err := wait(t, done); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestGrantMismatchNeverResolvesCredentials(t *testing.T) {
	base := &leaseStore{}
	store := &customStore{leaseStore: base, acquire: func(ctx context.Context, a domain.Account, id string, ttl time.Duration) (domain.OwnerGrant, error) {
		now := time.Now()
		return domain.OwnerGrant{AccountID: a.ID, BotID: "wrong-bot", InstanceID: id, Epoch: 1, Revision: a.Revision, ObservedAt: now, LeaseUntil: now.Add(ttl)}, nil
	}}
	res := &resolver{}
	supervisor, cancel, done := start(t, store, &source{accounts: []domain.Account{account(1)}}, res, &factory{}, options())
	eventually(t, func() bool { st, _ := supervisor.Status("account"); return st.Reason == app.ReasonLost })
	if res.calls.Load() != 0 {
		t.Fatal("invalid grant resolved credential")
	}
	cancel()
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}
func TestLeaseExpiryFencesEvenWhenRenewIgnoresCancellation(t *testing.T) {
	base := &leaseStore{}
	renewEntered := make(chan struct{})
	releaseRenew := make(chan struct{})
	store := &customStore{leaseStore: base, renew: func(ctx context.Context, g domain.OwnerGrant, ttl time.Duration) (domain.OwnerGrant, error) {
		close(renewEntered)
		<-releaseRenew
		return domain.OwnerGrant{}, domain.ErrLost
	}}
	fac := &factory{created: make(chan *client, 3)}
	supervisor, cancel, done := start(t, store, &source{accounts: []domain.Account{account(1)}}, &resolver{}, fac, options())
	c := wait(t, fac.created)
	wait(t, c.started)
	wait(t, renewEntered)
	wait(t, c.stopped)
	if st, _ := supervisor.Status("account"); st.Ready {
		t.Fatal("local expired account lease retained ready")
	}
	if !supervisor.Ready() {
		t.Fatal("single lost lease removed shared readiness")
	}
	if !c.quiesced.Load() {
		t.Fatal("expiry did not quiesce")
	}
	cancel()
	close(releaseRenew)
	if err := wait(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestReplacementPersistsBlockEvenWhenClientCloseFails(t *testing.T) {
	base := &leaseStore{}
	created := make(chan *client, 1)
	fac := factoryFunc(func(context.Context, domain.Account, domain.OwnerGrant, app.CredentialMaterial) (app.Client, error) {
		c := newClient()
		created <- c
		return &closeClient{client: c, closeFn: func(ctx context.Context) error { return errors.New("close failed") }}, nil
	})
	supervisor, cancel, done := start(t, base, &source{accounts: []domain.Account{account(1)}}, &resolver{}, fac, options())
	c := wait(t, created)
	wait(t, c.started)
	c.replaced.Store(true)
	wait(t, c.stopped)
	eventually(t, func() bool { st, _ := supervisor.Status("account"); return st.Reason == app.ReasonClose })
	if base.marked.Load() != 1 {
		t.Fatal("replacement block lost because Client.Close failed")
	}
	if base.releases.Load() != 0 {
		t.Fatal("replacement cleanup released lease")
	}
	cancel()
	if err := wait(t, done); !errors.Is(err, app.ErrCleanup) {
		t.Fatal(err)
	}
}
