package application_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	app "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/application"
	d "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/delivery/domain"
)

type dueFixture func(context.Context, app.DueAccountQuery) (app.DueAccountPage, error)

func (f dueFixture) ListDueAccounts(c context.Context, q app.DueAccountQuery) (app.DueAccountPage, error) {
	return f(c, q)
}

type eligibilityFixture func(context.Context, app.AccountKey) (app.SendEligibility, error)

func (f eligibilityFixture) InspectAccount(c context.Context, k app.AccountKey) (app.SendEligibility, error) {
	return f(c, k)
}

type dispatchFixture func(context.Context, d.ClaimRequest) (int, error)

func (f dispatchFixture) DispatchAccount(c context.Context, q d.ClaimRequest) (int, error) {
	return f(c, q)
}
func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("timed out")
		var z T
		return z
	}
}
func TestRunnerDispatchesOneFencedAccountAndRunsMaintenance(t *testing.T) {
	var swept atomic.Int32
	maintenanceStarted := make(chan struct{}, 1)
	m, err := app.NewMaintainer(maintenanceFixture{expire: func(context.Context, int) (int, error) {
		swept.Add(1)
		select {
		case maintenanceStarted <- struct{}{}:
		default:
		}
		return 0, nil
	}}, app.MaintenanceOptions{PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	sent := make(chan d.ClaimRequest, 1)
	r, err := app.NewRunner(dueFixture(func(ctx context.Context, q app.DueAccountQuery) (app.DueAccountPage, error) {
		return app.DueAccountPage{Accounts: []app.AccountKey{{Provider: "wecom", AccountID: "account"}}, NextAccountID: "account", Exhausted: true}, nil
	}), eligibilityFixture(func(context.Context, app.AccountKey) (app.SendEligibility, error) {
		return app.SendEligibility{Eligible: true, Owner: &d.OwnerFence{InstanceID: "instance", Epoch: 7, Revision: 2}}, nil
	}), dispatchFixture(func(ctx context.Context, q d.ClaimRequest) (int, error) {
		select {
		case sent <- q:
		default:
		}
		return 1, nil
	}), m, app.RunnerOptions{InstanceID: "instance", Providers: []string{"wecom"}, PollInterval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	q := receive(t, sent)
	if q.Limit != 1 || q.Owner == nil || q.Owner.Epoch != 7 || q.AccountID != "account" || q.InstanceID != "instance" {
		t.Fatalf("claim %+v", q)
	}
	receive(t, maintenanceStarted)
	cancel()
	if err = receive(t, done); err != nil {
		t.Fatal(err)
	}
	if swept.Load() == 0 {
		t.Fatal("maintenance never ran")
	}
	if err = r.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRejectsMalformedDiscoveryBeforeEligibility(t *testing.T) {
	var inspected atomic.Int32
	queried := make(chan struct{}, 1)
	m, _ := app.NewMaintainer(maintenanceFixture{}, app.MaintenanceOptions{})
	r, err := app.NewRunner(dueFixture(func(context.Context, app.DueAccountQuery) (app.DueAccountPage, error) {
		select {
		case queried <- struct{}{}:
		default:
		}
		return app.DueAccountPage{Accounts: []app.AccountKey{{Provider: "telegram", AccountID: "a"}}, NextAccountID: "z"}, nil
	}), eligibilityFixture(func(context.Context, app.AccountKey) (app.SendEligibility, error) {
		inspected.Add(1)
		return app.SendEligibility{Eligible: true}, nil
	}), dispatchFixture(func(context.Context, d.ClaimRequest) (int, error) { return 1, nil }), m, app.RunnerOptions{InstanceID: "instance", Providers: []string{"telegram"}, PollInterval: time.Millisecond, MaxPagesPerTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	receive(t, queried)
	receive(t, queried)
	cancel()
	if err = receive(t, done); err != nil {
		t.Fatal(err)
	}
	if inspected.Load() != 0 || !r.Snapshot().ScanFailed {
		t.Fatalf("inspection %d snapshot %+v", inspected.Load(), r.Snapshot())
	}
}

func TestRunnerQuiesceCancelsDiscoveryAndDrains(t *testing.T) {
	started, canceled := make(chan struct{}), make(chan struct{})
	m, _ := app.NewMaintainer(maintenanceFixture{}, app.MaintenanceOptions{})
	r, err := app.NewRunner(dueFixture(func(ctx context.Context, q app.DueAccountQuery) (app.DueAccountPage, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return app.DueAccountPage{}, ctx.Err()
	}), eligibilityFixture(func(context.Context, app.AccountKey) (app.SendEligibility, error) {
		t.Error("unexpected eligibility")
		return app.SendEligibility{}, nil
	}), dispatchFixture(func(context.Context, d.ClaimRequest) (int, error) { t.Error("unexpected dispatch"); return 0, nil }), m, app.RunnerOptions{InstanceID: "instance", OperationTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	receive(t, started)
	r.Quiesce()
	select {
	case <-canceled:
	case <-time.After(100 * time.Millisecond):
		t.Error("quiesce did not cancel discovery")
	}
	cancel()
	if err = receive(t, done); err != nil {
		t.Fatal(err)
	}
}

func pagedAccounts(accounts map[string][]string) dueFixture {
	return func(ctx context.Context, q app.DueAccountQuery) (app.DueAccountPage, error) {
		if ctx == nil {
			return app.DueAccountPage{}, d.ErrInvalid
		}
		page := app.DueAccountPage{Exhausted: true}
		for _, id := range accounts[q.Provider] {
			if id <= q.AfterAccountID {
				continue
			}
			if len(page.Accounts) == q.Limit {
				page.Exhausted = false
				break
			}
			page.Accounts = append(page.Accounts, app.AccountKey{Provider: q.Provider, AccountID: id})
			page.NextAccountID = id
		}
		return page, nil
	}
}
func eligibleFor(instance string) eligibilityFixture {
	return func(ctx context.Context, k app.AccountKey) (app.SendEligibility, error) {
		e := app.SendEligibility{Eligible: true}
		if k.Provider == "wecom" {
			e.Owner = &d.OwnerFence{InstanceID: instance, Epoch: 1, Revision: 1}
		}
		return e, nil
	}
}

func TestRunnerFairnessAcrossProvidersPagesAndUnavailableAccounts(t *testing.T) {
	m, _ := app.NewMaintainer(maintenanceFixture{}, app.MaintenanceOptions{})
	sent := make(chan app.AccountKey, 64)
	r, err := app.NewRunner(pagedAccounts(map[string][]string{"telegram": {"a-unavailable", "b-hot", "c-normal", "d-normal"}, "wecom": {"w-one", "w-two"}}), eligibilityFixture(func(ctx context.Context, k app.AccountKey) (app.SendEligibility, error) {
		if k.AccountID == "a-unavailable" {
			return app.SendEligibility{}, d.ErrUnavailable
		}
		return eligibleFor("instance")(ctx, k)
	}), dispatchFixture(func(ctx context.Context, q d.ClaimRequest) (int, error) {
		sent <- app.AccountKey{Provider: q.Provider, AccountID: q.AccountID}
		select {
		case <-time.After(2 * time.Millisecond):
		case <-ctx.Done():
		}
		return 1, nil
	}), m, app.RunnerOptions{InstanceID: "instance", PollInterval: time.Millisecond, PageSize: 2, MaxPagesPerTick: 2, MaxWorkers: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	observed := map[string]bool{}
	for len(observed) < 5 {
		key := receive(t, sent)
		if key.AccountID == "a-unavailable" {
			t.Fatal("unavailable account dispatched")
		}
		observed[key.AccountID] = true
	}
	cancel()
	if err = receive(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerBoundsWorkersAndDrainsEvidenceWithoutDuplicateAccount(t *testing.T) {
	m, _ := app.NewMaintainer(maintenanceFixture{}, app.MaintenanceOptions{})
	started := make(chan string, 16)
	release := make(chan struct{})
	var finished atomic.Int32
	r, err := app.NewRunner(pagedAccounts(map[string][]string{"telegram": {"a", "b", "c", "d", "e"}}), eligibleFor("instance"), dispatchFixture(func(ctx context.Context, q d.ClaimRequest) (int, error) {
		started <- q.AccountID
		<-release
		if ctx.Err() != nil {
			t.Error("quiesce canceled evidence window")
		}
		finished.Add(1)
		return 1, nil
	}), m, app.RunnerOptions{InstanceID: "instance", Providers: []string{"telegram"}, PollInterval: time.Millisecond, PageSize: 5})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		id := receive(t, started)
		if seen[id] {
			t.Error("duplicate account dispatch")
		}
		seen[id] = true
	}
	select {
	case id := <-started:
		t.Errorf("over worker budget: %s", id)
	case <-time.After(15 * time.Millisecond):
	}
	r.Quiesce()
	cancel()
	short, stop := context.WithTimeout(context.Background(), 5*time.Millisecond)
	if err = r.Drain(short); err != app.ErrRuntimeDrainTimeout {
		t.Errorf("drain %v", err)
	}
	stop()
	close(release)
	if err = receive(t, done); err != nil {
		t.Fatal(err)
	}
	if err = r.Drain(context.Background()); err != nil || finished.Load() != 4 || r.Snapshot().InFlight != 0 {
		t.Fatalf("drain %v finished %d snapshot %+v", err, finished.Load(), r.Snapshot())
	}
	select {
	case id := <-started:
		t.Fatalf("dispatch after quiesce: %s", id)
	default:
	}
}

func TestRunnerRejectsInvalidEligibilityFence(t *testing.T) {
	for name, tc := range map[string]struct {
		provider string
		owner    *d.OwnerFence
	}{
		"telegram_owner":      {"telegram", &d.OwnerFence{InstanceID: "instance", Epoch: 1, Revision: 1}},
		"missing_wecom_owner": {"wecom", nil}, "foreign_owner": {"wecom", &d.OwnerFence{InstanceID: "other", Epoch: 1, Revision: 1}},
		"invalid_epoch": {"wecom", &d.OwnerFence{InstanceID: "instance", Epoch: 0, Revision: 1}},
	} {
		t.Run(name, func(t *testing.T) {
			m, _ := app.NewMaintainer(maintenanceFixture{}, app.MaintenanceOptions{})
			inspected := make(chan struct{}, 16)
			var calls atomic.Int32
			r, err := app.NewRunner(pagedAccounts(map[string][]string{tc.provider: {"account"}}), eligibilityFixture(func(context.Context, app.AccountKey) (app.SendEligibility, error) {
				inspected <- struct{}{}
				return app.SendEligibility{Eligible: true, Owner: tc.owner}, nil
			}), dispatchFixture(func(context.Context, d.ClaimRequest) (int, error) { calls.Add(1); return 0, nil }), m, app.RunnerOptions{InstanceID: "instance", Providers: []string{tc.provider}, PollInterval: time.Millisecond, MaxPagesPerTick: 1})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- r.Run(ctx) }()
			receive(t, inspected)
			receive(t, inspected)
			cancel()
			if err = receive(t, done); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 0 {
				t.Fatal("invalid grant dispatched")
			}
		})
	}
}

func TestRunnerDoesNotSilentlyReuseCompletedMaintainer(t *testing.T) {
	m, _ := app.NewMaintainer(maintenanceFixture{}, app.MaintenanceOptions{})
	r, err := app.NewRunner(pagedAccounts(nil), eligibleFor("instance"), dispatchFixture(func(context.Context, d.ClaimRequest) (int, error) { return 0, nil }), m, app.RunnerOptions{InstanceID: "instance"})
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err = m.Run(canceled); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err = r.Run(ctx); err != app.ErrRuntimeStarted {
		t.Fatalf("reused maintainer: %v", err)
	}
}

func TestTwoRunnersCannotShareOneMaintenanceLifecycle(t *testing.T) {
	m, _ := app.NewMaintainer(maintenanceFixture{}, app.MaintenanceOptions{})
	newRunner := func() *app.Runner {
		r, err := app.NewRunner(pagedAccounts(nil), eligibleFor("instance"), dispatchFixture(func(context.Context, d.ClaimRequest) (int, error) { return 0, nil }), m, app.RunnerOptions{InstanceID: "instance", PollInterval: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	a, b := newRunner(), newRunner()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan error, 2)
	go func() { results <- a.Run(ctx) }()
	go func() { results <- b.Run(ctx) }()
	if err := receive(t, results); err != app.ErrRuntimeStarted {
		t.Fatalf("expected one rejected lifecycle, got %v", err)
	}
	cancel()
	if err := receive(t, results); err != nil {
		t.Fatal(err)
	}
	if a.Snapshot().Running || b.Snapshot().Running {
		t.Fatal("runner not stopped")
	}
}

func TestRunnerMaintenanceContinuesWithNoSendersAndBlockedDiscovery(t *testing.T) {
	swept := make(chan struct{}, 8)
	m, _ := app.NewMaintainer(maintenanceFixture{expire: func(context.Context, int) (int, error) { swept <- struct{}{}; return 0, nil }}, app.MaintenanceOptions{PollInterval: time.Millisecond})
	querying := make(chan struct{}, 1)
	r, err := app.NewRunner(dueFixture(func(ctx context.Context, q app.DueAccountQuery) (app.DueAccountPage, error) {
		querying <- struct{}{}
		<-ctx.Done()
		return app.DueAccountPage{}, ctx.Err()
	}), eligibilityFixture(func(context.Context, app.AccountKey) (app.SendEligibility, error) {
		t.Error("no accounts")
		return app.SendEligibility{}, nil
	}), dispatchFixture(func(context.Context, d.ClaimRequest) (int, error) { t.Error("no sender"); return 0, nil }), m, app.RunnerOptions{InstanceID: "instance"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	receive(t, querying)
	receive(t, swept)
	receive(t, swept)
	cancel()
	if err = receive(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRetriesDiscoveryAndDispatchErrorsWithoutLeakingThem(t *testing.T) {
	var queries, calls atomic.Int32
	m, _ := app.NewMaintainer(maintenanceFixture{}, app.MaintenanceOptions{})
	good := make(chan struct{}, 1)
	r, err := app.NewRunner(dueFixture(func(ctx context.Context, q app.DueAccountQuery) (app.DueAccountPage, error) {
		if queries.Add(1) < 3 {
			return app.DueAccountPage{}, fmt.Errorf("secret dependency text")
		}
		return pagedAccounts(map[string][]string{"telegram": {"account"}})(ctx, q)
	}), eligibleFor("instance"), dispatchFixture(func(context.Context, d.ClaimRequest) (int, error) {
		if calls.Add(1) == 1 {
			return 0, fmt.Errorf("secret provider text")
		}
		select {
		case good <- struct{}{}:
		default:
		}
		return 1, nil
	}), m, app.RunnerOptions{InstanceID: "instance", Providers: []string{"telegram"}, PollInterval: time.Millisecond, MaxPagesPerTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	receive(t, good)
	cancel()
	if err = receive(t, done); err != nil {
		t.Fatal(err)
	}
	if r.Snapshot().DispatchFailures != 1 || r.Snapshot().Dispatched < 2 {
		t.Fatalf("snapshot %+v", r.Snapshot())
	}
}

func TestRunnerDrainTimeoutReportsStillActiveWorkUntilActualCompletion(t *testing.T) {
	m, _ := app.NewMaintainer(maintenanceFixture{}, app.MaintenanceOptions{})
	started := make(chan struct{})
	release := make(chan struct{})
	r, err := app.NewRunner(pagedAccounts(map[string][]string{"telegram": {"a"}}), eligibleFor("instance"), dispatchFixture(func(context.Context, d.ClaimRequest) (int, error) { close(started); <-release; return 0, nil }), m, app.RunnerOptions{InstanceID: "instance", Providers: []string{"telegram"}, PollInterval: time.Millisecond, DrainTimeout: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	receive(t, started)
	cancel()
	if err = receive(t, done); err != app.ErrRuntimeDrainTimeout {
		t.Fatalf("run %v", err)
	}
	short, stop := context.WithTimeout(context.Background(), time.Millisecond)
	if err = r.Drain(short); err != app.ErrRuntimeDrainTimeout {
		t.Errorf("false drain %v", err)
	}
	stop()
	if r.Snapshot().InFlight != 1 {
		t.Error("hidden uncooperative dispatch")
	}
	close(release)
	wait, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if err = r.Drain(wait); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerLifecycleAndNilContexts(t *testing.T) {
	m, _ := app.NewMaintainer(maintenanceFixture{}, app.MaintenanceOptions{})
	r, err := app.NewRunner(pagedAccounts(nil), eligibleFor("instance"), dispatchFixture(func(context.Context, d.ClaimRequest) (int, error) { return 0, nil }), m, app.RunnerOptions{InstanceID: "instance"})
	if err != nil {
		t.Fatal(err)
	}
	if err = r.Run(nil); err != d.ErrInvalid {
		t.Fatal(err)
	}
	if err = r.Drain(nil); err != d.ErrInvalid {
		t.Fatal(err)
	}
	if err = r.Drain(context.Background()); err != d.ErrInvalid {
		t.Fatal(err)
	}
	r.Quiesce()
	r.Quiesce()
	if err = r.Run(context.Background()); err != app.ErrRuntimeStopped {
		t.Fatal(err)
	}
	if err = r.Run(context.Background()); err != app.ErrRuntimeStarted {
		t.Fatal(err)
	}
	if err = r.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerDoesNotDispatchExpiredEligibilityResult(t *testing.T) {
	m, _ := app.NewMaintainer(maintenanceFixture{}, app.MaintenanceOptions{})
	inspected := make(chan struct{}, 8)
	var calls atomic.Int32
	r, err := app.NewRunner(pagedAccounts(map[string][]string{"telegram": {"a"}}), eligibilityFixture(func(ctx context.Context, k app.AccountKey) (app.SendEligibility, error) {
		<-ctx.Done()
		inspected <- struct{}{}
		return app.SendEligibility{Eligible: true}, nil
	}), dispatchFixture(func(context.Context, d.ClaimRequest) (int, error) { calls.Add(1); return 0, nil }), m, app.RunnerOptions{InstanceID: "instance", Providers: []string{"telegram"}, PollInterval: time.Millisecond, OperationTimeout: 5 * time.Millisecond, MaxPagesPerTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	receive(t, inspected)
	receive(t, inspected)
	cancel()
	if err = receive(t, done); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 0 {
		t.Fatalf("expired pre-check admitted %d dispatches", calls.Load())
	}
}

func TestRunnerConstructorBoundsAndTypedNilPorts(t *testing.T) {
	makeRunner := func(o app.RunnerOptions) (*app.Runner, error) {
		m, _ := app.NewMaintainer(maintenanceFixture{}, app.MaintenanceOptions{})
		return app.NewRunner(pagedAccounts(nil), eligibleFor("instance"), dispatchFixture(func(context.Context, d.ClaimRequest) (int, error) { return 0, nil }), m, o)
	}
	for name, edit := range map[string]func(*app.RunnerOptions){
		"missing_instance": func(o *app.RunnerOptions) { o.InstanceID = "" }, "invalid_instance": func(o *app.RunnerOptions) { o.InstanceID = "bad id" },
		"unknown_provider": func(o *app.RunnerOptions) { o.Providers = []string{"other"} }, "duplicate_provider": func(o *app.RunnerOptions) { o.Providers = []string{"telegram", "telegram"} }, "empty_provider": func(o *app.RunnerOptions) { o.Providers = []string{} },
		"negative_workers": func(o *app.RunnerOptions) { o.MaxWorkers = -1 }, "huge_workers": func(o *app.RunnerOptions) { o.MaxWorkers = 129 }, "huge_page": func(o *app.RunnerOptions) { o.PageSize = 1001 }, "negative_page": func(o *app.RunnerOptions) { o.PageSize = -1 }, "huge_pages": func(o *app.RunnerOptions) { o.MaxPagesPerTick = 101 },
		"short_lease": func(o *app.RunnerOptions) { o.ClaimLease = time.Millisecond }, "long_lease": func(o *app.RunnerOptions) { o.ClaimLease = time.Hour }, "short_poll": func(o *app.RunnerOptions) { o.PollInterval = time.Nanosecond }, "long_poll": func(o *app.RunnerOptions) { o.PollInterval = 2 * time.Hour },
		"long_operation": func(o *app.RunnerOptions) { o.OperationTimeout = 2 * time.Minute }, "long_drain": func(o *app.RunnerOptions) { o.DrainTimeout = time.Hour },
	} {
		t.Run(name, func(t *testing.T) {
			o := app.RunnerOptions{InstanceID: "instance"}
			edit(&o)
			if _, err := makeRunner(o); err != d.ErrInvalid {
				t.Fatalf("error %v", err)
			}
		})
	}
	m, _ := app.NewMaintainer(maintenanceFixture{}, app.MaintenanceOptions{})
	var reader dueFixture
	if _, err := app.NewRunner(reader, eligibleFor("instance"), dispatchFixture(func(context.Context, d.ClaimRequest) (int, error) { return 0, nil }), m, app.RunnerOptions{InstanceID: "instance"}); err != d.ErrUnavailable {
		t.Fatal(err)
	}
}

func TestRunnerCountsExpiredDispatchReturnAsFailure(t *testing.T) {
	var called atomic.Bool
	m, _ := app.NewMaintainer(maintenanceFixture{}, app.MaintenanceOptions{})
	ended := make(chan struct{})
	r, err := app.NewRunner(dueFixture(func(ctx context.Context, q app.DueAccountQuery) (app.DueAccountPage, error) {
		if called.Load() {
			return app.DueAccountPage{Exhausted: true}, nil
		}
		return pagedAccounts(map[string][]string{"telegram": {"a"}})(ctx, q)
	}), eligibleFor("instance"), dispatchFixture(func(ctx context.Context, q d.ClaimRequest) (int, error) {
		called.Store(true)
		<-ctx.Done()
		close(ended)
		return 1, nil
	}), m, app.RunnerOptions{InstanceID: "instance", Providers: []string{"telegram"}, PollInterval: time.Millisecond, OperationTimeout: 5 * time.Millisecond, MaxPagesPerTick: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	receive(t, ended)
	until := time.Now().Add(time.Second)
	for r.Snapshot().InFlight != 0 && time.Now().Before(until) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err = receive(t, done); err != nil {
		t.Fatal(err)
	}
	if r.Snapshot().DispatchFailures != 1 {
		t.Fatalf("expired dispatch %+v", r.Snapshot())
	}
}

// A provider remains due after each call: fairness must not depend on finishing
// its backlog, or on a worker happening to finish in the middle of a scan.
func TestRunnerDefaultPoolSharesCapacityAcrossContinuouslyDueProviders(t *testing.T) {
	m, _ := app.NewMaintainer(maintenanceFixture{}, app.MaintenanceOptions{})
	accounts := pagedAccounts(map[string][]string{
		"telegram": {"a", "b", "c", "d"},
		"wecom":    {"a", "b", "c", "d"},
	})
	var queries, telegram, wecom, active, maximum, duplicates atomic.Int32
	var inFlight sync.Map
	scanned := make(chan struct{})
	r, err := app.NewRunner(dueFixture(func(ctx context.Context, q app.DueAccountQuery) (app.DueAccountPage, error) {
		// Twelve full default-sized scans offer each provider ample opportunity. At
		// the next scan, keep discovery stopped while the test reads the outcome.
		if queries.Add(1) == 49 {
			close(scanned)
			<-ctx.Done()
			return app.DueAccountPage{}, ctx.Err()
		}
		return accounts.ListDueAccounts(ctx, q)
	}), eligibleFor("instance"), dispatchFixture(func(ctx context.Context, q d.ClaimRequest) (int, error) {
		key := app.AccountKey{Provider: q.Provider, AccountID: q.AccountID}
		if _, loaded := inFlight.LoadOrStore(key, struct{}{}); loaded {
			duplicates.Add(1)
		}
		defer inFlight.Delete(key)
		n := active.Add(1)
		defer active.Add(-1)
		for old := maximum.Load(); n > old && !maximum.CompareAndSwap(old, n); old = maximum.Load() {
		}
		if q.Limit != 1 {
			t.Error("Runner changed the per-account claim limit")
		}
		if q.Provider == "telegram" {
			telegram.Add(1)
		} else {
			wecom.Add(1)
		}
		// Calls overlap the rest of this scan but complete before the next poll;
		// all four slots therefore become available together at each new scan.
		timer := time.NewTimer(10 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
		return 1, nil
	}), m, app.RunnerOptions{InstanceID: "instance", PollInterval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	receive(t, scanned)
	r.Quiesce()
	cancel()
	if err := receive(t, done); err != nil {
		t.Fatal(err)
	}
	if err := r.Drain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if telegram.Load() < 8 || wecom.Load() < 8 {
		t.Fatalf("continuous backlog starved a provider: telegram=%d wecom=%d (need >=8 each)", telegram.Load(), wecom.Load())
	}
	if maximum.Load() > 4 || duplicates.Load() != 0 || active.Load() != 0 {
		t.Fatalf("bounded single-account dispatch lost: max=%d duplicates=%d active=%d", maximum.Load(), duplicates.Load(), active.Load())
	}
}
