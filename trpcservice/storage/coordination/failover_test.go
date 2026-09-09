package coordination

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type testAuthority struct {
	mu      sync.Mutex
	epoch   storage.Epoch
	bumps   int
	bumpErr error
}

func newTestAuthority() *testAuthority { return &testAuthority{epoch: 1} }
func (a *testAuthority) GetEpoch(context.Context, string, string) (storage.Epoch, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.epoch, nil
}
func (a *testAuthority) BumpEpoch(context.Context, string, string) (storage.Epoch, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.bumpErr != nil {
		return 0, a.bumpErr
	}
	a.epoch++
	a.bumps++
	return a.epoch, nil
}
func (a *testAuthority) ValidateEpoch(_ context.Context, _ string, _ string, epoch storage.Epoch) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if epoch != a.epoch {
		return storage.ErrEpochRejected
	}
	return nil
}
func (a *testAuthority) bumpCount() int { a.mu.Lock(); defer a.mu.Unlock(); return a.bumps }

type testBackend struct {
	name        storage.CoordinationBackend
	mu          sync.Mutex
	claimErr    error
	leaseErr    error
	claims      int
	leases      int
	completes   int
	renews      int
	probeErr    error
	activateErr error
	authority   *testAuthority
}

func (b *testBackend) Claim(context.Context, tenant.TenantContext, storage.DedupKey, time.Duration, string) (storage.Claim, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.claims++
	if b.claimErr != nil {
		return storage.Claim{}, b.claimErr
	}
	epoch := storage.Epoch(1)
	if b.authority != nil {
		epoch, _ = b.authority.GetEpoch(context.Background(), "tenant", "resource")
	}
	return storage.Claim{Status: storage.ClaimAcquired, OwnerID: "owner", FenceToken: 1, Backend: b.name, Epoch: epoch}, nil
}
func (b *testBackend) Complete(context.Context, tenant.TenantContext, storage.DedupKey, string, string, storage.OperationGuard) error {
	b.mu.Lock()
	b.completes++
	b.mu.Unlock()
	return nil
}
func (b *testBackend) Fail(context.Context, tenant.TenantContext, storage.DedupKey, string, storage.OperationGuard, bool) error {
	return nil
}
func (b *testBackend) Acquire(context.Context, tenant.TenantContext, string, string, time.Duration) (storage.Lease, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.leases++
	if b.leaseErr != nil {
		return storage.Lease{}, b.leaseErr
	}
	epoch := storage.Epoch(1)
	if b.authority != nil {
		epoch, _ = b.authority.GetEpoch(context.Background(), "tenant", "resource")
	}
	return storage.Lease{TenantID: "tenant", SessionID: "resource", ResourceID: "resource", OwnerID: "owner", FenceToken: 1, Backend: b.name, Epoch: epoch, ExpiresAt: time.Now().Add(time.Second)}, nil
}
func (b *testBackend) Renew(context.Context, tenant.TenantContext, storage.Lease, time.Duration) (storage.Lease, error) {
	b.mu.Lock()
	b.renews++
	b.mu.Unlock()
	epoch := storage.Epoch(1)
	if b.authority != nil {
		epoch, _ = b.authority.GetEpoch(context.Background(), "tenant", "resource")
	}
	return storage.Lease{TenantID: "tenant", SessionID: "resource", ResourceID: "resource", OwnerID: "owner", FenceToken: 1, Backend: b.name, Epoch: epoch, ExpiresAt: time.Now().Add(time.Second)}, nil
}
func (b *testBackend) Release(context.Context, tenant.TenantContext, storage.Lease) error { return nil }
func (b *testBackend) Validate(context.Context, tenant.TenantContext, storage.Lease) error {
	return nil
}
func (b *testBackend) endpoint() Endpoint {
	return Endpoint{Name: b.name, Claims: b, Leases: b, Probe: func(context.Context) error { return b.probeErr }, Activate: func(context.Context, storage.Epoch) error { return b.activateErr }}
}

func newFailoverHarness(t *testing.T) (*FailoverStore, *testAuthority, *testBackend, *testBackend, tenant.TenantContext) {
	t.Helper()
	authority := newTestAuthority()
	primary := &testBackend{name: storage.BackendRedis, authority: authority}
	secondary := &testBackend{name: storage.BackendPostgres, authority: authority}
	tc := tenant.TenantContext{TenantID: "tenant", Channel: "web", BindingID: "binding"}
	store := NewFailoverStoreWithAuthority(primary.endpoint(), secondary.endpoint(), authority, tc.TenantID, "resource", Config{Now: time.Now})
	if err := store.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	return store, authority, primary, secondary, tc
}

func testKey() storage.DedupKey {
	return storage.DedupKey{TenantID: "tenant", Channel: "web", BindingID: "binding", ExternalMessageID: "message"}
}

func automaticQuarantineStore(t *testing.T) (*FailoverStore, *testAuthority, *testBackend, *testBackend, tenant.TenantContext) {
	store, authority, primary, secondary, tc := newFailoverHarness(t)
	primary.claimErr = storage.ErrBackendUnavailable
	if _, err := store.Claim(context.Background(), tc, testKey(), time.Second, "owner"); !errors.Is(err, storage.ErrOperationAmbiguous) {
		t.Fatalf("quarantine error=%v", err)
	}
	return store, authority, primary, secondary, tc
}

func quarantineStore(t *testing.T) (*FailoverStore, *testAuthority, *testBackend, *testBackend, tenant.TenantContext) {
	store, authority, primary, secondary, tc := newFailoverHarness(t)
	primary.claimErr = storage.ErrBackendUnavailable
	if _, err := store.Claim(context.Background(), tc, testKey(), time.Second, "owner"); !errors.Is(err, storage.ErrOperationAmbiguous) {
		t.Fatalf("quarantine error=%v", err)
	}
	return store, authority, primary, secondary, tc
}

func TestFailoverStoreAutomaticallySwitchesAfterBreakerOpen(t *testing.T) {
	store, authority, _, secondary, tc := automaticQuarantineStore(t)
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if epoch, err := store.AutomaticSwitch(context.Background()); err == nil && epoch == 2 {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 || authority.bumpCount() != 1 {
		t.Fatalf("successes=%d bumps=%d", successes.Load(), authority.bumpCount())
	}
	if snapshot := store.Snapshot(); snapshot.State != ActiveSecondary || snapshot.ActiveBackend != storage.BackendPostgres || snapshot.Epoch != 2 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	if _, err := store.Claim(context.Background(), tc, testKey(), time.Second, "owner"); err != nil {
		t.Fatal(err)
	}
	if secondary.claims != 1 {
		t.Fatalf("secondary claims=%d", secondary.claims)
	}
}

func TestAutomaticSwitchRejectsNonAvailabilityErrors(t *testing.T) {
	store, authority, primary, _, tc := newFailoverHarness(t)
	for _, injected := range []error{storage.ErrFenceRejected, storage.ErrEpochRejected, storage.ErrTenantMismatch, storage.ErrInvalidArgument, storage.ErrAlreadyClaimed, context.Canceled} {
		primary.claimErr = injected
		if _, err := store.Claim(context.Background(), tc, testKey(), time.Second, "owner"); !errors.Is(err, injected) {
			t.Fatalf("injected=%v err=%v", injected, err)
		}
		if _, err := store.AutomaticSwitch(context.Background()); !errors.Is(err, storage.ErrConflict) {
			t.Fatalf("injected=%v switch=%v", injected, err)
		}
		if store.State() != ActivePrimary {
			t.Fatalf("injected=%v state=%s", injected, store.State())
		}
	}
	if authority.bumpCount() != 0 {
		t.Fatalf("bumps=%d", authority.bumpCount())
	}
}

func TestSwitchFailureFailsClosed(t *testing.T) {
	store, authority, _, secondary, _ := automaticQuarantineStore(t)
	authority.bumpErr = errors.New("authority unavailable")
	if _, err := store.AutomaticSwitch(context.Background()); err == nil || store.State() != Blocked {
		t.Fatalf("state=%s err=%v", store.State(), err)
	}
	if authority.bumpCount() != 0 || secondary.leases != 0 {
		t.Fatalf("bumps=%d leases=%d", authority.bumpCount(), secondary.leases)
	}
	if _, err := store.AutomaticSwitch(context.Background()); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("repeat switch=%v", err)
	}

	activationStore, activationAuthority, _, activationSecondary, _ := automaticQuarantineStore(t)
	activationSecondary.activateErr = storage.ErrBackendUnavailable
	if _, err := activationStore.AutomaticSwitch(context.Background()); err == nil || activationStore.State() != Blocked {
		t.Fatalf("activation state=%s err=%v", activationStore.State(), err)
	}
	if activationAuthority.bumpCount() != 1 || activationSecondary.leases != 0 {
		t.Fatalf("activation bumps=%d leases=%d", activationAuthority.bumpCount(), activationSecondary.leases)
	}
}

func TestCrossBackendNoDoubleOwnerAfterSwitch(t *testing.T) {
	store, _, primary, _, tc := newFailoverHarness(t)
	oldLease, err := store.Acquire(context.Background(), tc, "resource", "old-owner", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	primary.leaseErr = storage.ErrBackendUnavailable
	if _, err := store.Acquire(context.Background(), tc, "resource", "trigger", time.Second); !errors.Is(err, storage.ErrOperationAmbiguous) {
		t.Fatal(err)
	}
	if _, err := store.AutomaticSwitch(context.Background()); err != nil {
		t.Fatal(err)
	}
	newLease, err := store.Acquire(context.Background(), tc, "resource", "new-owner", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if newLease.Backend != storage.BackendPostgres || newLease.Epoch != 2 {
		t.Fatalf("new lease=%+v", newLease)
	}
	if _, err := store.Renew(context.Background(), tc, oldLease, time.Second); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old renew=%v", err)
	}
	if err := store.Release(context.Background(), tc, oldLease); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old release=%v", err)
	}
	if err := store.Validate(context.Background(), tc, oldLease); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old validate=%v", err)
	}
}

func TestFailoverStoreStartsWithPrimary(t *testing.T) {
	store, _, primary, secondary, tc := newFailoverHarness(t)
	claim, err := store.Claim(context.Background(), tc, testKey(), time.Second, "owner")
	if err != nil {
		t.Fatal(err)
	}
	if claim.Backend != storage.BackendRedis || store.State() != ActivePrimary {
		t.Fatalf("claim=%+v state=%s", claim, store.State())
	}
	if primary.claims != 1 || secondary.claims != 0 {
		t.Fatalf("primary=%d secondary=%d", primary.claims, secondary.claims)
	}
}

func TestFailoverStoreQuarantinesOnlyUnavailableErrors(t *testing.T) {
	store, _, primary, _, tc := newFailoverHarness(t)
	for _, err := range []error{storage.ErrFenceRejected, storage.ErrEpochRejected, storage.ErrTenantMismatch, storage.ErrInvalidArgument, storage.ErrAlreadyClaimed} {
		primary.claimErr = err
		if _, got := store.Claim(context.Background(), tc, testKey(), time.Second, "owner"); !errors.Is(got, err) {
			t.Fatalf("input=%v got=%v", err, got)
		}
		if store.State() != ActivePrimary {
			t.Fatalf("input=%v state=%s", err, store.State())
		}
	}
	primary.claimErr = storage.ErrBackendUnavailable
	if _, err := store.Claim(context.Background(), tc, testKey(), time.Second, "owner"); !errors.Is(err, storage.ErrOperationAmbiguous) {
		t.Fatal(err)
	}
	if store.State() != Quarantined {
		t.Fatalf("state=%s", store.State())
	}
}

func TestFailoverStoreSwitchBumpsEpochOnce(t *testing.T) {
	store, authority, _, _, tc := quarantineStore(t)
	epoch, err := store.Switch(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if epoch != 2 || authority.bumpCount() != 1 || store.State() != ActiveSecondary {
		t.Fatalf("epoch=%d bumps=%d state=%s", epoch, authority.bumpCount(), store.State())
	}
	secondaryClaim, err := store.Claim(context.Background(), tc, testKey(), time.Second, "owner")
	if err != nil {
		t.Fatal(err)
	}
	if secondaryClaim.Backend != storage.BackendPostgres || secondaryClaim.Epoch != 2 {
		t.Fatalf("claim=%+v", secondaryClaim)
	}
	if _, err := store.Switch(context.Background()); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("repeat switch=%v", err)
	}
	if authority.bumpCount() != 1 {
		t.Fatalf("bumps=%d", authority.bumpCount())
	}
}

func TestFailoverStoreRejectsOldEpochAfterSwitch(t *testing.T) {
	store, _, primary, _, tc := newFailoverHarness(t)
	oldClaim, err := store.Claim(context.Background(), tc, testKey(), time.Second, "owner")
	if err != nil {
		t.Fatal(err)
	}
	oldGuard := storage.OperationGuard{Backend: oldClaim.Backend, Epoch: oldClaim.Epoch, OwnerID: oldClaim.OwnerID, FenceToken: oldClaim.FenceToken}
	oldLease, err := store.Acquire(context.Background(), tc, "resource", "owner", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	primary.claimErr = storage.ErrBackendUnavailable
	if _, err := store.Claim(context.Background(), tc, testKey(), time.Second, "owner"); !errors.Is(err, storage.ErrOperationAmbiguous) {
		t.Fatal(err)
	}
	if _, err := store.Switch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(context.Background(), tc, testKey(), "owner", "response", oldGuard); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old complete=%v", err)
	}
	if err := store.Fail(context.Background(), tc, testKey(), "owner", oldGuard, true); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old fail=%v", err)
	}
	if _, err := store.Renew(context.Background(), tc, oldLease, time.Second); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old renew=%v", err)
	}
	if err := store.Release(context.Background(), tc, oldLease); !errors.Is(err, storage.ErrEpochRejected) {
		t.Fatalf("old release=%v", err)
	}
}

func TestFailoverStoreDoesNotActivateRecoveredBackendDirectly(t *testing.T) {
	store, _, primary, _, tc := quarantineStore(t)
	if _, err := store.Switch(context.Background()); err != nil {
		t.Fatal(err)
	}
	primary.probeErr = errors.New("probe failed")
	if err := store.Probe(context.Background()); err == nil || store.State() != Quarantined {
		t.Fatalf("failed probe state=%s err=%v", store.State(), err)
	}
	primary.probeErr = nil
	if err := store.Probe(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.State() != Recovering {
		t.Fatalf("state=%s", store.State())
	}
	if _, err := store.Claim(context.Background(), tc, testKey(), time.Second, "owner"); !errors.Is(err, storage.ErrBackendUnavailable) {
		t.Fatalf("claim during recovery=%v", err)
	}
	before := store.Epoch()
	after, err := store.Recover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if store.State() != ActivePrimary || after <= before {
		t.Fatalf("before=%d after=%d state=%s", before, after, store.State())
	}
}

func TestCircuitBreakerClassification(t *testing.T) {
	now := time.Now()
	clock := now
	breaker := NewCircuitBreaker(BreakerConfig{FailureThreshold: 2, ProtectionWindow: time.Second, Now: func() time.Time { return clock }})
	for _, err := range []error{storage.ErrFenceRejected, storage.ErrEpochRejected, storage.ErrTenantMismatch, storage.ErrInvalidArgument, storage.ErrAlreadyClaimed, context.Canceled} {
		if got := breaker.RecordFailure(err); got != FailureNone {
			t.Fatalf("error=%v class=%s", err, got)
		}
		if breaker.State() != BreakerClosed {
			t.Fatalf("error=%v state=%s", err, breaker.State())
		}
	}
	if got := breaker.RecordFailure(storage.ErrBackendUnavailable); got != FailureBackend {
		t.Fatalf("class=%s", got)
	}
	if breaker.State() != BreakerClosed {
		t.Fatalf("after first failure state=%s", breaker.State())
	}
	if got := breaker.RecordFailure(context.DeadlineExceeded); got != FailureBackend {
		t.Fatalf("timeout class=%s", got)
	}
	if breaker.State() != BreakerOpen {
		t.Fatalf("state=%s", breaker.State())
	}
	if err := breaker.AllowBusiness(); !errors.Is(err, storage.ErrBackendUnavailable) {
		t.Fatalf("allow open=%v", err)
	}
	if err := breaker.BeginProbe(); !errors.Is(err, storage.ErrBackendUnavailable) {
		t.Fatalf("early probe=%v", err)
	}
	clock = clock.Add(time.Second)
	if err := breaker.BeginProbe(); err != nil {
		t.Fatal(err)
	}
	if err := breaker.BeginProbe(); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("second probe=%v", err)
	}
	if err := breaker.RecordProbeSuccess(); err != nil {
		t.Fatal(err)
	}
	if breaker.State() != BreakerClosed {
		t.Fatalf("recovered state=%s", breaker.State())
	}
}

func TestConcurrentHalfOpenProbe(t *testing.T) {
	clock := time.Now()
	breaker := NewCircuitBreaker(BreakerConfig{FailureThreshold: 1, Now: func() time.Time { return clock }})
	breaker.RecordFailure(storage.ErrBackendUnavailable)
	var success atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := breaker.BeginProbe(); err == nil {
				success.Add(1)
			}
		}()
	}
	wg.Wait()
	if success.Load() != 1 || !breaker.Snapshot().ProbeInFlight {
		t.Fatalf("success=%d snapshot=%+v", success.Load(), breaker.Snapshot())
	}
}

func TestFailoverStoreConcurrentTransitions(t *testing.T) {
	store, authority, _, _, _ := quarantineStore(t)
	var successes atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.Switch(context.Background()); err == nil {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()
	if successes.Load() != 1 || authority.bumpCount() != 1 || store.State() != ActiveSecondary {
		t.Fatalf("successes=%d bumps=%d state=%s", successes.Load(), authority.bumpCount(), store.State())
	}
}
