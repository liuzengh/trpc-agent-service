package storage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type runnerFakeLeaseStore struct {
	mu        sync.Mutex
	calls     int
	active    int
	maxActive int
	renewFn   func(context.Context, Lease, time.Duration) (Lease, error)
}

func (s *runnerFakeLeaseStore) Acquire(context.Context, tenant.TenantContext, string, string, time.Duration) (Lease, error) {
	return Lease{}, nil
}
func (s *runnerFakeLeaseStore) Release(context.Context, tenant.TenantContext, Lease) error {
	return nil
}
func (s *runnerFakeLeaseStore) Validate(context.Context, tenant.TenantContext, Lease) error {
	return nil
}
func (s *runnerFakeLeaseStore) Renew(ctx context.Context, _ tenant.TenantContext, lease Lease, ttl time.Duration) (Lease, error) {
	s.mu.Lock()
	s.calls++
	s.active++
	if s.active > s.maxActive {
		s.maxActive = s.active
	}
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.active--; s.mu.Unlock() }()
	if s.renewFn != nil {
		return s.renewFn(ctx, lease, ttl)
	}
	lease.ExpiresAt = time.Now().Add(ttl)
	return lease, nil
}
func (s *runnerFakeLeaseStore) stats() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.maxActive
}

func testRunnerLease() Lease {
	return Lease{TenantID: "tenant", SessionID: "session", ResourceID: "resource", OwnerID: "owner", FenceToken: 7, Backend: BackendRedis, Epoch: 3, ExpiresAt: time.Now().Add(80 * time.Millisecond)}
}
func testRunnerTenant() tenant.TenantContext {
	return tenant.TenantContext{TenantID: "tenant", Channel: "web", BindingID: "binding"}
}
func waitRunner(t *testing.T, r *LeaseRenewalRunner) error {
	t.Helper()
	select {
	case <-r.Done():
		return r.Wait()
	case <-time.After(time.Second):
		t.Fatal("runner did not stop")
		return nil
	}
}

func TestLeaseRenewalRunnerRenewsBeforeExpiry(t *testing.T) {
	store := &runnerFakeLeaseStore{}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	initial := testRunnerLease()
	r, err := NewLeaseRenewalRunner(parent, store, testRunnerTenant(), initial, 80*time.Millisecond, 8*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		calls, _ := store.stats()
		if calls >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("renew did not occur twice")
		}
		time.Sleep(time.Millisecond)
	}
	latest := r.Lease()
	if !latest.ExpiresAt.After(initial.ExpiresAt) {
		t.Fatalf("expiry did not extend: initial=%s latest=%s", initial.ExpiresAt, latest.ExpiresAt)
	}
	if latest.OwnerID != initial.OwnerID || latest.FenceToken != initial.FenceToken || latest.Epoch != initial.Epoch {
		t.Fatalf("lease identity changed: initial=%+v latest=%+v", initial, latest)
	}
	callsBefore, max := store.stats()
	if max != 1 {
		t.Fatalf("overlapping renews=%d", max)
	}
	cancel()
	if err := waitRunner(t, r); !errors.Is(err, context.Canceled) {
		t.Fatalf("stop error=%v", err)
	}
	callsAfter, _ := store.stats()
	time.Sleep(25 * time.Millisecond)
	callsFinal, _ := store.stats()
	if callsAfter != callsFinal || callsBefore > callsAfter {
		t.Fatalf("renew after cancel: before=%d after=%d final=%d", callsBefore, callsAfter, callsFinal)
	}
}

func TestLeaseRenewalRunnerStopsOnContextCancel(t *testing.T) {
	store := &runnerFakeLeaseStore{}
	ctx, cancel := context.WithCancel(context.Background())
	r, err := NewLeaseRenewalRunner(ctx, store, testRunnerTenant(), testRunnerLease(), 80*time.Millisecond, 10*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := waitRunner(t, r); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	calls, _ := store.stats()
	time.Sleep(30 * time.Millisecond)
	after, _ := store.stats()
	if calls != after {
		t.Fatalf("calls continued after cancel: %d -> %d", calls, after)
	}
}

func TestLeaseRenewalRunnerStopsOnLeaseLost(t *testing.T) {
	store := &runnerFakeLeaseStore{renewFn: func(context.Context, Lease, time.Duration) (Lease, error) { return Lease{}, ErrLeaseLost }}
	var callbacks atomic.Int32
	r, err := NewLeaseRenewalRunner(context.Background(), store, testRunnerTenant(), testRunnerLease(), 80*time.Millisecond, 8*time.Millisecond, func(error) { callbacks.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	if err := waitRunner(t, r); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("error=%v", err)
	}
	if callbacks.Load() != 1 {
		t.Fatalf("callbacks=%d", callbacks.Load())
	}
	select {
	case err := <-r.Errors():
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("reported=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("missing error report")
	}
	calls, _ := store.stats()
	time.Sleep(25 * time.Millisecond)
	after, _ := store.stats()
	if calls != after {
		t.Fatalf("renew retried after lease loss")
	}
}

func TestLeaseRenewalRunnerStopsOnEpochRejected(t *testing.T) {
	store := &runnerFakeLeaseStore{renewFn: func(context.Context, Lease, time.Duration) (Lease, error) { return Lease{}, ErrEpochRejected }}
	r, err := NewLeaseRenewalRunner(context.Background(), store, testRunnerTenant(), testRunnerLease(), 80*time.Millisecond, 8*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	if err := waitRunner(t, r); !errors.Is(err, ErrEpochRejected) {
		t.Fatalf("error=%v", err)
	}
	calls, _ := store.stats()
	time.Sleep(25 * time.Millisecond)
	after, _ := store.stats()
	if calls != after {
		t.Fatal("renew continued after epoch rejection")
	}
}

func TestLeaseRenewalRunnerStopIsIdempotent(t *testing.T) {
	store := &runnerFakeLeaseStore{}
	r, err := NewLeaseRenewalRunner(context.Background(), store, testRunnerTenant(), testRunnerLease(), 80*time.Millisecond, 20*time.Millisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("first stop=%v", err)
	}
	if err := r.Stop(); !errors.Is(err, context.Canceled) {
		t.Fatalf("second stop=%v", err)
	}
	if err := r.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait=%v", err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	if err := r.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("repeat wait=%v", err)
	}
}
