package admission

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNewValidation(t *testing.T) {
	for _, bad := range [][3]int{
		{0, 1, 1}, {1, 0, 1}, {1, 1, 0},
		{-1, 1, 1}, {1, -1, 1}, {1, 1, -1},
		{MaxSlots + 1, 1, 1}, {1, 2, 1}, {1, 1, 2},
	} {
		if _, err := New(bad[0], bad[1], bad[2]); err == nil {
			t.Fatalf("invalid budget accepted: %v", bad)
		}
	}
	if _, err := New(8, 4, 2); err != nil {
		t.Fatalf("valid budget rejected: %v", err)
	}
}

func TestGlobalCapFastReject(t *testing.T) {
	gate, err := New(2, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	r1, err := gate.Acquire(context.Background(), "t1", "b1")
	if err != nil {
		t.Fatal(err)
	}
	r2, err := gate.Acquire(context.Background(), "t2", "b2")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Acquire(context.Background(), "t3", "b3"); !errors.Is(err, ErrCapacityExhausted) {
		t.Fatalf("global cap not enforced: %v", err)
	}
	r1()
	r2()
	if gate.ActiveGlobal() != 0 {
		t.Fatalf("active after release: %d", gate.ActiveGlobal())
	}
	if _, err := gate.Acquire(context.Background(), "t3", "b3"); err != nil {
		t.Fatalf("acquire after release failed: %v", err)
	}
}

func TestPerTenantCapNoisyNeighborIsolation(t *testing.T) {
	gate, err := New(8, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	a1, _ := gate.Acquire(context.Background(), "tenant-a", "b")
	a2, _ := gate.Acquire(context.Background(), "tenant-a", "b")
	if _, err := gate.Acquire(context.Background(), "tenant-a", "b"); !errors.Is(err, ErrCapacityExhausted) {
		t.Fatalf("per-tenant cap not enforced")
	}
	// Noisy tenant A is saturated; tenant B must still be admitted.
	b1, err := gate.Acquire(context.Background(), "tenant-b", "b")
	if err != nil {
		t.Fatalf("noisy neighbor blocked tenant B: %v", err)
	}
	b1()
	a1()
	a2()
}

func TestPerBindingCap(t *testing.T) {
	gate, err := New(8, 4, 1)
	if err != nil {
		t.Fatal(err)
	}
	b1, _ := gate.Acquire(context.Background(), "t", "binding-1")
	if _, err := gate.Acquire(context.Background(), "t", "binding-1"); !errors.Is(err, ErrCapacityExhausted) {
		t.Fatalf("per-binding cap not enforced")
	}
	// A different binding of the same tenant is still admitted.
	b2, err := gate.Acquire(context.Background(), "t", "binding-2")
	if err != nil {
		t.Fatalf("second binding rejected: %v", err)
	}
	b1()
	b2()
}

func TestContextCancellationRejected(t *testing.T) {
	gate, _ := New(1, 1, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := gate.Acquire(ctx, "t", "b"); !errors.Is(err, ErrContextDone) {
		t.Fatalf("cancelled context admitted: %v", err)
	}
	if gate.ActiveGlobal() != 0 {
		t.Fatalf("slot held for cancelled context")
	}
	deadline := context.Background()
	deadlineCtx, deadlineCancel := context.WithDeadline(deadline, time.Now().Add(-time.Second))
	defer deadlineCancel()
	if _, err := gate.Acquire(deadlineCtx, "t", "b"); !errors.Is(err, ErrContextDone) {
		t.Fatalf("expired context admitted: %v", err)
	}
}

func TestReleaseIdempotentAndPanicSafe(t *testing.T) {
	gate, _ := New(1, 1, 1)
	release, err := gate.Acquire(context.Background(), "t", "b")
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer func() {
			if recover() != nil {
				t.Fatalf("release panicked")
			}
		}()
		release()
		release() // duplicate
		release() // third
	}()
	if got := gate.ActiveGlobal(); got != 0 {
		t.Fatalf("active after duplicate release: %d", got)
	}
	if got := gate.ActiveTenant("t"); got != 0 {
		t.Fatalf("tenant active after duplicate release: %d", got)
	}
	if got := gate.ActiveBinding("t", "b"); got != 0 {
		t.Fatalf("binding active after duplicate release: %d", got)
	}
	// Slot is usable again: no leak, no negative counts.
	if _, err := gate.Acquire(context.Background(), "t", "b"); err != nil {
		t.Fatalf("reacquire after duplicate release: %v", err)
	}
}

func TestConcurrentAcquireReleaseWithinBounds(t *testing.T) {
	gate, _ := New(16, 8, 4)
	var wg sync.WaitGroup
	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				release, err := gate.Acquire(context.Background(), "tenant", "binding")
				if err != nil {
					continue
				}
				if gate.ActiveGlobal() > 16 || gate.ActiveTenant("tenant") > 8 || gate.ActiveBinding("tenant", "binding") > 4 {
					t.Errorf("active count above configured bound")
				}
				release()
			}
		}(worker)
	}
	wg.Wait()
	global, tenant, binding := gate.HighWater()
	if global > 16 || tenant > 8 || binding > 4 {
		t.Fatalf("high water above bounds: global=%d tenant=%d binding=%d", global, tenant, binding)
	}
	if gate.ActiveGlobal() != 0 || gate.TenantCount() != 0 {
		t.Fatalf("leaked slots: global=%d tenants=%d", gate.ActiveGlobal(), gate.TenantCount())
	}
	if global == 0 || tenant == 0 || binding == 0 {
		t.Fatalf("high water never observed: %d/%d/%d", global, tenant, binding)
	}
}

func TestEmptyKeysRejected(t *testing.T) {
	gate, _ := New(1, 1, 1)
	if _, err := gate.Acquire(context.Background(), "", "b"); err == nil {
		t.Fatalf("empty tenant admitted")
	}
	if _, err := gate.Acquire(context.Background(), "t", ""); err == nil {
		t.Fatalf("empty binding admitted")
	}
}

func TestNilGateFailsClosed(t *testing.T) {
	var gate *Gate
	if _, err := gate.Acquire(context.Background(), "t", "b"); err == nil {
		t.Fatalf("nil gate admitted")
	}
}
