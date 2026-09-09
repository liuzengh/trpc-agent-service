//
// Tencent is pleased to support the open source community by making trpc-agent-go available.
//
// Copyright (C) 2025 Tencent.  All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.
//

package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCoordinator_ReadinessFailsBeforeDrain pins the defect that
// http.Server.Shutdown alone does not fix: during the drain every dependency is
// still reachable, so a plain health check keeps reporting 200 and a load
// balancer keeps routing new traffic into a closing process.
func TestCoordinator_ReadinessFailsBeforeDrain(t *testing.T) {
	c := NewCoordinator()
	h := New(WithDrainState(c))

	if _, code := h.Report(context.Background()); code != http.StatusOK {
		t.Fatalf("expected ready before shutdown, got %d", code)
	}

	// Hold one unit of work so the drain cannot complete instantly. Readiness
	// must already be failing while that work is still running.
	done, err := c.Begin()
	if err != nil {
		t.Fatalf("Begin before shutdown: %v", err)
	}

	shutdownReturned := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Shutdown(ctx)
		close(shutdownReturned)
	}()

	// Wait for the drain flag to flip.
	deadline := time.After(time.Second)
	for !c.Draining() {
		select {
		case <-deadline:
			t.Fatal("shutdown did not set draining state")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	body, code := h.Report(context.Background())
	if code != http.StatusServiceUnavailable {
		t.Errorf("readiness during drain = %d, want 503 so the LB stops routing", code)
	}
	if body["status"] != "draining" {
		t.Errorf("status during drain = %v, want draining", body["status"])
	}

	done()
	<-shutdownReturned
}

// TestCoordinator_WaitsForInFlightWork asserts the drain does not return until
// tracked work finishes, which is what prevents killing a request mid-flight.
func TestCoordinator_WaitsForInFlightWork(t *testing.T) {
	c := NewCoordinator()

	done, err := c.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	var finished atomic.Bool
	go func() {
		time.Sleep(150 * time.Millisecond)
		finished.Store(true)
		done()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if !finished.Load() {
		t.Error("Shutdown returned before in-flight work completed")
	}
}

// TestCoordinator_RefusesNewWorkWhileDraining asserts new work is rejected once
// shutdown starts, so the drain is guaranteed to terminate.
func TestCoordinator_RefusesNewWorkWhileDraining(t *testing.T) {
	c := NewCoordinator()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if _, err := c.Begin(); !errors.Is(err, ErrShuttingDown) {
		t.Errorf("Begin after shutdown error = %v, want ErrShuttingDown", err)
	}
}

func TestCoordinator_BeginDrainRefusesNewWork(t *testing.T) {
	c := NewCoordinator()

	c.BeginDrain()
	if !c.Draining() {
		t.Fatal("BeginDrain did not publish draining state")
	}
	if _, err := c.Begin(); !errors.Is(err, ErrShuttingDown) {
		t.Fatalf("Begin after BeginDrain error=%v, want ErrShuttingDown", err)
	}
}

// TestCoordinator_DrainTimeoutReported asserts a stuck request surfaces as an
// error instead of hanging shutdown forever.
func TestCoordinator_DrainTimeoutReported(t *testing.T) {
	c := NewCoordinator()

	done, err := c.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer done()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	err = c.Shutdown(ctx)
	if err == nil {
		t.Fatal("expected drain timeout error for work that never finishes")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want wrapped DeadlineExceeded", err)
	}
}

// TestCoordinator_HooksRunInReverseOrder asserts teardown order, so a dependency
// is not closed before the things that use it.
func TestCoordinator_HooksRunInReverseOrder(t *testing.T) {
	c := NewCoordinator()

	var mu sync.Mutex
	var order []string
	record := func(name string) func(context.Context) error {
		return func(context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			order = append(order, name)
			return nil
		}
	}

	// Registration order mirrors cmd/worker: redis first, storage second.
	c.OnShutdown("redis", record("redis"))
	c.OnShutdown("storage", record("storage"))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if len(order) != 2 || order[0] != "storage" || order[1] != "redis" {
		t.Errorf("teardown order = %v, want [storage redis]: releasing leases writes through Redis", order)
	}
}

// TestCoordinator_HookErrorsDoNotStrandOtherHooks asserts one failing
// dependency cannot prevent the rest from being released.
func TestCoordinator_HookErrorsDoNotStrandOtherHooks(t *testing.T) {
	c := NewCoordinator()

	var closed atomic.Bool
	c.OnShutdown("first", func(context.Context) error {
		closed.Store(true)
		return nil
	})
	c.OnShutdown("second", func(context.Context) error { return errors.New("boom") })

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := c.Shutdown(ctx)
	if err == nil {
		t.Error("expected hook error to be reported")
	}
	if !closed.Load() {
		t.Error("remaining hook was stranded by an earlier failure")
	}
}

func TestCoordinator_HookPanicDoesNotStrandOtherHooks(t *testing.T) {
	c := NewCoordinator()
	var laterRan atomic.Bool
	c.OnShutdown("later", func(context.Context) error {
		laterRan.Store(true)
		return nil
	})
	c.OnShutdown("panicking", func(context.Context) error {
		panic("connection password must not escape")
	})

	err := c.Shutdown(context.Background())
	if !errors.Is(err, ErrShutdownHookPanic) {
		t.Fatalf("Shutdown error=%v, want ErrShutdownHookPanic", err)
	}
	if !laterRan.Load() {
		t.Fatal("cleanup hook after panic did not run")
	}

	// A later caller must observe completion rather than block behind a hook
	// that panicked in the owner call.
	if err := c.Shutdown(context.Background()); !errors.Is(err, ErrShutdownHookPanic) {
		t.Fatalf("second Shutdown error=%v, want ErrShutdownHookPanic", err)
	}
}

// TestCoordinator_ShutdownIsIdempotent guards against double-close panics when
// shutdown is triggered from more than one path.
func TestCoordinator_ShutdownIsIdempotent(t *testing.T) {
	c := NewCoordinator()

	var calls atomic.Int32
	c.OnShutdown("dep", func(context.Context) error {
		calls.Add(1)
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for i := 0; i < 3; i++ {
		if err := c.Shutdown(ctx); err != nil {
			t.Fatalf("Shutdown #%d: %v", i, err)
		}
	}

	if got := calls.Load(); got != 1 {
		t.Errorf("hook ran %d times, want exactly 1", got)
	}
}

func TestCoordinator_ConcurrentShutdownWaitsForOwnerAndPropagatesResult(t *testing.T) {
	c := NewCoordinator()
	hookStarted := make(chan struct{})
	releaseHook := make(chan struct{})
	wantErr := errors.New("cleanup failed")
	c.OnShutdown("blocked", func(context.Context) error {
		close(hookStarted)
		<-releaseHook
		return wantErr
	})

	ownerDone := make(chan error, 1)
	go func() {
		ownerDone <- c.Shutdown(context.Background())
	}()
	<-hookStarted

	concurrentDone := make(chan error, 1)
	go func() {
		concurrentDone <- c.Shutdown(context.Background())
	}()

	select {
	case err := <-concurrentDone:
		t.Fatalf("concurrent Shutdown returned before owner completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseHook)
	if err := <-ownerDone; !errors.Is(err, wantErr) {
		t.Fatalf("owner Shutdown error=%v, want %v", err, wantErr)
	}
	if err := <-concurrentDone; !errors.Is(err, wantErr) {
		t.Fatalf("concurrent Shutdown error=%v, want %v", err, wantErr)
	}
}

func TestCoordinator_ConcurrentShutdownHonorsCallerContext(t *testing.T) {
	c := NewCoordinator()
	hookStarted := make(chan struct{})
	releaseHook := make(chan struct{})
	c.OnShutdown("blocked", func(context.Context) error {
		close(hookStarted)
		<-releaseHook
		return nil
	})

	ownerDone := make(chan error, 1)
	go func() { ownerDone <- c.Shutdown(context.Background()) }()
	<-hookStarted

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := c.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("concurrent Shutdown error=%v, want context deadline", err)
	}
	close(releaseHook)
	if err := <-ownerDone; err != nil {
		t.Fatalf("owner Shutdown: %v", err)
	}
}

// TestCoordinator_MiddlewareRejectsNewRequestsDuringDrain asserts the HTTP layer
// returns 503 rather than accepting work into a closing process.
func TestCoordinator_MiddlewareRejectsNewRequestsDuringDrain(t *testing.T) {
	c := NewCoordinator()

	var served atomic.Int32
	h := c.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/process", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("before shutdown: got %d, want 200", rec.Code)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/process", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("during drain: got %d, want 503", rec.Code)
	}
	if rec.Header().Get("Connection") != "close" {
		t.Error("expected Connection: close so proxies stop reusing this connection")
	}
	if served.Load() != 1 {
		t.Errorf("handler ran %d times, want 1: request was admitted while draining", served.Load())
	}
}

// TestCoordinator_MiddlewareTrackedRequestBlocksDrain asserts a request already
// executing is waited for, end to end through the HTTP layer.
func TestCoordinator_MiddlewareTrackedRequestBlocksDrain(t *testing.T) {
	c := NewCoordinator()

	release := make(chan struct{})
	var completed atomic.Bool

	srv := httptest.NewServer(c.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		completed.Store(true)
		w.WriteHeader(http.StatusOK)
	})))
	defer srv.Close()

	reqDone := make(chan struct{})
	go func() {
		defer close(reqDone)
		resp, err := http.Get(srv.URL + "/process")
		if err == nil {
			resp.Body.Close()
		}
	}()

	// Give the handler time to enter the middleware and register itself.
	time.Sleep(100 * time.Millisecond)

	drainDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		drainDone <- c.Shutdown(ctx)
	}()

	// The drain must still be waiting while the handler is blocked.
	select {
	case err := <-drainDone:
		t.Fatalf("drain completed while a request was still executing: %v", err)
	case <-time.After(200 * time.Millisecond):
	}

	close(release)

	if err := <-drainDone; err != nil {
		t.Errorf("Shutdown: %v", err)
	}
	if !completed.Load() {
		t.Error("in-flight request did not complete")
	}
	<-reqDone
}

// TestCoordinator_ConcurrentBeginDuringShutdown exercises the race between
// admitting work and starting the drain. Every accepted unit must be waited for,
// and rejected callers must see ErrShuttingDown.
func TestCoordinator_ConcurrentBeginDuringShutdown(t *testing.T) {
	c := NewCoordinator()

	var admitted, rejected atomic.Int32
	var wg sync.WaitGroup

	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			done, err := c.Begin()
			if err != nil {
				rejected.Add(1)
				return
			}
			admitted.Add(1)
			time.Sleep(20 * time.Millisecond)
			done()
		}()
	}

	time.Sleep(5 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	wg.Wait()

	if admitted.Load()+rejected.Load() != 50 {
		t.Errorf("accounted %d of 50 callers", admitted.Load()+rejected.Load())
	}
	if _, err := c.Begin(); !errors.Is(err, ErrShuttingDown) {
		t.Errorf("post-shutdown Begin = %v, want ErrShuttingDown", err)
	}
}

// TestCoordinator_BeginShutdownRaceUnderStress is the regression test for the
// data race that `go test -race` found in the original WaitGroup-based
// implementation: Begin's wg.Add ran concurrently with waitForDrain's wg.Wait,
// which is a WaitGroup misuse that an atomic draining flag checked before Add
// cannot prevent.
//
// Many goroutines call Begin while Shutdown runs, so the interleaving is hit
// reliably. Under -race this fails if admission and drain accounting are ever
// split across separate synchronisation primitives again.
func TestCoordinator_BeginShutdownRaceUnderStress(t *testing.T) {
	const goroutines = 64

	c := NewCoordinator()

	var started sync.WaitGroup
	started.Add(goroutines)

	release := make(chan struct{})
	var accepted, refused atomic.Int64

	for i := 0; i < goroutines; i++ {
		go func() {
			started.Done()
			<-release // maximise the chance of colliding with Shutdown

			done, err := c.Begin()
			if err != nil {
				refused.Add(1)
				return
			}
			accepted.Add(1)
			time.Sleep(time.Millisecond)
			done()
		}()
	}

	started.Wait()
	close(release)

	// Let a portion of the goroutines get admitted before draining begins,
	// so the interesting interleaving (Begin racing an active drain) is
	// actually exercised rather than every caller being refused outright.
	deadline := time.Now().Add(time.Second)
	for accepted.Load() < goroutines/4 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Microsecond)
	}
	if accepted.Load() == 0 {
		t.Fatal("no goroutine was admitted before shutdown; test would prove nothing")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown reported an error: %v", err)
	}

	// Every goroutine must have reached a decision by the time Shutdown
	// returned; none may still be sitting between the flag check and accounting.
	waitDeadline := time.Now().Add(2 * time.Second)
	for accepted.Load()+refused.Load() < goroutines && time.Now().Before(waitDeadline) {
		time.Sleep(time.Millisecond)
	}

	total := accepted.Load() + refused.Load()
	if total != goroutines {
		t.Errorf("accounted for %d of %d goroutines", total, goroutines)
	}

	// Whatever the split, Shutdown must have waited for everything it admitted.
	if c.inFlight != 0 {
		t.Errorf("inFlight = %d after Shutdown returned, want 0", c.inFlight)
	}

	// Begin must refuse work once shutdown has completed.
	if _, err := c.Begin(); !errors.Is(err, ErrShuttingDown) {
		t.Errorf("Begin after Shutdown returned %v, want ErrShuttingDown", err)
	}

	t.Logf("admitted %d, refused %d", accepted.Load(), refused.Load())
}

// TestCoordinator_DrainWaitsForWorkAdmittedJustBeforeShutdown pins the ordering
// guarantee that matters in production: work admitted before draining begins is
// waited for, never abandoned.
func TestCoordinator_DrainWaitsForWorkAdmittedJustBeforeShutdown(t *testing.T) {
	c := NewCoordinator()

	done, err := c.Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	var finished atomic.Bool
	go func() {
		time.Sleep(200 * time.Millisecond)
		finished.Store(true)
		done()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	if !finished.Load() {
		t.Error("Shutdown returned before in-flight work completed")
	}
}
