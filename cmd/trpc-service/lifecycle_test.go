package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/outbox"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

type lifecycleHTTPStub struct {
	shutdown func(context.Context) error
	close    func() error
}

func (s *lifecycleHTTPStub) Shutdown(ctx context.Context) error {
	if s.shutdown != nil {
		return s.shutdown(ctx)
	}
	return nil
}

func (s *lifecycleHTTPStub) Close() error {
	if s.close != nil {
		return s.close()
	}
	return nil
}

type lifecycleRuntimeStub struct {
	draining atomic.Bool
	stops    atomic.Int32
	forces   atomic.Int32
	stop     func(context.Context) error
}

func (r *lifecycleRuntimeStub) BeginDraining() { r.draining.Store(true) }

func (r *lifecycleRuntimeStub) Stop(ctx context.Context) error {
	r.stops.Add(1)
	if r.stop != nil {
		return r.stop(ctx)
	}
	return nil
}

func (r *lifecycleRuntimeStub) ForceClose() { r.forces.Add(1) }

func TestProcessLifecycleGracefulSignalRunsOrderedCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverDraining := atomic.Bool{}
	order := atomic.Int32{}
	httpDone := make(chan struct{})
	serveDone := make(chan error, 1)
	runtime := &lifecycleRuntimeStub{}
	server := &lifecycleHTTPStub{
		shutdown: func(context.Context) error {
			if runtime.draining.Load() {
				order.CompareAndSwap(0, 1)
			} else {
				order.Store(-1)
			}
			if !serverDraining.Load() {
				order.Store(-1)
			}
			close(httpDone)
			serveDone <- http.ErrServerClosed
			return nil
		},
	}
	runtime.stop = func(context.Context) error {
		select {
		case <-httpDone:
			if order.Load() == 1 {
				order.Store(2)
			}
		default:
			order.Store(-1)
		}
		return nil
	}
	cancel()

	exitCode, err := runProcessLifecycle(ctx, lifecycleCoordinatorConfig{
		server: server, serveDone: serveDone, runtime: runtime,
		beginDraining:   func() { serverDraining.Store(true) },
		shutdownTimeout: time.Second, forceCloseTimeout: 50 * time.Millisecond,
	})
	if err != nil || exitCode != processExitOK {
		t.Fatalf("graceful lifecycle exit=%d err=%v", exitCode, err)
	}
	if order.Load() != 2 {
		t.Fatalf("cleanup order=%d, want draining -> HTTP -> runtime", order.Load())
	}
	if runtime.stops.Load() != 1 || runtime.forces.Load() != 0 {
		t.Fatalf("runtime stop calls=%d force calls=%d", runtime.stops.Load(), runtime.forces.Load())
	}
}

func TestProcessLifecycleServeFailureStillCleansRuntime(t *testing.T) {
	serveFailure := errors.New("synthetic serve failure")
	serveDone := make(chan error, 1)
	serveDone <- serveFailure
	runtime := &lifecycleRuntimeStub{}
	server := &lifecycleHTTPStub{shutdown: func(context.Context) error {
		serveDone <- http.ErrServerClosed
		return nil
	}}

	exitCode, err := runProcessLifecycle(context.Background(), lifecycleCoordinatorConfig{
		server: server, serveDone: serveDone, runtime: runtime,
		shutdownTimeout: time.Second, forceCloseTimeout: 50 * time.Millisecond,
	})
	if exitCode == processExitOK || err == nil || !errors.Is(err, errProcessServeFailure) {
		t.Fatalf("serve failure exit=%d err=%v", exitCode, err)
	}
	if runtime.stops.Load() != 1 {
		t.Fatalf("runtime cleanup calls=%d", runtime.stops.Load())
	}
	if strings.Contains(err.Error(), "synthetic serve failure") {
		t.Fatalf("serve failure leaked cause: %s", err)
	}
	if got := (&lifecycleError{phase: "test", code: "secret", cause: errors.New("secret")}).Error(); got != "lifecycle phase=test code=secret" {
		t.Fatalf("lifecycle error leaked cause: %s", got)
	}
}

func TestProcessLifecycleHTTPDeadlineStillStopsRuntime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serveDone := make(chan error, 1)
	closeCalls := atomic.Int32{}
	runtime := &lifecycleRuntimeStub{}
	server := &lifecycleHTTPStub{
		shutdown: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
		close: func() error {
			closeCalls.Add(1)
			return nil
		},
	}
	cancel()

	exitCode, err := runProcessLifecycle(ctx, lifecycleCoordinatorConfig{
		server: server, serveDone: serveDone, runtime: runtime,
		shutdownTimeout: 20 * time.Millisecond, forceCloseTimeout: 50 * time.Millisecond,
	})
	if exitCode != processExitForced || err == nil || !errors.Is(err, errProcessHTTPDrain) {
		t.Fatalf("deadline exit=%d err=%v", exitCode, err)
	}
	if runtime.stops.Load() != 1 {
		t.Fatal("runtime cleanup was skipped after HTTP deadline")
	}
	if closeCalls.Load() == 0 {
		t.Fatal("HTTP forced close was not requested")
	}
}

func TestProcessLifecycleSecondSignalForcesBoundedClose(t *testing.T) {
	force := make(chan struct{})
	close(force)
	serveDone := make(chan error, 1)
	closeCalls := atomic.Int32{}
	closed := make(chan struct{})
	var closeOnce sync.Once
	runtime := &lifecycleRuntimeStub{}
	server := &lifecycleHTTPStub{
		shutdown: func(context.Context) error {
			<-closed
			return nil
		},
		close: func() error {
			closeCalls.Add(1)
			closeOnce.Do(func() { close(closed) })
			return nil
		},
	}

	started := time.Now()
	exitCode, err := runProcessLifecycle(context.Background(), lifecycleCoordinatorConfig{
		server: server, serveDone: serveDone, runtime: runtime, forceSignal: force,
		shutdownTimeout: time.Minute, forceCloseTimeout: 30 * time.Millisecond,
	})
	if exitCode != processExitForced || err == nil || !errors.Is(err, errProcessForcedClose) {
		t.Fatalf("forced exit=%d err=%v", exitCode, err)
	}
	if time.Since(started) > time.Second {
		t.Fatalf("forced shutdown was not bounded: %s", time.Since(started))
	}
	if closeCalls.Load() == 0 || runtime.stops.Load() != 1 || runtime.forces.Load() != 1 {
		t.Fatalf("forced cleanup close=%d runtime stops=%d force calls=%d", closeCalls.Load(), runtime.stops.Load(), runtime.forces.Load())
	}
}

func TestClassifyRuntimeStopPreservesWorkerAndDispatcherErrors(t *testing.T) {
	input := errors.Join(worker.ErrDrainTimeout, outbox.ErrShutdownTimeout)
	classified := classifyRuntimeStop(input)
	if !errors.Is(classified, errProcessWorkerDrain) || !errors.Is(classified, errProcessDispatcherDrain) {
		t.Fatalf("classified error lost phase: %v", classified)
	}
}

func TestSecondSignalGateRequiresTwoSignals(t *testing.T) {
	signals := make(chan os.Signal, 2)
	force, stop := startSecondSignalGate(signals)
	defer stop()
	signals <- syscall.SIGTERM
	select {
	case <-force:
		t.Fatal("first signal forced shutdown")
	case <-time.After(20 * time.Millisecond):
	}
	signals <- syscall.SIGTERM
	select {
	case <-force:
	case <-time.After(time.Second):
		t.Fatal("second signal did not force shutdown")
	}
}
