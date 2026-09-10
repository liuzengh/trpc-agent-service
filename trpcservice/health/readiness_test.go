package health_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/health"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func TestMonitorCombinesLifecycleAndLatestDependencyCycle(t *testing.T) {
	lifecycle := worker.NewLifecycle()
	var backendReady atomic.Bool
	backendReady.Store(true)
	monitor, err := health.NewMonitor(lifecycle, []health.Dependency{
		{Name: "postgres", Probe: func(context.Context) error { return nil }},
		{Name: "object_store", Probe: func(context.Context) error {
			if backendReady.Load() {
				return nil
			}
			return runtime.ErrBackendUnavailable
		}},
	}, time.Second, time.Minute)
	if err != nil || monitor.Ready() {
		t.Fatalf("monitor=%#v ready=%t err=%v", monitor, monitor.Ready(), err)
	}
	if err := monitor.ProbeOnce(context.Background()); err != nil || monitor.Ready() {
		t.Fatalf("starting lifecycle became ready err=%v", err)
	}
	if err := lifecycle.MarkReady(); err != nil || !monitor.Ready() {
		t.Fatalf("ready=%t err=%v snapshot=%#v", monitor.Ready(), err, monitor.Snapshot())
	}
	backendReady.Store(false)
	if err := monitor.ProbeOnce(context.Background()); err != nil || monitor.Ready() {
		t.Fatalf("failed dependency remained ready err=%v", err)
	}
	lifecycle.BeginDrain()
	backendReady.Store(true)
	if err := monitor.ProbeOnce(context.Background()); err != nil || monitor.Ready() {
		t.Fatalf("draining lifecycle became ready err=%v", err)
	}
}

func TestMonitorFailsClosedBeforeProbeAndBoundsTimeout(t *testing.T) {
	lifecycle := worker.NewLifecycle()
	if err := lifecycle.MarkReady(); err != nil {
		t.Fatal(err)
	}
	monitor, err := health.NewMonitor(lifecycle, []health.Dependency{{Name: "blocked", Probe: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}}, 10*time.Millisecond, time.Minute)
	if err != nil || monitor.Ready() {
		t.Fatalf("ready=%t err=%v", monitor.Ready(), err)
	}
	start := time.Now()
	if err := monitor.ProbeOnce(context.Background()); err != nil || monitor.Ready() || time.Since(start) > time.Second {
		t.Fatalf("ready=%t elapsed=%v err=%v", monitor.Ready(), time.Since(start), err)
	}
	if err := monitor.ProbeOnce(canceledContext()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err=%v", err)
	}
}

func TestMonitorRejectsDuplicateDependencyNames(t *testing.T) {
	probe := func(context.Context) error { return nil }
	_, err := health.NewMonitor(worker.NewLifecycle(), []health.Dependency{{Name: "postgres", Probe: probe}, {Name: "postgres", Probe: probe}}, time.Second, time.Second)
	if !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("err=%v", err)
	}
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}
