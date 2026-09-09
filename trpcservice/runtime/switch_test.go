package runtime

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

// A failed v2 build must not disturb the cached v1 Bundle, but a request pinned
// to v2 must fail rather than silently execute with v1 behavior.
func TestManagerFailedSwitchKeepsPreviousBundle(t *testing.T) {
	var failBuild atomic.Bool
	manager, err := NewManager(func(snapshot config.RuntimeSnapshot) (Runtime, error) {
		if failBuild.Load() {
			return nil, errors.New("fixture: cannot build bundle")
		}
		return &fakeRuntime{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	v1 := runtimeSnapshot(t, "tenant-a", 1)
	lease, err := manager.Acquire(v1)
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()

	failBuild.Store(true)
	if fallback, err := manager.Acquire(runtimeSnapshot(t, "tenant-a", 2)); err == nil || fallback != nil {
		t.Fatalf("failed v2 build must reject the pinned request: lease=%v err=%v", fallback, err)
	}
	// v1 is still the head and the failed v2 pin did not poison the manager.
	again, err := manager.Acquire(v1)
	if err != nil {
		t.Fatalf("v1 must stay available after a failed switch: %v", err)
	}
	again.Release()

	failBuild.Store(false)
	upgraded, err := manager.Acquire(runtimeSnapshot(t, "tenant-a", 2))
	if err != nil {
		t.Fatalf("v2 must build once the factory recovers: %v", err)
	}
	upgraded.Release()
	if err := manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// A lease on the old version keeps the old Bundle alive; the old Bundle is
// closed exactly once the last in-flight request releases it.
func TestManagerRetiredBundleClosesAfterDrain(t *testing.T) {
	var built []*fakeRuntime
	manager, err := NewManager(func(config.RuntimeSnapshot) (Runtime, error) {
		runtime := &fakeRuntime{}
		built = append(built, runtime)
		return runtime, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	oldLease, err := manager.Acquire(runtimeSnapshot(t, "tenant-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	newLease, err := manager.Acquire(runtimeSnapshot(t, "tenant-a", 2))
	if err != nil {
		t.Fatal(err)
	}
	if built[0].closed.Load() != 0 {
		t.Fatal("old bundle closed while an old request is in flight")
	}
	// The old request finishes on v1.
	if _, err := oldLease.Runtime.Run(context.Background(), RunInput{RequestID: "old", UserID: "user", SessionID: "s", Text: "hello"}); err != nil {
		t.Fatal(err)
	}
	oldLease.Release()
	if built[0].closed.Load() != 1 {
		t.Fatalf("old bundle must close after drain, closed=%d", built[0].closed.Load())
	}
	if built[1].closed.Load() != 0 {
		t.Fatal("new bundle must stay open")
	}
	newLease.Release()
	if err := manager.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
