package worker

import "testing"

func TestLifecycleTransitionsReadyDrainStopped(t *testing.T) {
	lifecycle := NewLifecycle()
	if lifecycle.State() != LifecycleStarting || lifecycle.Ready() {
		t.Fatalf("initial state=%s", lifecycle.State())
	}
	if err := lifecycle.MarkReady(); err != nil || !lifecycle.Ready() {
		t.Fatalf("ready state=%s err=%v", lifecycle.State(), err)
	}
	lifecycle.BeginDrain()
	lifecycle.BeginDrain()
	if lifecycle.State() != LifecycleDraining || lifecycle.Ready() {
		t.Fatalf("draining state=%s", lifecycle.State())
	}
	select {
	case <-lifecycle.Drain():
	default:
		t.Fatal("drain signal is not closed")
	}
	lifecycle.MarkStopped()
	if lifecycle.State() != LifecycleStopped {
		t.Fatalf("stopped state=%s", lifecycle.State())
	}
}
