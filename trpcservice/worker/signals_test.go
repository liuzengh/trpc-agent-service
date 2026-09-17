package worker

import (
	"context"
	"syscall"
	"testing"
	"time"
)

func TestInstallSignalDrainStopsLifecycleOnContext(t *testing.T) {
	lifecycle := NewLifecycle()
	if err := lifecycle.MarkReady(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	stop := InstallSignalDrain(ctx, lifecycle, syscall.SIGUSR1)
	cancel()
	defer stop()
	select {
	case <-lifecycle.Drain():
		t.Fatal("context cancellation must not announce process drain")
	case <-time.After(20 * time.Millisecond):
	}
	if lifecycle.State() != LifecycleReady {
		t.Fatalf("state=%s", lifecycle.State())
	}
}
