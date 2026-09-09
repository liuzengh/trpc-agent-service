package lifecycle

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"
)

func TestShutdownCancelsAdmissionAndWaits(t *testing.T) {
	s := New()
	release, ok := s.Acquire()
	if !ok {
		t.Fatal("initial work rejected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := s.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown err = %v", err)
	}
	if _, ok := s.Acquire(); ok {
		t.Fatal("work accepted during shutdown")
	}
	release()
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}

func TestRepeatedTimedOutWaitDoesNotLeaveWaiterGoroutines(t *testing.T) {
	s := New()
	release, _ := s.Acquire()
	s.BeginShutdown()
	baseline := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		if err := s.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Wait err = %v", err)
		}
		cancel()
	}
	if delta := runtime.NumGoroutine() - baseline; delta > 1 {
		t.Fatalf("Wait left %d goroutines", delta)
	}
	release()
	if err := s.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestReleaseAndShutdownAreIdempotent(t *testing.T) {
	s := New()
	release, _ := s.Acquire()
	release()
	release()
	if err := s.ShutdownWithTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
