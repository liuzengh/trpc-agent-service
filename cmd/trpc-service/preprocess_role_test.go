package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestRunPreprocessLoopRetriesTransientErrorsAndStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := make(chan int, 4)
	done := make(chan error, 1)
	logger := testLogger()
	go func() {
		done <- runPreprocessLoop(ctx, func(context.Context, int) (int, error) {
			calls <- 1
			return 0, errors.New("database unavailable")
		}, time.Millisecond, 1, logger)
	}()
	select {
	case <-calls:
	case <-time.After(time.Second):
		t.Fatal("worker loop did not run")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("loop error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker loop did not stop")
	}
}

func TestRunPreprocessLoopRejectsInvariantFailure(t *testing.T) {
	err := runPreprocessLoop(context.Background(), func(context.Context, int) (int, error) {
		return 0, runtime.ErrInvariantViolation
	}, time.Millisecond, 1, testLogger())
	if !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("error=%v", err)
	}
}

func TestRunRoleRejectsUnknownRoleBeforeOpeningDependencies(t *testing.T) {
	err := runRole(context.Background(), nil, testLogger(), "unknown")
	if err == nil || err.Error() != "unsupported service role" {
		t.Fatalf("error=%v", err)
	}
}
