package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestRunRecoverableLoopRetriesTransientFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- runRecoverableLoop(ctx, "test worker", func(context.Context) error {
			if calls.Add(1) == 1 {
				return errors.New("redis unavailable")
			}
			cancel()
			return context.Canceled
		}, time.Millisecond, &roleLogger{})
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) || calls.Load() != 2 {
			t.Fatalf("err=%v calls=%d", err, calls.Load())
		}
	case <-time.After(time.Second):
		t.Fatal("recovery loop did not retry")
	}
}

func TestRunRecoverableLoopKeepsInvariantFailureFatal(t *testing.T) {
	err := runRecoverableLoop(context.Background(), "test worker", func(context.Context) error {
		return runtime.ErrInvariantViolation
	}, time.Millisecond, &roleLogger{})
	if !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("err=%v", err)
	}
}
