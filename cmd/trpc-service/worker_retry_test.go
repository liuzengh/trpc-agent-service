package main

import (
	"context"
	"testing"
	"time"
)

// TestWorkerRetryBackoffEscalatesAndCaps verifies the redelivery pacing policy:
// delays grow exponentially, never exceed the cap, and keep pacing indefinitely
// (MaxElapsedTime=0) because Kafka holds the message and the DLQ policy is the
// real retry bound.
func TestWorkerRetryBackoffEscalatesAndCaps(t *testing.T) {
	t.Parallel()

	retry := newWorkerRetryBackoff()
	// The production policy keeps jitter enabled; disable it here so the
	// escalation assertion is deterministic.
	retry.RandomizationFactor = 0
	first := retry.NextBackOff()
	second := retry.NextBackOff()
	third := retry.NextBackOff()
	if !(first > 0 && second > first && third > second) {
		t.Fatalf("backoff is not increasing: %v, %v, %v", first, second, third)
	}
	if third > workerRetryMaxInterval {
		t.Fatalf("backoff exceeded cap early: %v > %v", third, workerRetryMaxInterval)
	}
	for index := 0; index < 50; index++ {
		if delay := retry.NextBackOff(); delay > workerRetryMaxInterval {
			t.Fatalf("backoff exceeded cap at step %d: %v > %v", index, delay, workerRetryMaxInterval)
		}
	}
}

// TestWaitRetryDelayHonorsContextCancellation verifies the worker loop exits
// promptly during shutdown instead of sleeping through the delay.
func TestWaitRetryDelayHonorsContextCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitRetryDelay(ctx, newWorkerRetryBackoff()) {
		t.Fatal("waitRetryDelay() = true on canceled context, want false")
	}
}

// TestWaitRetryDelayPacesRedelivery verifies the timer path actually waits for
// the configured interval before allowing the next attempt.
func TestWaitRetryDelayPacesRedelivery(t *testing.T) {
	t.Parallel()

	retry := newWorkerRetryBackoff()
	retry.InitialInterval = time.Millisecond
	retry.MaxInterval = time.Millisecond
	retry.Multiplier = 1

	start := time.Now()
	if !waitRetryDelay(context.Background(), retry) {
		t.Fatal("waitRetryDelay() = false, want true")
	}
	if elapsed := time.Since(start); elapsed < time.Millisecond {
		t.Fatalf("waitRetryDelay() returned before the interval elapsed: %v", elapsed)
	}
}
