package bus

import (
	"testing"
	"time"
)

func TestOutboxBackoffIsExponentialAndCapped(t *testing.T) {
	if got := outboxBackoff(0); got != 0 {
		t.Errorf("backoff(0) = %v, want 0 (first attempt is immediate)", got)
	}
	if got := outboxBackoff(1); got != outboxRetryBase {
		t.Errorf("backoff(1) = %v, want %v", got, outboxRetryBase)
	}
	if got := outboxBackoff(3); got != 8*time.Second {
		t.Errorf("backoff(3) = %v, want 8s", got)
	}
	// The cap bounds the retry interval: an unbounded doubling would make the
	// last attempts hours apart and defeat the retry budget's purpose.
	for _, retries := range []int{6, 10, 40} {
		if got := outboxBackoff(retries); got != outboxRetryMax {
			t.Errorf("backoff(%d) = %v, want the %v cap", retries, got, outboxRetryMax)
		}
	}
	prev := time.Duration(0)
	for retries := 0; retries <= 12; retries++ {
		got := outboxBackoff(retries)
		if got < prev {
			t.Fatalf("backoff(%d) = %v, decreased from %v", retries, got, prev)
		}
		prev = got
	}
}

func TestOutboxRetryWindowIsBounded(t *testing.T) {
	// 2+4+8+16+32+60+60+60+60+60 seconds: minutes of coverage, not seconds, so
	// a Redis restart or failover is absorbed instead of dead-lettered.
	var total time.Duration
	for i := 1; i <= DefaultMaxOutboxRetries; i++ {
		total += outboxBackoff(i)
	}
	if total < 6*time.Minute {
		t.Errorf("total retry window = %v, want at least 6 minutes", total)
	}
	if total > 30*time.Minute {
		t.Errorf("total retry window = %v, want a bounded window", total)
	}
}

// TestOutboxDueFollowsTheSchedule pins the due-check to the event's *age*, which
// the database measures, plus the rule that a never-attempted event is always
// due. Comparing a server-written timestamp against this process's clock is what
// silently stopped reply delivery on a UTC+8 server: the two readings of the
// same row were eight hours apart, so every fresh event looked like it was still
// in the future.
func TestOutboxDueFollowsTheSchedule(t *testing.T) {
	// Never attempted: due whatever the clock says (a negative age is exactly
	// what a row written under another session time zone looks like).
	if !outboxDue(0, 0) {
		t.Error("a never-attempted event must be due immediately")
	}
	if !outboxDue(-8*time.Hour, 0) {
		t.Error("a never-attempted event written under a skewed clock must still be due")
	}
	// Retries respect the backoff.
	if outboxDue(time.Second, 1) {
		t.Error("event in backoff must not be due before its schedule")
	}
	if !outboxDue(3*time.Second, 1) {
		t.Error("event must be due once its backoff elapsed")
	}
	if !outboxDue(6*time.Minute, DefaultMaxOutboxRetries) {
		t.Error("a long-pending event must become due again, not stay deferred forever")
	}
}

func TestOutboxExhaustedStopsAtTheBudget(t *testing.T) {
	if outboxExhausted(2, 3) {
		t.Error("attempt 2 of 3 must still be retried")
	}
	if !outboxExhausted(3, 3) {
		t.Error("the budget must be spent once the attempt count reaches it")
	}
	// A zero budget falls back to the default instead of never retrying.
	if outboxExhausted(DefaultMaxOutboxRetries-1, 0) {
		t.Error("a zero budget must fall back to the default, not park immediately")
	}
	if !outboxExhausted(DefaultMaxOutboxRetries, 0) {
		t.Error("the default budget must still park the event")
	}
}
