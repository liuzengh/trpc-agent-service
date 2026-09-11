package health

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// testSupervisor is the production shape with millisecond timings so a test can
// observe a full outage plus recovery in well under a second.
func testSupervisor(name string) *Supervisor {
	return &Supervisor{
		Name: name, Base: 10 * time.Millisecond, Max: 40 * time.Millisecond,
		DegradeAfter: 2,
	}
}

// waitFor polls cond until it holds or the budget runs out. The supervisor is
// time-based, so every assertion here is an eventual one.
func waitFor(t *testing.T, budget time.Duration, cond func() bool, msg string) bool {
	t.Helper()
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("timed out waiting for %s", msg)
	return false
}

func issueOpen(name string) bool {
	_, reason := Degraded()
	return strings.Contains(reason, name)
}

// TestSupervisorReconnectsAndReportsRecovery is the regression for the defect
// this type was extracted from: the loop must both reconnect on its own and
// *clear* the degraded flag afterwards. Clearing is the part a synchronous retry
// loop cannot do, because a healthy consume loop never returns — the deployed
// stack stayed 503 forever after a single Redis blip.
func TestSupervisorReconnectsAndReportsRecovery(t *testing.T) {
	const name = "test: reconnect"
	t.Cleanup(func() { Resolve(name) })
	sup := testSupervisor(name)

	var calls int32
	loop := func(ctx context.Context) error {
		if atomic.AddInt32(&calls, 1) <= 2 {
			return errors.New("connection refused")
		}
		// The dependency answered a read: this is the honest attachment signal
		// (RedisBus.SetConsumeReady / Gateway.SetReady in production).
		sup.Attached()
		<-ctx.Done()
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- sup.Run(ctx, loop) }()

	if !waitFor(t, 5*time.Second, func() bool { return issueOpen(name) }, "the outage to be reported") {
		return
	}
	if !waitFor(t, 5*time.Second, func() bool { return !issueOpen(name) }, "recovery to clear the probe") {
		return
	}
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Errorf("attempts = %d, want 3 (two failures, then stay attached)", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v on shutdown, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop when the context was cancelled")
	}
}

// TestSupervisorStaysDegradedWhileFailing: a node that answers HTTP but consumes
// nothing has to be visible from outside, not just in a log line.
func TestSupervisorStaysDegradedWhileFailing(t *testing.T) {
	const name = "test: always down"
	t.Cleanup(func() { Resolve(name) })
	sup := testSupervisor(name)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = sup.Run(ctx, func(context.Context) error {
			return errors.New("dial tcp: connect: connection refused")
		})
	}()

	if !waitFor(t, 5*time.Second, func() bool { return issueOpen(name) }, "the outage to be reported") {
		return
	}
	if _, reason := Degraded(); !strings.Contains(reason, "connection refused") {
		t.Errorf("probe reason = %q, want it to carry the cause", reason)
	}
	cancel()
	<-done
	if issueOpen(name) {
		t.Error("shutdown must resolve the issue the supervisor opened")
	}
}

// TestSupervisorKeepsReportingAfterRecovery guards the reset: once the loop
// reattached, a second outage must be reported again (a stale failure counter or
// a stuck "failing" flag would hide it).
func TestSupervisorKeepsReportingAfterRecovery(t *testing.T) {
	const name = "test: flapping"
	t.Cleanup(func() { Resolve(name) })
	sup := testSupervisor(name)

	var calls int32
	loop := func(ctx context.Context) error {
		switch n := atomic.AddInt32(&calls, 1); {
		case n <= 2:
			return errors.New("connection refused")
		case n == 3:
			sup.Attached()
			return errors.New("connection reset by peer") // fails again after recovery
		case n == 4:
			return errors.New("connection reset by peer") // second consecutive failure
		default:
			<-ctx.Done()
			return nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() { _ = sup.Run(ctx, loop) }()

	if !waitFor(t, 5*time.Second, func() bool { return issueOpen(name) }, "the first outage") {
		return
	}
	if !waitFor(t, 5*time.Second, func() bool { return !issueOpen(name) }, "the recovery") {
		return
	}
	if !waitFor(t, 5*time.Second, func() bool { return issueOpen(name) }, "the second outage to be reported") {
		return
	}
	cancel()
}

// TestSupervisorReportsSilentDependencyOnFirstFailure pins the rule the fault
// drill forced: degradation is decided by how long the dependency has been
// silent, not by counting failed attempts. One attempt can cost seconds when the
// pool hands out a connection to a dead server, which is how the deployed stack
// answered /healthz 200 through a 31 second Redis stop.
func TestSupervisorReportsSilentDependencyOnFirstFailure(t *testing.T) {
	const name = "test: silent dependency"
	t.Cleanup(func() { Resolve(name) })
	// DegradeAfter is unreachable on purpose: only the silence rule can fire.
	sup := &Supervisor{Name: name, Base: 10 * time.Millisecond, DegradeAfter: 1000, Grace: 30 * time.Millisecond}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		_ = sup.Run(ctx, func(context.Context) error {
			// A slow failure: silent for longer than the grace period.
			time.Sleep(40 * time.Millisecond)
			return errors.New("dial tcp: connect: connection refused")
		})
	}()

	if !waitFor(t, 5*time.Second, func() bool { return issueOpen(name) }, "a silent dependency to be reported") {
		return
	}
	cancel()
	if !waitFor(t, 5*time.Second, func() bool { return !issueOpen(name) }, "shutdown to resolve the issue") {
		return
	}
}

// TestSupervisorPassesThroughCleanExit: a loop that returns nil ended because
// its context was cancelled, which is a shutdown, not a failure to retry.
func TestSupervisorPassesThroughCleanExit(t *testing.T) {
	calls := 0
	sup := testSupervisor("test: clean exit")
	if err := sup.Run(context.Background(), func(context.Context) error {
		calls++
		return nil
	}); err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}
	if calls != 1 {
		t.Errorf("calls = %d, want 1 (a clean exit is not retried)", calls)
	}
}

// TestSupervisorDoesNotStartOnCancelledContext keeps shutdown honest.
func TestSupervisorDoesNotStartOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	sup := testSupervisor("test: cancelled")
	if err := sup.Run(ctx, func(context.Context) error {
		calls++
		return errors.New("connection refused")
	}); err != nil {
		t.Errorf("Run returned %v, want nil", err)
	}
	if calls != 0 {
		t.Errorf("loop was called %d time(s) with a cancelled context, want 0", calls)
	}
}

// TestIssuesAreIndependent: independent producers must not clear each other.
// With one global flag, the consumer reattaching would have hidden a database
// that never came up (and any other combination of the three producers).
func TestIssuesAreIndependent(t *testing.T) {
	Report("test: consumer", "lost the bus: refused")
	Report("test: mysql", "unavailable")
	t.Cleanup(func() {
		Resolve("test: consumer")
		Resolve("test: mysql")
	})

	degraded, reason := Degraded()
	if !degraded {
		t.Fatal("Degraded() = false with two open issues, want true")
	}
	for _, want := range []string{"test: consumer lost the bus: refused", "test: mysql unavailable"} {
		if !strings.Contains(reason, want) {
			t.Errorf("reason = %q, want it to contain %q", reason, want)
		}
	}

	Resolve("test: consumer")
	degraded, reason = Degraded()
	if !degraded || !strings.Contains(reason, "mysql unavailable") {
		t.Errorf("after resolving the consumer: degraded=%v reason=%q, want the mysql issue to remain", degraded, reason)
	}
	if strings.Contains(reason, "consumer") {
		t.Errorf("reason = %q, want the resolved issue gone", reason)
	}

	Resolve("test: mysql")
	if degraded, reason := Degraded(); degraded {
		t.Errorf("Degraded() = true (%q) with no open issue, want false", reason)
	}
	Resolve("test: never-opened") // must not panic
}
