package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// zeroTenant is a syntactically valid uuid for tests that have no real tenant.
const zeroTenant = "00000000-0000-0000-0000-000000000000"

func TestAuditorAsyncBatchAndSync(t *testing.T) {
	pool, err := NewPG(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	ctx := context.Background()
	tag := fmt.Sprintf("audit-test-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM audit_log WHERE trace_id LIKE $1", tag+"%")
		pool.Close()
	})

	a := NewAuditor(pool)
	a.Start()

	// Async lane: buffered, flushed by the 1s ticker.
	for i := 0; i < 3; i++ {
		a.LogAsync(AuditEvent{
			TenantID: zeroTenant, Channel: "mock", UserID: "u1",
			Decision: "allow", TraceID: fmt.Sprintf("%s-async-%d", tag, i),
		})
	}

	// Sync lane (deny/review): visible immediately.
	if err := a.LogSync(ctx, AuditEvent{
		TenantID: zeroTenant, Channel: "mock", UserID: "u1",
		Decision: "deny", ErrorType: "budget_exceeded", TraceID: tag + "-sync",
	}); err != nil {
		t.Fatal(err)
	}

	count := func() int {
		var n int
		if err := pool.QueryRow(ctx,
			"SELECT count(*) FROM audit_log WHERE trace_id LIKE $1", tag+"%").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	if n := count(); n != 1 {
		t.Fatalf("sync deny should be visible immediately, got %d", n)
	}

	// Async events land after the flush tick.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && count() != 4 {
		time.Sleep(100 * time.Millisecond)
	}
	if n := count(); n != 4 {
		t.Fatalf("expected 4 audit rows after flush, got %d", n)
	}

	// Close flushes whatever is still buffered.
	a.LogAsync(AuditEvent{
		TenantID: zeroTenant, Channel: "mock", Decision: "allow", TraceID: tag + "-tail",
	})
	a.Close()
	if n := count(); n != 5 {
		t.Fatalf("expected 5 audit rows after close-flush, got %d", n)
	}
}

// Close must be safe on an Auditor that never started (and safe twice): with
// no loop running, nothing else is left to close done, so Close does it.
func TestAuditorCloseWithoutStart(t *testing.T) {
	a := NewAuditor(nil)
	a.Close()
	a.Close()
}

// A burst queued right before shutdown must still be written: the flush loop
// drains the channel instead of stopping at the buffer it happens to hold.
func TestAuditorCloseDrainsQueuedEvents(t *testing.T) {
	pool, err := NewPG(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	ctx := context.Background()
	tag := fmt.Sprintf("audit-drain-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM audit_log WHERE trace_id LIKE $1", tag+"%")
		pool.Close()
	})

	a := NewAuditor(pool)
	a.Start()
	for i := 0; i < 5; i++ {
		a.LogAsync(AuditEvent{
			TenantID: zeroTenant, Channel: "mock", UserID: "u1",
			Decision: "allow", TraceID: fmt.Sprintf("%s-%d", tag, i),
		})
	}
	a.Close() // no flush tick in between: only the drain can save these

	var n int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM audit_log WHERE trace_id LIKE $1", tag+"%").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("want 5 drained rows after close, got %d", n)
	}
	if a.Dropped() != 0 {
		t.Fatalf("no event should be dropped, got %d", a.Dropped())
	}
}

// Start must be idempotent. With a cancel func built per Start, the second
// Start overwrites the first and orphans a loop nobody can stop: it keeps
// taking events off the queue into a buffer that is never drained at shutdown,
// and it outlives the pool Close was supposed to release. Needs a live pool —
// a loop that actually runs needs somewhere to write.
func TestAuditorStartTwiceRunsOneLoop(t *testing.T) {
	pool, err := NewPG(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	ctx := context.Background()
	tag := fmt.Sprintf("audit-start-twice-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM audit_log WHERE trace_id LIKE $1", tag+"%")
		pool.Close()
	})

	a := NewAuditor(pool)
	a.Start()
	a.Start()
	for i := 0; i < 3; i++ {
		a.LogAsync(AuditEvent{
			TenantID: zeroTenant, Channel: "mock", UserID: "u1",
			Decision: "allow", TraceID: fmt.Sprintf("%s-%d", tag, i),
		})
	}
	a.Close()
	a.Close()

	var n int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM audit_log WHERE trace_id LIKE $1", tag+"%").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("want the 3 queued rows written exactly once, got %d", n)
	}
}

// A restart cycle is the interleaving that panics outright under a per-Start
// loop: the second Close cancels the loop the second Start spawned, and that
// loop closes a.done a second time. Nothing is logged here, so the drain finds
// an empty buffer and never reaches an insert — a nil pool is enough.
func TestAuditorRestartCycleDoesNotPanic(t *testing.T) {
	a := NewAuditor(nil)
	a.Start()
	a.Close()
	a.Start()
	a.Close()
	// The second loop only closes a.done once it wakes up, and a Close that
	// finds done already closed does not wait for it — stay alive so an
	// async panic still lands inside this test instead of after the binary
	// has exited.
	time.Sleep(100 * time.Millisecond)
}

// Close is terminal. An Auditor closed before it ever started must not be
// brought back by a later Start: nobody is left to stop that loop, so it keeps
// writing to a pool the caller has already released.
func TestAuditorStartAfterCloseIsInert(t *testing.T) {
	pool, err := NewPG(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	ctx := context.Background()
	tag := fmt.Sprintf("audit-start-late-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM audit_log WHERE trace_id LIKE $1", tag+"%")
		pool.Close()
	})

	a := NewAuditor(pool)
	a.Close()
	a.Start()
	a.LogAsync(AuditEvent{
		TenantID: zeroTenant, Channel: "mock", UserID: "u1",
		Decision: "allow", TraceID: tag,
	})

	// Past the 1s flush tick: a resurrected loop would have written the row.
	time.Sleep(1500 * time.Millisecond)
	var n int
	if err := pool.QueryRow(ctx,
		"SELECT count(*) FROM audit_log WHERE trace_id LIKE $1", tag+"%").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("a Start after Close must not write, got %d rows", n)
	}
	if a.Dropped() != 0 {
		t.Fatalf("nothing was flushed, so nothing may be counted as dropped: %d", a.Dropped())
	}
}

// Start and Close can arrive from different goroutines (a supervisor shuts the
// role down while it is still booting), so the loop has to be handed over
// exactly once. Run under -race; nothing is logged, so a nil pool is safe —
// an empty buffer never reaches an insert.
func TestAuditorLifecycleConcurrent(t *testing.T) {
	for range 200 {
		a := NewAuditor(nil)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); a.Start() }()
		go func() { defer wg.Done(); a.Close() }()
		wg.Wait()
	}
}
