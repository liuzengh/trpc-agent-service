package storage

import (
	"context"
	"testing"
	"time"
)

// NewArchiver fills in the documented defaults for zero fields.
func TestNewArchiverDefaults(t *testing.T) {
	a := NewArchiver(nil, 0, 0)
	if a.Retention != 30*24*time.Hour {
		t.Fatalf("default retention must be 30d, got %s", a.Retention)
	}
	if a.Interval != 24*time.Hour {
		t.Fatalf("default interval must be 24h, got %s", a.Interval)
	}
	if a.BatchSize != 1000 || a.BatchPause != 100*time.Millisecond {
		t.Fatalf("unexpected batch defaults: %d %s", a.BatchSize, a.BatchPause)
	}
	custom := NewArchiver(nil, time.Hour, time.Minute)
	if custom.Retention != time.Hour || custom.Interval != time.Minute {
		t.Fatalf("explicit values must be kept: %s %s", custom.Retention, custom.Interval)
	}
}

// A sustained overload drops events after the enqueue wait and counts them:
// the audit trail must visibly develop holes instead of blocking producers.
func TestAuditorLogAsyncDropsOnFullQueue(t *testing.T) {
	pool, err := NewPG(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	defer pool.Close()

	a := NewAuditor(pool)
	a.ch = make(chan AuditEvent, 1) // shrink the buffer to force the drop
	a.ch <- AuditEvent{Decision: "allow"}

	a.LogAsync(AuditEvent{TenantID: zeroTenant, Decision: "allow", TraceID: "drop-1"})
	if got := a.Dropped(); got != 1 {
		t.Fatalf("the overflowing event must be counted as dropped, got %d", got)
	}
}

// LogAsync stamps the creation time when the producer left it zero.
func TestAuditorLogAsyncStampsCreatedAt(t *testing.T) {
	pool, err := NewPG(context.Background(), testPGDSN)
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	defer pool.Close()

	a := NewAuditor(pool)
	a.ch = make(chan AuditEvent, 1)
	ev := AuditEvent{TenantID: zeroTenant, Decision: "allow", TraceID: "stamp-1"}
	a.LogAsync(ev)
	got := <-a.ch
	if got.CreatedAt.IsZero() {
		t.Fatal("LogAsync must stamp a zero CreatedAt")
	}
	if got.TraceID != "stamp-1" {
		t.Fatalf("event must pass through unchanged, got %+v", got)
	}
}
