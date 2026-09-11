package channels

import (
	"testing"
	"time"
)

func TestBudgetTrackerAllowsWithinLimits(t *testing.T) {
	b := NewBudgetTracker()
	ok := b.Spend("t1", 100, 50, 10, 1000, 500, 100, 50, 0)
	if !ok {
		t.Fatal("first spend within limits must be allowed")
	}
}

func TestBudgetTrackerCallLimit(t *testing.T) {
	b := NewBudgetTracker()
	for i := 0; i < 2; i++ {
		if !b.Spend("t1", 0, 0, 0, 0, 0, 0, 3, 0) {
			t.Fatalf("spend %d/3 must be allowed", i+1)
		}
	}
	// 3rd call hits the limit (maxCalls=3 → third is rejected).
	if b.Spend("t1", 0, 0, 0, 0, 0, 0, 3, 0) {
		t.Fatal("3rd call must be rejected (already at limit)")
	}
}

func TestBudgetTrackerResetsAfterInterval(t *testing.T) {
	b := NewBudgetTracker()
	// First batch: 5 calls allowed, 5 calls used → full.
	for i := 0; i < 5; i++ {
		b.Spend("t1", 0, 0, 0, 0, 0, 0, 5, time.Hour)
	}
	if b.Spend("t1", 0, 0, 0, 0, 0, 0, 5, time.Hour) {
		t.Fatal("6th call before reset must be rejected")
	}

	// Manually age the timestamp past the window.
	b.mu.Lock()
	b.m["t1"].resetAt = time.Now().Add(-time.Hour)
	b.mu.Unlock()

	if !b.Spend("t1", 0, 0, 0, 0, 0, 0, 5, time.Hour) {
		t.Fatal("call after reset must be allowed")
	}
}

func TestBudgetTrackerTokenAndCostLimits(t *testing.T) {
	b := NewBudgetTracker()
	// 1000 prompt token limit
	ok := b.Spend("t1", 600, 0, 0, 1000, 0, 0, 0, 0)
	if !ok {
		t.Fatal("600 prompt tokens within 1000 must be allowed")
	}
	// 399 more: 600+399=999, still under 1000.
	ok = b.Spend("t1", 399, 0, 0, 1000, 0, 0, 0, 0)
	if !ok {
		t.Fatal("399+600=999 within 1000 must be allowed")
	}
	// One more token hits the limit.
	ok = b.Spend("t1", 1, 0, 0, 1000, 0, 0, 0, 0)
	if ok {
		t.Fatal("1000 tokens total must be rejected (>= limit)")
	}
}

func TestBudgetTrackerRecord(t *testing.T) {
	b := NewBudgetTracker()
	b.Spend("t1", 0, 0, 0, 100, 100, 100, 10, 0)
	b.Record("t1", 30, 20, 5)

	b.mu.Lock()
	bs := b.m["t1"]
	b.mu.Unlock()

	if bs.promptTokens != 30 || bs.compTokens != 20 || bs.costCents != 5 || bs.calls != 1 {
		t.Fatalf("snapshot = %+v, want prompt=30 comp=20 cost=5 calls=1", bs)
	}
}

func TestNilBudgetTrackerIsNoop(t *testing.T) {
	var b *BudgetTracker
	if !b.Spend("t1", 10000, 10000, 10000, 1, 1, 1, 1, 0) {
		t.Fatal("nil BudgetTracker must allow everything")
	}
	b.Record("t1", 10000, 10000, 10000) // must not panic
}
