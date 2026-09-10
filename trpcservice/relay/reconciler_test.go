package relay

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

type reconciliationStoreStub struct {
	issues []messaging.ReconciliationIssue
	before time.Time
	limit  int
}

func (s *reconciliationStoreStub) FindReconciliationIssues(_ context.Context, before time.Time, limit int) ([]messaging.ReconciliationIssue, error) {
	s.before, s.limit = before, limit
	return s.issues, nil
}

type reconciliationHandlerStub struct {
	issues []messaging.ReconciliationIssue
}

func (s *reconciliationHandlerStub) Reconcile(_ context.Context, issue messaging.ReconciliationIssue) error {
	s.issues = append(s.issues, issue)
	return nil
}

func TestReconcilerDelegatesToTypedTransitions(t *testing.T) {
	issues := []messaging.ReconciliationIssue{{Kind: messaging.IssueExpiredOutboxClaim, TenantID: "tenant", AggregateID: "request", RefID: "outbox", Version: 2}, {Kind: messaging.IssueMissingReplyOutbox, TenantID: "tenant", AggregateID: "request", RefID: "commit", Version: 3}}
	handler := &reconciliationHandlerStub{}
	r := Reconciler{Store: &reconciliationStoreStub{issues: issues}, Handler: handler}
	count, err := r.RunOnce(context.Background())
	if err != nil || count != len(issues) || len(handler.issues) != len(issues) {
		t.Fatalf("count=%d err=%v handled=%#v", count, err, handler.issues)
	}
}

func TestReconcilerDeduplicatesExactReplayAndUsesInjectedWatermark(t *testing.T) {
	now := time.Date(2026, 9, 8, 9, 0, 0, 0, time.UTC)
	newer := messaging.ReconciliationIssue{Kind: messaging.IssueExpiredOutboxClaim, TenantID: "tenant", AggregateID: "request", RefID: "outbox", Version: 4}
	// The older version is intentionally out of order. It must not cause a
	// stale typed transition after the durable record has advanced.
	older := newer
	older.Version = 3
	store := &reconciliationStoreStub{issues: []messaging.ReconciliationIssue{older, newer, newer}}
	handler := &reconciliationHandlerStub{}
	r := Reconciler{Store: store, Handler: handler, StuckAfter: 2 * time.Minute, BatchSize: 9, Now: func() time.Time { return now }}
	count, err := r.RunOnce(context.Background())
	if err != nil || count != 1 || len(handler.issues) != 1 {
		t.Fatalf("count=%d err=%v handled=%#v", count, err, handler.issues)
	}
	if handler.issues[0] != newer {
		t.Fatalf("unexpected replay handling: %#v", handler.issues)
	}
	if !store.before.Equal(now.Add(-2*time.Minute)) || store.limit != 9 {
		t.Fatalf("watermark=%v limit=%d", store.before, store.limit)
	}
}
