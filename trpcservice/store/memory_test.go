package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
)

func newInbox(id, dedup string) *InboxRecord {
	return &InboxRecord{
		InboxID: id, TenantID: "acme", ChannelType: "telegram", BindingID: "b1",
		ExternalMessageID: "ext-" + id, DedupKey: dedup, PartitionKey: dedup,
		Payload: []byte(`{"m":"hi"}`),
	}
}

func TestMemoryInsertInboxDeduplicates(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	now := time.Now()
	if err := st.InsertInbox(ctx, newInbox("a", "k1"), now); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	err := st.InsertInbox(ctx, newInbox("b", "k1"), now)
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("want ErrDuplicate, got %v", err)
	}
	// A different message with a different key is accepted.
	if err := st.InsertInbox(ctx, newInbox("c", "k2"), now); err != nil {
		t.Fatalf("distinct insert: %v", err)
	}
}

func TestMemoryLeaseInboxCountsAttemptsAndHonorsNextAttempt(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	now := time.Now()
	if err := st.InsertInbox(ctx, newInbox("a", "k1"), now.Add(-time.Minute)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	leased, err := st.LeaseInbox(ctx, "w1", now, time.Minute, 10)
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease: %v %d", err, len(leased))
	}
	if leased[0].AttemptCount != 1 {
		t.Fatalf("attempt count = %d, want 1", leased[0].AttemptCount)
	}
	// A leased (processing) row is not leased again by another worker.
	again, err := st.LeaseInbox(ctx, "w2", now, time.Minute, 10)
	if err != nil || len(again) != 0 {
		t.Fatalf("second lease should be empty: %v %d", err, len(again))
	}
	// A retried row whose next_attempt_at is in the future is not leased yet.
	if err := st.RetryInbox(ctx, "a", "w1", "internal", now, now.Add(time.Hour)); err != nil {
		t.Fatalf("retry: %v", err)
	}
	future, err := st.LeaseInbox(ctx, "w2", now, time.Minute, 10)
	if err != nil || len(future) != 0 {
		t.Fatalf("future lease should be empty: %v %d", err, len(future))
	}
	// Once the retry time arrives it becomes runnable again, with attempts kept.
	past := now.Add(2 * time.Hour)
	relaunched, err := st.LeaseInbox(ctx, "w2", past, time.Minute, 10)
	if err != nil || len(relaunched) != 1 {
		t.Fatalf("relaunch: %v %d", err, len(relaunched))
	}
	if relaunched[0].AttemptCount != 2 {
		t.Fatalf("attempt count = %d, want 2", relaunched[0].AttemptCount)
	}
}

func TestMemoryInboxPartitionIsStrictFIFOAcrossRetry(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	now := time.Now()
	first := newInbox("z-first", "k-first")
	first.PartitionKey = "session-a"
	second := newInbox("a-second", "k-second")
	second.PartitionKey = "session-a"
	other := newInbox("m-other", "k-other")
	other.PartitionKey = "session-b"
	for _, rec := range []*InboxRecord{first, second, other} {
		if err := st.InsertInbox(ctx, rec, now); err != nil {
			t.Fatalf("insert %s: %v", rec.InboxID, err)
		}
	}

	leased, err := st.LeaseInbox(ctx, "worker-a", now, time.Minute, 10)
	if err != nil {
		t.Fatalf("lease heads: %v", err)
	}
	if len(leased) != 2 || leased[0].InboxID != first.InboxID || leased[1].InboxID != other.InboxID {
		t.Fatalf("leased heads = %+v, want first and cross-session row", leased)
	}
	// A future retry remains the partition head; its due successor cannot
	// overtake it while another session remains independent.
	if err := st.RetryInbox(ctx, first.InboxID, "worker-a", "temporary", now, now.Add(time.Hour)); err != nil {
		t.Fatalf("retry first: %v", err)
	}
	blocked, err := st.LeaseInbox(ctx, "worker-b", now, time.Minute, 10)
	if err != nil || len(blocked) != 0 {
		t.Fatalf("successor overtook retry head: records=%+v err=%v", blocked, err)
	}
	if err := st.CompleteInbox(ctx, other.InboxID, "worker-a", nil, now); err != nil {
		t.Fatalf("complete other: %v", err)
	}

	later := now.Add(2 * time.Hour)
	retried, err := st.LeaseInbox(ctx, "worker-b", later, time.Minute, 10)
	if err != nil || len(retried) != 1 || retried[0].InboxID != first.InboxID {
		t.Fatalf("re-lease head: records=%+v err=%v", retried, err)
	}
	if err := st.DeadLetterInbox(ctx, first.InboxID, "worker-b", "exhausted", later); err != nil {
		t.Fatalf("dead-letter first: %v", err)
	}
	successor, err := st.LeaseInbox(ctx, "worker-c", later, time.Minute, 10)
	if err != nil || len(successor) != 1 || successor[0].InboxID != second.InboxID {
		t.Fatalf("lease successor: records=%+v err=%v", successor, err)
	}
}

func TestMemoryOutboxPartitionIsStrictFIFOAcrossRetry(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	now := time.Now()
	first := newInbox("in-1", "k1")
	first.PartitionKey = "session-a"
	second := newInbox("in-2", "k2")
	second.PartitionKey = "session-a"
	if err := st.InsertInbox(ctx, first, now); err != nil {
		t.Fatal(err)
	}
	if err := st.InsertInbox(ctx, second, now); err != nil {
		t.Fatal(err)
	}
	leased, err := st.LeaseInbox(ctx, "relay", now, time.Minute, 10)
	if err != nil || len(leased) != 1 || leased[0].InboxID != first.InboxID {
		t.Fatalf("lease first inbox: records=%+v err=%v", leased, err)
	}
	if err := st.CompleteInbox(ctx, first.InboxID, "relay", &OutboxRecord{
		OutboxID: "out-1", TenantID: "acme", ChannelType: "telegram", BindingID: "b1",
		DedupKey: "o1", PartitionKey: "must-not-override-inbox", Payload: []byte(`{}`),
	}, now); err != nil {
		t.Fatal(err)
	}
	leased, err = st.LeaseInbox(ctx, "relay", now, time.Minute, 10)
	if err != nil || len(leased) != 1 || leased[0].InboxID != second.InboxID {
		t.Fatalf("lease second inbox: records=%+v err=%v", leased, err)
	}
	if err := st.CompleteInbox(ctx, second.InboxID, "relay", &OutboxRecord{
		OutboxID: "out-2", TenantID: "acme", ChannelType: "telegram", BindingID: "b1",
		DedupKey: "o2", Payload: []byte(`{}`),
	}, now); err != nil {
		t.Fatal(err)
	}

	out, err := st.LeaseOutbox(ctx, "sender-a", now, time.Minute, 10)
	if err != nil || len(out) != 1 || out[0].OutboxID != "out-1" || out[0].PartitionKey != "session-a" {
		t.Fatalf("lease first outbox: records=%+v err=%v", out, err)
	}
	if err := st.RetryOutbox(ctx, "out-1", "sender-a", "rate_limit", now, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	blocked, err := st.LeaseOutbox(ctx, "sender-b", now, time.Minute, 10)
	if err != nil || len(blocked) != 0 {
		t.Fatalf("second reply overtook retry head: records=%+v err=%v", blocked, err)
	}
	later := now.Add(2 * time.Hour)
	out, err = st.LeaseOutbox(ctx, "sender-b", later, time.Minute, 10)
	if err != nil || len(out) != 1 || out[0].OutboxID != "out-1" {
		t.Fatalf("re-lease first outbox: records=%+v err=%v", out, err)
	}
	if err := st.CompleteOutbox(ctx, "out-1", "sender-b", later); err != nil {
		t.Fatal(err)
	}
	out, err = st.LeaseOutbox(ctx, "sender-c", later, time.Minute, 10)
	if err != nil || len(out) != 1 || out[0].OutboxID != "out-2" {
		t.Fatalf("lease second outbox: records=%+v err=%v", out, err)
	}
}

func TestMemoryCompleteInboxEnqueuesOutboxAtomicallyAndFencesOwner(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	now := time.Now()
	if err := st.InsertInbox(ctx, newInbox("a", "k1"), now); err != nil {
		t.Fatalf("insert: %v", err)
	}
	leased, err := st.LeaseInbox(ctx, "w1", now, time.Minute, 10)
	if err != nil || len(leased) != 1 {
		t.Fatalf("lease: %v", err)
	}
	outbox := &OutboxRecord{
		OutboxID: "o1", TenantID: "acme", ChannelType: "telegram", BindingID: "b1",
		DedupKey: "k1:outbox", Payload: []byte(`{"text":"ok"}`),
	}
	if err := st.CompleteInbox(ctx, "a", "w1", outbox, now); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// The reply is pending delivery.
	sent, err := st.LeaseOutbox(ctx, "w1", now, time.Minute, 10)
	if err != nil || len(sent) != 1 || sent[0].OutboxID != "o1" {
		t.Fatalf("outbox lease: %v %+v", err, sent)
	}
	// Completion from the wrong owner is fenced.
	if err := st.CompleteOutbox(ctx, "o1", "w2", now); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("fenced completion = %v, want ErrLeaseLost", err)
	}
	if err := st.CompleteOutbox(ctx, "o1", "w1", now); err != nil {
		t.Fatalf("complete outbox: %v", err)
	}
	// The processed inbox row is no longer completable by anyone.
	if err := st.CompleteInbox(ctx, "a", "w1", nil, now); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("replay completion = %v, want ErrLeaseLost", err)
	}
}

func TestMemoryLeaseRenewalFencesExpiredAttempt(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	now := time.Now()
	if err := st.InsertInbox(ctx, newInbox("a", "k1"), now); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := st.LeaseInbox(ctx, "attempt-1", now, time.Second, 1); err != nil {
		t.Fatalf("lease: %v", err)
	}
	if err := st.RenewInboxLease(ctx, "a", "attempt-1", now.Add(500*time.Millisecond), time.Second); err != nil {
		t.Fatalf("renew live lease: %v", err)
	}
	if reclaimed, _, err := st.ReclaimExpired(ctx, now.Add(1100*time.Millisecond)); err != nil || reclaimed != 0 {
		t.Fatalf("renewed lease was reclaimed: count=%d err=%v", reclaimed, err)
	}
	if err := st.CompleteInbox(ctx, "a", "attempt-1", nil, now.Add(1600*time.Millisecond)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired completion before reclaim = %v, want ErrLeaseLost", err)
	}
	if reclaimed, _, err := st.ReclaimExpired(ctx, now.Add(1600*time.Millisecond)); err != nil || reclaimed != 1 {
		t.Fatalf("expired lease was not reclaimed: count=%d err=%v", reclaimed, err)
	}
	if err := st.RenewInboxLease(ctx, "a", "attempt-1", now.Add(1600*time.Millisecond), time.Second); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale renewal = %v, want ErrLeaseLost", err)
	}
	leased, err := st.LeaseInbox(ctx, "attempt-2", now.Add(1600*time.Millisecond), time.Second, 1)
	if err != nil || len(leased) != 1 {
		t.Fatalf("re-lease: records=%d err=%v", len(leased), err)
	}
	if err := st.CompleteInbox(ctx, "a", "attempt-1", nil, now.Add(1600*time.Millisecond)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale completion = %v, want ErrLeaseLost", err)
	}
}

func TestMemoryRetryAndDeadLetterFenceOwner(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	now := time.Now()
	if err := st.InsertInbox(ctx, newInbox("a", "k1"), now); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := st.LeaseInbox(ctx, "w1", now, time.Minute, 10); err != nil {
		t.Fatalf("lease: %v", err)
	}
	if err := st.RetryInbox(ctx, "a", "w2", "internal", now, now); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("wrong-owner retry = %v, want ErrLeaseLost", err)
	}
	if _, err := st.LeaseInbox(ctx, "w1", now.Add(time.Minute), time.Minute, 10); err != nil {
		t.Fatalf("re-lease: %v", err)
	}
	if err := st.DeadLetterInbox(ctx, "a", "w1", "exhausted", now); err != nil {
		t.Fatalf("dead letter: %v", err)
	}
	_, _, _, _, _, err := st.Depths(ctx)
	if err != nil {
		t.Fatalf("depths: %v", err)
	}
	runnable, dead, _, _ := depthsOf(t, st)
	if runnable != 0 || dead != 1 {
		t.Fatalf("depths after dead letter: runnable=%d dead=%d", runnable, dead)
	}
}

func depthsOf(t *testing.T, st *Memory) (int, int, int, int) {
	t.Helper()
	runnable, deadIn, pending, deadOut, _, err := st.Depths(context.Background())
	if err != nil {
		t.Fatalf("depths: %v", err)
	}
	return runnable, deadIn, pending, deadOut
}

func TestMemoryReclaimExpiredResetsBothQueues(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	now := time.Now()
	// One inbox row driven through the happy path: leased, completed with an
	// outbox reply, then leased for delivery.
	if err := st.InsertInbox(ctx, newInbox("a", "k1"), now); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := st.LeaseInbox(ctx, "w1", now, time.Second, 10); err != nil {
		t.Fatalf("lease inbox: %v", err)
	}
	if err := st.CompleteInbox(ctx, "a", "w1", &OutboxRecord{
		OutboxID: "o1", DedupKey: "k1:outbox", Payload: []byte(`{}`),
	}, now); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if _, err := st.LeaseOutbox(ctx, "w1", now, time.Second, 10); err != nil {
		t.Fatalf("lease outbox: %v", err)
	}
	// A second inbox row whose worker vanished right after leasing.
	if err := st.InsertInbox(ctx, newInbox("b", "k2"), now); err != nil {
		t.Fatalf("insert orphan: %v", err)
	}
	if _, err := st.LeaseInbox(ctx, "ghost", now, time.Second, 10); err != nil {
		t.Fatalf("ghost lease: %v", err)
	}
	// Before expiry, reclaim does nothing.
	in, out, err := st.ReclaimExpired(ctx, now.Add(500*time.Millisecond))
	if err != nil || in != 0 || out != 0 {
		t.Fatalf("pre-expiry reclaim: %v %d %d", err, in, out)
	}
	// After expiry the stuck rows are runnable again for the next owner.
	in, out, err = st.ReclaimExpired(ctx, now.Add(2*time.Second))
	if err != nil || in != 1 || out != 1 {
		t.Fatalf("reclaim: %v %d %d", err, in, out)
	}
	if _, err := st.LeaseInbox(ctx, "w2", now.Add(2*time.Second), time.Minute, 10); err != nil {
		t.Fatalf("re-lease inbox: %v", err)
	}
	sent, err := st.LeaseOutbox(ctx, "w2", now.Add(2*time.Second), time.Minute, 10)
	if err != nil || len(sent) != 1 {
		t.Fatalf("re-lease outbox: %v %d", err, len(sent))
	}
}

func TestMemoryCompleteInboxDuplicateOutboxIsIdempotent(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	now := time.Now()
	if err := st.InsertInbox(ctx, newInbox("a", "k1"), now); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := st.LeaseInbox(ctx, "w1", now, time.Minute, 10); err != nil {
		t.Fatalf("lease: %v", err)
	}
	outbox := &OutboxRecord{OutboxID: "o1", DedupKey: "k1:outbox", Payload: []byte(`{}`)}
	if err := st.CompleteInbox(ctx, "a", "w1", outbox, now); err != nil {
		t.Fatalf("complete: %v", err)
	}
	// Re-inserting the same dedup key (crash between commit and ACK replay)
	// must not create a second delivery.
	if err := st.CompleteInbox(ctx, "a", "w1", outbox, now); err == nil {
		t.Fatal("second completion should lose the lease (already processed)")
	} else if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("second completion = %v, want ErrLeaseLost", err)
	}
}

func TestMemoryCompleteInboxBatchIsAtomicOrderedAndExactIdempotent(t *testing.T) {
	st := NewMemory()
	ctx := context.Background()
	now := time.Now()
	firstInbox := newInbox("batch-1", "batch-in-1")
	firstInbox.PartitionKey = "session-batch"
	if err := st.InsertInbox(ctx, firstInbox, now); err != nil {
		t.Fatal(err)
	}
	if _, err := st.LeaseInbox(ctx, "batch-owner-1", now, time.Minute, 1); err != nil {
		t.Fatal(err)
	}
	parts := []OutboxRecord{
		{
			OutboxID: "batch-out-0", OperationKey: "batch-operation-0", OperationVersion: 1,
			PartIndex: 0, PartCount: 2, TenantID: "acme", ChannelType: "telegram", BindingID: "b1",
			DedupKey: "batch-operation-0", PartitionKey: "caller-must-not-redirect", Payload: []byte(`{"text":"zero"}`),
		},
		{
			OutboxID: "batch-out-1", OperationKey: "batch-operation-1", OperationVersion: 1,
			PartIndex: 1, PartCount: 2, TenantID: "acme", ChannelType: "telegram", BindingID: "b1",
			DedupKey: "batch-operation-1", Payload: []byte(`{"text":"one"}`),
		},
	}
	if err := st.CompleteInboxBatch(ctx, firstInbox.InboxID, "batch-owner-1", parts, now); err != nil {
		t.Fatalf("complete batch: %v", err)
	}
	if got := st.outbox["batch-out-0"]; got == nil || got.PartitionKey != firstInbox.PartitionKey ||
		got.DeliveryState != DeliveryPending || got.PayloadHash == "" {
		t.Fatalf("normalized first part = %+v", got)
	}
	if st.orderOut["batch-out-0"] >= st.orderOut["batch-out-1"] {
		t.Fatalf("batch order = %d then %d", st.orderOut["batch-out-0"], st.orderOut["batch-out-1"])
	}

	// A second Inbox may replay the same deterministic operations with fresh
	// row IDs. Exact operation identity is idempotent and keeps canonical rows.
	replayInbox := newInbox("batch-2", "batch-in-2")
	replayInbox.PartitionKey = firstInbox.PartitionKey
	if err := st.InsertInbox(ctx, replayInbox, now); err != nil {
		t.Fatal(err)
	}
	if _, err := st.LeaseInbox(ctx, "batch-owner-2", now, time.Minute, 1); err != nil {
		t.Fatal(err)
	}
	replay := append([]OutboxRecord(nil), parts...)
	replay[0].OutboxID = "replayed-out-0"
	replay[1].OutboxID = "replayed-out-1"
	if err := st.CompleteInboxBatch(ctx, replayInbox.InboxID, "batch-owner-2", replay, now); err != nil {
		t.Fatalf("exact replay: %v", err)
	}
	if len(st.outbox) != 2 || st.outbox["replayed-out-0"] != nil || st.inbox[replayInbox.InboxID].Status != InboxProcessed {
		t.Fatalf("exact replay created rows or failed completion: outbox=%d inbox=%s", len(st.outbox), st.inbox[replayInbox.InboxID].Status)
	}

	// Preflight is all-or-nothing: even when a valid new part precedes a
	// conflicting replay, no part and no Inbox completion may leak.
	conflictInbox := newInbox("batch-3", "batch-in-3")
	conflictInbox.PartitionKey = firstInbox.PartitionKey
	if err := st.InsertInbox(ctx, conflictInbox, now); err != nil {
		t.Fatal(err)
	}
	if _, err := st.LeaseInbox(ctx, "batch-owner-3", now, time.Minute, 1); err != nil {
		t.Fatal(err)
	}
	conflict := parts[0]
	conflict.OutboxID = "conflicting-replay"
	conflict.Payload = []byte(`{"text":"different"}`)
	conflict.PayloadHash = ""
	err := st.CompleteInboxBatch(ctx, conflictInbox.InboxID, "batch-owner-3", []OutboxRecord{
		{
			OutboxID: "must-rollback", OperationKey: "new-before-conflict", OperationVersion: 1,
			PartIndex: 0, PartCount: 1, TenantID: "acme", ChannelType: "telegram", BindingID: "b1",
			Payload: []byte(`{"text":"new"}`),
		},
		conflict,
	}, now)
	if !errors.Is(err, ErrOperationConflict) {
		t.Fatalf("conflicting operation = %v, want ErrOperationConflict", err)
	}
	if st.outbox["must-rollback"] != nil || st.inbox[conflictInbox.InboxID].Status != InboxProcessing {
		t.Fatalf("conflicting batch leaked mutation: outbox=%v inbox=%s", st.outbox["must-rollback"], st.inbox[conflictInbox.InboxID].Status)
	}
}

func TestMemoryOutboxAttemptStateMachineAndLegacyWrappers(t *testing.T) {
	tests := []struct {
		name          string
		result        delivery.Result
		dispatch      bool
		exhausted     bool
		wantStatus    string
		wantDelivery  string
		wantPending   int
		wantDead      int
		wantUncertain int
	}{
		{
			name: "confirmed", dispatch: true,
			result:     delivery.Result{Outcome: delivery.Confirmed, ProviderMessageID: "provider-1", ResponseHash: "response-hash"},
			wantStatus: OutboxSent, wantDelivery: DeliveryConfirmed,
		},
		{
			name: "retryable", dispatch: true, result: delivery.Result{Outcome: delivery.RetryableNotSent, ErrorType: "rate_limited"},
			wantStatus: OutboxRetry, wantDelivery: DeliveryRetryableNotSent, wantPending: 1,
		},
		{
			name: "retry exhausted", dispatch: true, result: delivery.Result{Outcome: delivery.RetryableNotSent, ErrorType: "rate_limited"}, exhausted: true,
			wantStatus: OutboxDead, wantDelivery: DeliveryRetryExhausted, wantDead: 1,
		},
		{
			name: "permanent", dispatch: true, result: delivery.Result{Outcome: delivery.PermanentRejected, ErrorType: "target_missing"},
			wantStatus: OutboxDead, wantDelivery: DeliveryPermanentRejected, wantDead: 1,
		},
		{
			name: "unknown", dispatch: true,
			result:     delivery.Result{Outcome: delivery.Unknown, ErrorType: "response_lost", ProviderRequestID: "request-1"},
			wantStatus: OutboxSending, wantDelivery: DeliveryUnknown, wantUncertain: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := NewMemory()
			now := time.Now()
			enqueueMemoryOutbox(t, st, "attempt-in", "attempt-op", "session-attempt", now)
			leased, err := st.LeaseOutbox(context.Background(), "attempt-owner", now, time.Minute, 1)
			if err != nil || len(leased) != 1 {
				t.Fatalf("lease: records=%+v err=%v", leased, err)
			}
			attemptNo := leased[0].AttemptCount
			attempt := st.attempts[leased[0].OutboxID][attemptNo]
			if attempt == nil || attempt.Phase != AttemptLeased || leased[0].AttemptPhase != AttemptLeased {
				t.Fatalf("leased attempt = %+v row=%+v", attempt, leased[0])
			}
			if tt.dispatch {
				if err := st.MarkOutboxDispatched(context.Background(), leased[0].OutboxID, "attempt-owner", attemptNo, now); err != nil {
					t.Fatalf("mark dispatched: %v", err)
				}
			}
			if err := st.FinishOutboxAttempt(context.Background(), leased[0].OutboxID, "attempt-owner", attemptNo,
				tt.result, now, now.Add(time.Hour), tt.exhausted); err != nil {
				t.Fatalf("finish: %v", err)
			}
			rec := st.outbox[leased[0].OutboxID]
			if rec.Status != tt.wantStatus || rec.DeliveryState != tt.wantDelivery || rec.LeaseExpiresAt != (time.Time{}) {
				t.Fatalf("finished row = %+v", rec)
			}
			attempt = st.attempts[rec.OutboxID][attemptNo]
			if attempt.Phase != AttemptFinished || attempt.Outcome != tt.result.Outcome ||
				attempt.ProviderMessageID != tt.result.ProviderMessageID || attempt.ProviderRequestID != tt.result.ProviderRequestID {
				t.Fatalf("finished attempt = %+v", attempt)
			}
			_, _, pending, dead, uncertain, err := st.Depths(context.Background())
			if err != nil || pending != tt.wantPending || dead != tt.wantDead || uncertain != tt.wantUncertain {
				t.Fatalf("depths pending=%d dead=%d uncertain=%d err=%v", pending, dead, uncertain, err)
			}
		})
	}

	// Existing Complete/Retry/Dead methods remain ledger-aware while callers
	// migrate to structured results.
	legacy := []struct {
		name         string
		finish       func(*Memory, string, time.Time) error
		wantOutcome  delivery.Outcome
		wantDelivery string
	}{
		{name: "complete", finish: func(st *Memory, id string, now time.Time) error {
			return st.CompleteOutbox(context.Background(), id, "legacy-owner", now)
		}, wantOutcome: delivery.Confirmed, wantDelivery: DeliveryConfirmed},
		{name: "retry", finish: func(st *Memory, id string, now time.Time) error {
			return st.RetryOutbox(context.Background(), id, "legacy-owner", "temporary", now, now.Add(time.Minute))
		}, wantOutcome: delivery.RetryableNotSent, wantDelivery: DeliveryRetryableNotSent},
		{name: "dead", finish: func(st *Memory, id string, now time.Time) error {
			return st.DeadLetterOutbox(context.Background(), id, "legacy-owner", "rejected", now)
		}, wantOutcome: delivery.PermanentRejected, wantDelivery: DeliveryPermanentRejected},
	}
	for _, tt := range legacy {
		t.Run("legacy "+tt.name, func(t *testing.T) {
			st := NewMemory()
			now := time.Now()
			enqueueMemoryOutbox(t, st, "legacy-in", "legacy-op", "legacy-session", now)
			leased, err := st.LeaseOutbox(context.Background(), "legacy-owner", now, time.Minute, 1)
			if err != nil || len(leased) != 1 {
				t.Fatalf("lease: records=%+v err=%v", leased, err)
			}
			if err := tt.finish(st, leased[0].OutboxID, now); err != nil {
				t.Fatalf("legacy finish: %v", err)
			}
			attempt := st.attempts[leased[0].OutboxID][leased[0].AttemptCount]
			if attempt == nil || attempt.Phase != AttemptFinished || attempt.Outcome != tt.wantOutcome ||
				st.outbox[leased[0].OutboxID].DeliveryState != tt.wantDelivery {
				t.Fatalf("legacy ledger row=%+v attempt=%+v", st.outbox[leased[0].OutboxID], attempt)
			}
		})
	}
}

func TestMemoryReclaimDistinguishesLeasedDispatchedAndLegacy(t *testing.T) {
	t.Run("leased is known not sent", func(t *testing.T) {
		st := NewMemory()
		now := time.Now()
		enqueueMemoryOutbox(t, st, "leased-in", "leased-op", "session-leased", now)
		leased, err := st.LeaseOutbox(context.Background(), "leased-owner", now, time.Second, 1)
		if err != nil || len(leased) != 1 {
			t.Fatalf("lease: records=%+v err=%v", leased, err)
		}
		_, reclaimed, err := st.ReclaimExpired(context.Background(), now.Add(2*time.Second))
		if err != nil || reclaimed != 1 {
			t.Fatalf("reclaim: count=%d err=%v", reclaimed, err)
		}
		rec := st.outbox[leased[0].OutboxID]
		attempt := st.attempts[rec.OutboxID][rec.AttemptCount]
		if rec.Status != OutboxRetry || rec.DeliveryState != DeliveryRetryableNotSent ||
			attempt.Outcome != delivery.RetryableNotSent {
			t.Fatalf("leased reclaim row=%+v attempt=%+v", rec, attempt)
		}
	})

	t.Run("dispatched becomes unknown and blocks only its session", func(t *testing.T) {
		st := NewMemory()
		now := time.Now()
		enqueueMemoryOutboxParts(t, st, "unknown-in", "session-unknown", now, "unknown-head", "unknown-successor")
		leased, err := st.LeaseOutbox(context.Background(), "unknown-owner", now, time.Second, 1)
		if err != nil || len(leased) != 1 || leased[0].OperationKey != "unknown-head" {
			t.Fatalf("lease head: records=%+v err=%v", leased, err)
		}
		if err := st.MarkOutboxDispatched(context.Background(), leased[0].OutboxID, "unknown-owner", leased[0].AttemptCount, now); err != nil {
			t.Fatal(err)
		}
		_, reclaimed, err := st.ReclaimExpired(context.Background(), now.Add(2*time.Second))
		if err != nil || reclaimed != 1 {
			t.Fatalf("reclaim dispatched: count=%d err=%v", reclaimed, err)
		}
		rec := st.outbox[leased[0].OutboxID]
		if rec.Status != OutboxSending || rec.DeliveryState != DeliveryUnknown ||
			st.attempts[rec.OutboxID][rec.AttemptCount].Outcome != delivery.Unknown {
			t.Fatalf("unknown reclaim row=%+v attempt=%+v", rec, st.attempts[rec.OutboxID][rec.AttemptCount])
		}
		if _, again, err := st.ReclaimExpired(context.Background(), now.Add(3*time.Second)); err != nil || again != 0 {
			t.Fatalf("unknown was reclaimed again: count=%d err=%v", again, err)
		}
		blocked, err := st.LeaseOutbox(context.Background(), "blocked-owner", now.Add(3*time.Second), time.Minute, 10)
		if err != nil || len(blocked) != 0 {
			t.Fatalf("same-session successor passed unknown: records=%+v err=%v", blocked, err)
		}

		enqueueMemoryOutbox(t, st, "other-in", "other-operation", "session-other", now.Add(4*time.Second))
		other, err := st.LeaseOutbox(context.Background(), "other-owner", now.Add(4*time.Second), time.Minute, 10)
		if err != nil || len(other) != 1 || other[0].OperationKey != "other-operation" {
			t.Fatalf("cross-session operation blocked: records=%+v err=%v", other, err)
		}
	})

	t.Run("legacy in-flight is conservatively unknown", func(t *testing.T) {
		st := NewMemory()
		now := time.Now()
		enqueueMemoryOutbox(t, st, "legacy-reclaim-in", "legacy-reclaim-op", "session-legacy-reclaim", now)
		leased, err := st.LeaseOutbox(context.Background(), "legacy-reclaim-owner", now, time.Second, 1)
		if err != nil || len(leased) != 1 {
			t.Fatalf("lease: records=%+v err=%v", leased, err)
		}
		delete(st.attempts, leased[0].OutboxID)
		st.outbox[leased[0].OutboxID].DeliveryState = ""
		st.outbox[leased[0].OutboxID].AttemptPhase = ""
		_, reclaimed, err := st.ReclaimExpired(context.Background(), now.Add(2*time.Second))
		if err != nil || reclaimed != 1 || st.outbox[leased[0].OutboxID].DeliveryState != DeliveryUnknown {
			t.Fatalf("legacy reclaim: count=%d row=%+v err=%v", reclaimed, st.outbox[leased[0].OutboxID], err)
		}
	})
}

func TestMemoryResolveUnknownIsAuditedCASAndControlsFIFO(t *testing.T) {
	tests := []struct {
		name         string
		action       string
		wantStatus   string
		wantDelivery string
		wantNext     string
	}{
		{name: "assume delivered", action: ResolveAssumeDelivered, wantStatus: OutboxSent, wantDelivery: DeliveryConfirmed, wantNext: "resolve-successor"},
		{name: "retry", action: ResolveRetry, wantStatus: OutboxRetry, wantDelivery: DeliveryRetryableNotSent, wantNext: "resolve-head"},
		{name: "cancel", action: ResolveCancel, wantStatus: OutboxDead, wantDelivery: DeliveryCanceled, wantNext: "resolve-successor"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := NewMemory()
			now := time.Now()
			enqueueMemoryOutboxParts(t, st, "resolve-in", "session-resolve", now, "resolve-head", "resolve-successor")
			leased, err := st.LeaseOutbox(context.Background(), "resolve-owner", now, time.Second, 1)
			if err != nil || len(leased) != 1 {
				t.Fatalf("lease: records=%+v err=%v", leased, err)
			}
			if err := st.MarkOutboxDispatched(context.Background(), leased[0].OutboxID, "resolve-owner", leased[0].AttemptCount, now); err != nil {
				t.Fatal(err)
			}
			if _, _, err := st.ReclaimExpired(context.Background(), now.Add(2*time.Second)); err != nil {
				t.Fatal(err)
			}
			unknown, err := st.ListUncertainOutbox(context.Background(), 10)
			if err != nil || len(unknown) != 1 {
				t.Fatalf("list uncertain: records=%+v err=%v", unknown, err)
			}
			req := ResolveRequest{
				ResolutionID: "resolution-" + tt.action, OutboxID: unknown[0].OutboxID,
				ExpectedVersion: unknown[0].StateVersion, ExpectedAttempt: unknown[0].AttemptCount,
				Action: tt.action, Actor: "operator-1", Reason: "verified", Now: now.Add(3 * time.Second),
			}
			if err := st.ResolveOutbox(context.Background(), req); err != nil {
				t.Fatalf("resolve: %v", err)
			}
			rec := st.outbox[unknown[0].OutboxID]
			if rec.Status != tt.wantStatus || rec.DeliveryState != tt.wantDelivery || len(st.resolutions) != 1 {
				t.Fatalf("resolved row=%+v resolutions=%+v", rec, st.resolutions)
			}
			// Same resolution ID is idempotent even if the caller regenerates Now.
			req.Now = req.Now.Add(time.Minute)
			if err := st.ResolveOutbox(context.Background(), req); err != nil || len(st.resolutions) != 1 {
				t.Fatalf("resolution replay: err=%v resolutions=%d", err, len(st.resolutions))
			}
			conflict := req
			conflict.Action = ResolveCancel
			if conflict.Action == tt.action {
				conflict.Action = ResolveRetry
			}
			if err := st.ResolveOutbox(context.Background(), conflict); !errors.Is(err, ErrOperationConflict) {
				t.Fatalf("resolution ID conflict = %v", err)
			}
			stale := req
			stale.ResolutionID += "-stale"
			if err := st.ResolveOutbox(context.Background(), stale); !errors.Is(err, ErrInvalidTransition) {
				t.Fatalf("stale CAS = %v", err)
			}

			next, err := st.LeaseOutbox(context.Background(), "after-resolution", now.Add(4*time.Second), time.Minute, 1)
			if err != nil || len(next) != 1 || next[0].OperationKey != tt.wantNext {
				t.Fatalf("next after resolution: records=%+v err=%v", next, err)
			}
		})
	}
}

func enqueueMemoryOutbox(t *testing.T, st *Memory, inboxID, operationKey, partition string, now time.Time) {
	t.Helper()
	enqueueMemoryOutboxParts(t, st, inboxID, partition, now, operationKey)
}

func enqueueMemoryOutboxParts(t *testing.T, st *Memory, inboxID, partition string, now time.Time, operationKeys ...string) {
	t.Helper()
	inbox := newInbox(inboxID, "dedup-"+inboxID)
	inbox.PartitionKey = partition
	if err := st.InsertInbox(context.Background(), inbox, now); err != nil {
		t.Fatalf("insert inbox: %v", err)
	}
	owner := "inbox-owner-" + inboxID
	leased, err := st.LeaseInbox(context.Background(), owner, now, time.Minute, 1)
	if err != nil || len(leased) != 1 || leased[0].InboxID != inboxID {
		t.Fatalf("lease inbox: records=%+v err=%v", leased, err)
	}
	parts := make([]OutboxRecord, 0, len(operationKeys))
	for index, operationKey := range operationKeys {
		parts = append(parts, OutboxRecord{
			OutboxID: operationKey + "-id", OperationKey: operationKey, OperationVersion: 1,
			PartIndex: index, PartCount: len(operationKeys), TenantID: "acme", ChannelType: "telegram", BindingID: "b1",
			DedupKey: operationKey, Payload: []byte(`{"text":"` + operationKey + `"}`),
		})
	}
	if err := st.CompleteInboxBatch(context.Background(), inboxID, owner, parts, now); err != nil {
		t.Fatalf("complete inbox batch: %v", err)
	}
}
