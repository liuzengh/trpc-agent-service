package reliable

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func newTestInbox() *InboxMessage {
	return &InboxMessage{
		TenantID:          "tenant-a",
		ChannelType:       "wework",
		ChannelAccountID:  "corp-a",
		AgentApp:          "assistant",
		ExternalMessageID: "message-1",
		ConversationID:    "conversation-1",
		ReplyToID:         "provider-message-1",
		UserID:            "user-1",
		SessionID:         "session-1",
		PayloadHash:       strings.Repeat("a", 64),
		Payload:           []byte(`{"content":"hello"}`),
	}
}

func TestMemoryStoreDeduplicatesAndDetectsConflict(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	first := newTestInbox()
	inserted, err := store.EnqueueInbox(ctx, first)
	if err != nil || !inserted {
		t.Fatalf("first enqueue: inserted=%v err=%v", inserted, err)
	}

	duplicate := newTestInbox()
	inserted, err = store.EnqueueInbox(ctx, duplicate)
	if err != nil || inserted || duplicate.ID != first.ID {
		t.Fatalf("duplicate enqueue: inserted=%v id=%d err=%v", inserted, duplicate.ID, err)
	}
	if first.SessionSequence != 1 || duplicate.SessionSequence != first.SessionSequence {
		t.Fatalf("duplicate sequence first=%d duplicate=%d", first.SessionSequence, duplicate.SessionSequence)
	}
	third := newTestInbox()
	third.ExternalMessageID = "message-2"
	third.ReplyToID = "provider-message-2"
	if inserted, err := store.EnqueueInbox(ctx, third); err != nil || !inserted {
		t.Fatalf("third enqueue: inserted=%v err=%v", inserted, err)
	}
	if third.SessionSequence != 2 {
		t.Fatalf("sequence after duplicate=%d, want 2", third.SessionSequence)
	}

	conflict := newTestInbox()
	conflict.PayloadHash = strings.Repeat("b", 64)
	if _, err := store.EnqueueInbox(ctx, conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}
}

func TestMemoryStoreInspectQueueReportsAutomaticWorkOnly(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	first := newTestInbox()
	if _, err := store.EnqueueInbox(ctx, first); err != nil {
		t.Fatal(err)
	}
	second := newTestInbox()
	second.ExternalMessageID = "message-2"
	second.ReplyToID = "provider-message-2"
	if _, err := store.EnqueueInbox(ctx, second); err != nil {
		t.Fatal(err)
	}
	stats, err := store.InspectQueue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.InboxDepth != 2 || stats.InboxOldest.IsZero() || stats.OutboxDepth != 0 || !stats.OutboxOldest.IsZero() {
		t.Fatalf("initial queue stats=%+v", stats)
	}
	claim, err := store.ClaimInbox(ctx, "consumer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.CompleteInbox(ctx, claim.ID, claim.Lease, OutboxReply{Content: "reply"})
	if err != nil {
		t.Fatal(err)
	}
	stats, err = store.InspectQueue(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.InboxDepth != 1 || stats.OutboxDepth != 1 || stats.OutboxOldest.IsZero() {
		t.Fatalf("post-completion queue stats=%+v", stats)
	}
	if err := store.ReplayInbox(ctx, claim.ID, "operator", "test"); err == nil {
		t.Fatal("completed Inbox unexpectedly replayed")
	}
}

func TestMemoryStoreFairClaimRespectsTenantInflightLimit(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	first := newTestInbox()
	second := newTestInbox()
	second.ExternalMessageID = "message-2"
	second.ReplyToID = "provider-message-2"
	second.SessionID = "session-2"
	other := newTestInbox()
	other.TenantID = "tenant-b"
	other.ChannelAccountID = "corp-b"
	other.ExternalMessageID = "message-b"
	other.ReplyToID = "provider-message-b"
	other.ConversationID = "conversation-b"
	other.SessionID = "session-b"
	other.UserID = "user-b"
	for _, msg := range []*InboxMessage{first, second, other} {
		if _, err := store.EnqueueInbox(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.UpsertTenantQueuePolicy(ctx, TenantQueuePolicy{TenantID: "tenant-a", Weight: 1, MaxInflight: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertTenantQueuePolicy(ctx, TenantQueuePolicy{TenantID: "tenant-b", Weight: 1}); err != nil {
		t.Fatal(err)
	}
	claimA, err := store.ClaimInboxFair(ctx, "consumer-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claimA.TenantID != "tenant-a" {
		t.Fatalf("first fair claim tenant=%q, want tenant-a", claimA.TenantID)
	}
	claimB, err := store.ClaimInboxFair(ctx, "consumer-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claimB.TenantID != "tenant-b" {
		t.Fatalf("second fair claim tenant=%q, want tenant-b while tenant-a is at MaxInflight", claimB.TenantID)
	}
}

func TestMemoryStoreFairClaimHonorsWeightOverSustainedLoad(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.UpsertTenantQueuePolicy(ctx, TenantQueuePolicy{TenantID: "tenant-a", Weight: 4}); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertTenantQueuePolicy(ctx, TenantQueuePolicy{TenantID: "tenant-b", Weight: 1}); err != nil {
		t.Fatal(err)
	}
	const perTenant = 40
	for i := 0; i < perTenant; i++ {
		for _, tenant := range []string{"tenant-a", "tenant-b"} {
			msg := newTestInbox()
			msg.TenantID = tenant
			msg.ChannelAccountID = "account-" + tenant
			msg.ExternalMessageID = fmt.Sprintf("message-%s-%d", tenant, i)
			msg.ReplyToID = fmt.Sprintf("reply-%s-%d", tenant, i)
			msg.ConversationID = fmt.Sprintf("conversation-%s-%d", tenant, i)
			msg.SessionID = fmt.Sprintf("session-%s-%d", tenant, i)
			if _, err := store.EnqueueInbox(ctx, msg); err != nil {
				t.Fatal(err)
			}
		}
	}

	counts := map[string]int{}
	for i := 0; i < perTenant+perTenant/4; i++ {
		claim, err := store.ClaimInboxFair(ctx, "consumer", time.Minute)
		if err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		counts[claim.TenantID]++
	}
	if counts["tenant-a"] != 40 || counts["tenant-b"] != 10 {
		t.Fatalf("weighted claims=%v, want tenant-a=40 tenant-b=10", counts)
	}
}

func TestMemoryStoreFairClaimDoesNotResumeExpiredApproval(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	msg := newTestInbox()
	if _, err := store.EnqueueInbox(ctx, msg); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WaitInboxApproval(ctx, claim.ID, claim.Lease, errors.New("approval required"), time.Second, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return now.Add(2 * time.Minute) }
	if _, err := store.ClaimInboxFair(ctx, "consumer-2", time.Minute); !errors.Is(err, ErrNoWork) {
		t.Fatalf("expired approval fair claim err=%v, want ErrNoWork", err)
	}
}

func TestMemoryStoreTenantQueuePolicyValidation(t *testing.T) {
	store := NewMemoryStore()
	for _, policy := range []TenantQueuePolicy{
		{},
		{TenantID: "tenant-a", Weight: 0},
		{TenantID: "tenant-a", Weight: 1, MaxQueued: -1},
		{TenantID: "tenant-a", Weight: 1, MaxInflight: -1},
	} {
		if err := store.UpsertTenantQueuePolicy(context.Background(), policy); !errors.Is(err, ErrInvalidTenantQueuePolicy) {
			t.Fatalf("policy %+v error=%v, want ErrInvalidTenantQueuePolicy", policy, err)
		}
	}
}

func TestMemoryStoreQueueAdmissionIsAtomicAndDoesNotConsumeSequence(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.UpsertTenantQueuePolicy(ctx, TenantQueuePolicy{TenantID: "tenant-a", Weight: 1, MaxQueued: 1}); err != nil {
		t.Fatal(err)
	}
	first := newTestInbox()
	if inserted, err := store.EnqueueInboxWithAdmission(ctx, first); err != nil || !inserted {
		t.Fatalf("first admitted enqueue inserted=%v err=%v", inserted, err)
	}
	second := newTestInbox()
	second.ExternalMessageID = "message-2"
	second.ReplyToID = "provider-message-2"
	if inserted, err := store.EnqueueInboxWithAdmission(ctx, second); !errors.Is(err, ErrTenantQueueFull) || inserted {
		t.Fatalf("second enqueue inserted=%v err=%v, want atomic queue-full rejection", inserted, err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if claim.SessionSequence != 1 {
		t.Fatalf("queue rejection consumed session sequence: got %d, want 1", claim.SessionSequence)
	}
	duplicate := newTestInbox()
	if inserted, err := store.EnqueueInboxWithAdmission(ctx, duplicate); err != nil || inserted || duplicate.ID != first.ID {
		t.Fatalf("duplicate redelivery under full queue inserted=%v id=%d err=%v", inserted, duplicate.ID, err)
	}
}

func TestMemoryStoreDeletingQueuePolicyRestoresDefaultAndKeepsBacklogClaimable(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.UpsertTenantQueuePolicy(ctx, TenantQueuePolicy{TenantID: "tenant-a", Weight: 1, MaxQueued: 1}); err != nil {
		t.Fatal(err)
	}
	first := newTestInbox()
	if inserted, err := store.EnqueueInboxWithAdmission(ctx, first); err != nil || !inserted {
		t.Fatalf("first admitted enqueue inserted=%v err=%v", inserted, err)
	}
	if err := store.DeleteTenantQueuePolicy(ctx, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	second := newTestInbox()
	second.ExternalMessageID = "message-2"
	second.ReplyToID = "provider-message-2"
	if inserted, err := store.EnqueueInboxWithAdmission(ctx, second); err != nil || !inserted {
		t.Fatalf("enqueue after policy reset inserted=%v err=%v", inserted, err)
	}
	claim, err := store.ClaimInboxFair(ctx, "consumer", time.Minute)
	if err != nil || claim == nil {
		t.Fatalf("fair claim after policy reset claim=%+v err=%v", claim, err)
	}
}

func TestMemoryStoreDispatchFenceBlocksExpiredAutomaticResend(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	if _, err := store.EnqueueInbox(ctx, newTestInbox()); err != nil {
		t.Fatal(err)
	}
	inbox, err := store.ClaimInbox(ctx, "consumer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := store.CompleteInbox(ctx, inbox.ID, inbox.Lease, OutboxReply{Content: "reply"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimOutbox(ctx, "delivery-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDispatchStarted(ctx, claim.ID, claim.Lease); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	if _, err := store.ClaimOutbox(ctx, "delivery-b", time.Minute); !errors.Is(err, ErrNoWork) {
		t.Fatalf("dispatch-started row was automatically reclaimed: %v", err)
	}
	result, err := store.ReapExpired(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.OutboxDispatchUnknown != 1 || result.OutboxFinalAttemptExpired != 0 {
		t.Fatalf("unexpected dispatch reaping result: %+v", result)
	}
	if err := store.ReplayOutbox(ctx, outbox.ID, "operator", "provider reconciled"); err != nil {
		t.Fatal(err)
	}
	replayed, err := store.ClaimOutbox(ctx, "delivery-c", time.Minute)
	if err != nil || replayed.DeliveryCursor != 0 {
		t.Fatalf("replayed outbox=%#v err=%v", replayed, err)
	}
}

func TestMemoryStoreDispatchFenceAllowsPreDispatchReclaim(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 8, 28, 13, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	if _, err := store.EnqueueInbox(ctx, newTestInbox()); err != nil {
		t.Fatal(err)
	}
	inbox, err := store.ClaimInbox(ctx, "consumer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteInbox(ctx, inbox.ID, inbox.Lease, OutboxReply{Content: "reply"}); err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimOutbox(ctx, "delivery-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	second, err := store.ClaimOutbox(ctx, "delivery-b", time.Minute)
	if err != nil || second.Lease.Owner != "delivery-b" {
		t.Fatalf("pre-dispatch expired row was not reclaimable: %#v err=%v", second, err)
	}
	if first.Lease.Fence == second.Lease.Fence {
		t.Fatalf("reclaim did not advance fence: first=%d second=%d", first.Lease.Fence, second.Lease.Fence)
	}
}

func TestMemoryStoreClaimRejectsInvalidLeaseOwner(t *testing.T) {
	store := NewMemoryStore()
	for _, owner := range []string{"", string([]byte{0xff}), "bad\nowner", strings.Repeat("x", 257)} {
		if _, err := store.ClaimInbox(context.Background(), owner, time.Minute); err == nil {
			t.Fatalf("ClaimInbox accepted invalid owner %q", owner)
		}
		if _, err := store.ClaimOutbox(context.Background(), owner, time.Minute); err == nil {
			t.Fatalf("ClaimOutbox accepted invalid owner %q", owner)
		}
	}
}

func TestMemoryStoreRejectsDuplicateWithMismatchedRoutingIdentity(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	first := newTestInbox()
	if inserted, err := store.EnqueueInbox(ctx, first); err != nil || !inserted {
		t.Fatalf("first enqueue: inserted=%v err=%v", inserted, err)
	}

	cases := []struct {
		name   string
		mutate func(*InboxMessage)
	}{
		{name: "agent app", mutate: func(msg *InboxMessage) { msg.AgentApp = "other-agent" }},
		{name: "conversation", mutate: func(msg *InboxMessage) { msg.ConversationID = "other-conversation" }},
		{name: "reply target", mutate: func(msg *InboxMessage) { msg.ReplyToID = "other-reply" }},
		{name: "user", mutate: func(msg *InboxMessage) { msg.UserID = "other-user" }},
		{name: "session", mutate: func(msg *InboxMessage) { msg.SessionID = "other-session" }},
		{name: "payload hash", mutate: func(msg *InboxMessage) { msg.PayloadHash = strings.Repeat("b", 64) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			duplicate := newTestInbox()
			tc.mutate(duplicate)
			if inserted, err := store.EnqueueInbox(ctx, duplicate); inserted || !errors.Is(err, ErrIdempotencyConflict) {
				t.Fatalf("duplicate enqueue: inserted=%v err=%v, want conflict", inserted, err)
			}
		})
	}
}

func TestMemoryStoreTreatsPayloadHashHexCaseAsSameIdentity(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	first := newTestInbox()
	if inserted, err := store.EnqueueInbox(ctx, first); err != nil || !inserted {
		t.Fatalf("first enqueue: inserted=%v err=%v", inserted, err)
	}
	duplicate := newTestInbox()
	duplicate.PayloadHash = strings.ToUpper(duplicate.PayloadHash)
	if inserted, err := store.EnqueueInbox(ctx, duplicate); err != nil || inserted {
		t.Fatalf("case-insensitive duplicate enqueue: inserted=%v err=%v", inserted, err)
	}
}

func TestMemoryStoreRejectsStaleFenceAndCreatesOutboxAtomically(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	msg := newTestInbox()
	if _, err := store.EnqueueInbox(ctx, msg); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	stale := claim.Lease
	stale.Fence--
	if _, err := store.CompleteInbox(ctx, claim.ID, stale, OutboxReply{Content: "world"}); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("expected stale lease, got %v", err)
	}

	reply := OutboxReply{
		ContentType: "text",
		Content:     "world",
	}
	created, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, reply)
	if err != nil {
		t.Fatal(err)
	}
	if created.TenantID != claim.TenantID || created.ChannelType != claim.ChannelType ||
		created.ChannelAccountID != claim.ChannelAccountID || created.ConversationID != claim.ConversationID ||
		created.ReplyToID != claim.ReplyToID || created.AgentApp != claim.AgentApp ||
		created.SessionID != claim.SessionID || created.SessionSequence != claim.SessionSequence {
		t.Fatalf("outbox route was not derived from inbox: %#v", created)
	}
	if created.MaxAttempts != 8 {
		t.Fatalf("default outbox max attempts=%d, want 8", created.MaxAttempts)
	}
	out, err := store.ClaimOutbox(ctx, "delivery-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if out.InboxID != claim.ID || out.Content != "world" {
		t.Fatalf("unexpected outbox: %#v", out)
	}
	if err := store.MarkDispatchStarted(ctx, out.ID, out.Lease); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDelivered(ctx, out.ID, out.Lease); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryStoreReclaimsExpiredLeaseWithHigherFence(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	msg := newTestInbox()
	if _, err := store.EnqueueInbox(ctx, msg); err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimInbox(ctx, "consumer-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	second, err := store.ClaimInbox(ctx, "consumer-b", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if second.Lease.Fence <= first.Lease.Fence {
		t.Fatalf("fence did not increase: first=%d second=%d", first.Lease.Fence, second.Lease.Fence)
	}
	if err := store.RetryInbox(ctx, first.ID, first.Lease, errors.New("late"), now); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale worker mutation was not rejected: %v", err)
	}
}

func TestMemoryStoreRecordsReasonWhenFinalLeaseExpires(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	message := newTestInbox()
	message.MaxAttempts = 1
	if _, err := store.EnqueueInbox(ctx, message); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer-a", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Millisecond)
	if _, err := store.ClaimInbox(ctx, "consumer-b", time.Minute); !errors.Is(err, ErrNoWork) {
		t.Fatalf("expired final attempt should not be claimable: %v", err)
	}
	result, err := store.ReapExpired(ctx, 10)
	if err != nil {
		t.Fatalf("reap expired: %v", err)
	}
	if result.InboxFinalAttemptExpired != 1 || result.InboxApprovalExpired != 0 || result.OutboxFinalAttemptExpired != 0 {
		t.Fatalf("unexpected reap result: %+v", result)
	}
	if claim.LastError != "" {
		t.Fatalf("claim snapshot unexpectedly mutated: %q", claim.LastError)
	}
	store.mu.Lock()
	lastError := store.inbox[message.ID].LastError
	status := store.inbox[message.ID].Status
	store.mu.Unlock()
	if status != InboxDeadLetter || lastError != ErrLeaseExpiredAfterFinalAttempt.Error() {
		t.Fatalf("final lease expiry status=%s last_error=%q", status, lastError)
	}
}

func TestMemoryStoreNeverReusesFenceForSameOwner(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 8, 24, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	message := newTestInbox()
	if _, err := store.EnqueueInbox(ctx, message); err != nil {
		t.Fatal(err)
	}
	oldClaim, err := store.ClaimInbox(ctx, "consumer-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RetryInbox(ctx, oldClaim.ID, oldClaim.Lease, errors.New("retry"), now); err != nil {
		t.Fatal(err)
	}
	newClaim, err := store.ClaimInbox(ctx, "consumer-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if newClaim.Lease.Fence <= oldClaim.Lease.Fence {
		t.Fatalf("fence was reused: old=%d new=%d", oldClaim.Lease.Fence, newClaim.Lease.Fence)
	}
	if _, err := store.CompleteInbox(ctx, oldClaim.ID, oldClaim.Lease, OutboxReply{Content: "stale"}); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("old inbox lease was accepted after same-owner reclaim: %v", err)
	}
	if _, err := store.CompleteInbox(ctx, newClaim.ID, newClaim.Lease, OutboxReply{Content: "valid"}); err != nil {
		t.Fatal(err)
	}

	oldOutbox, err := store.ClaimOutbox(ctx, "delivery-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDispatchStarted(ctx, oldOutbox.ID, oldOutbox.Lease); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceOutbox(ctx, oldOutbox.ID, oldOutbox.Lease, 1); err != nil {
		t.Fatal(err)
	}
	newOutbox, err := store.ClaimOutbox(ctx, "delivery-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if newOutbox.Lease.Fence <= oldOutbox.Lease.Fence {
		t.Fatalf("outbox fence was reused: old=%d new=%d", oldOutbox.Lease.Fence, newOutbox.Lease.Fence)
	}
	if err := store.MarkDelivered(ctx, oldOutbox.ID, oldOutbox.Lease); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("old outbox lease was accepted after same-owner reclaim: %v", err)
	}
}

func TestMemoryStoreRequiresDispatchFenceForSuccessfulOutboxCommit(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if _, err := store.EnqueueInbox(ctx, newTestInbox()); err != nil {
		t.Fatal(err)
	}
	inbox, err := store.ClaimInbox(ctx, "consumer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := store.CompleteInbox(ctx, inbox.ID, inbox.Lease, OutboxReply{Content: "reply"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimOutbox(ctx, "delivery", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDelivered(ctx, claim.ID, claim.Lease); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("pre-dispatch MarkDelivered error=%v, want ErrStaleLease", err)
	}
	if err := store.AdvanceOutbox(ctx, claim.ID, claim.Lease, 1); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("pre-dispatch AdvanceOutbox error=%v, want ErrStaleLease", err)
	}
	if err := store.MarkDispatchStarted(ctx, outbox.ID, claim.Lease); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDelivered(ctx, claim.ID, claim.Lease); err != nil {
		t.Fatalf("dispatch-fenced MarkDelivered failed: %v", err)
	}
}

func TestMemoryStoreIgnoresCallerOwnedLifecycleFields(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	message := newTestInbox()
	nextAttempt := time.Now().Add(time.Hour)
	message.Status = InboxCompleted
	message.AttemptCount = 99
	message.NextAttemptAt = &nextAttempt
	message.Lease = Lease{Owner: "attacker", Fence: 99, Until: nextAttempt}
	message.LastError = "poisoned"
	if _, err := store.EnqueueInbox(ctx, message); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer-a", time.Minute)
	if err != nil {
		t.Fatalf("poisoned lifecycle fields made message unclaimable: %v", err)
	}
	if claim.Status != InboxProcessing || claim.AttemptCount != 1 || claim.Lease.Fence != 1 || claim.LastError != "" {
		t.Fatalf("caller lifecycle fields reached storage: %#v", claim)
	}
}

func TestMemoryStoreRejectsSubMillisecondLeases(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	message := newTestInbox()
	if _, err := store.EnqueueInbox(ctx, message); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimInbox(ctx, "consumer-a", time.Millisecond-time.Nanosecond); err == nil {
		t.Fatal("sub-millisecond inbox lease accepted")
	}
	claim, err := store.ClaimInbox(ctx, "consumer-a", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RenewInbox(ctx, claim.ID, claim.Lease, time.Millisecond-time.Nanosecond); err == nil {
		t.Fatal("sub-millisecond inbox renewal accepted")
	}
}

func TestMemoryStoreRejectsNegativeRetryDelays(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	inbox := newTestInbox()
	if _, err := store.EnqueueInbox(ctx, inbox); err != nil {
		t.Fatal(err)
	}
	inboxClaim, err := store.ClaimInbox(ctx, "consumer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RetryInboxAfter(ctx, inboxClaim.ID, inboxClaim.Lease, errors.New("temporary"), -time.Second); err == nil {
		t.Fatal("negative inbox retry delay was accepted")
	}

	if _, err := store.CompleteInbox(ctx, inboxClaim.ID, inboxClaim.Lease, OutboxReply{Content: "reply"}); err != nil {
		t.Fatal(err)
	}
	outboxClaim, err := store.ClaimOutbox(ctx, "delivery", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RetryOutboxAfter(ctx, outboxClaim.ID, outboxClaim.Lease, errors.New("temporary"), -time.Second, 0); err == nil {
		t.Fatal("negative outbox retry delay was accepted")
	}
}

func TestMemoryStoreApprovalWaitDoesNotConsumeAttemptsAndExpires(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	message := newTestInbox()
	if _, err := store.EnqueueInbox(ctx, message); err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimInbox(ctx, "consumer-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	deadline := now.Add(10 * time.Second)
	if err := store.WaitInboxApproval(ctx, first.ID, first.Lease,
		errors.New("tool approval required"), time.Second, deadline); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	second, err := store.ClaimInbox(ctx, "consumer-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.AttemptCount != first.AttemptCount {
		t.Fatalf("approval wait consumed attempt: first=%d second=%d", first.AttemptCount, second.AttemptCount)
	}
	if second.Status != InboxProcessing {
		t.Fatalf("approval retry status=%s, want %s", second.Status, InboxProcessing)
	}
	if err := store.WaitInboxApproval(ctx, second.ID, second.Lease,
		errors.New("tool approval required"), time.Second, deadline); err != nil {
		t.Fatal(err)
	}
	now = deadline
	if _, err := store.ClaimInbox(ctx, "consumer-c", time.Minute); !errors.Is(err, ErrNoWork) {
		t.Fatalf("expired approval remained claimable: %v", err)
	}
	result, err := store.ReapExpired(ctx, 10)
	if err != nil {
		t.Fatalf("reap expired approval: %v", err)
	}
	if result.InboxApprovalExpired != 1 || result.InboxFinalAttemptExpired != 0 {
		t.Fatalf("unexpected approval reap result: %+v", result)
	}
	store.mu.Lock()
	status := store.inbox[message.ID].Status
	lastError := store.inbox[message.ID].LastError
	store.mu.Unlock()
	if status != InboxDeadLetter || lastError != "tool approval expired" {
		t.Fatalf("expired approval status=%s last_error=%q", status, lastError)
	}
}

func TestMemoryStoreReapExpiredOutboxAndRejectsUnboundedBatch(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	message := newTestInbox()
	if _, err := store.EnqueueInbox(ctx, message); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, OutboxReply{Content: "reply"}); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.outbox[1].MaxAttempts = 1
	store.mu.Unlock()
	delivery, err := store.ClaimOutbox(ctx, "delivery-a", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Millisecond)
	if _, err := store.ClaimOutbox(ctx, "delivery-b", time.Minute); !errors.Is(err, ErrNoWork) {
		t.Fatalf("expired final delivery should not be claimable: %v", err)
	}
	if _, err := store.ReapExpired(ctx, 0); !errors.Is(err, ErrInvalidExpiredWorkReapBatchSize) {
		t.Fatalf("zero batch error=%v, want invalid batch", err)
	}
	result, err := store.ReapExpired(ctx, 10)
	if err != nil {
		t.Fatalf("reap expired outbox: %v", err)
	}
	if result.OutboxFinalAttemptExpired != 1 {
		t.Fatalf("unexpected outbox reap result: %+v", result)
	}
	if err := store.MarkDelivered(ctx, delivery.ID, delivery.Lease); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("expired lease was accepted after reap: %v", err)
	}
}

func TestMemoryStoreReapExpiredHonorsPerQueueBatchLimit(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 8, 26, 13, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	for i := 0; i < 2; i++ {
		message := newTestInbox()
		message.ExternalMessageID = "message-" + string(rune('a'+i))
		message.ReplyToID = "reply-" + string(rune('a'+i))
		message.SessionID = "session-" + string(rune('a'+i))
		message.MaxAttempts = 1
		if _, err := store.EnqueueInbox(ctx, message); err != nil {
			t.Fatal(err)
		}
		claim, err := store.ClaimInbox(ctx, "consumer", time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			now = now.Add(2 * time.Millisecond)
		}
		_ = claim
	}
	now = now.Add(2 * time.Millisecond)
	result, err := store.ReapExpired(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if result.InboxFinalAttemptExpired != 1 {
		t.Fatalf("batch limit not enforced: %+v", result)
	}
	result, err = store.ReapExpired(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if result.InboxFinalAttemptExpired != 1 {
		t.Fatalf("second bounded pass did not make progress: %+v", result)
	}
}

func TestMemoryStoreApprovalWaitRequiresFutureDeadline(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	message := newTestInbox()
	if _, err := store.EnqueueInbox(ctx, message); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WaitInboxApproval(ctx, claim.ID, claim.Lease,
		errors.New("tool approval required"), time.Second, now); !errors.Is(err, ErrApprovalDeadlineInvalid) {
		t.Fatalf("approval wait error=%v, want invalid deadline", err)
	}
	if _, err := store.ClaimInbox(ctx, "consumer-b", time.Minute); !errors.Is(err, ErrNoWork) {
		t.Fatalf("failed approval wait released message unexpectedly: %v", err)
	}
}

func TestMemoryStoreApprovalWaitRejectsDeadlineOutsideStoreWindow(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	message := newTestInbox()
	if _, err := store.EnqueueInbox(ctx, message); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	err = store.WaitInboxApproval(ctx, claim.ID, claim.Lease,
		errors.New("tool approval required"), time.Second, now.Add(MaxApprovalWait+time.Nanosecond))
	if !errors.Is(err, ErrApprovalDeadlineInvalid) {
		t.Fatalf("approval wait error=%v, want invalid deadline", err)
	}
}

func TestMemoryStorePersistsChunkCursorBetweenClaims(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	inbox := newTestInbox()
	if _, err := store.EnqueueInbox(ctx, inbox); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, OutboxReply{Content: "long reply"}); err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimOutbox(ctx, "delivery-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDispatchStarted(ctx, first.ID, first.Lease); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceOutbox(ctx, first.ID, first.Lease, 1); err != nil {
		t.Fatal(err)
	}
	second, err := store.ClaimOutbox(ctx, "delivery-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.DeliveryCursor != 1 || second.AttemptCount != 1 {
		t.Fatalf("cursor=%d attempt=%d", second.DeliveryCursor, second.AttemptCount)
	}
}

func TestMemoryStoreOutboxReplayDefaultsToResume(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	inbox := newTestInbox()
	if _, err := store.EnqueueInbox(ctx, inbox); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, OutboxReply{Content: "long reply"}); err != nil {
		t.Fatal(err)
	}
	outbox, err := store.ClaimOutbox(ctx, "delivery", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDispatchStarted(ctx, outbox.ID, outbox.Lease); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceOutbox(ctx, outbox.ID, outbox.Lease, 2); err != nil {
		t.Fatal(err)
	}
	outbox, err = store.ClaimOutbox(ctx, "delivery", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DeadLetterOutbox(ctx, outbox.ID, outbox.Lease, errors.New("permanent provider failure")); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplayOutbox(ctx, outbox.ID, "operator", "provider recovered"); err != nil {
		t.Fatal(err)
	}
	outbox, err = store.ClaimOutbox(ctx, "delivery", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if outbox.DeliveryCursor != 2 {
		t.Fatalf("resumed cursor=%d, want 2", outbox.DeliveryCursor)
	}
	if err := store.DeadLetterOutbox(ctx, outbox.ID, outbox.Lease, errors.New("operator requested restart")); err != nil {
		t.Fatal(err)
	}
	if err := store.RestartOutbox(ctx, outbox.ID, "operator", "provider requires full resend"); err != nil {
		t.Fatal(err)
	}
	outbox, err = store.ClaimOutbox(ctx, "delivery", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if outbox.DeliveryCursor != 0 {
		t.Fatalf("restarted cursor=%d, want 0", outbox.DeliveryCursor)
	}
	records := store.ReplayAuditRecords()
	if len(records) != 2 || records[0].Queue != ReplayQueueOutbox || records[0].Mode != OutboxReplayResume ||
		records[1].Mode != OutboxReplayRestart || records[1].RequestedBy != "operator" {
		t.Fatalf("unexpected replay audit: %#v", records)
	}
	records[0].Reason = "mutated"
	if got := store.ReplayAuditRecords()[0].Reason; got != "provider recovered" {
		t.Fatalf("audit snapshot was mutable: %q", got)
	}
}

func TestMemoryStoreBlocksOutboxUntilAuditedReplay(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	inbox := newTestInbox()
	if _, err := store.EnqueueInbox(ctx, inbox); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, OutboxReply{Content: "reply"})
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := store.ClaimOutbox(ctx, "delivery", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.BlockOutbox(ctx, delivery.ID, delivery.Lease, errors.New("tenant suspended")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimOutbox(ctx, "delivery-2", time.Minute); !errors.Is(err, ErrNoWork) {
		t.Fatalf("blocked Outbox remained claimable: %v", err)
	}
	if err := store.ReplayOutbox(ctx, outbox.ID, "operator", "tenant reactivated"); err != nil {
		t.Fatal(err)
	}
	replayed, err := store.ClaimOutbox(ctx, "delivery-3", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ID != outbox.ID || replayed.DeliveryCursor != 0 {
		t.Fatalf("unexpected replayed Outbox: %+v", replayed)
	}
	records := store.ReplayAuditRecords()
	if len(records) != 1 || records[0].Queue != ReplayQueueOutbox || records[0].RequestedBy != "operator" {
		t.Fatalf("unexpected replay audit: %+v", records)
	}
}

func TestMemoryStoreEnforcesOutboxFIFOUntilPredecessorDelivered(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	first := newTestInbox()
	second := newTestInbox()
	second.ExternalMessageID = "message-2"
	second.ReplyToID = "provider-message-2"
	for _, message := range []*InboxMessage{first, second} {
		if _, err := store.EnqueueInbox(ctx, message); err != nil {
			t.Fatal(err)
		}
	}
	for _, content := range []string{"first", "second"} {
		claim, err := store.ClaimInbox(ctx, "consumer", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, OutboxReply{Content: content}); err != nil {
			t.Fatal(err)
		}
	}

	firstReply, err := store.ClaimOutbox(ctx, "delivery-a", time.Minute)
	if err != nil || firstReply.SessionSequence != 1 {
		t.Fatalf("first reply=%#v err=%v", firstReply, err)
	}
	if _, err := store.ClaimOutbox(ctx, "delivery-b", time.Minute); !errors.Is(err, ErrNoWork) {
		t.Fatalf("later reply bypassed delivering predecessor: %v", err)
	}
	if err := store.DeadLetterOutbox(ctx, firstReply.ID, firstReply.Lease, errors.New("provider failure")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimOutbox(ctx, "delivery-b", time.Minute); !errors.Is(err, ErrNoWork) {
		t.Fatalf("later reply bypassed dead-letter predecessor: %v", err)
	}
	if err := store.ReplayOutbox(ctx, firstReply.ID, "operator", "provider recovered"); err != nil {
		t.Fatal(err)
	}
	firstReply, err = store.ClaimOutbox(ctx, "delivery-c", time.Minute)
	if err != nil || firstReply.SessionSequence != 1 {
		t.Fatalf("replayed first reply=%#v err=%v", firstReply, err)
	}
	if err := store.MarkDispatchStarted(ctx, firstReply.ID, firstReply.Lease); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkDelivered(ctx, firstReply.ID, firstReply.Lease); err != nil {
		t.Fatal(err)
	}
	secondReply, err := store.ClaimOutbox(ctx, "delivery-d", time.Minute)
	if err != nil || secondReply.SessionSequence != 2 {
		t.Fatalf("second reply after predecessor=%#v err=%v", secondReply, err)
	}
}

func TestMemoryStoreRequiresDurableSessionAndByteLimits(t *testing.T) {
	store := NewMemoryStore()
	msg := newTestInbox()
	msg.SessionID = ""
	if _, err := store.EnqueueInbox(context.Background(), msg); err == nil {
		t.Fatal("empty session accepted")
	}

	msg = newTestInbox()
	msg.AgentApp = strings.Repeat("界", 43) // 129 UTF-8 bytes.
	if _, err := store.EnqueueInbox(context.Background(), msg); err == nil {
		t.Fatal("129-byte agent app accepted")
	}
	msg.AgentApp = strings.Repeat("界", 42)
	msg.ExternalMessageID = "message-agent-app-boundary"
	msg.ReplyToID = "provider-message-agent-app-boundary"
	if _, err := store.EnqueueInbox(context.Background(), msg); err != nil {
		t.Fatalf("126-byte agent app rejected: %v", err)
	}

	msg = newTestInbox()
	msg.ExternalMessageID = "message-session-boundary"
	msg.ReplyToID = "provider-message-session-boundary"
	msg.UserID = strings.Repeat("u", 255)
	msg.SessionID = strings.Repeat("s", 255)
	if _, err := store.EnqueueInbox(context.Background(), msg); err != nil {
		t.Fatalf("session backend boundary rejected: %v", err)
	}
	msg = newTestInbox()
	msg.UserID = strings.Repeat("u", 256)
	if _, err := store.EnqueueInbox(context.Background(), msg); err == nil {
		t.Fatal("256-byte user ID accepted before VARCHAR(255) session backend")
	}
	msg = newTestInbox()
	msg.SessionID = strings.Repeat("s", 256)
	if _, err := store.EnqueueInbox(context.Background(), msg); err == nil {
		t.Fatal("256-byte session ID accepted before VARCHAR(255) session backend")
	}

	msg = newTestInbox()
	msg.ExternalMessageID = "message-invalid-hash"
	msg.PayloadHash = "not-a-sha256-digest"
	if _, err := store.EnqueueInbox(context.Background(), msg); err == nil {
		t.Fatal("non-SHA-256 payload hash accepted")
	}
}

func TestMemoryStoreInvalidReplyDoesNotCompleteInbox(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	message := newTestInbox()
	if _, err := store.EnqueueInbox(ctx, message); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, OutboxReply{}); !errors.Is(err, ErrInvalidInboxMessage) {
		t.Fatalf("empty reply error = %v, want ErrInvalidInboxMessage", err)
	}
	if _, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, OutboxReply{Content: ""}); !errors.Is(err, ErrInvalidInboxMessage) {
		t.Fatalf("empty reply content error = %v, want ErrInvalidInboxMessage", err)
	}
	if _, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, OutboxReply{ContentType: "bad\nheader", Content: "valid"}); !errors.Is(err, ErrInvalidInboxMessage) {
		t.Fatalf("invalid reply content type error = %v, want ErrInvalidInboxMessage", err)
	}
	if _, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, OutboxReply{Content: "valid"}); err != nil {
		t.Fatalf("invalid reply changed inbox state: %v", err)
	}
	if _, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, OutboxReply{}); err == nil {
		t.Fatal("empty reply accepted")
	}
}

func TestMemoryStoreReportsExistingOutboxAsReconciliationConflict(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	message := newTestInbox()
	if _, err := store.EnqueueInbox(ctx, message); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, OutboxReply{Content: "reply"}); err != nil {
		t.Fatal(err)
	}

	// Reproduce a durable inconsistency left by a repair/import path: an Outbox
	// row exists while the Inbox is still processing under a valid lease.
	store.mu.Lock()
	stored := store.inbox[claim.ID]
	stored.Status = InboxProcessing
	stored.Lease = claim.Lease
	stored.Lease.Until = store.now().UTC().Add(time.Minute)
	store.mu.Unlock()

	if _, err := store.CompleteInbox(ctx, claim.ID, stored.Lease, OutboxReply{Content: "different"}); !errors.Is(err, ErrOutboxConflict) {
		t.Fatalf("existing Outbox error = %v, want ErrOutboxConflict", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.inbox[claim.ID].Status != InboxProcessing {
		t.Fatalf("conflict changed Inbox status to %s", store.inbox[claim.ID].Status)
	}
}

func TestMemoryStoreEnforcesFIFOAndAllowsIndependentSessions(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 8, 24, 1, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	first := newTestInbox()
	second := newTestInbox()
	second.ExternalMessageID = "message-2"
	second.ReplyToID = "provider-message-2"
	other := newTestInbox()
	other.ExternalMessageID = "message-3"
	other.ReplyToID = "provider-message-3"
	other.SessionID = "session-2"
	for _, msg := range []*InboxMessage{first, second, other} {
		if _, err := store.EnqueueInbox(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}

	firstClaim, err := store.ClaimInbox(ctx, "consumer-a", time.Minute)
	if err != nil || firstClaim.ID != first.ID {
		t.Fatalf("first claim=%#v err=%v", firstClaim, err)
	}
	otherClaim, err := store.ClaimInbox(ctx, "consumer-b", time.Minute)
	if err != nil || otherClaim.ID != other.ID {
		t.Fatalf("independent session claim=%#v err=%v", otherClaim, err)
	}
	if _, err := store.CompleteInbox(ctx, otherClaim.ID, otherClaim.Lease, OutboxReply{Content: "other"}); err != nil {
		t.Fatal(err)
	}
	if err := store.RetryInbox(ctx, firstClaim.ID, firstClaim.Lease, errors.New("temporary"), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimInbox(ctx, "consumer-c", time.Minute); !errors.Is(err, ErrNoWork) {
		t.Fatalf("successor bypassed retry-wait predecessor: %v", err)
	}

	now = now.Add(2 * time.Hour)
	firstClaim, err = store.ClaimInbox(ctx, "consumer-d", time.Minute)
	if err != nil || firstClaim.ID != first.ID {
		t.Fatalf("retry claim=%#v err=%v", firstClaim, err)
	}
	if _, err := store.CompleteInbox(ctx, firstClaim.ID, firstClaim.Lease, OutboxReply{Content: "first"}); err != nil {
		t.Fatal(err)
	}
	secondClaim, err := store.ClaimInbox(ctx, "consumer-e", time.Minute)
	if err != nil || secondClaim.ID != second.ID {
		t.Fatalf("successor claim=%#v err=%v", secondClaim, err)
	}
}

func TestMemoryStoreDeadLetterPausesStreamUntilAuditedReplayCompletes(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	first := newTestInbox()
	first.MaxAttempts = 1
	second := newTestInbox()
	second.ExternalMessageID = "message-2"
	second.ReplyToID = "provider-message-2"
	for _, msg := range []*InboxMessage{first, second} {
		if _, err := store.EnqueueInbox(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}

	claim, err := store.ClaimInbox(ctx, "consumer-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RetryInbox(ctx, claim.ID, claim.Lease, errors.New("permanent"), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimInbox(ctx, "consumer-b", time.Minute); !errors.Is(err, ErrNoWork) {
		t.Fatalf("successor bypassed dead-letter predecessor: %v", err)
	}
	if err := store.ReplayInbox(ctx, first.ID, "operator-a", "incident resolved"); err != nil {
		t.Fatal(err)
	}
	records := store.ReplayAuditRecords()
	if len(records) != 1 || records[0].Queue != ReplayQueueInbox || records[0].MessageID != first.ID ||
		records[0].TenantID != first.TenantID || records[0].RequestedBy != "operator-a" ||
		records[0].Reason != "incident resolved" || records[0].Mode != OutboxReplayRestart {
		t.Fatalf("unexpected replay audit: %#v", records)
	}
	records[0].Reason = "mutated"
	if store.ReplayAuditRecords()[0].Reason != "incident resolved" {
		t.Fatal("replay audit accessor leaked mutable storage")
	}

	claim, err = store.ClaimInbox(ctx, "consumer-c", time.Minute)
	if err != nil || claim.ID != first.ID {
		t.Fatalf("replayed predecessor claim=%#v err=%v", claim, err)
	}
	if _, err := store.CompleteInbox(ctx, claim.ID, claim.Lease, OutboxReply{Content: "recovered"}); err != nil {
		t.Fatal(err)
	}
	claim, err = store.ClaimInbox(ctx, "consumer-d", time.Minute)
	if err != nil || claim.ID != second.ID {
		t.Fatalf("successor after replay=%#v err=%v", claim, err)
	}
}

func TestMemoryStoreReclaimsExpiredPredecessorBeforeSuccessor(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Date(2026, 8, 24, 2, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	first := newTestInbox()
	second := newTestInbox()
	second.ExternalMessageID = "message-2"
	second.ReplyToID = "provider-message-2"
	for _, msg := range []*InboxMessage{first, second} {
		if _, err := store.EnqueueInbox(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	oldClaim, err := store.ClaimInbox(ctx, "consumer-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	newClaim, err := store.ClaimInbox(ctx, "consumer-b", time.Minute)
	if err != nil || newClaim.ID != first.ID || newClaim.Lease.Fence <= oldClaim.Lease.Fence {
		t.Fatalf("expired predecessor was not reclaimed first: old=%#v new=%#v err=%v", oldClaim, newClaim, err)
	}
	if _, err := store.CompleteInbox(ctx, newClaim.ID, newClaim.Lease, OutboxReply{Content: "first"}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(ctx, "consumer-c", time.Minute)
	if err != nil || claim.ID != second.ID {
		t.Fatalf("successor claim=%#v err=%v", claim, err)
	}
}

type storeWithoutRestart struct{ Store }

func TestOutboxRestartRemainsOptionalStoreSeam(t *testing.T) {
	var base Store = storeWithoutRestart{Store: NewMemoryStore()}
	if _, ok := base.(OutboxRestartStore); ok {
		t.Fatal("base Store unexpectedly exposes destructive outbox restart")
	}
}
