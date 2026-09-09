// Package channelstest holds the behaviour contract every channels.Store
// implementation has to satisfy.
package channelstest

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const suiteTimeout = 60 * time.Second

// NewStore returns an empty Store isolated from every other subtest.
type NewStore func(t *testing.T) channels.Store

// RunStoreSuite runs the shared Inbox, Run, and Outbox contract.
func RunStoreSuite(t *testing.T, newStore NewStore) {
	t.Helper()

	t.Run("AcceptDeduplicatesAndIsolatesTenants", func(t *testing.T) {
		assertAcceptDeduplicatesAndIsolatesTenants(t, newStore(t))
	})
	t.Run("ConcurrentDuplicateAcceptHasOneWinner", func(t *testing.T) {
		assertConcurrentDuplicateAcceptHasOneWinner(t, newStore(t))
	})
	t.Run("ClaimsRunsInSessionOrder", func(t *testing.T) {
		assertClaimsRunsInSessionOrder(t, newStore(t))
	})
	t.Run("FencesYieldAndRecoveryAttempts", func(t *testing.T) {
		assertFencesYieldAndRecoveryAttempts(t, newStore(t))
	})
	t.Run("FinishesRunAndOutboxAtomically", func(t *testing.T) {
		assertFinishesRunAndOutboxAtomically(t, newStore(t))
	})
	t.Run("ClassifiesAndFencesOutboxAttempts", func(t *testing.T) {
		assertClassifiesAndFencesOutboxAttempts(t, newStore(t))
	})
	t.Run("SendsAnAnswersPartsInOrder", func(t *testing.T) {
		assertSendsAnAnswersPartsInOrder(t, newStore(t))
	})
	t.Run("RefusesAClaimForTheWrongChannel", func(t *testing.T) {
		assertRefusesAClaimForTheWrongChannel(t, newStore(t))
	})
	t.Run("PersistsTheSendDeadline", func(t *testing.T) {
		assertPersistsTheSendDeadline(t, newStore(t))
	})
	t.Run("ScansLostWakeups", func(t *testing.T) {
		assertScansLostWakeups(t, newStore(t))
	})
	t.Run("ClearsDispatchMarksWhenRequeued", func(t *testing.T) {
		assertClearsDispatchMarksWhenRequeued(t, newStore(t))
	})
	t.Run("FencesDispatchGenerations", func(t *testing.T) {
		assertFencesDispatchGenerations(t, newStore(t))
	})
	t.Run("RecordsFirstExecutionPermanently", func(t *testing.T) {
		assertRecordsFirstExecutionPermanently(t, newStore(t))
	})
	t.Run("HonorsCanceledAndInvalidCalls", func(t *testing.T) {
		assertHonorsCanceledAndInvalidCalls(t, newStore(t))
	})
	t.Run("RejectsUnrepresentableOrInconsistentRecords", func(t *testing.T) {
		assertRejectsUnrepresentableOrInconsistentRecords(t, newStore(t))
	})
}

// Context returns a bounded context for one conformance subtest.
func Context(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), suiteTimeout)
	t.Cleanup(cancel)
	return ctx
}

// Request builds a valid event fixture. It is exported so backend-specific
// tests can exercise migrations and constraints with the same domain input.
func Request(tenantID, suffix, sessionID, eventID string) channels.AcceptRequest {
	now := fixtureTime()
	return channels.AcceptRequest{
		IDs: channels.AcceptIDs{
			InboxID:   "inbox-" + suffix,
			RunID:     "run-" + suffix,
			RequestID: "request-" + suffix,
		},
		Policy: channels.RunPolicy{
			MaxAttempts:    3,
			MaxRunDuration: time.Minute,
			RecoveryGrace:  10 * time.Second,
		},
		Envelope: channels.InboundEnvelope{
			TenantID:         tenantID,
			Channel:          channels.ChannelFeishu,
			ChannelBindingID: "binding-main",
			AgentAppID:       "app-main",
			PrincipalID:      "principal-main",
			SessionID:        sessionID,
			ExternalEventID:  eventID,
			ReceivedAt:       now.Add(-time.Second),
			Message: channels.InboundMessage{
				Text: "hello from " + suffix,
				Attachments: []channels.AttachmentRef{{
					Kind:       channels.AttachmentFile,
					ExternalID: "media-" + suffix,
					MediaType:  "text/plain",
					Name:       "note.txt",
					SizeBytes:  12,
				}},
				ReplyTo: &channels.MessageReference{
					ExternalMessageID: "reply-" + suffix,
				},
			},
			DeliveryTarget: channels.DeliveryTarget{
				Channel: channels.ChannelFeishu,
				Version: 1,
				Payload: []byte(`{"chat_id":"chat-main","reply_to":"message-main"}`),
			},
		},
		Now: now,
	}
}

func fixtureTime() time.Time {
	return channels.NormalizeTime(time.Date(2026, 9, 3, 9, 0, 0, 123456000, time.UTC))
}

func scope(tenantID string) tenant.TenantContext {
	return tenant.TenantContext{TenantID: tenantID}
}

func accept(
	t *testing.T,
	ctx context.Context,
	store channels.Store,
	request channels.AcceptRequest,
) channels.AcceptResult {
	t.Helper()
	result, err := store.Accept(ctx, scope(request.Envelope.TenantID), request)
	require.NoError(t, err)
	return result
}

func claimRun(
	t *testing.T,
	ctx context.Context,
	store channels.Store,
	tenantID string,
	key sessiondir.Key,
	token string,
	now time.Time,
) channels.RunClaim {
	t.Helper()
	claim, ok, err := store.ClaimNextRun(ctx, scope(tenantID), key, channels.ClaimRunRequest{
		ClaimToken: token,
		ClaimedBy:  "worker-main",
		Now:        now,
	})
	require.NoError(t, err)
	require.True(t, ok)
	return claim
}

func finishSucceeded(
	t *testing.T,
	ctx context.Context,
	store channels.Store,
	tenantID string,
	claim channels.RunClaim,
	drafts []channels.OutboxDraft,
	now time.Time,
) channels.FinishRunResult {
	t.Helper()
	result, err := store.FinishRun(ctx, scope(tenantID), channels.FinishRunRequest{
		Token: channels.RunToken{
			RunID:      claim.Run.RunID,
			ClaimToken: claim.Run.ClaimToken,
		},
		Status:     channels.RunSucceeded,
		RevisionID: "revision-main",
		Stats: channels.RunStats{
			EventCount:      2,
			OutputParts:     int32(len(drafts)),
			ExecutionMillis: 125,
		},
		Outbox: drafts,
		Now:    now,
	})
	require.NoError(t, err)
	return result
}

func claimOutbox(
	t *testing.T,
	ctx context.Context,
	store channels.Store,
	tenantID, outboxID, token string,
	now time.Time,
) channels.OutboxClaim {
	t.Helper()
	claim, ok, err := store.ClaimOutbox(ctx, scope(tenantID), channels.ClaimOutboxRequest{
		OutboxID:    outboxID,
		SendToken:   token,
		SentBy:      "sender-main",
		SendTimeout: 20 * time.Second,
		Now:         now,
	})
	require.NoError(t, err)
	require.True(t, ok)
	return claim
}

func completeOutbox(
	t *testing.T,
	ctx context.Context,
	store channels.Store,
	tenantID, outboxID, token string,
	result channels.SendResult,
	now time.Time,
) channels.OutboxStatus {
	t.Helper()
	status, err := store.CompleteOutbox(ctx, scope(tenantID), channels.CompleteOutboxRequest{
		Token:  channels.OutboxToken{OutboxID: outboxID, SendToken: token},
		Result: result,
		Now:    now,
	})
	require.NoError(t, err)
	return status
}

func draft(partNo int32, maxAttempts int32) channels.OutboxDraft {
	return channels.OutboxDraft{
		OutboxID:        fmt.Sprintf("outbox-%d", partNo),
		PartNo:          partNo,
		ClientMessageID: fmt.Sprintf("client-message-%d", partNo),
		MaxAttempts:     maxAttempts,
		Message: channels.OutboundMessage{
			Text: fmt.Sprintf("answer part %d", partNo),
		},
		DeliveryTarget: channels.DeliveryTarget{
			Channel: channels.ChannelFeishu,
			Version: 1,
			Payload: []byte(`{"chat_id":"chat-main","reply_to":"message-main"}`),
		},
	}
}

// draftIn is draft with the answer it belongs to written into its ids.
//
// Several subtests below need more than one Run, because the parts of one Run
// are now sequenced: a part cannot be claimed while an earlier one is unsent,
// and a part that fails terminally closes every later part of the same answer.
// Scenarios that must not disturb each other therefore cannot share a Run, and
// parts of different Runs cannot share an id.
func draftIn(suffix string, partNo int32, maxAttempts int32) channels.OutboxDraft {
	part := draft(partNo, maxAttempts)
	part.OutboxID = fmt.Sprintf("outbox-%s-%d", suffix, partNo)
	part.ClientMessageID = fmt.Sprintf("client-message-%s-%d", suffix, partNo)
	return part
}

// answer accepts one request, claims the Run it created and finishes it
// successfully with the given parts. It is the shortest path to an Outbox with
// something in it. See draftIn for why a subtest wants several of them.
func answer(
	t *testing.T,
	ctx context.Context,
	store channels.Store,
	tenantID string,
	suffix string,
	drafts []channels.OutboxDraft,
	now time.Time,
) channels.FinishRunResult {
	t.Helper()
	request := Request(tenantID, suffix, "session-"+suffix, "event-"+suffix)
	accept(t, ctx, store, request)
	runClaim := claimRun(
		t, ctx, store, tenantID, request.Envelope.SessionKey(), "claim-"+suffix, now)
	return finishSucceeded(t, ctx, store, tenantID, runClaim, drafts, now.Add(time.Second))
}

func assertAcceptDeduplicatesAndIsolatesTenants(t *testing.T, store channels.Store) {
	ctx := Context(t)
	request := Request("tenant-a", "first", "session-main", "sensitive-event-id")
	wantText := request.Envelope.Message.Text
	wantMediaID := request.Envelope.Message.Attachments[0].ExternalID
	wantReplyID := request.Envelope.Message.ReplyTo.ExternalMessageID
	wantTarget := string(request.Envelope.DeliveryTarget.Payload)

	first := accept(t, ctx, store, request)
	require.False(t, first.Duplicate)
	require.Equal(t, int64(1), first.AcceptSequence)

	// Accept must copy mutable input before returning.
	request.Envelope.Message.Text = "mutated"
	request.Envelope.Message.Attachments[0].ExternalID = "mutated"
	request.Envelope.Message.ReplyTo.ExternalMessageID = "mutated"
	request.Envelope.DeliveryTarget.Payload[2] = 'X'

	for _, suffix := range []string{"duplicate-a", "duplicate-b"} {
		duplicateRequest := Request("tenant-a", suffix, "session-main", "sensitive-event-id")
		duplicate, err := store.Accept(ctx, scope("tenant-a"), duplicateRequest)
		require.NoError(t, err)
		require.True(t, duplicate.Duplicate)
		require.Equal(t, first.InboxID, duplicate.InboxID)
		require.Equal(t, first.RunID, duplicate.RunID)
		require.Equal(t, first.RequestID, duplicate.RequestID)
		require.Equal(t, first.AcceptSequence, duplicate.AcceptSequence)
	}

	inbox, err := store.GetInbox(ctx, scope("tenant-a"), first.InboxID)
	require.NoError(t, err)
	require.Equal(t, wantText, inbox.Message.Text)
	require.Equal(t, wantMediaID, inbox.Message.Attachments[0].ExternalID)
	require.Equal(t, wantReplyID, inbox.Message.ReplyTo.ExternalMessageID)
	require.Equal(t, wantTarget, string(inbox.DeliveryTarget.Payload))

	// Reads must also return deep copies.
	inbox.Message.Attachments[0].ExternalID = "mutated-after-read"
	inbox.Message.ReplyTo.ExternalMessageID = "mutated-after-read"
	inbox.DeliveryTarget.Payload[2] = 'X'
	reloaded, err := store.GetInbox(ctx, scope("tenant-a"), first.InboxID)
	require.NoError(t, err)
	require.Equal(t, wantMediaID, reloaded.Message.Attachments[0].ExternalID)
	require.Equal(t, wantReplyID, reloaded.Message.ReplyTo.ExternalMessageID)
	require.Equal(t, wantTarget, string(reloaded.DeliveryTarget.Payload))

	runs, err := store.ListSessionRuns(ctx, scope("tenant-a"), reloaded.SessionKey(), channels.ListRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.Equal(t, first.RunID, runs[0].RunID)

	_, err = store.GetInbox(ctx, scope("tenant-b"), first.InboxID)
	require.ErrorIs(t, err, tenant.ErrNotFound)
	_, err = store.GetRun(ctx, scope("tenant-b"), first.RunID)
	require.ErrorIs(t, err, tenant.ErrNotFound)

	// External event identity is tenant scoped.
	otherTenant := Request("tenant-b", "other-tenant", "session-main", "sensitive-event-id")
	other := accept(t, ctx, store, otherTenant)
	require.False(t, other.Duplicate)
	require.Equal(t, int64(1), other.AcceptSequence)

	// Run IDs are deliberately global so a wakeup can resolve one unambiguously.
	globalCollision := Request("tenant-b", "global-collision", "session-other", "event-other")
	globalCollision.IDs.RunID = first.RunID
	_, err = store.Accept(ctx, scope("tenant-b"), globalCollision)
	require.ErrorIs(t, err, tenant.ErrAlreadyExists)

	tooLong := Request("tenant-a", "invalid", "session-main", "event-invalid")
	secret := "do-not-leak-" + string(make([]byte, channels.MaxExternalEventIDBytes+1))
	tooLong.Envelope.ExternalEventID = secret
	_, err = store.Accept(ctx, scope("tenant-a"), tooLong)
	require.Error(t, err)
	require.NotContains(t, err.Error(), secret)
}

func assertConcurrentDuplicateAcceptHasOneWinner(t *testing.T, store channels.Store) {
	ctx := Context(t)
	const workers = 8
	results := make([]channels.AcceptResult, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(workers)
	done.Add(workers)

	for i := 0; i < workers; i++ {
		i := i
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			request := Request(
				"tenant-a",
				fmt.Sprintf("concurrent-%d", i),
				fmt.Sprintf("session-%d", i),
				"event-concurrent",
			)
			results[i], errs[i] = store.Accept(ctx, scope("tenant-a"), request)
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()

	winners := 0
	var winner channels.AcceptResult
	for i := range results {
		require.NoErrorf(t, errs[i], "accept %d", i)
		if !results[i].Duplicate {
			winners++
			winner = results[i]
		}
	}
	require.Equal(t, 1, winners)
	for _, result := range results {
		require.Equal(t, winner.InboxID, result.InboxID)
		require.Equal(t, winner.RunID, result.RunID)
		require.Equal(t, winner.RequestID, result.RequestID)
	}
}

func assertClaimsRunsInSessionOrder(t *testing.T, store channels.Store) {
	ctx := Context(t)
	now := fixtureTime()
	firstRequest := Request("tenant-a", "order-first", "session-order", "event-order-first")
	secondRequest := Request("tenant-a", "order-second", "session-order", "event-order-second")
	otherRequest := Request("tenant-a", "order-other", "session-other", "event-order-other")
	first := accept(t, ctx, store, firstRequest)
	second := accept(t, ctx, store, secondRequest)
	other := accept(t, ctx, store, otherRequest)
	require.Equal(t, int64(1), first.AcceptSequence)
	require.Equal(t, int64(2), second.AcceptSequence)
	require.Equal(t, int64(1), other.AcceptSequence)

	key := firstRequest.Envelope.SessionKey()
	firstClaim := claimRun(t, ctx, store, "tenant-a", key, "claim-order-first", now)
	require.Equal(t, first.RunID, firstClaim.Run.RunID)
	require.Equal(t, first.RequestID, firstClaim.Run.RequestID)
	require.Equal(t, firstRequest.Policy.MaxRunDuration, firstClaim.RemainingExecutionBudget)

	_, ok, err := store.ClaimNextRun(ctx, scope("tenant-a"), key, channels.ClaimRunRequest{
		ClaimToken: "claim-order-second-too-early",
		ClaimedBy:  "worker-second",
		Now:        now,
	})
	require.NoError(t, err)
	require.False(t, ok, "a running head must block the next message")

	otherClaim := claimRun(
		t, ctx, store, "tenant-a", otherRequest.Envelope.SessionKey(), "claim-order-other", now)
	require.Equal(t, other.RunID, otherClaim.Run.RunID, "a different session must remain claimable")

	finishSucceeded(t, ctx, store, "tenant-a", firstClaim, nil, now.Add(time.Second))
	secondClaim := claimRun(t, ctx, store, "tenant-a", key, "claim-order-second", now.Add(time.Second))
	require.Equal(t, second.RunID, secondClaim.Run.RunID)
	require.Equal(t, second.RequestID, secondClaim.Run.RequestID)
}

func assertFencesYieldAndRecoveryAttempts(t *testing.T, store channels.Store) {
	ctx := Context(t)
	now := fixtureTime()
	firstRequest := Request("tenant-a", "recover-first", "session-recover", "event-recover-first")
	firstRequest.Policy.MaxAttempts = 2
	secondRequest := Request("tenant-a", "recover-second", "session-recover", "event-recover-second")
	accept(t, ctx, store, firstRequest)
	second := accept(t, ctx, store, secondRequest)

	claim := claimRun(
		t, ctx, store, "tenant-a", firstRequest.Envelope.SessionKey(), "claim-recover-old", now)
	token := channels.RunToken{RunID: claim.Run.RunID, ClaimToken: claim.Run.ClaimToken}
	require.Equal(t, int32(1), claim.Run.Attempt)
	require.NotNil(t, claim.Run.ExecuteDeadlineAt)
	require.NotNil(t, claim.Run.RecoverAfter)
	require.True(t, claim.Run.ExecuteDeadlineAt.Before(*claim.Run.RecoverAfter))

	require.NoError(t, store.RecordRunRevision(ctx, scope("tenant-a"), token, "revision-old", now.Add(time.Second)))
	startedAt := now.Add(2 * time.Second)
	require.NoError(t, store.MarkRunStarted(ctx, scope("tenant-a"), token, startedAt))
	require.NoError(t, store.MarkRunStarted(ctx, scope("tenant-a"), token, startedAt.Add(time.Second)))
	stored, err := store.GetRun(ctx, scope("tenant-a"), claim.Run.RunID)
	require.NoError(t, err)
	require.Equal(t, channels.NormalizeTime(startedAt), *stored.ExecutionStartedAt)

	_, err = store.YieldRun(ctx, scope("tenant-a"), token, channels.YieldRunRequest{
		ErrorType: channels.ErrorRunCancelled,
		Now:       now.Add(3 * time.Second),
	})
	require.ErrorIs(t, err, channels.ErrExecutionStarted)

	recovered, err := store.RecoverRuns(ctx, channels.RecoverRequest{
		Now:   claim.Run.RecoverAfter.Add(-time.Microsecond),
		Limit: 10,
	})
	require.NoError(t, err)
	require.Empty(t, recovered)
	recoveryTime := *claim.Run.RecoverAfter
	recovered, err = store.RecoverRuns(ctx, channels.RecoverRequest{Now: recoveryTime, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []channels.RunRecovery{{
		RunRef:  channels.RunRef{TenantID: "tenant-a", RunID: claim.Run.RunID},
		Outcome: channels.RequeueRetried,
	}}, recovered)

	require.ErrorIs(t,
		store.MarkRunStarted(ctx, scope("tenant-a"), token, recoveryTime),
		channels.ErrStaleClaim)
	require.ErrorIs(t,
		store.RecordRunRevision(ctx, scope("tenant-a"), token, "revision-stale", recoveryTime),
		channels.ErrStaleClaim)
	_, err = store.FinishRun(ctx, scope("tenant-a"), channels.FinishRunRequest{
		Token:      token,
		Status:     channels.RunFailed,
		RevisionID: "revision-old",
		ErrorType:  channels.ErrorRunTimeout,
		Now:        recoveryTime,
	})
	require.ErrorIs(t, err, channels.ErrStaleClaim)

	requeued, err := store.GetRun(ctx, scope("tenant-a"), claim.Run.RunID)
	require.NoError(t, err)
	require.Equal(t, channels.RunAccepted, requeued.Status)
	require.Equal(t, channels.ErrorRunTimeout, requeued.ErrorType)
	require.Empty(t, requeued.ClaimToken)
	require.Equal(t,
		channels.NormalizeTime(recoveryTime.Add(channels.Backoff(1, requeued.RunID))),
		requeued.NextAttemptAt)

	_, ok, err := store.ClaimNextRun(ctx, scope("tenant-a"), firstRequest.Envelope.SessionKey(), channels.ClaimRunRequest{
		ClaimToken: "claim-recover-too-early",
		ClaimedBy:  "worker-main",
		Now:        requeued.NextAttemptAt.Add(-time.Microsecond),
	})
	require.NoError(t, err)
	require.False(t, ok, "a backing-off head must block the next message")

	secondAttempt := claimRun(
		t, ctx, store, "tenant-a", firstRequest.Envelope.SessionKey(),
		"claim-recover-new", requeued.NextAttemptAt)
	require.Equal(t, int32(2), secondAttempt.Run.Attempt)
	require.NotEqual(t, token.ClaimToken, secondAttempt.Run.ClaimToken)
	outcome, err := store.YieldRun(ctx, scope("tenant-a"), channels.RunToken{
		RunID:      secondAttempt.Run.RunID,
		ClaimToken: secondAttempt.Run.ClaimToken,
	}, channels.YieldRunRequest{
		ErrorType: channels.ErrorRunCancelled,
		Now:       requeued.NextAttemptAt,
	})
	require.NoError(t, err)
	require.Equal(t, channels.RequeueExhausted, outcome)

	exhausted, err := store.GetRun(ctx, scope("tenant-a"), claim.Run.RunID)
	require.NoError(t, err)
	require.Equal(t, channels.RunFailed, exhausted.Status)
	require.Equal(t, channels.ErrorAttemptsExhausted, exhausted.ErrorType)
	next := claimRun(
		t, ctx, store, "tenant-a", firstRequest.Envelope.SessionKey(),
		"claim-after-exhaustion", requeued.NextAttemptAt)
	require.Equal(t, second.RunID, next.Run.RunID)
}

func assertFinishesRunAndOutboxAtomically(t *testing.T, store channels.Store) {
	ctx := Context(t)
	now := fixtureTime()
	request := Request("tenant-a", "finish", "session-finish", "event-finish")
	accept(t, ctx, store, request)
	claim := claimRun(t, ctx, store, "tenant-a", request.Envelope.SessionKey(), "claim-finish", now)
	drafts := []channels.OutboxDraft{draft(1, 3), draft(0, 3)}
	result := finishSucceeded(t, ctx, store, "tenant-a", claim, drafts, now.Add(time.Second))
	require.Equal(t, channels.RunSucceeded, result.Run.Status)
	require.Equal(t, "revision-main", result.Run.RevisionID)
	require.Empty(t, result.Run.ClaimToken)
	require.Len(t, result.Outbox, 2)
	require.Equal(t, int32(0), result.Outbox[0].PartNo)
	require.Equal(t, int32(1), result.Outbox[1].PartNo)
	require.Equal(t, channels.IdempotencyKeyFor(result.Run.RequestID, 0), result.Outbox[0].IdempotencyKey)

	result.Outbox[0].Message.Text = "mutated"
	result.Outbox[0].DeliveryTarget.Payload[2] = 'X'
	part, err := store.GetOutboxPart(ctx, scope("tenant-a"), "outbox-0")
	require.NoError(t, err)
	require.Equal(t, "answer part 0", part.Message.Text)
	require.JSONEq(t, `{"chat_id":"chat-main","reply_to":"message-main"}`, string(part.DeliveryTarget.Payload))

	staleDraft := draft(2, 3)
	_, err = store.FinishRun(ctx, scope("tenant-a"), channels.FinishRunRequest{
		Token: channels.RunToken{
			RunID:      claim.Run.RunID,
			ClaimToken: claim.Run.ClaimToken,
		},
		Status:     channels.RunSucceeded,
		RevisionID: "revision-main",
		Stats:      channels.RunStats{OutputParts: 1},
		Outbox:     []channels.OutboxDraft{staleDraft},
		Now:        now.Add(2 * time.Second),
	})
	require.ErrorIs(t, err, channels.ErrStaleClaim)
	parts, err := store.ListRunOutbox(ctx, scope("tenant-a"), claim.Run.RunID, channels.ListRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, parts, 2, "a stale finish must not insert any part")
	_, err = store.GetOutboxPart(ctx, scope("tenant-b"), "outbox-0")
	require.ErrorIs(t, err, tenant.ErrNotFound)

	// A webhook can be redelivered after the answer is already durable. It
	// still resolves to the original Run and cannot create another answer.
	for _, suffix := range []string{"finish-duplicate-a", "finish-duplicate-b"} {
		duplicate := Request("tenant-a", suffix, request.Envelope.SessionID, request.Envelope.ExternalEventID)
		accepted, err := store.Accept(ctx, scope("tenant-a"), duplicate)
		require.NoError(t, err)
		require.True(t, accepted.Duplicate)
		require.Equal(t, claim.Run.RunID, accepted.RunID)
		require.Equal(t, claim.Run.RequestID, accepted.RequestID)
	}
	runs, err := store.ListSessionRuns(
		ctx, scope("tenant-a"), request.Envelope.SessionKey(), channels.ListRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	parts, err = store.ListRunOutbox(ctx, scope("tenant-a"), claim.Run.RunID, channels.ListRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, parts, 2)

	badRequest := Request("tenant-a", "finish-invalid", "session-finish-invalid", "event-finish-invalid")
	accept(t, ctx, store, badRequest)
	badClaim := claimRun(
		t, ctx, store, "tenant-a", badRequest.Envelope.SessionKey(), "claim-finish-invalid", now)
	badDraft := draft(3, 3)
	badDraft.DeliveryTarget.Channel = channels.ChannelWeCom
	_, err = store.FinishRun(ctx, scope("tenant-a"), channels.FinishRunRequest{
		Token: channels.RunToken{
			RunID:      badClaim.Run.RunID,
			ClaimToken: badClaim.Run.ClaimToken,
		},
		Status:     channels.RunSucceeded,
		RevisionID: "revision-main",
		Stats:      channels.RunStats{OutputParts: 1},
		Outbox:     []channels.OutboxDraft{badDraft},
		Now:        now.Add(time.Second),
	})
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	badStored, err := store.GetRun(ctx, scope("tenant-a"), badClaim.Run.RunID)
	require.NoError(t, err)
	require.Equal(t, channels.RunRunning, badStored.Status)
	badParts, err := store.ListRunOutbox(
		ctx, scope("tenant-a"), badClaim.Run.RunID, channels.ListRequest{Limit: 10})
	require.NoError(t, err)
	require.Empty(t, badParts)
}

// assertClassifiesAndFencesOutboxAttempts covers what one send outcome does to
// one part.
//
// Each outcome gets a Run of its own rather than a part of a shared one. That is
// not tidiness: a part cannot be claimed while an earlier part of its answer is
// unsent, and a terminal failure closes every later part of the same answer, so
// five outcomes on five parts of one Run would be four scenarios the first one
// had already decided.
func assertClassifiesAndFencesOutboxAttempts(t *testing.T, store channels.Store) {
	ctx := Context(t)
	now := fixtureTime()
	sendAt := now.Add(2 * time.Second)
	doneAt := now.Add(3 * time.Second)
	claim := func(outboxID, token string, at time.Time) channels.OutboxClaim {
		t.Helper()
		outboxClaim, ok, err := store.ClaimOutbox(ctx, scope("tenant-a"), channels.ClaimOutboxRequest{
			OutboxID:    outboxID,
			SendToken:   token,
			SentBy:      "sender-main",
			SendTimeout: 20 * time.Second,
			Now:         at,
		})
		require.NoError(t, err)
		require.True(t, ok)
		return outboxClaim
	}
	complete := func(
		outboxID, token string,
		result channels.SendResult,
		at time.Time,
	) channels.OutboxStatus {
		t.Helper()
		status, err := store.CompleteOutbox(ctx, scope("tenant-a"), channels.CompleteOutboxRequest{
			Token:  channels.OutboxToken{OutboxID: outboxID, SendToken: token},
			Result: result,
			Now:    at,
		})
		require.NoError(t, err)
		return status
	}
	part := func(outboxID string) channels.OutboxPart {
		t.Helper()
		stored, err := store.GetOutboxPart(ctx, scope("tenant-a"), outboxID)
		require.NoError(t, err)
		return stored
	}

	// Delivered.
	successDraft := draftIn("send-success", 0, 2)
	answer(t, ctx, store, "tenant-a", "send-success", []channels.OutboxDraft{successDraft}, now)
	success := claim(successDraft.OutboxID, "send-success", sendAt)
	require.Equal(t, successDraft.ClientMessageID, success.Part.ClientMessageID)
	require.Equal(t, channels.OutboxSent, complete(
		successDraft.OutboxID,
		"send-success",
		channels.SendResult{
			Outcome:           channels.SendSucceeded,
			ExternalMessageID: "external-message-success",
		},
		doneAt,
	))
	sent := part(successDraft.OutboxID)
	require.Equal(t, "external-message-success", sent.ExternalMessageID)
	require.NotNil(t, sent.SentAt)

	// Refused for now, then delivered. The old token cannot reach the new
	// generation, and the new one cannot be claimed before its backoff.
	retryDraft := draftIn("send-retry", 0, 2)
	answer(t, ctx, store, "tenant-a", "send-retry", []channels.OutboxDraft{retryDraft}, now)
	retry := claim(retryDraft.OutboxID, "send-retry-old", sendAt)
	require.Equal(t, channels.OutboxPending, complete(
		retryDraft.OutboxID,
		"send-retry-old",
		channels.SendResult{Outcome: channels.SendRetryable, ErrorType: channels.ErrorRateLimited},
		doneAt,
	))
	retried := part(retryDraft.OutboxID)
	require.Equal(t, channels.ErrorRateLimited, retried.ErrorType)
	require.Equal(
		t,
		channels.NormalizeTime(doneAt.Add(channels.Backoff(1, retried.OutboxID))),
		retried.NextAttemptAt,
	)
	_, ok, err := store.ClaimOutbox(ctx, scope("tenant-a"), channels.ClaimOutboxRequest{
		OutboxID:    retried.OutboxID,
		SendToken:   "send-retry-too-early",
		SentBy:      "sender-main",
		SendTimeout: 20 * time.Second,
		Now:         retried.NextAttemptAt.Add(-time.Microsecond),
	})
	require.NoError(t, err)
	require.False(t, ok)
	retryNew := claim(retryDraft.OutboxID, "send-retry-new", retried.NextAttemptAt)
	require.Equal(t, retry.Part.ClientMessageID, retryNew.Part.ClientMessageID)
	_, err = store.CompleteOutbox(ctx, scope("tenant-a"), channels.CompleteOutboxRequest{
		Token: channels.OutboxToken{
			OutboxID:  retryDraft.OutboxID,
			SendToken: "send-retry-old",
		},
		Result: channels.SendResult{Outcome: channels.SendSucceeded},
		Now:    retried.NextAttemptAt,
	})
	require.ErrorIs(t, err, channels.ErrStaleClaim)
	require.Equal(t, channels.OutboxSent, complete(
		retryDraft.OutboxID,
		"send-retry-new",
		channels.SendResult{Outcome: channels.SendSucceeded},
		retried.NextAttemptAt.Add(time.Second),
	))

	// Refused for good.
	permanentDraft := draftIn("send-permanent", 0, 2)
	answer(t, ctx, store, "tenant-a", "send-permanent", []channels.OutboxDraft{permanentDraft}, now)
	claim(permanentDraft.OutboxID, "send-permanent", sendAt)
	require.Equal(t, channels.OutboxFailed, complete(
		permanentDraft.OutboxID,
		"send-permanent",
		channels.SendResult{Outcome: channels.SendPermanent, ErrorType: channels.ErrorPermanent},
		doneAt,
	))

	// Twice unknown is out of attempts, and the duplicate risk is sticky across
	// both of them: the part may already have been delivered twice.
	unknownDraft := draftIn("send-unknown", 0, 2)
	answer(t, ctx, store, "tenant-a", "send-unknown", []channels.OutboxDraft{unknownDraft}, now)
	claim(unknownDraft.OutboxID, "send-unknown-first", sendAt)
	require.Equal(t, channels.OutboxPending, complete(
		unknownDraft.OutboxID,
		"send-unknown-first",
		channels.SendResult{Outcome: channels.SendUnknown, ErrorType: channels.ErrorOutcomeUnknown},
		doneAt,
	))
	unknown := part(unknownDraft.OutboxID)
	require.True(t, unknown.DuplicateRisk)
	claim(unknownDraft.OutboxID, "send-unknown-second", unknown.NextAttemptAt)
	require.Equal(t, channels.OutboxFailed, complete(
		unknownDraft.OutboxID,
		"send-unknown-second",
		channels.SendResult{Outcome: channels.SendUnknown, ErrorType: channels.ErrorOutcomeUnknown},
		unknown.NextAttemptAt.Add(time.Second),
	))
	unknown = part(unknownDraft.OutboxID)
	require.True(t, unknown.DuplicateRisk)
	require.Equal(t, channels.ErrorAttemptsExhausted, unknown.ErrorType)

	// A send that ran out of time is an unknown outcome, not a failure: the
	// request left this process and may well have arrived.
	expiringDraft := draftIn("send-expiring", 0, 2)
	answer(t, ctx, store, "tenant-a", "send-expiring", []channels.OutboxDraft{expiringDraft}, now)
	expiring := claim(expiringDraft.OutboxID, "send-expiring", sendAt)
	recovered, err := store.RecoverOutbox(ctx, channels.RecoverRequest{
		Now:   *expiring.Part.SendDeadlineAt,
		Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, []channels.OutboxRecovery{{
		OutboxRef: channels.OutboxRef{TenantID: "tenant-a", OutboxID: expiringDraft.OutboxID},
		Outcome:   channels.RequeueRetried,
	}}, recovered)
	_, err = store.CompleteOutbox(ctx, scope("tenant-a"), channels.CompleteOutboxRequest{
		Token: channels.OutboxToken{
			OutboxID:  expiringDraft.OutboxID,
			SendToken: "send-expiring",
		},
		Result: channels.SendResult{Outcome: channels.SendSucceeded},
		Now:    *expiring.Part.SendDeadlineAt,
	})
	require.ErrorIs(t, err, channels.ErrStaleClaim)
	expired := part(expiringDraft.OutboxID)
	require.Equal(t, channels.OutboxPending, expired.Status)
	require.True(t, expired.DuplicateRisk)
	require.Equal(t, channels.ErrorOutcomeUnknown, expired.ErrorType)
}

func assertScansLostWakeups(t *testing.T, store channels.Store) {
	ctx := Context(t)
	now := fixtureTime()
	requestB := Request("tenant-b", "scan-z", "session-scan-b", "event-scan-b")
	requestA := Request("tenant-a", "scan-a", "session-scan-a", "event-scan-a")
	acceptedB := accept(t, ctx, store, requestB)
	acceptedA := accept(t, ctx, store, requestA)
	scan := channels.DispatchScanRequest{Now: now, StaleAfter: time.Minute, Limit: 10}
	runs, err := store.ListDispatchableRuns(ctx, scan)
	require.NoError(t, err)
	require.Equal(t, []channels.RunDispatch{
		{RunRef: channels.RunRef{TenantID: "tenant-a", RunID: acceptedA.RunID}},
		{RunRef: channels.RunRef{TenantID: "tenant-b", RunID: acceptedB.RunID}},
	}, runs, "a fresh accept is due on attempt 0")
	marked, err := store.MarkRunsDispatched(ctx, runs, now)
	require.NoError(t, err)
	require.Equal(t, 2, marked)
	runs, err = store.ListDispatchableRuns(ctx, channels.DispatchScanRequest{
		Now: now.Add(30 * time.Second), StaleAfter: time.Minute, Limit: 10,
	})
	require.NoError(t, err)
	require.Empty(t, runs)
	runs, err = store.ListDispatchableRuns(ctx, channels.DispatchScanRequest{
		Now: now.Add(2 * time.Minute), StaleAfter: time.Minute, Limit: 10,
	})
	require.NoError(t, err)
	require.Len(t, runs, 2)

	claim := claimRun(t, ctx, store, "tenant-a", requestA.Envelope.SessionKey(), "claim-scan-run", now)
	finishSucceeded(t, ctx, store, "tenant-a", claim, []channels.OutboxDraft{draft(0, 2)}, now.Add(time.Second))
	outboxScanTime := now.Add(2 * time.Second)
	outbox, err := store.ListDispatchableOutbox(ctx, channels.DispatchScanRequest{
		Now: outboxScanTime, StaleAfter: time.Minute, Limit: 10,
	})
	require.NoError(t, err)
	// The channel travels out of the scan with the candidate, so the wakeup the
	// scanner publishes can name it and a dispatcher can pick a sender before it
	// claims anything. See channels.OutboxWakeup.Channel.
	wantPart := []channels.OutboxDispatch{
		{
			OutboxRef: channels.OutboxRef{TenantID: "tenant-a", OutboxID: "outbox-0"},
			Channel:   channels.ChannelFeishu,
		},
	}
	require.Equal(t, wantPart, outbox)
	marked, err = store.MarkOutboxDispatched(ctx, outbox, outboxScanTime)
	require.NoError(t, err)
	require.Equal(t, 1, marked)
	outbox, err = store.ListDispatchableOutbox(ctx, channels.DispatchScanRequest{
		Now: outboxScanTime.Add(30 * time.Second), StaleAfter: time.Minute, Limit: 10,
	})
	require.NoError(t, err)
	require.Empty(t, outbox)
	outbox, err = store.ListDispatchableOutbox(ctx, channels.DispatchScanRequest{
		Now: outboxScanTime.Add(2 * time.Minute), StaleAfter: time.Minute, Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, wantPart, outbox)
}

// assertClearsDispatchMarksWhenRequeued covers every path that puts work back
// into the queue. A requeue starts a new generation, so the wakeup recorded for
// the generation that just ended must not survive it: if it did, the row would
// stay out of the dispatch scan until the mark went stale, which is a scanner
// tuning constant, instead of until its own backoff expired.
func assertClearsDispatchMarksWhenRequeued(t *testing.T, store channels.Store) {
	ctx := Context(t)
	now := fixtureTime()
	runRef := func(runID string) channels.RunRef {
		return channels.RunRef{TenantID: "tenant-a", RunID: runID}
	}
	outboxRef := func(outboxID string) channels.OutboxRef {
		return channels.OutboxRef{TenantID: "tenant-a", OutboxID: outboxID}
	}
	// StaleAfter is deliberately far longer than any backoff used below, so a
	// row that becomes listable again can only have got there by losing its
	// mark, never by the mark ageing out.
	scan := func(at time.Time) channels.DispatchScanRequest {
		return channels.DispatchScanRequest{
			Now: at, StaleAfter: time.Hour, Limit: channels.MaxListLimit,
		}
	}
	dueRuns := func(at time.Time) []channels.RunDispatch {
		t.Helper()
		candidates, err := store.ListDispatchableRuns(ctx, scan(at))
		require.NoError(t, err)
		return candidates
	}
	dueParts := func(at time.Time) []channels.OutboxDispatch {
		t.Helper()
		candidates, err := store.ListDispatchableOutbox(ctx, scan(at))
		require.NoError(t, err)
		return candidates
	}
	markRun := func(runID string, attempt int32, at time.Time) int {
		t.Helper()
		marked, err := store.MarkRunsDispatched(ctx, []channels.RunDispatch{
			{RunRef: runRef(runID), Attempt: attempt},
		}, at)
		require.NoError(t, err)
		return marked
	}
	markPart := func(outboxID string, attempt int32, at time.Time) int {
		t.Helper()
		marked, err := store.MarkOutboxDispatched(ctx, []channels.OutboxDispatch{
			{OutboxRef: outboxRef(outboxID), Channel: channels.ChannelFeishu, Attempt: attempt},
		}, at)
		require.NoError(t, err)
		return marked
	}
	requireRunRequeued := func(runID string, attempt int32, requeueAt time.Time) {
		t.Helper()
		stored, err := store.GetRun(ctx, scope("tenant-a"), runID)
		require.NoError(t, err)
		require.Equal(t, channels.RunAccepted, stored.Status)
		require.Equal(t, attempt, stored.Attempt)
		require.Nil(t, stored.LastDispatchedAt,
			"the requeue must drop the mark that belonged to the finished attempt")
		require.Equal(t,
			channels.NormalizeTime(requeueAt.Add(channels.Backoff(attempt, runID))),
			stored.NextAttemptAt)
		want := channels.RunDispatch{RunRef: runRef(runID), Attempt: attempt}
		require.NotContains(t, dueRuns(stored.NextAttemptAt.Add(-time.Microsecond)), want)
		require.Contains(t, dueRuns(stored.NextAttemptAt), want,
			"the new attempt is due on its backoff, not one staleness window later")
	}
	requirePartRequeued := func(outboxID string, attempt int32, requeueAt time.Time) {
		t.Helper()
		stored, err := store.GetOutboxPart(ctx, scope("tenant-a"), outboxID)
		require.NoError(t, err)
		require.Equal(t, channels.OutboxPending, stored.Status)
		require.Equal(t, attempt, stored.Attempt)
		require.Nil(t, stored.LastDispatchedAt,
			"the requeue must drop the mark that belonged to the finished send")
		require.Equal(t,
			channels.NormalizeTime(requeueAt.Add(channels.Backoff(attempt, outboxID))),
			stored.NextAttemptAt)
		want := channels.OutboxDispatch{
			OutboxRef: outboxRef(outboxID),
			Channel:   channels.ChannelFeishu,
			Attempt:   attempt,
		}
		require.NotContains(t, dueParts(stored.NextAttemptAt.Add(-time.Microsecond)), want)
		require.Contains(t, dueParts(stored.NextAttemptAt), want,
			"the new send is due on its backoff, not one staleness window later")
	}

	// A Worker that gave the Run back before starting it.
	yieldRequest := Request("tenant-a", "requeue-yield", "session-requeue-yield", "event-requeue-yield")
	yielded := accept(t, ctx, store, yieldRequest)
	require.Equal(t, 1, markRun(yielded.RunID, yielded.Attempt, now))
	yieldClaim := claimRun(
		t, ctx, store, "tenant-a", yieldRequest.Envelope.SessionKey(), "claim-requeue-yield", now)
	yieldAt := now.Add(time.Second)
	outcome, err := store.YieldRun(ctx, scope("tenant-a"), channels.RunToken{
		RunID:      yieldClaim.Run.RunID,
		ClaimToken: yieldClaim.Run.ClaimToken,
	}, channels.YieldRunRequest{ErrorType: channels.ErrorRunCancelled, Now: yieldAt})
	require.NoError(t, err)
	require.Equal(t, channels.RequeueRetried, outcome)
	requireRunRequeued(yielded.RunID, 1, yieldAt)

	// A Worker that died holding the claim.
	recoverRequest := Request(
		"tenant-a", "requeue-recover", "session-requeue-recover", "event-requeue-recover")
	recovering := accept(t, ctx, store, recoverRequest)
	require.Equal(t, 1, markRun(recovering.RunID, recovering.Attempt, now))
	recoverClaim := claimRun(
		t, ctx, store, "tenant-a", recoverRequest.Envelope.SessionKey(), "claim-requeue-recover", now)
	recoveryTime := *recoverClaim.Run.RecoverAfter
	recoveries, err := store.RecoverRuns(ctx, channels.RecoverRequest{Now: recoveryTime, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []channels.RunRecovery{{
		RunRef:  runRef(recovering.RunID),
		Outcome: channels.RequeueRetried,
	}}, recoveries)
	requireRunRequeued(recovering.RunID, 1, recoveryTime)

	// The same three shapes on the sending side. One answer each, because a part
	// is only claimable while it is its answer's head and these three have to be
	// in flight independently — see draftIn.
	finishedAt := now.Add(time.Second)
	retryableDraft := draftIn("requeue-retryable", 0, 3)
	answer(t, ctx, store, "tenant-a", "requeue-retryable", []channels.OutboxDraft{retryableDraft}, now)
	require.Equal(t, 1, markPart(retryableDraft.OutboxID, 0, finishedAt))
	claimOutbox(t, ctx, store, "tenant-a", retryableDraft.OutboxID, "send-requeue-retryable", finishedAt)
	retryableAt := finishedAt.Add(time.Second)
	require.Equal(t, channels.OutboxPending, completeOutbox(
		t, ctx, store, "tenant-a", retryableDraft.OutboxID, "send-requeue-retryable",
		channels.SendResult{Outcome: channels.SendRetryable, ErrorType: channels.ErrorRateLimited},
		retryableAt))
	requirePartRequeued(retryableDraft.OutboxID, 1, retryableAt)

	unknownDraft := draftIn("requeue-unknown", 0, 3)
	answer(t, ctx, store, "tenant-a", "requeue-unknown", []channels.OutboxDraft{unknownDraft}, now)
	require.Equal(t, 1, markPart(unknownDraft.OutboxID, 0, finishedAt))
	claimOutbox(t, ctx, store, "tenant-a", unknownDraft.OutboxID, "send-requeue-unknown", finishedAt)
	unknownAt := finishedAt.Add(2 * time.Second)
	require.Equal(t, channels.OutboxPending, completeOutbox(
		t, ctx, store, "tenant-a", unknownDraft.OutboxID, "send-requeue-unknown",
		channels.SendResult{Outcome: channels.SendUnknown, ErrorType: channels.ErrorOutcomeUnknown},
		unknownAt))
	requirePartRequeued(unknownDraft.OutboxID, 1, unknownAt)

	expiringDraft := draftIn("requeue-expiring", 0, 3)
	answer(t, ctx, store, "tenant-a", "requeue-expiring", []channels.OutboxDraft{expiringDraft}, now)
	require.Equal(t, 1, markPart(expiringDraft.OutboxID, 0, finishedAt))
	expiring := claimOutbox(
		t, ctx, store, "tenant-a", expiringDraft.OutboxID, "send-requeue-expiring", finishedAt)
	partRecoveryTime := *expiring.Part.SendDeadlineAt
	partRecoveries, err := store.RecoverOutbox(
		ctx, channels.RecoverRequest{Now: partRecoveryTime, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []channels.OutboxRecovery{{
		OutboxRef: outboxRef(expiringDraft.OutboxID),
		Outcome:   channels.RequeueRetried,
	}}, partRecoveries)
	requirePartRequeued(expiringDraft.OutboxID, 1, partRecoveryTime)
}

// assertFencesDispatchGenerations pins the CAS that keeps a wakeup mark tied to
// the attempt it was published for. Announcing and recording are two calls, so
// the row can be claimed and requeued in between; the mark from the old
// generation must then be refused rather than silence the new one. The same CAS
// also refuses a mark for a generation that is still backing off, which is what
// stops a duplicate ingress from suppressing the scan that is meant to pick the
// retry up.
func assertFencesDispatchGenerations(t *testing.T, store channels.Store) {
	ctx := Context(t)
	now := fixtureTime()
	runRef := func(runID string) channels.RunRef {
		return channels.RunRef{TenantID: "tenant-a", RunID: runID}
	}
	outboxRef := func(outboxID string) channels.OutboxRef {
		return channels.OutboxRef{TenantID: "tenant-a", OutboxID: outboxID}
	}
	markRuns := func(dispatches []channels.RunDispatch, at time.Time) int {
		t.Helper()
		marked, err := store.MarkRunsDispatched(ctx, dispatches, at)
		require.NoError(t, err)
		return marked
	}
	markParts := func(dispatches []channels.OutboxDispatch, at time.Time) int {
		t.Helper()
		marked, err := store.MarkOutboxDispatched(ctx, dispatches, at)
		require.NoError(t, err)
		return marked
	}
	runStamp := func(runID string) *time.Time {
		t.Helper()
		stored, err := store.GetRun(ctx, scope("tenant-a"), runID)
		require.NoError(t, err)
		return stored.LastDispatchedAt
	}
	partStamp := func(outboxID string) *time.Time {
		t.Helper()
		stored, err := store.GetOutboxPart(ctx, scope("tenant-a"), outboxID)
		require.NoError(t, err)
		return stored.LastDispatchedAt
	}

	request := Request("tenant-a", "generation", "session-generation", "event-generation")
	accepted := accept(t, ctx, store, request)
	require.Equal(t, int32(0), accepted.Attempt, "a fresh accept is the zeroth generation")
	stale := channels.RunDispatch{RunRef: runRef(accepted.RunID), Attempt: accepted.Attempt}
	require.Equal(t, 1, markRuns([]channels.RunDispatch{stale}, now))
	require.Equal(t, channels.NormalizeTime(now), *runStamp(accepted.RunID))

	claim := claimRun(
		t, ctx, store, "tenant-a", request.Envelope.SessionKey(), "claim-generation", now)
	require.Equal(t, int32(1), claim.Run.Attempt)
	yieldAt := now.Add(time.Second)
	outcome, err := store.YieldRun(ctx, scope("tenant-a"), channels.RunToken{
		RunID:      claim.Run.RunID,
		ClaimToken: claim.Run.ClaimToken,
	}, channels.YieldRunRequest{ErrorType: channels.ErrorRunCancelled, Now: yieldAt})
	require.NoError(t, err)
	require.Equal(t, channels.RequeueRetried, outcome)
	require.Nil(t, runStamp(accepted.RunID))

	// The publisher of the previous generation finally gets to its Mark.
	require.Equal(t, 0, markRuns([]channels.RunDispatch{stale}, yieldAt.Add(time.Second)))
	require.Nil(t, runStamp(accepted.RunID),
		"a spent generation must not record a wakeup for the one that replaced it")

	current := channels.RunDispatch{RunRef: runRef(accepted.RunID), Attempt: 1}
	dueAt := channels.NormalizeTime(yieldAt.Add(channels.Backoff(1, accepted.RunID)))
	require.Equal(t, 0, markRuns([]channels.RunDispatch{current}, dueAt.Add(-time.Microsecond)))
	require.Nil(t, runStamp(accepted.RunID),
		"a wakeup published while the row is still backing off announces nothing yet")
	require.Equal(t, 1, markRuns([]channels.RunDispatch{current}, dueAt))
	require.Equal(t, dueAt, *runStamp(accepted.RunID))

	// Two cycles overlapped and the older one finished last.
	later := channels.NormalizeTime(dueAt.Add(time.Minute))
	require.Equal(t, 1, markRuns([]channels.RunDispatch{current}, later))
	require.Equal(t, later, *runStamp(accepted.RunID))
	require.Equal(t, 1, markRuns([]channels.RunDispatch{current}, dueAt),
		"the row still matched the CAS, so it still counts as marked")
	require.Equal(t, later, *runStamp(accepted.RunID),
		"an older cycle must not reopen a row a newer cycle already announced")

	// A batch reports the rows it actually matched, not the rows it was handed.
	batch := make([]channels.RunDispatch, 0, 3)
	for _, suffix := range []string{"batch-a", "batch-b", "batch-c"} {
		batchRequest := Request("tenant-a", suffix, "session-"+suffix, "event-"+suffix)
		batchAccepted := accept(t, ctx, store, batchRequest)
		batch = append(batch, channels.RunDispatch{
			RunRef:  runRef(batchAccepted.RunID),
			Attempt: batchAccepted.Attempt,
		})
	}
	missed := batch[1]
	batch[1].Attempt = 7
	require.Equal(t, 2, markRuns(batch, now))
	require.NotNil(t, runStamp(batch[0].RunID))
	require.Nil(t, runStamp(missed.RunID), "one wrong generation must not carry the batch")
	require.NotNil(t, runStamp(batch[2].RunID))
	require.Equal(t, 0, markRuns([]channels.RunDispatch{
		{RunRef: channels.RunRef{TenantID: "tenant-b", RunID: batch[0].RunID}, Attempt: batch[0].Attempt},
		{RunRef: runRef("run-absent"), Attempt: 0},
	}, now.Add(time.Second)))
	require.Equal(t, channels.NormalizeTime(now), *runStamp(batch[0].RunID),
		"another tenant's candidate must not reach this tenant's row")

	// The Outbox is fenced by the same rules.
	partsRequest := Request(
		"tenant-a", "generation-parts", "session-generation-parts", "event-generation-parts")
	accept(t, ctx, store, partsRequest)
	partsClaim := claimRun(
		t, ctx, store, "tenant-a", partsRequest.Envelope.SessionKey(), "claim-generation-parts", now)
	finishedAt := now.Add(time.Second)
	finishSucceeded(t, ctx, store, "tenant-a", partsClaim,
		[]channels.OutboxDraft{draft(0, 3), draft(1, 3)}, finishedAt)

	stalePart := channels.OutboxDispatch{
		OutboxRef: outboxRef("outbox-0"),
		Channel:   channels.ChannelFeishu,
		Attempt:   0,
	}
	require.Equal(t, 2, markParts([]channels.OutboxDispatch{
		stalePart,
		{OutboxRef: outboxRef("outbox-1"), Channel: channels.ChannelFeishu, Attempt: 0},
	}, finishedAt))
	claimOutbox(t, ctx, store, "tenant-a", "outbox-0", "send-generation", finishedAt)
	retryAt := finishedAt.Add(time.Second)
	require.Equal(t, channels.OutboxPending, completeOutbox(
		t, ctx, store, "tenant-a", "outbox-0", "send-generation",
		channels.SendResult{Outcome: channels.SendRetryable, ErrorType: channels.ErrorRateLimited},
		retryAt))
	require.Nil(t, partStamp("outbox-0"))

	require.Equal(t, 0, markParts([]channels.OutboxDispatch{stalePart}, retryAt.Add(time.Second)))
	require.Nil(t, partStamp("outbox-0"),
		"a spent send generation must not record a wakeup for the retry")
	currentPart := channels.OutboxDispatch{
		OutboxRef: outboxRef("outbox-0"),
		Channel:   channels.ChannelFeishu,
		Attempt:   1,
	}
	partDueAt := channels.NormalizeTime(retryAt.Add(channels.Backoff(1, "outbox-0")))
	require.Equal(t, 0, markParts([]channels.OutboxDispatch{currentPart}, partDueAt.Add(-time.Microsecond)))
	require.Nil(t, partStamp("outbox-0"))
	require.Equal(t, 1, markParts([]channels.OutboxDispatch{
		currentPart,
		{OutboxRef: outboxRef("outbox-1"), Channel: channels.ChannelFeishu, Attempt: 9},
	}, partDueAt))
	require.Equal(t, partDueAt, *partStamp("outbox-0"))
	require.Equal(t, channels.NormalizeTime(finishedAt), *partStamp("outbox-1"))

	partLater := channels.NormalizeTime(partDueAt.Add(time.Minute))
	require.Equal(t, 1, markParts([]channels.OutboxDispatch{currentPart}, partLater))
	require.Equal(t, partLater, *partStamp("outbox-0"))
	require.Equal(t, 1, markParts([]channels.OutboxDispatch{currentPart}, partDueAt))
	require.Equal(t, partLater, *partStamp("outbox-0"))
}

// assertRecordsFirstExecutionPermanently pins the marker that survives a claim.
// ExecutionStartedAt belongs to the current attempt and has to be cleared with
// the rest of the claim, or the next attempt could never yield. That would lose
// the fact that some attempt reached the Runner, which is what decides whether a
// retry may be silently re-executed, so a second marker records it once and is
// then never moved and never cleared.
func assertRecordsFirstExecutionPermanently(t *testing.T, store channels.Store) {
	ctx := Context(t)
	now := fixtureTime()
	request := Request("tenant-a", "first-execution", "session-first-execution", "event-first-execution")
	accepted := accept(t, ctx, store, request)
	get := func() channels.Run {
		t.Helper()
		stored, err := store.GetRun(ctx, scope("tenant-a"), accepted.RunID)
		require.NoError(t, err)
		return stored
	}
	fresh := get()
	require.Nil(t, fresh.ExecutionStartedAt)
	require.Nil(t, fresh.FirstExecutionStartedAt)

	claim := claimRun(
		t, ctx, store, "tenant-a", request.Envelope.SessionKey(), "claim-first-execution", now)
	require.Nil(t, claim.Run.ExecutionStartedAt)
	require.Nil(t, claim.Run.FirstExecutionStartedAt,
		"a claim is not an execution: the Worker may still hand the Run back")
	token := channels.RunToken{RunID: claim.Run.RunID, ClaimToken: claim.Run.ClaimToken}

	startedAt := channels.NormalizeTime(now.Add(2 * time.Second))
	require.NoError(t, store.MarkRunStarted(ctx, scope("tenant-a"), token, startedAt))
	require.NoError(t, store.MarkRunStarted(
		ctx, scope("tenant-a"), token, startedAt.Add(time.Second)))
	started := get()
	require.Equal(t, startedAt, *started.ExecutionStartedAt)
	require.Equal(t, startedAt, *started.FirstExecutionStartedAt,
		"both marks are written under one claim, and a retried write moves neither")

	// A returned pointer must not be a handle on the Store's own record.
	*started.FirstExecutionStartedAt = startedAt.Add(time.Hour)
	require.Equal(t, startedAt, *get().FirstExecutionStartedAt)

	recoveryTime := *claim.Run.RecoverAfter
	recovered, err := store.RecoverRuns(ctx, channels.RecoverRequest{Now: recoveryTime, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []channels.RunRecovery{{
		RunRef:  channels.RunRef{TenantID: "tenant-a", RunID: accepted.RunID},
		Outcome: channels.RequeueRetried,
	}}, recovered)
	requeued := get()
	require.Nil(t, requeued.ExecutionStartedAt, "the next attempt has not reached the Runner yet")
	require.Equal(t, startedAt, *requeued.FirstExecutionStartedAt)

	require.ErrorIs(t,
		store.MarkRunStarted(ctx, scope("tenant-a"), token, recoveryTime),
		channels.ErrStaleClaim)
	afterStale := get()
	require.Nil(t, afterStale.ExecutionStartedAt)
	require.Equal(t, startedAt, *afterStale.FirstExecutionStartedAt,
		"a recovered claim writes neither marker")

	// The permanent marker must not make the next attempt un-yieldable.
	secondAttempt := claimRun(t, ctx, store, "tenant-a", request.Envelope.SessionKey(),
		"claim-first-execution-second", requeued.NextAttemptAt)
	require.Equal(t, int32(2), secondAttempt.Run.Attempt)
	require.Nil(t, secondAttempt.Run.ExecutionStartedAt)
	require.Equal(t, startedAt, *secondAttempt.Run.FirstExecutionStartedAt)
	yieldAt := requeued.NextAttemptAt.Add(time.Second)
	outcome, err := store.YieldRun(ctx, scope("tenant-a"), channels.RunToken{
		RunID:      secondAttempt.Run.RunID,
		ClaimToken: secondAttempt.Run.ClaimToken,
	}, channels.YieldRunRequest{ErrorType: channels.ErrorRunCancelled, Now: yieldAt})
	require.NoError(t, err)
	require.Equal(t, channels.RequeueRetried, outcome)
	require.Equal(t, startedAt, *get().FirstExecutionStartedAt)

	// A later execution moves only the attempt-scoped marker.
	thirdAt := get().NextAttemptAt
	thirdAttempt := claimRun(t, ctx, store, "tenant-a", request.Envelope.SessionKey(),
		"claim-first-execution-third", thirdAt)
	require.Equal(t, int32(3), thirdAttempt.Run.Attempt)
	restartedAt := channels.NormalizeTime(thirdAt.Add(time.Second))
	require.NoError(t, store.MarkRunStarted(ctx, scope("tenant-a"), channels.RunToken{
		RunID:      thirdAttempt.Run.RunID,
		ClaimToken: thirdAttempt.Run.ClaimToken,
	}, restartedAt))
	restarted := get()
	require.Equal(t, restartedAt, *restarted.ExecutionStartedAt)
	require.Equal(t, startedAt, *restarted.FirstExecutionStartedAt,
		"the first execution is recorded once and never moves")

	finishSucceeded(t, ctx, store, "tenant-a", thirdAttempt,
		[]channels.OutboxDraft{draft(0, 2)}, restartedAt.Add(time.Second))
	finished := get()
	require.Equal(t, channels.RunSucceeded, finished.Status)
	require.Nil(t, finished.ExecutionStartedAt)
	require.Equal(t, startedAt, *finished.FirstExecutionStartedAt)

	// Exhaustion is the other terminal path, and it keeps the marker too.
	shortRequest := Request("tenant-a", "first-execution-spent",
		"session-first-execution-spent", "event-first-execution-spent")
	shortRequest.Policy.MaxAttempts = 1
	shortAccepted := accept(t, ctx, store, shortRequest)
	shortClaim := claimRun(t, ctx, store, "tenant-a", shortRequest.Envelope.SessionKey(),
		"claim-first-execution-spent", now)
	shortStartedAt := channels.NormalizeTime(now.Add(time.Second))
	require.NoError(t, store.MarkRunStarted(ctx, scope("tenant-a"), channels.RunToken{
		RunID:      shortClaim.Run.RunID,
		ClaimToken: shortClaim.Run.ClaimToken,
	}, shortStartedAt))
	spent, err := store.RecoverRuns(
		ctx, channels.RecoverRequest{Now: *shortClaim.Run.RecoverAfter, Limit: 10})
	require.NoError(t, err)
	require.Equal(t, []channels.RunRecovery{{
		RunRef:  channels.RunRef{TenantID: "tenant-a", RunID: shortAccepted.RunID},
		Outcome: channels.RequeueExhausted,
	}}, spent)
	exhausted, err := store.GetRun(ctx, scope("tenant-a"), shortAccepted.RunID)
	require.NoError(t, err)
	require.Equal(t, channels.RunFailed, exhausted.Status)
	require.Nil(t, exhausted.ExecutionStartedAt)
	require.Equal(t, shortStartedAt, *exhausted.FirstExecutionStartedAt)
}

func assertHonorsCanceledAndInvalidCalls(t *testing.T, store channels.Store) {
	request := Request("tenant-a", "invalid-call", "session-invalid", "event-invalid")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := store.Accept(canceled, scope("tenant-a"), request)
	require.ErrorIs(t, err, context.Canceled)

	_, err = store.Accept(nil, scope("tenant-a"), request)
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)

	_, err = store.Accept(Context(t), scope("tenant-b"), request)
	require.ErrorIs(t, err, tenant.ErrTenantScope)

	_, _, err = store.ClaimNextRun(Context(t), scope("tenant-a"), sessiondir.Key{
		TenantID: "tenant-b", AppID: "app-main", PrincipalID: "principal-main", SessionID: "session-invalid",
	}, channels.ClaimRunRequest{
		ClaimToken: "claim-invalid",
		ClaimedBy:  "worker-main",
		Now:        fixtureTime(),
	})
	require.ErrorIs(t, err, tenant.ErrTenantScope)

	_, err = store.ListDispatchableRuns(Context(t), channels.DispatchScanRequest{
		Now: fixtureTime(), StaleAfter: time.Minute, Limit: channels.MaxListLimit + 1,
	})
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	require.True(t, errors.Is(err, tenant.ErrInvalidArgument))
}

func assertRejectsUnrepresentableOrInconsistentRecords(t *testing.T, store channels.Store) {
	ctx := Context(t)
	now := fixtureTime()

	// PostgreSQL persists these fields as integer milliseconds. Rejecting a
	// finer value at the domain boundary keeps the memory and SQL Stores from
	// returning different policies for the same accepted request.
	tooPrecise := Request("tenant-a", "precise-policy", "session-precise", "event-precise")
	tooPrecise.Policy.MaxRunDuration += time.Microsecond
	_, err := store.Accept(ctx, scope("tenant-a"), tooPrecise)
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)

	request := Request("tenant-a", "stats-mismatch", "session-stats", "event-stats")
	accept(t, ctx, store, request)
	claim := claimRun(t, ctx, store, "tenant-a", request.Envelope.SessionKey(), "claim-stats", now)
	_, err = store.FinishRun(ctx, scope("tenant-a"), channels.FinishRunRequest{
		Token: channels.RunToken{
			RunID:      claim.Run.RunID,
			ClaimToken: claim.Run.ClaimToken,
		},
		Status:     channels.RunSucceeded,
		RevisionID: "revision-main",
		Stats:      channels.RunStats{OutputParts: 2},
		Outbox:     []channels.OutboxDraft{draft(0, 2)},
		Now:        now.Add(time.Second),
	})
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)
	stored, err := store.GetRun(ctx, scope("tenant-a"), claim.Run.RunID)
	require.NoError(t, err)
	require.Equal(t, channels.RunRunning, stored.Status)
	parts, err := store.ListRunOutbox(ctx, scope("tenant-a"), claim.Run.RunID, channels.ListRequest{Limit: 10})
	require.NoError(t, err)
	require.Empty(t, parts)
}

// assertSendsAnAnswersPartsInOrder pins the rule that makes several parts one
// answer rather than several messages that happen to share a Run.
//
// Both halves of it are here because both are the same rule seen from different
// ends. A part may not be claimed while an earlier part of its answer is unsent,
// so a reply cannot reach a user out of order or with a hole in it; and once a
// part has failed for good, the parts behind it are closed rather than left
// pending, because the alternative is a queue full of rows that can never become
// eligible and a user who is shown the second half of a sentence whose first
// half was refused.
//
// The scan has to agree with the claim, or the pipeline announces wakeups for
// parts nothing is allowed to send — each of which costs an acknowledgement, and
// then a dispatch mark that hides the row for a whole staleness window.
func assertSendsAnAnswersPartsInOrder(t *testing.T, store channels.Store) {
	ctx := Context(t)
	now := fixtureTime()
	drafts := []channels.OutboxDraft{
		draftIn("ordered", 0, 2),
		draftIn("ordered", 1, 2),
		draftIn("ordered", 2, 2),
	}
	answer(t, ctx, store, "tenant-a", "ordered", drafts, now)

	tryClaim := func(outboxID, token string, at time.Time) bool {
		t.Helper()
		_, ok, err := store.ClaimOutbox(ctx, scope("tenant-a"), channels.ClaimOutboxRequest{
			OutboxID:    outboxID,
			SendToken:   token,
			SentBy:      "sender-main",
			SendTimeout: 20 * time.Second,
			Now:         at,
		})
		require.NoError(t, err)
		return ok
	}
	part := func(outboxID string) channels.OutboxPart {
		t.Helper()
		stored, err := store.GetOutboxPart(ctx, scope("tenant-a"), outboxID)
		require.NoError(t, err)
		return stored
	}
	due := func(at time.Time) []channels.OutboxDispatch {
		t.Helper()
		candidates, err := store.ListDispatchableOutbox(ctx, channels.DispatchScanRequest{
			Now: at, StaleAfter: time.Minute, Limit: channels.MaxListLimit,
		})
		require.NoError(t, err)
		return candidates
	}
	dispatch := func(outboxID string) channels.OutboxDispatch {
		return channels.OutboxDispatch{
			OutboxRef: channels.OutboxRef{TenantID: "tenant-a", OutboxID: outboxID},
			Channel:   channels.ChannelFeishu,
			Attempt:   0,
		}
	}
	send := func(outboxID, token string, at time.Time) {
		t.Helper()
		require.True(t, tryClaim(outboxID, token, at))
		require.Equal(t, channels.OutboxSent, completeOutbox(
			t, ctx, store, "tenant-a", outboxID, token,
			channels.SendResult{Outcome: channels.SendSucceeded}, at.Add(time.Second)))
	}

	// Every part is due by the clock. Only the first is claimable, and refusing
	// the others costs them nothing: an attempt consumed here would be an attempt
	// spent on not trying.
	sendAt := now.Add(2 * time.Second)
	for _, later := range drafts[1:] {
		require.False(t, tryClaim(later.OutboxID, "send-out-of-order", sendAt))
		refused := part(later.OutboxID)
		require.Equal(t, channels.OutboxPending, refused.Status)
		require.Equal(t, int32(0), refused.Attempt, "a part that was not tried consumes no attempt")
		require.Empty(t, refused.SendToken)
	}
	require.Equal(t, []channels.OutboxDispatch{dispatch(drafts[0].OutboxID)}, due(sendAt),
		"the scan announces the head and nothing queued behind it")

	// Delivering the head moves it along by exactly one.
	send(drafts[0].OutboxID, "send-ordered-0", sendAt)
	nextAt := sendAt.Add(2 * time.Second)
	require.Equal(t, []channels.OutboxDispatch{dispatch(drafts[1].OutboxID)}, due(nextAt))
	require.False(t, tryClaim(drafts[2].OutboxID, "send-still-out-of-order", nextAt))

	// The new head fails for good, and the tail behind it is closed with it.
	require.True(t, tryClaim(drafts[1].OutboxID, "send-ordered-1", nextAt))
	failedAt := nextAt.Add(time.Second)
	require.Equal(t, channels.OutboxFailed, completeOutbox(
		t, ctx, store, "tenant-a", drafts[1].OutboxID, "send-ordered-1",
		channels.SendResult{Outcome: channels.SendPermanent, ErrorType: channels.ErrorPermanent},
		failedAt))
	tail := part(drafts[2].OutboxID)
	require.Equal(t, channels.OutboxFailed, tail.Status)
	require.Equal(t, channels.ErrorPredecessorFailed, tail.ErrorType)
	require.Equal(t, int32(0), tail.Attempt, "a part closed by its predecessor was never attempted")
	require.False(t, tail.DuplicateRisk, "nothing was sent, so nothing may have been sent twice")
	require.Nil(t, tail.SentAt)
	require.Empty(t, tail.SendToken)

	// What was already delivered is left alone, and there is nothing left to
	// announce or to claim.
	require.Equal(t, channels.OutboxSent, part(drafts[0].OutboxID).Status)
	require.Empty(t, due(failedAt.Add(time.Hour)))
	require.False(t, tryClaim(drafts[2].OutboxID, "send-after-close", failedAt.Add(time.Hour)))
	require.False(t, tryClaim("", "send-anything-due", failedAt.Add(time.Hour)),
		"the recovery route finds nothing either: eligibility is a property of the row")
}

// assertRefusesAClaimForTheWrongChannel pins the re-check that stops a wakeup
// from deciding which protocol an answer goes out over.
//
// A wakeup names a channel so a shared dispatcher can pick a sender before it
// claims anything, and it arrives over a transport that may lose, duplicate,
// delay and — in the case of a forged one — invent. So the name is a hint, and
// the row is the authority: a claim that disagrees with the row must send
// nothing and, just as importantly, must leave the part exactly as it found it.
// Consuming an attempt on a wakeup that was merely wrong would let a stale
// transport spend a real answer's retries.
func assertRefusesAClaimForTheWrongChannel(t *testing.T, store channels.Store) {
	ctx := Context(t)
	now := fixtureTime()
	only := draftIn("wrong-channel", 0, 2)
	answer(t, ctx, store, "tenant-a", "wrong-channel", []channels.OutboxDraft{only}, now)
	sendAt := now.Add(2 * time.Second)
	claim := func(expected channels.ChannelType, token string) (channels.OutboxClaim, bool) {
		t.Helper()
		outboxClaim, ok, err := store.ClaimOutbox(ctx, scope("tenant-a"), channels.ClaimOutboxRequest{
			OutboxID:        only.OutboxID,
			ExpectedChannel: expected,
			SendToken:       token,
			SentBy:          "sender-main",
			SendTimeout:     20 * time.Second,
			Now:             sendAt,
		})
		require.NoError(t, err)
		return outboxClaim, ok
	}

	before, err := store.GetOutboxPart(ctx, scope("tenant-a"), only.OutboxID)
	require.NoError(t, err)
	require.Equal(t, channels.ChannelFeishu, before.Channel)

	_, ok := claim(channels.ChannelWeCom, "send-wrong-channel")
	require.False(t, ok)
	after, err := store.GetOutboxPart(ctx, scope("tenant-a"), only.OutboxID)
	require.NoError(t, err)
	require.Equal(t, before, after, "a mismatched claim writes nothing at all")

	// The same part, correctly named, is claimable — so the refusal was the
	// channel and not something the row had already spent.
	claimed, ok := claim(channels.ChannelFeishu, "send-right-channel")
	require.True(t, ok)
	require.Equal(t, channels.ChannelFeishu, claimed.Part.Channel)
	require.Equal(t, int32(1), claimed.Part.Attempt)
}

// assertPersistsTheSendDeadline pins the one value every process derives its
// send budget from.
//
// The deadline is absolute and it is written down, because the alternative — a
// duration handed out at claim time — is already wrong by the time the caller
// uses it, and two processes computing it from their own clocks would disagree
// about when the recovery scanner is entitled to take the part back. So the
// Store writes NormalizeTime(now + timeout) and returns exactly that, both
// Stores to the microsecond, and a timeout finer than that resolution is refused
// rather than rounded.
func assertPersistsTheSendDeadline(t *testing.T, store channels.Store) {
	ctx := Context(t)
	now := fixtureTime()
	only := draftIn("deadline", 0, 2)
	answer(t, ctx, store, "tenant-a", "deadline", []channels.OutboxDraft{only}, now)

	// Deliberately not a round number of seconds, and deliberately not aligned to
	// the fixture clock: a Store that stored its own idea of "now" instead of the
	// caller's, or that truncated to the second, would still pass a rounder one.
	sendAt := now.Add(2500 * time.Millisecond)
	timeout := 20*time.Second + 500*time.Microsecond
	claimed, ok, err := store.ClaimOutbox(ctx, scope("tenant-a"), channels.ClaimOutboxRequest{
		OutboxID:    only.OutboxID,
		SendToken:   "send-deadline",
		SentBy:      "sender-main",
		SendTimeout: timeout,
		Now:         sendAt,
	})
	require.NoError(t, err)
	require.True(t, ok)
	want := channels.NormalizeTime(sendAt.Add(timeout))
	require.NotNil(t, claimed.Part.SendDeadlineAt)
	require.Equal(t, want, *claimed.Part.SendDeadlineAt)
	stored, err := store.GetOutboxPart(ctx, scope("tenant-a"), only.OutboxID)
	require.NoError(t, err)
	require.NotNil(t, stored.SendDeadlineAt)
	require.Equal(t, want, *stored.SendDeadlineAt,
		"the claim's answer and the persisted row are the same deadline")

	// A budget the column cannot represent is refused, not rounded. Both Stores
	// have to refuse it, or the same claim would produce two different deadlines.
	rejected := draftIn("deadline-refused", 0, 2)
	answer(t, ctx, store, "tenant-a", "deadline-refused", []channels.OutboxDraft{rejected}, now)
	for _, unrepresentable := range []time.Duration{
		0,
		-time.Second,
		channels.MinSendTimeout - 1,
		time.Second + time.Nanosecond,
	} {
		_, _, err := store.ClaimOutbox(ctx, scope("tenant-a"), channels.ClaimOutboxRequest{
			OutboxID:    rejected.OutboxID,
			SendToken:   "send-unrepresentable",
			SentBy:      "sender-main",
			SendTimeout: unrepresentable,
			Now:         sendAt,
		})
		require.ErrorIs(t, err, tenant.ErrInvalidArgument, "send timeout %s", unrepresentable)
	}
	untouched, err := store.GetOutboxPart(ctx, scope("tenant-a"), rejected.OutboxID)
	require.NoError(t, err)
	require.Equal(t, channels.OutboxPending, untouched.Status)
	require.Equal(t, int32(0), untouched.Attempt)
}
