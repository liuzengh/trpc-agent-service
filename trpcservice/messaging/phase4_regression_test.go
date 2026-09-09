package messaging

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestStrongCompleteTurnPersistsMultipleRounds(t *testing.T) {
	store, server := newTestStore(t)
	store.config.SessionFencing = "strong"
	store.config.SessionLockDuration = 5 * time.Second
	store.config.MaxTurnEvents = 8
	svc, err := sessionfence.New("redis://"+server.Addr()+"/0", store.config.KeyPrefix, store.config.KeyPrefix)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	ctx := context.Background()
	firstTask := testTask("phase4-cross-component-1", "phase4-cross-message-1")
	key := session.Key{AppName: tenant.AppName(firstTask.TenantID, firstTask.AgentAppID), UserID: firstTask.RunnerUserID, SessionID: firstTask.SessionID}

	completeRound := func(task message.ExecutionTask, worker string, state session.StateMap, eventID string) (*session.Session, string) {
		t.Helper()
		task.TraceParent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
		task.DigestVersion = 2
		task.PayloadDigest = task.CanonicalDigest()
		if _, _, err := store.Submit(ctx, task); err != nil {
			t.Fatal(err)
		}
		delivery, err := store.ReadTask(ctx, worker, time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		lease, err := store.Begin(ctx, delivery, worker)
		if err != nil {
			t.Fatal(err)
		}
		commit := prepareTestTurn(t, svc, lease, task, state, eventID)
		if err := store.CompleteTurn(ctx, lease, testReply(task), commit); err != nil {
			t.Fatal(err)
		}
		replyDelivery, err := store.ReadReply(ctx, "gateway-trace", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if replyDelivery.Result.TraceParent != task.TraceParent || replyDelivery.Result.DigestVersion != task.DigestVersion {
			t.Fatalf("reply trace metadata=(%q,%d), want (%q,%d)", replyDelivery.Result.TraceParent, replyDelivery.Result.DigestVersion, task.TraceParent, task.DigestVersion)
		}
		read, err := svc.GetSession(ctx, key)
		if err != nil || read == nil {
			t.Fatalf("fenced read=(%#v,%v)", read, err)
		}
		return read, lease.SessionCoord
	}

	first, firstCoord := completeRound(firstTask, "worker-a", session.StateMap{"round_1": []byte("one")}, "event-1")
	if string(first.State["round_1"]) != "one" || len(first.Events) != 1 || first.Events[0].ID != "event-1" {
		t.Fatalf("first round session=%#v", first)
	}
	server.SetTime(first.UpdatedAt.Add(time.Second))
	secondTask := testTask("phase4-cross-component-2", "phase4-cross-message-2")
	secondState := first.SnapshotState()
	secondState["round_2"] = []byte("two")
	second, secondCoord := completeRound(secondTask, "worker-b", secondState, "event-2")
	if firstCoord != secondCoord {
		t.Fatalf("session coord changed between rounds: %q != %q", firstCoord, secondCoord)
	}
	if string(second.State["round_1"]) != "one" || string(second.State["round_2"]) != "two" {
		t.Fatalf("second round state=%#v", second.State)
	}
	if len(second.Events) != 2 || second.Events[0].ID != "event-1" || second.Events[1].ID != "event-2" {
		t.Fatalf("second round events=%#v", second.Events)
	}
	if !second.CreatedAt.Equal(first.CreatedAt) || !second.UpdatedAt.After(first.UpdatedAt) {
		t.Fatalf("session timestamps first=(%v,%v) second=(%v,%v)", first.CreatedAt, first.UpdatedAt, second.CreatedAt, second.UpdatedAt)
	}
	if cursor := store.client.HGet(ctx, store.sessionStateKey(secondCoord), "last_completed_seq").Val(); cursor != "2" {
		t.Fatalf("last_completed_seq=%q, want 2", cursor)
	}
	listed, err := svc.ListSessions(ctx, session.UserKey{AppName: key.AppName, UserID: key.UserID})
	if err != nil || len(listed) != 1 || listed[0].ID != key.SessionID {
		t.Fatalf("fenced list=(%#v,%v)", listed, err)
	}
	if indexes := store.client.HLen(ctx, store.fencedIndexKey(sessionfence.UserCoord(session.UserKey{AppName: key.AppName, UserID: key.UserID}))).Val(); indexes != 1 {
		t.Fatalf("session index entries=%d, want 1", indexes)
	}
	if replies := store.client.XLen(ctx, store.replyStream).Val(); replies != 2 {
		t.Fatalf("reply stream entries=%d, want 2", replies)
	}
}

func TestStrongCompleteTurnRejectsLateFence(t *testing.T) {
	store, server := newTestStore(t)
	store.config.SessionFencing = "strong"
	store.config.SessionLockDuration = 5 * time.Second
	svc, err := sessionfence.New("redis://"+server.Addr()+"/0", store.config.KeyPrefix, store.config.KeyPrefix)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	ctx := context.Background()
	task := testTask("strong-late-commit", "strong-late-commit-message")
	if _, _, err := store.Submit(ctx, task); err != nil {
		t.Fatal(err)
	}
	delivery, err := store.ReadTask(ctx, "worker-old", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Begin(ctx, delivery, "worker-old")
	if err != nil {
		t.Fatal(err)
	}
	commit := prepareTestTurn(t, svc, lease, task, session.StateMap{"late": []byte("value")}, "late-event")
	if err := store.client.HSet(ctx, store.sessionLockKey(lease.SessionCoord), "token", "replacement-token").Err(); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteTurn(ctx, lease, testReply(task), commit); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("late CompleteTurn error=%v, want ErrLeaseLost", err)
	}
	snapshot, err := store.Snapshot(ctx, task.InboxID())
	if err != nil || snapshot.State != StateProcessing || snapshot.Owner != "worker-old" {
		t.Fatalf("late CompleteTurn changed Inbox=(%#v,%v)", snapshot, err)
	}
	if store.client.Exists(ctx, store.fencedMetaKey(lease.SessionCoord), store.fencedEventsKey(lease.SessionCoord)).Val() != 0 {
		t.Fatal("late CompleteTurn wrote Session data")
	}
	if replies := store.client.XLen(ctx, store.replyStream).Val(); replies != 0 {
		t.Fatalf("late CompleteTurn wrote %d replies", replies)
	}
}

func TestStrongCompleteTurnWrongIndexTypeHasNoPartialWrite(t *testing.T) {
	store, server := newTestStore(t)
	store.config.SessionFencing = "strong"
	store.config.SessionLockDuration = 5 * time.Second
	svc, err := sessionfence.New("redis://"+server.Addr()+"/0", store.config.KeyPrefix, store.config.KeyPrefix)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	ctx := context.Background()
	task := testTask("strong-wrong-index", "strong-wrong-index-message")
	if _, _, err := store.Submit(ctx, task); err != nil {
		t.Fatal(err)
	}
	delivery, err := store.ReadTask(ctx, "worker-a", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Begin(ctx, delivery, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	commit := prepareTestTurn(t, svc, lease, task, session.StateMap{"answer": []byte("blocked")}, "blocked-event")
	if err := store.client.Set(ctx, store.fencedIndexKey(commit.UserCoord), "wrong-type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteTurn(ctx, lease, testReply(task), commit); !errors.Is(err, ErrKeyType) {
		t.Fatalf("CompleteTurn wrong index error=%v, want ErrKeyType", err)
	}
	if store.client.Exists(ctx, store.fencedMetaKey(lease.SessionCoord), store.fencedEventsKey(lease.SessionCoord)).Val() != 0 {
		t.Fatal("rejected CompleteTurn left partial Session data")
	}
	snapshot, err := store.Snapshot(ctx, task.InboxID())
	if err != nil || snapshot.State != StateProcessing || snapshot.Owner != "worker-a" {
		t.Fatalf("rejected CompleteTurn changed Inbox=(%#v,%v)", snapshot, err)
	}
	if cursor := store.client.HGet(ctx, store.sessionStateKey(lease.SessionCoord), "last_completed_seq").Val(); cursor != "0" {
		t.Fatalf("rejected CompleteTurn advanced cursor to %q", cursor)
	}
	if replies := store.client.XLen(ctx, store.replyStream).Val(); replies != 0 {
		t.Fatalf("rejected CompleteTurn wrote %d replies", replies)
	}
}

func TestStrongStoreRejectsLegacyComplete(t *testing.T) {
	store, _ := newTestStore(t)
	store.config.SessionFencing = "strong"
	task := testTask("strong-no-legacy-complete", "strong-no-legacy-complete-message")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	delivery, err := store.ReadTask(context.Background(), "worker-a", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Begin(context.Background(), delivery, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(context.Background(), lease, testReply(task)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Strong Complete error=%v, want ErrLeaseLost", err)
	}
	snapshot, err := store.Snapshot(context.Background(), task.InboxID())
	if err != nil || snapshot.State != StateProcessing {
		t.Fatalf("Strong Complete changed Inbox=(%#v,%v)", snapshot, err)
	}
}

func TestStrongRetryAndQueuedRecoverDoNotAdvanceCursor(t *testing.T) {
	t.Run("retry", func(t *testing.T) {
		store, _ := newTestStore(t)
		store.config.SessionFencing = "strong"
		task := testTask("strong-retry-cursor", "strong-retry-message")
		if _, _, err := store.Submit(context.Background(), task); err != nil {
			t.Fatal(err)
		}
		delivery, err := store.ReadTask(context.Background(), "worker-a", time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		lease, err := store.Begin(context.Background(), delivery, "worker-a")
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Retry(context.Background(), lease, "agent_failed", true); err != nil {
			t.Fatal(err)
		}
		if got := store.client.HGet(context.Background(), store.sessionStateKey(lease.SessionCoord), "last_completed_seq").Val(); got != "0" {
			t.Fatalf("Retry advanced cursor to %q", got)
		}
	})

	t.Run("queued recover", func(t *testing.T) {
		store, _ := newTestStore(t)
		store.config.SessionFencing = "strong"
		task := testTask("strong-queued-recover", "strong-queued-recover-message")
		if _, _, err := store.Submit(context.Background(), task); err != nil {
			t.Fatal(err)
		}
		delivery, err := store.ReadTask(context.Background(), "worker-old", time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Recover(context.Background(), delivery, "worker-new"); err != nil {
			t.Fatal(err)
		}
		snapshot, err := store.Snapshot(context.Background(), task.InboxID())
		if err != nil || snapshot.Attempt != task.Attempt || snapshot.SessionSeq != 1 {
			t.Fatalf("queued Recover snapshot=(%#v,%v)", snapshot, err)
		}
		if got := store.client.HGet(context.Background(), store.sessionStateKey(snapshot.SessionCoord), "last_completed_seq").Val(); got != "0" {
			t.Fatalf("queued Recover advanced cursor to %q", got)
		}
	})
}

func TestStrongRejectAndWorkerLostAdvanceCursor(t *testing.T) {
	t.Run("reject", func(t *testing.T) {
		store, _ := newTestStore(t)
		store.config.SessionFencing = "strong"
		task := testTask("strong-reject-cursor", "strong-reject-message")
		if _, _, err := store.Submit(context.Background(), task); err != nil {
			t.Fatal(err)
		}
		delivery, err := store.ReadTask(context.Background(), "worker-a", time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Reject(context.Background(), delivery, "invalid_task"); err != nil {
			t.Fatal(err)
		}
		snapshot, err := store.Snapshot(context.Background(), task.InboxID())
		if err != nil || snapshot.State != StateFailedTerminal {
			t.Fatalf("Reject snapshot=(%#v,%v)", snapshot, err)
		}
		if got := store.client.HGet(context.Background(), store.sessionStateKey(snapshot.SessionCoord), "last_completed_seq").Val(); got != "1" {
			t.Fatalf("Reject cursor=%q, want 1", got)
		}
	})

	t.Run("worker lost", func(t *testing.T) {
		store, server := newTestStore(t)
		store.config.SessionFencing = "strong"
		store.config.MaxAttempts = 1
		task := testTask("strong-worker-lost", "strong-worker-lost-message")
		if _, _, err := store.Submit(context.Background(), task); err != nil {
			t.Fatal(err)
		}
		delivery, err := store.ReadTask(context.Background(), "worker-old", time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		lease, err := store.Begin(context.Background(), delivery, "worker-old")
		if err != nil {
			t.Fatal(err)
		}
		server.SetTime(time.Now().Add(2 * time.Second))
		if err := store.Recover(context.Background(), delivery, "worker-new"); err != nil {
			t.Fatal(err)
		}
		snapshot, err := store.Snapshot(context.Background(), task.InboxID())
		if err != nil || snapshot.State != StateFailedTerminal || snapshot.ErrorCode != "worker_lost" {
			t.Fatalf("worker_lost snapshot=(%#v,%v)", snapshot, err)
		}
		if got := store.client.HGet(context.Background(), store.sessionStateKey(lease.SessionCoord), "last_completed_seq").Val(); got != "1" {
			t.Fatalf("worker_lost cursor=%q, want 1", got)
		}
	})
}

func TestConcurrentStrongRecoverTransfersPendingOnce(t *testing.T) {
	store, server := newTestStore(t)
	store.config.SessionFencing = "strong"
	task := testTask("strong-recover-race", "strong-recover-race-message")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	delivery, err := store.ReadTask(context.Background(), "worker-old", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Begin(context.Background(), delivery, "worker-old"); err != nil {
		t.Fatal(err)
	}
	server.SetTime(time.Now().Add(2 * time.Second))
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, consumer := range []string{"worker-new-a", "worker-new-b"} {
		consumer := consumer
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- store.Recover(context.Background(), delivery, consumer)
		}()
	}
	wg.Wait()
	close(results)
	succeeded, fenced := 0, 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrLeaseLost):
			fenced++
		default:
			t.Fatalf("Recover race error=%v", err)
		}
	}
	if succeeded != 1 || fenced != 1 {
		t.Fatalf("Recover race results success=%d fenced=%d", succeeded, fenced)
	}
	snapshot, err := store.Snapshot(context.Background(), task.InboxID())
	if err != nil || snapshot.State != StateQueued || snapshot.Attempt != task.Attempt+1 {
		t.Fatalf("Recover race snapshot=(%#v,%v)", snapshot, err)
	}
	if length := store.client.XLen(context.Background(), store.taskStream).Val(); length != 1 {
		t.Fatalf("task stream length=%d, want one requeued entry", length)
	}
}

func TestStrongRecoverWaitsForActiveSessionLock(t *testing.T) {
	store, server := newTestStore(t)
	store.config.SessionFencing = "strong"
	store.config.LeaseDuration = time.Second
	store.config.SessionLockDuration = 3 * time.Second
	ctx := context.Background()
	startedAt, err := store.redisTime(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task := testTask("strong-active-lock", "strong-active-lock-message")
	if _, _, err := store.Submit(ctx, task); err != nil {
		t.Fatal(err)
	}
	delivery, err := store.ReadTask(ctx, "worker-old", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Begin(ctx, delivery, "worker-old")
	if err != nil {
		t.Fatal(err)
	}
	lockBefore, err := store.client.HGetAll(ctx, store.sessionLockKey(lease.SessionCoord)).Result()
	if err != nil {
		t.Fatal(err)
	}

	server.SetTime(startedAt.Add(2 * time.Second))
	stale, err := store.ClaimStale(ctx, "worker-new", 10)
	if err != nil || len(stale) != 1 {
		t.Fatalf("ClaimStale with active Session lock=(%#v,%v)", stale, err)
	}
	if err := store.Recover(ctx, stale[0], "worker-new"); !errors.Is(err, ErrSessionLockActive) {
		t.Fatalf("Recover with active Session lock error=%v, want ErrSessionLockActive", err)
	}
	pending, err := store.client.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: store.taskStream, Group: workerGroup, Start: delivery.StreamID, End: delivery.StreamID, Count: 1}).Result()
	if err != nil || len(pending) != 1 || pending[0].Consumer != "worker-old" {
		t.Fatalf("Pending owner after refused Recover=(%#v,%v)", pending, err)
	}
	snapshot, err := store.Snapshot(ctx, task.InboxID())
	if err != nil || snapshot.State != StateProcessing || snapshot.Owner != "worker-old" || snapshot.LeaseEpoch != lease.Epoch {
		t.Fatalf("Inbox after refused Recover=(%#v,%v)", snapshot, err)
	}
	lockAfter, err := store.client.HGetAll(ctx, store.sessionLockKey(lease.SessionCoord)).Result()
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"token", "task_id", "owner", "lease_epoch", "expires_at_ms"} {
		if lockAfter[field] != lockBefore[field] {
			t.Fatalf("Session lock field %s changed: before=%q after=%q", field, lockBefore[field], lockAfter[field])
		}
	}
	if replies := store.client.XLen(ctx, store.replyStream).Val(); replies != 0 {
		t.Fatalf("refused Recover wrote %d replies", replies)
	}

	server.SetTime(startedAt.Add(4 * time.Second))
	stale, err = store.ClaimStale(ctx, "worker-new", 10)
	if err != nil || len(stale) != 1 {
		t.Fatalf("ClaimStale after Session lock expiry=(%#v,%v)", stale, err)
	}
	if err := store.Recover(ctx, stale[0], "worker-new"); err != nil {
		t.Fatal(err)
	}
	snapshot, err = store.Snapshot(ctx, task.InboxID())
	if err != nil || snapshot.State != StateQueued || snapshot.Attempt != task.Attempt+1 {
		t.Fatalf("Inbox after permitted Recover=(%#v,%v)", snapshot, err)
	}
	requeued, err := store.ReadTask(ctx, "worker-new", time.Millisecond)
	if err != nil || requeued.Task.TaskID != task.TaskID || requeued.Task.Attempt != task.Attempt+1 {
		t.Fatalf("requeued task=(%#v,%v)", requeued, err)
	}
}

func TestStrongStaleRejectOnlyCleansPendingWithoutAdvancingCursor(t *testing.T) {
	store, _ := newTestStore(t)
	store.config.SessionFencing = "strong"
	task := testTask("strong-stale-reject", "strong-stale-reject-message")
	snapshot, _, err := store.Submit(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := store.ReadTask(context.Background(), "worker-a", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.client.HSet(context.Background(), store.sessionStateKey(snapshot.SessionCoord), "last_completed_seq", snapshot.SessionSeq).Err(); err != nil {
		t.Fatal(err)
	}
	if err := store.Reject(context.Background(), delivery, "session_stale"); err != nil {
		t.Fatal(err)
	}
	after, err := store.Snapshot(context.Background(), task.InboxID())
	if err != nil || after.State != StateQueued || after.ErrorCode != "" {
		t.Fatalf("stale Reject snapshot=(%#v,%v)", after, err)
	}
	if got := store.client.HGet(context.Background(), store.sessionStateKey(snapshot.SessionCoord), "last_completed_seq").Val(); got != "1" {
		t.Fatalf("stale Reject cursor=%q, want unchanged 1", got)
	}
	if pending := store.client.XPending(context.Background(), store.taskStream, workerGroup).Val(); pending.Count != 0 {
		t.Fatalf("stale Reject left %d Pending entries", pending.Count)
	}
	if replies := store.client.XLen(context.Background(), store.replyStream).Val(); replies != 0 {
		t.Fatalf("stale Reject wrote %d duplicate replies", replies)
	}
}

func TestStrongWorkerLostWaitsForPredecessorWithoutClaiming(t *testing.T) {
	store, server := newTestStore(t)
	store.config.SessionFencing = "strong"
	store.config.MaxAttempts = 1
	first := testTask("recover-predecessor", "recover-predecessor-message")
	second := testTask("recover-future", "recover-future-message")
	second.SessionID = first.SessionID
	second.PayloadDigest = second.CanonicalDigest()
	if _, _, err := store.Submit(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	secondSnapshot, _, err := store.Submit(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ReadTask(context.Background(), "worker-old", time.Millisecond); err != nil {
		t.Fatal(err)
	}
	delivery, err := store.ReadTask(context.Background(), "worker-old", time.Millisecond)
	if err != nil || delivery.Task.TaskID != second.TaskID {
		t.Fatalf("second delivery=(%#v,%v)", delivery, err)
	}
	inboxKey := store.inboxKey(delivery.InboxID)
	if err := store.client.HSet(context.Background(), inboxKey, "state", StateProcessing, "owner", "worker-old", "lease_epoch", 1, "lease_until", 1).Err(); err != nil {
		t.Fatal(err)
	}
	if err := store.client.HSet(context.Background(), store.sessionLockKey(secondSnapshot.SessionCoord), "token", "old-token", "task_id", second.TaskID, "owner", "worker-old", "lease_epoch", 1, "expires_at_ms", 1).Err(); err != nil {
		t.Fatal(err)
	}
	server.SetTime(time.Now().Add(2 * time.Second))
	if err := store.Recover(context.Background(), delivery, "worker-new"); !errors.Is(err, ErrSessionWait) {
		t.Fatalf("future worker_lost Recover error=%v", err)
	}
	pending, err := store.client.XPendingExt(context.Background(), &redis.XPendingExtArgs{Stream: store.taskStream, Group: workerGroup, Start: delivery.StreamID, End: delivery.StreamID, Count: 1}).Result()
	if err != nil || len(pending) != 1 || pending[0].Consumer != "worker-old" {
		t.Fatalf("future Pending owner=(%#v,%v)", pending, err)
	}
	after, err := store.Snapshot(context.Background(), second.InboxID())
	if err != nil || after.State != StateProcessing || after.SessionSeq != 2 {
		t.Fatalf("future Recover snapshot=(%#v,%v)", after, err)
	}
}

func TestStrongFailAdvancesSessionCursorAndReleaseIsFenced(t *testing.T) {
	store, _ := newTestStore(t)
	store.config.SessionFencing = "strong"
	task := testTask("phase4-fail-cursor", "phase4-fail-message")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	delivery, err := store.ReadTask(context.Background(), "worker-a", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Begin(context.Background(), delivery, "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Fail(context.Background(), lease, "session_turn_too_large"); err != nil {
		t.Fatal(err)
	}
	state, err := store.client.HGet(context.Background(), store.sessionStateKey(lease.SessionCoord), "last_completed_seq").Result()
	if err != nil || state != "1" {
		t.Fatalf("last_completed_seq=(%q,%v)", state, err)
	}
	if err := store.ReleaseSessionLock(context.Background(), lease); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("late release error=%v, want ErrLeaseLost", err)
	}
}

func TestLegacyTerminalFailurePathRemainsCompatible(t *testing.T) {
	store, _ := newTestStore(t)
	task := testTask("legacy-fail", "legacy-fail-message")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	delivery, err := store.ReadTask(context.Background(), "legacy-worker", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Begin(context.Background(), delivery, "legacy-worker")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Fail(context.Background(), lease, "agent_failed"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(context.Background(), task.InboxID())
	if err != nil || snapshot.State != StateFailedTerminal || snapshot.ErrorCode != "agent_failed" {
		t.Fatalf("legacy snapshot=(%#v,%v)", snapshot, err)
	}
}

func prepareTestTurn(t *testing.T, svc *sessionfence.Service, lease Lease, task message.ExecutionTask, state session.StateMap, eventIDs ...string) sessionfence.TurnCommit {
	t.Helper()
	key := session.Key{AppName: tenant.AppName(task.TenantID, task.AgentAppID), UserID: task.RunnerUserID, SessionID: task.SessionID}
	turn := svc.StartTurn(key, sessionfence.Fence{
		TaskID:       task.TaskID,
		SessionCoord: lease.SessionCoord,
		UserCoord:    sessionfence.UserCoord(session.UserKey{AppName: key.AppName, UserID: key.UserID}),
		SessionSeq:   lease.SessionSeq,
		LeaseEpoch:   lease.Epoch,
		LockToken:    lease.LockToken,
	})
	turnCtx := sessionfence.WithTurn(context.Background(), turn)
	staged := session.NewSession(key.AppName, key.UserID, key.SessionID, session.WithSessionState(state))
	for index, eventID := range eventIDs {
		current := &event.Event{
			Response:     &model.Response{Choices: []model.Choice{{Message: model.NewUserMessage(eventID)}}},
			ID:           eventID,
			InvocationID: fmt.Sprintf("invocation-%d", index),
			Author:       "user",
			Timestamp:    time.Unix(int64(index+1), 0).UTC(),
		}
		if err := svc.AppendEvent(turnCtx, staged, current); err != nil {
			t.Fatal(err)
		}
	}
	if err := svc.UpdateSessionState(turnCtx, key, state); err != nil {
		t.Fatal(err)
	}
	commit, err := svc.Prepare(turn)
	if err != nil {
		t.Fatal(err)
	}
	return commit
}

func TestRedis7Phase4StrongSmoke(t *testing.T) {
	rawURL := os.Getenv("PHASE4_REDIS_SMOKE_URL")
	if rawURL == "" {
		t.Skip("PHASE4_REDIS_SMOKE_URL is not set")
	}
	redisURL, err := config.NormalizeRedisURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("phase4-smoke:%d", time.Now().UnixNano())
	cfg := config.MessagingConfig{
		RedisURL: redisURL, KeyPrefix: prefix,
		LeaseDuration: 300 * time.Millisecond, HeartbeatInterval: 50 * time.Millisecond,
		InitialBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, MaxAttempts: 3,
		InboxRetention: time.Minute, ReplyWaitTimeout: time.Second,
		SessionFencing: "strong", SessionLockDuration: 700 * time.Millisecond,
		SessionWaitBackoff: 10 * time.Millisecond, SessionWaitMaxBackoff: 50 * time.Millisecond,
		MaxTurnEvents: 32, MaxTurnBytes: 32 << 10, ShutdownTimeout: time.Second,
	}
	first, err := NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewStore(cfg)
	if err != nil {
		first.Close()
		t.Fatal(err)
	}
	cleanup := func() {
		var cursor uint64
		for {
			keys, next, scanErr := first.client.Scan(context.Background(), cursor, first.basePrefix+":*", 100).Result()
			if scanErr == nil && len(keys) > 0 {
				_ = first.client.Del(context.Background(), keys...).Err()
			}
			if scanErr != nil || next == 0 {
				break
			}
			cursor = next
		}
		_ = second.Close()
		_ = first.Close()
	}
	t.Cleanup(cleanup)
	ctx := context.Background()
	if err := first.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if err := second.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.client.Do(ctx, "CLIENT", "PAUSE", 250, "ALL").Err(); err != nil {
		t.Fatal(err)
	}
	unavailableCtx, unavailableCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	if err := first.Ready(unavailableCtx); err == nil {
		unavailableCancel()
		t.Fatal("Strong readiness stayed ready while Redis was paused")
	}
	unavailableCancel()
	time.Sleep(300 * time.Millisecond)
	if err := first.Ready(ctx); err != nil {
		t.Fatalf("Strong readiness did not recover after Redis resumed: %v", err)
	}

	task := testTask("redis7-strong", "redis7-strong-message")
	if _, _, err := first.Submit(ctx, task); err != nil {
		t.Fatal(err)
	}
	delivery, err := first.ReadTask(ctx, "redis7-worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := first.Begin(ctx, delivery, "redis7-worker-a")
	if err != nil {
		t.Fatal(err)
	}
	commit := sessionfence.TurnCommit{SessionCoord: lease.SessionCoord, SessionSeq: lease.SessionSeq, AppName: tenant.AppName(task.TenantID, task.AgentAppID), UserID: task.RunnerUserID, SessionID: task.SessionID, FinalState: session.StateMap{"redis7": []byte("visible")}}
	if err := first.CompleteTurn(ctx, lease, testReply(task), commit); err != nil {
		t.Fatal(err)
	}
	svc, err := sessionfence.New(redisURL, prefix, prefix)
	if err != nil {
		t.Fatal(err)
	}
	defer svc.Close()
	read, err := svc.GetSession(ctx, session.Key{AppName: commit.AppName, UserID: commit.UserID, SessionID: commit.SessionID})
	if err != nil || read == nil || string(read.State["redis7"]) != "visible" {
		t.Fatalf("real Redis fenced read=(%#v,%v)", read, err)
	}

	failTask := testTask("redis7-fail", "redis7-fail-message")
	failTask.SessionID = "redis7-fail-session"
	failTask.PayloadDigest = failTask.CanonicalDigest()
	if _, _, err := first.Submit(ctx, failTask); err != nil {
		t.Fatal(err)
	}
	failDelivery, err := first.ReadTask(ctx, "redis7-worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	failLease, err := first.Begin(ctx, failDelivery, "redis7-worker-a")
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Fail(ctx, failLease, "agent_failed"); err != nil {
		t.Fatal(err)
	}
	if got := first.client.HGet(ctx, first.sessionStateKey(failLease.SessionCoord), "last_completed_seq").Val(); got != "1" {
		t.Fatalf("real Redis Fail cursor=%q, want 1", got)
	}

	rejectTask := testTask("redis7-reject", "redis7-reject-message")
	rejectTask.SessionID = "redis7-reject-session"
	rejectTask.PayloadDigest = rejectTask.CanonicalDigest()
	if _, _, err := first.Submit(ctx, rejectTask); err != nil {
		t.Fatal(err)
	}
	rejectDelivery, err := first.ReadTask(ctx, "redis7-worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Reject(ctx, rejectDelivery, "invalid_task"); err != nil {
		t.Fatal(err)
	}
	rejectSnapshot, err := first.Snapshot(ctx, rejectTask.InboxID())
	if err != nil || rejectSnapshot.State != StateFailedTerminal {
		t.Fatalf("real Redis Reject snapshot=(%#v,%v)", rejectSnapshot, err)
	}
	if got := first.client.HGet(ctx, first.sessionStateKey(rejectSnapshot.SessionCoord), "last_completed_seq").Val(); got != "1" {
		t.Fatalf("real Redis Reject cursor=%q, want 1", got)
	}

	recoverTask := testTask("redis7-recover", "redis7-recover-message")
	recoverTask.SessionID = "redis7-recover-session"
	recoverTask.PayloadDigest = recoverTask.CanonicalDigest()
	if _, _, err := first.Submit(ctx, recoverTask); err != nil {
		t.Fatal(err)
	}
	recoverDelivery, err := first.ReadTask(ctx, "redis7-worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	oldLease, err := first.Begin(ctx, recoverDelivery, "redis7-worker-a")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(350 * time.Millisecond)
	stale, err := second.ClaimStale(ctx, "redis7-worker-b", 4)
	if err != nil || len(stale) != 1 {
		t.Fatalf("stale while lock active=(%#v,%v)", stale, err)
	}
	if err := second.Recover(ctx, stale[0], "redis7-worker-b"); !errors.Is(err, ErrSessionLockActive) {
		t.Fatalf("Recover with active lock=%v", err)
	}
	pending, err := second.client.XPendingExt(ctx, &redis.XPendingExtArgs{Stream: second.taskStream, Group: workerGroup, Start: "-", End: "+", Count: 10}).Result()
	if err != nil || len(pending) != 1 || pending[0].Consumer != "redis7-worker-a" {
		t.Fatalf("Pending owner after refused recovery=(%#v,%v)", pending, err)
	}
	time.Sleep(450 * time.Millisecond)
	stale, err = second.ClaimStale(ctx, "redis7-worker-b", 4)
	if err != nil || len(stale) != 1 {
		t.Fatalf("stale after lock expiry=(%#v,%v)", stale, err)
	}
	if err := second.Recover(ctx, stale[0], "redis7-worker-b"); err != nil {
		t.Fatal(err)
	}
	if err := first.ReleaseSessionLock(ctx, oldLease); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("late release=%v, want ErrLeaseLost", err)
	}
	requeued, err := second.ReadTask(ctx, "redis7-worker-b", time.Second)
	if err != nil || requeued.Task.Attempt != recoverTask.Attempt+1 {
		t.Fatalf("requeued recovery=(%#v,%v)", requeued, err)
	}
}

func testReply(task message.ExecutionTask) message.OutboundMessage {
	return message.OutboundMessage{Channel: task.Channel, BindingID: task.ChannelBindingID, RequestID: task.RequestID, TraceID: task.TraceID, SessionID: task.SessionID, Text: "ok"}
}
