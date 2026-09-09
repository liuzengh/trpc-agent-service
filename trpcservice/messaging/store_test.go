package messaging

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
)

func TestSubmitIsAtomicAndDetectsPayloadConflict(t *testing.T) {
	store, _ := newTestStore(t)
	task := testTask("task-1", "message-1")
	const concurrency = 50
	var wg sync.WaitGroup
	var created int
	var mu sync.Mutex
	for index := 0; index < concurrency; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, wasCreated, err := store.Submit(context.Background(), task)
			if err != nil {
				t.Errorf("Submit() error = %v", err)
				return
			}
			if wasCreated {
				mu.Lock()
				created++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if created != 1 {
		t.Fatalf("created = %d, want 1", created)
	}
	delivery, err := store.ReadTask(context.Background(), "worker-a", time.Millisecond)
	if err != nil || delivery.Task.TaskID != task.TaskID {
		t.Fatalf("ReadTask() = (%#v, %v)", delivery, err)
	}

	conflict := task
	conflict.TaskID = "task-other"
	conflict.Text = "different"
	conflict.PayloadDigest = conflict.CanonicalDigest()
	if _, _, err := store.Submit(context.Background(), conflict); err != ErrConflict {
		t.Fatalf("conflicting Submit() error = %v, want ErrConflict", err)
	}
}

func TestSubmitRejectsWrongKeyTypeWithoutPartialWrite(t *testing.T) {
	store, _ := newTestStore(t)
	task := testTask("task-wrongtype", "message-wrongtype")
	before, err := store.client.XLen(context.Background(), store.taskStream).Result()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.client.Set(context.Background(), store.inboxKey(task.InboxID()), "wrong-type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Submit(context.Background(), task); !errors.Is(err, ErrKeyType) {
		t.Fatalf("Submit() error = %v, want ErrKeyType", err)
	}
	after, err := store.client.XLen(context.Background(), store.taskStream).Result()
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("task stream length changed after rejected script: before=%d after=%d", before, after)
	}
}

func TestStrongReadyRejectsUnverifiableTopology(t *testing.T) {
	server := miniredis.RunT(t)
	cfg := config.MessagingConfig{
		RedisURL: "redis://" + server.Addr() + "/0", KeyPrefix: "strong-topology",
		LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
		InitialBackoff: time.Millisecond, MaxBackoff: time.Second, MaxAttempts: 3,
		InboxRetention: time.Hour, ReplyWaitTimeout: time.Second,
		SessionFencing: "strong", SessionLockDuration: time.Second,
	}
	store, err := NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Ready(context.Background()); err == nil {
		t.Fatal("Strong Ready unexpectedly accepted Redis without topology commands")
	}
	if server.Exists(store.taskStream) || server.Exists(store.replyStream) {
		t.Fatal("failed Strong Ready created Stream keys")
	}
}

func TestLeaseHeartbeatCompleteAndReply(t *testing.T) {
	store, _ := newTestStore(t)
	task := testTask("task-complete", "message-complete")
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
	if err := store.Heartbeat(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	reply := message.OutboundMessage{RequestID: task.RequestID, TraceID: task.TraceID, SessionID: task.SessionID, Text: "ok"}
	if err := store.Complete(context.Background(), lease, reply); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(context.Background(), task.InboxID())
	if err != nil || snapshot.State != StateSucceeded || snapshot.RequestID != task.RequestID || snapshot.RawPayload != "" || snapshot.Result == nil || snapshot.Result.Reply.Text != "ok" {
		t.Fatalf("terminal snapshot = (%#v, %v)", snapshot, err)
	}
	if snapshot.TenantID != task.TenantID || snapshot.AgentAppID != task.AgentAppID || snapshot.Channel != task.Channel || snapshot.BindingID != task.ChannelBindingID || snapshot.PlatformMessageID != task.PlatformMessageID || snapshot.ReceivedAt.IsZero() {
		t.Fatalf("terminal operational metadata = %#v", snapshot)
	}
	listed, err := store.ListSnapshots(context.Background(), 10)
	if err != nil || len(listed) != 1 || listed[0].TaskID != task.TaskID {
		t.Fatalf("ListSnapshots() = (%#v, %v)", listed, err)
	}
	replyDelivery, err := store.ReadReply(context.Background(), "gateway-a", time.Millisecond)
	if err != nil || !replyDelivery.Result.Succeeded {
		t.Fatalf("ReadReply() = (%#v, %v)", replyDelivery, err)
	}
	if err := store.AckReply(context.Background(), replyDelivery.StreamID); err != nil {
		t.Fatal(err)
	}
	if err := store.Complete(context.Background(), lease, reply); err != ErrLeaseLost {
		t.Fatalf("late Complete() error = %v, want ErrLeaseLost", err)
	}
}

func TestSessionHeartbeatDoesNotRenewTaskLease(t *testing.T) {
	store, server := newTestStore(t)
	store.config.SessionFencing = "strong"
	task := testTask("task-session-heartbeat", "message-session-heartbeat")
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
	before, err := store.client.HGet(context.Background(), lease.InboxKey, "lease_until").Result()
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now()
	server.SetTime(base.Add(100 * time.Millisecond))
	if err := store.SessionHeartbeat(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	after, err := store.client.HGet(context.Background(), lease.InboxKey, "lease_until").Result()
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("session heartbeat changed task lease: before=%s after=%s", before, after)
	}
	server.SetTime(base.Add(200 * time.Millisecond))
	pending, err := store.client.XPendingExt(context.Background(), &redis.XPendingExtArgs{Stream: store.taskStream, Group: workerGroup, Start: delivery.StreamID, End: delivery.StreamID, Count: 1}).Result()
	if err != nil || len(pending) != 1 || pending[0].Idle < 150*time.Millisecond {
		t.Fatalf("session heartbeat reset task Pending idle: (%#v,%v)", pending, err)
	}
	if exists, err := store.client.Exists(context.Background(), store.sessionLockKey(lease.SessionCoord)).Result(); err != nil || exists != 1 {
		t.Fatalf("session lock missing: exists=%d err=%v", exists, err)
	}
}

func TestTaskHeartbeatDoesNotRenewSessionLock(t *testing.T) {
	store, server := newTestStore(t)
	store.config.SessionFencing = "strong"
	task := testTask("task-heartbeat-isolation", "message-heartbeat-isolation")
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
	lockKey := store.sessionLockKey(lease.SessionCoord)
	before, err := store.client.HGet(context.Background(), lockKey, "expires_at_ms").Result()
	if err != nil {
		t.Fatal(err)
	}
	server.SetTime(time.Now().Add(100 * time.Millisecond))
	if err := store.Heartbeat(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	after, err := store.client.HGet(context.Background(), lockKey, "expires_at_ms").Result()
	if err != nil || after != before {
		t.Fatalf("task heartbeat changed Session lock expiry: before=%s after=%s err=%v", before, after, err)
	}
}

func TestSubmitAllocatesPerSessionSequence(t *testing.T) {
	store, _ := newTestStore(t)
	store.config.SessionFencing = "strong"
	first := testTask("task-seq-1", "message-seq-1")
	second := testTask("task-seq-2", "message-seq-2")
	second.SessionID = first.SessionID
	second.PayloadDigest = second.CanonicalDigest()
	firstSnapshot, _, err := store.Submit(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	secondSnapshot, _, err := store.Submit(context.Background(), second)
	if err != nil {
		t.Fatal(err)
	}
	if firstSnapshot.SessionCoord == "" || secondSnapshot.SessionCoord != firstSnapshot.SessionCoord {
		t.Fatalf("session coords differ: %#v %#v", firstSnapshot, secondSnapshot)
	}
	if firstSnapshot.SessionSeq != 1 || secondSnapshot.SessionSeq != 2 {
		t.Fatalf("session seqs = %d, %d", firstSnapshot.SessionSeq, secondSnapshot.SessionSeq)
	}
}

func TestLegacySubmitDoesNotCreateSessionCoordinationKeys(t *testing.T) {
	store, _ := newTestStore(t)
	task := testTask("legacy-submit", "legacy-submit-message")
	snapshot, created, err := store.Submit(context.Background(), task)
	if err != nil || !created {
		t.Fatalf("Submit()=(%#v,%v,%v)", snapshot, created, err)
	}
	if snapshot.SessionCoord != "" || snapshot.SessionSeq != 0 {
		t.Fatalf("legacy snapshot contains Session coordination: %#v", snapshot)
	}
	coord := sessionCoord(task)
	count, err := store.client.Exists(context.Background(), store.sessionSeqKey(coord), store.sessionStateKey(coord)).Result()
	if err != nil || count != 0 {
		t.Fatalf("legacy Session coordination keys=(%d,%v), want none", count, err)
	}
}

func TestBeginFailureRequeuesQueuedPendingEntry(t *testing.T) {
	store, _ := newTestStore(t)
	task := testTask("task-begin-failure", "message-begin-failure")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	delivery, err := store.ReadTask(context.Background(), "worker-a", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	wrongAttempt := delivery
	wrongAttempt.Task.Attempt = 2
	if _, err := store.Begin(context.Background(), wrongAttempt, "worker-a"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Begin() error = %v, want ErrLeaseLost", err)
	}
	if err := store.RequeueAfterBeginFailure(context.Background(), wrongAttempt); err != nil {
		t.Fatal(err)
	}
	if pending, err := store.client.XPending(context.Background(), store.taskStream, workerGroup).Result(); err != nil || pending.Count != 0 {
		t.Fatalf("pending after requeue = (%#v, %v)", pending, err)
	}
	requeued, err := store.ReadTask(context.Background(), "worker-a", time.Millisecond)
	if err != nil || requeued.Task.TaskID != task.TaskID || requeued.Task.Attempt != task.Attempt {
		t.Fatalf("requeued delivery = (%#v, %v)", requeued, err)
	}
}

func TestRetryPromotionAndAttemptLimit(t *testing.T) {
	store, server := newTestStore(t)
	task := testTask("task-retry", "message-retry")
	_, _, _ = store.Submit(context.Background(), task)
	delivery, _ := store.ReadTask(context.Background(), "worker-a", time.Millisecond)
	lease, _ := store.Begin(context.Background(), delivery, "worker-a")
	if err := store.Retry(context.Background(), lease, "agent_failed", false); err != nil {
		t.Fatal(err)
	}
	snapshot, _ := store.Snapshot(context.Background(), task.InboxID())
	if snapshot.State != StateRetryWait || snapshot.Attempt != 2 {
		t.Fatalf("retry snapshot = %#v", snapshot)
	}
	server.SetTime(time.Now().Add(2 * time.Second))
	if promoted, err := store.PromoteRetries(context.Background(), 10); err != nil || promoted != 1 {
		t.Fatalf("PromoteRetries() = (%d, %v)", promoted, err)
	}
	retryDelivery, err := store.ReadTask(context.Background(), "worker-a", time.Millisecond)
	if err != nil || retryDelivery.Task.Attempt != 2 {
		t.Fatalf("retry delivery = (%#v, %v)", retryDelivery, err)
	}
}

func TestStalePendingIsRequeuedWithNextAttempt(t *testing.T) {
	store, server := newTestStore(t)
	task := testTask("task-stale", "message-stale")
	_, _, _ = store.Submit(context.Background(), task)
	delivery, _ := store.ReadTask(context.Background(), "worker-old", time.Millisecond)
	if _, err := store.Begin(context.Background(), delivery, "worker-old"); err != nil {
		t.Fatal(err)
	}
	server.SetTime(time.Now().Add(2 * time.Second))
	claimed, err := store.ClaimStale(context.Background(), "worker-new", 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimStale() = (%#v, %v)", claimed, err)
	}
	if err := store.Recover(context.Background(), claimed[0]); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.ReadTask(context.Background(), "worker-new", time.Millisecond)
	if err != nil || recovered.Task.Attempt != 2 {
		t.Fatalf("recovered delivery = (%#v, %v)", recovered, err)
	}
}

func TestQueuedPendingIsRecoveredAfterCrashBeforeBegin(t *testing.T) {
	store, server := newTestStore(t)
	task := testTask("task-queued-crash", "message-queued-crash")
	_, _, _ = store.Submit(context.Background(), task)
	if _, err := store.ReadTask(context.Background(), "worker-old", time.Millisecond); err != nil {
		t.Fatal(err)
	}
	server.SetTime(time.Now().Add(2 * time.Second))
	claimed, err := store.ClaimStale(context.Background(), "worker-new", 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimStale() = (%#v, %v)", claimed, err)
	}
	if err := store.Recover(context.Background(), claimed[0]); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.ReadTask(context.Background(), "worker-new", time.Millisecond)
	if err != nil || recovered.Task.Attempt != 2 {
		t.Fatalf("recovered delivery = (%#v, %v)", recovered, err)
	}
}

func TestRejectTamperedDigestStoresTerminalFailure(t *testing.T) {
	store, _ := newTestStore(t)
	task := testTask("task-tampered", "message-tampered")
	_, _, _ = store.Submit(context.Background(), task)
	delivery, err := store.ReadTask(context.Background(), "worker-a", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	delivery.Task.PayloadDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	if err := store.Reject(context.Background(), delivery, "invalid_task"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(context.Background(), task.InboxID())
	if err != nil || snapshot.State != StateFailedTerminal || snapshot.ErrorCode != "invalid_task" {
		t.Fatalf("terminal snapshot = (%#v, %v)", snapshot, err)
	}
	reply, err := store.ReadReply(context.Background(), "gateway-invalid", time.Millisecond)
	if err != nil || reply.Result.Target.Valid() {
		t.Fatalf("invalid task reply target = (%#v, %v)", reply.Result.Target, err)
	}
}

func newTestStore(t *testing.T) (*Store, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	server.SetTime(time.Now())
	cfg := config.MessagingConfig{
		RedisURL: "redis://" + server.Addr() + "/0", KeyPrefix: "phase3-test-" + fmt.Sprint(time.Now().UnixNano()),
		LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
		InitialBackoff: time.Second, MaxBackoff: 5 * time.Second, MaxAttempts: 3,
		InboxRetention: time.Hour, ReplyWaitTimeout: time.Second,
	}
	store, err := NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	return store, server
}

func testTask(taskID, messageID string) message.ExecutionTask {
	task := message.ExecutionTask{
		SchemaVersion: message.TaskSchemaVersion, TaskID: taskID, Channel: "demo",
		ChannelBindingID: "binding-a", ExternalAccountID: "demo-account", TenantID: "tenant-a", AgentAppID: "assistant", ConfigVersion: "v1",
		RunnerUserID: "u_user", SessionID: "s_session", PlatformMessageID: messageID,
		ActorUserID: "actor-a", ConversationID: "conversation-a", ConversationType: message.ConversationDirect,
		Text: "hello", RequestID: "request", TraceID: "trace", ReceivedAt: time.Now().UTC(), Attempt: 1,
	}
	task.PayloadDigest = task.CanonicalDigest()
	return task
}
