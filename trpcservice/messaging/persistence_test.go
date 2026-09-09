package messaging

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func preparedPersistence(t *testing.T, taskID string) (*Store, interface{ SetTime(time.Time) }, message.ExecutionTask, Lease, persistence.Envelope) {
	t.Helper()
	store, server := newTestStore(t)
	store.config.SessionFencing = "strong"
	store.config.SessionLockDuration = time.Second
	task := testTask(taskID, taskID+"-message")
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
	route := persistence.Route{TenantID: task.TenantID, AgentAppID: task.AgentAppID, Fingerprint: persistence.BackendFingerprint{SchemaVersion: persistence.FingerprintSchemaVersion, Kind: tenant.StorageKindPostgres, StorageProfileID: "pg", DatabaseIdentity: "db.example:5432/app", Namespace: "public.tenant_"}}
	commit := sessionfence.TurnCommit{SessionCoord: lease.SessionCoord, SessionSeq: lease.SessionSeq, AppName: tenant.AppName(task.TenantID, task.AgentAppID), UserID: task.RunnerUserID, SessionID: task.SessionID, FinalState: map[string][]byte{}}
	reply := message.OutboundMessage{RequestID: task.RequestID, TraceID: task.TraceID, SessionID: task.SessionID, Text: "answer"}
	envelope, err := store.PreparePersistence(context.Background(), lease, route, reply, commit)
	if err != nil {
		t.Fatal(err)
	}
	return store, server, task, lease, envelope
}

func TestSQLPersistenceRetryResumeAndFinalize(t *testing.T) {
	store, server := newTestStore(t)
	store.config.SessionFencing = "strong"
	store.config.SessionLockDuration = time.Second
	store.config.PersistenceInitialBackoff = 10 * time.Millisecond
	store.config.PersistenceMaxBackoff = time.Second
	task := testTask("sql-persist", "sql-persist-message")
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
	route := persistence.Route{TenantID: task.TenantID, AgentAppID: task.AgentAppID, Fingerprint: persistence.BackendFingerprint{
		SchemaVersion: persistence.FingerprintSchemaVersion, Kind: tenant.StorageKindPostgres,
		StorageProfileID: "pg", DatabaseIdentity: "db.example:5432/app", Namespace: "public.tenant_",
	}}
	commit := sessionfence.TurnCommit{SessionCoord: lease.SessionCoord, SessionSeq: lease.SessionSeq,
		AppName: tenant.AppName(task.TenantID, task.AgentAppID), UserID: task.RunnerUserID, SessionID: task.SessionID,
		FinalState: map[string][]byte{"state": []byte("value")}}
	reply := message.OutboundMessage{RequestID: task.RequestID, TraceID: task.TraceID, SessionID: task.SessionID, Text: "answer"}
	envelope, err := store.PreparePersistence(context.Background(), lease, route, reply, commit)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(context.Background(), task.InboxID())
	if err != nil || snapshot.State != StatePersisting || snapshot.PersistAttempt != 1 || snapshot.EnvelopeDigest != envelope.EnvelopeDigest {
		t.Fatalf("prepared snapshot = (%#v, %v)", snapshot, err)
	}
	if ttl := server.TTL(lease.InboxKey); ttl != 0 {
		t.Fatalf("persisting Inbox TTL = %s, want no TTL", ttl)
	}
	if err := store.Heartbeat(context.Background(), lease); err != nil {
		t.Fatalf("persisting task heartbeat: %v", err)
	}
	if err := store.SessionHeartbeat(context.Background(), lease); err != nil {
		t.Fatalf("persisting session heartbeat: %v", err)
	}
	retried, err := store.DeferPersistence(context.Background(), lease, envelope, "sql_unavailable")
	if err != nil {
		t.Fatal(err)
	}
	if retried.PersistAttempt != 2 || retried.EnvelopeDigest != envelope.EnvelopeDigest {
		t.Fatalf("retried envelope changed immutable digest: %#v", retried)
	}
	snapshot, _ = store.Snapshot(context.Background(), task.InboxID())
	if snapshot.State != StatePersistRetryWait || snapshot.PersistAttempt != 2 || snapshot.Attempt != task.Attempt {
		t.Fatalf("retry snapshot = %#v", snapshot)
	}
	if ttl := server.TTL(lease.InboxKey); ttl != 0 {
		t.Fatalf("persist_retry_wait Inbox TTL = %s, want no TTL", ttl)
	}
	members, err := store.client.ZRangeWithScores(context.Background(), store.persistenceRetryKey, 0, 0).Result()
	if err != nil || len(members) != 1 {
		t.Fatalf("persistence retry member = (%#v,%v)", members, err)
	}
	server.SetTime(time.UnixMilli(int64(members[0].Score) + 1))
	if promoted, err := store.PromotePersistenceRetries(context.Background(), 10); err != nil || promoted != 1 {
		t.Fatalf("PromotePersistenceRetries = (%d,%v)", promoted, err)
	}
	replayed, err := store.ReadTask(context.Background(), "worker-b", time.Millisecond)
	if err != nil || replayed.Task.Attempt != task.Attempt {
		t.Fatalf("replayed task = (%#v,%v)", replayed.Task, err)
	}
	resumedLease, resumedEnvelope, err := store.BeginPersistence(context.Background(), replayed, "worker-b")
	if err != nil {
		t.Fatal(err)
	}
	if resumedEnvelope.PersistAttempt != 2 || resumedEnvelope.EnvelopeDigest != envelope.EnvelopeDigest {
		t.Fatalf("resumed envelope = %#v", resumedEnvelope)
	}
	if err := store.FinalizePersistence(context.Background(), resumedLease, resumedEnvelope); err != nil {
		t.Fatal(err)
	}
	final, err := store.Snapshot(context.Background(), task.InboxID())
	if err != nil || final.State != StateSucceeded || final.RequestID != task.RequestID || final.RawPayload != "" || final.RawEnvelope != "" || final.EnvelopeDigest != "" {
		t.Fatalf("final snapshot = (%#v,%v)", final, err)
	}
	if ttl := server.TTL(resumedLease.InboxKey); ttl <= 0 {
		t.Fatalf("terminal Inbox TTL = %s", ttl)
	}
	deliveredReply, err := store.ReadReply(context.Background(), "gateway-sql", time.Millisecond)
	if err != nil || !deliveredReply.Result.Succeeded || deliveredReply.Result.Reply.Text != "answer" {
		t.Fatalf("SQL reply = (%#v,%v)", deliveredReply, err)
	}
}

func TestDeferPersistenceWrongTypeHasNoPartialSideEffects(t *testing.T) {
	store, _, task, lease, envelope := preparedPersistence(t, "sql-wrong-type")
	ctx := context.Background()
	before, _ := store.Snapshot(ctx, task.InboxID())
	lockBefore, _ := store.client.HGetAll(ctx, store.sessionLockKey(lease.SessionCoord)).Result()
	streamBefore := store.client.XLen(ctx, store.taskStream).Val()
	if err := store.client.Set(ctx, store.persistenceRetryKey, "wrong-type", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.DeferPersistence(ctx, lease, envelope, "sql_unavailable"); !errors.Is(err, ErrKeyType) {
		t.Fatalf("DeferPersistence error = %v", err)
	}
	after, _ := store.Snapshot(ctx, task.InboxID())
	lockAfter, _ := store.client.HGetAll(ctx, store.sessionLockKey(lease.SessionCoord)).Result()
	if after.State != before.State || after.PersistAttempt != before.PersistAttempt || after.RawEnvelope != before.RawEnvelope || !reflect.DeepEqual(lockAfter, lockBefore) || store.client.XLen(ctx, store.taskStream).Val() != streamBefore {
		t.Fatalf("wrong-type changed state: before=%#v after=%#v", before, after)
	}
}

func TestRecoverPersistingRequeuesEnvelopeWithoutAgentAttempt(t *testing.T) {
	store, clock, task, _, envelope := preparedPersistence(t, "sql-recover")
	clock.SetTime(time.Now().Add(2 * time.Second))
	stale, err := store.ClaimStale(context.Background(), "worker-new", 10)
	if err != nil || len(stale) != 1 {
		t.Fatalf("ClaimStale = (%#v,%v)", stale, err)
	}
	if err := store.Recover(context.Background(), stale[0], "worker-new"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Snapshot(context.Background(), task.InboxID())
	if err != nil || snapshot.State != StatePersisting || snapshot.Attempt != task.Attempt || snapshot.PersistAttempt != envelope.PersistAttempt || snapshot.EnvelopeDigest != envelope.EnvelopeDigest {
		t.Fatalf("recovered persistence snapshot = (%#v,%v)", snapshot, err)
	}
	replayed, err := store.ReadTask(context.Background(), "worker-new", time.Millisecond)
	if err != nil || replayed.Task.Attempt != task.Attempt {
		t.Fatalf("recovered delivery = (%#v,%v)", replayed, err)
	}
}

func TestCorruptPersistenceEnvelopeTerminatesSafely(t *testing.T) {
	store, _, task, lease, _ := preparedPersistence(t, "sql-corrupt")
	ctx := context.Background()
	if err := store.client.HSet(ctx, lease.InboxKey, "persistence_envelope", "{").Err(); err != nil {
		t.Fatal(err)
	}
	if err := store.client.HDel(ctx, lease.InboxKey, "owner", "lease_until").Err(); err != nil {
		t.Fatal(err)
	}
	if err := store.client.Del(ctx, store.sessionLockKey(lease.SessionCoord)).Err(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.BeginPersistence(ctx, lease.Delivery, "worker-new"); !errors.Is(err, persistence.ErrInvalidEnvelope) {
		t.Fatalf("BeginPersistence corrupt envelope error = %v", err)
	}
	snapshot, err := store.Snapshot(ctx, task.InboxID())
	if err != nil || snapshot.State != StateFailedTerminal || snapshot.ErrorCode != "persistence_envelope_invalid" || snapshot.RawEnvelope != "" || snapshot.EnvelopeDigest != "" {
		t.Fatalf("corrupt envelope terminal snapshot = (%#v,%v)", snapshot, err)
	}
	if ttl := store.client.TTL(ctx, lease.InboxKey).Val(); ttl <= 0 {
		t.Fatalf("corrupt terminal Inbox TTL = %s", ttl)
	}
	if snapshot.Result == nil || snapshot.Result.ErrorCode != "persistence_envelope_invalid" {
		t.Fatalf("corrupt terminal result = %#v", snapshot.Result)
	}
	lastCompleted, err := store.client.HGet(ctx, store.sessionStateKey(lease.SessionCoord), "last_completed_seq").Int64()
	if err != nil || lastCompleted != lease.SessionSeq {
		t.Fatalf("corrupt terminal session sequence = (%d,%v)", lastCompleted, err)
	}
}
