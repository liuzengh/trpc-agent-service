package redis_test

import (
	"context"
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	redisstore "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/redis"
	sessionstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/session"
	"github.com/alicebob/miniredis/v2"
	redisclient "github.com/redis/go-redis/v9"
)

func newStore(t *testing.T, server *miniredis.Miniredis) *redisstore.Store {
	t.Helper()
	client := redisclient.NewClient(&redisclient.Options{Addr: server.Addr()})
	store, err := redisstore.New(client, "test:runtime:v1")
	if err != nil {
		_ = client.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = store.Close()
		_ = client.Close()
	})
	return store
}

func seedEvent(t *testing.T, store *redisstore.Store, tenantID, sessionID, eventID string) runtimestorage.MessageEvent {
	t.Helper()
	if _, err := store.CreateSession(context.Background(), tenantID, sessionID, nil); err != nil {
		t.Fatal(err)
	}
	event, duplicate, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{
		TenantID: tenantID, SessionID: sessionID, BindingID: "binding-" + eventID,
		ExternalMessageID: "external-" + eventID, EventID: eventID,
	})
	if err != nil || duplicate {
		t.Fatalf("seed event = %+v duplicate=%v err=%v", event, duplicate, err)
	}
	return event
}

func TestRedisTenantIsolationAndReconnect(t *testing.T) {
	server := miniredis.RunT(t)
	first := newStore(t, server)
	second := newStore(t, server)
	ctx := context.Background()
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		if _, err := first.CreateSession(ctx, tenantID, "same-session", map[string]any{"tenant": tenantID}); err != nil {
			t.Fatal(err)
		}
		event, _, err := first.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: tenantID, SessionID: "same-session", BindingID: "same-binding", ExternalMessageID: "same-external", EventID: "same-event"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := first.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: tenantID, ReplyID: "same-reply", EventID: event.EventID, SegmentIndex: 0, SegmentCount: 1, Payload: tenantID}); err != nil {
			t.Fatal(err)
		}
		if _, err := first.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: tenantID, MemoryID: "same-memory", UserID: "user", Content: tenantID}); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := second.GetSession(ctx, "tenant-a", "same-session"); err != nil || got.State["tenant"] != "tenant-a" {
		t.Fatalf("tenant-a session = %+v, %v", got, err)
	}
	if got, err := second.GetSession(ctx, "tenant-b", "same-session"); err != nil || got.State["tenant"] != "tenant-b" {
		t.Fatalf("tenant-b session = %+v, %v", got, err)
	}
	if got, err := second.GetReply(ctx, "tenant-b", "same-reply", 0); err != nil || got.Payload != "tenant-b" {
		t.Fatalf("tenant-b reply = %+v, %v", got, err)
	}
	if _, err := second.GetMemory(ctx, "tenant-a", "same-memory"); err != nil {
		t.Fatal(err)
	}

	// A fresh client sees committed state after the original store is closed.
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := newStore(t, server)
	if got, err := reopened.GetMessage(ctx, "tenant-a", "same-event"); err != nil || got.SessionID != "same-session" {
		t.Fatalf("reopened event = %+v, %v", got, err)
	}
}

func TestRedisDuplicateDeliveryAndConcurrentSequence(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	if _, err := store.CreateSession(ctx, "tenant-a", "session", nil); err != nil {
		t.Fatal(err)
	}
	input := runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: "binding", ExternalMessageID: "external", EventID: "event-1"}
	first, duplicate, err := store.RecordMessage(ctx, input)
	if err != nil || duplicate {
		t.Fatalf("first = %+v duplicate=%v err=%v", first, duplicate, err)
	}
	input.EventID = "event-duplicate"
	replayed, duplicate, err := store.RecordMessage(ctx, input)
	if err != nil || !duplicate || replayed.EventID != first.EventID {
		t.Fatalf("replayed = %+v duplicate=%v err=%v", replayed, duplicate, err)
	}

	var wg sync.WaitGroup
	results := make(chan int64, 2)
	for _, eventID := range []string{"event-2", "event-3"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			value, _, callErr := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: id, ExternalMessageID: id, EventID: id})
			if callErr == nil {
				results <- value.EventSeq
			}
		}(eventID)
	}
	wg.Wait()
	close(results)
	var sequences []int64
	for value := range results {
		sequences = append(sequences, value)
	}
	if len(sequences) != 2 || sequences[0] == sequences[1] {
		t.Fatalf("concurrent sequences = %v", sequences)
	}
}

func TestRedisHistoryOrderingAndDefensiveCopies(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	seedEvent(t, store, "tenant-a", "session", "inbound")
	payload := []byte(`{"id":1}`)
	first, err := store.AppendEventPayload(context.Background(), sessionstorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "runner-1", Payload: payload})
	if err != nil || first.HistorySeq != 1 {
		t.Fatalf("first history = %+v, %v", first, err)
	}
	first.Payload[0] = 'x'
	if _, err := store.AppendEventPayload(context.Background(), sessionstorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "runner-1", Payload: []byte(`{"id":1}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendEventPayload(context.Background(), sessionstorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "runner-1", Payload: []byte(`{"id":2}`)}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("history conflict = %v", err)
	}
	second, err := store.AppendEventPayload(context.Background(), sessionstorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "runner-2", Payload: []byte(`{"id":2}`)})
	if err != nil || second.HistorySeq != 2 {
		t.Fatalf("second history = %+v, %v", second, err)
	}
	items, err := store.ListEventPayloads(context.Background(), "tenant-a", "session")
	if err != nil || len(items) != 2 || items[0].HistorySeq != 1 || items[1].HistorySeq != 2 || string(items[0].Payload) != `{"id":1}` {
		t.Fatalf("history = %+v, %v", items, err)
	}
}

func TestRedisMessageLeaseExpiryAndFencing(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	event := seedEvent(t, store, "tenant-a", "session", "event")
	running, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "worker-a", LeaseDuration: 15 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	reconciling, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventRunning, To: runtimestorage.EventExecutionReconciling, Owner: "worker-b"})
	if err != nil || reconciling.FencingToken != running.FencingToken+1 || reconciling.LeaseOwner != "" {
		t.Fatalf("reconciling = %+v, %v", reconciling, err)
	}
	if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventExecutionReconciling, To: runtimestorage.EventRunning, Owner: "worker-b", LeaseDuration: time.Second}); err != nil {
		t.Fatal(err)
	}
}

func TestRedisReplyBatchRetryAndDeadLetter(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	event := seedEvent(t, store, "tenant-a", "session", "event")
	batch := []runtimestorage.ReplyOutbox{
		{TenantID: "tenant-a", ReplyID: "reply", EventID: event.EventID, SegmentIndex: 0, SegmentCount: 2, Payload: "one"},
		{TenantID: "tenant-a", ReplyID: "reply", EventID: event.EventID, SegmentIndex: 1, SegmentCount: 2, Payload: "two"},
	}
	rows, err := store.EnqueueReplies(context.Background(), batch)
	if err != nil || len(rows) != 2 || rows[0].Status != runtimestorage.ReplyPending {
		t.Fatalf("batch = %+v, %v", rows, err)
	}
	if _, err := store.EnqueueReplies(context.Background(), []runtimestorage.ReplyOutbox{batch[0], {TenantID: "tenant-a", ReplyID: "reply", EventID: event.EventID, SegmentIndex: 1, SegmentCount: 2, Payload: "changed"}}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("batch conflict = %v", err)
	}
	claimed, err := store.ClaimReply(context.Background(), "tenant-a", "reply", 0, "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply", SegmentIndex: 0, From: runtimestorage.ReplySending, To: runtimestorage.ReplyRetryable, Owner: "worker-b", FencingToken: claimed.FencingToken}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale reply worker = %v", err)
	}
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply", SegmentIndex: 0, From: runtimestorage.ReplySending, To: runtimestorage.ReplyRetryable, Owner: "worker-a", FencingToken: claimed.FencingToken, ErrorClass: "rate_limited"}); err != nil {
		t.Fatal(err)
	}
	retry, err := store.ClaimReply(context.Background(), "tenant-a", "reply", 0, "worker-b", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	dead, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply", SegmentIndex: 0, From: runtimestorage.ReplySending, To: runtimestorage.ReplyDeadLetter, Owner: "worker-b", FencingToken: retry.FencingToken, ErrorClass: "permanent"})
	if err != nil || dead.Status != runtimestorage.ReplyDeadLetter || dead.LeaseOwner != "" || dead.LeaseExpiresAt != nil {
		t.Fatalf("dead letter = %+v, %v", dead, err)
	}
}

func TestRedisRecordsReplyReceiptWithinCurrentLease(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	event := seedEvent(t, store, "tenant-a", "session-receipt", "event-receipt")
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-receipt", EventID: event.EventID, SegmentIndex: 0, SegmentCount: 1, Payload: "hello"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimReply(context.Background(), "tenant-a", "reply-receipt", 0, "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	recorded, err := store.RecordReplyReceipt(context.Background(), runtimestorage.ReplyReceipt{TenantID: claimed.TenantID, ReplyID: claimed.ReplyID, SegmentIndex: claimed.SegmentIndex, Owner: claimed.LeaseOwner, FencingToken: claimed.FencingToken, ProviderID: "provider-1"})
	if err != nil || recorded.Status != runtimestorage.ReplySending || recorded.ProviderMessageID != "provider-1" || recorded.FencingToken != claimed.FencingToken || recorded.LeaseOwner != claimed.LeaseOwner {
		t.Fatalf("recorded receipt = %+v, %v", recorded, err)
	}
	if _, err := store.RecordReplyReceipt(context.Background(), runtimestorage.ReplyReceipt{TenantID: claimed.TenantID, ReplyID: claimed.ReplyID, SegmentIndex: claimed.SegmentIndex, Owner: claimed.LeaseOwner, FencingToken: claimed.FencingToken, ProviderID: "other-provider"}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("conflicting receipt = %v", err)
	}
	retry, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: claimed.TenantID, ReplyID: claimed.ReplyID, SegmentIndex: claimed.SegmentIndex, From: runtimestorage.ReplySending, To: runtimestorage.ReplyRetryable, Owner: claimed.LeaseOwner, FencingToken: claimed.FencingToken})
	if err != nil || retry.ProviderMessageID != "provider-1" {
		t.Fatalf("retry preserves receipt = %+v, %v", retry, err)
	}
}

func TestRedisMemoryDurabilityAndIndexHandoff(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	value, err := store.PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "memory", UserID: "user", Content: "likes coffee", Metadata: map[string]any{"kind": "fact"}, Embedding: []float64{1, 0}})
	if err != nil {
		t.Fatal(err)
	}
	value.Metadata["kind"] = "changed"
	if err := store.EnqueueMemoryIndex(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if err := store.WaitForMemoryIndex(context.Background(), "tenant-a", "memory", value.Version); err != nil {
		t.Fatalf("index handoff = %v", err)
	}
	value2, err := store.PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "memory", UserID: "user", Content: "likes tea"})
	if err != nil || value2.Version != 2 {
		t.Fatalf("memory update = %+v, %v", value2, err)
	}
	if err := store.WaitForMemoryIndex(context.Background(), "tenant-a", "memory", value2.Version); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("missing new handoff = %v", err)
	}
	if err := store.EnqueueMemoryIndex(context.Background(), value2); err != nil {
		t.Fatal(err)
	}
	if err := store.WaitForMemoryIndex(context.Background(), "tenant-a", "memory", value2.Version); err != nil {
		t.Fatal(err)
	}
	reopened := newStore(t, server)
	got, err := reopened.GetMemory(context.Background(), "tenant-a", "memory")
	if err != nil || got.Content != "likes tea" || got.Metadata == nil {
		t.Fatalf("reopened memory = %+v, %v", got, err)
	}
}

func TestRedisCancellationCloseOwnershipAndRedaction(t *testing.T) {
	server := miniredis.RunT(t)
	client := redisclient.NewClient(&redisclient.Options{Addr: server.Addr()})
	borrowed, err := redisstore.New(client, "test:runtime:v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := borrowed.Close(); err != nil || client.Ping(context.Background()).Err() != nil {
		t.Fatalf("borrowed close = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := borrowed.GetSession(canceled, "tenant-a", "session"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read = %v", err)
	}
	addr := server.Addr()
	server.Close()
	if _, err := redisstore.NewFromConfig(context.Background(), redisstore.Config{Addr: addr, Password: "super-secret"}); err == nil || strings.Contains(err.Error(), addr) || strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("redacted unavailable error = %v", err)
	}
	_ = client.Close()
}

func TestRedisScopedKeyCannotCollide(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	event := seedEvent(t, store, "tenant-a", "session", "event")
	for _, reply := range []runtimestorage.ReplyOutbox{
		{TenantID: "tenant-a", ReplyID: "ab", EventID: event.EventID, SegmentIndex: 0, SegmentCount: 1, Payload: "first"},
		{TenantID: "tenant-a", ReplyID: "a", EventID: event.EventID, SegmentIndex: 0, SegmentCount: 1, Payload: "second"},
	} {
		if _, err := store.EnqueueReply(context.Background(), reply); err != nil {
			t.Fatal(err)
		}
	}
	for replyID, payload := range map[string]string{"ab": "first", "a": "second"} {
		got, err := store.GetReply(context.Background(), "tenant-a", replyID, 0)
		if err != nil || got.Payload != payload {
			t.Fatalf("reply %q = %+v, %v", replyID, got, err)
		}
	}
}

func TestRedisSessionCASAndDeleteCascade(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	created, err := store.CreateSession(ctx, "tenant-a", "session", map[string]any{"nested": map[string]any{"value": "one"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(ctx, "tenant-a", "session", nil); !errors.Is(err, runtimestorage.ErrDuplicate) {
		t.Fatalf("duplicate session = %v", err)
	}
	if _, err := store.UpdateSessionState(ctx, "tenant-a", "session", created.Version+1, nil); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale session update = %v", err)
	}
	updated, err := store.UpdateSessionState(ctx, "tenant-a", "session", created.Version, map[string]any{"nested": map[string]any{"value": "two"}})
	if err != nil || updated.Version != created.Version+1 {
		t.Fatalf("session update = %+v, %v", updated, err)
	}
	updated.State["nested"].(map[string]any)["value"] = "mutated"
	got, err := store.GetSession(ctx, "tenant-a", "session")
	if err != nil || got.State["nested"].(map[string]any)["value"] != "two" {
		t.Fatalf("session copy = %+v, %v", got, err)
	}

	event, duplicate, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: "binding", ExternalMessageID: "external", EventID: "event"})
	if err != nil || duplicate {
		t.Fatalf("event = %+v duplicate=%v err=%v", event, duplicate, err)
	}
	if _, err := store.AppendEventPayload(ctx, sessionstorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "runner", Payload: []byte(`{"kind":"event"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: event.EventID, SegmentCount: 1, Payload: "payload"}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteSession(ctx, "tenant-a", "session"); err != nil {
		t.Fatal(err)
	}
	for name, check := range map[string]func() error{
		"session": func() error { _, err := store.GetSession(ctx, "tenant-a", "session"); return err },
		"event":   func() error { _, err := store.GetMessage(ctx, "tenant-a", event.EventID); return err },
		"reply":   func() error { _, err := store.GetReply(ctx, "tenant-a", "reply", 0); return err },
		"history": func() error { _, err := store.ListEventPayloads(ctx, "tenant-a", "session"); return err },
	} {
		if err := check(); !errors.Is(err, runtimestorage.ErrNotFound) {
			t.Fatalf("deleted %s = %v", name, err)
		}
	}
	if _, err := store.CreateSession(ctx, "tenant-a", "session", nil); err != nil {
		t.Fatal(err)
	}
	if _, duplicate, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: "binding", ExternalMessageID: "external", EventID: "event"}); err != nil || duplicate {
		t.Fatalf("reused inbound identity duplicate=%v err=%v", duplicate, err)
	}
}

func TestRedisReplyCorrelationAndCandidateOrdering(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	const traceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	target := runtimestorage.ReplyTarget{BindingID: "binding", ConversationKind: "direct", ReceiverID: "receiver"}
	if _, err := store.CreateSession(ctx, "tenant-a", "session", nil); err != nil {
		t.Fatal(err)
	}
	event, duplicate, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: "binding", ExternalMessageID: "external", EventID: "event", ReplyTarget: target})
	if err != nil || duplicate {
		t.Fatalf("event = %+v duplicate=%v err=%v", event, duplicate, err)
	}
	correlation := runtimestorage.ReplyCorrelation{TenantID: "tenant-a", EventID: event.EventID, RequestID: "request", TraceID: "trace", TraceParent: traceParent}
	batch := []runtimestorage.ReplyOutbox{
		{TenantID: "tenant-a", ReplyID: "reply-b", EventID: event.EventID, SegmentIndex: 1, SegmentCount: 2, Payload: "two", ReplyTarget: target},
		{TenantID: "tenant-a", ReplyID: "reply-b", EventID: event.EventID, SegmentIndex: 0, SegmentCount: 2, Payload: "one", ReplyTarget: target},
	}
	if _, err := store.EnqueueRepliesWithCorrelation(ctx, correlation, batch); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueRepliesWithCorrelation(ctx, correlation, batch); err != nil {
		t.Fatalf("idempotent correlated enqueue = %v", err)
	}
	if _, err := store.EnqueueRepliesWithCorrelation(ctx, runtimestorage.ReplyCorrelation{TenantID: "tenant-a", EventID: event.EventID, RequestID: "other"}, batch); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("correlation conflict = %v", err)
	}
	got, err := store.GetReplyCorrelation(ctx, "tenant-a", event.EventID)
	if err != nil || got != correlation {
		t.Fatalf("correlation = %+v, %v", got, err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-a", EventID: event.EventID, SegmentCount: 1, Payload: "first", ReplyTarget: target}); err != nil {
		t.Fatal(err)
	}
	candidates, err := store.ListReplyCandidates(ctx, "tenant-a")
	if err != nil || len(candidates) != 3 || candidates[0].ReplyID != "reply-a" || candidates[1].SegmentIndex != 0 || candidates[2].SegmentIndex != 1 {
		t.Fatalf("sorted candidates = %+v, %v", candidates, err)
	}
	if _, err := store.GetReplyCorrelation(ctx, "tenant-a", "missing"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing correlation = %v", err)
	}
}

func seedRedisMemories(t *testing.T, store *redisstore.Store) {
	t.Helper()
	for _, value := range []runtimestorage.MemoryInput{
		{TenantID: "tenant-a", MemoryID: "both", UserID: "user", Content: "coffee and tea", Topics: []string{"drink"}, Metadata: map[string]any{"source": "test"}, Embedding: []float64{1, 0}},
		{TenantID: "tenant-a", MemoryID: "coffee", UserID: "user", Content: "coffee", Embedding: []float64{0, 1}},
		{TenantID: "tenant-a", MemoryID: "other-user", UserID: "other", Content: "coffee and tea"},
	} {
		if _, err := store.PutMemory(context.Background(), value); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRedisMemoryQueriesAndDefensiveCopies(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	seedRedisMemories(t, store)
	values, err := store.ListMemories(ctx, "tenant-a", "user", 1)
	if err != nil || len(values) != 1 || values[0].UserID != "user" {
		t.Fatalf("limited memories = %+v, %v", values, err)
	}
	results, err := store.SearchMemories(ctx, "tenant-a", "user", "coffee tea", 10)
	if err != nil || len(results) != 2 || results[0].Memory.MemoryID != "both" || results[0].Score != 1 || results[1].Memory.MemoryID != "coffee" || results[1].Score != 0.5 {
		t.Fatalf("memory search = %+v, %v", results, err)
	}
	record, err := store.GetMemory(ctx, "tenant-a", "both")
	if err != nil {
		t.Fatal(err)
	}
	record.Topics[0] = "changed"
	record.Metadata["source"] = "changed"
	record.Embedding[0] = 9
	persisted, err := store.GetMemory(ctx, "tenant-a", "both")
	if err != nil || persisted.Topics[0] != "drink" || persisted.Metadata["source"] != "test" || persisted.Embedding[0] != 1 {
		t.Fatalf("memory defensive copy = %+v, %v", persisted, err)
	}
}

func TestRedisMemoryTombstoneRemovesIndexHandoff(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	seedRedisMemories(t, store)
	persisted, err := store.GetMemory(ctx, "tenant-a", "both")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnqueueMemoryIndex(ctx, persisted); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteMemory(ctx, "tenant-a", "both"); err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		name string
		call func() error
	}{
		{name: "read", call: func() error { _, err := store.GetMemory(ctx, "tenant-a", "both"); return err }},
		{name: "index", call: func() error { return store.EnqueueMemoryIndex(ctx, persisted) }},
		{name: "wait", call: func() error { return store.WaitForMemoryIndex(ctx, "tenant-a", "both", persisted.Version) }},
	}
	for _, check := range checks {
		assertRedisError(t, check.name, check.call(), runtimestorage.ErrNotFound)
	}
	values, err := store.ListMemories(ctx, "tenant-a", "user", 10)
	if err != nil || len(values) != 1 || values[0].MemoryID != "coffee" {
		t.Fatalf("tombstoned memories = %+v, %v", values, err)
	}
}

func TestRedisOwnedConstructorsAndPing(t *testing.T) {
	server := miniredis.RunT(t)
	ctx := context.Background()
	if _, err := redisstore.New(nil, ""); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("nil client = %v", err)
	}
	if _, err := redisstore.NewFromURL(ctx, "not a redis URL"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid url = %v", err)
	}
	fromURL, err := redisstore.NewFromURL(ctx, "redis://"+server.Addr()+"/2")
	if err != nil {
		t.Fatal(err)
	}
	if err := fromURL.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fromURL.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fromURL.Ping(ctx); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("closed url client ping = %v", err)
	}
	fromConfig, err := redisstore.NewFromConfig(ctx, redisstore.Config{Addr: server.Addr(), DB: 1, KeyPrefix: "custom", PoolSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := fromConfig.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fromConfig.Close(); err != nil {
		t.Fatal(err)
	}
}

func assertRedisError(t *testing.T, name string, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("%s = %v, want %v", name, err, want)
	}
}

func TestRedisConstructorAndContextValidation(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	var nilStore *redisstore.Store
	assertRedisError(t, "nil store ping", nilStore.Ping(ctx), runtimestorage.ErrStorage)
	assertRedisError(t, "nil context ping", store.Ping(nil), runtimestorage.ErrInvalid)
	_, err := store.GetSession(nil, "tenant-a", "session")
	assertRedisError(t, "nil context read", err, runtimestorage.ErrInvalid)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = redisstore.NewFromURL(canceled, "redis://"+server.Addr())
	assertRedisError(t, "canceled URL constructor", err, context.Canceled)
	_, err = redisstore.NewFromConfig(canceled, redisstore.Config{Addr: server.Addr()})
	assertRedisError(t, "canceled config constructor", err, context.Canceled)
	checks := []struct {
		name string
		call func() error
	}{
		{name: "empty config address", call: func() error { _, err := redisstore.NewFromConfig(ctx, redisstore.Config{}); return err }},
		{name: "negative config DB", call: func() error {
			_, err := redisstore.NewFromConfig(ctx, redisstore.Config{Addr: server.Addr(), DB: -1})
			return err
		}},
		{name: "nil URL context", call: func() error { _, err := redisstore.NewFromURL(nil, "redis://"+server.Addr()); return err }},
		{name: "empty URL", call: func() error { _, err := redisstore.NewFromURL(ctx, ""); return err }},
	}
	for _, check := range checks {
		assertRedisError(t, check.name, check.call(), runtimestorage.ErrInvalid)
	}
}

func TestRedisSessionAndEventValidation(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	checks := []struct {
		name string
		call func() error
	}{
		{name: "invalid correlation read", call: func() error { _, err := store.GetReplyCorrelation(ctx, "", "event"); return err }},
		{name: "empty correlation event", call: func() error { _, err := store.GetReplyCorrelation(ctx, "tenant-a", ""); return err }},
		{name: "invalid session tenant", call: func() error { _, err := store.GetSession(ctx, "", "session"); return err }},
		{name: "empty session ID", call: func() error { _, err := store.CreateSession(ctx, "tenant-a", "", nil); return err }},
		{name: "unencodable session state", call: func() error {
			_, err := store.CreateSession(ctx, "tenant-a", "bad-state", map[string]any{"channel": make(chan int)})
			return err
		}},
		{name: "unencodable update state", call: func() error {
			_, err := store.UpdateSessionState(ctx, "tenant-a", "missing", 1, map[string]any{"channel": make(chan int)})
			return err
		}},
		{name: "invalid session delete", call: func() error { return store.DeleteSession(ctx, "", "session") }},
		{name: "incomplete inbound event", call: func() error {
			_, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session"})
			return err
		}},
		{name: "mismatched reply target", call: func() error {
			_, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: "binding", ExternalMessageID: "external", EventID: "event", ReplyTarget: runtimestorage.ReplyTarget{BindingID: "other", ConversationKind: "direct", ReceiverID: "receiver"}})
			return err
		}},
		{name: "invalid message read", call: func() error { _, err := store.GetMessage(ctx, "", "event"); return err }},
		{name: "invalid history payload", call: func() error {
			_, err := store.AppendEventPayload(ctx, sessionstorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "event", Payload: []byte("not-json")})
			return err
		}},
		{name: "invalid history tenant", call: func() error { _, err := store.ListEventPayloads(ctx, "", "session"); return err }},
	}
	for _, check := range checks {
		assertRedisError(t, check.name, check.call(), runtimestorage.ErrInvalid)
	}
	transitions := []struct {
		name  string
		value runtimestorage.MessageTransition
		want  error
	}{
		{name: "missing owner", value: runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: "event", From: runtimestorage.EventReceived, To: runtimestorage.EventFailed}, want: runtimestorage.ErrInvalid},
		{name: "illegal transition", value: runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: "event", From: runtimestorage.EventReceived, To: runtimestorage.EventCompleted, Owner: "worker"}, want: runtimestorage.ErrIllegalTransition},
		{name: "running without lease", value: runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: "event", From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "worker"}, want: runtimestorage.ErrInvalid},
	}
	for _, transition := range transitions {
		_, err := store.TransitionMessage(ctx, transition.value)
		assertRedisError(t, transition.name, err, transition.want)
	}
}

func TestRedisReplyValidation(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	checks := []struct {
		name string
		call func() error
		want error
	}{
		{name: "non-pending reply", call: func() error {
			_, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", SegmentCount: 1, Status: runtimestorage.ReplySent})
			return err
		}, want: runtimestorage.ErrInvalid},
		{name: "out-of-range reply segment", call: func() error {
			_, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", SegmentIndex: 1, SegmentCount: 1})
			return err
		}, want: runtimestorage.ErrInvalid},
		{name: "incomplete reply batch", call: func() error {
			_, err := store.EnqueueReplies(ctx, []runtimestorage.ReplyOutbox{{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", SegmentCount: 2}})
			return err
		}, want: runtimestorage.ErrInvalid},
		{name: "empty correlation enqueue", call: func() error {
			_, err := store.EnqueueRepliesWithCorrelation(ctx, runtimestorage.ReplyCorrelation{}, nil)
			return err
		}, want: runtimestorage.ErrInvalid},
		{name: "empty reply ID", call: func() error { _, err := store.GetReply(ctx, "tenant-a", "", 0); return err }, want: runtimestorage.ErrInvalid},
		{name: "invalid candidates tenant", call: func() error { _, err := store.ListReplyCandidates(ctx, ""); return err }, want: runtimestorage.ErrInvalid},
		{name: "empty reply owner", call: func() error { _, err := store.ClaimReply(ctx, "tenant-a", "reply", 0, "", time.Second); return err }, want: runtimestorage.ErrInvalid},
		{name: "illegal reply transition", call: func() error {
			_, err := store.TransitionReply(ctx, runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply", From: runtimestorage.ReplySent, To: runtimestorage.ReplySending, Owner: "worker"})
			return err
		}, want: runtimestorage.ErrIllegalTransition},
	}
	for _, check := range checks {
		assertRedisError(t, check.name, check.call(), check.want)
	}
}

func TestRedisMemoryValidation(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	checks := []struct {
		name string
		call func() error
	}{
		{name: "empty memory user", call: func() error {
			_, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", Content: "content"})
			return err
		}},
		{name: "non-finite memory embedding", call: func() error {
			_, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", UserID: "user", Content: "content", Embedding: []float64{math.NaN()}})
			return err
		}},
		{name: "unencodable memory metadata", call: func() error {
			_, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", UserID: "user", Content: "content", Metadata: map[string]any{"channel": make(chan int)}})
			return err
		}},
		{name: "invalid memory read", call: func() error { _, err := store.GetMemory(ctx, "", "memory"); return err }},
		{name: "empty memory list user", call: func() error { _, err := store.ListMemories(ctx, "tenant-a", "", 0); return err }},
		{name: "negative memory limit", call: func() error { _, err := store.ListMemories(ctx, "tenant-a", "user", -1); return err }},
		{name: "empty memory query", call: func() error { _, err := store.SearchMemories(ctx, "tenant-a", "user", "", 0); return err }},
		{name: "empty memory delete", call: func() error { return store.DeleteMemory(ctx, "tenant-a", "") }},
		{name: "invalid memory index", call: func() error {
			return store.EnqueueMemoryIndex(ctx, runtimestorage.MemoryRecord{TenantID: "tenant-a", MemoryID: "memory"})
		}},
		{name: "invalid memory handoff wait", call: func() error { return store.WaitForMemoryIndex(ctx, "tenant-a", "memory", 0) }},
	}
	for _, check := range checks {
		assertRedisError(t, check.name, check.call(), runtimestorage.ErrInvalid)
	}
}

func TestRedisOperationsHonorCanceledContext(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reply := runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", SegmentCount: 1}
	calls := []struct {
		name string
		call func() error
	}{
		{name: "ping", call: func() error { return store.Ping(ctx) }},
		{name: "correlation", call: func() error { _, err := store.GetReplyCorrelation(ctx, "tenant-a", "event"); return err }},
		{name: "get session", call: func() error { _, err := store.GetSession(ctx, "tenant-a", "session"); return err }},
		{name: "create session", call: func() error { _, err := store.CreateSession(ctx, "tenant-a", "session", nil); return err }},
		{name: "update session", call: func() error { _, err := store.UpdateSessionState(ctx, "tenant-a", "session", 1, nil); return err }},
		{name: "delete session", call: func() error { return store.DeleteSession(ctx, "tenant-a", "session") }},
		{name: "record message", call: func() error {
			_, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: "binding", ExternalMessageID: "external", EventID: "event"})
			return err
		}},
		{name: "get message", call: func() error { _, err := store.GetMessage(ctx, "tenant-a", "event"); return err }},
		{name: "transition message", call: func() error {
			_, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: "event", From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "worker", LeaseDuration: time.Second})
			return err
		}},
		{name: "append payload", call: func() error {
			_, err := store.AppendEventPayload(ctx, sessionstorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "event", Payload: []byte(`{}`)})
			return err
		}},
		{name: "list payloads", call: func() error { _, err := store.ListEventPayloads(ctx, "tenant-a", "session"); return err }},
		{name: "enqueue reply", call: func() error { _, err := store.EnqueueReply(ctx, reply); return err }},
		{name: "enqueue replies", call: func() error { _, err := store.EnqueueReplies(ctx, []runtimestorage.ReplyOutbox{reply}); return err }},
		{name: "enqueue correlated replies", call: func() error {
			_, err := store.EnqueueRepliesWithCorrelation(ctx, runtimestorage.ReplyCorrelation{TenantID: "tenant-a", EventID: "event", RequestID: "request"}, []runtimestorage.ReplyOutbox{reply})
			return err
		}},
		{name: "get reply", call: func() error { _, err := store.GetReply(ctx, "tenant-a", "reply", 0); return err }},
		{name: "list candidates", call: func() error { _, err := store.ListReplyCandidates(ctx, "tenant-a"); return err }},
		{name: "claim reply", call: func() error {
			_, err := store.ClaimReply(ctx, "tenant-a", "reply", 0, "worker", time.Second)
			return err
		}},
		{name: "transition reply", call: func() error {
			_, err := store.TransitionReply(ctx, runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply", From: runtimestorage.ReplyPending, To: runtimestorage.ReplySending, Owner: "worker"})
			return err
		}},
		{name: "put memory", call: func() error {
			_, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", UserID: "user", Content: "content"})
			return err
		}},
		{name: "get memory", call: func() error { _, err := store.GetMemory(ctx, "tenant-a", "memory"); return err }},
		{name: "list memories", call: func() error { _, err := store.ListMemories(ctx, "tenant-a", "user", 0); return err }},
		{name: "search memories", call: func() error { _, err := store.SearchMemories(ctx, "tenant-a", "user", "content", 0); return err }},
		{name: "delete memory", call: func() error { return store.DeleteMemory(ctx, "tenant-a", "memory") }},
		{name: "enqueue memory index", call: func() error {
			return store.EnqueueMemoryIndex(ctx, runtimestorage.MemoryRecord{TenantID: "tenant-a", MemoryID: "memory", Version: 1})
		}},
		{name: "wait memory index", call: func() error { return store.WaitForMemoryIndex(ctx, "tenant-a", "memory", 1) }},
	}
	for _, value := range calls {
		assertRedisError(t, value.name, value.call(), context.Canceled)
	}
}

func TestRedisMissingRecordsReturnNotFound(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	reply := runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", SegmentCount: 1}
	calls := []struct {
		name string
		call func() error
	}{
		{name: "correlation", call: func() error { _, err := store.GetReplyCorrelation(ctx, "tenant-a", "event"); return err }},
		{name: "session", call: func() error { _, err := store.GetSession(ctx, "tenant-a", "session"); return err }},
		{name: "session update", call: func() error { _, err := store.UpdateSessionState(ctx, "tenant-a", "session", 1, nil); return err }},
		{name: "session delete", call: func() error { return store.DeleteSession(ctx, "tenant-a", "session") }},
		{name: "message record", call: func() error {
			_, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: "binding", ExternalMessageID: "external", EventID: "event"})
			return err
		}},
		{name: "message", call: func() error { _, err := store.GetMessage(ctx, "tenant-a", "event"); return err }},
		{name: "message transition", call: func() error {
			_, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: "event", From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "worker", LeaseDuration: time.Second})
			return err
		}},
		{name: "payload append", call: func() error {
			_, err := store.AppendEventPayload(ctx, sessionstorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "event", Payload: []byte(`{}`)})
			return err
		}},
		{name: "payload list", call: func() error { _, err := store.ListEventPayloads(ctx, "tenant-a", "session"); return err }},
		{name: "reply enqueue", call: func() error { _, err := store.EnqueueReply(ctx, reply); return err }},
		{name: "reply batch", call: func() error { _, err := store.EnqueueReplies(ctx, []runtimestorage.ReplyOutbox{reply}); return err }},
		{name: "reply", call: func() error { _, err := store.GetReply(ctx, "tenant-a", "reply", 0); return err }},
		{name: "reply claim", call: func() error {
			_, err := store.ClaimReply(ctx, "tenant-a", "reply", 0, "worker", time.Second)
			return err
		}},
		{name: "reply transition", call: func() error {
			_, err := store.TransitionReply(ctx, runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply", From: runtimestorage.ReplyPending, To: runtimestorage.ReplySending, Owner: "worker"})
			return err
		}},
		{name: "memory", call: func() error { _, err := store.GetMemory(ctx, "tenant-a", "memory"); return err }},
		{name: "memory delete", call: func() error { return store.DeleteMemory(ctx, "tenant-a", "memory") }},
		{name: "memory index", call: func() error {
			return store.EnqueueMemoryIndex(ctx, runtimestorage.MemoryRecord{TenantID: "tenant-a", MemoryID: "memory", Version: 1})
		}},
		{name: "memory index wait", call: func() error { return store.WaitForMemoryIndex(ctx, "tenant-a", "memory", 1) }},
	}
	for _, value := range calls {
		assertRedisError(t, value.name, value.call(), runtimestorage.ErrNotFound)
	}
	memories, err := store.ListMemories(ctx, "tenant-a", "user", 0)
	if err != nil || len(memories) != 0 {
		t.Fatalf("empty memories = %+v, %v", memories, err)
	}
	candidates, err := store.ListReplyCandidates(ctx, "tenant-a")
	if err != nil || len(candidates) != 0 {
		t.Fatalf("empty reply candidates = %+v, %v", candidates, err)
	}
}

func TestRedisMessageSuccessTransitions(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	event := seedEvent(t, store, "tenant-a", "session", "event")
	running, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "worker", LeaseDuration: time.Second})
	if err != nil || running.Status != runtimestorage.EventRunning || running.FencingToken != 1 || running.LeaseOwner != "worker" {
		t.Fatalf("running event = %+v, %v", running, err)
	}
	completed, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventRunning, To: runtimestorage.EventCompleted, Owner: "worker", FencingToken: running.FencingToken})
	if err != nil || completed.Status != runtimestorage.EventCompleted || completed.LeaseExpiresAt != nil {
		t.Fatalf("completed event = %+v, %v", completed, err)
	}
	pending, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventCompleted, To: runtimestorage.EventReplyPending, Owner: "worker", ReplyID: "reply", SegmentCount: 1})
	if err != nil || pending.Status != runtimestorage.EventReplyPending || pending.ReplyID != "reply" || pending.SegmentCount != 1 {
		t.Fatalf("pending reply event = %+v, %v", pending, err)
	}
	replied, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventReplyPending, To: runtimestorage.EventReplied, Owner: "worker"})
	if err != nil || replied.Status != runtimestorage.EventReplied {
		t.Fatalf("replied event = %+v, %v", replied, err)
	}
	if got, err := store.GetMessage(ctx, "tenant-a", event.EventID); err != nil || got.Status != runtimestorage.EventReplied {
		t.Fatalf("persisted replied event = %+v, %v", got, err)
	}
}

func TestRedisReplyDeliverySuccessTransitions(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	event := seedEvent(t, store, "tenant-a", "session", "event")
	reply, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: event.EventID, SegmentCount: 1, Payload: "response"})
	if err != nil || reply.Status != runtimestorage.ReplyPending {
		t.Fatalf("pending reply = %+v, %v", reply, err)
	}
	claimed, err := store.ClaimReply(ctx, "tenant-a", "reply", 0, "worker", time.Second)
	if err != nil || claimed.Status != runtimestorage.ReplySending || claimed.Attempts != 1 {
		t.Fatalf("claimed reply = %+v, %v", claimed, err)
	}
	sent, err := store.TransitionReply(ctx, runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply", From: runtimestorage.ReplySending, To: runtimestorage.ReplySent, Owner: "worker", FencingToken: claimed.FencingToken, ProviderID: "provider-message"})
	if err != nil || sent.Status != runtimestorage.ReplySent || sent.ProviderMessageID != "provider-message" || sent.LeaseExpiresAt != nil {
		t.Fatalf("sent reply = %+v, %v", sent, err)
	}
}

func TestRedisMemoryDefaultValues(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	value, err := store.PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", UserID: "user", Content: "content"})
	if err != nil || !strings.HasPrefix(value.MemoryID, "mem_") || value.Version != 1 {
		t.Fatalf("defaulted memory = %+v, %v", value, err)
	}
	if _, err := store.SearchMemories(context.Background(), "tenant-a", "user", "absent", 0); err != nil {
		t.Fatalf("empty search = %v", err)
	}
}

func TestRedisConstructorsNormalizeDefaultsAndOptions(t *testing.T) {
	server := miniredis.RunT(t)
	ctx := context.Background()
	client := redisclient.NewClient(&redisclient.Options{Addr: server.Addr()})
	borrowed, err := redisstore.New(client, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = borrowed.Close(); _ = client.Close() })
	if _, err := borrowed.CreateSession(ctx, "tenant-a", "session", nil); err != nil {
		t.Fatal(err)
	}
	if !server.Exists("trpc:runtime:v1:" + hex.EncodeToString([]byte("tenant-a"))) {
		t.Fatal("default Redis key prefix was not used")
	}
	configured, err := redisstore.NewFromConfig(ctx, redisstore.Config{
		Addr:         server.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PoolSize:     2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := configured.Close(); err != nil {
		t.Fatal(err)
	}
	addr := server.Addr()
	server.Close()
	if _, err := redisstore.NewFromURL(ctx, "redis://"+addr); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("unavailable URL constructor = %v", err)
	}
}

func TestRedisReplyInsertionIsIdempotentAndFenced(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	target := runtimestorage.ReplyTarget{BindingID: "binding-target", ConversationKind: "direct", ReceiverID: "receiver"}
	event := seedEvent(t, store, "tenant-a", "session", "event")
	if _, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: "binding-target", ExternalMessageID: "external-target", EventID: "event-target", ReplyTarget: target}); err != nil {
		t.Fatal(err)
	}
	input := runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: event.EventID, SegmentCount: 1, Payload: "payload"}
	first, err := store.EnqueueReply(ctx, input)
	if err != nil || first.Status != runtimestorage.ReplyPending {
		t.Fatalf("first reply = %+v, %v", first, err)
	}
	second, err := store.EnqueueReply(ctx, input)
	if err != nil || second.CreatedAt != first.CreatedAt {
		t.Fatalf("idempotent reply = %+v, %v", second, err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: event.EventID, SegmentCount: 1, Payload: "changed"}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("reply payload conflict = %v", err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "target", EventID: "event-target", SegmentCount: 1}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("reply target conflict = %v", err)
	}
}

func TestRedisReplyBatchRejectsInconsistentSegments(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	event := seedEvent(t, store, "tenant-a", "session", "event")
	base := runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: event.EventID, SegmentCount: 2}
	tests := []struct {
		name  string
		batch []runtimestorage.ReplyOutbox
	}{
		{name: "empty", batch: nil},
		{name: "invalid first", batch: []runtimestorage.ReplyOutbox{{TenantID: "tenant-a", ReplyID: "reply", EventID: event.EventID, SegmentCount: 1, Status: runtimestorage.ReplySent}}},
		{name: "different tenant", batch: []runtimestorage.ReplyOutbox{base, {TenantID: "tenant-b", ReplyID: "reply", EventID: event.EventID, SegmentIndex: 1, SegmentCount: 2}}},
		{name: "different reply", batch: []runtimestorage.ReplyOutbox{base, {TenantID: "tenant-a", ReplyID: "other", EventID: event.EventID, SegmentIndex: 1, SegmentCount: 2}}},
		{name: "different event", batch: []runtimestorage.ReplyOutbox{base, {TenantID: "tenant-a", ReplyID: "reply", EventID: "other", SegmentIndex: 1, SegmentCount: 2}}},
		{name: "different count", batch: []runtimestorage.ReplyOutbox{base, {TenantID: "tenant-a", ReplyID: "reply", EventID: event.EventID, SegmentIndex: 1, SegmentCount: 3}}},
		{name: "duplicate segment", batch: []runtimestorage.ReplyOutbox{base, base}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := store.EnqueueReplies(context.Background(), test.batch); !errors.Is(err, runtimestorage.ErrInvalid) {
				t.Fatalf("batch error = %v", err)
			}
		})
	}
}

func TestRedisReplyTransitionCanStartDelivery(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	event := seedEvent(t, store, "tenant-a", "session", "event")
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: event.EventID, SegmentCount: 1}); err != nil {
		t.Fatal(err)
	}
	sending, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply", From: runtimestorage.ReplyPending, To: runtimestorage.ReplySending, Owner: "worker", LeaseDuration: time.Second})
	if err != nil || sending.Status != runtimestorage.ReplySending || sending.Attempts != 1 || sending.LeaseExpiresAt == nil {
		t.Fatalf("sending reply = %+v, %v", sending, err)
	}
}

func TestRedisMalformedStateAndUnavailableCommandsFailClosed(t *testing.T) {
	server := miniredis.RunT(t)
	client := redisclient.NewClient(&redisclient.Options{Addr: server.Addr()})
	store, err := redisstore.New(client, "test:runtime:v1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = client.Close() })
	ctx := context.Background()
	key := "test:runtime:v1:" + hex.EncodeToString([]byte("tenant-a"))
	if err := client.Set(ctx, key, "{", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetSession(ctx, "tenant-a", "session"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("malformed state read = %v", err)
	}
	if err := client.Set(ctx, key, `{"version":99}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetSession(ctx, "tenant-a", "session"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("unsupported state version = %v", err)
	}
	server.Close()
	if _, err := store.GetSession(ctx, "tenant-a", "session"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("unavailable read = %v", err)
	}
	if _, err := store.CreateSession(ctx, "tenant-a", "session", nil); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("unavailable mutation = %v", err)
	}
}
