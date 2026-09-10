package inmemory_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	sessionstorage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/session"
)

func seedEvent(t *testing.T, store *inmemory.Store, tenantID, sessionID, eventID string) {
	t.Helper()
	if _, err := store.CreateSession(context.Background(), tenantID, sessionID, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{
		TenantID: tenantID, SessionID: sessionID, BindingID: "binding-" + eventID,
		ExternalMessageID: "external-" + eventID, EventID: eventID,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestStoreReplyKeysSupportLargeSegmentsAndSharedPrefixes(t *testing.T) {
	store := inmemory.New()
	seedEvent(t, store, "tenant-a", "session-reply-key", "event-reply-key")
	for _, value := range []runtimestorage.ReplyOutbox{
		{TenantID: "tenant-a", ReplyID: "reply", EventID: "event-reply-key", SegmentIndex: 0, SegmentCount: 2, Payload: "first"},
		{TenantID: "tenant-a", ReplyID: "reply", EventID: "event-reply-key", SegmentIndex: 1, SegmentCount: 2, Payload: "second"},
	} {
		if _, err := store.EnqueueReply(context.Background(), value); err != nil {
			t.Fatal(err)
		}
	}
	large := runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "large", EventID: "event-reply-key", SegmentIndex: 1 << 30, SegmentCount: 1<<30 + 1, Payload: "large"}
	if _, err := store.EnqueueReply(context.Background(), large); err != nil {
		t.Fatal(err)
	}
	for _, want := range []runtimestorage.ReplyOutbox{
		{TenantID: "tenant-a", ReplyID: "reply", SegmentIndex: 0},
		{TenantID: "tenant-a", ReplyID: "reply", SegmentIndex: 1},
		{TenantID: "tenant-a", ReplyID: "large", SegmentIndex: 1 << 30},
	} {
		got, err := store.GetReply(context.Background(), want.TenantID, want.ReplyID, want.SegmentIndex)
		if err != nil || got.Payload == "" {
			t.Fatalf("GetReply(%+v) = %+v, %v", want, got, err)
		}
	}
}

func TestStoreTenantIsolationAndCAS(t *testing.T) {
	store := inmemory.New()
	first, err := store.CreateSession(context.Background(), "tenant-a", "session-1", map[string]any{"nested": map[string]any{"n": 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetSession(context.Background(), "tenant-b", first.SessionID); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("cross-tenant read = %v", err)
	}
	if _, err := store.UpdateSessionState(context.Background(), first.TenantID, first.SessionID, 99, nil); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("CAS error = %v", err)
	}
	updated, err := store.UpdateSessionState(context.Background(), first.TenantID, first.SessionID, first.Version, map[string]any{"nested": map[string]any{"n": 2}})
	if err != nil || updated.Version != 2 {
		t.Fatalf("update = %+v, %v", updated, err)
	}
	updated.State["nested"].(map[string]any)["n"] = 9
	readBack, err := store.GetSession(context.Background(), "tenant-a", "session-1")
	if err != nil || readBack.State["nested"].(map[string]any)["n"] == 9 {
		t.Fatalf("nested state aliased: %+v", readBack.State)
	}
}

func TestStoreReplyCorrelationIsIdempotentAndConflictSafe(t *testing.T) {
	store := inmemory.New()
	value := runtimestorage.ReplyCorrelation{TenantID: "tenant-a", EventID: "event-a", RequestID: "request-a", TraceID: "trace-a"}
	if _, err := store.CreateSession(context.Background(), value.TenantID, "session-correlation", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: value.TenantID, EventID: value.EventID, SessionID: "session-correlation", BindingID: "binding-correlation", ExternalMessageID: "external-correlation"}); err != nil {
		t.Fatal(err)
	}
	batch := []runtimestorage.ReplyOutbox{{TenantID: value.TenantID, ReplyID: "reply-correlation", EventID: value.EventID, SegmentIndex: 0, SegmentCount: 1, Payload: "payload"}}
	if _, err := store.EnqueueRepliesWithCorrelation(context.Background(), value, batch); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueRepliesWithCorrelation(context.Background(), value, batch); err != nil {
		t.Fatalf("idempotent save = %v", err)
	}
	if _, err := store.EnqueueRepliesWithCorrelation(context.Background(), runtimestorage.ReplyCorrelation{TenantID: value.TenantID, EventID: value.EventID, RequestID: "other"}, batch); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("conflicting save = %v", err)
	}
	got, err := store.GetReplyCorrelation(context.Background(), value.TenantID, value.EventID)
	if err != nil || got != value {
		t.Fatalf("correlation = %+v, %v", got, err)
	}
	if _, err := store.EnqueueRepliesWithCorrelation(context.Background(), runtimestorage.ReplyCorrelation{}, nil); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid atomic enqueue = %v", err)
	}
}

func TestStoreReplyCorrelationNormalizesTraceParentAtPersistenceBoundary(t *testing.T) {
	store := inmemory.New()
	seedEvent(t, store, "tenant-a", "session-trace-parent", "event-trace-parent")
	batch := []runtimestorage.ReplyOutbox{{TenantID: "tenant-a", ReplyID: "reply-trace-parent", EventID: "event-trace-parent", SegmentIndex: 0, SegmentCount: 1, Payload: "payload"}}
	valid := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if _, err := store.EnqueueRepliesWithCorrelation(context.Background(), runtimestorage.ReplyCorrelation{TenantID: "tenant-a", EventID: "event-trace-parent", RequestID: "request", TraceParent: valid}, batch); err != nil {
		t.Fatal(err)
	}
	got, err := store.GetReplyCorrelation(context.Background(), "tenant-a", "event-trace-parent")
	if err != nil || got.TraceParent != valid {
		t.Fatalf("valid traceparent = %q, %v", got.TraceParent, err)
	}
	seedEvent(t, store, "tenant-a", "session-invalid-trace-parent", "event-invalid-trace-parent")
	invalid := runtimestorage.ReplyCorrelation{TenantID: "tenant-a", EventID: "event-invalid-trace-parent", RequestID: "request", TraceParent: "malformed"}
	invalidBatch := []runtimestorage.ReplyOutbox{{TenantID: "tenant-a", ReplyID: "reply-invalid-trace-parent", EventID: invalid.EventID, SegmentIndex: 0, SegmentCount: 1, Payload: "payload"}}
	if _, err := store.EnqueueRepliesWithCorrelation(context.Background(), invalid, invalidBatch); err != nil {
		t.Fatal(err)
	}
	got, err = store.GetReplyCorrelation(context.Background(), invalid.TenantID, invalid.EventID)
	if err != nil || got.TraceParent != "" {
		t.Fatalf("invalid traceparent persisted as %q, %v", got.TraceParent, err)
	}
}

func TestStorePersistsMediaReplyContractAndDetectsConflicts(t *testing.T) {
	store := inmemory.New()
	seedEvent(t, store, "tenant-a", "session-media-reply", "event-media-reply")
	reference := mediaReplyReference(t, attachment.KindImage, "image/png", []byte("png"))
	reply := runtimestorage.ReplyOutbox{
		TenantID: "tenant-a", ReplyID: "reply-media", EventID: "event-media-reply", SegmentIndex: 0, SegmentCount: 1,
		Kind: runtimestorage.ReplyKindImage, Payload: "caption", Attachment: reference, Fallback: "[image attachment: chart.png]",
	}
	first, err := store.EnqueueReply(context.Background(), reply)
	if err != nil {
		t.Fatal(err)
	}
	if first.Kind != runtimestorage.ReplyKindImage || first.Attachment != reference || first.Fallback != reply.Fallback {
		t.Fatalf("stored media reply = %+v", first)
	}
	if second, err := store.EnqueueReply(context.Background(), reply); err != nil || second.Attachment != reference {
		t.Fatalf("idempotent media reply = %+v err=%v", second, err)
	}
	conflict := reply
	conflict.Fallback = "[image attachment: changed]"
	if _, err := store.EnqueueReply(context.Background(), conflict); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("media conflict = %v", err)
	}
	invalid := reply
	invalid.Fallback = ""
	if _, err := store.EnqueueReply(context.Background(), invalid); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid media fallback = %v", err)
	}
}

func TestStoreDuplicateMessageAndConcurrentSequence(t *testing.T) {
	store := inmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-1", nil); err != nil {
		t.Fatal(err)
	}
	input := runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session-1", BindingID: "binding-1", ExternalMessageID: "external-1", IdempotencyKey: "idem-1"}
	input.EventID = "event-1"
	first, duplicate, err := store.RecordMessage(context.Background(), input)
	if err != nil || duplicate {
		t.Fatalf("first = %+v duplicate=%v err=%v", first, duplicate, err)
	}
	second, duplicate, err := store.RecordMessage(context.Background(), input)
	if err != nil || !duplicate || second.EventID != first.EventID {
		t.Fatalf("duplicate = %+v duplicate=%v err=%v", second, duplicate, err)
	}

	var wg sync.WaitGroup
	results := make(chan int64, 2)
	for _, id := range []string{"event-2", "event-3"} {
		wg.Add(1)
		go func(eventID string) {
			defer wg.Done()
			value, _, callErr := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session-1", BindingID: eventID, ExternalMessageID: eventID, EventID: eventID})
			if callErr == nil {
				results <- value.EventSeq
			}
		}(id)
	}
	wg.Wait()
	close(results)
	var seq []int64
	for value := range results {
		seq = append(seq, value)
	}
	if len(seq) != 2 || seq[0] == seq[1] {
		t.Fatalf("concurrent sequences = %v", seq)
	}
}

func mediaReplyReference(t *testing.T, kind attachment.Kind, contentType string, data []byte) attachment.Reference {
	t.Helper()
	digest := sha256.Sum256(data)
	reference := attachment.Reference{ID: "attachment-media", Kind: kind, MIMEType: contentType, Name: "chart.png", Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
	if _, err := reference.Normalize(); err != nil {
		t.Fatalf("test attachment = %v", err)
	}
	return reference
}

func TestStorePersistsFirstReplyTargetForDuplicateMessage(t *testing.T) {
	store := inmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-1", nil); err != nil {
		t.Fatal(err)
	}
	first := runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session-1", BindingID: "binding-1", ExternalMessageID: "external-1", EventID: "event-1", ReplyTarget: runtimestorage.ReplyTarget{BindingID: "binding-1", ConversationKind: "direct", ReceiverID: "user-1"}}
	event, duplicate, err := store.RecordMessage(context.Background(), first)
	if err != nil || duplicate {
		t.Fatalf("first record = %+v duplicate=%v err=%v", event, duplicate, err)
	}
	first.EventID = "event-2"
	first.ReplyTarget.ReceiverID = "user-2"
	duplicateEvent, duplicate, err := store.RecordMessage(context.Background(), first)
	if err != nil || !duplicate || duplicateEvent.ReplyTarget.ReceiverID != "user-1" {
		t.Fatalf("duplicate record = %+v duplicate=%v err=%v", duplicateEvent, duplicate, err)
	}
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-1", EventID: event.EventID, SegmentCount: 1, ReplyTarget: event.ReplyTarget}); err != nil {
		t.Fatalf("enqueue matching target = %v", err)
	}
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-2", EventID: event.EventID, SegmentCount: 1, ReplyTarget: first.ReplyTarget}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("enqueue mismatched target = %v", err)
	}
}

func TestStoreRejectsEventIDCollisionWithoutChangingSession(t *testing.T) {
	store := inmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-1", nil); err != nil {
		t.Fatal(err)
	}
	firstInput := runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session-1", BindingID: "binding-1", ExternalMessageID: "external-1", EventID: "event-1"}
	first, duplicate, err := store.RecordMessage(context.Background(), firstInput)
	if err != nil || duplicate {
		t.Fatalf("first message = %+v duplicate=%v err=%v", first, duplicate, err)
	}
	_, duplicate, err = store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session-1", BindingID: "binding-2", ExternalMessageID: "external-2", EventID: "event-1"})
	if !errors.Is(err, runtimestorage.ErrDuplicate) || duplicate {
		t.Fatalf("event ID collision = duplicate=%v err=%v", duplicate, err)
	}
	persisted, err := store.GetMessage(context.Background(), "tenant-a", "event-1")
	if err != nil || persisted.BindingID != first.BindingID || persisted.ExternalMessageID != first.ExternalMessageID {
		t.Fatalf("first message changed = %+v err=%v", persisted, err)
	}
	sess, err := store.GetSession(context.Background(), "tenant-a", "session-1")
	if err != nil || sess.Version != first.EventSeq {
		t.Fatalf("session version after collision = %d, want %d (err=%v)", sess.Version, first.EventSeq, err)
	}
}

func TestStoreDeleteSessionRemovesAssociatedRuntimeData(t *testing.T) {
	store := inmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-1", nil); err != nil {
		t.Fatal(err)
	}
	input := runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session-1", BindingID: "binding-1", ExternalMessageID: "external-1", EventID: "event-1"}
	if _, _, err := store.RecordMessage(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-1", EventID: "event-1", SegmentIndex: 0, SegmentCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteSession(context.Background(), "tenant-a", "session-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetSession(context.Background(), "tenant-a", "session-1"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("deleted session = %v", err)
	}
	if _, err := store.GetMessage(context.Background(), "tenant-a", "event-1"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("deleted event = %v", err)
	}
	if _, err := store.GetReply(context.Background(), "tenant-a", "reply-1", 0); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("deleted reply = %v", err)
	}
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-1", nil); err != nil {
		t.Fatalf("recreate session: %v", err)
	}
	if _, duplicate, err := store.RecordMessage(context.Background(), input); err != nil || duplicate {
		t.Fatalf("reuse external message after delete = duplicate=%v err=%v", duplicate, err)
	}
}

func TestStoreEnqueueReplyRequiresTenantEvent(t *testing.T) {
	store := inmemory.New()
	seedEvent(t, store, "tenant-a", "session-reply", "event-1")

	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{
		TenantID: "tenant-a", ReplyID: "missing-event", EventID: "event-missing", SegmentIndex: 0, SegmentCount: 1,
	}); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing event = %v", err)
	}
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{
		TenantID: "tenant-b", ReplyID: "cross-tenant", EventID: "event-1", SegmentIndex: 0, SegmentCount: 1,
	}); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("cross-tenant event = %v", err)
	}
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{
		TenantID: "tenant-a", ReplyID: "valid", EventID: "event-1", SegmentIndex: 0, SegmentCount: 1,
	}); err != nil {
		t.Fatalf("valid event = %v", err)
	}
}

func TestStoreEnqueueRepliesValidatesAndCommitsAtomically(t *testing.T) {
	store := inmemory.New()
	seedEvent(t, store, "tenant-a", "session-batch", "event-batch")
	ctx := context.Background()
	if _, err := store.EnqueueReplies(ctx, nil); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("empty batch = %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.EnqueueReplies(canceled, []runtimestorage.ReplyOutbox{{TenantID: "tenant-a", ReplyID: "reply", EventID: "event-batch", SegmentIndex: 0, SegmentCount: 1}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled batch = %v", err)
	}
	valid := []runtimestorage.ReplyOutbox{
		{TenantID: "tenant-a", ReplyID: "reply-batch", EventID: "event-batch", SegmentIndex: 0, SegmentCount: 2, Payload: "one"},
		{TenantID: "tenant-a", ReplyID: "reply-batch", EventID: "event-batch", SegmentIndex: 1, SegmentCount: 2, Payload: "two"},
	}
	rows, err := store.EnqueueReplies(ctx, valid)
	if err != nil || len(rows) != 2 {
		t.Fatalf("valid batch = %d err=%v", len(rows), err)
	}
	first := runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-batch", EventID: "event-batch", SegmentIndex: 0, SegmentCount: 2, Payload: "one"}
	if _, err := store.EnqueueReplies(ctx, []runtimestorage.ReplyOutbox{first, {TenantID: "tenant-a", ReplyID: "reply-batch", EventID: "event-batch", SegmentIndex: 1, SegmentCount: 2, Payload: "changed"}}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("conflicting batch = %v", err)
	}
	if _, err := store.EnqueueReplies(ctx, []runtimestorage.ReplyOutbox{{TenantID: "tenant-a", ReplyID: "missing", EventID: "missing", SegmentIndex: 0, SegmentCount: 1}}); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing event batch = %v", err)
	}
}

func TestStoreDeleteSessionValidationAndMissingBranches(t *testing.T) {
	store := inmemory.New()
	if err := store.DeleteSession(context.Background(), "", "session"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid delete = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.DeleteSession(canceled, "tenant-a", "session"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled delete = %v", err)
	}
	if err := store.DeleteSession(context.Background(), "tenant-a", "missing"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing delete = %v", err)
	}
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: "binding", ExternalMessageID: "external", EventID: "event"}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteSession(context.Background(), "tenant-a", "session"); err != nil {
		t.Fatal(err)
	}
}

func TestStoreMethodsRespectCanceledContext(t *testing.T) {
	store := inmemory.New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.GetSession(ctx, "tenant-a", "session"); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetSession = %v", err)
	}
	if _, err := store.CreateSession(ctx, "tenant-a", "session", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateSession = %v", err)
	}
	if _, err := store.UpdateSessionState(ctx, "tenant-a", "session", 1, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("UpdateSessionState = %v", err)
	}
	if err := store.DeleteSession(ctx, "tenant-a", "session"); !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteSession = %v", err)
	}
	if _, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: "binding", ExternalMessageID: "external", EventID: "event"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("RecordMessage = %v", err)
	}
	if _, err := store.GetMessage(ctx, "tenant-a", "event"); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetMessage = %v", err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", SegmentCount: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("EnqueueReply = %v", err)
	}
	if _, err := store.GetReply(ctx, "tenant-a", "reply", 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetReply = %v", err)
	}
	if _, err := store.ClaimReply(ctx, "tenant-a", "reply", 0, "worker", time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("ClaimReply = %v", err)
	}
	if _, err := store.TransitionReply(ctx, runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply", Owner: "worker", From: runtimestorage.ReplyPending, To: runtimestorage.ReplySending}); !errors.Is(err, context.Canceled) {
		t.Fatalf("TransitionReply = %v", err)
	}
}

func TestStoreReplyStateMachineAndFencing(t *testing.T) {
	store := inmemory.New()
	seedEvent(t, store, "tenant-a", "session-reply", "event-1")
	reply, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-1", EventID: "event-1", SegmentIndex: 0, SegmentCount: 1, Payload: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	sending, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: reply.TenantID, ReplyID: reply.ReplyID, SegmentIndex: 0, From: runtimestorage.ReplyPending, To: runtimestorage.ReplySending, Owner: "worker-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: reply.TenantID, ReplyID: reply.ReplyID, SegmentIndex: 0, From: runtimestorage.ReplySending, To: runtimestorage.ReplySent, Owner: "worker-b", FencingToken: sending.FencingToken}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale owner = %v", err)
	}
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: reply.TenantID, ReplyID: reply.ReplyID, SegmentIndex: 0, From: runtimestorage.ReplySending, To: runtimestorage.ReplySent, Owner: "worker-a", FencingToken: sending.FencingToken, ProviderID: "provider-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: reply.TenantID, ReplyID: reply.ReplyID, SegmentIndex: 0, From: runtimestorage.ReplySent, To: runtimestorage.ReplySending, Owner: "worker-a"}); !errors.Is(err, runtimestorage.ErrIllegalTransition) {
		t.Fatalf("illegal transition = %v", err)
	}
}

func TestStoreRecordsReplyReceiptWithinCurrentLease(t *testing.T) {
	store := inmemory.New()
	seedEvent(t, store, "tenant-a", "session-receipt", "event-receipt")
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-receipt", EventID: "event-receipt", SegmentIndex: 0, SegmentCount: 1, Payload: "hello"}); err != nil {
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
	if repeated, err := store.RecordReplyReceipt(context.Background(), runtimestorage.ReplyReceipt{TenantID: claimed.TenantID, ReplyID: claimed.ReplyID, SegmentIndex: claimed.SegmentIndex, Owner: claimed.LeaseOwner, FencingToken: claimed.FencingToken, ProviderID: "provider-1"}); err != nil || repeated.ProviderMessageID != "provider-1" {
		t.Fatalf("repeated receipt = %+v, %v", repeated, err)
	}
	if _, err := store.RecordReplyReceipt(context.Background(), runtimestorage.ReplyReceipt{TenantID: claimed.TenantID, ReplyID: claimed.ReplyID, SegmentIndex: claimed.SegmentIndex, Owner: claimed.LeaseOwner, FencingToken: claimed.FencingToken, ProviderID: "other-provider"}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("conflicting receipt = %v", err)
	}
	if _, err := store.RecordReplyReceipt(context.Background(), runtimestorage.ReplyReceipt{TenantID: "tenant-a", ReplyID: "missing", Owner: "worker-a", FencingToken: 1, ProviderID: "provider-1"}); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing receipt = %v", err)
	}
	if _, err := store.RecordReplyReceipt(context.Background(), runtimestorage.ReplyReceipt{TenantID: "tenant-a", ReplyID: "reply-receipt", Owner: "worker-a", FencingToken: 1}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid receipt = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.RecordReplyReceipt(canceled, runtimestorage.ReplyReceipt{TenantID: claimed.TenantID, ReplyID: claimed.ReplyID, SegmentIndex: claimed.SegmentIndex, Owner: claimed.LeaseOwner, FencingToken: claimed.FencingToken, ProviderID: "provider-1"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled receipt = %v", err)
	}
	retry, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: claimed.TenantID, ReplyID: claimed.ReplyID, SegmentIndex: claimed.SegmentIndex, From: runtimestorage.ReplySending, To: runtimestorage.ReplyRetryable, Owner: claimed.LeaseOwner, FencingToken: claimed.FencingToken})
	if err != nil || retry.ProviderMessageID != "provider-1" {
		t.Fatalf("retry preserves receipt = %+v, %v", retry, err)
	}
}

func TestStoreListReplyCandidatesReturnsTenantRows(t *testing.T) {
	store := inmemory.New()
	seedEvent(t, store, "tenant-a", "candidate-event", "candidate-event")
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "candidate-reply", EventID: "candidate-event", SegmentIndex: 0, SegmentCount: 1, Payload: "payload"}); err != nil {
		t.Fatal(err)
	}
	values, err := store.ListReplyCandidates(context.Background(), "tenant-a")
	if err != nil || len(values) != 1 || values[0].ReplyID != "candidate-reply" {
		t.Fatalf("reply candidates = %+v err=%v", values, err)
	}
	if _, err := store.ListReplyCandidates(context.Background(), ""); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid candidates = %v", err)
	}
}

func TestStoreClaimReplyFencesExpiredWorker(t *testing.T) {
	store := inmemory.New()
	seedEvent(t, store, "tenant-a", "session-claim", "event-1")
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-claim", EventID: "event-1", SegmentIndex: 0, SegmentCount: 1}); err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimReply(context.Background(), "tenant-a", "reply-claim", 0, "worker-a", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	second, err := store.ClaimReply(context.Background(), "tenant-a", "reply-claim", 0, "worker-b", time.Second)
	if err != nil || second.FencingToken <= first.FencingToken || second.LeaseOwner != "worker-b" {
		t.Fatalf("claim = %+v, err=%v", second, err)
	}
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply-claim", SegmentIndex: 0, From: runtimestorage.ReplySending, To: runtimestorage.ReplySent, Owner: "worker-a", FencingToken: first.FencingToken}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale transition = %v", err)
	}
}

func TestStoreMessageReplyReadsAndValidationEdges(t *testing.T) {
	store := inmemory.New()
	if _, err := store.GetMessage(context.Background(), "tenant-a", "missing"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing message = %v", err)
	}
	if _, err := store.GetReply(context.Background(), "tenant-a", "missing", 0); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing reply = %v", err)
	}
	if _, err := store.GetMessage(context.Background(), "", "event"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid message = %v", err)
	}
	if _, err := store.GetReply(context.Background(), "tenant-a", "reply", -1); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid reply = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.GetMessage(canceled, "tenant-a", "event"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled message = %v", err)
	}
	if _, err := store.GetReply(canceled, "tenant-a", "reply", 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled reply = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreCreateAndTransitionValidationEdges(t *testing.T) {
	store := inmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "encode-error", map[string]any{"bad": make(chan int)}); err != nil {
		t.Fatalf("in-memory encode fallback = %v", err)
	}
	if _, err := store.CreateSession(context.Background(), "tenant-a", "", nil); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid session = %v", err)
	}
	if _, err := store.CreateSession(context.Background(), "tenant-a", "encode-error", nil); !errors.Is(err, runtimestorage.ErrDuplicate) {
		t.Fatalf("duplicate session = %v", err)
	}
	if _, err := store.UpdateSessionState(context.Background(), "tenant-a", "missing", 1, nil); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing update = %v", err)
	}
	seedEvent(t, store, "tenant-a", "session-reply", "event")
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", SegmentIndex: 1, SegmentCount: 1}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid segment = %v", err)
	}
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", SegmentIndex: 0, SegmentCount: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", SegmentIndex: 0, SegmentCount: 1}); err != nil {
		t.Fatalf("idempotent enqueue = %v", err)
	}
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply", Owner: "worker", From: runtimestorage.ReplyPending, To: runtimestorage.ReplySent}); !errors.Is(err, runtimestorage.ErrIllegalTransition) {
		t.Fatalf("illegal transition = %v", err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session"}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid message = %v", err)
	}
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", SegmentIndex: 0, SegmentCount: 1, Status: runtimestorage.ReplySent}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid reply status = %v", err)
	}
	if _, err := store.ClaimReply(context.Background(), "tenant-a", "reply", 0, "", time.Second); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid claim owner = %v", err)
	}
	if _, err := store.UpdateSessionState(context.Background(), "tenant-a", "encode-error", 1, map[string]any{"bad": make(chan int)}); err != nil {
		t.Fatalf("in-memory update clone fallback = %v", err)
	}
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "missing", Owner: "worker", From: runtimestorage.ReplyPending, To: runtimestorage.ReplySending}); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing transition = %v", err)
	}
}

func TestStoreTransitionMessageRequiresRunningLease(t *testing.T) {
	store := inmemory.New()
	seedEvent(t, store, "tenant-a", "session-zero-lease", "event-zero-lease")
	if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{
		TenantID: "tenant-a", EventID: "event-zero-lease", From: runtimestorage.EventReceived,
		To: runtimestorage.EventRunning, Owner: "worker",
	}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("zero running lease error = %v", err)
	}
}

func TestStoreEventHistoryAndMessageLifecycle(t *testing.T) {
	store := inmemory.New()
	seedEvent(t, store, "tenant-a", "session-history", "inbound-1")
	payload := []byte("{\"ID\":\"runner-1\"}")
	first, err := store.AppendEventPayload(context.Background(), sessionstorage.EventPayload{TenantID: "tenant-a", SessionID: "session-history", EventID: "runner-1", Payload: payload})
	if err != nil || first.HistorySeq != 1 {
		t.Fatalf("append = %+v err=%v", first, err)
	}
	first.Payload[0] = 'x'
	replay, err := store.AppendEventPayload(context.Background(), sessionstorage.EventPayload{TenantID: "tenant-a", SessionID: "session-history", EventID: "runner-1", Payload: payload})
	if err != nil || string(replay.Payload) != string(payload) {
		t.Fatalf("idempotent append = %+v err=%v", replay, err)
	}
	if _, err := store.AppendEventPayload(context.Background(), sessionstorage.EventPayload{TenantID: "tenant-a", SessionID: "session-history", EventID: "runner-1", Payload: []byte("{ \"ID\": \"runner-1\" }")}); err != nil {
		t.Fatalf("semantic JSON duplicate = %v", err)
	}
	if _, err := store.AppendEventPayload(context.Background(), sessionstorage.EventPayload{TenantID: "tenant-a", SessionID: "session-history", EventID: "runner-1", Payload: []byte("{\"ID\":\"changed\"}")}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("payload conflict = %v", err)
	}
	items, err := store.ListEventPayloads(context.Background(), "tenant-a", "session-history")
	if err != nil || len(items) != 1 || items[0].HistorySeq != 1 {
		t.Fatalf("history = %+v err=%v", items, err)
	}
	event, err := store.GetMessage(context.Background(), "tenant-a", "inbound-1")
	if err != nil {
		t.Fatal(err)
	}
	running, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "worker-a", LeaseDuration: time.Minute})
	if err != nil || running.Status != runtimestorage.EventRunning || running.FencingToken != 1 {
		t.Fatalf("running = %+v err=%v", running, err)
	}
	if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventRunning, To: runtimestorage.EventCompleted, Owner: "worker-b", FencingToken: running.FencingToken}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale message worker = %v", err)
	}
	completed, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventRunning, To: runtimestorage.EventCompleted, Owner: "worker-a", FencingToken: running.FencingToken})
	if err != nil || completed.Status != runtimestorage.EventCompleted {
		t.Fatalf("completed = %+v err=%v", completed, err)
	}
	if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventCompleted, To: runtimestorage.EventReplied, Owner: "worker-a"}); !errors.Is(err, runtimestorage.ErrIllegalTransition) {
		t.Fatalf("illegal message transition = %v", err)
	}
}

func TestStoreExpiredRunningMessageCanBeTakenForReconciliation(t *testing.T) {
	store := inmemory.New()
	seedEvent(t, store, "tenant-a", "session-expired", "expired-1")
	event, err := store.GetMessage(context.Background(), "tenant-a", "expired-1")
	if err != nil {
		t.Fatal(err)
	}
	running, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "old-worker", LeaseDuration: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	reconciling, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventRunning, To: runtimestorage.EventExecutionReconciling, Owner: "new-worker"})
	if err != nil || reconciling.Status != runtimestorage.EventExecutionReconciling || reconciling.LeaseOwner != "" || reconciling.LeaseExpiresAt != nil || reconciling.FencingToken != running.FencingToken+1 {
		t.Fatalf("expired recovery = %+v err=%v", reconciling, err)
	}
}
