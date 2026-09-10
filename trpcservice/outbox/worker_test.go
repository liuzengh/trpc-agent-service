package outbox_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
)

type providerStub struct {
	deliverID       string
	deliverErr      error
	reconcileStatus outbox.DeliveryStatus
	reconcileID     string
	reconcileErr    error
	deliveries      int
	reconciliations int
	segments        []int
	deliverFn       func(runtimestorage.ReplyOutbox) error
}

func (p *providerStub) Deliver(_ context.Context, value runtimestorage.ReplyOutbox) (string, error) {
	p.deliveries++
	p.segments = append(p.segments, value.SegmentIndex)
	if p.deliverFn != nil {
		return p.deliverID, p.deliverFn(value)
	}
	return p.deliverID, p.deliverErr
}
func (p *providerStub) Reconcile(context.Context, runtimestorage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	p.reconciliations++
	return p.reconcileStatus, p.reconcileID, p.reconcileErr
}

func seedReply(t *testing.T, store *inmemory.Store, tenant, event, reply string) {
	t.Helper()
	if _, err := store.CreateSession(context.Background(), tenant, "session-"+event, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: tenant, SessionID: "session-" + event, BindingID: "binding-" + event, ExternalMessageID: "external-" + event, EventID: event}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: tenant, ReplyID: reply, EventID: event, SegmentIndex: 0, SegmentCount: 1, Payload: "payload"}); err != nil {
		t.Fatal(err)
	}
}

type reverseCandidateStore struct{ *inmemory.Store }

func failFirstSegmentOnce() func(runtimestorage.ReplyOutbox) error {
	first := true
	return func(value runtimestorage.ReplyOutbox) error {
		if value.SegmentIndex == 0 && first {
			first = false
			return &outbox.DeliveryError{Class: "unavailable", Retryable: true}
		}
		return nil
	}
}

func (s reverseCandidateStore) ListReplyCandidates(ctx context.Context, tenantID string) ([]runtimestorage.ReplyOutbox, error) {
	values, err := s.Store.ListReplyCandidates(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	sort.Slice(values, func(i, j int) bool {
		if values[i].ReplyID == values[j].ReplyID {
			return values[i].SegmentIndex > values[j].SegmentIndex
		}
		return values[i].ReplyID > values[j].ReplyID
	})
	return values, nil
}

func TestWorkerDeliversAndFencesProviderReceipt(t *testing.T) {
	store := inmemory.New()
	seedReply(t, store, "tenant-a", "event-1", "reply-1")
	event, err := store.GetMessage(context.Background(), "tenant-a", "event-1")
	if err != nil {
		t.Fatal(err)
	}
	running, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "runner", LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventRunning, To: runtimestorage.EventCompleted, Owner: "runner", FencingToken: running.FencingToken}); err != nil {
		t.Fatal(err)
	}
	provider := &providerStub{deliverID: "provider-1"}
	worker, err := outbox.New(outbox.Config{Store: store, MessageStore: store, Provider: provider, TenantID: "tenant-a", Owner: "worker-a", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	processed, err := worker.RunOnce(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("run = %d err=%v", processed, err)
	}
	value, err := store.GetReply(context.Background(), "tenant-a", "reply-1", 0)
	if err != nil || value.Status != runtimestorage.ReplySent || value.ProviderMessageID != "provider-1" || provider.deliveries != 1 {
		t.Fatalf("reply = %+v deliveries=%d err=%v", value, provider.deliveries, err)
	}
	event, err = store.GetMessage(context.Background(), "tenant-a", "event-1")
	if err != nil || event.Status != runtimestorage.EventReplied {
		t.Fatalf("event lifecycle = %+v err=%v", event, err)
	}
}

func TestWorkerDeliversReplySegmentsInOrder(t *testing.T) {
	base := inmemory.New()
	seedReply(t, base, "tenant-a", "event-segments", "reply-segments")
	if _, err := base.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", EventID: "event-segments", ReplyID: "reply-segments", SegmentIndex: 1, SegmentCount: 2, Payload: "second"}); err != nil {
		t.Fatal(err)
	}
	provider := &providerStub{deliverID: "provider-segments"}
	worker, err := outbox.New(outbox.Config{Store: reverseCandidateStore{Store: base}, MessageStore: base, Provider: provider, TenantID: "tenant-a", Owner: "worker-a", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	processed, err := worker.RunOnce(context.Background())
	if err != nil || processed != 2 {
		t.Fatalf("run = %d err=%v", processed, err)
	}
	if len(provider.segments) != 2 || provider.segments[0] != 0 || provider.segments[1] != 1 {
		t.Fatalf("delivered segments = %v", provider.segments)
	}
}

func TestWorkerWaitsForRetryablePrecedingSegment(t *testing.T) {
	store := inmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-retry", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session-retry", BindingID: "binding-retry", ExternalMessageID: "external-retry", EventID: "event-retry"}); err != nil {
		t.Fatal(err)
	}
	for _, value := range []runtimestorage.ReplyOutbox{
		{TenantID: "tenant-a", EventID: "event-retry", ReplyID: "reply-retry", SegmentIndex: 0, SegmentCount: 2, Payload: "first"},
		{TenantID: "tenant-a", EventID: "event-retry", ReplyID: "reply-retry", SegmentIndex: 1, SegmentCount: 2, Payload: "second"},
	} {
		if _, err := store.EnqueueReply(context.Background(), value); err != nil {
			t.Fatal(err)
		}
	}
	provider := &providerStub{deliverID: "provider-retry", deliverFn: failFirstSegmentOnce()}
	worker, err := outbox.New(outbox.Config{Store: store, MessageStore: store, Provider: provider, TenantID: "tenant-a", Owner: "worker-a", LeaseDuration: time.Second, BackoffBase: time.Nanosecond, BackoffMax: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	processed, err := worker.RunOnce(context.Background())
	if err != nil || processed != 1 || len(provider.segments) != 1 || provider.segments[0] != 0 {
		t.Fatalf("first run = %d err=%v segments=%v", processed, err, provider.segments)
	}
	firstSegment, err := store.GetReply(context.Background(), "tenant-a", "reply-retry", 0)
	if err != nil || firstSegment.Status != runtimestorage.ReplyRetryable {
		t.Fatalf("first segment = %+v err=%v", firstSegment, err)
	}
	secondSegment, err := store.GetReply(context.Background(), "tenant-a", "reply-retry", 1)
	if err != nil || secondSegment.Status != runtimestorage.ReplyPending {
		t.Fatalf("second segment = %+v err=%v", secondSegment, err)
	}
	time.Sleep(time.Millisecond)
	processed, err = worker.RunOnce(context.Background())
	if err != nil || processed != 2 || len(provider.segments) != 3 || provider.segments[1] != 0 || provider.segments[2] != 1 {
		t.Fatalf("second run = %d err=%v segments=%v", processed, err, provider.segments)
	}
}

func TestWorkerDeadLettersSegmentsAfterADeadLetteredPredecessor(t *testing.T) {
	store := inmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-dead", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session-dead", BindingID: "binding-dead", ExternalMessageID: "external-dead", EventID: "event-dead"}); err != nil {
		t.Fatal(err)
	}
	for _, value := range []runtimestorage.ReplyOutbox{
		{TenantID: "tenant-a", EventID: "event-dead", ReplyID: "reply-dead", SegmentIndex: 0, SegmentCount: 2, Payload: "first"},
		{TenantID: "tenant-a", EventID: "event-dead", ReplyID: "reply-dead", SegmentIndex: 1, SegmentCount: 2, Payload: "second"},
	} {
		if _, err := store.EnqueueReply(context.Background(), value); err != nil {
			t.Fatal(err)
		}
	}
	provider := &providerStub{deliverID: "provider-dead", deliverFn: func(value runtimestorage.ReplyOutbox) error {
		if value.SegmentIndex == 0 {
			return &outbox.DeliveryError{Class: "rejected", Retryable: false}
		}
		t.Fatal("worker delivered a segment after its predecessor dead-lettered")
		return nil
	}}
	worker, err := outbox.New(outbox.Config{Store: store, MessageStore: store, Provider: provider, TenantID: "tenant-a", Owner: "worker-a", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	processed, err := worker.RunOnce(context.Background())
	if err != nil || processed != 2 || len(provider.segments) != 1 || provider.segments[0] != 0 {
		t.Fatalf("run = %d err=%v segments=%v", processed, err, provider.segments)
	}
	for index := 0; index < 2; index++ {
		value, err := store.GetReply(context.Background(), "tenant-a", "reply-dead", index)
		if err != nil || value.Status != runtimestorage.ReplyDeadLetter {
			t.Fatalf("segment %d = %+v err=%v", index, value, err)
		}
	}
}

func TestWorkerRetriesThenDeadLettersStableProviderErrors(t *testing.T) {
	store := inmemory.New()
	seedReply(t, store, "tenant-a", "event-2", "reply-2")
	provider := &providerStub{deliverErr: &outbox.DeliveryError{Class: "rate_limited", Retryable: true}}
	worker, err := outbox.New(outbox.Config{Store: store, MessageStore: store, Provider: provider, TenantID: "tenant-a", Owner: "worker-a", LeaseDuration: time.Second, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	value, err := store.GetReply(context.Background(), "tenant-a", "reply-2", 0)
	if err != nil || value.Status != runtimestorage.ReplyDeadLetter || value.LastErrorClass != "rate_limited" {
		t.Fatalf("dead letter = %+v err=%v", value, err)
	}
}

func TestWorkerReconcilesExpiredSendingBeforeRedelivery(t *testing.T) {
	store := inmemory.New()
	seedReply(t, store, "tenant-a", "event-3", "reply-3")
	event, err := store.GetMessage(context.Background(), "tenant-a", "event-3")
	if err != nil {
		t.Fatal(err)
	}
	running, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "runner", LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventRunning, To: runtimestorage.EventCompleted, Owner: "runner", FencingToken: running.FencingToken}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimReply(context.Background(), "tenant-a", "reply-3", 0, "old-worker", time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	provider := &providerStub{reconcileStatus: outbox.DeliveryAccepted, reconcileID: "provider-recovered"}
	worker, err := outbox.New(outbox.Config{Store: store, MessageStore: store, Provider: provider, TenantID: "tenant-a", Owner: "new-worker", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	value, err := store.GetReply(context.Background(), "tenant-a", "reply-3", 0)
	if err != nil || value.Status != runtimestorage.ReplySent || value.ProviderMessageID != "provider-recovered" || provider.reconciliations != 1 || provider.deliveries != 0 {
		t.Fatalf("reconciled = %+v provider=%+v err=%v", value, provider, err)
	}
	updated, err := store.GetMessage(context.Background(), "tenant-a", event.EventID)
	if err != nil || updated.Status != runtimestorage.EventReplied {
		t.Fatalf("reconciled event status = %+v err=%v", updated, err)
	}
}

func TestWorkerValidationAndCancellation(t *testing.T) {
	if _, err := outbox.New(outbox.Config{}); !errors.Is(err, outbox.ErrInvalid) {
		t.Fatalf("invalid worker = %v", err)
	}
	store := inmemory.New()
	worker, err := outbox.New(outbox.Config{Store: store, MessageStore: store, Provider: &providerStub{}, TenantID: "tenant-a", Owner: "worker", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := worker.RunOnce(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled run = %v", err)
	}
}

func TestMaterializerSegmentsIdempotently(t *testing.T) {
	store := inmemory.New()
	seedReply(t, store, "tenant-a", "event-materialize", "unused")
	m, err := outbox.NewMaterializer(outbox.MaterializerConfig{BatchStore: store, SegmentSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	count, err := m.Materialize(context.Background(), outbox.MaterializeInput{TenantID: "tenant-a", EventID: "event-materialize", ReplyID: "reply-materialize", Payload: "abcdef"})
	if err != nil || count != 2 {
		t.Fatalf("materialize = %d err=%v", count, err)
	}
	if count, err = m.Materialize(context.Background(), outbox.MaterializeInput{TenantID: "tenant-a", EventID: "event-materialize", ReplyID: "reply-materialize", Payload: "abcdef"}); err != nil || count != 2 {
		t.Fatalf("idempotent materialize = %d err=%v", count, err)
	}
	if _, err := m.Materialize(context.Background(), outbox.MaterializeInput{TenantID: "tenant-a", EventID: "event-materialize", ReplyID: "reply-materialize", Payload: "xyz"}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("materialization conflict = %v", err)
	}
	values, err := store.ListReplyCandidates(context.Background(), "tenant-a")
	if err != nil || len(values) != 3 {
		t.Fatalf("materialized rows = %d err=%v", len(values), err)
	}
}

func TestMaterializerWritesIdempotentStructuredMediaReply(t *testing.T) {
	store := inmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "media-session", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "media-event", SessionID: "media-session", BindingID: "media-binding", ExternalMessageID: "media-message", ReplyTarget: runtimestorage.ReplyTarget{BindingID: "media-binding", ConversationKind: "direct", ReceiverID: "user-a"}}); err != nil {
		t.Fatal(err)
	}
	data := []byte("png")
	digest := sha256.Sum256(data)
	reference := attachment.Reference{ID: "tool-image", Kind: attachment.KindImage, MIMEType: "image/png", Name: "test.png", Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:]), Provider: "tool", ProviderID: "send_test_image"}
	materializer, err := outbox.NewMaterializer(outbox.MaterializerConfig{BatchStore: store, SegmentSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	input := outbox.MaterializeInput{
		TenantID: "tenant-a", EventID: "media-event", ReplyID: "media-reply", RequestID: "media-request", TraceID: "media-trace",
		Segments:    []outbox.ReplySegment{{Kind: runtimestorage.ReplyKindImage, Payload: "caption", Attachment: reference, Fallback: "[image attachment: test.png]"}},
		ReplyTarget: runtimestorage.ReplyTarget{BindingID: "media-binding", ConversationKind: "direct", ReceiverID: "user-a"},
	}
	if count, err := materializer.Materialize(context.Background(), input); err != nil || count != 1 {
		t.Fatalf("media materialization = %d, %v", count, err)
	}
	if count, err := materializer.Materialize(context.Background(), input); err != nil || count != 1 {
		t.Fatalf("idempotent media materialization = %d, %v", count, err)
	}
	reply, err := store.GetReply(context.Background(), "tenant-a", "media-reply", 0)
	if err != nil || reply.Kind != runtimestorage.ReplyKindImage || reply.Attachment != reference || reply.Fallback != "[image attachment: test.png]" || reply.ReplyTarget.ReceiverID != "user-a" {
		t.Fatalf("structured media reply = %+v, err=%v", reply, err)
	}
	correlation, err := store.GetReplyCorrelation(context.Background(), "tenant-a", "media-event")
	if err != nil || correlation.RequestID != "media-request" || correlation.TraceID != "media-trace" {
		t.Fatalf("media correlation = %+v, err=%v", correlation, err)
	}
	input.Payload = "text"
	if _, err := materializer.Materialize(context.Background(), input); !errors.Is(err, outbox.ErrInvalid) {
		t.Fatalf("mixed text and structured media = %v", err)
	}
}

func TestMaterializerDoesNotExposePrefixWhenAnySegmentConflicts(t *testing.T) {
	store := inmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", EventID: "event", BindingID: "binding", ExternalMessageID: "external"}); err != nil {
		t.Fatal(err)
	}
	// Segment one represents an incompatible prior attempt. A sequential write
	// would persist segment zero before discovering this conflict.
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", EventID: "event", ReplyID: "reply", SegmentIndex: 1, SegmentCount: 2, Payload: "other"}); err != nil {
		t.Fatal(err)
	}
	materializer, err := outbox.NewMaterializer(outbox.MaterializerConfig{BatchStore: store, SegmentSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := materializer.Materialize(context.Background(), outbox.MaterializeInput{TenantID: "tenant-a", EventID: "event", ReplyID: "reply", Payload: "abcdef"}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("materialization conflict = %v", err)
	}
	if _, err := store.GetReply(context.Background(), "tenant-a", "reply", 0); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("partial prefix was exposed: %v", err)
	}
}

func TestWorkerRunStopsOnCancellationAndRejectsConcurrentRun(t *testing.T) {
	store := inmemory.New()
	provider := &providerStub{}
	worker, err := outbox.New(outbox.Config{Store: store, MessageStore: store, Provider: provider, TenantID: "tenant-a", Owner: "worker", LeaseDuration: time.Second, BackoffBase: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx, time.Millisecond) }()
	time.Sleep(2 * time.Millisecond)
	if err := worker.Run(ctx, time.Millisecond); !errors.Is(err, outbox.ErrAlreadyRunning) {
		t.Fatalf("concurrent run = %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run cancellation = %v", err)
	}
	if err := worker.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerStartReservesLifecycleBeforeReturning(t *testing.T) {
	worker, err := outbox.New(outbox.Config{Store: inmemory.New(), MessageStore: inmemory.New(), Provider: &providerStub{}, TenantID: "tenant-a", Owner: "worker", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(context.Background(), time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := worker.Run(context.Background(), time.Hour); !errors.Is(err, outbox.ErrAlreadyRunning) {
		t.Fatalf("concurrent run = %v", err)
	}
	if err := worker.Close(); err != nil {
		t.Fatal(err)
	}
}
