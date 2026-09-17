package delivery

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	memory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
)

type adapterStub struct {
	calls            int
	err              error
	clientRequestIDs []string
	requests         []channel.DeliveryRequest
	delay            time.Duration
	maxTextBytes     int
	errorsByCall     map[int]error
	result           *channel.DeliveryResult
}

func (*adapterStub) ID() string                { return "fake" }
func (*adapterStub) Run(context.Context) error { return nil }
func (*adapterStub) PublicRoute(context.Context, channel.CallbackRequest) (channel.PublicRouteHint, error) {
	return channel.PublicRouteHint{}, nil
}
func (*adapterStub) Verify(context.Context, channel.CallbackRequest, channel.ScopedVerifierHandle) (channel.VerifiedCallback, channel.VerificationReceipt, error) {
	return channel.VerifiedCallback{}, channel.VerificationReceipt{}, nil
}
func (*adapterStub) Decode(context.Context, channel.VerifiedCallback) ([]channel.ProviderEvent, error) {
	return nil, nil
}
func (s *adapterStub) Deliver(_ context.Context, request channel.DeliveryRequest) (channel.DeliveryResult, error) {
	s.calls++
	s.clientRequestIDs = append(s.clientRequestIDs, request.ClientRequestID)
	s.requests = append(s.requests, request)
	if s.delay > 0 {
		time.Sleep(s.delay)
	}
	if err := s.errorsByCall[s.calls]; err != nil {
		return channel.DeliveryResult{}, err
	}
	if s.err != nil {
		return channel.DeliveryResult{}, s.err
	}
	if s.result != nil {
		return *s.result, nil
	}
	return channel.DeliveryResult{ProviderMessageID: "provider-message", Delivered: true}, nil
}
func (*adapterStub) Capabilities() channel.Capabilities { return channel.Capabilities{Text: true} }
func (s *adapterStub) MaxTextBytes() int                { return s.maxTextBytes }

type reconcilingAdapter struct {
	adapterStub
	result         channel.ReconciliationResult
	reconcileCalls int
}

func (a *reconcilingAdapter) ReconcileDelivery(_ context.Context, request channel.ReconciliationRequest) (channel.ReconciliationResult, error) {
	a.reconcileCalls++
	if request.ClientRequestID == "" {
		return channel.ReconciliationResult{}, errors.New("missing client request id")
	}
	return a.result, nil
}

type resolverStub struct{ adapter channel.Adapter }

func (s resolverStub) ResolveAdapter(context.Context, string, string) (channel.Adapter, error) {
	return s.adapter, nil
}

type versionedResolverStub struct {
	adapter channel.Adapter
	version int64
}

func (s *versionedResolverStub) ResolveAdapter(context.Context, string, string) (channel.Adapter, error) {
	return nil, errors.New("unversioned resolver used")
}
func (s *versionedResolverStub) ResolveVersionedAdapter(_ context.Context, tenantID, bindingID string, version int64) (channel.Adapter, error) {
	if tenantID != "tenant" || bindingID != "binding" {
		return nil, runtime.ErrTenantScope
	}
	s.version = version
	return s.adapter, nil
}

func testReplyEvent(contentRef string) channel.ReplyEvent {
	return channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "r1_reply",
		ConfigVersion: 1, ContentRef: contentRef, Target: channel.DeliveryTarget{Channel: "fake", ExternalAccountID: "account", ExternalMessageID: "message"}, Final: true}
}

func TestDeliveryLedgerPreventsDuplicateProviderEffect(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	if err := store.PutResult(context.Background(), result); err != nil {
		t.Fatal(err)
	}
	adapter := &adapterStub{}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "adapter-1"}
	event := testReplyEvent(result.ResultRef)
	if err := service.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := service.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if adapter.calls != 1 {
		t.Fatalf("provider calls=%d", adapter.calls)
	}
	if len(adapter.clientRequestIDs) != 1 || adapter.clientRequestIDs[0] == "" {
		t.Fatalf("client request IDs=%#v", adapter.clientRequestIDs)
	}
	if len(adapter.requests) != 1 || string(adapter.requests[0].Content) != "done" || adapter.requests[0].ContentDigest != "digest" || adapter.requests[0].Target != testReplyEvent(result.ResultRef).Target {
		t.Fatalf("delivery request=%#v", adapter.requests)
	}
}

func TestLongTextIsDurablySegmentedOnUTF8Boundaries(t *testing.T) {
	store := memory.New()
	content := []byte(strings.Repeat("你", 5)) // 15 bytes; 6-byte limit yields 3 complete-rune segments.
	if err := store.PutResult(context.Background(), messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "whole-content", Content: content, KeyVersion: 1}); err != nil {
		t.Fatal(err)
	}
	adapter := &adapterStub{maxTextBytes: 6}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "adapter-1"}
	event := testReplyEvent("result://request")
	if err := service.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if err := service.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if adapter.calls != 3 || len(adapter.requests) != 3 || len(adapter.clientRequestIDs) != 3 {
		t.Fatalf("calls=%d requests=%#v ids=%#v", adapter.calls, adapter.requests, adapter.clientRequestIDs)
	}
	var rebuilt []byte
	for index, request := range adapter.requests {
		if !utf8.Valid(request.Content) || len(request.Content) > 6 {
			t.Fatalf("segment %d=%q", index, request.Content)
		}
		rebuilt = append(rebuilt, request.Content...)
		record, err := store.GetDelivery(context.Background(), messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: event.DeliveryKey, SegmentNo: index})
		if err != nil || record.State != messaging.DeliverySent || record.Plan.SegmentCount != 3 || record.Plan.FormatVersion != "text-segment-v1" {
			t.Fatalf("record %d=%#v err=%v", index, record, err)
		}
	}
	if !bytes.Equal(rebuilt, content) || adapter.clientRequestIDs[0] == adapter.clientRequestIDs[1] {
		t.Fatalf("rebuilt=%q clientIDs=%v", rebuilt, adapter.clientRequestIDs)
	}
}

func TestSegmentReplayResumesAfterTheFirstUnsentSegment(t *testing.T) {
	store := memory.New()
	content := []byte(strings.Repeat("你", 4))
	if err := store.PutResult(context.Background(), messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "whole-content", Content: content, KeyVersion: 1}); err != nil {
		t.Fatal(err)
	}
	adapter := &adapterStub{maxTextBytes: 6, errorsByCall: map[int]error{2: channel.RetryableDeliveryError{Err: errors.New("rate limited"), RetryAfter: time.Nanosecond}}}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "adapter-1", DefaultRetryDelay: time.Nanosecond}
	event := testReplyEvent("result://request")
	if err := service.Deliver(context.Background(), event); err == nil {
		t.Fatal("first attempt unexpectedly succeeded")
	}
	time.Sleep(time.Millisecond)
	if err := service.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if adapter.calls != 3 || len(adapter.requests) != 3 || !bytes.Equal(adapter.requests[1].Content, adapter.requests[2].Content) {
		t.Fatalf("calls=%d requests=%#v", adapter.calls, adapter.requests)
	}
	first, err := store.GetDelivery(context.Background(), messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: event.DeliveryKey, SegmentNo: 0})
	if err != nil || first.Attempt != 1 || first.State != messaging.DeliverySent {
		t.Fatalf("first=%#v err=%v", first, err)
	}
	second, err := store.GetDelivery(context.Background(), messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: event.DeliveryKey, SegmentNo: 1})
	if err != nil || second.Attempt != 2 || second.State != messaging.DeliverySent {
		t.Fatalf("second=%#v err=%v", second, err)
	}
}

func TestDeliveryUsesFrozenConfigVersionForAdapterResolution(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	if err := store.PutResult(context.Background(), result); err != nil {
		t.Fatal(err)
	}
	adapter := &adapterStub{}
	resolver := &versionedResolverStub{adapter: adapter}
	service := Service{Results: store, Ledger: store, Adapters: resolver, Owner: "adapter-1"}
	event := testReplyEvent(result.ResultRef)
	event.ConfigVersion = 17
	if err := service.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if resolver.version != 17 || adapter.calls != 1 {
		t.Fatalf("version=%d calls=%d", resolver.version, adapter.calls)
	}
}

func TestExpiredOwnerReconcilesDeliveredWithoutResend(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	key := messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_reply", SegmentNo: 0}
	plan := messaging.DeliveryPlan{RendererVersion: "terminal-text-v1", FormatVersion: "text-v1", ContentDigest: result.ContentDigest, SegmentCount: 1}
	if _, acquired, err := store.ClaimDelivery(context.Background(), key, plan, messaging.DeliveryClaim{Owner: "dead-owner", TTL: time.Nanosecond}); err != nil || !acquired {
		t.Fatalf("preclaim acquired=%t err=%v", acquired, err)
	}
	time.Sleep(time.Millisecond)
	adapter := &reconcilingAdapter{result: channel.ReconciliationResult{Status: channel.ReconciliationDelivered, ProviderMessageID: "provider-existing"}}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "new-owner"}
	event := testReplyEvent(result.ResultRef)
	if err := service.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	record, err := store.GetDelivery(context.Background(), key)
	if err != nil || record.State != messaging.DeliverySent || record.ProviderMessageID != "provider-existing" || adapter.calls != 0 || adapter.reconcileCalls != 1 {
		t.Fatalf("record=%#v calls=%d reconcile=%d err=%v", record, adapter.calls, adapter.reconcileCalls, err)
	}
}

func TestExpiredOwnerReconcilesNotDeliveredThenRetriesSameRequestID(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	key := messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_reply", SegmentNo: 0}
	plan := messaging.DeliveryPlan{RendererVersion: "terminal-text-v1", FormatVersion: "text-v1", ContentDigest: result.ContentDigest, SegmentCount: 1}
	claimed, acquired, err := store.ClaimDelivery(context.Background(), key, plan, messaging.DeliveryClaim{Owner: "dead-owner", TTL: time.Nanosecond})
	if err != nil || !acquired {
		t.Fatalf("preclaim=%#v acquired=%t err=%v", claimed, acquired, err)
	}
	time.Sleep(time.Millisecond)
	adapter := &reconcilingAdapter{result: channel.ReconciliationResult{Status: channel.ReconciliationNotDelivered}}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "new-owner"}
	event := testReplyEvent(result.ResultRef)
	var deferred DeferredError
	if err := service.Deliver(context.Background(), event); !errors.As(err, &deferred) {
		t.Fatalf("reconcile err=%v", err)
	}
	if err := service.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if adapter.calls != 1 || len(adapter.clientRequestIDs) != 1 || adapter.clientRequestIDs[0] != claimed.ClientRequestID {
		t.Fatalf("calls=%d IDs=%#v claimed=%s", adapter.calls, adapter.clientRequestIDs, claimed.ClientRequestID)
	}
}

func TestUnknownReconciliationStopsAtMaxAttempts(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	key := messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_reply", SegmentNo: 0}
	plan := messaging.DeliveryPlan{RendererVersion: "terminal-text-v1", FormatVersion: "text-v1", ContentDigest: result.ContentDigest, SegmentCount: 1}
	_, _, _ = store.ClaimDelivery(context.Background(), key, plan, messaging.DeliveryClaim{Owner: "dead-owner", TTL: time.Nanosecond})
	time.Sleep(time.Millisecond)
	adapter := &reconcilingAdapter{result: channel.ReconciliationResult{Status: channel.ReconciliationUnknown}}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "new-owner", MaxReconcileAttempts: 2, DefaultRetryDelay: time.Nanosecond}
	event := testReplyEvent(result.ResultRef)
	var deferred DeferredError
	if err := service.Deliver(context.Background(), event); !errors.As(err, &deferred) {
		t.Fatalf("first reconcile err=%v", err)
	}
	time.Sleep(time.Millisecond)
	var terminal TerminalError
	if err := service.Deliver(context.Background(), event); !errors.As(err, &terminal) {
		t.Fatalf("second reconcile err=%v", err)
	}
	record, _ := store.GetDelivery(context.Background(), key)
	if record.State != messaging.DeliveryFailed || record.LastErrorClass != "reconcile_exhausted" || adapter.reconcileCalls != 2 {
		t.Fatalf("record=%#v reconcile calls=%d", record, adapter.reconcileCalls)
	}
}

func TestNonReconcilableAmbiguousDeliveryReachesFailedTerminal(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	key := messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_reply", SegmentNo: 0}
	plan := messaging.DeliveryPlan{RendererVersion: "terminal-text-v1", FormatVersion: "text-v1", ContentDigest: result.ContentDigest, SegmentCount: 1}
	_, _, _ = store.ClaimDelivery(context.Background(), key, plan, messaging.DeliveryClaim{Owner: "dead-owner", TTL: time.Nanosecond})
	time.Sleep(time.Millisecond)
	// adapterStub does not implement DeliveryReconciler.
	adapter := &adapterStub{}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "new-owner", MaxReconcileAttempts: 2, DefaultRetryDelay: time.Nanosecond}
	event := testReplyEvent(result.ResultRef)
	var deferred DeferredError
	if err := service.Deliver(context.Background(), event); !errors.As(err, &deferred) {
		t.Fatalf("first reconcile err=%v", err)
	}
	time.Sleep(time.Millisecond)
	var terminal TerminalError
	if err := service.Deliver(context.Background(), event); !errors.As(err, &terminal) {
		t.Fatalf("expected exhausted terminal error, got %v", err)
	}
	record, _ := store.GetDelivery(context.Background(), key)
	if record.State != messaging.DeliveryFailed || record.LastErrorClass != "reconcile_exhausted" || adapter.calls != 0 {
		t.Fatalf("record=%#v provider calls=%d", record, adapter.calls)
	}
}

func TestAmbiguousDeliveryIsNotBlindlyRetried(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	adapter := &adapterStub{err: AmbiguousError{Err: errors.New("response lost")}}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "adapter-1"}
	event := testReplyEvent(result.ResultRef)
	if err := service.Deliver(context.Background(), event); err == nil {
		t.Fatal("expected ambiguous delivery error")
	}
	if err := service.Deliver(context.Background(), event); err == nil {
		t.Fatal("expected unresolved ambiguous state")
	}
	if adapter.calls != 1 {
		t.Fatalf("provider calls=%d", adapter.calls)
	}
}

func TestRetryWaitIsDeferredInsteadOfReportedAsDelivered(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	adapter := &adapterStub{err: RetryAfterError{Err: errors.New("rate limited")}}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "adapter-1", DefaultRetryDelay: time.Hour}
	event := testReplyEvent(result.ResultRef)
	if err := service.Deliver(context.Background(), event); err == nil {
		t.Fatal("expected first provider error")
	}
	var deferred DeferredError
	if err := service.Deliver(context.Background(), event); !errors.As(err, &deferred) {
		t.Fatalf("expected deferred retry, got %v", err)
	}
	if adapter.calls != 1 {
		t.Fatalf("provider calls=%d", adapter.calls)
	}
}

func TestPermanentFailureReachesFailedTerminal(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	adapter := &adapterStub{err: PermanentError{Err: errors.New("recipient blocked"), Class: "recipient_blocked"}}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "adapter-1"}
	event := testReplyEvent(result.ResultRef)
	var terminal TerminalError
	if err := service.Deliver(context.Background(), event); !errors.As(err, &terminal) {
		t.Fatalf("expected terminal error, got %v", err)
	}
	record, err := store.GetDelivery(context.Background(), messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_reply", SegmentNo: 0})
	if err != nil || record.State != messaging.DeliveryFailed || record.LastErrorClass != "recipient_blocked" {
		t.Fatalf("record=%#v err=%v", record, err)
	}
	if err := service.Deliver(context.Background(), event); err != nil || adapter.calls != 1 {
		t.Fatalf("terminal replay calls=%d err=%v", adapter.calls, err)
	}
}

func TestRetryableFailureStopsAtMaxAttempts(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	adapter := &adapterStub{err: errors.New("temporary")}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "adapter-1", MaxAttempts: 2, DefaultRetryDelay: time.Nanosecond}
	event := testReplyEvent(result.ResultRef)
	if err := service.Deliver(context.Background(), event); err == nil {
		t.Fatal("expected first retryable error")
	}
	time.Sleep(time.Millisecond)
	var terminal TerminalError
	if err := service.Deliver(context.Background(), event); !errors.As(err, &terminal) {
		t.Fatalf("expected exhausted terminal error, got %v", err)
	}
	record, _ := store.GetDelivery(context.Background(), messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_reply", SegmentNo: 0})
	if record.State != messaging.DeliveryFailed || record.LastErrorClass != "retry_exhausted" || adapter.calls != 2 {
		t.Fatalf("record=%#v calls=%d", record, adapter.calls)
	}
}

func TestLongProviderCallRenewsSendingClaim(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
	_ = store.PutResult(context.Background(), result)
	adapter := &adapterStub{delay: 60 * time.Millisecond}
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "adapter-1", ClaimTTL: 20 * time.Millisecond, ClaimRenewInterval: 5 * time.Millisecond}
	event := testReplyEvent(result.ResultRef)
	if err := service.Deliver(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	record, _ := store.GetDelivery(context.Background(), messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_reply", SegmentNo: 0})
	if record.State != messaging.DeliverySent || record.Version < 4 {
		t.Fatalf("record=%#v", record)
	}
}

func TestResultReferenceMismatchUsesVersionError(t *testing.T) {
	store := memory.New()
	_ = store.PutResult(context.Background(), messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://right", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1})
	service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: &adapterStub{}}, Owner: "adapter-1"}
	event := testReplyEvent("result://stale")
	if err := service.Deliver(context.Background(), event); !errors.Is(err, runtime.ErrVersionMismatch) {
		t.Fatalf("err=%v", err)
	}
}

func TestDeliveryRejectsInvalidEventBeforeAnyStorageAccess(t *testing.T) {
	service := Service{Results: memory.New(), Ledger: memory.New(), Adapters: resolverStub{adapter: &adapterStub{}}, Owner: "adapter-1"}
	if err := service.Deliver(context.Background(), channel.ReplyEvent{}); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("Deliver error=%v", err)
	}
}

func TestDeliveryDoesNotMarkProviderResponsesWithoutDeliveryEvidenceAsSent(t *testing.T) {
	for name, result := range map[string]channel.DeliveryResult{
		"not-delivered":               {Delivered: false},
		"missing-provider-message-id": {Delivered: true},
	} {
		t.Run(name, func(t *testing.T) {
			store := memory.New()
			resultRecord := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1}
			if err := store.PutResult(context.Background(), resultRecord); err != nil {
				t.Fatal(err)
			}
			adapter := &adapterStub{result: &result}
			service := Service{Results: store, Ledger: store, Adapters: resolverStub{adapter: adapter}, Owner: "adapter-1", DefaultRetryDelay: time.Hour}
			err := service.Deliver(context.Background(), testReplyEvent(resultRecord.ResultRef))
			if err == nil {
				t.Fatal("expected delivery failure")
			}
			record, getErr := store.GetDelivery(context.Background(), messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_reply", SegmentNo: 0})
			if getErr != nil {
				t.Fatal(getErr)
			}
			if name == "not-delivered" && record.State != messaging.DeliveryRetryWait {
				t.Fatalf("record=%#v, want retry wait", record)
			}
			if name == "missing-provider-message-id" && record.State != messaging.DeliveryAmbiguous {
				t.Fatalf("record=%#v, want ambiguous", record)
			}
		})
	}
}

func TestDeliveryUtilityPolicies(t *testing.T) {
	service := Service{}
	if got := (DeferredError{}).Error(); got != "reply delivery is deferred" {
		t.Fatalf("DeferredError=%q", got)
	}
	if got := (TerminalError{}).Error(); got != "reply delivery permanently failed" {
		t.Fatalf("TerminalError=%q", got)
	}
	cause := errors.New("provider failed")
	terminal := TerminalError{Err: cause}
	if terminal.Error() != "provider failed" || !errors.Is(terminal, cause) {
		t.Fatalf("terminal=%v", terminal)
	}
	if service.rendererVersion() != "terminal-text-v1" || service.formatVersion(messaging.ContentTypeCard, 1) != "card-v1" ||
		service.formatVersion("image/png", 1) != "image-v1" || service.formatVersion(messaging.ContentTypeText, 2) != "text-segment-v1" ||
		service.formatVersion(messaging.ContentTypeText, 1) != "text-v1" {
		t.Fatal("unexpected default renderer or format policy")
	}
	service = Service{RendererVersion: "renderer-v2", FormatVersion: "format-v2", DefaultRetryDelay: 2 * time.Second, MaxRetryDelay: 5 * time.Second}
	if service.rendererVersion() != "renderer-v2" || service.formatVersion(messaging.ContentTypeText, 1) != "format-v2" {
		t.Fatal("explicit renderer policy ignored")
	}
	for _, test := range []struct {
		attempt   int
		requested time.Duration
		want      time.Duration
	}{
		{attempt: 1, want: 2 * time.Second},
		{attempt: 2, want: 4 * time.Second},
		{attempt: 3, want: 5 * time.Second},
		{attempt: 1, requested: 4 * time.Second, want: 4 * time.Second},
		{attempt: 1, requested: 10 * time.Second, want: 5 * time.Second},
	} {
		if got := service.backoff(test.attempt, test.requested); got != test.want {
			t.Fatalf("backoff(%d, %s)=%s want %s", test.attempt, test.requested, got, test.want)
		}
	}
	if got := normalizeErrorClass("tenant blocked"); got != "permanent" {
		t.Fatalf("normalized class=%q", got)
	}
}
