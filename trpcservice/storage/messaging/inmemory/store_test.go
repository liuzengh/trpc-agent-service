package inmemory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
)

func TestConcurrentClaimReturnsOneStableRequest(t *testing.T) {
	store := New()
	key := messaging.InboxKey{TenantID: "t", Channel: "fake", ExternalAccountID: "account", ExternalMessageID: "message"}
	const callers = 50
	results := make(chan string, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			record, err := store.ClaimInbox(context.Background(), messaging.ClaimInboxRequest{InboxKey: key, AgentAppID: "app", SessionID: "session", PayloadDigest: "digest", KeyVersion: 1, InitialState: messaging.InboxDispatchPending})
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			results <- record.RequestID
		}()
	}
	wg.Wait()
	close(results)
	want, _ := messaging.StableInboxIdentity(key)
	for requestID := range results {
		if requestID != want {
			t.Fatalf("request=%q", requestID)
		}
	}
}

func TestDeliveryLedgerClaimFinishAndIdempotentReplay(t *testing.T) {
	store := New()
	key := messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_reply", SegmentNo: 0}
	plan := messaging.DeliveryPlan{RendererVersion: "renderer-v1", FormatVersion: "text-v1", ContentDigest: "digest", SegmentCount: 1}
	claim := messaging.DeliveryClaim{Owner: "adapter-1", TTL: time.Hour}
	claimed, acquired, err := store.ClaimDelivery(context.Background(), key, plan, claim)
	if err != nil || !acquired || claimed.State != messaging.DeliverySending || claimed.Attempt != 1 {
		t.Fatalf("claimed=%#v acquired=%t err=%v", claimed, acquired, err)
	}
	if duplicate, acquired, err := store.ClaimDelivery(context.Background(), key, plan, claim); err != nil || acquired || duplicate.Version != claimed.Version {
		t.Fatalf("duplicate=%#v acquired=%t err=%v", duplicate, acquired, err)
	}
	claimed.State = messaging.DeliverySent
	claimed.ProviderMessageID = "provider-message"
	sent, err := store.FinishDelivery(context.Background(), claimed, claimed.Version)
	if err != nil || sent.State != messaging.DeliverySent {
		t.Fatalf("sent=%#v err=%v", sent, err)
	}
	if replay, acquired, err := store.ClaimDelivery(context.Background(), key, plan, claim); err != nil || acquired || replay.State != messaging.DeliverySent {
		t.Fatalf("replay=%#v acquired=%t err=%v", replay, acquired, err)
	}
}

func TestDeliveryLedgerRetryWaitHonorsNotBefore(t *testing.T) {
	store := New()
	key := messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_retry", SegmentNo: 0}
	plan := messaging.DeliveryPlan{RendererVersion: "renderer-v1", FormatVersion: "text-v1", ContentDigest: "digest", SegmentCount: 1}
	claim := messaging.DeliveryClaim{Owner: "adapter-1", TTL: time.Hour}
	claimed, _, _ := store.ClaimDelivery(context.Background(), key, plan, claim)
	claimed.State = messaging.DeliveryRetryWait
	claimed.NotBefore = time.Now().Add(time.Hour)
	retry, err := store.FinishDelivery(context.Background(), claimed, claimed.Version)
	if err != nil {
		t.Fatal(err)
	}
	if replay, acquired, err := store.ClaimDelivery(context.Background(), key, plan, claim); err != nil || acquired || replay.Version != retry.Version {
		t.Fatalf("replay=%#v acquired=%t err=%v", replay, acquired, err)
	}
}

func TestDeliveryLedgerExpiredSendingBecomesAmbiguous(t *testing.T) {
	store := New()
	key := messaging.DeliveryKey{TenantID: "tenant", DeliveryKey: "r1_crash", SegmentNo: 0}
	plan := messaging.DeliveryPlan{RendererVersion: "renderer-v1", FormatVersion: "text-v1", ContentDigest: "digest", SegmentCount: 1}
	claimed, acquired, err := store.ClaimDelivery(context.Background(), key, plan, messaging.DeliveryClaim{Owner: "dead-owner", TTL: time.Nanosecond})
	if err != nil || !acquired {
		t.Fatalf("claimed=%#v acquired=%t err=%v", claimed, acquired, err)
	}
	time.Sleep(time.Millisecond)
	recovered, acquired, err := store.ClaimDelivery(context.Background(), key, plan, messaging.DeliveryClaim{Owner: "new-owner", TTL: time.Hour})
	if err != nil || acquired || recovered.State != messaging.DeliveryAmbiguous || recovered.LastErrorClass != "owner_lost" || recovered.ClaimOwner != "" || recovered.ClientRequestID != claimed.ClientRequestID {
		t.Fatalf("recovered=%#v acquired=%t err=%v", recovered, acquired, err)
	}
}

func TestClaimRejectsDigestCollision(t *testing.T) {
	store := New()
	key := messaging.InboxKey{TenantID: "t", Channel: "fake", ExternalAccountID: "account", ExternalMessageID: "message"}
	base := messaging.ClaimInboxRequest{InboxKey: key, AgentAppID: "app", PayloadDigest: "digest-1", KeyVersion: 1, InitialState: messaging.InboxPreprocessPending}
	if _, err := store.ClaimInbox(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	base.PayloadDigest = "digest-2"
	if _, err := store.ClaimInbox(context.Background(), base); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("got %v", err)
	}
}
