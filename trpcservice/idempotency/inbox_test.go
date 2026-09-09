package idempotency

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

func testMessage(tenantID string) gateway.InboundMessage {
	return gateway.InboundMessage{TenantID: tenantID, BindingID: "http", ExternalMessageID: "message-1", AppID: "app", UserID: "user", SessionID: "session", Text: "hello", ConfigVersion: 1}
}

func TestConcurrentDuplicatesHaveOneWinner(t *testing.T) {
	store := NewMemoryStore()
	var winners int
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, won, err := store.Claim(context.Background(), testMessage("tenant-a"), "worker", time.Minute)
			if err != nil {
				t.Error(err)
			}
			if won {
				mu.Lock()
				winners++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if winners != 1 {
		t.Fatalf("winners=%d", winners)
	}
	if _, won, err := store.Claim(context.Background(), testMessage("tenant-b"), "worker", time.Minute); err != nil || !won {
		t.Fatalf("cross tenant won=%v err=%v", won, err)
	}
}

func TestInboxRejectsUnresolvedProviderMediaReference(t *testing.T) {
	message := testMessage("tenant-a")
	message.Media = &gateway.MediaReference{Kind: "image", Key: "private-media-key"}
	_, _, err := NewMemoryStore().Claim(context.Background(), message, "worker", time.Minute)
	if err == nil || strings.Contains(err.Error(), "private-media-key") {
		t.Fatalf("error=%v", err)
	}
}

func TestExpiredClaimRejectsOldCompletion(t *testing.T) {
	store := NewMemoryStore()
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	first, won, _ := store.Claim(context.Background(), testMessage("tenant-a"), "worker-a", time.Second)
	if !won {
		t.Fatal("first claim lost")
	}
	now = now.Add(2 * time.Second)
	second, won, _ := store.Claim(context.Background(), testMessage("tenant-a"), "worker-b", time.Second)
	if !won || second.ClaimToken == first.ClaimToken {
		t.Fatal("reclaim failed")
	}
	if err := store.Complete(context.Background(), first); !errors.Is(err, ErrClaimOwner) {
		t.Fatalf("old complete err=%v", err)
	}
	if err := store.Complete(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if _, won, _ := store.Claim(context.Background(), testMessage("tenant-a"), "worker-c", time.Second); won {
		t.Fatal("completed inbox reclaimed")
	}
}

func TestRetryWaitsUntilScheduledTime(t *testing.T) {
	store := NewMemoryStore()
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	claim, _, _ := store.Claim(context.Background(), testMessage("tenant-a"), "a", time.Second)
	if err := store.Fail(context.Background(), claim, errors.New("boom"), now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, won, _ := store.Claim(context.Background(), testMessage("tenant-a"), "b", time.Second); won {
		t.Fatal("early retry won")
	}
	now = now.Add(time.Minute)
	if _, won, _ := store.Claim(context.Background(), testMessage("tenant-a"), "b", time.Second); !won {
		t.Fatal("due retry did not win")
	}
}

func TestCapacityDeferDoesNotConsumeRetryAttempt(t *testing.T) {
	store := NewMemoryStore()
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	claim, won, err := store.Claim(context.Background(), testMessage("tenant-a"), "gateway", time.Minute)
	if err != nil || !won || claim.Attempt != 1 {
		t.Fatalf("claim=%+v won=%v err=%v", claim, won, err)
	}
	if err := store.Defer(context.Background(), claim, now); err != nil {
		t.Fatal(err)
	}
	ready, err := store.ClaimReady(context.Background(), "worker", time.Minute, 1)
	if err != nil || len(ready) != 1 || ready[0].Attempt != 1 {
		t.Fatalf("deferred ready=%+v err=%v", ready, err)
	}
	if err := store.Defer(context.Background(), claim, now); !errors.Is(err, ErrClaimOwner) {
		t.Fatalf("stale defer error=%v", err)
	}
}

func TestRejectedInboxIsTerminal(t *testing.T) {
	store := NewMemoryStore()
	message := testMessage("tenant-a")
	message.BindingID = "binding"
	message.ExternalMessageID = "denied"
	claim, won, err := store.Claim(context.Background(), message, "worker", time.Minute)
	if err != nil || !won {
		t.Fatalf("claim=%+v won=%v err=%v", claim, won, err)
	}
	if err := store.Reject(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	duplicate, won, err := store.Claim(context.Background(), message, "other", time.Minute)
	if err != nil || won || duplicate.Status != StatusRejected {
		t.Fatalf("duplicate=%+v won=%v err=%v", duplicate, won, err)
	}
}

func TestRenewPreventsReclaimAndRejectsStaleOwner(t *testing.T) {
	store := NewMemoryStore()
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	claim, won, err := store.Claim(context.Background(), testMessage("tenant-a"), "worker-a", time.Second)
	if err != nil || !won {
		t.Fatalf("claim=%+v won=%v err=%v", claim, won, err)
	}
	now = now.Add(500 * time.Millisecond)
	renewed, err := store.Renew(context.Background(), claim, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(700 * time.Millisecond)
	if _, won, err := store.Claim(context.Background(), testMessage("tenant-a"), "worker-b", time.Second); err != nil || won {
		t.Fatalf("renewed claim reclaimed won=%v err=%v", won, err)
	}
	stale := claim
	stale.ClaimToken += "-stale"
	if _, err := store.Renew(context.Background(), stale, time.Second); !errors.Is(err, ErrClaimOwner) {
		t.Fatalf("stale renew err=%v", err)
	}
	if err := store.Complete(context.Background(), renewed); err != nil {
		t.Fatal(err)
	}
}

func TestClaimReadyRecoversExpiredLeaseAndRejectsOldToken(t *testing.T) {
	store := NewMemoryStore()
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	first, won, err := store.Claim(context.Background(), testMessage("tenant-a"), "gateway", time.Second)
	if err != nil || !won {
		t.Fatalf("claim=%+v won=%v err=%v", first, won, err)
	}
	now = now.Add(2 * time.Second)
	ready, err := store.ClaimReady(context.Background(), "worker-b", time.Minute, 10)
	if err != nil || len(ready) != 1 || ready[0].Attempt != 2 || ready[0].ClaimToken == first.ClaimToken {
		t.Fatalf("ready=%+v err=%v", ready, err)
	}
	if err := store.Complete(context.Background(), first); !errors.Is(err, ErrClaimOwner) {
		t.Fatalf("old completion error=%v", err)
	}
	if err := store.Complete(context.Background(), ready[0]); err != nil {
		t.Fatal(err)
	}
}

func TestClaimReadyPreservesSessionSequence(t *testing.T) {
	store := NewMemoryStore()
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	firstMessage := testMessage("tenant-a")
	firstMessage.ExternalMessageID = "first"
	firstMessage.ReceivedAt = now
	first, _, _ := store.Claim(context.Background(), firstMessage, "gateway", time.Second)
	secondMessage := firstMessage
	secondMessage.ExternalMessageID = "second"
	secondMessage.ReceivedAt = now.Add(time.Millisecond)
	second, _, _ := store.Claim(context.Background(), secondMessage, "gateway", time.Second)
	now = now.Add(2 * time.Second)
	ready, err := store.ClaimReady(context.Background(), "worker", time.Minute, 10)
	if err != nil || len(ready) != 1 || ready[0].InboxID != first.InboxID {
		t.Fatalf("first ready=%+v second=%+v err=%v", ready, second, err)
	}
	if err := store.Complete(context.Background(), ready[0]); err != nil {
		t.Fatal(err)
	}
	ready, err = store.ClaimReady(context.Background(), "worker", time.Minute, 10)
	if err != nil || len(ready) != 1 || ready[0].InboxID != second.InboxID {
		t.Fatalf("second ready=%+v err=%v", ready, err)
	}
}

func TestClaimReadyMovesExhaustedWorkToDLQ(t *testing.T) {
	store := NewMemoryStore()
	store.maxAttempts = 1
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	message := testMessage("tenant-a")
	claim, _, _ := store.Claim(context.Background(), message, "gateway", time.Second)
	now = now.Add(2 * time.Second)
	ready, err := store.ClaimReady(context.Background(), "worker", time.Minute, 10)
	if err != nil || len(ready) != 0 {
		t.Fatalf("ready=%+v err=%v", ready, err)
	}
	duplicate, won, err := store.Claim(context.Background(), message, "other", time.Minute)
	if err != nil || won || duplicate.Status != StatusDLQ || duplicate.ClaimToken != claim.ClaimToken {
		t.Fatalf("duplicate=%+v won=%v err=%v", duplicate, won, err)
	}
}

func TestExecutionStateSurvivesClaimRecovery(t *testing.T) {
	store := NewMemoryStore()
	message := testMessage("tenant-a")
	first, won, err := store.Claim(context.Background(), message, "worker-a", time.Minute)
	if err != nil || !won {
		t.Fatalf("first claim=%+v won=%v err=%v", first, won, err)
	}
	if err := store.SaveExecution(context.Background(), first, ExecutionRecord{Stage: ExecutionRunnerCommitted, Reply: "answer", EventID: "agent:" + first.InboxID}); err != nil {
		t.Fatal(err)
	}
	store.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	second, won, err := store.Claim(context.Background(), message, "worker-b", time.Minute)
	if err != nil || !won {
		t.Fatalf("reclaimed claim=%+v won=%v err=%v", second, won, err)
	}
	// A new claim token is required after takeover; the durable execution
	// record itself remains available to the new owner.
	execution, err := store.GetExecution(context.Background(), second)
	if err != nil || execution.Stage != ExecutionRunnerCommitted || execution.Reply != "answer" {
		t.Fatalf("execution=%+v err=%v", execution, err)
	}
}
