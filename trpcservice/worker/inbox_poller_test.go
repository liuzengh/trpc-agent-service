package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
)

type pollCapture struct {
	mu       sync.Mutex
	requests []gateway.RunRequest
	err      error
}

func (capture *pollCapture) Submit(request gateway.RunRequest) error {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	capture.requests = append(capture.requests, request)
	return capture.err
}

func TestInboxPollerRecoversExpiredClaimOnce(t *testing.T) {
	store := idempotency.NewMemoryStore()
	message := gateway.InboundMessage{TenantID: "tenant", AppID: "app", BindingID: "binding", ExternalMessageID: "message", ExternalUserID: "external", UserID: "user", SessionID: "session", Text: "hello", ConfigVersion: 1}
	first, won, err := store.Claim(context.Background(), message, "dead-node", time.Millisecond)
	if err != nil || !won {
		t.Fatalf("claim=%+v won=%v err=%v", first, won, err)
	}
	time.Sleep(2 * time.Millisecond)
	capture := &pollCapture{}
	poller, err := NewInboxPoller(context.Background(), store, capture, InboxPollerConfig{Owner: "recovery-node", PollInterval: time.Hour, ClaimTTL: time.Minute, BatchSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	defer poller.Close(context.Background())
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		capture.mu.Lock()
		count := len(capture.requests)
		capture.mu.Unlock()
		if count == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.requests) != 1 || capture.requests[0].ClaimAttempt != 2 || capture.requests[0].ClaimToken == first.ClaimToken {
		t.Fatalf("requests=%+v", capture.requests)
	}
}

func TestInboxPollerReturnsSubmitFailureToRetry(t *testing.T) {
	store := idempotency.NewMemoryStore()
	message := gateway.InboundMessage{TenantID: "tenant", AppID: "app", BindingID: "binding", ExternalMessageID: "message", ExternalUserID: "external", UserID: "user", SessionID: "session", Text: "hello", ConfigVersion: 1}
	_, _, _ = store.Claim(context.Background(), message, "dead-node", time.Millisecond)
	time.Sleep(2 * time.Millisecond)
	capture := &pollCapture{err: errors.New("queue unavailable")}
	poller, err := NewInboxPoller(context.Background(), store, capture, InboxPollerConfig{Owner: "recovery-node", PollInterval: time.Millisecond, ClaimTTL: time.Minute, BatchSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if err := poller.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.requests) < 2 {
		t.Fatalf("submit attempts=%d", len(capture.requests))
	}
}
