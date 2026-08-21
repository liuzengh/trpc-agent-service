package platform

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestRunnerIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.SaveTenant(ctx, Tenant{ID: "t1", Agent: AgentConfig{Name: "a1"}}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store, Responder: EchoResponder{}}
	in := Message{ID: "m1", TenantID: "t1", BindingID: "binding-1", Channel: "web", UserID: "u1", SessionID: "s1", Content: "hello"}
	_, first, err := runner.Run(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	_, second, err := runner.Run(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if first == "" || second != "" {
		t.Fatalf("expected first reply and empty duplicate reply, got %q and %q", first, second)
	}
	messages, err := store.Messages(ctx, "t1", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected one request and one response, got %d messages", len(messages))
	}
}

func TestTenantIsolation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.SaveTenant(ctx, Tenant{ID: "known"}); err != nil {
		t.Fatal(err)
	}
	_, _, err := (Runner{Store: store, Responder: EchoResponder{}}).Run(ctx, Message{ID: "m", TenantID: "unknown", BindingID: "binding-1", Channel: "web", SessionID: "s", Content: "x"})
	if err == nil {
		t.Fatal("expected unknown tenant to be rejected")
	}
	if err := store.SaveTenant(ctx, Tenant{ID: "other"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(ctx, "other", "web", "binding-1", "m1")
	if err != nil || !claimed {
		t.Fatalf("same external message ID must be isolated by tenant: claimed=%v err=%v", claimed, err)
	}
}

func TestClaimHasOneConcurrentWinner(t *testing.T) {
	store := NewMemoryStore()
	var winners atomic.Int32
	const contenders = 100
	done := make(chan struct{}, contenders)
	for i := 0; i < contenders; i++ {
		go func() {
			claimed, err := store.Claim(context.Background(), "tenant-a", "web", "binding-a", "message-a")
			if err == nil && claimed {
				winners.Add(1)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < contenders; i++ {
		<-done
	}
	if got := winners.Load(); got != 1 {
		t.Fatalf("expected one claim winner, got %d", got)
	}
}

func TestClaimHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewMemoryStore().Claim(ctx, "tenant-a", "web", "binding-a", "message-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled context, got %v", err)
	}
}
