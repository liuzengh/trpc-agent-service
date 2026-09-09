package dispatcher

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

func TestDispatcherReportsProcessorErrors(t *testing.T) {
	want := errors.New("boom")
	reported := make(chan error, 1)
	dispatcher, err := NewWithErrorHandler(context.Background(), func(context.Context, gateway.RunRequest) error {
		return want
	}, func(_ context.Context, _ gateway.RunRequest, err error) {
		reported <- err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Submit(gateway.RunRequest{TenantID: "t", AppID: "a", UserID: "u", SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-reported:
		if !errors.Is(got, want) {
			t.Fatalf("reported error=%v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("processor error was swallowed")
	}
	if err := dispatcher.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestDispatcherOrdersSameSession(t *testing.T) {
	var mu sync.Mutex
	var order []string
	done := make(chan struct{}, 10)
	dispatcher, _ := New(context.Background(), func(_ context.Context, request gateway.RunRequest) error {
		mu.Lock()
		order = append(order, request.InboxID)
		mu.Unlock()
		done <- struct{}{}
		return nil
	})
	for i := 0; i < 10; i++ {
		_ = dispatcher.Submit(gateway.RunRequest{InboxID: string(rune('a' + i)), TenantID: "t", AppID: "a", UserID: "u", SessionID: "s"})
	}
	for i := 0; i < 10; i++ {
		<-done
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := dispatcher.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for i, value := range order {
		if value != string(rune('a'+i)) {
			t.Fatalf("order=%v", order)
		}
	}
}

func TestDispatcherRunsDifferentSessionsInParallel(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	dispatcher, _ := New(context.Background(), func(context.Context, gateway.RunRequest) error { started <- struct{}{}; <-release; return nil })
	_ = dispatcher.Submit(gateway.RunRequest{TenantID: "t", AppID: "a", UserID: "u", SessionID: "one"})
	_ = dispatcher.Submit(gateway.RunRequest{TenantID: "t", AppID: "a", UserID: "u", SessionID: "two"})
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("sessions serialized")
		}
	}
	close(release)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := dispatcher.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Submit(gateway.RunRequest{}); err == nil {
		t.Fatal("submit after close succeeded")
	}
}
