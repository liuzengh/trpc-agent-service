package cluster

import (
	"context"
	"sync"
	"testing"
	"time"
)

type controlPubSub struct {
	mu            sync.Mutex
	subs          map[string][]chan []byte
	subscriptions int
}

func (backend *controlPubSub) Publish(_ context.Context, channel string, payload []byte) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	for _, subscriber := range backend.subs[channel] {
		subscriber <- append([]byte(nil), payload...)
	}
	return nil
}
func (backend *controlPubSub) Subscribe(_ context.Context, channel string) (<-chan []byte, func() error, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.subs == nil {
		backend.subs = make(map[string][]chan []byte)
	}
	backend.subscriptions++
	stream := make(chan []byte, 4)
	backend.subs[channel] = append(backend.subs[channel], stream)
	return stream, func() error { return nil }, nil
}

func TestCancelPublisherDoesNotSubscribeGatewayToWorkerControl(t *testing.T) {
	backend := &controlPubSub{}
	publisher, err := NewCancelPublisher(backend, &durableCancelStub{allowed: true}, "test")
	if err != nil {
		t.Fatal(err)
	}
	if !publisher.Cancel("tenant-a", "request") {
		t.Fatal("cancel was not accepted")
	}
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.subscriptions != 0 {
		t.Fatalf("gateway publisher opened %d subscriptions", backend.subscriptions)
	}
}

type durableCancelStub struct{ allowed bool }

func (store *durableCancelStub) Cancel(string, string) bool { return store.allowed }

func TestCancelBusPersistsThenBroadcastsAcrossNodes(t *testing.T) {
	backend := &controlPubSub{}
	store := &durableCancelStub{allowed: true}
	delivered := make(chan string, 2)
	first, err := NewCancelBus(context.Background(), backend, store, func(_, requestID string) bool {
		delivered <- "node-a:" + requestID
		return false
	}, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := NewCancelBus(context.Background(), backend, store, func(_, requestID string) bool {
		delivered <- "node-b:" + requestID
		return true
	}, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if !first.Cancel("tenant-a", "tenant-a/binding/message") {
		t.Fatal("cancel was not accepted")
	}
	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case value := <-delivered:
			seen[value] = true
		case <-time.After(time.Second):
			t.Fatalf("broadcasts=%v", seen)
		}
	}
	if !seen["node-a:tenant-a/binding/message"] || !seen["node-b:tenant-a/binding/message"] {
		t.Fatalf("broadcasts=%v", seen)
	}
}

func TestCancelBusDoesNotBroadcastRejectedIntent(t *testing.T) {
	backend := &controlPubSub{}
	delivered := make(chan string, 1)
	bus, err := NewCancelBus(context.Background(), backend, &durableCancelStub{}, func(_, requestID string) bool {
		delivered <- requestID
		return true
	}, "test")
	if err != nil {
		t.Fatal(err)
	}
	defer bus.Close()
	if bus.Cancel("tenant-a", "request") {
		t.Fatal("rejected durable cancellation was accepted")
	}
	select {
	case value := <-delivered:
		t.Fatalf("unexpected broadcast %q", value)
	case <-time.After(20 * time.Millisecond):
	}
}
