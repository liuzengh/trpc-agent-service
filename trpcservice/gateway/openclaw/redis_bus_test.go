package openclaw

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

type memoryPubSub struct {
	mu   sync.Mutex
	subs map[string][]chan []byte
}

func (backend *memoryPubSub) Publish(_ context.Context, channel string, payload []byte) error {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	for _, subscriber := range backend.subs[channel] {
		subscriber <- append([]byte(nil), payload...)
	}
	return nil
}
func (backend *memoryPubSub) Subscribe(_ context.Context, channel string) (<-chan []byte, func() error, error) {
	backend.mu.Lock()
	defer backend.mu.Unlock()
	if backend.subs == nil {
		backend.subs = make(map[string][]chan []byte)
	}
	stream := make(chan []byte, 1)
	backend.subs[channel] = append(backend.subs[channel], stream)
	return stream, func() error { return nil }, nil
}

func TestRedisEventBusBridgesWorkerAndGatewayNodes(t *testing.T) {
	backend := &memoryPubSub{}
	gatewayBus := &RedisEventBus{Backend: backend}
	workerBus := &RedisEventBus{Backend: backend}
	stream, unsubscribe := gatewayBus.Subscribe("request")
	defer unsubscribe()
	workerBus.Publish(gateway.RunEvent{Type: "message.delta", RequestID: "request", SessionID: "session", Delta: "hello", TraceID: "trace"})
	select {
	case event := <-stream:
		if event.Type != "message.delta" || event.Delta != "hello" || event.SessionID != "session" || event.TraceID != "trace" {
			t.Fatalf("event=%+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("distributed event was not delivered")
	}
}
