package openclaw

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// RedisPubSub is the minimal adapter surface for go-redis or another shared bus.
type RedisPubSub interface {
	Publish(context.Context, string, []byte) error
	Subscribe(context.Context, string) (<-chan []byte, func() error, error)
}

// RedisEventBus carries SSE projections across Gateway and Worker nodes.
type RedisEventBus struct {
	Backend RedisPubSub
	Prefix  string
	mu      sync.Mutex
	cancel  map[<-chan StreamEvent]func()
}

func (bus *RedisEventBus) Publish(event gateway.RunEvent) {
	if bus == nil || bus.Backend == nil {
		return
	}
	projected := StreamEvent{Type: event.Type, RequestID: event.RequestID, SessionID: event.SessionID, TraceID: event.TraceID, Delta: event.Delta, Reply: event.Message, Stage: event.Stage, ToolName: event.ToolName, ToolCallID: event.ToolCallID, ToolStatus: event.ToolStatus, Error: event.Error, Terminal: event.Terminal}
	if event.TotalTokens != 0 {
		projected.Usage = &Usage{PromptTokens: event.PromptTokens, CompletionTokens: event.CompletionTokens, TotalTokens: event.TotalTokens}
	}
	payload, err := json.Marshal(projected)
	if err == nil {
		_ = bus.Backend.Publish(context.Background(), bus.channel(event.RequestID), payload)
	}
}

func (bus *RedisEventBus) Subscribe(requestID string) (<-chan StreamEvent, func()) {
	output := make(chan StreamEvent, 64)
	if bus == nil || bus.Backend == nil {
		close(output)
		return output, func() {}
	}
	input, closeBackend, err := bus.Backend.Subscribe(context.Background(), bus.channel(requestID))
	if err != nil {
		close(output)
		return output, func() {}
	}
	done := make(chan struct{})
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			close(done)
			_ = closeBackend()
			bus.mu.Lock()
			delete(bus.cancel, (<-chan StreamEvent)(output))
			bus.mu.Unlock()
		})
	}
	bus.mu.Lock()
	if bus.cancel == nil {
		bus.cancel = make(map[<-chan StreamEvent]func())
	}
	bus.cancel[(<-chan StreamEvent)(output)] = cancel
	bus.mu.Unlock()
	go func() {
		defer close(output)
		defer cancel()
		for {
			select {
			case <-done:
				return
			case payload, ok := <-input:
				if !ok {
					return
				}
				var event StreamEvent
				if json.Unmarshal(payload, &event) == nil {
					select {
					case output <- event:
					case <-done:
						return
					}
					if event.Terminal {
						return
					}
				}
			}
		}
	}()
	return output, cancel
}

func (bus *RedisEventBus) Unsubscribe(_ string, target <-chan StreamEvent) {
	if bus == nil {
		return
	}
	bus.mu.Lock()
	cancel := bus.cancel[target]
	bus.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}
func (bus *RedisEventBus) channel(requestID string) string {
	prefix := bus.Prefix
	if prefix == "" {
		prefix = "trpc-agent-service"
	}
	return prefix + ":run-events:" + requestID
}
