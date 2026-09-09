package channel

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

type registryTestAdapter struct{}

func (registryTestAdapter) VerifySignature(*http.Request, *tenant.ChannelBinding) error { return nil }
func (registryTestAdapter) ParseInbound(*http.Request, *tenant.ChannelBinding) (*InboundMessage, error) {
	return &InboundMessage{}, nil
}
func (registryTestAdapter) SendReply(context.Context, *tenant.ChannelBinding, *OutboundMessage) error {
	return nil
}
func (registryTestAdapter) SendStreamChunk(context.Context, *tenant.ChannelBinding, *StreamChunk) error {
	return nil
}
func (registryTestAdapter) SupportsStreaming() bool             { return false }
func (registryTestAdapter) HandleRateLimit(error) time.Duration { return 0 }

func TestAdapterRegistryConcurrentRegisterAndGet(t *testing.T) {
	registry := NewAdapterRegistry()
	adapter := registryTestAdapter{}
	const workers = 8
	const iterations = 500
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				channelType := ChannelType("test-" + string(rune('a'+(worker%4))))
				registry.Register(channelType, adapter)
				if got, ok := registry.Get(channelType); !ok || got == nil {
					t.Errorf("Get(%q) = (%v, %v), want registered adapter", channelType, got, ok)
				}
			}
		}(worker)
	}
	wg.Wait()
}

func TestAdapterRegistryRejectsNilInputs(t *testing.T) {
	var nilRegistry *AdapterRegistry
	nilRegistry.Register(ChannelTypeTelegram, registryTestAdapter{})
	if adapter, ok := nilRegistry.Get(ChannelTypeTelegram); ok || adapter != nil {
		t.Fatalf("nil registry Get = (%v, %v), want (nil, false)", adapter, ok)
	}
	registry := NewAdapterRegistry()
	registry.Register(ChannelTypeTelegram, nil)
	if adapter, ok := registry.Get(ChannelTypeTelegram); ok || adapter != nil {
		t.Fatalf("nil adapter was registered: (%v, %v)", adapter, ok)
	}
	var typedNil *registryTestAdapter
	registry.Register(ChannelTypeTelegram, typedNil)
	if adapter, ok := registry.Get(ChannelTypeTelegram); ok || adapter != nil {
		t.Fatalf("typed-nil adapter was registered: (%v, %v)", adapter, ok)
	}
}
