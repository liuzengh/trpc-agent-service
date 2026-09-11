package channels

import (
	"context"
	"errors"
	"testing"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
)

type fakeWeComAIBotClient struct {
	connected bool
	target    string
	content   string
	requestID string
	err       error
}

func (f *fakeWeComAIBotClient) IsConnected() bool { return f.connected }

func (f *fakeWeComAIBotClient) SendMarkdown(target, content string) (string, error) {
	f.target = target
	f.content = content
	return f.requestID, f.err
}

func TestWeComAIBotPlanUsesUTF8ByteLimit(t *testing.T) {
	adapter := NewWeComAIBot()
	parts, err := adapter.Plan(config.ChannelConfig{MaxMessageLength: 6}, domain.OutboundMessage{Text: "你好世界"})
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 2 || parts[0].Message.Text != "你好" || parts[1].Message.Text != "世界" {
		t.Fatalf("unexpected parts: %#v", parts)
	}
}

func TestWeComAIBotDeliveryOutcomes(t *testing.T) {
	adapter := NewWeComAIBot()
	binding := config.ChannelConfig{BindingID: "main"}
	request := delivery.Request{Message: domain.OutboundMessage{Target: "chat-1", Text: "hello"}}

	result := adapter.Deliver(context.Background(), binding, request)
	if result.Outcome != delivery.RetryableNotSent {
		t.Fatalf("missing connection outcome = %s", result.Outcome)
	}

	client := &fakeWeComAIBotClient{connected: true, requestID: "req-1"}
	adapter.RegisterClient(binding.BindingID, client)
	result = adapter.Deliver(context.Background(), binding, request)
	if result.Outcome != delivery.Confirmed || result.ProviderRequestID != "req-1" {
		t.Fatalf("confirmed result = %+v", result)
	}
	if client.target != "chat-1" || client.content != "hello" {
		t.Fatalf("send args = target %q content %q", client.target, client.content)
	}

	client.err = errors.New("ack timeout")
	result = adapter.Deliver(context.Background(), binding, request)
	if result.Outcome != delivery.Unknown {
		t.Fatalf("ambiguous acknowledgement outcome = %s", result.Outcome)
	}
}
