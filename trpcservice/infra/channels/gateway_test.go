package channels

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

type fakeBus struct {
	published chan *bus.Message
	outbound  []*bus.Message
}

func (f *fakeBus) PublishInbound(_ context.Context, m *bus.Message) error {
	f.published <- m
	return nil
}

func (f *fakeBus) ReadOutbound(_ context.Context, _ string) ([]*bus.Message, string, error) {
	msgs := f.outbound
	f.outbound = nil
	return msgs, "0", nil
}

type fakeAdapter struct {
	name    string
	inbound chan *InboundMessage
	sent    chan *OutboundMessage
}

func (a *fakeAdapter) Name() string                    { return a.name }
func (a *fakeAdapter) Start(context.Context) error     { return nil }
func (a *fakeAdapter) Stop(context.Context) error      { return nil }
func (a *fakeAdapter) Inbound() <-chan *InboundMessage { return a.inbound }
func (a *fakeAdapter) Send(_ context.Context, m *OutboundMessage) error {
	a.sent <- m
	return nil
}

func TestGatewayInboundPublishesAndRoutes(t *testing.T) {
	ctx := context.Background()
	b := &fakeBus{published: make(chan *bus.Message, 8)}
	g := NewGateway(b, nil)
	a := &fakeAdapter{name: "wecom", inbound: make(chan *InboundMessage, 8), sent: make(chan *OutboundMessage, 8)}

	g.Attach(ctx, a, Attach{AccountID: "corp-1", AgentID: "agent-1"})

	a.inbound <- &InboundMessage{
		PlatformMsgID: "p1", TenantID: "t1", SessionID: "s1",
		UserID: "u1", ChatType: ChatTypeSingle, ChatID: "chat-1", Content: "hi",
	}

	select {
	case m := <-b.published:
		if m.Channel != "wecom" || m.SessionID != "s1" || m.AgentID != "agent-1" {
			t.Errorf("published = %+v, want wecom/s1/agent-1", m)
		}
		if m.Content == nil || m.Content.Content != "hi" {
			t.Errorf("content = %+v, want hi", m.Content)
		}
		if m.ID != "wecom:p1" {
			t.Errorf("id = %q, want wecom:p1", m.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("inbound message never published")
	}
}

func TestGatewayOutboundDispatchesToAdapter(t *testing.T) {
	ctx := context.Background()
	b := &fakeBus{published: make(chan *bus.Message, 8)}
	g := NewGateway(b, nil)
	a := &fakeAdapter{name: "wecom", inbound: make(chan *InboundMessage, 8), sent: make(chan *OutboundMessage, 8)}
	g.Attach(ctx, a, Attach{AgentID: "agent-1"})

	a.inbound <- &InboundMessage{
		PlatformMsgID: "p1", TenantID: "t1", SessionID: "s1",
		UserID: "u1", ChatType: ChatTypeSingle, ChatID: "chat-9", Content: "hi",
	}
	select {
	case <-b.published:
	case <-time.After(2 * time.Second):
		t.Fatal("inbound not published")
	}

	reply := model.NewAssistantMessage("hello back")
	b.outbound = []*bus.Message{{
		ID: "r1", TenantID: "t1", SessionID: "s1", Channel: "wecom", Content: &reply,
	}}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = g.Run(runCtx) }()

	select {
	case sent := <-a.sent:
		if sent.Kind != KindText || sent.Text() != "hello back" {
			t.Errorf("sent = %+v, want text hello back", sent)
		}
		if sent.Inbound == nil || sent.Inbound.ChatID != "chat-9" {
			t.Errorf("sent inbound chatID = %+v, want chat-9", sent.Inbound)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("outbound reply never dispatched")
	}
}

func TestGatewayResolveAgentPrefersBinding(t *testing.T) {
	ctx := context.Background()
	b := &fakeBus{published: make(chan *bus.Message, 8)}
	store := NewMemBindingStore()
	_ = store.Create(ctx, ChannelBinding{
		BindingID: "b1", TenantID: "t1", AgentID: "bound-agent",
		Channel: ChannelWeCom, AccountID: "corp-1",
	})
	g := NewGateway(b, store)
	a := &fakeAdapter{name: "wecom", inbound: make(chan *InboundMessage, 8), sent: make(chan *OutboundMessage, 8)}
	g.Attach(ctx, a, Attach{AccountID: "corp-1", AgentID: "fallback-agent"})

	a.inbound <- &InboundMessage{
		PlatformMsgID: "p1", TenantID: "t1", SessionID: "s1",
		UserID: "u1", ChatID: "c", Content: "hi",
	}
	select {
	case m := <-b.published:
		if m.AgentID != "bound-agent" {
			t.Errorf("agent = %q, want bound-agent (binding wins)", m.AgentID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("inbound not published")
	}
}
