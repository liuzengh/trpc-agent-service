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

// durableBus is a fakeBus with the optional persistence capability: it keeps a
// cursor and a route table across "restarts" (two Gateway instances).
type durableBus struct {
	fakeBus
	cursor   string
	routes   map[string]string
	saved    []string
	readFrom []string
}

func newDurableBus() *durableBus {
	return &durableBus{fakeBus: fakeBus{published: make(chan *bus.Message, 8)}, routes: map[string]string{}}
}

func (d *durableBus) LoadOutboundCursor(context.Context) (string, error) {
	if d.cursor == "" {
		return "$", nil
	}
	return d.cursor, nil
}

func (d *durableBus) SaveOutboundCursor(_ context.Context, id string) error {
	d.cursor = id
	d.saved = append(d.saved, id)
	return nil
}

func (d *durableBus) SaveIMRoute(_ context.Context, sessionID, payload string) error {
	d.routes[sessionID] = payload
	return nil
}

func (d *durableBus) LoadIMRoute(_ context.Context, sessionID string) (string, bool, error) {
	p, ok := d.routes[sessionID]
	return p, ok, nil
}

func (d *durableBus) ReadOutbound(_ context.Context, fromID string) ([]*bus.Message, string, error) {
	d.readFrom = append(d.readFrom, fromID)
	msgs := d.outbound
	d.outbound = nil
	if len(msgs) == 0 {
		return nil, "", nil
	}
	return msgs, "1700000000000-0", nil
}

// TestGatewayResumesFromThePersistedCursor covers the restart gap: a reply
// published while the gateway was down must be delivered on the next start
// instead of being skipped by a follower that jumps to the stream tail.
func TestGatewayResumesFromThePersistedCursor(t *testing.T) {
	ctx := context.Background()
	b := newDurableBus()
	b.cursor = "1699999999999-0"

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	g := NewGateway(b, nil)
	go func() { _ = g.Run(runCtx) }()

	deadline := time.After(2 * time.Second)
	for {
		if len(b.readFrom) > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("gateway never read the outbound stream")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	if b.readFrom[0] != "1699999999999-0" {
		t.Errorf("first read from %q, want the persisted cursor", b.readFrom[0])
	}
}

// TestGatewayPersistsCursorAndRoute is the other half: the position advances
// only after the batch was handled, and the conversation route is stored so a
// fresh Gateway (a restarted process) can still answer.
func TestGatewayPersistsCursorAndRoute(t *testing.T) {
	ctx := context.Background()
	b := newDurableBus()
	g := NewGateway(b, nil)
	a := &fakeAdapter{name: "wecom", inbound: make(chan *InboundMessage, 8), sent: make(chan *OutboundMessage, 8)}
	g.Attach(ctx, a, Attach{BindingID: "b-1", AgentID: "agent-1"})

	a.inbound <- &InboundMessage{
		PlatformMsgID: "p1", TenantID: "t1", SessionID: "s1",
		UserID: "u1", ChatType: ChatTypeSingle, ChatID: "chat-9", Content: "hi",
	}
	select {
	case <-b.published:
	case <-time.After(2 * time.Second):
		t.Fatal("inbound not published")
	}

	storeWait(t, func() bool { return b.routes["s1"] != "" })
	if b.routes["s1"] == "" {
		t.Fatal("the conversation route was never persisted")
	}

	reply := model.NewAssistantMessage("hello back")
	b.outbound = []*bus.Message{{
		ID: "r1", TenantID: "t1", SessionID: "s1", Channel: "wecom", Content: &reply,
	}}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// A brand-new gateway (the restarted process): its in-memory route table is
	// empty, so the reply can only reach the chat through the persisted route.
	fresh := NewGateway(b, nil)
	fresh.SetAdapterLookup(func(bindingID string) (Adapter, bool) {
		if bindingID == "b-1" {
			return a, true
		}
		return nil, false
	})
	go func() { _ = fresh.Run(runCtx) }()

	select {
	case sent := <-a.sent:
		if sent.Inbound == nil || sent.Inbound.ChatID != "chat-9" {
			t.Errorf("rehydrated route sent to %+v, want chat-9", sent.Inbound)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the reply was not delivered after the restart")
	}
	storeWait(t, func() bool { return len(b.saved) > 0 })
	if len(b.saved) == 0 {
		t.Error("the advanced outbound cursor was never persisted")
	}
}

// storeWait polls for a condition the gateway reaches asynchronously, so the
// assertions do not race the delivery goroutine.
func storeWait(t *testing.T, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if done() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
}
