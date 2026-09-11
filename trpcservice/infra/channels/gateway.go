// Gateway bridges IM adapters to the platform bus.
//
// inbound:  each attached adapter's Inbound() channel is pumped into
//
//	bus.PublishInbound (the same pipeline the worker consumes); the
//	session -> (adapter, chatID) route is remembered for replies.
//
// outbound: Run() follows stream:outbound (no consumer group, so it coexists
//
//	with the admin chat SSE reader) and dispatches each reply to the
//	adapter + chat that originated the conversation.
//
// The real WSS Conn implementations live outside this package and are wired by
// main; here Conn is satisfied by mocks so the whole bridge is testable.
package channels

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/metrics"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// tracer names the IM bridge's spans (im.callback / im.reply).
var tracer = otel.Tracer("trpc-agent-service/channels")

// Bus is the slice of the message bus the gateway needs (bus.RedisBus
// satisfies it).
type Bus interface {
	PublishInbound(ctx context.Context, m *bus.Message) error
	ReadOutbound(ctx context.Context, fromID string) ([]*bus.Message, string, error)
}

// Durable is the optional durability capability of the bus: the outbound read
// position and the conversation routes survive a gateway restart. A bus that
// does not implement it (tests, an in-memory bus) leaves the gateway following
// the stream tail with in-memory routes only, which is exactly the pre-restart
// behavior — the capability is additive, never required.
type Durable interface {
	LoadOutboundCursor(ctx context.Context) (string, error)
	SaveOutboundCursor(ctx context.Context, id string) error
	SaveIMRoute(ctx context.Context, sessionID, payload string) error
	LoadIMRoute(ctx context.Context, sessionID string) (string, bool, error)
}

// Route is the persisted part of a conversation's reply path: which binding's
// adapter answers and to which IM chat. The adapter itself is resolved through
// AdapterLookup, because a restarted process holds new adapter instances.
type Route struct {
	BindingID string `json:"binding_id"`
	Channel   string `json:"channel"`
	ChatID    string `json:"chat_id"`
}

// Attach binds an adapter to the gateway. accountID/agentID are the defaults
// used when an inbound message does not carry its own agent id (IM callbacks
// usually omit it; the binding store then resolves the agent). BindingID ties
// the attach to a bindable account so replies can be routed again after a
// restart.
type Attach struct {
	BindingID string
	AccountID string
	AgentID   string
}

type routeEntry struct {
	adapter Adapter
	chatID  string
	route   Route
}

// Gateway routes IM traffic through the platform bus.
type Gateway struct {
	bus      Bus
	bindings BindingStore
	rl       RateLimiter

	// adapters resolves a binding id to its live adapter. Installed by the
	// Manager, which owns the connections; nil if the gateway is used alone
	// (then persisted routes cannot be rehydrated and are only used in-process).
	adapters func(bindingID string) (Adapter, bool)

	mu     sync.RWMutex
	routes map[string]routeEntry // sessionID -> adapter + chatID
}

// NewGateway returns a bridge over the bus. bindings may be nil (agent id is
// then taken from the inbound message or the attach defaults).
func NewGateway(b Bus, bindings BindingStore) *Gateway {
	return &Gateway{bus: b, bindings: bindings, routes: make(map[string]routeEntry)}
}

// SetAdapterLookup installs the resolver used to rehydrate a persisted route
// after a restart.
func (g *Gateway) SetAdapterLookup(lookup func(bindingID string) (Adapter, bool)) {
	g.adapters = lookup
}

// durable returns the bus's persistence capability, if it has one.
func (g *Gateway) durable() Durable {
	d, _ := g.bus.(Durable)
	return d
}

// SetRateLimiter installs an inbound rate limiter (nil disables limiting).
func (g *Gateway) SetRateLimiter(rl RateLimiter) {
	g.rl = rl
}

// Attach starts pumping an adapter's inbound messages into the bus. It returns
// immediately; the pump goroutine runs until ctx is done.
func (g *Gateway) Attach(ctx context.Context, a Adapter, opt Attach) {
	go g.pumpInbound(ctx, a, opt)
}

// Run follows stream:outbound and dispatches replies to the originating
// adapter. It blocks until ctx is done. It resumes from the persisted cursor
// when the bus supports it, so a reply published while this node was down is
// delivered on the next start rather than skipped.
func (g *Gateway) Run(ctx context.Context) error {
	cursor := "$"
	if d := g.durable(); d != nil {
		stored, err := d.LoadOutboundCursor(ctx)
		if err != nil {
			// Not fatal: following the tail only risks the replies published
			// during the outage, while failing here would stop IM delivery
			// entirely.
			slog.Warn("channels: outbound cursor unavailable, following the stream tail", "err", err)
		} else {
			cursor = stored
			if stored != "$" {
				slog.Info("channels: resuming outbound from the persisted cursor", "cursor", stored)
			}
		}
	}
	for {
		msgs, next, err := g.bus.ReadOutbound(ctx, cursor)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if len(msgs) > 0 {
			cursor = next
		}
		for _, m := range msgs {
			if m == nil || m.Channel == "" || m.Channel == "admin" || m.SessionID == "" || m.Content == nil {
				continue
			}
			text := strings.TrimSpace(m.Content.Content)
			if text == "" {
				continue
			}
			// Enforce the platform text ceiling before send so an over-limit
			// reply can never fail the whole outbound stream.
			text = truncateText(text, maxTextLen(m.Channel))
			route, ok := g.route(ctx, m.SessionID)
			if !ok {
				slog.Warn("channels: no route for session", "session", m.SessionID, "channel", m.Channel)
				continue
			}
			if route.adapter.Name() != m.Channel {
				continue
			}
			// im.reply span shares the trace id carried from the originating
			// im.callback, so Jaeger shows one end-to-end trace.
			replyCtx, replySpan := tracer.Start(metrics.TraceContextFromID(context.Background(), m.TraceID), "im.reply",
				trace.WithAttributes(
					attribute.String("channel", m.Channel),
					attribute.String("session_id", m.SessionID),
				),
			)
			var outMsg *OutboundMessage
			switch m.Kind {
			case KindStream:
				outMsg = &OutboundMessage{
					Inbound: &InboundMessage{SessionID: m.SessionID, ChatID: route.chatID},
					Kind:    KindStream,
				}
			case KindCard:
				outMsg = &OutboundMessage{
					Inbound:  &InboundMessage{SessionID: m.SessionID, ChatID: route.chatID},
					Kind:     KindCard,
					Segments: convertSegments(m.Segments),
				}
			default: // KindText or empty
				outMsg = &OutboundMessage{
					Inbound:  &InboundMessage{SessionID: m.SessionID, ChatID: route.chatID},
					Kind:     KindText,
					Segments: []Segment{{Type: "text", Text: text}},
				}
			}
			sendErr := route.adapter.Send(replyCtx, outMsg)
			replySpan.End()
			// Delivery metrics: every attempt is bucketed by ok (for the IM
			// success rate); a successful send additionally counts as an
			// outbound reply.
			metrics.IMDelivery(replyCtx, m.Channel, sendErr == nil)
			if sendErr == nil {
				metrics.OutboundMessage(replyCtx, m.TenantID, m.Channel)
			} else {
				slog.Warn("channels: adapter send failed", "channel", m.Channel, "err", sendErr)
			}
		}
		// Persist the advanced position only after the batch was handled, so a
		// crash mid-batch replays it instead of skipping it.
		if len(msgs) > 0 {
			if d := g.durable(); d != nil {
				if err := d.SaveOutboundCursor(ctx, cursor); err != nil {
					slog.Warn("channels: saving the outbound cursor failed", "cursor", cursor, "err", err)
				}
			}
		}
	}
}

// route resolves the reply path of a session: the in-memory table first, then
// the persisted route, which is what lets a restarted gateway answer a reply
// that was published while it was down. A rehydrated route is cached so the
// lookup happens once per conversation.
func (g *Gateway) route(ctx context.Context, sessionID string) (routeEntry, bool) {
	g.mu.RLock()
	entry, ok := g.routes[sessionID]
	g.mu.RUnlock()
	if ok {
		return entry, true
	}
	d := g.durable()
	if d == nil || g.adapters == nil {
		return routeEntry{}, false
	}
	payload, found, err := d.LoadIMRoute(ctx, sessionID)
	if err != nil {
		slog.Warn("channels: loading the persisted route failed", "session", sessionID, "err", err)
		return routeEntry{}, false
	}
	if !found {
		return routeEntry{}, false
	}
	var r Route
	if err := json.Unmarshal([]byte(payload), &r); err != nil {
		slog.Warn("channels: persisted route is unreadable", "session", sessionID, "err", err)
		return routeEntry{}, false
	}
	a, ok := g.adapters(r.BindingID)
	if !ok {
		// The binding was deleted (or its adapter failed to start): the reply
		// has nowhere to go. Log loudly — this is the one case where an IM
		// reply is dropped after being stored.
		slog.Warn("channels: persisted route has no live adapter, dropping reply",
			"session", sessionID, "binding", r.BindingID, "channel", r.Channel)
		return routeEntry{}, false
	}
	entry = routeEntry{adapter: a, chatID: r.ChatID, route: r}
	g.mu.Lock()
	g.routes[sessionID] = entry
	g.mu.Unlock()
	return entry, true
}

// pumpInbound normalizes an adapter's inbound messages into bus messages and
// remembers the session route for replies.
func (g *Gateway) pumpInbound(ctx context.Context, a Adapter, opt Attach) {
	for {
		select {
		case <-ctx.Done():
			return
		case in, ok := <-a.Inbound():
			if !ok {
				return
			}
			if in == nil || in.SessionID == "" {
				continue
			}
			// Inbound rate limit: per tenant+channel+sender. A limiter error
			// fails open (availability) and is logged; a denial drops the
			// message before it can spend any model/tool budget.
			if g.rl != nil {
				key := inboundLimitKey(in.TenantID, a.Name(), in.UserID)
				allow, err := g.rl.Allow(ctx, key)
				if err != nil {
					slog.Warn("channels: rate limiter unavailable, allowing", "channel", a.Name(), "err", err)
				} else if !allow {
					slog.Warn("channels: inbound rate limited, dropping",
						"channel", a.Name(), "tenant", in.TenantID, "user", in.UserID)
					continue
				}
			}
			agentID := in.AgentID
			if agentID == "" {
				agentID = g.resolveAgent(ctx, a.Name(), opt)
			}
			if agentID == "" {
				slog.Warn("channels: no agent for inbound", "channel", a.Name(), "session", in.SessionID)
				continue
			}
			content := model.NewUserMessage(in.Content)

			// The IM callback is the trace root: emit an im.callback span and
			// carry its trace id on the bus message so the worker's agent.run
			// and the im.reply spans all share one trace in Jaeger.
			cbCtx, cbSpan := tracer.Start(context.Background(), "im.callback",
				trace.WithAttributes(
					attribute.String("channel", a.Name()),
					attribute.String("session_id", in.SessionID),
					attribute.String("platform_msg_id", in.PlatformMsgID),
				),
			)
			traceID := cbSpan.SpanContext().TraceID().String()
			cbSpan.End()
			_ = cbCtx

			msg := &bus.Message{
				ID:        a.Name() + ":" + in.PlatformMsgID, // idempotent across redeliveries
				TenantID:  in.TenantID,
				AgentID:   agentID,
				SessionID: in.SessionID,
				Channel:   a.Name(),
				UserID:    in.UserID,
				TraceID:   traceID,
				Content:   &content,
			}
			if err := g.bus.PublishInbound(ctx, msg); err != nil {
				slog.Error("channels: publish inbound", "channel", a.Name(), "err", err)
				continue
			}
			g.mu.Lock()
			g.routes[in.SessionID] = routeEntry{
				adapter: a,
				chatID:  in.ChatID,
				route:   Route{BindingID: opt.BindingID, Channel: a.Name(), ChatID: in.ChatID},
			}
			g.mu.Unlock()
			// Remember the route beyond this process: a restarted gateway must
			// still know which chat to answer when it replays a missed reply.
			if d := g.durable(); d != nil {
				if payload, err := json.Marshal(Route{BindingID: opt.BindingID, Channel: a.Name(), ChatID: in.ChatID}); err != nil {
					slog.Warn("channels: encoding the reply route failed", "session", in.SessionID, "err", err)
				} else if err := d.SaveIMRoute(ctx, in.SessionID, string(payload)); err != nil {
					slog.Warn("channels: persisting the reply route failed", "session", in.SessionID, "err", err)
				}
			}
		}
	}
}

// resolveAgent finds the agent id: binding store first (account match), then
// the attach default.
func (g *Gateway) resolveAgent(ctx context.Context, channel string, opt Attach) string {
	if g.bindings != nil && opt.AccountID != "" {
		list, err := g.bindings.List(ctx, "", channel)
		if err != nil {
			// Fail-open to the attach default rather than drop the message;
			// log so a store outage that misroutes messages is visible.
			slog.Warn("channels: binding lookup failed, using attach default", "channel", channel, "err", err)
		} else {
			for _, b := range list {
				if b.AccountID == opt.AccountID && b.AgentID != "" {
					return b.AgentID
				}
			}
		}
	}
	return opt.AgentID
}

// convertSegments converts bus.Segment slice to channels.Segment slice.
func convertSegments(busSegs []bus.Segment) []Segment {
	if len(busSegs) == 0 {
		return nil
	}
	segs := make([]Segment, len(busSegs))
	for i, s := range busSegs {
		segs[i] = Segment{Type: s.Type, Text: s.Text, URL: s.URL}
	}
	return segs
}
