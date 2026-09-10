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

// Attach binds an adapter to the gateway. accountID/agentID are the defaults
// used when an inbound message does not carry its own agent id (IM callbacks
// usually omit it; the binding store then resolves the agent).
type Attach struct {
	AccountID string
	AgentID   string
}

type routeEntry struct {
	adapter Adapter
	chatID  string
}

// Gateway routes IM traffic through the platform bus.
type Gateway struct {
	bus      Bus
	bindings BindingStore
	rl       RateLimiter

	mu     sync.RWMutex
	routes map[string]routeEntry // sessionID -> adapter + chatID
}

// NewGateway returns a bridge over the bus. bindings may be nil (agent id is
// then taken from the inbound message or the attach defaults).
func NewGateway(b Bus, bindings BindingStore) *Gateway {
	return &Gateway{bus: b, bindings: bindings, routes: make(map[string]routeEntry)}
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
// adapter. It blocks until ctx is done.
func (g *Gateway) Run(ctx context.Context) error {
	cursor := "$"
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
			g.mu.RLock()
			route, ok := g.routes[m.SessionID]
			g.mu.RUnlock()
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
	}
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
			g.routes[in.SessionID] = routeEntry{adapter: a, chatID: in.ChatID}
			g.mu.Unlock()
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
