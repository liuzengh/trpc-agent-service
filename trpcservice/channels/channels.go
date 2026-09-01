// Package channels adapts IM platforms (WeCom, WeChat KF, web chat) to the
// normalized message model consumed by tenant runners.
package channels

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

// Type identifies one channel implementation.
type Type string

// Channel types implemented or reserved by the platform.
const (
	TypeWebChat  Type = "webchat"   // browser-based local self-verification channel
	TypeWeCom    Type = "wecom"     // 企业微信
	TypeWeChatKF Type = "wechat_kf" // 微信客服
)

// InboundMessage is the protocol-neutral form of one IM message.
type InboundMessage struct {
	TenantID string
	Channel  Type
	UserID   string
	GroupID  string // non-empty for group chat
	MsgID    string // IM-provided id, used for duplicate-delivery dedup
	Text     string
}

// SessionID follows the proposal doc 3.3 rules: single chat is
// tenant:channel:user, group chat is tenant:channel:group.
func (m *InboundMessage) SessionID() string {
	scope := m.UserID
	if m.GroupID != "" {
		scope = m.GroupID
	}
	return m.TenantID + ":" + string(m.Channel) + ":" + scope
}

// OutboundMessage is one reply delivery towards a channel. Chunk=true is a
// streaming fragment; Done=true marks the end of one reply.
type OutboundMessage struct {
	Target InboundMessage
	Text   string
	Chunk  bool
	Done   bool
}

// Adapter bridges one IM protocol to the normalized model. Implementations
// must write the protocol-level ACK inside Callback on the success path
// (e.g. WeCom "success", echostr probe) and return nil, nil for probes.
type Adapter interface {
	Type() Type
	// Callback verifies and parses one webhook request.
	Callback(w http.ResponseWriter, r *http.Request) (*InboundMessage, error)
	// Send delivers one outbound message (chunk or final) to the IM.
	Send(ctx context.Context, msg *OutboundMessage) error
}

// ExtraRoutes lets an adapter expose auxiliary HTTP routes (SSE stream etc.).
type ExtraRoutes interface {
	Routes() map[string]http.Handler
}

// Gateway receives callbacks from all channels, dedups by msg_id, and
// dispatches to the tenant's runner; replies flow back through the
// originating adapter.
type Gateway struct {
	runners  *agent.Registry
	adapters map[Type]Adapter
	dedup    *dedup
}

// NewGateway builds a gateway over the runner registry with the adapters.
func NewGateway(runners *agent.Registry, adapters ...Adapter) *Gateway {
	g := &Gateway{
		runners:  runners,
		adapters: make(map[Type]Adapter, len(adapters)),
		dedup:    newDedup(10 * time.Minute),
	}
	for _, a := range adapters {
		g.adapters[a.Type()] = a
	}
	return g
}

// Handler mounts /callback/{channel}/{tenant_id} plus adapter extra routes.
func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	for _, a := range g.adapters {
		prefix := "/callback/" + string(a.Type()) + "/"
		mux.HandleFunc(prefix, func(w http.ResponseWriter, r *http.Request) {
			tenantID := strings.Trim(strings.TrimPrefix(r.URL.Path, prefix), "/")
			g.handleCallback(a, tenantID, w, r)
		})
		if er, ok := a.(ExtraRoutes); ok {
			for path, h := range er.Routes() {
				mux.Handle(path, h)
			}
		}
	}
	return mux
}

func (g *Gateway) handleCallback(a Adapter, tenantID string, w http.ResponseWriter, r *http.Request) {
	in, err := a.Callback(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if in == nil { // protocol probe already answered inside Callback
		return
	}
	in.TenantID = tenantID
	in.Channel = a.Type()
	if g.dedup.seen(in.MsgID) {
		return // duplicate delivery: idempotent skip, ACK already written
	}
	go g.dispatch(a, in)
}

// dispatch runs the tenant's runner and streams reply chunks back through
// the adapter. Events are drained until the channel closes.
func (g *Gateway) dispatch(a Adapter, in *InboundMessage) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	runner, ok := g.runners.Runner(in.TenantID)
	if !ok {
		_ = a.Send(ctx, &OutboundMessage{Target: *in,
			Text: fmt.Sprintf("unknown tenant %q", in.TenantID), Done: true})
		return
	}
	events, err := runner.Run(ctx, in.UserID, in.SessionID(), model.NewUserMessage(in.Text))
	if err != nil {
		_ = a.Send(ctx, &OutboundMessage{Target: *in, Text: "runner error: " + err.Error(), Done: true})
		return
	}
	for ev := range events {
		if ev.IsError() {
			_ = a.Send(ctx, &OutboundMessage{Target: *in,
				Text: "agent error: " + ev.Response.Error.Message, Done: true})
			return
		}
		if ev.Response.Object == model.ObjectTypeChatCompletionChunk &&
			len(ev.Response.Choices) > 0 {
			if text := ev.Response.Choices[0].Delta.Content; text != "" {
				_ = a.Send(ctx, &OutboundMessage{Target: *in, Text: text, Chunk: true})
			}
		}
	}
	_ = a.Send(ctx, &OutboundMessage{Target: *in, Done: true})
}

// dedup is the in-memory msg_id idempotency table (proposal doc 3.4); it will
// be replaced by Redis SETNX once the Storage Adapter lands.
type dedup struct {
	mu  sync.Mutex
	m   map[string]time.Time
	ttl time.Duration
}

func newDedup(ttl time.Duration) *dedup {
	return &dedup{m: make(map[string]time.Time), ttl: ttl}
}

// seen reports whether id was already processed within the TTL window and
// records it otherwise. Empty ids are never deduped.
func (d *dedup) seen(id string) bool {
	if id == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if t, ok := d.m[id]; ok && time.Since(t) < d.ttl {
		return true
	}
	d.m[id] = time.Now()
	if len(d.m) > 4096 {
		for k, v := range d.m {
			if time.Since(v) > d.ttl {
				delete(d.m, k)
			}
		}
	}
	return false
}
