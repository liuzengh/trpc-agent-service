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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/guardrail"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

// Message results used as metric labels.
const (
	resultOK        = "ok"
	resultGuardrail = "guardrail"
	resultError     = "error"
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
// Callback returns a batch because pull-based protocols (WeChat KF sync_msg)
// fetch several messages per webhook event.
type Adapter interface {
	Type() Type
	// Callback verifies and parses one webhook request into 0..n messages.
	Callback(w http.ResponseWriter, r *http.Request) ([]*InboundMessage, error)
	// Send delivers one outbound message (chunk or final) to the IM.
	Send(ctx context.Context, msg *OutboundMessage) error
}

// ExtraRoutes lets an adapter expose auxiliary HTTP routes (SSE stream etc.).
type ExtraRoutes interface {
	Routes() map[string]http.Handler
}

// Gateway receives callbacks from all channels, dedups by msg_id, and
// dispatches to the tenant's runner; replies flow back through the
// originating adapter. Dispatches of the same session are serialized
// (proposal doc 2.3) so concurrent messages never interleave writes to the
// shared session state.
type Gateway struct {
	runners  *agent.Registry
	adapters map[Type]Adapter
	dedup    *dedup
	serial   *sessionSerializer
	gov      Governance
}

// Governance bundles the per-dispatch governance hooks of proposal doc 3.5:
// the tenant guardrail policy, the audit trail, and the metric recorder.
// The zero value disables all of them, so tests and the walking skeleton
// can build a gateway without any wiring.
type Governance struct {
	// PolicyFor returns the tenant's current guardrail policy; nil means
	// no policy is enforced.
	PolicyFor func(tenantID string) tenant.Guardrails
	Audit     *audit.Logger
	Metrics   *metrics.Recorder
}

// WithGovernance attaches the governance hooks; main wires it once at boot.
func (g *Gateway) WithGovernance(gov Governance) *Gateway {
	g.gov = gov
	return g
}

// policy resolves the tenant policy, tolerating an unwired gateway.
func (g *Gateway) policy(tenantID string) tenant.Guardrails {
	if g.gov.PolicyFor == nil {
		return tenant.Guardrails{}
	}
	return g.gov.PolicyFor(tenantID)
}

// traceIDOf renders the trace id active in ctx, or "" when tracing is off:
// the noop provider reports an invalid span context, and writing its
// all-zero rendering into every audit line would be pure noise.
func traceIDOf(ctx context.Context) string {
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}

// recordBlock writes the audit line and the counter of one rejection.
func (g *Gateway) recordBlock(traceID string, in *InboundMessage, stage, rule string) {
	g.gov.Audit.Log(audit.Record{
		TraceID: traceID, Event: audit.EventGuardrailBlock,
		TenantID: in.TenantID, Channel: string(in.Channel),
		UserID: in.UserID, SessionID: in.SessionID(),
		Stage: stage, Rule: rule, Decision: audit.DecisionBlock,
	})
	g.gov.Metrics.GuardrailBlock(in.TenantID, stage, rule)
}

// NewGateway builds a gateway over the runner registry with the adapters.
func NewGateway(runners *agent.Registry, adapters ...Adapter) *Gateway {
	g := &Gateway{
		runners:  runners,
		adapters: make(map[Type]Adapter, len(adapters)),
		dedup:    newDedup(10 * time.Minute),
		serial:   newSessionSerializer(),
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
	batch, err := a.Callback(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var fresh []*InboundMessage
	for _, in := range batch {
		if in == nil {
			continue
		}
		in.TenantID = tenantID
		in.Channel = a.Type()
		if g.dedup.seen(in.MsgID) {
			continue // duplicate delivery: idempotent skip, ACK already written
		}
		fresh = append(fresh, in)
	}
	if len(fresh) == 0 { // probe answered inside Callback, or all duplicates
		return
	}
	// The trace starts at the IM entry (proposal doc 3.5). WithoutCancel
	// detaches the dispatched goroutines from the request deadline while
	// keeping the trace parentage.
	ctx, span := otel.Tracer(metrics.ServiceName).Start(r.Context(), "im.callback", trace.WithAttributes(
		attribute.String("tenant", tenantID),
		attribute.String("channel", string(a.Type())),
	))
	defer span.End()
	ctx = context.WithoutCancel(ctx)
	// One callback batch is dispatched sequentially so same-session messages
	// keep their order (a WeChat KF sync_msg pull returns many at once);
	// distinct callbacks still run in parallel.
	go func() {
		for _, in := range fresh {
			g.dispatch(ctx, a, in)
		}
	}()
}

// dispatch runs the tenant's runner and streams reply chunks back through
// the adapter, holding the per-session lock for the whole execution. Every
// stage is governed (proposal doc 3.5): input guardrail before the model
// call, streaming output tripwire over the chunks, audit records and
// tenant-labelled metrics around the model call, all under one trace.
// Events are drained until the channel closes.
func (g *Gateway) dispatch(parent context.Context, a Adapter, in *InboundMessage) {
	release := g.serial.lock(in.SessionID())
	defer release()

	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	ctx, span := otel.Tracer(metrics.ServiceName).Start(ctx, "gateway.dispatch", trace.WithAttributes(
		attribute.String("tenant", in.TenantID),
		attribute.String("channel", string(in.Channel)),
		attribute.String("session_id", in.SessionID()),
		attribute.String("user_id", in.UserID),
	))
	defer span.End()
	traceID := traceIDOf(ctx)

	g.gov.Audit.Log(audit.Record{
		TraceID: traceID, Event: audit.EventInbound,
		TenantID: in.TenantID, Channel: string(in.Channel),
		UserID: in.UserID, SessionID: in.SessionID(),
		Decision: audit.DecisionAllow,
	})

	pol := g.policy(in.TenantID)
	if rule := guardrail.CheckInput(pol, in.Text); rule != "" {
		g.recordBlock(traceID, in, "input", rule)
		g.gov.Metrics.Message(in.TenantID, string(in.Channel), resultGuardrail)
		_ = a.Send(ctx, &OutboundMessage{Target: *in, Text: guardrail.RejectionText, Done: true})
		return
	}

	runner, ok := g.runners.Runner(in.TenantID)
	if !ok {
		g.gov.Audit.Log(audit.Record{
			TraceID: traceID, Event: audit.EventModelCall,
			TenantID: in.TenantID, Channel: string(in.Channel), SessionID: in.SessionID(),
			Decision: audit.DecisionError, ErrorType: "unknown_tenant",
		})
		g.gov.Metrics.Message(in.TenantID, string(in.Channel), resultError)
		_ = a.Send(ctx, &OutboundMessage{Target: *in,
			Text: fmt.Sprintf("unknown tenant %q", in.TenantID), Done: true})
		return
	}

	start := time.Now()
	// One span wraps the whole model interaction (call plus stream drain) so
	// the trace shows where the dispatch time actually went.
	ctx, callSpan := otel.Tracer(metrics.ServiceName).Start(ctx, "model.call", trace.WithAttributes(
		attribute.String("tenant", in.TenantID),
		attribute.String("agent_name", agent.AgentName),
	))
	events, err := runner.Run(ctx, in.UserID, in.SessionID(), model.NewUserMessage(in.Text))
	if err != nil {
		callSpan.SetStatus(codes.Error, err.Error())
		callSpan.End()
		// Latency is recorded on failures too: a slow failing model call is
		// exactly the data point an operator needs.
		g.gov.Metrics.ModelLatency(in.TenantID, time.Since(start))
		g.gov.Audit.Log(audit.Record{
			TraceID: traceID, Event: audit.EventModelCall,
			TenantID: in.TenantID, Channel: string(in.Channel), SessionID: in.SessionID(),
			AgentName: agent.AgentName, Decision: audit.DecisionError, ErrorType: "runner",
			LatencyMS: time.Since(start).Milliseconds(), Detail: err.Error(),
		})
		g.gov.Metrics.Message(in.TenantID, string(in.Channel), resultError)
		_ = a.Send(ctx, &OutboundMessage{Target: *in, Text: "runner error: " + err.Error(), Done: true})
		return
	}

	sc := guardrail.NewStreamChecker(pol)
	tripped := false
	var promptTokens, completionTokens int
	for ev := range events {
		if ev.IsError() {
			callSpan.SetStatus(codes.Error, ev.Response.Error.Message)
			callSpan.End()
			g.gov.Metrics.ModelLatency(in.TenantID, time.Since(start))
			g.gov.Audit.Log(audit.Record{
				TraceID: traceID, Event: audit.EventModelCall,
				TenantID: in.TenantID, Channel: string(in.Channel), SessionID: in.SessionID(),
				AgentName: agent.AgentName, Decision: audit.DecisionError, ErrorType: "agent",
				LatencyMS: time.Since(start).Milliseconds(), Detail: ev.Response.Error.Message,
			})
			g.gov.Metrics.Message(in.TenantID, string(in.Channel), resultError)
			_ = a.Send(ctx, &OutboundMessage{Target: *in,
				Text: "agent error: " + ev.Response.Error.Message, Done: true})
			return
		}
		if ev.Usage != nil {
			promptTokens += ev.Usage.PromptTokens
			completionTokens += ev.Usage.CompletionTokens
		}
		if ev.Response.Object == model.ObjectTypeChatCompletionChunk &&
			len(ev.Response.Choices) > 0 && !tripped {
			if text := ev.Response.Choices[0].Delta.Content; text != "" {
				if kw := sc.Add(text); kw != "" {
					tripped = true
					g.recordBlock(traceID, in, "output", guardrail.RuleKeyword)
					continue // stop forwarding, keep draining until close
				}
				if err := a.Send(ctx, &OutboundMessage{Target: *in, Text: text, Chunk: true}); err != nil {
					g.gov.Metrics.SendError(in.TenantID, string(in.Channel))
				}
			}
		}
	}

	latency := time.Since(start)
	callSpan.SetAttributes(
		attribute.Int("prompt_tokens", promptTokens),
		attribute.Int("completion_tokens", completionTokens),
		attribute.Bool("guardrail_tripped", tripped),
	)
	callSpan.End()
	g.gov.Audit.Log(audit.Record{
		TraceID: traceID, Event: audit.EventModelCall,
		TenantID: in.TenantID, Channel: string(in.Channel), SessionID: in.SessionID(),
		AgentName: agent.AgentName, Decision: audit.DecisionOK,
		LatencyMS:    latency.Milliseconds(),
		PromptTokens: promptTokens, CompletionTokens: completionTokens,
	})
	g.gov.Metrics.ModelLatency(in.TenantID, latency)
	g.gov.Metrics.Tokens(in.TenantID, "prompt", promptTokens)
	g.gov.Metrics.Tokens(in.TenantID, "completion", completionTokens)

	result, notice := resultOK, ""
	replyDecision := audit.DecisionOK
	if tripped {
		result, notice = resultGuardrail, guardrail.CutoffText
		replyDecision = audit.DecisionBlock
	}
	if err := a.Send(ctx, &OutboundMessage{Target: *in, Text: notice, Done: true}); err != nil {
		g.gov.Metrics.SendError(in.TenantID, string(in.Channel))
	}
	g.gov.Audit.Log(audit.Record{
		TraceID: traceID, Event: audit.EventReply,
		TenantID: in.TenantID, Channel: string(in.Channel),
		UserID: in.UserID, SessionID: in.SessionID(),
		Decision: replyDecision,
	})
	g.gov.Metrics.Message(in.TenantID, string(in.Channel), result)
}

// sessionSerializer provides one mutex per session_id. It is the
// single-node placeholder for the consistent-hash routing of proposal doc
// 2.3: once the platform spans nodes, the same session will always land on
// the same worker and this in-process lock remains sufficient there.
// Entries are refcounted and removed when idle, so the map does not grow
// with distinct sessions.
type sessionSerializer struct {
	mu    sync.Mutex
	locks map[string]*sessionLock
}

type sessionLock struct {
	mu   sync.Mutex
	refs int
}

func newSessionSerializer() *sessionSerializer {
	return &sessionSerializer{locks: make(map[string]*sessionLock)}
}

// lock acquires the mutex of session id, queueing behind earlier callers of
// the same session. The returned release func must be called exactly once.
func (s *sessionSerializer) lock(id string) func() {
	s.mu.Lock()
	l, ok := s.locks[id]
	if !ok {
		l = &sessionLock{}
		s.locks[id] = l
	}
	l.refs++
	s.mu.Unlock()

	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		s.mu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(s.locks, id)
		}
		s.mu.Unlock()
	}
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
