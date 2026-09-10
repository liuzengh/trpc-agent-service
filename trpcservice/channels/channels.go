// Package channels adapts IM platforms (WeCom, WeChat KF, web chat) to the
// normalized message model consumed by tenant runners.
package channels

import (
	"context"
	"errors"
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
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
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
	resultThrottled = "throttled"
)

// errorTypeTimeout separates "the model was too slow" from "the model is
// broken". Both used to land in the same error_type, which is exactly the
// distinction an operator needs mid-incident: a timeout means raise the budget
// or chase the upstream SLA, an error means read the detail.
const errorTypeTimeout = "timeout"

// errorTypeSend marks a terminal reply the adapter refused to carry. It is a
// different axis from the model error types: the platform did its job and the
// last hop failed, so the user is sitting there with nothing. Before this was
// recorded the only trace was a counter, and the success path went further and
// logged the reply as decision=ok.
const errorTypeSend = "send"

// User-facing replies of the three resilience paths. They live here rather than
// in guardrail because none is a tenant security decision: one is a budget, one
// an admission control outcome, one an upstream that failed.
//
// FailureText deliberately carries no detail. The wording used to be
// "runner error: " + err.Error(), which put the raw chain in front of the user —
// measured on the deployed stack with Redis stopped, the reply a visitor
// received was:
//
//	runner error: check session exists: check session exists pipeline:
//	dial tcp: lookup redis on 127.0.0.11:53: no such host
//
// That is the internal service name, the Docker embedded DNS address and the
// whole error chain, handed to anyone who can reach /callback — which needs no
// credentials. The detail is not lost, it is where an operator looks for it:
// the audit row keeps it verbatim in `detail`, and the span keeps it as the
// status description. Telling a user "context deadline exceeded" was already
// recognised as useless here (that is why TimeoutText exists); telling them the
// topology is the same mistake pointed the other way.
const (
	TimeoutText  = "本次回复超时，请稍后重试。"
	ThrottleText = "当前咨询量较大，请稍后重试。"
	FailureText  = "服务暂时不可用，请稍后重试。"
)

// Audit coordinates of the admission decision.
const (
	stageAdmission  = "admission" // ahead of the guardrails: nothing was inspected yet
	ruleConcurrency = "concurrency"
)

// replyTimeout bounds a terminal reply independently of the dispatch budget.
const replyTimeout = 10 * time.Second

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
	quota    *tenantQuota
	gov      Governance
}

// Governance bundles the per-dispatch governance hooks of proposal doc 3.5:
// the tenant guardrail policy, the audit trail, and the metric recorder, plus
// the runtime envelope that bounds one message. The zero value disables all of
// them, so tests and the walking skeleton can build a gateway without any
// wiring.
type Governance struct {
	// PolicyFor returns the tenant's current guardrail policy; nil means
	// no policy is enforced.
	PolicyFor func(tenantID string) tenant.Guardrails
	// LimitsFor returns the live runtime envelope; nil means the defaults.
	// It is re-read per dispatch rather than captured at wiring time, so a
	// change to the live config lands on the next message with no restart.
	LimitsFor func() config.AgentConfig
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

// limits resolves the runtime envelope, tolerating an unwired gateway. The
// fallback is the pre-quota behaviour (default budget, unlimited concurrency),
// and a non-positive timeout coming out of LimitsFor is corrected rather than
// trusted: WithTimeout with a non-positive duration yields an
// already-expired context, which would fail every single message.
func (g *Gateway) limits() config.AgentConfig {
	lim := config.AgentConfig{MessageTimeout: config.DefaultMessageTimeout}
	if g.gov.LimitsFor != nil {
		lim = g.gov.LimitsFor()
	}
	if lim.MessageTimeout <= 0 {
		lim.MessageTimeout = config.DefaultMessageTimeout
	}
	return lim
}

// errorTypeOf classifies a failure against the dispatch deadline, falling back
// to the caller's own bucket when the budget was not what ran out.
func errorTypeOf(ctx context.Context, fallback string) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return errorTypeTimeout
	}
	return fallback
}

// reply delivers one terminal (Done) message and records the delivery. Every
// user-visible outcome funnels through here — the successful stream close, the
// input-guardrail rejection, the throttle, the timeout, the model failure, the
// unknown tenant — which makes it the only place that can honestly say whether
// the user was told anything at all.
//
// It deliberately detaches from the dispatch context: adapters select on
// ctx.Done() while sending (WebChat does), so replying with an already-expired
// context fails silently — and the timeout path is precisely the one whose only
// job is to tell the user that something timed out. WithoutCancel keeps the
// trace parentage.
//
// decision is the caller's intent (ok, block, error). A Send failure overrides
// it: the audit row used to be written by the caller and only on the success
// path, which left two holes the drills walked into — failure paths told the
// user something the trail never mentioned, and the success path logged
// decision=ok unconditionally, so a Send that errored was recorded as a reply
// that got through. "We tried to say X" is not "we said X".
func (g *Gateway) reply(ctx context.Context, a Adapter, in *InboundMessage, text, decision string) {
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), replyTimeout)
	defer cancel()

	rec := audit.Record{
		TraceID: traceIDOf(ctx), Event: audit.EventReply,
		TenantID: in.TenantID, Channel: string(in.Channel),
		UserID: in.UserID, SessionID: in.SessionID(),
		Decision: decision,
	}
	if err := a.Send(rctx, &OutboundMessage{Target: *in, Text: text, Done: true}); err != nil {
		g.gov.Metrics.SendError(in.TenantID, string(in.Channel))
		rec.Decision, rec.ErrorType, rec.Detail = audit.DecisionError, errorTypeSend, err.Error()
	}
	g.gov.Audit.Log(rec)
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
		quota:    newTenantQuota(),
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
// stage is governed (proposal doc 3.5): the admission quota and the input
// guardrail before the model call, a streaming output tripwire over the
// chunks, audit records and tenant-labelled metrics around the model call,
// all under one trace and one end-to-end deadline. Events are drained until
// the channel closes or the deadline runs out, whichever comes first.
func (g *Gateway) dispatch(parent context.Context, a Adapter, in *InboundMessage) {
	lim := g.limits()
	// The deadline is taken before the session lock, so queueing behind an
	// earlier message of the same session counts towards it: the promise is
	// that one message is bounded end to end, and a bound that only starts
	// after an unbounded wait is not a bound.
	ctx, cancel := context.WithTimeout(parent, lim.MessageTimeout)
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

	// Admission control runs ahead of the session lock on purpose: taken the
	// other way round, every message over the cap would queue on the lock and
	// find the quota free again by the time it woke up.
	release, admitted := g.quota.acquire(in.TenantID, lim.MaxConcurrencyPerTenant)
	if !admitted {
		g.gov.Audit.Log(audit.Record{
			TraceID: traceID, Event: audit.EventThrottled,
			TenantID: in.TenantID, Channel: string(in.Channel),
			UserID: in.UserID, SessionID: in.SessionID(),
			Stage: stageAdmission, Rule: ruleConcurrency, Decision: audit.DecisionBlock,
			Detail: fmt.Sprintf("max_concurrency_per_tenant=%d", lim.MaxConcurrencyPerTenant),
		})
		g.gov.Metrics.Message(in.TenantID, string(in.Channel), resultThrottled)
		g.reply(ctx, a, in, ThrottleText, audit.DecisionBlock)
		return
	}
	defer release()

	unlock := g.serial.lock(in.SessionID())
	defer unlock()

	pol := g.policy(in.TenantID)
	if rule := guardrail.CheckInput(pol, in.Text); rule != "" {
		g.recordBlock(traceID, in, "input", rule)
		g.gov.Metrics.Message(in.TenantID, string(in.Channel), resultGuardrail)
		g.reply(ctx, a, in, guardrail.RejectionText, audit.DecisionBlock)
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
		g.reply(ctx, a, in, fmt.Sprintf("unknown tenant %q", in.TenantID), audit.DecisionError)
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
		g.failModel(ctx, a, modelFailure{
			span: callSpan, traceID: traceID, in: in, start: start,
			errType: errorTypeOf(ctx, "runner"), detail: err.Error(),
		})
		return
	}

	sc := guardrail.NewStreamChecker(pol)
	tripped := false
	var promptTokens, completionTokens int
drain:
	for {
		select {
		case <-ctx.Done():
			// The budget ran out while the stream was still open. Stop waiting
			// for the producer instead of blocking on it: this goroutine holds
			// the session lock, so one uncooperative stream would wedge every
			// later message of the same session.
			break drain
		case ev, open := <-events:
			if !open {
				break drain
			}
			if ev.IsError() {
				g.failModel(ctx, a, modelFailure{
					span: callSpan, traceID: traceID, in: in, start: start,
					errType: errorTypeOf(ctx, "agent"), detail: ev.Response.Error.Message,
				})
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
	}

	// A budget that ran out mid-drain leaves the user with a partial reply at
	// best, so it is a failure rather than an ok. The guardrail trip is the
	// exception: forwarding had already stopped, so the user-visible outcome
	// (CutoffText) is unchanged and the more specific record wins.
	if err := ctx.Err(); err != nil && !tripped {
		g.failModel(ctx, a, modelFailure{
			span: callSpan, traceID: traceID, in: in, start: start,
			errType: errorTypeOf(ctx, "agent"), detail: err.Error(),
		})
		return
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
	g.reply(ctx, a, in, notice, replyDecision)
	g.gov.Metrics.Message(in.TenantID, string(in.Channel), result)
}

// modelFailure carries what the three model-failure paths have in common.
type modelFailure struct {
	span    trace.Span
	traceID string
	in      *InboundMessage
	start   time.Time
	errType string // errorTypeTimeout, "runner", or "agent"
	detail  string // verbatim upstream message, kept for the audit trail
}

// failModel closes the model span and records one failure. Every path goes
// through it (Run returned an error, the stream carried an error event, the
// budget ran out) so the classification, the recorded fields and the wording
// cannot drift between them. Latency is recorded on failures too: a slow
// failing model call is exactly the data point an operator needs.
func (g *Gateway) failModel(ctx context.Context, a Adapter, f modelFailure) {
	// The wording is chosen here rather than passed in, so all three paths say
	// something a user can act on and none of them leaks f.detail. Two variants
	// only because they ask for different behaviour: a timeout is the platform's
	// own budget running out, so "try again shortly" is honest; a failure means
	// the upstream is broken and an immediate retry will probably fail the same
	// way, so the wording does not promise one will work.
	reply := FailureText
	if f.errType == errorTypeTimeout {
		reply = TimeoutText
	}
	f.span.SetStatus(codes.Error, f.detail)
	f.span.End()
	latency := time.Since(f.start)
	g.gov.Metrics.ModelLatency(f.in.TenantID, latency)
	g.gov.Audit.Log(audit.Record{
		TraceID: f.traceID, Event: audit.EventModelCall,
		TenantID: f.in.TenantID, Channel: string(f.in.Channel), SessionID: f.in.SessionID(),
		AgentName: agent.AgentName, Decision: audit.DecisionError, ErrorType: f.errType,
		LatencyMS: latency.Milliseconds(), Detail: f.detail,
	})
	g.gov.Metrics.Message(f.in.TenantID, string(f.in.Channel), resultError)
	g.reply(ctx, a, f.in, reply, audit.DecisionError)
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

// tenantQuota caps how many messages of one tenant may be in flight at once
// (proposal doc 2.3 「租户级配额限制单租户最大并发」, risk list #3). It is a
// counting semaphore per tenant rather than a queue: a message over the cap is
// rejected immediately, because an in-process queue would only turn an
// overload into memory growth plus unbounded latency, and the IM side already
// retries. Like dedup and the session lock it is per process, so the cap is
// per replica — see the deployment note in spec §2.4.
type tenantQuota struct {
	mu    sync.Mutex
	inflt map[string]int
}

func newTenantQuota() *tenantQuota { return &tenantQuota{inflt: make(map[string]int)} }

// acquire takes one slot for the tenant, reporting false when the cap is
// already reached. A limit of zero or less means unlimited (the config
// default), so an unwired platform behaves exactly as it did before quotas.
// The returned release func is idempotent and must be called once on the
// admitted path.
func (q *tenantQuota) acquire(tenantID string, limit int) (func(), bool) {
	if limit <= 0 {
		return func() {}, true
	}
	q.mu.Lock()
	if q.inflt[tenantID] >= limit {
		q.mu.Unlock()
		return nil, false
	}
	q.inflt[tenantID]++
	q.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			q.mu.Lock()
			q.inflt[tenantID]--
			if q.inflt[tenantID] == 0 {
				// Drop idle tenants so the map tracks active load, not the
				// set of tenants that ever sent a message.
				delete(q.inflt, tenantID)
			}
			q.mu.Unlock()
		})
	}, true
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
