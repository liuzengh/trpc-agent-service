package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	plog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// auditLogger is the audit sink used by the guardrail (*storage.Auditor
// satisfies it). A small interface keeps the guardrail testable without PG.
type auditLogger interface {
	LogAsync(ev storage.AuditEvent)
	LogSync(ctx context.Context, ev storage.AuditEvent) error
}

// auditDecision describes a guardrail decision to audit for one message. An
// empty decision means "nothing to audit" (e.g. a non-requester's answer
// attempt in a group chat).
type auditDecision struct {
	decision  string // allow / deny / review / review_timeout
	errorType string
	toolName  string
	latencyMs int
}

// InputChecker screens an inbound message; denied=true blocks processing.
type InputChecker func(ctx context.Context, msg channels.InboundMessage) (reason string, denied bool)

// OutputChecker post-processes a reply (desensitization), returning the text
// to send.
type OutputChecker func(ctx context.Context, msg channels.InboundMessage, reply string) string

// DefaultBlockedWords is the platform input denylist baseline; a tenant's
// guardrail_policy.input_deny_words replaces it for that tenant's messages.
var DefaultBlockedWords = []string{"赌博", "毒品", "枪支"}

// degradedReply answers the user when the model keeps failing after its
// retry: the message is acked and answered immediately instead of riding the
// Stream redelivery path.
const degradedReply = "服务繁忙，请稍后再试。"

// BudgetGate tracks per-tenant daily token budgets:
// Allow pre-checks the accumulated usage, Record accounts the actual tokens
// after the run. *storage.Budget satisfies it.
type BudgetGate interface {
	Allow(ctx context.Context, tenantID string, maxPerDay int64) (bool, error)
	Record(ctx context.Context, tenantID string, tokens int64)
}

// Guarded wraps a Processor with the guardrail chain:
//
//	recall → tenant policy → allowlist → approval answer → input denylist
//	→ budget gate → inner processor → budget accounting → output checks
//	(redact/denylist) → approval confirmation composition
type Guarded struct {
	Inner    Processor
	Approver *Approver   // nil disables tool-approval handling
	Auditor  auditLogger // nil disables auditing
	// Input/Output are the platform baseline checks; tenant policies
	// (guardrail_policy) layer on top per message via PolicyFor.
	Input  []InputChecker
	Output []OutputChecker
	// PolicyFor resolves a tenant's guardrail policy; nil means platform
	// defaults only.
	PolicyFor func(ctx context.Context, tenantID string) (tenant.GuardrailPolicy, error)
	// Budget, when set, enforces the tenant's daily token budget
	// (guardrail_policy.max_tokens_per_day).
	Budget BudgetGate
	// StateMark, when set, persists a session-state marker for recall events.
	StateMark func(ctx context.Context, msg channels.InboundMessage, key string, value []byte) error
}

// Process implements Processor.
func (g *Guarded) Process(ctx context.Context, msg channels.InboundMessage) (channels.OutboundMessage, error) {
	// The guardrail is a distinct link in the trace chain.
	ctx, span := tracer.Start(ctx, "guardrail.process")
	defer span.End()
	span.SetAttributes(
		attribute.String("channel", msg.Channel),
		attribute.String("tenant_id", msg.TenantID),
		attribute.String("session_key", msg.SessionKey),
	)

	// 0. Recall events never reach the model: audit the
	//    recall, mark the session state (the event journal stays append-only),
	//    and produce an empty reply — the worker acks without an outbound hop.
	if msg.Type == channels.TypeRecall {
		span.SetAttributes(attribute.String("decision", "recall"))
		g.handleRecall(ctx, msg)
		return replyShell(msg), nil
	}

	// 1. Tenant policy resolution (guardrail_policy): failures degrade to the
	//    platform baseline with a warning. It runs before anything that answers
	//    the user, so every reply path — the approval answer included — is
	//    filtered by the tenant's own output rules.
	policy := g.policyFor(ctx, msg.TenantID)

	// 2. Allowlist gate, ahead of the approval answer: a user dropped from the
	//    allowlist must not be able to confirm a dangerous call that was
	//    intercepted while they were still on it. The pending approval is left
	//    to expire on its own.
	if len(policy.InputAllowUsers) > 0 && !slices.Contains(policy.InputAllowUsers, msg.UserID) {
		span.SetAttributes(attribute.String("decision", "deny"))
		plog.Warnf("user %s not in tenant %s allowlist", msg.UserID, msg.TenantID)
		g.syncAudit(msg, auditDecision{decision: "deny", errorType: "user_not_allowed"})
		out := replyShell(msg)
		out.Text = "抱歉，您没有权限使用该服务。"
		return out, nil
	}

	// 3. A pending approval consumes confirm/reject answers before the input
	//    denylist runs — an answer is a control word, not content. Its reply
	//    carries a tool RESULT the model never produced, so it rides the same
	//    output checks as an LLM reply.
	if g.Approver != nil {
		handled, out, dec, err := g.Approver.Answer(ctx, msg)
		if err != nil {
			return channels.OutboundMessage{}, err
		}
		if handled {
			out, deniedWord := g.redactOutput(ctx, msg, policy, out)
			if deniedWord != "" {
				// The tool has already run, so the execution decision is the
				// compliance record; the redaction rides that same row instead
				// of adding a second terminal audit for one message.
				plog.Warnf("output deny word %q hit in the approval reply (session=%s)",
					deniedWord, msg.SessionKey)
				if dec.errorType == "" {
					dec.errorType = "sensitive_output"
				}
			}
			span.SetAttributes(attribute.String("decision", firstNonEmpty(dec.decision, "allow")))
			g.syncAudit(msg, dec)
			return out, nil
		}
	}

	// 4. Input checks: the platform baseline, or the tenant's deny-word list
	//    when it replaces the baseline (guardrail_policy.input_deny_words).
	inputCheckers := g.Input
	if len(policy.InputDenyWords) > 0 {
		inputCheckers = []InputChecker{SensitiveWordInput(policy.InputDenyWords)}
	}
	for _, check := range inputCheckers {
		reason, denied := check(ctx, msg)
		if denied {
			span.SetAttributes(attribute.String("decision", "deny"))
			plog.Warnf("input denied (session=%s): %s", msg.SessionKey, reason)
			g.syncAudit(msg, auditDecision{decision: "deny", errorType: "sensitive_input"})
			out := replyShell(msg)
			out.Text = "抱歉，您的消息包含受限内容，已被拦截。"
			return out, nil
		}
	}

	// 5. Budget gate: deny when the tenant's daily token usage is
	//    already over budget; the run's actual tokens are recorded below.
	if g.Budget != nil && policy.MaxTokensPerDay > 0 && msg.TenantID != "" {
		ok, err := g.Budget.Allow(ctx, msg.TenantID, policy.MaxTokensPerDay)
		if err != nil {
			plog.Warnf("budget check failed (fail open, tenant=%s): %v", msg.TenantID, err)
		} else if !ok {
			span.SetAttributes(attribute.String("decision", "deny"))
			g.syncAudit(msg, auditDecision{decision: "deny", errorType: "budget_exceeded"})
			out := replyShell(msg)
			out.Text = "今日用量已达上限，请明日再试。"
			return out, nil
		}
	}
	// 6. Inner processing (Runner or echo fallback). Routine allow audit,
	//    async lane; process errors ride the same event as error_type.
	started := time.Now()
	out, err := g.Inner.Process(ctx, msg)
	// Charge the run's actual tokens on every exit path: a model failure can
	// burn prompt tokens before dying, and charging only successful runs
	// would let a sustained model outage bypass the daily budget. Zero usage
	// (infra failure before any model call) is a no-op in Record.
	if g.Budget != nil && msg.TenantID != "" {
		g.Budget.Record(ctx, msg.TenantID, int64(out.PromptTokens+out.CompletionTokens))
	}

	// Drain the interception signal even on error: a redelivery regenerates
	// it, and a stale signal must not leak into an unrelated run.
	var sig Signal
	var signaled bool
	if g.Approver != nil {
		sig, signaled = g.Approver.TakeSignal(approvalScope{appID: msg.AppID, sessionKey: msg.SessionKey})
	}
	if err != nil {
		var mErr *ModelError
		if !errors.As(err, &mErr) {
			// Infrastructure failure (session store, queue, ...): leave the
			// message pending for Stream redelivery.
			span.SetAttributes(attribute.String("decision", "error"))
			span.RecordError(err)
			g.asyncAudit(msg, out, started, err)
			return out, err
		}
		// Model failure (deadline hit, retried once by the runner already):
		// degrade to a busy reply so the user gets an immediate answer
		// instead of waiting minutes for a redelivery that will likely fail
		// the same way.
		if signaled {
			// A dangerous call was intercepted before the failure: the
			// pending approval is real, so the confirmation notice (not the
			// busy reply) is what the user needs.
			return g.deliverSignal(ctx, span, msg, policy, sig, replyShell(msg)), nil
		}
		span.SetAttributes(attribute.String("decision", "degraded"))
		metrics.ProcessErrorTotal.Add(ctx, 1, processAttr(msg))
		g.asyncModelAudit(msg, out, started, mErr)
		out = replyShell(msg)
		out.Text = degradedReply
		return out, nil
	}
	// 7. Output checks: platform desensitization, then the tenant's output
	//    denylist — a hit replaces the reply and audits a deny.
	out, deniedWord := g.redactOutput(ctx, msg, policy, out)
	if deniedWord != "" {
		g.denyOutput(span, msg, deniedWord, "")
		return out, nil
	}

	// 8. A dangerous call was intercepted during the run: audit the decision
	//    with full tenant context and replace the LLM reply with the
	//    deterministic confirmation/conflict/timeout notice.
	if signaled {
		return g.deliverSignal(ctx, span, msg, policy, sig, out), nil
	}
	// Terminal allow audit — deliberately last: each message gets exactly one
	// terminal audit, from here or from the branches above, so a denied reply
	// is not double-counted as an allow.
	span.SetAttributes(attribute.String("decision", "allow"))
	g.asyncAudit(msg, out, started, nil)
	return out, nil
}

// policyFor resolves the tenant's guardrail policy; lookup failures and empty
// tenant IDs degrade to the zero policy (platform baseline only).
func (g *Guarded) policyFor(ctx context.Context, tenantID string) tenant.GuardrailPolicy {
	if g.PolicyFor == nil || tenantID == "" {
		return tenant.GuardrailPolicy{}
	}
	p, err := g.PolicyFor(ctx, tenantID)
	if err != nil {
		plog.Warnf("guardrail policy lookup %s: %v", tenantID, err)
		return tenant.GuardrailPolicy{}
	}
	return p
}

// outputDeniedReply replaces a reply that tripped the tenant's output
// denylist. It deliberately does not name the word.
const outputDeniedReply = "抱歉，回复包含受限内容，已被拦截。"

// redactOutput runs the platform desensitization checkers over a reply and
// then the tenant's output denylist. A non-empty deniedWord means the reply hit
// that denylist and its text was replaced by outputDeniedReply.
//
// Every reply the guardrail sends goes through here, not just the model's: the
// approval notices embed raw tool arguments and results, which no model-side
// check has seen and which land in the whole group chat.
func (g *Guarded) redactOutput(ctx context.Context, msg channels.InboundMessage,
	policy tenant.GuardrailPolicy, out channels.OutboundMessage) (channels.OutboundMessage, string) {
	for _, check := range g.Output {
		out.Text = check(ctx, msg, out.Text)
	}
	for _, w := range policy.OutputDenyWords {
		if w != "" && strings.Contains(out.Text, w) {
			out.Text = outputDeniedReply
			return out, w
		}
	}
	return out, ""
}

// denyOutput records an output-denylist interception: the span decision, the
// warning and the synchronous audit row. It is the message's only terminal
// audit, so callers must not write another. toolName is set when the denied
// text was an approval notice, tying the row to the intercepted call.
func (g *Guarded) denyOutput(span trace.Span, msg channels.InboundMessage, word, toolName string) {
	span.SetAttributes(attribute.String("decision", "deny"))
	plog.Warnf("output deny word %q hit (session=%s)", word, msg.SessionKey)
	g.syncAudit(msg, auditDecision{decision: "deny", errorType: "sensitive_output", toolName: toolName})
}

// deliverSignal replaces out's text with the deterministic notice for an
// interception and filters it like any other reply. Exactly one terminal audit
// follows: the deny when the notice itself trips the tenant denylist (the raw
// arguments are the leak the denylist exists to stop), otherwise the
// interception decision.
func (g *Guarded) deliverSignal(ctx context.Context, span trace.Span, msg channels.InboundMessage,
	policy tenant.GuardrailPolicy, sig Signal, out channels.OutboundMessage) channels.OutboundMessage {
	out.Text = signalReply(sig)
	out, deniedWord := g.redactOutput(ctx, msg, policy, out)
	if deniedWord != "" {
		g.denyOutput(span, msg, deniedWord, sig.ToolName)
		return out
	}
	// Card-capable channels render the confirmation notice as a card; the rest
	// read Text. Every "created" signal gets a card — including a redelivery
	// re-hit (Fresh=false), where a duplicate card is harmless. The card is
	// attached only AFTER the output filters, and its Desc reuses the filtered
	// text verbatim: the raw tool arguments may embed sensitive words that
	// redaction just stripped, and a desc built from sig.Args directly would
	// smuggle them past the denylist. Conflict and timeout notices stay plain
	// text — they carry no pending action to highlight.
	if sig.Kind == "created" {
		out.Card = &channels.Card{Title: "危险操作待确认", Desc: out.Text}
	}
	span.SetAttributes(attribute.String("decision", signalDecision(sig).decision))
	g.syncAudit(msg, signalDecision(sig))
	return out
}

// SensitiveWordInput denies messages containing any of the words.
func SensitiveWordInput(words []string) InputChecker {
	return func(_ context.Context, msg channels.InboundMessage) (string, bool) {
		for _, w := range words {
			if w != "" && strings.Contains(msg.Text, w) {
				return w, true
			}
		}
		return "", false
	}
}

// redactRules are the default output desensitization patterns: phone numbers,
// ID card numbers and email addresses are masked before a reply leaves the
// platform.
var redactRules = []*regexp.Regexp{
	regexp.MustCompile(`1[3-9]\d{9}`),             // phone number
	regexp.MustCompile(`\d{17}[\dXx]`),            // ID card number
	regexp.MustCompile(`[\w.+-]+@[\w-]+\.[\w.]+`), // email address
}

// RedactOutput masks sensitive patterns (phone / ID card / email) in replies.
func RedactOutput() OutputChecker {
	return func(_ context.Context, msg channels.InboundMessage, reply string) string {
		masked := reply
		for _, re := range redactRules {
			masked = re.ReplaceAllString(masked, "***")
		}
		if masked != reply {
			plog.Debugf("output redacted (session=%s)", msg.SessionKey)
		}
		return masked
	}
}

// handleRecall audits the recall and marks the session state. The recalled
// message id arrives as "recall:{msgid}" (the adapter namespaces it away from
// the original message's dedup key).
func (g *Guarded) handleRecall(ctx context.Context, msg channels.InboundMessage) {
	recalled := strings.TrimPrefix(msg.MsgID, "recall:")
	detail, _ := json.Marshal(map[string]string{"recalled_msg_id": recalled})
	g.syncAuditDetail(msg, auditDecision{decision: "recall"}, detail)
	if g.StateMark == nil {
		return
	}
	mark, _ := json.Marshal(map[string]any{"at": time.Now().Unix()})
	if err := g.StateMark(ctx, msg, "recalled:"+recalled, mark); err != nil {
		plog.Warnf("recall state mark failed (session=%s): %v", msg.SessionKey, err)
	}
}

// signalDecision maps an interception signal to its audit event. Re-hitting
// an already-pending call (Fresh=false) audits nothing: the review was
// recorded when the pending was first created.
func signalDecision(sig Signal) auditDecision {
	switch sig.Kind {
	case "created":
		if !sig.Fresh {
			return auditDecision{}
		}
		return auditDecision{decision: "review", toolName: sig.ToolName}
	case "conflict":
		return auditDecision{decision: "deny", errorType: "approval_conflict", toolName: sig.ToolName}
	case "timeout":
		return auditDecision{decision: "review_timeout", toolName: sig.ToolName}
	}
	return auditDecision{}
}

// signalReply composes the deterministic user-facing notice for an
// interception, replacing whatever the LLM said.
func signalReply(sig Signal) string {
	switch sig.Kind {
	case "created":
		remaining := time.Until(sig.Deadline).Round(time.Second)
		return fmt.Sprintf("⚠️ 检测到危险操作，待您确认：\n• 工具：%s\n• 参数：%s\n请在 %s 内回复「确认」执行，或回复「拒绝」取消。",
			sig.ToolName, sig.Args, remaining)
	case "conflict":
		return fmt.Sprintf("当前已有待确认的操作（工具：%s），请先回复「确认」或「拒绝」完成该审批，再发起新的危险操作。", sig.Pending)
	case "timeout":
		return fmt.Sprintf("之前待确认的操作（工具：%s）已超时作废，如需执行请重新发起。", sig.ToolName)
	}
	return ""
}

// asyncAudit records the routine (allow) decision for a processed message,
// off the request path, with the run's token usage and cost attached.
// Messages that bypassed tenant routing (dev fallback) are filed under the
// zero UUID.
func (g *Guarded) asyncAudit(msg channels.InboundMessage, out channels.OutboundMessage, started time.Time, processErr error) {
	if g.Auditor == nil {
		return
	}
	ev := storage.AuditEvent{
		TenantID:         tenantOrZero(msg.TenantID),
		Channel:          msg.Channel,
		UserID:           msg.UserID,
		Decision:         "allow",
		LatencyMs:        int(time.Since(started).Milliseconds()),
		TraceID:          msg.TraceID,
		PromptTokens:     out.PromptTokens,
		CompletionTokens: out.CompletionTokens,
		Cost:             CostUSD(out.Model, out.PromptTokens, out.CompletionTokens),
	}
	if processErr != nil {
		ev.ErrorType = "process_error"
	}
	g.Auditor.LogAsync(ev)
}

// asyncModelAudit records a degraded model failure: the decision stays
// "allow" (the message itself was let through) and error_type carries the
// cause — model_timeout vs model_error. Tokens burned before the failure are
// still accounted.
func (g *Guarded) asyncModelAudit(msg channels.InboundMessage, out channels.OutboundMessage, started time.Time, mErr *ModelError) {
	if g.Auditor == nil {
		return
	}
	errorType := "model_error"
	if errors.Is(mErr.Err, context.DeadlineExceeded) {
		errorType = "model_timeout"
	}
	g.Auditor.LogAsync(storage.AuditEvent{
		TenantID:         tenantOrZero(msg.TenantID),
		Channel:          msg.Channel,
		UserID:           msg.UserID,
		Decision:         "allow",
		LatencyMs:        int(time.Since(started).Milliseconds()),
		ErrorType:        errorType,
		TraceID:          msg.TraceID,
		PromptTokens:     out.PromptTokens,
		CompletionTokens: out.CompletionTokens,
		Cost:             CostUSD(out.Model, out.PromptTokens, out.CompletionTokens),
	})
}

// syncAudit writes a critical guardrail decision synchronously: deny /
// review / review_timeout / dangerous-tool execution must not be lost, even
// at the cost of milliseconds of latency (compliance red line).
func (g *Guarded) syncAudit(msg channels.InboundMessage, dec auditDecision) {
	g.syncAuditDetail(msg, dec, nil)
}

// syncAuditDetail is syncAudit with an optional detail payload (recall events
// carry the recalled message id).
func (g *Guarded) syncAuditDetail(msg channels.InboundMessage, dec auditDecision, detail json.RawMessage) {
	if g.Auditor == nil || dec.decision == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := g.Auditor.LogSync(ctx, storage.AuditEvent{
		TenantID:  tenantOrZero(msg.TenantID),
		Channel:   msg.Channel,
		UserID:    msg.UserID,
		ToolName:  dec.toolName,
		Decision:  dec.decision,
		ErrorType: dec.errorType,
		LatencyMs: dec.latencyMs,
		TraceID:   msg.TraceID,
		Detail:    detail,
	})
	if err != nil {
		plog.Errorf("sync audit %s failed: %v", dec.decision, err)
	}
}

func tenantOrZero(tenantID string) string {
	if tenantID == "" {
		return "00000000-0000-0000-0000-000000000000"
	}
	return tenantID
}

func firstNonEmpty(s, fallback string) string {
	if s != "" {
		return s
	}
	return fallback
}
