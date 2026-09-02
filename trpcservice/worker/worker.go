// Package worker consumes inbound messages from the bus, runs the bound
// agent via the tRPC-Agent-Go runner, and records the reply in the MySQL
// outbox. Idempotency (Redis fast path + MySQL marker in the outbox
// transaction) and the per-session Redis lock keep redeliveries and
// concurrent workers safe.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/bus"
	"github.com/liuzengh/trpc-agent-service/trpcservice/chat"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/skill"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	fwtool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// tracer names the platform's worker spans.
var tracer = otel.Tracer("trpc-agent-service/worker")

// withTraceID attaches the given trace id to ctx, so spans started on it join
// the trace the IM gateway stamped on the message.
func withTraceID(ctx context.Context, traceID string) context.Context {
	if traceID == "" {
		return ctx
	}
	tid, err := trace.TraceIDFromHex(traceID)
	if err != nil {
		return ctx
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		TraceFlags: trace.FlagsSampled,
	})
	return trace.ContextWithSpanContext(ctx, sc)
}

// Consumer group on stream:inbound shared by all worker nodes.
const Group = "workers"

// StateBus is the bus plus the cross-node session state the worker relies on;
// bus.RedisBus satisfies it.
type StateBus interface {
	bus.Bus
	// Idempotent atomically claims msgKey (SetNX): true = first claim, false =
	// already processed. ClearIdem releases the claim so a failed attempt can
	// be retried on redelivery.
	Idempotent(ctx context.Context, msgKey string) (bool, error)
	ClearIdem(ctx context.Context, msgKey string) error
	SeenIdem(ctx context.Context, msgKey string) (bool, error)
	MarkIdem(ctx context.Context, msgKey string) error
	Route(ctx context.Context, tenantID, sessionID string) (string, error)
	SetRoute(ctx context.Context, tenantID, sessionID, agentID string) error
	LockSession(ctx context.Context, tenantID, sessionID, token string) (bool, error)
	UnlockSession(ctx context.Context, tenantID, sessionID, token string) error
	// RefreshLock extends the session lock TTL when token still owns it; a
	// long approval wait must not let the lock expire under the worker.
	RefreshLock(ctx context.Context, tenantID, sessionID, token string) (bool, error)
	// Approval state: at most one pending human approval per session.
	SetPendingApproval(ctx context.Context, tenantID, sessionID, payload string, ttl time.Duration) error
	PendingApproval(ctx context.Context, tenantID, sessionID string) (string, error)
	ClearPendingApproval(ctx context.Context, tenantID, sessionID string) error
	ResolveApproval(ctx context.Context, tenantID, sessionID, decision string) error
	ApprovalResult(ctx context.Context, tenantID, sessionID string) (string, error)
}

// ToolSource resolves a registered tool id to its runtime implementation.
type ToolSource func(id string) (fwtool.Tool, bool)

// Worker turns inbound messages into agent replies.
type Worker struct {
	bus       StateBus
	agents    *agent.Manager
	tools     *tool.Registry
	toolSrc   ToolSource
	outbox    *bus.Outbox
	sessions  *storage.Router    // optional: per-tenant session backend
	knowledge *knowledge.Manager // optional: KB search tools
	skills    *skill.Manager     // optional: mounted skills -> instruction splice
	auditor   audit.Recorder     // optional: audit log
	artifacts artifact.Service   // optional: code-execution artifacts (MinIO)
	ledger    chat.Ledger        // optional: business conversation ledger

	budget     *governance.Budget     // optional: tenant token quota
	permission *governance.Permission // optional: IM user allow-list
}

// New assembles a worker. sessions, kbs, skills, auditor, artifacts and
// ledger may be nil (no multi-turn persistence / no knowledge bases / no
// skills / no audit / no artifact persistence / no chat ledger).
func New(b StateBus, agents *agent.Manager, tools *tool.Registry, toolSrc ToolSource, outbox *bus.Outbox, sessions *storage.Router, kbs *knowledge.Manager, skills *skill.Manager, auditor audit.Recorder, artifacts artifact.Service, ledger chat.Ledger) *Worker {
	return &Worker{bus: b, agents: agents, tools: tools, toolSrc: toolSrc, outbox: outbox, sessions: sessions, knowledge: kbs, skills: skills, auditor: auditor, artifacts: artifacts, ledger: ledger}
}

// SetGovernance wires the tenant-level budget and IM-user permission checks.
// May be nil (checks disabled).
func (w *Worker) SetGovernance(budget *governance.Budget, permission *governance.Permission) {
	w.budget = budget
	w.permission = permission
}

// Run joins the consumer group and blocks until ctx is done. The consumer name
// is unique per process so XAUTOCLAIM can tell dead consumers apart.
func (w *Worker) Run(ctx context.Context) error {
	host, _ := os.Hostname()
	consumer := fmt.Sprintf("%s-%d", host, os.Getpid())
	return w.bus.ConsumeInbound(ctx, Group, consumer, w.handle)
}

// handle processes one inbound message. An error leaves the message pending
// for redelivery; nil acks it.
func (w *Worker) handle(ctx context.Context, m *bus.Message) error {
	if m == nil || m.ID == "" || m.Content == nil {
		return nil // malformed envelope: ack and drop
	}
	metrics.InboundMessage(ctx, m.TenantID, m.Channel)

	// Atomic dedup (Redis SetNX): the first claim wins, so concurrent
	// redeliveries of the same message are dropped before any work. The old
	// SeenIdem-check + MarkIdem-commit left a window where redeliveries passed
	// the check and each wrote an audit row.
	first, err := w.bus.Idempotent(ctx, m.ID)
	if err != nil {
		return err // transient Redis error: retry later
	}
	if !first {
		return nil // duplicate redelivery
	}
	// fail releases the idempotency claim so a transient failure is retried on
	// redelivery, rather than being silently dropped.
	fail := func(err error) error {
		_ = w.bus.ClearIdem(ctx, m.ID)
		return err
	}

	// A human approval reply resolves the pending approval of the session.
	// This runs BEFORE the session lock is taken: while an agent turn is
	// blocked waiting for the decision, its own reply must still get through.
	if handled, err := w.tryResolveApproval(ctx, m); err != nil {
		return fail(err)
	} else if handled {
		return nil // consumed as an approval decision, not an agent turn
	}

	agentID := m.AgentID
	if agentID == "" {
		var err error
		agentID, err = w.bus.Route(ctx, m.TenantID, m.SessionID)
		if err != nil {
			return fail(err)
		}
		if agentID == "" {
			// No agent bound for this session and none on the message: a
			// configuration gap, not a transient failure. Drop.
			slog.Warn("worker: no agent bound, dropping message", "tenant", m.TenantID, "session", m.SessionID)
			return nil
		}
	} else {
		if err := w.bus.SetRoute(ctx, m.TenantID, m.SessionID, agentID); err != nil {
			return fail(err)
		}
	}

	// IM user permission gate: deny unauthorized users before any work. A
	// denial is a policy outcome (drop), not a transient failure (no retry).
	if w.permission != nil && !w.permission.Check(ctx, m.TenantID, m.UserID) {
		slog.Warn("worker: IM user not allowed, dropping message", "tenant", m.TenantID, "user", m.UserID)
		return nil
	}

	// Serialize handling of one session across nodes.
	token := uuid.NewString()
	ok, err := w.bus.LockSession(ctx, m.TenantID, m.SessionID, token)
	if err != nil {
		return fail(err)
	}
	if !ok {
		return fail(fmt.Errorf("worker: session %s busy", m.SessionID)) // stays pending, retried
	}
	defer func() {
		_ = w.bus.UnlockSession(ctx, m.TenantID, m.SessionID, token)
	}()

	start := time.Now()
	reply, toolNames, tokens, err := w.run(ctx, agentID, m, token)
	dur := time.Since(start)
	if err != nil {
		metrics.AgentError(ctx, m.TenantID, agentID)
		w.recordAudit(m, agentID, audit.DecisionFailed, dur, err, nil, 0)
		return fail(err) // transient (LLM/tool failure): redeliver and retry
	}
	metrics.AgentRun(ctx, m.TenantID, agentID, dur)
	w.recordAudit(m, agentID, audit.DecisionExecuted, dur, nil, toolNames, tokens)
	if reply == nil {
		return nil // nothing to send back
	}

	if err := w.outbox.Append(ctx, reply, m.ID); err != nil {
		if errors.Is(err, bus.ErrDuplicateIdem) {
			return nil // already processed before a crash: ack
		}
		return fail(err)
	}
	w.recordLedger(ctx, m, agentID, reply)
	return nil
}

// recordUsage meters the turn's token consumption into usage_records,
// best-effort and idempotent: record_id is derived from the inbound message id
// + dimension, so a redelivered turn never double-counts.
func (w *Worker) recordUsage(ctx context.Context, m *bus.Message, agentID string, tokens int64) {
	if w.auditor == nil || m == nil || tokens <= 0 {
		return
	}
	err := w.auditor.RecordUsage(ctx, audit.UsageEntry{
		RecordID:  m.ID + ":" + audit.UsageDimensionToken,
		TenantID:  m.TenantID,
		AgentID:   agentID,
		Dimension: audit.UsageDimensionToken,
		Amount:    float64(tokens),
	})
	if err != nil {
		slog.Warn("worker: usage metering skipped (best-effort)", "session", m.SessionID, "err", err)
	}
}

// recordLedger writes the USER + ASSISTANT rows of the finished turn into the
// business conversation ledger, best-effort: a ledger failure must never fail
// or retry the reply flow (redelivery is idempotent by message_id).
func (w *Worker) recordLedger(ctx context.Context, m *bus.Message, agentID string, reply *bus.Message) {
	if w.ledger == nil || m == nil || m.Content == nil || reply == nil || reply.Content == nil {
		return
	}
	turn := chat.Turn{
		TenantID:   m.TenantID,
		AgentID:    agentID,
		SessionID:  m.SessionID,
		MemberID:   m.UserID,
		Channel:    m.Channel,
		UserMsgID:  m.ID,
		UserText:   m.Content.Content,
		ReplyMsgID: reply.ID,
		ReplyText:  reply.Content.Content,
		TurnID:     uuid.NewString(),
		TurnTS:     time.Now().UnixMilli(),
	}
	if err := w.ledger.RecordTurn(ctx, turn); err != nil {
		slog.Warn("worker: chat ledger write failed (best-effort)", "session", m.SessionID, "err", err)
	}
}

// recordAudit writes one audit entry for an agent run, when an auditor is
// wired. AgentName carries the agent id (the worker resolves only the id; the
// durable agent name lives in the management domain). ToolName lists the tools
// the turn actually invoked (comma-joined); Cost carries the turn's token
// consumption (token count, not currency — no price table).
func (w *Worker) recordAudit(m *bus.Message, agentID, decision string, dur time.Duration, runErr error, toolNames []string, tokens int64) {
	if w.auditor == nil {
		return
	}
	entry := audit.Entry{
		TenantID:  m.TenantID,
		Channel:   m.Channel,
		UserID:    m.UserID,
		SessionID: m.SessionID,
		AgentName: agentID,
		ToolName:  strings.Join(toolNames, ","),
		Decision:  decision,
		Latency:   dur,
		Cost:      float64(tokens),
		TraceID:   m.TraceID,
	}
	if runErr != nil {
		entry.ErrorType = runErr.Error()
	}
	w.auditor.Record(entry)
}

// run builds the agent from its current runtime profile and executes one turn.
// lockToken is the session lock the worker holds; a pending human approval
// refreshes it while waiting.
func (w *Worker) run(ctx context.Context, agentID string, m *bus.Message, lockToken string) (*bus.Message, []string, int64, error) {
	// Share the trace id the IM gateway stamped on the message, so the
	// agent.run span (and its Runner/Tool/Session children) join the same
	// trace as im.callback / im.reply in Jaeger.
	ctx = withTraceID(ctx, m.TraceID)
	ctx, span := tracer.Start(ctx, "agent.run",
		trace.WithAttributes(
			attribute.String("tenant_id", m.TenantID),
			attribute.String("agent_id", agentID),
			attribute.String("session_id", m.SessionID),
			attribute.String("channel", m.Channel),
			attribute.String("trace_id", m.TraceID),
		),
	)
	defer span.End()

	// Resolve the profile once: tools / KB tools / skill instruction all hang
	// off it, so the worker avoids re-reading the store per concern.
	profile, err := w.agents.Resolve(ctx, agentID)
	if err != nil {
		return nil, nil, 0, err
	}

	// Tenant token budget: reject the turn before spending any model/tool cost.
	if w.budget != nil {
		exceeded, err := w.budget.Exceeded(ctx, m.TenantID)
		if err != nil {
			slog.Warn("worker: budget check failed", "tenant", m.TenantID, "err", err)
		} else if exceeded {
			return nil, nil, 0, fmt.Errorf("worker: tenant %s token budget exceeded", m.TenantID)
		}
	}

	tools, approvalToolNames := w.toolsFromProfile(ctx, agentID, profile)
	tools = append(tools, w.resolveKnowledgeTools(ctx, agentID)...)
	instruction := w.skillInstruction(ctx, profile)
	ag, err := w.agents.BuildFromProfile(ctx, agentID, profile, tools, instruction)
	if err != nil {
		return nil, nil, 0, err
	}

	// A human approval plugin pauses tool calls that the profile marked for
	// approval. Built per turn because the tool-name set follows the profile.
	plugin := w.approvalPlugin(ctx, m, approvalToolNames, lockToken)

	var opts []runner.Option
	if plugin != nil {
		opts = append(opts, runner.WithPlugins(plugin))
	}
	// Redaction always runs so sensitive data never reaches the model verbatim.
	opts = append(opts, runner.WithPlugins(governance.NewRedactionFilter()))
	if w.artifacts != nil {
		opts = append(opts, runner.WithArtifactService(w.artifacts))
	}
	if w.sessions != nil {
		s, err := w.sessions.Sessions(ctx, m.TenantID)
		if err != nil {
			slog.Warn("worker: session backend unavailable, running stateless",
				"tenant", m.TenantID, "err", err)
		} else {
			opts = append(opts, runner.WithSessionService(s.Service()))
		}
	}

	r := runner.NewRunner(m.TenantID, ag, opts...)
	defer func() { _ = r.Close() }()

	events, err := r.Run(ctx, m.UserID, m.SessionID, *m.Content)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("worker: run agent %q: %w", agentID, err)
	}
	text, tokens, toolNames, toolDur, err := finalTextWithUsage(events)
	if err != nil {
		return nil, nil, 0, err
	}
	metrics.TokenUsage(ctx, m.TenantID, tokens)
	if toolDur > 0 {
		metrics.ToolCallDuration(ctx, m.TenantID, agentID, toolDur)
	}
	w.recordUsage(ctx, m, agentID, tokens)
	if text == "" {
		return nil, nil, 0, nil
	}
	reply := model.NewAssistantMessage(text)

	return &bus.Message{
		ID:        uuid.NewString(),
		TraceID:   m.TraceID,
		TenantID:  m.TenantID,
		AgentID:   agentID,
		SessionID: m.SessionID,
		Channel:   m.Channel,
		UserID:    m.UserID,
		Content:   &reply,
		ReplyTo:   m.ID,
	}, toolNames, tokens, nil
}

// toolsFromProfile returns the tool implementations the agent may use (the
// profile's tool ids intersected with the RBAC grants) plus the set of tool
// names whose calls need human approval. Approval is triggered by either of
// the two rails: the tool is listed in profile.ApprovalToolIDs, or its
// definition is risk_level=high.
func (w *Worker) toolsFromProfile(ctx context.Context, agentID string, profile agent.RuntimeProfile) ([]fwtool.Tool, map[string]bool) {
	if len(profile.ToolIDs) == 0 || w.toolSrc == nil || w.tools == nil {
		return nil, nil
	}
	manuallyApproved := make(map[string]bool, len(profile.ApprovalToolIDs))
	for _, id := range profile.ApprovalToolIDs {
		manuallyApproved[id] = true
	}
	var out []fwtool.Tool
	approvalNames := make(map[string]bool)
	for _, id := range profile.ToolIDs {
		allowed, err := w.tools.IsAllowed(ctx, agentID, id)
		if err != nil || !allowed {
			continue
		}
		t, ok := w.toolSrc(id)
		if !ok {
			continue
		}
		out = append(out, t)
		def, err := w.tools.Get(ctx, id)
		if err != nil {
			continue
		}
		if manuallyApproved[id] || def.RiskLevel == tool.RiskHigh {
			approvalNames[def.Name] = true
		}
	}
	return out, approvalNames
}

// resolveKnowledgeTools returns one search tool per KB mounted on the agent's
// profile. The first tool keeps the framework's default name; extra KBs get
// numbered names so the LLM can address them separately.
func (w *Worker) resolveKnowledgeTools(ctx context.Context, agentID string) []fwtool.Tool {
	if w.knowledge == nil {
		return nil
	}
	profile, err := w.agents.Resolve(ctx, agentID)
	if err != nil || len(profile.KnowledgeIDs) == 0 {
		return nil
	}
	var out []fwtool.Tool
	for i, kbID := range profile.KnowledgeIDs {
		name := "knowledge_search"
		if i > 0 {
			name = fmt.Sprintf("knowledge_search_%d", i+1)
		}
		t, err := w.knowledge.SearchTool(ctx, kbID, name)
		if err != nil {
			// A missing KB must not take down the whole agent run.
			slog.Warn("worker: knowledge tool unavailable", "kb", kbID, "err", err)
			continue
		}
		out = append(out, t)
	}
	return out
}

// skillInstruction splices the text of the skills mounted on the agent's
// profile into an instruction fragment appended to the system prompt. Each
// skill renders its SKILL.md body, or its prompt_template when the body is
// empty. When skills are not wired (nil) the result is empty.
func (w *Worker) skillInstruction(ctx context.Context, profile agent.RuntimeProfile) string {
	if w.skills == nil || len(profile.SkillIDs) == 0 {
		return ""
	}
	loaded, err := w.skills.LoadByIDs(ctx, profile.SkillIDs)
	if err != nil {
		slog.Warn("worker: skill load failed", "err", err)
		return ""
	}
	if len(loaded) == 0 {
		return ""
	}
	var b strings.Builder
	for _, s := range loaded {
		text := s.ContentMD
		if text == "" {
			text = s.PromptTemplate
		}
		if text == "" {
			continue
		}
		fmt.Fprintf(&b, "\n===== Skill: %s (v%d) =====\n%s\n===== End Skill: %s =====\n",
			s.Code, s.Version, text, s.Code)
	}
	return b.String()
}

// finalTextWithUsage extracts the last non-partial assistant text, the summed
// token usage, the distinct tool names invoked and the total tool-call latency
// (first tool call to last tool response), from the event stream. A nil
// *event.Event is skipped (the channel may close early).
func finalTextWithUsage(events <-chan *event.Event) (string, int64, []string, time.Duration, error) {
	var last string
	var tokens int64
	var toolNames []string
	var toolStart, toolEnd time.Time
	seen := make(map[string]struct{})
	for evt := range events {
		if evt == nil {
			continue
		}
		if evt.Error != nil {
			return "", tokens, toolNames, 0, fmt.Errorf("worker: agent error: %s", evt.Error.Message)
		}
		if evt.Usage != nil {
			tokens += int64(evt.Usage.PromptTokens + evt.Usage.CompletionTokens)
		}
		// Collect tool-call names (present on the assistant message that
		// requests the call), deduplicated in order.
		for _, c := range evt.Choices {
			for _, tc := range c.Message.ToolCalls {
				if tc.Function.Name == "" {
					continue
				}
				if _, ok := seen[tc.Function.Name]; !ok {
					seen[tc.Function.Name] = struct{}{}
					toolNames = append(toolNames, tc.Function.Name)
				}
				if toolStart.IsZero() {
					toolStart = evt.Timestamp
				}
			}
		}
		if evt.IsToolCallResponse() && toolEnd.IsZero() {
			toolEnd = evt.Timestamp
		}
		if evt.IsPartial || evt.IsToolCallResponse() {
			continue
		}
		for _, c := range evt.Choices {
			if c.Message.Role == model.RoleAssistant && c.Message.Content != "" {
				last = c.Message.Content
			}
		}
	}
	var toolDur time.Duration
	if !toolStart.IsZero() && toolEnd.After(toolStart) {
		toolDur = toolEnd.Sub(toolStart)
	}
	return last, tokens, toolNames, toolDur, nil
}
