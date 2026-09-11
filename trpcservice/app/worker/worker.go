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

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/chat"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/skill"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	fwtool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// tracer names the platform's worker spans.
var tracer = otel.Tracer("trpc-agent-service/worker")

// Consumer group on stream:inbound shared by all worker nodes.
const Group = "workers"

// runTimeout bounds one agent turn end-to-end. A turn may issue several model
// calls (tool loop), each individually capped by the model HTTP timeout; this
// is the outer budget that prevents a pathological turn from blocking the IM
// user indefinitely. Default is generous enough for a deep reasoning model.
const runTimeout = 300 * time.Second

// Idempotency is the cross-node dedup contract. Idempotent atomically claims
// msgKey (SetNX) as a *lease* — true = first claim, false = another worker
// holds or already committed it. CommitIdem makes the claim durable once the
// work is recorded; ClearIdem releases it so a failed attempt can be retried on
// redelivery; ExpireIdemLease keeps a long turn's claim alive.
type Idempotency interface {
	Idempotent(ctx context.Context, msgKey string) (bool, error)
	CommitIdem(ctx context.Context, msgKey string) error
	ClearIdem(ctx context.Context, msgKey string) error
	ExpireIdemLease(ctx context.Context, msgKey string) (bool, error)
}

// SessionRouter binds sessions to agents so stateless workers resolve the
// right agent for any session.
type SessionRouter interface {
	Route(ctx context.Context, tenantID, sessionID string) (string, error)
	SetRoute(ctx context.Context, tenantID, sessionID, agentID string) error
}

// SessionLocker serializes handling of one session across nodes.
type SessionLocker interface {
	LockSession(ctx context.Context, tenantID, sessionID, token string) (bool, error)
	UnlockSession(ctx context.Context, tenantID, sessionID, token string) error
	// RefreshLock extends the session lock TTL when token still owns it; a
	// long approval wait must not let the lock expire under the worker.
	RefreshLock(ctx context.Context, tenantID, sessionID, token string) (bool, error)
}

// ApprovalState is the cross-node approval state: at most one pending human
// approval per session.
type ApprovalState interface {
	SetPendingApproval(ctx context.Context, tenantID, sessionID, payload string, ttl time.Duration) error
	PendingApproval(ctx context.Context, tenantID, sessionID string) (string, error)
	ClearPendingApproval(ctx context.Context, tenantID, sessionID string) error
	ResolveApproval(ctx context.Context, tenantID, sessionID, decision string) error
	ApprovalResult(ctx context.Context, tenantID, sessionID string) (string, error)
}

// StateBus is the full cross-node state the worker relies on. It composes the
// message bus with the four narrower contracts (idempotency, routing,
// locking, approval) so one implementation (bus.RedisBus) satisfies it, while
// callers can still depend on only the slice they use.
type StateBus interface {
	bus.Bus
	Idempotency
	SessionRouter
	SessionLocker
	ApprovalState
}

// ToolSource resolves a registered tool id to its runtime implementation.
type ToolSource func(id string) (fwtool.Tool, bool)

// Worker turns inbound messages into agent replies.
type Worker struct {
	bus       StateBus
	agents    *agent.Manager
	toolRes   *toolResolver // optional: static tools + KB search tools
	outbox    *bus.Outbox
	sessions  *storage.Router  // optional: per-tenant session backend
	skills    *skill.Manager   // optional: mounted skills -> instruction splice
	auditor   audit.Recorder   // optional: audit log
	artifacts artifact.Service // optional: code-execution artifacts (MinIO)
	ledger    chat.Ledger      // optional: business conversation ledger

	tenants TenantSource                                              // optional: tenant governance config source
	usage   func(ctx context.Context, tenantID string) (int64, error) // optional: token meter for budget checks
}

// New assembles a worker. toolRes, sessions, skills, auditor, artifacts and
// ledger may be nil (no tools / no multi-turn persistence / no skills / no
// audit / no artifact persistence / no chat ledger).
func New(b StateBus, agents *agent.Manager, toolRes *toolResolver, outbox *bus.Outbox, sessions *storage.Router, skills *skill.Manager, auditor audit.Recorder, artifacts artifact.Service, ledger chat.Ledger) *Worker {
	return &Worker{bus: b, agents: agents, toolRes: toolRes, outbox: outbox, sessions: sessions, skills: skills, auditor: auditor, artifacts: artifacts, ledger: ledger}
}

// SetGovernance wires the per-tenant governance source and the token meter
// used for budget checks. tenants may be nil (governance disabled — safe
// defaults apply); usage may be nil (budget never enforced even if a tenant
// configures a quota).
func (w *Worker) SetGovernance(tenants TenantSource, usage func(ctx context.Context, tenantID string) (int64, error)) {
	w.tenants = tenants
	w.usage = usage
}

// Run joins the consumer group and blocks until ctx is done. The consumer name
// is unique per process so XAUTOCLAIM can tell dead consumers apart.
func (w *Worker) Run(ctx context.Context) error {
	host, _ := os.Hostname()
	consumer := fmt.Sprintf("%s-%d", host, os.Getpid())
	return w.bus.ConsumeInbound(ctx, Group, consumer, w.handle)
}

// startTurnHeartbeat keeps this worker's claims alive until the returned stop
// function is called: the session lock (mutual exclusion) and the idempotency
// lease (ownership of the message). A turn may run up to runTimeout and wait
// approvalTimeout on top of that, while the lock TTL is 30s and the lease TTL
// 90s, so without a heartbeat both would expire mid-turn — the lock allowing a
// second message into the session, the lease letting a redelivery re-run a turn
// that is still in flight.
//
// The heartbeat logs once when it loses the lock: that means the turn outlived
// its lock (or Redis dropped the key), so concurrent execution is possible and
// the operator should see it rather than discover it in the data.
func (w *Worker) startTurnHeartbeat(ctx context.Context, m *bus.Message, token string) func() {
	tenantID, sessionID, msgID := m.TenantID, m.SessionID, m.ID
	// One ticker refreshes both claims, so it must run at the faster cadence.
	interval := bus.SessionLockRefreshInterval()
	if lease := bus.IdemLeaseRefreshInterval(); interval <= 0 || (lease > 0 && lease < interval) {
		interval = lease
	}
	if interval <= 0 {
		return func() {}
	}
	hbCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		lockLost, leaseLost := false, false
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-ticker.C:
				// Detached from the turn context on purpose: a cancelled turn
				// still has to keep its claims until it releases them, otherwise
				// they expire while the deferred releases have not run yet.
				refreshCtx, cancelRefresh := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				if !lockLost {
					held, err := w.bus.RefreshLock(refreshCtx, tenantID, sessionID, token)
					switch {
					case err != nil:
						slog.Warn("worker: session lock refresh failed", "tenant", tenantID, "session", sessionID, "err", err)
					case !held:
						slog.Warn("worker: session lock lost, concurrent execution is possible",
							"tenant", tenantID, "session", sessionID)
						lockLost = true
						// Mutual exclusion is gone: this is a governance event,
						// not just an operational log line.
						w.recordAudit(m, m.AgentID, audit.DecisionFailed, 0,
							errLockLost, nil, 0)
					}
				}
				if !leaseLost && msgID != "" {
					held, err := w.bus.ExpireIdemLease(refreshCtx, msgID)
					switch {
					case err != nil:
						slog.Warn("worker: idempotency lease refresh failed", "message", msgID, "err", err)
					case !held:
						slog.Warn("worker: idempotency lease lost", "message", msgID)
						leaseLost = true
					}
				}
				cancelRefresh()
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// errLockLost marks a turn that outlived its own session lock. The audit error
// class for it is "lock_lost" so it is distinguishable from a model failure.
var errLockLost = errors.New("session lock lost during turn")

// handle processes one inbound message. An error leaves the message pending
// for redelivery; nil acks it.
func (w *Worker) handle(ctx context.Context, m *bus.Message) error {
	if m == nil || m.ID == "" || m.Content == nil {
		return nil // malformed envelope: ack and drop
	}
	metrics.InboundMessage(ctx, m.TenantID, m.Channel)

	// Atomic dedup: the first claim wins, so concurrent redeliveries of the
	// same message are dropped before any work. The claim is a *lease*: it is
	// only made durable (commit) once this attempt has produced a recorded
	// outcome, so a worker that dies mid-turn does not swallow the message —
	// the lease expires, the redelivery reprocesses the turn, and the durable
	// MySQL marker keeps the reprocess from double-replying.
	first, err := w.bus.Idempotent(ctx, m.ID)
	if err != nil {
		return err // transient Redis error: retry later
	}
	if !first {
		return nil // another worker holds or completed this message
	}
	// fail releases the claim so a transient failure is retried on redelivery,
	// rather than being silently dropped.
	fail := func(err error) error {
		_ = w.bus.ClearIdem(ctx, m.ID)
		return err
	}
	// done records the outcome durably: the message was handled (reply queued,
	// or deliberately dropped) and must never be processed again. A commit
	// failure is not fatal — the lease still holds the message for a while, and
	// the worst case is one redundant reprocess blocked by the outbox marker.
	done := func() {
		if err := w.bus.CommitIdem(ctx, m.ID); err != nil {
			slog.Warn("worker: idempotency commit failed", "message", m.ID, "err", err)
		}
	}

	// A human approval reply resolves the pending approval of the session.
	// This runs BEFORE the session lock is taken: while an agent turn is
	// blocked waiting for the decision, its own reply must still get through.
	if handled, err := w.tryResolveApproval(ctx, m); err != nil {
		return fail(err)
	} else if handled {
		done()
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
			done()
			return nil
		}
	}

	// Tenant guard, applied before the route is written: the agent must belong
	// to the tenant that asked for it. The chat API and the IM gateway both
	// carry the tenant on the message, so this is the chokepoint that stops a
	// crafted agent_id from running (and writing a session under) another
	// tenant's agent — and from poisoning the session route with it. A mismatch
	// is a policy refusal, not a transient failure: drop instead of redelivering
	// a message that can never succeed.
	agDef, err := w.agents.Get(ctx, agentID)
	if err != nil {
		return fail(fmt.Errorf("worker: agent %s not found: %w", agentID, err))
	}
	if agDef.TenantID != m.TenantID {
		slog.Error("worker: agent belongs to another tenant, dropping message",
			"agent", agentID, "agent_tenant", agDef.TenantID, "message_tenant", m.TenantID)
		w.recordAudit(m, agentID, audit.DecisionDeny, 0,
			fmt.Errorf("tenant mismatch: agent %s", agentID), nil, 0)
		done()
		return nil
	}

	if m.AgentID != "" {
		if err := w.bus.SetRoute(ctx, m.TenantID, m.SessionID, agentID); err != nil {
			return fail(err)
		}
	}

	// Resolve the tenant's governance snapshot once per message and share it
	// with run via the context, so the IM allow-list (here) and the budget /
	// tool whitelist / approval-union / redaction checks (run) agree.
	policy := w.resolveTenantPolicy(ctx, m.TenantID)
	ctx = withTenantPolicy(ctx, policy)

	// IM user permission gate: deny unauthorized IM users before any work. A
	// denial is a policy outcome (drop), not a transient failure (no retry).
	// The allow-list applies to IM-originated messages only; platform console
	// (admin) traffic is authenticated by the platform, not a tenant policy.
	if !policy.imUserAllowed(m.Channel, m.UserID) {
		slog.Warn("worker: IM user not allowed, dropping message", "tenant", m.TenantID, "user", m.UserID)
		w.recordAudit(m, agentID, audit.DecisionDeny, 0,
			fmt.Errorf("IM user %s not in the tenant allow-list", m.UserID), nil, 0)
		done()
		return nil
	}

	// Serialize handling of one session across nodes.
	token := uuid.NewString()
	ok, err := w.bus.LockSession(ctx, m.TenantID, m.SessionID, token)
	if err != nil {
		return fail(err)
	}
	if !ok {
		// A busy session is transient contention, not a bad message: requeue it
		// without counting toward the dead-letter threshold, otherwise a burst on
		// one session would dead-letter healthy messages. Releasing the
		// idempotency claim lets the retry re-claim it.
		slog.Debug("worker: session busy, will retry", "tenant", m.TenantID, "session", m.SessionID)
		_ = w.bus.ClearIdem(ctx, m.ID)
		return bus.ErrRequeue
	}
	// Keep both claims alive for the whole turn (see startTurnHeartbeat).
	stopHeartbeat := w.startTurnHeartbeat(ctx, m, token)
	defer func() {
		stopHeartbeat()
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
		done()
		return nil // nothing to send back
	}

	if err := w.outbox.Append(ctx, reply, m.ID); err != nil {
		if errors.Is(err, bus.ErrDuplicateIdem) {
			done()
			return nil // already processed before a crash: ack
		}
		return fail(err)
	}
	// The reply is durable now: only here may the claim become permanent.
	done()
	w.recordLedger(ctx, m, agentID, reply)
	return nil
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
		entry.ErrorType = classifyRunError(runErr)
	}
	w.auditor.Record(entry)
}

// classifyRunError maps a run error to a coarse audit error class. Raw error
// text (which may embed request internals) never reaches the audit trail
// verbatim; the full error remains available in operational logs.
func classifyRunError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, errLockLost):
		return "lock_lost"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "run_error"
	}
}

// run builds the agent from its current runtime profile and executes one turn.
// lockToken is the session lock the worker holds; a pending human approval
// refreshes it while waiting.
func (w *Worker) run(ctx context.Context, agentID string, m *bus.Message, lockToken string) (*bus.Message, []string, int64, error) {
	// Share the trace id the IM gateway stamped on the message, so the
	// agent.run span (and its Runner/Tool/Session children) join the same
	// trace as im.callback / im.reply in Jaeger.
	ctx = metrics.TraceContextFromID(ctx, m.TraceID)
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
	// off it, so the worker avoids re-reading the store per concern. A canary
	// release is applied here: the session id decides which version serves the
	// whole conversation, and the chosen version is recorded on the span so a
	// rollout can be told apart from the baseline in traces.
	profile, agentVersion, err := w.agents.ResolveForSession(ctx, agentID, m.SessionID)
	if err != nil {
		return nil, nil, 0, err
	}
	span.SetAttributes(attribute.Int("agent_version", agentVersion))

	// Tenant token budget: reject the turn before spending any model/tool
	// cost. The quota comes from the tenant's governance snapshot; a meter
	// failure logs and lets the turn proceed (availability over strictness).
	policy := tenantPolicyFrom(ctx)
	exceeded, err := budgetExceeded(ctx, policy.TokenQuota, m.TenantID, w.usage)
	if err != nil {
		slog.Warn("worker: budget check failed", "tenant", m.TenantID, "err", err)
	} else if exceeded {
		return nil, nil, 0, fmt.Errorf("worker: tenant %s token budget exceeded", m.TenantID)
	}

	var tools []fwtool.Tool
	var approvalToolNames map[string]bool
	if w.toolRes != nil {
		tools, approvalToolNames = w.toolRes.fromProfile(ctx, agentID, profile, policy)
		tools = append(tools, w.toolRes.knowledgeTools(ctx, profile)...)
	}
	// Long-term memory (per tenant + user, framework-provided): the memory
	// tools let the agent record what is worth remembering, and the preload
	// budget on the built agent injects what it already knows. Both are off
	// when the platform disables memory, so a disabled node pays no tool
	// schema tokens and no memory lookups.
	memSvc, memTools := w.memoryForTurn(ctx, m.TenantID)
	tools = append(tools, memTools...)
	// usedSkills lists which mounted skills were actually injected this turn
	// (loaded with content); the usage meter counts only those as "used".
	instruction, usedSkills := w.skillInstruction(ctx, profile)
	ag, err := w.agents.BuildFromProfile(ctx, agentID, profile, tools, instruction)
	if err != nil {
		return nil, nil, 0, err
	}

	// Assemble the per-turn runner configuration (approval plugin, model timer,
	// redaction, artifact meter, session backend).
	opts, timer, artMeter := w.buildRunnerOptions(ctx, m, policy, approvalToolNames, lockToken)
	if memSvc != nil {
		// The runner needs the memory service for the preload read path; the
		// agent's tools already hold it for writes.
		opts = append(opts, runner.WithMemoryService(memSvc))
	}

	r := runner.NewRunner(m.TenantID, ag, opts...)
	defer func() { _ = r.Close() }()

	// Bound the whole turn so a hung model/tool cannot stall the worker (the
	// IM user waits on this). The per-model HTTP timeout already caps a single
	// request; this is the end-to-end budget for a multi-call turn.
	runCtx, cancel := context.WithTimeout(ctx, runTimeout)
	defer cancel()

	events, err := r.Run(runCtx, m.UserID, m.SessionID, *m.Content)
	// Record model latency even when the run failed: model calls did happen
	// and a timeout spike is exactly what the metric should surface.
	if md := timer.Duration(); md > 0 {
		metrics.ModelCallDuration(ctx, m.TenantID, agentID, md)
	}
	if err != nil {
		return nil, nil, 0, fmt.Errorf("worker: run agent %q: %w", agentID, err)
	}
	text, tokens, toolNames, toolDur, toolCalls, err := finalTextWithUsage(events)
	if err != nil {
		return nil, nil, 0, err
	}
	w.recordTurnUsage(ctx, m, agentID, tokens, toolDur, toolCalls, usedSkills, artMeter)
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

// memoryForTurn resolves the tenant's long-term memory service and the memory
// tools the agent may call this turn.
//
// The service comes from the Router, so a tenant can be pinned to its own
// memory backend (tenant.data_backend.memory) while the framework keys every
// entry by <tenant, user> — memory never crosses a tenant boundary. Failures
// degrade to "no memory this turn" instead of failing the user's message: a
// missing Redis or MySQL must not silence the agent, and the session transcript
// (short-term context) is unaffected.
func (w *Worker) memoryForTurn(ctx context.Context, tenantID string) (memory.Service, []fwtool.Tool) {
	if w.sessions == nil || w.agents == nil || w.agents.MemoryPreload() == 0 {
		return nil, nil
	}
	start := time.Now()
	m, err := w.sessions.Memories(ctx, tenantID)
	metrics.SessionLatency(ctx, tenantID, time.Since(start))
	if err != nil {
		slog.Warn("worker: memory backend unavailable, turn runs without long-term memory",
			"tenant", tenantID, "err", err)
		return nil, nil
	}
	svc := m.Service()
	return svc, svc.Tools()
}

// buildRunnerOptions assembles the per-turn runner configuration: the human
// approval plugin (built per turn because the tool-name set follows the
// profile and tenant policy), the model-latency timer, sensitive-data
// redaction, the artifact-save meter, and the shared session backend. It
// returns the options plus the timer and artifact meter the caller uses to
// record model latency and artifact-save counts after the run.
func (w *Worker) buildRunnerOptions(ctx context.Context, m *bus.Message, policy *tenantPolicy, approvalNames map[string]bool, lockToken string) ([]runner.Option, *modelTimer, *artifactUsage) {
	var opts []runner.Option
	if plugin := w.approvalPlugin(ctx, m, approvalNames, lockToken); plugin != nil {
		opts = append(opts, runner.WithPlugins(plugin))
	}
	// The model timer brackets every model call of this turn so the platform
	// can report model latency separately from the end-to-end run duration.
	timer := newModelTimer()
	opts = append(opts, runner.WithPlugins(timer))
	// Sensitive-data redaction runs by default so tool args/results never
	// reach the model or audit log verbatim; a tenant may opt out.
	if p := redactionPlugin(policy); p != nil {
		opts = append(opts, runner.WithPlugins(p))
	}
	var artMeter *artifactUsage
	if w.artifacts != nil {
		// Count artifact saves for the usage meter while forwarding every
		// operation to the real service.
		artMeter = &artifactUsage{inner: w.artifacts}
		opts = append(opts, runner.WithArtifactService(artMeter))
	}
	if w.sessions != nil {
		// Time the shared session-backend access; a slow backend shows up as
		// platform.session_latency and is tenant-partitioned.
		sessStart := time.Now()
		s, err := w.sessions.Sessions(ctx, m.TenantID)
		metrics.SessionLatency(ctx, m.TenantID, time.Since(sessStart))
		if err != nil {
			slog.Warn("worker: session backend unavailable, running stateless",
				"tenant", m.TenantID, "err", err)
		} else {
			opts = append(opts, runner.WithSessionService(s.Service()))
		}
	}
	return opts, timer, artMeter
}

// recordTurnUsage records the finished turn's consumption: the
// token/cost/tool-latency metrics plus the idempotent usage_records entries
// (token + tool + sandbox + artifact + skill). Only positive dimensions are
// written, each keyed on m.ID + ":" + dimension so a redelivered turn never
// double-counts.
func (w *Worker) recordTurnUsage(ctx context.Context, m *bus.Message, agentID string, tokens int64, toolDur time.Duration, toolCalls map[string]int, usedSkills []skillUsageRef, artMeter *artifactUsage) {
	metrics.TokenUsage(ctx, m.TenantID, tokens)
	// Per-tenant cost: the platform meters cost in tokens (no price table),
	// so the counter value equals this turn's token consumption.
	metrics.TenantCost(ctx, m.TenantID, float64(tokens))
	if toolDur > 0 {
		metrics.ToolCallDuration(ctx, m.TenantID, agentID, toolDur)
	}
	var artSaves int32
	if artMeter != nil {
		artSaves = artMeter.count()
	}
	w.recordUsage(ctx, m, agentID, buildUsageEntries(m, agentID, tokens, toolCalls, usedSkills, artSaves))
}

// skillInstruction splices the text of the skills mounted on the agent's
// profile into an instruction fragment appended to the system prompt. Each
// skill renders its SKILL.md body, or its prompt_template when the body is
// empty. When skills are not wired (nil) the result is empty. It also returns
// the ids of the skills actually injected (loaded, published and non-empty),
// which the usage meter reports as the turn's skill dimension — a skill is
// "used" when its SKILL.md shapes the turn, not merely mounted.
func (w *Worker) skillInstruction(ctx context.Context, profile agent.RuntimeProfile) (string, []skillUsageRef) {
	if w.skills == nil || len(profile.SkillIDs) == 0 {
		return "", nil
	}
	loaded, err := w.skills.LoadByIDs(ctx, profile.SkillIDs)
	if err != nil {
		slog.Warn("worker: skill load failed", "err", err)
		return "", nil
	}
	var b strings.Builder
	injected := make([]skillUsageRef, 0, len(loaded))
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
		injected = append(injected, skillUsageRef{SkillID: s.SkillID, Code: s.Code, Name: s.Name, Version: s.Version})
	}
	return b.String(), injected
}

// finalTextWithUsage extracts the last non-partial assistant text, the summed
// token usage, the distinct tool names invoked (in order), the per-tool call
// counts (including repeats), and the total tool-call latency (first tool
// call to last tool response), from the event stream. A nil *event.Event is
// skipped (the channel may close early).
func finalTextWithUsage(events <-chan *event.Event) (string, int64, []string, time.Duration, map[string]int, error) {
	var last string
	var tokens int64
	var toolNames []string
	var toolStart, toolEnd time.Time
	seen := make(map[string]struct{})
	toolCalls := make(map[string]int)

	// recordToolCall counts one tool-call request: names deduped in order,
	// with repeat counts so the usage meter reports real invocation counts.
	// Tool calls sit on Message.ToolCalls (non-streaming responses) or on
	// Delta.ToolCalls (streaming adapters); the framework reads both (see
	// model.Response.GetToolCallIDs), so the worker must too — otherwise a
	// streaming model's calls never reach the tool usage dimension.
	recordToolCall := func(tc model.ToolCall, ts time.Time) {
		if tc.Function.Name == "" {
			return
		}
		toolCalls[tc.Function.Name]++
		if _, ok := seen[tc.Function.Name]; !ok {
			seen[tc.Function.Name] = struct{}{}
			toolNames = append(toolNames, tc.Function.Name)
		}
		if toolStart.IsZero() {
			toolStart = ts
		}
	}

	for evt := range events {
		if evt == nil {
			continue
		}
		if evt.Error != nil {
			return "", tokens, toolNames, 0, nil, fmt.Errorf("worker: agent error: %s", evt.Error.Message)
		}
		if evt.Usage != nil {
			tokens += int64(evt.Usage.PromptTokens + evt.Usage.CompletionTokens)
		}
		// Collect tool-call names from the assistant message(s) requesting
		// them, covering both response shapes (see recordToolCall).
		for _, c := range evt.Choices {
			for _, tc := range c.Message.ToolCalls {
				recordToolCall(tc, evt.Timestamp)
			}
			for _, tc := range c.Delta.ToolCalls {
				recordToolCall(tc, evt.Timestamp)
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
	return last, tokens, toolNames, toolDur, toolCalls, nil
}
