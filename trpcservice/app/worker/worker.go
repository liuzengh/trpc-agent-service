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
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/health"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	fwagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/externalization"
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
// msgKey as a *lease* and reports which state it found: IdemClaimed = process
// it, IdemInFlight = another attempt owns it (retry the delivery later),
// IdemCompleted = the work is already recorded (ack and drop). Collapsing the
// last two into one answer is what loses messages when a worker is killed
// mid-turn. CommitIdem makes the claim durable once the work is recorded;
// ClearIdem releases it so a failed attempt can be retried on redelivery;
// ExpireIdemLease keeps a long turn's claim alive.
type Idempotency interface {
	Idempotent(ctx context.Context, msgKey string) (bus.IdemState, error)
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

// OutboxAppender durably queues one outbound message under an idempotency key
// (bus.Outbox satisfies it). The worker only ever appends — dispatching belongs
// to the node that owns the outbound loop — so it depends on this narrow slice
// and can be exercised without a database.
type OutboxAppender interface {
	Append(ctx context.Context, m *bus.Message, msgKey string) error
}

// Worker turns inbound messages into agent replies.
type Worker struct {
	bus       StateBus
	agents    *agent.Manager
	toolRes   *toolResolver // optional: static tools + KB search tools
	outbox    OutboxAppender
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
func New(b StateBus, agents *agent.Manager, toolRes *toolResolver, outbox OutboxAppender, sessions *storage.Router, skills *skill.Manager, auditor audit.Recorder, artifacts artifact.Service, ledger chat.Ledger) *Worker {
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
//
// Transient bus errors are retried inside (see retryConsume): the consumer is
// the only thing draining stream:inbound, so a Redis blip must not detach it for
// the rest of the process's life.
func (w *Worker) Run(ctx context.Context) error {
	host, _ := os.Hostname()
	consumer := fmt.Sprintf("%s-%d", host, os.Getpid())
	// Attach the recovery signal before the loop starts: the bus reports every
	// read the server answered, which is the only honest way to tell a
	// reconnected consumer from one parked on a dead socket.
	if rs, ok := w.bus.(readySignaler); ok {
		rs.SetConsumeReady(consumerSupervisor.Attached)
	}
	return retryConsume(ctx, func(ctx context.Context) error {
		return w.bus.ConsumeInbound(ctx, Group, consumer, w.handle)
	})
}

// readySignaler is the optional bus capability behind the recovery signal (see
// bus.RedisBus.SetConsumeReady). It is optional so that test buses and
// alternative backends do not have to fake it.
type readySignaler interface {
	SetConsumeReady(func())
}

// Consumer reconnect policy. The base delay is short because a Redis restart is
// usually over in seconds; the cap keeps a long outage from becoming a hot loop
// against a dead socket.
const (
	consumeRetryBase    = time.Second
	consumeRetryMax     = 30 * time.Second
	consumeDegradeAfter = 3
	// consumeGrace is the silence budget of the consumer. The bus answers about
	// once a second (the 1s XREADGROUP block), so no answer for 5s is an outage;
	// a single failed attempt is not (a stale pooled connection costs seconds to
	// detect on its own).
	consumeGrace = 5 * time.Second
)

// consumerSupervisor owns the reconnect loop. It is shared by every worker
// process (the loop itself is stateless), and it is what lets the consumer
// report *recovery*: a healthy consume loop never returns, so the attachment
// signal comes from the bus (RedisBus.SetConsumeReady → Attached).
var consumerSupervisor = &health.Supervisor{
	Name: "worker: consumer", Base: consumeRetryBase, Max: consumeRetryMax,
	DegradeAfter: consumeDegradeAfter, Grace: consumeGrace,
}

// retryConsume keeps a consume loop attached across transient bus failures.
//
// Returning on the first failed XREADGROUP turns a Redis blip into a permanent
// outage: publishing works again as soon as Redis is back, so every new message
// is accepted and then never processed — while /healthz keeps answering 200,
// i.e. the node becomes a black hole that looks healthy. This exact failure was
// observed on the deployed stack (a 20s Redis stop killed both the consumer and
// the IM follower for good), which is why the loop reconnects with bounded
// backoff until the context is cancelled, marks the node degraded after
// consumeDegradeAfter consecutive failures, and clears the mark once it is
// consuming again.
func retryConsume(ctx context.Context, consume func(context.Context) error) error {
	return consumerSupervisor.Run(ctx, consume)
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
	//
	// The two "not ours" answers must NOT be treated the same, which is exactly
	// the bug the node-failure drill found (scripts/faults/node-failure.ps1):
	// after a SIGKILL, XAUTOCLAIM handed the dead worker's 3 in-flight messages
	// to a survivor within the 90s lease, the survivor read "not first" as
	// "already handled" and ACKED them — 3 of 20 messages were dropped without
	// ever running, while /healthz, lag and pending all looked clean.
	state, err := w.bus.Idempotent(ctx, m.ID)
	if err != nil {
		return err // transient Redis error: retry later
	}
	switch state {
	case bus.IdemCompleted:
		// The turn already produced a durable outcome (reply in the outbox, or a
		// deliberate drop): this is a provider re-push or a redelivery of
		// finished work. Ack it — and say so, because "a delivery was acked
		// without doing any work" is exactly the shape of a silent message loss
		// and must be distinguishable from "handled" in the logs.
		slog.Info("worker: delivery already completed, acking",
			"message", m.ID, "tenant", m.TenantID, "session", m.SessionID)
		return nil
	case bus.IdemInFlight:
		// Some attempt owns the message: a concurrent delivery of the same
		// envelope, or a worker that died with its lease still alive. Leaving it
		// pending is the only safe answer — the owner commits (the next reclaim
		// then sees IdemCompleted and acks) or its lease expires (the next
		// reclaim claims it and runs the turn). Acking here loses the message.
		return bus.ErrRequeue
	case bus.IdemClaimed:
		// Ours to process.
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
		// Detached: a graceful shutdown must not turn a finished turn into an
		// uncommitted one (see shutdownSafe).
		c, cancel := shutdownSafe(ctx)
		defer cancel()
		if err := w.bus.CommitIdem(c, m.ID); err != nil {
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

	// Unreadable attachments are reported to the user and audited before the
	// turn starts, so the gap is visible even if the agent run later fails.
	w.reportUnreadableMedia(ctx, m)

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
	// The ledger is the turn's user-visible history and is written last, so it
	// is the write most exposed to a shutdown: the node-failure drill showed
	// turns with a durable reply in the outbox and NO ledger row, because the
	// rolling restart cancelled the context between the two writes. Detach it
	// (bounded) so a graceful restart finishes recording what it already
	// answered.
	lc, cancel := shutdownSafe(ctx)
	defer cancel()
	w.recordLedger(lc, m, agentID, reply)
	return nil
}

// shutdownSafe detaches a best-effort write from ctx cancellation and bounds it.
//
// A rolling restart (SIGTERM) cancels the consumer context while turns are in
// flight. The turn itself is fine — its reply is already in the outbox — but
// every write after that point used the cancelled context and silently failed:
// the drill's log showed "worker: usage metering skipped (best-effort)
// err=... context canceled" and "worker: idempotency commit failed
// err=bus: commit idem: context canceled", and the affected turns ended up with
// no ledger row. Accounting writes about work that is already durable must
// survive the shutdown that interrupted them; the timeout keeps a dead database
// from holding the process open.
func shutdownSafe(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), shutdownWriteTimeout)
}

// shutdownWriteTimeout bounds a detached accounting write.
const shutdownWriteTimeout = 5 * time.Second

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

// timeoutAware keeps the platform's own deadline visible to the classifier.
//
// When runTimeout cuts a turn, the framework reports the cancelled model call in
// its own words, so errors.Is(err, context.DeadlineExceeded) is false and the
// audit row said run_error — indistinguishable from "the provider answered with
// an error". The model-timeout drill measured exactly that: a hung upstream was
// cut at ~283s and audited as run_error (2026-09-11), even though the turn was
// terminated by our budget, not by the provider.
func timeoutAware(ctx context.Context, err error) error {
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		// Both errors stay wrapped: the classifier sees the deadline, the logs
		// keep the framework's own description of what it was doing.
		return fmt.Errorf("%w: %w", context.DeadlineExceeded, err)
	}
	return err
}

// resumeRecordedTurn looks for a completed answer of this inbound message in
// the session log. Returns the reply to deliver when one is found.
//
// Why the session and not the outbox: the outbox append and the idempotency
// commit are the last steps of a turn, so a crash just before them leaves the
// model output persisted in the session (the framework stamps every event of a
// run with its RequestID, which the worker sets to the message id) and nothing
// in the outbox. Reusing that text keeps the session free of a duplicated user
// message and avoids re-running — and re-billing — the model.
//
// A partial (still streaming) answer is deliberately not resumed: delivering a
// truncated reply is worse than running the turn again.
func (w *Worker) resumeRecordedTurn(ctx context.Context, m *bus.Message) (*bus.Message, bool) {
	if w.sessions == nil || m == nil || m.ID == "" || m.SessionID == "" {
		return nil, false
	}
	s, err := w.sessions.Sessions(ctx, m.TenantID)
	if err != nil {
		return nil, false
	}
	sess, err := s.Get(ctx, m.TenantID, m.UserID, m.SessionID)
	if err != nil || sess == nil {
		return nil, false
	}
	text := recordedAssistantText(sess.Events, m.ID)
	if text == "" {
		return nil, false
	}
	reply := model.NewAssistantMessage(text)
	return &bus.Message{
		ID:        uuid.NewString(),
		TraceID:   m.TraceID,
		TenantID:  m.TenantID,
		AgentID:   m.AgentID,
		SessionID: m.SessionID,
		Channel:   m.Channel,
		UserID:    m.UserID,
		Content:   &reply,
		ReplyTo:   m.ID,
	}, true
}

// recordedAssistantText returns the last completed assistant answer persisted
// for requestID, or "" when the turn has no recorded answer.
func recordedAssistantText(events []event.Event, requestID string) string {
	if requestID == "" {
		return ""
	}
	var text string
	for i := range events {
		ev := events[i]
		if ev.RequestID != requestID || ev.Response == nil {
			continue
		}
		if ev.IsPartial || ev.IsToolCallResponse() {
			continue
		}
		for _, c := range ev.Choices {
			if c.Message.Role == model.RoleAssistant && c.Message.Content != "" {
				text = c.Message.Content
			}
		}
	}
	return text
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
	profile, agentVersion, err := w.agents.ProfileForSession(ctx, agentID, m.SessionID)
	if err != nil {
		return nil, nil, 0, err
	}
	span.SetAttributes(attribute.Int("agent_version", agentVersion))

	// Event-level idempotency: a previous attempt of this same inbound message
	// may have finished the model work and died before the reply was recorded
	// (see handle's two-phase idempotency). The session log is the durable
	// record of that attempt — the run stamps every event with the message id
	// as its RequestID — so the reply is recovered from it instead of paying
	// for a second model turn and appending the user message twice.
	if reply, ok := w.resumeRecordedTurn(ctx, m); ok {
		slog.Info("worker: resumed a turn recorded before the crash",
			"tenant", m.TenantID, "session", m.SessionID, "message", m.ID)
		return reply, nil, 0, nil
	}

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
		tools, approvalToolNames = w.toolRes.fromProfile(ctx, m.TenantID, agentID, profile, policy)
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

	// The inbound message id is the run's RequestID: every event this turn
	// persists (the user message, assistant text, tool calls) then carries the
	// same idempotency key the bus uses. That is what makes a replay after a
	// crash recognisable — the framework stamps its events with the RequestID,
	// and its own restore path can tell an already-persisted user message of
	// this request apart from a new one (see the framework's content processor).
	events, err := r.Run(runCtx, m.UserID, m.SessionID, *m.Content, fwagent.WithRequestID(m.ID))
	// Record model latency even when the run failed: model calls did happen
	// and a timeout spike is exactly what the metric should surface.
	if md := timer.Duration(); md > 0 {
		metrics.ModelCallDuration(ctx, m.TenantID, agentID, md)
	}
	if err != nil {
		return nil, nil, 0, timeoutAware(runCtx, fmt.Errorf("worker: run agent %q: %w", agentID, err))
	}
	text, tokens, toolNames, toolDur, toolCalls, err := finalTextWithUsage(events)
	if err != nil {
		return nil, nil, 0, timeoutAware(runCtx, err)
	}
	w.recordTurnUsage(ctx, m, agentID, tokens, toolDur, toolCalls, usedSkills, artMeter)
	if text == "" {
		// A turn that ends without assistant text is acked with nothing to send
		// back: the user asked and got silence. Log it loudly — it is a visible
		// product gap, not a transient failure to retry (the model answered, it
		// just answered with no text), and without this line the only symptom is
		// a missing reply.
		slog.Warn("worker: agent produced no text, acking without a reply",
			"tenant", m.TenantID, "session", m.SessionID, "agent", agentID,
			"tools", toolNames, "tokens", tokens)
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
			opts = append(opts, runner.WithSessionService(w.externalizeSessionContent(s.Service(), artMeter)))
		}
	}
	return opts, timer, artMeter
}

// externalizeSessionContent moves inline attachment payloads out of the session
// events and into artifact storage before they are persisted, hydrating them
// again on read.
//
// Why it matters: an inbound image arrives as inline bytes in the model message,
// and the framework would otherwise persist those bytes (base64 in JSON) into
// session_events — one user image per turn, in the hot MySQL table. Externalizing
// keeps the event small and leaves the bytes in object storage, addressed by a
// pinned, sha256-verified artifact reference. This is the framework's own
// mechanism (session/externalization), not a platform re-implementation.
//
// The meter doubles as the artifact service so externalized payloads are counted
// in the artifact usage dimension alongside tool-produced artifacts.
func (w *Worker) externalizeSessionContent(svc session.Service, meter *artifactUsage) session.Service {
	if svc == nil || meter == nil {
		return svc
	}
	return externalization.Wrap(svc, meter, externalization.Config{Enabled: true})
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
	// Usage rows are metering of a turn that has already been produced; a
	// shutdown in the middle of them must not lose the record (see
	// shutdownSafe).
	uc, cancel := shutdownSafe(ctx)
	defer cancel()
	w.recordUsage(uc, m, agentID, buildUsageEntries(m, agentID, tokens, toolCalls, usedSkills, artSaves))
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
