// Human-approval support for the worker.
//
// The platform reuses the framework approval plugin
// (plugin/guardrail/approval): a runner-scoped BeforeTool hook that pauses a
// tool call until a reviewer returns a decision. The platform reviewer
// implements HUMAN approval — it sends an approval notice to the user over
// the outbox, then blocks polling the approval:res Redis key until the user
// replies 批准/拒绝 (or the wait times out). The reply is recognized by the
// worker BEFORE the session lock is taken (tryResolveApproval), so a blocked
// agent turn can always be resolved by its own session's reply.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	fwapproval "trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval"
	fwreview "trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval/review"
)

// Approval timing. The wait budget bounds how long an agent turn stays
// suspended. The session lock is kept alive by the worker's lock heartbeat
// (worker.startLockHeartbeat), which covers this wait, so no refresh timer is
// needed here.
const (
	approvalTimeout      = 5 * time.Minute
	approvalPollInterval = 500 * time.Millisecond
)

// pendingApproval is what the worker stores under approval:req for one
// waiting approval. It carries enough context for the reply recognizer to
// build the confirmation message and audit trail.
type pendingApproval struct {
	ReqID     string    `json:"req_id"`
	Tool      string    `json:"tool"`
	AgentID   string    `json:"agent_id"`
	UserID    string    `json:"user_id"`
	CreatedAt time.Time `json:"created_at"`
}

// humanReviewer implements review.Reviewer on top of the platform state bus:
// it notifies the user, then blocks until the human decision lands on the
// approval:res key. The agent turn is genuinely suspended meanwhile.
type humanReviewer struct {
	w       *Worker
	m       *bus.Message // the inbound message whose turn triggered approval
	lockTok string       // session lock token, refreshed while waiting
}

// Review notifies the user and waits for the human decision (approve / deny /
// timeout). The decision shape follows the framework contract: Approved=true
// lets the tool call proceed, Approved=false denies it (the approval plugin
// turns the denial into a CustomResult the model can react to).
func (r *humanReviewer) Review(ctx context.Context, req *fwreview.Request) (*fwreview.Decision, error) {
	if req == nil || req.Action.ToolName == "" {
		return nil, errors.New("worker: approval review requires a tool action")
	}
	m := r.m
	pending := pendingApproval{
		ReqID:     uuid.NewString(),
		Tool:      req.Action.ToolName,
		AgentID:   m.AgentID,
		UserID:    m.UserID,
		CreatedAt: time.Now(),
	}
	payload, err := json.Marshal(pending)
	if err != nil {
		return nil, fmt.Errorf("worker: encode pending approval: %w", err)
	}

	// Notify the user (durable, via the same outbox as replies). The
	// idempotency key is scoped to the inbound message so a redelivered turn
	// does not spam the user with duplicate notices.
	notice := approvalNotice(m, pending.Tool, summarizeArgs(req.Action.Arguments))
	if err := r.w.outbox.Append(ctx, notice, m.ID+":approval"); err != nil && !errors.Is(err, bus.ErrDuplicateIdem) {
		return nil, fmt.Errorf("worker: append approval notice: %w", err)
	}
	if err := r.w.bus.SetPendingApproval(ctx, m.TenantID, m.SessionID, string(payload),
		approvalTimeout+bus.SessionLockRefreshInterval()); err != nil {
		return nil, fmt.Errorf("worker: register pending approval: %w", err)
	}

	deadline := time.Now().Add(approvalTimeout)
	ticker := time.NewTicker(approvalPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			_ = r.w.bus.ClearPendingApproval(ctx, m.TenantID, m.SessionID)
			// A cancelled wait is a governance outcome too: the turn was
			// abandoned before the human decided, which the audit trail has to
			// show next to the approvals that did happen.
			r.w.recordAudit(m, m.AgentID, audit.DecisionDeny, 0,
				fmt.Errorf("approval cancelled for tool %s: %w", pending.Tool, ctx.Err()), nil, 0)
			return &fwreview.Decision{Approved: false, RiskScore: 90, RiskLevel: "human", Reason: "审批流程已取消"}, nil
		case <-ticker.C:
		}

		decision, err := r.w.bus.ApprovalResult(ctx, m.TenantID, m.SessionID)
		if err != nil {
			return nil, err
		}
		switch decision {
		case "approve", "deny":
			_ = r.w.bus.ClearPendingApproval(ctx, m.TenantID, m.SessionID)
			if decision == "approve" {
				return &fwreview.Decision{Approved: true, RiskScore: 10, RiskLevel: "human", Reason: "用户已批准"}, nil
			}
			return &fwreview.Decision{Approved: false, RiskScore: 90, RiskLevel: "human", Reason: "用户已拒绝"}, nil
		}
		if time.Now().After(deadline) {
			_ = r.w.bus.ClearPendingApproval(ctx, m.TenantID, m.SessionID)
			// Auto-deny on timeout is a policy decision made by the platform,
			// so it belongs in the audit trail as such (an operator asking "why
			// was this tool blocked?" must find the answer).
			r.w.recordAudit(m, m.AgentID, audit.DecisionDeny, 0,
				fmt.Errorf("approval timed out for tool %s after %s", pending.Tool, approvalTimeout), nil, 0)
			return &fwreview.Decision{Approved: false, RiskScore: 90, RiskLevel: "human", Reason: "审批超时未获批准"}, nil
		}
		// The session lock is kept alive by the worker's lock heartbeat, which
		// covers the whole turn including this wait; no refresh here.
	}
}

// approvalPlugin builds the framework approval plugin for one turn: default
// skip, and require_approval only for the tool names the profile marked.
// nil when no tool needs approval or the worker cannot send notices.
func (w *Worker) approvalPlugin(ctx context.Context, m *bus.Message, approvalNames map[string]bool, lockToken string) plugin.Plugin {
	if len(approvalNames) == 0 || w.bus == nil || w.outbox == nil || m == nil || m.Content == nil {
		return nil
	}
	opts := []fwapproval.Option{
		fwapproval.WithDefaultToolPolicy(fwapproval.ToolPolicySkipApproval),
		fwapproval.WithReviewer(&humanReviewer{w: w, m: m, lockTok: lockToken}),
	}
	for name := range approvalNames {
		opts = append(opts, fwapproval.WithToolPolicy(name, fwapproval.ToolPolicyRequireApproval))
	}
	p, err := fwapproval.New(opts...)
	if err != nil {
		// Misconfiguration must not take down the whole turn.
		slog.Warn("worker: approval plugin disabled", "err", err)
		return nil
	}
	return p
}

// tryResolveApproval recognizes a human approval reply and applies it. It is
// called BEFORE the session lock is taken: while an agent turn blocks on a
// pending approval, the reply of that very session must still get through.
// Returns handled=true when the message was consumed as an approval decision.
func (w *Worker) tryResolveApproval(ctx context.Context, m *bus.Message) (bool, error) {
	if m == nil || m.Content == nil || w.bus == nil || w.outbox == nil {
		return false, nil
	}
	text := strings.TrimSpace(m.Content.Content)
	if text == "" {
		return false, nil
	}
	pending, err := w.bus.PendingApproval(ctx, m.TenantID, m.SessionID)
	if err != nil {
		return false, err
	}
	if pending == "" {
		return false, nil // not an approval conversation
	}
	var p pendingApproval
	if err := json.Unmarshal([]byte(pending), &p); err != nil {
		// Corrupt pending record: clear it and treat the message normally.
		_ = w.bus.ClearPendingApproval(ctx, m.TenantID, m.SessionID)
		return false, nil
	}
	decision := classifyApprovalReply(text)
	if decision == "" {
		return false, nil // ordinary message during an approval wait; the session lock keeps it queued
	}
	if err := w.bus.ResolveApproval(ctx, m.TenantID, m.SessionID, decision); err != nil {
		return false, err
	}
	// Confirm back to the user and record the governance decision. The
	// outbox append is idempotent on the inbound message id, so duplicate
	// deliveries do not produce duplicate confirmations.
	confirm := approvalConfirm(m, p, decision)
	if err := w.outbox.Append(ctx, confirm, m.ID); err != nil && !errors.Is(err, bus.ErrDuplicateIdem) {
		return false, err
	}
	auditDecision := audit.DecisionDeny
	if decision == "approve" {
		auditDecision = audit.DecisionApprove
	}
	w.recordAudit(m, p.AgentID, auditDecision, 0, nil, nil, 0)
	return true, nil
}

// classifyApprovalReply maps a user reply to "approve" / "deny". It only
// matches whole-word replies so an ordinary message during an approval wait
// is never mistaken for a decision.
func classifyApprovalReply(text string) string {
	switch strings.ToLower(strings.Trim(strings.TrimSpace(text), " 　。.!！")) {
	case "批准", "同意", "允许", "批准执行", "同意执行", "允许执行", "approve", "yes", "y":
		return "approve"
	case "拒绝", "驳回", "不允许", "不同意", "拒绝执行", "驳回执行", "不允许执行", "deny", "no", "n", "reject":
		return "deny"
	}
	return ""
}

// approvalNotice renders the outbound message that asks the user to decide.
//
// On a channel that can deliver interactive buttons (Feishu) the notice is a
// card with 批准/拒绝 buttons whose callback value carries the session and the
// decision, so the user approves with one click; the body still spells out the
// typed reply as a fallback. Other channels (WeCom) get the plain text prompt.
func approvalNotice(m *bus.Message, toolName, args string) *bus.Message {
	var b strings.Builder
	fmt.Fprintf(&b, "⚠️ 需要您审批（任务已暂停）\n\nAgent 请求执行工具：%s", toolName)
	if args != "" {
		fmt.Fprintf(&b, "\n参数：%s", args)
	}
	b.WriteString("\n\n回复「批准」允许执行，或「拒绝」阻止本次调用。")
	fmt.Fprintf(&b, "\n%d 分钟内有效，超时自动拒绝。", int(approvalTimeout.Minutes()))

	notice := replyMessage(m, b.String())
	if !channels.CardActionCapable(m.Channel) {
		return notice
	}
	notice.Kind = channels.KindCard
	notice.Segments = []bus.Segment{
		{Type: "title", Text: "需要人工审批"},
		{Type: "markdown", Text: b.String()},
		{Type: "actions", Actions: []bus.SegmentAction{
			{Text: "批准", Value: approvalValue(m, "approve")},
			{Text: "拒绝", Value: approvalValue(m, "deny")},
		}},
	}
	return notice
}

// approvalValue is the callback payload a card button sends back. It carries the
// session and decision only: the reply routing (chat) is already known to the
// gateway from the original inbound message.
func approvalValue(m *bus.Message, decision string) map[string]string {
	return map[string]string{
		"session_id": m.SessionID,
		"decision":   decision,
		"tool":       m.AgentID,
	}
}

// approvalConfirm renders the acknowledgement sent after a decision.
func approvalConfirm(m *bus.Message, p pendingApproval, decision string) *bus.Message {
	verb := "已拒绝"
	if decision == "approve" {
		verb = "已批准"
	}
	return replyMessage(m, fmt.Sprintf("%s执行「%s」。Agent 将收到你的决定。", verb, p.Tool))
}

// replyMessage wraps text into an outbound assistant message addressed to the
// session of the original inbound message.
func replyMessage(m *bus.Message, text string) *bus.Message {
	content := model.NewAssistantMessage(text)
	return &bus.Message{
		ID:        uuid.NewString(),
		TraceID:   m.TraceID,
		TenantID:  m.TenantID,
		AgentID:   m.AgentID,
		SessionID: m.SessionID,
		Channel:   m.Channel,
		UserID:    m.UserID,
		Content:   &content,
		ReplyTo:   m.ID,
	}
}

// summarizeArgs renders tool arguments for the approval notice, truncated.
func summarizeArgs(args []byte) string {
	if len(args) == 0 {
		return ""
	}
	s := strings.TrimSpace(string(args))
	if len(s) > 200 {
		s = s[:200] + "…"
	}
	return s
}
