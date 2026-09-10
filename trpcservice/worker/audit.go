package worker

import (
	"context"
	"errors"
	"log"
	"time"

	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/guardrail"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const auditAgentName = "assistant"

func (w Worker) recordAudit(ctx context.Context, exec Execution, event platformaudit.Event) {
	if w.Audit == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if event.TenantID == "" {
		event.TenantID = exec.Tenant.TenantID
	}
	if event.AppID == "" {
		event.AppID = exec.Tenant.AppID
	}
	if event.Channel == "" {
		event.Channel = exec.Tenant.Channel
	}
	if event.UserID == "" {
		event.UserID = exec.Tenant.UserID
	}
	if event.SessionID == "" {
		event.SessionID = exec.Tenant.SessionID
	}
	if event.AgentName == "" {
		event.AgentName = auditAgentName
	}
	if event.TraceID == "" {
		event.TraceID = exec.Tenant.TraceID
	}
	if event.RequestID == "" {
		event.RequestID = exec.RequestID
	}
	if event.ConfigVersion == "" {
		event.ConfigVersion = exec.Tenant.ConfigVersion
	}
	if exec.Config.Audit.RedactPII {
		event = platformaudit.RedactEvent(event)
	}
	auditCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := w.Audit.Record(auditCtx, event); err != nil {
		log.Printf("audit write failed tenant=%s app=%s event=%s: %s", event.TenantID, event.AppID, event.EventType, platformlog.SafeError(err))
		if w.Metrics != nil {
			w.Metrics.RecordAuditFailure(ctx, platformmetrics.Labels{
				TenantID: event.TenantID,
				AppID:    event.AppID,
				Channel:  event.Channel,
			})
		}
	}
}

func (w Worker) recordToolDecision(
	ctx context.Context,
	exec Execution,
	name string,
	decision frameworktool.PermissionDecision,
	started time.Time,
) {
	if !exec.Config.Audit.Enabled || !exec.Config.Audit.RecordToolDecisions {
		return
	}
	eventType := platformaudit.ToolAllowed
	eventDecision := string(decision.Action)
	errorType := ""
	policyRuleID := ""
	policyReason := ""
	switch decision.Action {
	case frameworktool.PermissionActionDeny:
		eventType = platformaudit.ToolDenied
		errorType = "policy_denied"
		policyRuleID = "tool_policy.executable_tools"
		policyReason = "tool_not_authorized"
	case frameworktool.PermissionActionAsk:
		eventType = platformaudit.ToolReviewRequired
		errorType = "human_review_required"
		policyRuleID = "tool_policy.review_required"
		policyReason = "human_review_required"
	case frameworktool.PermissionActionAllow:
		if decision.Reason != "" {
			policyRuleID = "tool_policy.review_required"
			policyReason = "approval_granted"
		}
	}
	w.recordAudit(ctx, exec, platformaudit.Event{
		ToolName:     name,
		Decision:     eventDecision,
		PolicyRuleID: policyRuleID,
		PolicyReason: policyReason,
		Latency:      time.Since(started),
		ErrorType:    errorType,
		EventType:    eventType,
	})
}

func (w Worker) accumulateUsage(result *RunResult, evt *event.Event) {
	if result == nil || evt == nil || evt.Response == nil || evt.Response.Usage == nil {
		return
	}
	usage := evt.Response.Usage
	if usage.PromptTokens > 0 {
		result.InputTokens += usage.PromptTokens
	}
	if usage.CompletionTokens > 0 {
		result.OutputTokens += usage.CompletionTokens
	}
	total := usage.TotalTokens
	if total <= 0 {
		total = usage.PromptTokens + usage.CompletionTokens
	}
	if total > 0 {
		result.TotalTokens += total
	}
}

func executionErrorType(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, tenant.ErrBudgetExceeded) {
		return "budget_exceeded"
	}
	if errors.Is(err, tenant.ErrQuotaExceeded) {
		return "quota_exceeded"
	}
	if errors.Is(err, guardrail.ErrInputBlocked) {
		return "guardrail_input_blocked"
	}
	if errors.Is(err, ErrExecutionCanceled) {
		return "canceled"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "execution"
}

func metricResultForError(errorType string) string {
	if errorType == "" {
		return "success"
	}
	return "error"
}
