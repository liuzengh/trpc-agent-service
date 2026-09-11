package agent

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"go.opentelemetry.io/otel/trace"
)

// DefaultToolCallBudget bounds tool calls per message execution until the
// a tenant configuration overrides it.
const DefaultToolCallBudget = 8
const DefaultBudgetUnits int64 = 100

type actorRoleKey struct{}

// WithActorRole records a resolved role for one Runtime invocation.
func WithActorRole(ctx context.Context, role string) context.Context {
	if role == "" {
		role = "member"
	}
	return context.WithValue(ctx, actorRoleKey{}, role)
}

func actorRoleFromContext(ctx context.Context) string {
	if role, ok := ctx.Value(actorRoleKey{}).(string); ok && role != "" {
		return role
	}
	return "member"
}

// DefaultInvocationFactory builds the request-scoped governance invocation for
// every production execution. The policy version is pinned to the ingress
// configuration snapshot, the budget is bounded per message, and the actor
// role comes from the tenant membership resolver when one is configured.
func DefaultInvocationFactory(ctx context.Context, snapshot tenant.Snapshot, bindingID string, inbound channels.InboundMessage, sessionID string) (governance.Invocation, error) {
	traceID := trace.SpanContextFromContext(ctx).TraceID().String()
	if traceID == "" || traceID == "00000000000000000000000000000000" {
		traceID = inbound.MessageID
	}
	if traceID == "" {
		traceID = "unknown"
	}
	toolCallBudget := snapshot.Config.Governance.MaxToolCalls
	if toolCallBudget == 0 {
		toolCallBudget = DefaultToolCallBudget
	}
	unitBudget := snapshot.Config.Governance.BudgetUnits
	if unitBudget == 0 {
		unitBudget = DefaultBudgetUnits
	}
	allowedTools := make(map[string]struct{}, len(snapshot.Config.Tools.Allowed))
	for _, name := range snapshot.Config.Tools.Allowed {
		allowedTools[name] = struct{}{}
	}
	return governance.Invocation{
		Execution: governance.ExecutionContext{
			TenantID:           snapshot.Config.TenantID,
			AppCode:            snapshot.Config.AppCode,
			ConfigVersion:      snapshot.Config.ConfigVersion,
			ToolRoles:          snapshot.Config.Tools.AllowedRoles,
			Role:               actorRoleFromContext(ctx),
			TraceID:            traceID,
			RequestID:          inbound.MessageID,
			Channel:            string(inbound.Channel),
			BindingID:          bindingID,
			ConversationID:     inbound.ConversationID,
			ConversationScope:  string(inbound.ConversationScope),
			ExternalUserID:     inbound.SenderID,
			ProgressMessageID:  inbound.ProgressMessageID,
			ProviderReplyToken: inbound.ProviderReplyToken,
			UserID:             inbound.SubjectID,
			SessionID:          sessionID,
			AgentName:          "assistant",
			PolicyVersion:      fmt.Sprintf("%d", snapshot.Config.ConfigVersion),
			AllowedTools:       allowedTools,
		},
		Budget: governance.NewCallBudget(toolCallBudget),
		Units:  governance.NewUnitBudget(unitBudget),
	}, nil
}
