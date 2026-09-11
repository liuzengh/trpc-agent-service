package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestDefaultInvocationFactoryUsesTenantToolBudget(t *testing.T) {
	invocation, err := DefaultInvocationFactory(context.Background(), tenant.Snapshot{Config: config.TenantConfig{
		TenantID: "acme", ConfigVersion: 3, Governance: config.GovernancePolicy{MaxToolCalls: 2, BudgetUnits: 2},
	}}, "web-console", channels.InboundMessage{}, "acme/support/session/session-1")
	if err != nil {
		t.Fatalf("DefaultInvocationFactory() error = %v", err)
	}
	if err := invocation.Budget.Consume(); err != nil {
		t.Fatalf("first Consume() error = %v", err)
	}
	if err := invocation.Budget.Consume(); err != nil {
		t.Fatalf("second Consume() error = %v", err)
	}
	if err := invocation.Budget.Consume(); !errors.Is(err, governance.ErrBudgetExceeded) {
		t.Fatalf("third Consume() error = %v, want ErrBudgetExceeded", err)
	}
	if err := invocation.Units.Consume(1); err != nil {
		t.Fatalf("first unit Consume() error = %v", err)
	}
	if err := invocation.Units.Consume(1); err != nil {
		t.Fatalf("second unit Consume() error = %v", err)
	}
	if err := invocation.Units.Consume(1); !errors.Is(err, governance.ErrBudgetUnitsExceeded) {
		t.Fatalf("third unit Consume() error = %v, want ErrBudgetUnitsExceeded", err)
	}
}

func TestDefaultInvocationFactoryCopiesToolIdentity(t *testing.T) {
	invocation, err := DefaultInvocationFactory(context.Background(), tenant.Snapshot{Config: config.TenantConfig{
		TenantID: "acme", AppCode: "support", ConfigVersion: 1,
		Tools: config.ToolPolicy{AllowedRoles: map[string][]string{"platform_save_artifact": {"admin"}}},
	}}, "web-console", channels.InboundMessage{
		Channel: channels.Web, SenderID: "user-1", SubjectID: "user-1", MessageID: "msg-1", ConversationID: "conv-1",
		ProgressMessageID: "progress-1", ProviderReplyToken: "reply-token-1",
	}, "acme/support/session/conv-1")
	if err != nil {
		t.Fatalf("DefaultInvocationFactory() error = %v", err)
	}
	if invocation.Execution.Channel != "web" || invocation.Execution.UserID != "user-1" || invocation.Execution.AgentName != "assistant" {
		t.Fatalf("execution identity = %+v", invocation.Execution)
	}
	if invocation.Execution.ProgressMessageID != "progress-1" || invocation.Execution.ProviderReplyToken != "reply-token-1" {
		t.Fatalf("progress routing = %+v", invocation.Execution)
	}
	if got := invocation.Execution.ToolRoles["platform_save_artifact"]; len(got) != 1 || got[0] != "admin" {
		t.Fatalf("tool roles = %v", invocation.Execution.ToolRoles)
	}
}
