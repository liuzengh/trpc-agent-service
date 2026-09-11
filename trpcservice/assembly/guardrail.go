package assembly

import (
	"context"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/plugin/guardrail"
	"trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval"
	approvalreview "trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval/review"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type approvalGuardrailPlugin struct{ inner plugin.Plugin }

func (p approvalGuardrailPlugin) Name() string { return p.inner.Name() }

func (p approvalGuardrailPlugin) Register(registry *plugin.Registry) {
	p.inner.Register(registry)
	// trpc-agent-go v1.11.2 turns a tool result into CustomResult when a
	// plugin has no AfterTool callbacks. Returning an explicit empty result
	// keeps the normal tool pipeline running so agent-level AfterTool callbacks
	// can finalize the durable execution ledger and audit record.
	registry.AfterTool(func(context.Context, *agenttool.AfterToolArgs) (*agenttool.AfterToolResult, error) {
		return &agenttool.AfterToolResult{}, nil
	})
}

func (p approvalGuardrailPlugin) Close(ctx context.Context) error {
	if closer, ok := p.inner.(plugin.Closer); ok {
		return closer.Close(ctx)
	}
	return nil
}

func tenantGuardrailPlugin(tenantConfig config.TenantConfig, reviewer approvalreview.Reviewer) (plugin.Plugin, error) {
	if len(tenantConfig.Tools.RequireConfirmation) == 0 {
		return nil, nil
	}
	if reviewer == nil {
		reviewer = governance.RoleApprovalReviewer{}
	}
	options := []approval.Option{
		approval.WithDefaultToolPolicy(approval.ToolPolicySkipApproval),
		approval.WithReviewer(reviewer),
	}
	for _, name := range tenantConfig.Tools.RequireConfirmation {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		options = append(options, approval.WithToolPolicy(name, approval.ToolPolicyRequireApproval))
	}
	approvalPlugin, err := approval.New(options...)
	if err != nil {
		return nil, err
	}
	guard, err := guardrail.New(guardrail.WithApproval(approvalPlugin))
	if err != nil {
		return nil, err
	}
	return approvalGuardrailPlugin{inner: guard}, nil
}
