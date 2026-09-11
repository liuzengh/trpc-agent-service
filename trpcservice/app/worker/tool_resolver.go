// tool_resolver.go assembles the runtime tool set for one agent turn from the
// profile's tool ids, the RBAC grants, the tenant whitelist, and the mounted
// knowledge bases. It owns the three dependencies the worker otherwise had to
// hold (tool registry, tool source, knowledge manager) solely to resolve
// tools, keeping the worker's orchestration surface smaller.
package worker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tool"

	fwtool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// toolResolver resolves tool ids to their runtime implementations for a turn.
type toolResolver struct {
	tools     *tool.Registry
	toolSrc   ToolSource
	knowledge *knowledge.Manager
}

// NewToolResolver assembles a tool resolver. tools, toolSrc and knowledge may
// be nil (no static tools / no tool implementations / no KB search tools).
func NewToolResolver(tools *tool.Registry, toolSrc ToolSource, knowledge *knowledge.Manager) *toolResolver {
	return &toolResolver{tools: tools, toolSrc: toolSrc, knowledge: knowledge}
}

// fromProfile returns the tool implementations the agent may use (the
// profile's tool ids intersected with the RBAC grants and the tenant's tool
// whitelist) plus the set of tool names whose calls need human approval.
// Approval is triggered by any of the three rails: the tool is listed in
// profile.ApprovalToolIDs, in the tenant's force-approval set, or its
// definition is risk_level=high.
//
// The RBAC check is tenant-aware: the grant must belong to the tenant running
// the turn (tenantID), so a grant that crossed a tenant boundary — however it
// got there — cannot authorise a tool call.
func (r *toolResolver) fromProfile(ctx context.Context, tenantID, agentID string, profile agent.RuntimeProfile, policy *tenantPolicy) ([]fwtool.Tool, map[string]bool) {
	if len(profile.ToolIDs) == 0 || r.toolSrc == nil || r.tools == nil {
		return nil, nil
	}
	if policy == nil {
		policy = defaultTenantPolicy()
	}
	manuallyApproved := make(map[string]bool, len(profile.ApprovalToolIDs))
	for _, id := range profile.ApprovalToolIDs {
		manuallyApproved[id] = true
	}
	var out []fwtool.Tool
	approvalNames := make(map[string]bool)
	for _, id := range profile.ToolIDs {
		allowed, err := r.tools.IsAllowedForTenant(ctx, tenantID, agentID, id)
		if err != nil {
			slog.Warn("worker: RBAC check failed, skipping tool", "agent", agentID, "tool", id, "err", err)
			continue
		}
		if !allowed {
			continue
		}
		// Tenant whitelist (static tools only): knowledge_search tools are
		// appended by knowledgeTools and never restricted here.
		if !policy.toolAllowed(id) {
			continue
		}
		// Resolve the tool definition (risk level) BEFORE mounting: a missing
		// definition drops the tool rather than mounting it without its
		// approval gate. A high-risk / force-approved tool that mounted
		// without its approval entry would otherwise execute ungoverned.
		def, err := r.tools.Get(ctx, id)
		if err != nil {
			slog.Warn("worker: tool definition unavailable, skipping tool", "agent", agentID, "tool", id, "err", err)
			continue
		}
		t, ok := r.toolSrc(id)
		if !ok {
			continue
		}
		out = append(out, t)
		_, forced := policy.ForceApproval[id]
		if manuallyApproved[id] || forced || def.RiskLevel == tool.RiskHigh {
			approvalNames[def.Name] = true
		}
	}
	return out, approvalNames
}

// knowledgeTools returns one search tool per KB mounted on the agent's
// profile. The first tool keeps the framework's default name; extra KBs get
// numbered names so the LLM can address them separately.
func (r *toolResolver) knowledgeTools(ctx context.Context, profile agent.RuntimeProfile) []fwtool.Tool {
	if r.knowledge == nil || len(profile.KnowledgeIDs) == 0 {
		return nil
	}
	var out []fwtool.Tool
	for i, kbID := range profile.KnowledgeIDs {
		name := "knowledge_search"
		if i > 0 {
			name = fmt.Sprintf("knowledge_search_%d", i+1)
		}
		t, err := r.knowledge.SearchTool(ctx, kbID, name)
		if err != nil {
			// A missing KB must not take down the whole agent run.
			slog.Warn("worker: knowledge tool unavailable", "kb", kbID, "err", err)
			continue
		}
		out = append(out, t)
	}
	return out
}
