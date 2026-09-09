package agent

import (
	"context"
	"slices"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

// Debugging never broadens an Agent policy. MCP read-only exceptions come
// from deployment credentials, not annotations supplied by an MCP server.
func (c *RevisionCompiler) debugPolicy(ctx context.Context, tenant string, policy governance.ToolPolicy, servers []platformtool.MCPServerSpec) (governance.ToolPolicy, error) {
	dangerous, err := platformtool.MCPDangerousTools(ctx, c.secrets, tenant, servers)
	if err != nil {
		return policy, err
	}
	dangerous = append(dangerous, policy.DangerousTools...)
	allowed := []string{}
	for _, name := range policy.AllowedTools {
		if name != "skill_run" && (slices.Contains(dangerous, name) || c.toolCatalog.IsManagedSideEffect(name) || c.toolCatalog.RequiresApproval(name)) {
			continue
		}
		allowed = append(allowed, name)
	}
	policy.AllowedTools = allowed
	return policy, nil
}
