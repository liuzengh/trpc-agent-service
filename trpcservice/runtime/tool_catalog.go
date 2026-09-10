package runtime

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
	frameworktodo "trpc.group/trpc-go/trpc-agent-go/tool/todo"
)

// ToolCatalog is the deployed runtime tool set. Tenant policy selects names
// from this catalog; execution authorization remains enforced by Worker.
type ToolCatalog struct {
}

// NewToolCatalog creates the deployed runtime tool catalog.
func NewToolCatalog() *ToolCatalog {
	return &ToolCatalog{}
}

// ResolveTools returns tools selected by one immutable execution config.
func (c *ToolCatalog) ResolveTools(ctx context.Context, exec worker.Execution) ([]frameworktool.Tool, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if err := c.ValidateToolPolicy(ctx, exec.Config.Tools); err != nil {
		return nil, err
	}
	names := runtimeToolNames(exec.Config.Tools)
	tools := make([]frameworktool.Tool, 0, len(names))
	for _, name := range names {
		switch name {
		case frameworktodo.DefaultToolName:
			tools = append(tools, frameworktodo.New())
		default:
			return nil, fmt.Errorf("unsupported runtime tool %q", name)
		}
	}
	return tools, nil
}

// ValidateToolPolicy checks names supported by the deployed runtime.
func (c *ToolCatalog) ValidateToolPolicy(_ context.Context, policy tenant.ToolPolicy) error {
	if err := policy.Validate(); err != nil {
		return err
	}
	for _, name := range runtimeToolNames(policy) {
		if _, ok := c.Safety(name); !ok {
			return fmt.Errorf("unsupported runtime tool %q", name)
		}
	}
	return nil
}

// Safety returns the immutable external-side-effect contract for a deployed
// runtime tool. Unknown tools are never assigned a permissive default.
func (*ToolCatalog) Safety(name string) (platformtool.Safety, bool) {
	switch name {
	case frameworktodo.DefaultToolName:
		// todo_write replaces session state with the supplied list. Repeating
		// the same call is therefore safe at the provider boundary.
		return platformtool.SafetyIdempotent, true
	default:
		return "", false
	}
}

func runtimeToolNames(policy tenant.ToolPolicy) []string {
	seen := make(map[string]struct{}, len(policy.VisibleTools)+len(policy.ExecutableTools))
	names := make([]string, 0, len(policy.VisibleTools)+len(policy.ExecutableTools)+len(policy.ReviewRequiredTools))
	for _, values := range [][]string{policy.VisibleTools, policy.ExecutableTools, policy.ReviewRequiredTools} {
		for _, name := range values {
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			names = append(names, name)
		}
	}
	return names
}
