// Package tool registers platform tools and tenant-scoped MCP / function tools.
package tool

import (
	"context"
	"fmt"
	"sort"

	ttool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

// Tool wraps a framework tool with platform metadata.
type Tool struct {
	Tool ttool.Tool // the framework tool
	// Dangerous marks tools whose execution requires in-band user approval:
	// the guardrail intercepts the call and only releases it after the user
	// confirms in the same conversation.
	Dangerous bool
}

// Registry is the set of platform tools an agent may use. Tenant- and
// app-level whitelisting (tenant.tool_policy / agent_app.config.tools)
// narrows this set per message through Allowed.
type Registry struct {
	tools map[string]Tool
}

// NewRegistry creates a Registry from the given tools.
func NewRegistry(tools ...Tool) *Registry {
	r := &Registry{tools: make(map[string]Tool, len(tools))}
	for _, t := range tools {
		r.tools[t.Tool.Declaration().Name] = t
	}
	return r
}

// All returns the framework tools in name order, for llmagent.WithTools.
func (r *Registry) All() []ttool.Tool {
	out := make([]ttool.Tool, 0, len(r.tools))
	for _, t := range r.tools {
		out = append(out, t.Tool)
	}
	// Deterministic order: the list goes into the model's prompt, and a
	// per-call shuffle (map iteration) is noise the model does not need.
	sort.Slice(out, func(i, j int) bool { return out[i].Declaration().Name < out[j].Declaration().Name })
	return out
}

// IsDangerous reports whether the named tool requires user approval.
func (r *Registry) IsDangerous(name string) bool {
	t, ok := r.tools[name]
	return ok && t.Dangerous
}

// ToolPolicy restricts the registry: a non-empty Allow list is a whitelist
// (only listed tools are kept), and Deny entries are subtracted afterwards.
// tenant.tool_policy and agent_app.config.tools share this shape; the app
// policy can only narrow what the tenant policy allows. Names not present in
// the registry are ignored.
type ToolPolicy struct {
	Allow []string `json:"allow"`
	Deny  []string `json:"deny"`
}

// Allowed returns the registry tools surviving both policies, tenant first,
// then app. An empty policy keeps everything.
func (r *Registry) Allowed(policies ...ToolPolicy) []ttool.Tool {
	allowed := make(map[string]bool, len(r.tools))
	for name := range r.tools {
		allowed[name] = true
	}
	for _, p := range policies {
		if len(p.Allow) > 0 {
			keep := make(map[string]bool, len(p.Allow))
			for _, name := range p.Allow {
				if allowed[name] {
					keep[name] = true
				}
			}
			allowed = keep
		}
		for _, name := range p.Deny {
			delete(allowed, name)
		}
	}
	out := make([]ttool.Tool, 0, len(allowed))
	for name := range allowed {
		out = append(out, r.tools[name].Tool)
	}
	// Deterministic order: the list goes into the model's prompt, and a
	// per-call shuffle (map iteration) is noise the model does not need.
	sort.Slice(out, func(i, j int) bool { return out[i].Declaration().Name < out[j].Declaration().Name })
	return out
}

// Call executes a tool directly with JSON arguments. It is used to release
// the original tool call after the user confirms a pending approval.
func (r *Registry) Call(ctx context.Context, name string, jsonArgs []byte) (any, error) {
	t, ok := r.tools[name]
	if !ok {
		return nil, fmt.Errorf("unknown tool %q", name)
	}
	callable, ok := t.Tool.(ttool.CallableTool)
	if !ok {
		return nil, fmt.Errorf("tool %q is not callable", name)
	}
	return callable.Call(ctx, jsonArgs)
}

// DemoTools returns a demo registry: one safe tool plus one dangerous tool so
// the approval chain is exercisable end to end. Both tool bodies are stubs —
// the dangerous one performs no real destructive action.
func DemoTools() *Registry {
	weather := function.NewFunctionTool(getWeather,
		function.WithName("get_weather"),
		function.WithDescription("查询指定城市的天气"))
	del := function.NewFunctionTool(deleteUserData,
		function.WithName("delete_user_data"),
		function.WithDescription("删除指定用户的全部数据，不可恢复（危险操作，执行前需用户确认）"))
	return NewRegistry(
		Tool{Tool: weather},
		Tool{Tool: del, Dangerous: true},
	)
}

type weatherArgs struct {
	City string `json:"city" jsonschema:"description=要查询的城市"`
}

type weatherResult struct {
	City    string `json:"city"`
	Weather string `json:"weather"`
}

func getWeather(_ context.Context, in weatherArgs) (weatherResult, error) {
	return weatherResult{City: in.City, Weather: "晴，26℃（演示数据）"}, nil
}

type deleteArgs struct {
	UserID string `json:"user_id" jsonschema:"description=要删除数据的用户 ID"`
}

type deleteResult struct {
	UserID  string `json:"user_id"`
	Deleted bool   `json:"deleted"`
	Note    string `json:"note"`
}

func deleteUserData(_ context.Context, in deleteArgs) (deleteResult, error) {
	return deleteResult{UserID: in.UserID, Deleted: true, Note: "演示工具，未删除任何真实数据"}, nil
}
