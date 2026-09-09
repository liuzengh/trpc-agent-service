package tool

import (
	"context"
	"slices"
	"sort"
	"testing"

	ttool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

func testRegistry() *Registry {
	mk := func(name string) ttool.Tool {
		return function.NewFunctionTool(
			func(_ context.Context, in struct {
				X string `json:"x"`
			}) (map[string]string, error) {
				return map[string]string{"tool": name, "x": in.X}, nil
			},
			function.WithName(name), function.WithDescription("test tool "+name))
	}
	return NewRegistry(
		Tool{Tool: mk("alpha")},
		Tool{Tool: mk("beta")},
		Tool{Tool: mk("gamma"), Dangerous: true},
	)
}

func toolNames(tools []ttool.Tool) []string {
	names := make([]string, 0, len(tools))
	for _, t := range tools {
		names = append(names, t.Declaration().Name)
	}
	sort.Strings(names)
	return names
}

func equalNames(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestAllowedEmptyPolicyKeepsEverything(t *testing.T) {
	r := testRegistry()
	got := toolNames(r.Allowed(ToolPolicy{}, ToolPolicy{}))
	if !equalNames(got, "alpha", "beta", "gamma") {
		t.Fatalf("want all tools, got %v", got)
	}
}

func TestAllowedTenantWhitelist(t *testing.T) {
	r := testRegistry()
	got := toolNames(r.Allowed(ToolPolicy{Allow: []string{"alpha", "beta"}}))
	if !equalNames(got, "alpha", "beta") {
		t.Fatalf("want alpha+beta, got %v", got)
	}
}

func TestAllowedDenySubtracts(t *testing.T) {
	r := testRegistry()
	got := toolNames(r.Allowed(ToolPolicy{Deny: []string{"gamma"}}))
	if !equalNames(got, "alpha", "beta") {
		t.Fatalf("want gamma removed, got %v", got)
	}
}

func TestAllowedAppNarrowsTenant(t *testing.T) {
	r := testRegistry()
	// The app policy can only narrow the tenant whitelist: gamma is not in
	// the tenant allow list, so allowing it at app level has no effect.
	got := toolNames(r.Allowed(
		ToolPolicy{Allow: []string{"alpha", "beta"}},
		ToolPolicy{Allow: []string{"beta", "gamma"}},
	))
	if !equalNames(got, "beta") {
		t.Fatalf("want only beta, got %v", got)
	}
}

func TestAllowedUnknownNamesIgnored(t *testing.T) {
	r := testRegistry()
	got := toolNames(r.Allowed(ToolPolicy{Allow: []string{"alpha", "nope"}, Deny: []string{"ghost"}}))
	if !equalNames(got, "alpha") {
		t.Fatalf("want alpha only, got %v", got)
	}
}

// The tool list goes into the model's prompt; map iteration would reshuffle
// it on every call, so both All and Allowed return tools in name order.
func TestRegistryToolsAreSortedByName(t *testing.T) {
	r := NewRegistry(
		Tool{Tool: notCallableTool{name: "weather"}},
		Tool{Tool: notCallableTool{name: "memory_search"}},
		Tool{Tool: notCallableTool{name: "approval_check"}},
	)
	names := func(tools []ttool.Tool) []string {
		out := make([]string, 0, len(tools))
		for _, tool := range tools {
			out = append(out, tool.Declaration().Name)
		}
		return out
	}
	want := []string{"approval_check", "memory_search", "weather"}
	for i := 0; i < 20; i++ {
		if got := names(r.All()); !slices.Equal(got, want) {
			t.Fatalf("All() = %v, want %v", got, want)
		}
		if got := names(r.Allowed()); !slices.Equal(got, want) {
			t.Fatalf("Allowed() = %v, want %v", got, want)
		}
	}
	// The policy-narrowed list keeps the same ordering guarantee.
	got := names(r.Allowed(ToolPolicy{Allow: []string{"weather", "memory_search"}}))
	if !slices.Equal(got, []string{"memory_search", "weather"}) {
		t.Fatalf("Allowed(allow) = %v, want [memory_search weather]", got)
	}
}
