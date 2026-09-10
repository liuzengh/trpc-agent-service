package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

type governedTestTool struct {
	tool.Tool
	ref governance.VersionedRef
}

func (t governedTestTool) GovernanceToolRef() governance.VersionedRef { return t.ref }

func newGovernedTestTool() tool.Tool {
	return governedTestTool{Tool: function.NewFunctionTool(
		func(context.Context, struct{}) (string, error) { return "ok", nil },
		function.WithName("weather"),
		function.WithDescription("read weather"),
	), ref: governance.VersionedRef{ID: "weather", Version: 2}}
}

func TestBuildPluginsOnlyMaterializesReviewedOfficialPlugins(t *testing.T) {
	deferred := newGovernedTestTool()
	plugins, err := BuildPlugins([]profile.PluginRef{
		{ID: agentapp.PluginToolCallID, Version: 1},
		{ID: agentapp.PluginMessageMerger, Version: 1},
		{ID: agentapp.PluginToolSearch, Version: 1},
	}, []tool.Tool{deferred})
	if err != nil {
		t.Fatal(err)
	}
	if len(plugins) != 3 || plugins[0].Name() != agentapp.PluginToolCallID || plugins[1].Name() != agentapp.PluginMessageMerger || plugins[2].Name() != agentapp.PluginToolSearch {
		t.Fatalf("plugins=%#v", plugins)
	}
	for _, refs := range [][]profile.PluginRef{
		{{ID: "arbitrary_callback", Version: 1}},
		{{ID: agentapp.PluginToolCallID, Version: 2}},
		{{ID: agentapp.PluginToolCallID, Version: 1}, {ID: agentapp.PluginToolCallID, Version: 1}},
	} {
		if _, err := BuildPlugins(refs, nil); !errors.Is(err, runtime.ErrCapabilityUnsupported) && !errors.Is(err, runtime.ErrInvariantViolation) {
			t.Fatalf("refs=%#v err=%v", refs, err)
		}
	}
}

func TestBuildPluginsRejectsToolSearchWithoutGovernedSurface(t *testing.T) {
	_, err := BuildPlugins([]profile.PluginRef{{ID: agentapp.PluginToolSearch, Version: 1}}, nil)
	if !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("err=%v", err)
	}
}

func TestToolSearchPermissionFilterUsesExecutionPolicySnapshot(t *testing.T) {
	filter, err := newToolSearchPermissionFilter([]tool.Tool{newGovernedTestTool()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant-a", RequestID: "request-a", PolicyVersion: 7})
	if got := filter(ctx, []string{"weather"}); got["weather"] {
		t.Fatalf("tool visible without policy: %#v", got)
	}
	policy := governance.PolicySnapshot{TenantID: "tenant-a", Version: 7, Policy: governance.PolicyV1{
		Tools: []governance.ToolRule{{ToolID: "weather", Version: 2}},
	}}
	ctx = WithToolSearchPolicy(ctx, policy)
	if got := filter(ctx, []string{"weather", "unknown"}); !got["weather"] || got["unknown"] {
		t.Fatalf("policy visibility=%#v", got)
	}
	wrong := WithToolSearchPolicy(ctx, governance.PolicySnapshot{TenantID: "tenant-a", Version: 8, Policy: policy.Policy})
	if got := filter(wrong, []string{"weather"}); got["weather"] {
		t.Fatalf("mismatched policy version visible: %#v", got)
	}
}

func TestBuildPluginsRejectsToolSearchWithoutVersionedGovernanceTool(t *testing.T) {
	raw := function.NewFunctionTool(
		func(context.Context, struct{}) (string, error) { return "ok", nil },
		function.WithName("weather"),
	)
	_, err := BuildPlugins([]profile.PluginRef{{ID: agentapp.PluginToolSearch, Version: 1}}, []tool.Tool{raw})
	if !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("err=%v", err)
	}
}

func TestAwaitUserReplyExtensionHasNoRunnerPlugin(t *testing.T) {
	plugins, err := BuildPlugins([]profile.PluginRef{{ID: agentapp.PluginAwaitUserReply, Version: 1}}, nil)
	if err != nil {
		t.Fatalf("BuildPlugins await_user_reply: %v", err)
	}
	if len(plugins) != 0 {
		t.Fatalf("await_user_reply produced runner plugins: %d", len(plugins))
	}
}
