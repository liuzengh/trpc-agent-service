package agent

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/plugin/messagemerger"
	"trpc.group/trpc-go/trpc-agent-go/plugin/toolcallid"
	"trpc.group/trpc-go/trpc-agent-go/plugin/toolsearch"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// PluginPlan is the code-owned projection of a Revision's framework-extension
// references. It carries no tenant supplied constructors or callback options.
type PluginPlan struct {
	ToolSearch     bool
	AwaitUserReply bool
}

type toolSearchPolicyContextKey struct{}

// WithToolSearchPolicy binds the already-admitted, immutable policy snapshot
// to one execution. It is deliberately set only by the Worker after it has
// obtained a RunPermit; an absent or mismatched value makes deferred tools
// invisible rather than falling back to a process default.
func WithToolSearchPolicy(ctx context.Context, policy governance.PolicySnapshot) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, toolSearchPolicyContextKey{}, policy)
}

func toolSearchPolicyFromContext(ctx context.Context) (governance.PolicySnapshot, bool) {
	policy, ok := ctx.Value(toolSearchPolicyContextKey{}).(governance.PolicySnapshot)
	execution, executionOK := runtime.ExecutionContextFrom(ctx)
	return policy, ok && executionOK && policy.TenantID == execution.TenantID && policy.Version == execution.PolicyVersion
}

// CompilePluginPlan validates a Revision's framework-extension capability
// references before the factory opens model or tool surfaces.
func CompilePluginPlan(refs []profile.PluginRef) (PluginPlan, error) {
	var result PluginPlan
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		if ref.ID == "" || ref.Version < 1 {
			return PluginPlan{}, runtime.ErrInvariantViolation
		}
		if _, exists := seen[ref.ID]; exists {
			return PluginPlan{}, runtime.ErrInvariantViolation
		}
		seen[ref.ID] = struct{}{}
		switch ref {
		case profile.PluginRef{ID: agentapp.PluginToolCallID, Version: 1},
			profile.PluginRef{ID: agentapp.PluginMessageMerger, Version: 1}:
		case profile.PluginRef{ID: agentapp.PluginToolSearch, Version: 1}:
			result.ToolSearch = true
		case profile.PluginRef{ID: agentapp.PluginAwaitUserReply, Version: 1}:
			result.AwaitUserReply = true
		default:
			return PluginPlan{}, fmt.Errorf("%w: plugin %s@%d", runtime.ErrCapabilityUnsupported, ref.ID, ref.Version)
		}
	}
	return result, nil
}

// BuildPlugins materializes only code-owned, reviewed trpc-agent-go plugins.
// deferredTools must be the exact, governance-wrapped Tool Surface registered
// on the root LLM Agent. toolsearch uses the same tool values for its catalog;
// it never resolves a second, bypass-capable tool path.
func BuildPlugins(refs []profile.PluginRef, deferredTools []tool.Tool) ([]plugin.Plugin, error) {
	if _, err := CompilePluginPlan(refs); err != nil {
		return nil, err
	}
	result := make([]plugin.Plugin, 0, len(refs))
	for _, ref := range refs {
		switch ref {
		case profile.PluginRef{ID: agentapp.PluginToolCallID, Version: 1}:
			result = append(result, toolcallid.New())
		case profile.PluginRef{ID: agentapp.PluginMessageMerger, Version: 1}:
			// The platform identifier is the single source of truth: the plugin
			// instance is renamed to the reviewed ID a revision references, so
			// observability and diagnostics never leak the framework's default
			// plugin name as a separate contract symbol.
			result = append(result, messagemerger.New(messagemerger.WithName(agentapp.PluginMessageMerger)))
		case profile.PluginRef{ID: agentapp.PluginToolSearch, Version: 1}:
			if len(deferredTools) == 0 {
				return nil, fmt.Errorf("%w: tool_search requires at least one governed tool", runtime.ErrCapabilityUnsupported)
			}
			permissionFilter, filterErr := newToolSearchPermissionFilter(deferredTools)
			if filterErr != nil {
				return nil, filterErr
			}
			// NativeToolCalls is the framework default. It gives a discovered tool
			// its regular function-call schema while keeping the initial model
			// request to one discovery tool. The runner owns its session state.
			result = append(result, toolsearch.New(nil,
				toolsearch.WithName(agentapp.PluginToolSearch),
				toolsearch.WithDeferredTools(deferredTools),
				toolsearch.WithToolPermissionFilter(permissionFilter),
			))
		case profile.PluginRef{ID: agentapp.PluginAwaitUserReply, Version: 1}:
			// This extension is an llmagent option, not a runner.Plugin. Factory
			// materializes it only on the root LLM while Bundle owns the matching
			// session-state route consumer.
		default:
			return nil, fmt.Errorf("%w: plugin %s@%d", runtime.ErrCapabilityUnsupported, ref.ID, ref.Version)
		}
	}
	return result, nil
}

// newToolSearchPermissionFilter keeps the SDK catalog and the Worker tool
// surface on the same exact versioned policy decision. Checking a declaration
// name alone would permit a catalog entry to drift from its guarded callable.
func newToolSearchPermissionFilter(values []tool.Tool) (toolsearch.ToolPermissionFilter, error) {
	refs := make(map[string]governance.VersionedRef, len(values))
	for _, value := range values {
		versioned, ok := value.(governance.VersionedTool)
		if !ok || value == nil || value.Declaration() == nil {
			return nil, runtime.ErrCapabilityUnsupported
		}
		ref := versioned.GovernanceToolRef()
		if ref.ID == "" || ref.Version < 1 || ref.ID != value.Declaration().Name {
			return nil, runtime.ErrCapabilityUnsupported
		}
		if _, exists := refs[ref.ID]; exists {
			return nil, runtime.ErrInvariantViolation
		}
		refs[ref.ID] = ref
	}
	return func(ctx context.Context, names []string) map[string]bool {
		policy, ok := toolSearchPolicyFromContext(ctx)
		if !ok {
			return map[string]bool{}
		}
		allowed := make(map[string]bool, len(names))
		for _, name := range names {
			ref, exists := refs[name]
			if !exists {
				continue
			}
			decision := governance.ToolDecision(policy, ref)
			allowed[name] = decision.Action == governance.ActionAllow || decision.Action == governance.ActionAsk
		}
		return allowed
	}, nil
}
