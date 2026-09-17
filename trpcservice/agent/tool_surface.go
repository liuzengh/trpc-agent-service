package agent

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	upstreamknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	knowledgetool "trpc.group/trpc-go/trpc-agent-go/knowledge/tool"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// ToolSurfaceCompiler is the sole construction path for tools exposed to an
// LLM Agent. Framework tools and service tools are intentionally indistinct at
// this boundary: every entry is revision-pinned, guarded, budgeted and traced
// before llmagent receives it.
type ToolSurfaceCompiler struct {
	Tools         ToolResolver
	Knowledge     KnowledgeResolver
	Memory        agentmemory.Service
	Policies      governance.Repository
	Confirmations governance.ConfirmationCoordinator
	ToolResults   messaging.ToolResultStore
	Telemetry     telemetry.Provider
}

func (c ToolSurfaceCompiler) Compile(ctx context.Context, plan RuntimePlan) ([]tool.Tool, error) {
	bindings := make([]toolBinding, 0, len(plan.ToolSurface.RevisionRefs)+len(memoryToolRefs)+1)
	if len(plan.ToolSurface.RevisionRefs) != 0 {
		if c.Tools == nil || !plan.Uses(CapabilityTool) {
			return nil, runtime.ErrCapabilityUnsupported
		}
		values, err := c.Tools.ResolveTools(ctx, plan.Snapshot.Key.TenantID, plan.ToolSurface.RevisionRefs)
		if err != nil {
			return nil, err
		}
		if len(values) != len(plan.ToolSurface.RevisionRefs) {
			return nil, runtime.ErrInvariantViolation
		}
		bindings = append(bindings, bindTools(governanceRefs(plan.ToolSurface.RevisionRefs), values)...)
	}
	if c.Memory != nil {
		if !plan.ToolSurface.UsesMemory || !plan.Uses(CapabilityMemory) {
			return nil, runtime.ErrCapabilityUnsupported
		}
		values := c.Memory.Tools()
		refs, err := refsForMemoryTools(values)
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, bindTools(refs, values)...)
	}
	if len(plan.ToolSurface.KnowledgeRefs) != 0 {
		if c.Knowledge == nil || !plan.Uses(CapabilityKnowledge) {
			return nil, runtime.ErrCapabilityUnsupported
		}
		knowledge, err := c.Knowledge.ResolveKnowledge(ctx, plan.Snapshot.Key.TenantID, plan.Snapshot.Key.AgentAppID,
			plan.ToolSurface.KnowledgeRefs, plan.Snapshot.Key.ConfigVersion)
		if err != nil {
			return nil, err
		}
		if knowledge == nil {
			return nil, runtime.ErrCapabilityUnsupported
		}
		bindings = append(bindings, toolBinding{ref: knowledgeToolRef, value: knowledgetool.NewKnowledgeSearchTool(knowledge)})
	}
	if err := validateToolBindings(bindings); err != nil {
		return nil, err
	}
	refs := make([]governance.VersionedRef, len(bindings))
	values := make([]tool.Tool, len(bindings))
	for index, binding := range bindings {
		refs[index], values[index] = binding.ref, binding.value
	}
	if len(values) == 0 {
		return nil, nil
	}
	if c.Policies == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	guarded, err := servicetool.GuardCallablesWithConfirmation(c.Policies, c.Confirmations, c.ToolResults, refs, values)
	if err != nil {
		return nil, err
	}
	return instrumentCallables(c.Telemetry, budgetCallables(guarded)), nil
}

type toolBinding struct {
	ref   governance.VersionedRef
	value tool.Tool
}

func bindTools(refs []governance.VersionedRef, values []tool.Tool) []toolBinding {
	result := make([]toolBinding, 0, len(values))
	for index, value := range values {
		result = append(result, toolBinding{ref: refs[index], value: value})
	}
	return result
}

func validateToolBindings(bindings []toolBinding) error {
	seen := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		if binding.ref.ID == "" || binding.ref.Version < 1 || binding.value == nil || binding.value.Declaration() == nil ||
			binding.value.Declaration().Name != binding.ref.ID {
			return runtime.ErrCapabilityUnsupported
		}
		if _, ok := seen[binding.ref.ID]; ok {
			return runtime.ErrInvariantViolation
		}
		seen[binding.ref.ID] = struct{}{}
	}
	return nil
}

func refsForMemoryTools(values []tool.Tool) ([]governance.VersionedRef, error) {
	refs := make([]governance.VersionedRef, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value == nil || value.Declaration() == nil {
			return nil, runtime.ErrCapabilityUnsupported
		}
		name := value.Declaration().Name
		if _, exists := seen[name]; exists {
			return nil, runtime.ErrInvariantViolation
		}
		seen[name] = struct{}{}
		found := false
		for _, ref := range memoryToolRefs {
			if ref.ID == name {
				refs = append(refs, ref)
				found = true
				break
			}
		}
		if !found {
			return nil, runtime.ErrCapabilityUnsupported
		}
	}
	return refs, nil
}

func isMemoryToolRef(value governance.VersionedRef) bool {
	for _, ref := range memoryToolRefs {
		if ref == value {
			return true
		}
	}
	return false
}

// Keep the import-time assertion close to the compiler. The service accepts
// only the public framework Knowledge interface, never a provider-specific
// vector client at the Agent boundary.
var _ upstreamknowledge.Knowledge = nil
