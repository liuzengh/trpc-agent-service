package agent

import (
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

func TestCapabilityRegistryCompilesOnlyDeclaredRuntimeSurfaces(t *testing.T) {
	plan, err := DefaultCapabilityRegistry().CompileRuntimePlan(profile.ExecutionProfileSnapshot{
		AgentKind:       agentapp.AgentKindLLM,
		ModelProfileRef: profile.VersionedRef{ID: "model", Version: 1},
		ToolRefs:        []profile.VersionedRef{{ID: "weather", Version: 2}},
		KnowledgeRefs:   []profile.VersionedRef{{ID: "handbook", Version: 3}},
		SkillRefs: []profile.SkillRef{{ID: "skill", Version: 1,
			ContentDigest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}},
		PluginRefs: []profile.PluginRef{{ID: agentapp.PluginToolSearch, Version: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, capability := range []RuntimeCapability{CapabilityModel, CapabilityTool, CapabilityKnowledge, CapabilitySkill, CapabilityPlugin} {
		if !plan.Uses(capability) {
			t.Fatalf("compiled plan missing capability %q", capability)
		}
	}
	if plan.Uses(CapabilityMemory) || plan.Uses(CapabilityCheckpoint) {
		t.Fatalf("plan admitted undeclared capability: %#v", plan.Capabilities)
	}
	if len(plan.ToolSurface.RevisionRefs) != 1 || len(plan.ToolSurface.KnowledgeRefs) != 1 {
		t.Fatalf("unexpected tool surface: %#v", plan.ToolSurface)
	}
}

func TestCapabilityRegistryRejectsAmbiguousOrReservedToolRefs(t *testing.T) {
	registry := DefaultCapabilityRegistry()
	base := profile.ExecutionProfileSnapshot{AgentKind: agentapp.AgentKindLLM,
		ModelProfileRef: profile.VersionedRef{ID: "model", Version: 1}}
	for _, refs := range [][]profile.VersionedRef{
		{{ID: "same", Version: 1}, {ID: "same", Version: 1}},
		{{ID: knowledgeToolRef.ID, Version: platformToolVersion}},
		{{ID: agentapp.PluginToolSearch, Version: platformToolVersion}},
		{{ID: "call_tool", Version: platformToolVersion}},
		{{ID: "", Version: 1}},
	} {
		base.ToolRefs = refs
		if _, err := registry.CompileRuntimePlan(base); !errors.Is(err, runtime.ErrInvariantViolation) &&
			!errors.Is(err, runtime.ErrCapabilityUnsupported) {
			t.Fatalf("refs=%#v err=%v", refs, err)
		}
	}
}
