package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// RuntimeCapability names a service-approved tRPC-Agent-Go capability.  These
// identifiers are deliberately owned by this service rather than mirroring Go
// package paths: a published revision may select a reviewed capability, but it
// can never select arbitrary framework code or constructor options.
type RuntimeCapability string

const (
	CapabilityModel      RuntimeCapability = "model"
	CapabilityTool       RuntimeCapability = "tool"
	CapabilityMemory     RuntimeCapability = "memory"
	CapabilityKnowledge  RuntimeCapability = "knowledge"
	CapabilitySkill      RuntimeCapability = "skill"
	CapabilityPlugin     RuntimeCapability = "plugin"
	CapabilityCheckpoint RuntimeCapability = "checkpoint"
	// CapabilityGraphCondition authorizes only a published Graph's static,
	// code-reviewed branch selector.  It does not authorize user expressions,
	// scripts, or dynamically selected node names.
	CapabilityGraphCondition RuntimeCapability = "graph_condition"
)

// CapabilityDescriptor is the static, code-owned admission record for a
// runtime feature. Version is a service contract version, not an upstream
// module version; changing its semantics requires a new published revision.
type CapabilityDescriptor struct {
	ID      RuntimeCapability
	Version int64
}

// CapabilityRegistry is the allow-list through which a revision is compiled
// into a runtime plan. It is intentionally small: provider, tool and backend
// catalogs still own their type-specific immutable profiles, while this
// registry owns composition at the Agent runtime boundary.
type CapabilityRegistry struct {
	descriptors map[RuntimeCapability]CapabilityDescriptor
}

func DefaultCapabilityRegistry() CapabilityRegistry {
	return CapabilityRegistry{descriptors: map[RuntimeCapability]CapabilityDescriptor{
		CapabilityModel:          {ID: CapabilityModel, Version: 1},
		CapabilityTool:           {ID: CapabilityTool, Version: 1},
		CapabilityMemory:         {ID: CapabilityMemory, Version: 1},
		CapabilityKnowledge:      {ID: CapabilityKnowledge, Version: 1},
		CapabilitySkill:          {ID: CapabilitySkill, Version: 1},
		CapabilityPlugin:         {ID: CapabilityPlugin, Version: 1},
		CapabilityCheckpoint:     {ID: CapabilityCheckpoint, Version: 1},
		CapabilityGraphCondition: {ID: CapabilityGraphCondition, Version: 1},
	}}
}

func (r CapabilityRegistry) supports(capability RuntimeCapability) bool {
	descriptor, ok := r.descriptors[capability]
	return ok && descriptor.ID == capability && descriptor.Version == 1
}

// RuntimePlan is the only input used to compose upstream runtime objects. It
// projects an immutable execution snapshot into declared framework surfaces;
// it does not contain live connections or raw tools.
type RuntimePlan struct {
	Snapshot     profile.ExecutionProfileSnapshot
	Capabilities map[RuntimeCapability]CapabilityDescriptor
	ToolSurface  ToolSurfacePlan
}

func (p RuntimePlan) Uses(capability RuntimeCapability) bool {
	_, ok := p.Capabilities[capability]
	return ok
}

// ToolSurfacePlan identifies every source that can contribute a tool to an
// LLM Agent. Keeping the sources explicit prevents helpers such as
// llmagent.WithKnowledge from silently adding an ungoverned tool.
type ToolSurfacePlan struct {
	RevisionRefs  []profile.VersionedRef
	UsesMemory    bool
	KnowledgeRefs []profile.VersionedRef
}

// CompileRuntimePlan validates the control-plane projection before any
// framework constructor is called. This is an admission boundary, not a
// replacement for revision validation: it also protects snapshots from custom
// resolvers and tests that bypass persistence.
func (r CapabilityRegistry) CompileRuntimePlan(snapshot profile.ExecutionProfileSnapshot) (RuntimePlan, error) {
	if len(r.descriptors) == 0 {
		r = DefaultCapabilityRegistry()
	}
	plan := RuntimePlan{Snapshot: snapshot, Capabilities: make(map[RuntimeCapability]CapabilityDescriptor)}
	add := func(capability RuntimeCapability) error {
		if !r.supports(capability) {
			return runtime.ErrCapabilityUnsupported
		}
		plan.Capabilities[capability] = r.descriptors[capability]
		return nil
	}
	if snapshot.AgentKind == agentapp.AgentKindLLM {
		if snapshot.ModelProfileRef.ID == "" || snapshot.ModelProfileRef.Version < 1 {
			return RuntimePlan{}, runtime.ErrInvariantViolation
		}
		if err := add(CapabilityModel); err != nil {
			return RuntimePlan{}, err
		}
	}
	if len(snapshot.ToolRefs) != 0 {
		if err := validateVersionedRefs(snapshot.ToolRefs); err != nil {
			return RuntimePlan{}, err
		}
		if err := rejectReservedToolRefs(snapshot.ToolRefs); err != nil {
			return RuntimePlan{}, err
		}
		if err := add(CapabilityTool); err != nil {
			return RuntimePlan{}, err
		}
		plan.ToolSurface.RevisionRefs = append([]profile.VersionedRef(nil), snapshot.ToolRefs...)
	}
	if len(snapshot.SkillRefs) != 0 {
		if err := validateSkillRefs(snapshot.SkillRefs); err != nil {
			return RuntimePlan{}, err
		}
		if err := add(CapabilitySkill); err != nil {
			return RuntimePlan{}, err
		}
	}
	if len(snapshot.PluginRefs) != 0 {
		plugins, err := CompilePluginPlan(snapshot.PluginRefs)
		if err != nil {
			return RuntimePlan{}, err
		}
		if (plugins.ToolSearch || plugins.AwaitUserReply) && snapshot.AgentKind != agentapp.AgentKindLLM {
			return RuntimePlan{}, runtime.ErrCapabilityUnsupported
		}
		if err := add(CapabilityPlugin); err != nil {
			return RuntimePlan{}, err
		}
	}
	if len(snapshot.KnowledgeRefs) != 0 {
		if err := validateVersionedRefs(snapshot.KnowledgeRefs); err != nil {
			return RuntimePlan{}, err
		}
		if err := add(CapabilityKnowledge); err != nil {
			return RuntimePlan{}, err
		}
		plan.ToolSurface.KnowledgeRefs = append([]profile.VersionedRef(nil), snapshot.KnowledgeRefs...)
	}
	if snapshot.AgentKind == agentapp.AgentKindGraph && snapshot.AgentSpec.Checkpoint.Required {
		if err := add(CapabilityCheckpoint); err != nil {
			return RuntimePlan{}, err
		}
	}
	if snapshot.AgentKind == agentapp.AgentKindGraph && hasConditionalGraphEdges(snapshot.AgentSpec.Edges) {
		if err := add(CapabilityGraphCondition); err != nil {
			return RuntimePlan{}, err
		}
	}
	return plan, nil
}

func hasConditionalGraphEdges(edges []agentapp.AgentEdgeSpecV1) bool {
	for _, edge := range edges {
		if edge.ConditionRef != nil {
			return true
		}
	}
	return false
}

func validateVersionedRefs(refs []profile.VersionedRef) error {
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		key := fmt.Sprintf("%s\x00%d", ref.ID, ref.Version)
		if ref.ID == "" || ref.Version < 1 {
			return runtime.ErrInvariantViolation
		}
		if _, ok := seen[key]; ok {
			return runtime.ErrInvariantViolation
		}
		seen[key] = struct{}{}
	}
	return nil
}

// validateSkillRefs repeats the immutable publication contract at the runtime
// admission boundary. Control-plane repositories already validate drafts, but
// custom profile resolvers and tests can construct snapshots directly.
func validateSkillRefs(refs []profile.SkillRef) error {
	seen := make(map[string]struct{}, len(refs))
	for _, ref := range refs {
		if ref.ID == "" || ref.Version < 1 || !isSHA256Digest(ref.ContentDigest) {
			return runtime.ErrInvariantViolation
		}
		if _, exists := seen[ref.ID]; exists {
			return runtime.ErrInvariantViolation
		}
		seen[ref.ID] = struct{}{}
	}
	return nil
}

func isSHA256Digest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func governanceRefs(refs []profile.VersionedRef) []governance.VersionedRef {
	result := make([]governance.VersionedRef, len(refs))
	for index, ref := range refs {
		result[index] = governance.VersionedRef{ID: ref.ID, Version: ref.Version}
	}
	return result
}
