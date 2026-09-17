// Package profile resolves immutable control-plane versions into executable
// runtime snapshots.
package profile

import "github.com/liuzengh/trpc-agent-service/trpcservice/agentapp"

type VersionedRef struct {
	ID            string
	Version       int64
	ContentDigest string
}

type SkillRef = agentapp.SkillRef
type PluginRef = agentapp.PluginRef
type AgentSpecV1 = agentapp.AgentSpecV1

type CapabilitySet map[string]bool
type GenerationConfigV1 map[string]any
type RuntimePolicyV1 map[string]any

// BackendBinding pins one runtime storage domain to the exact Backend Profile
// revision selected by the published ConfigSnapshot.  It intentionally keeps
// only control-plane references: deployment connection strings remain outside
// the tenant profile and are resolved by the process composition root.
type BackendBinding struct {
	Domain           string
	BackendProfileID string
	BackendVersion   int64
	Required         []string
}

// ExecutionBudgetV1 is the immutable execution limiter projected from the
// published Agent App revision.
type ExecutionBudgetV1 struct {
	MaxLLMCalls             int
	MaxToolCalls            int
	MaxParallelTools        int
	ExecutionTimeoutSeconds int
}

type ExecutionProfileKey struct {
	TenantID         string
	TenantVersion    int64
	AgentAppID       string
	AgentAppVersion  int64
	AgentAppRevision int64
	ContentDigest    string
	ConfigVersion    int64
	PolicyVersion    int64
}

type ExecutionProfileSnapshot struct {
	Key                 ExecutionProfileKey
	TenantVersion       int64
	AgentAppVersion     int64
	ContentDigest       string
	AppName             string
	AgentKind           agentapp.AgentKind
	AgentSpec           AgentSpecV1
	Description         string
	Instruction         string
	GlobalInstruction   string
	ModelProfileRef     VersionedRef
	FallbackModelRefs   []VersionedRef
	ToolRefs            []VersionedRef
	SkillRefs           []SkillRef
	PluginRefs          []PluginRef
	KnowledgeRefs       []VersionedRef
	GenerationConfig    GenerationConfigV1
	RuntimePolicy       RuntimePolicyV1
	ExecutionBudget     ExecutionBudgetV1
	BackendRequirements CapabilitySet
	BackendBindings     []BackendBinding
}

// BackendBindingFor returns the one pinned binding for a storage domain. A
// duplicate domain is rejected rather than silently selecting an arbitrary
// backend from an immutable execution snapshot.
func (s ExecutionProfileSnapshot) BackendBindingFor(domain string) (BackendBinding, bool) {
	var result BackendBinding
	for _, binding := range s.BackendBindings {
		if binding.Domain != domain {
			continue
		}
		if result.Domain != "" {
			return BackendBinding{}, false
		}
		result = BackendBinding{Domain: binding.Domain, BackendProfileID: binding.BackendProfileID,
			BackendVersion: binding.BackendVersion, Required: append([]string(nil), binding.Required...)}
	}
	return result, result.Domain != ""
}
