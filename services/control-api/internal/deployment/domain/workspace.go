package domain

import (
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

func compileExecutors(in CompileInput, d *[]Diagnostic) (map[string]ManifestExecutorResource, map[string]string) {
	out := map[string]ManifestExecutorResource{}
	resolved := map[string]string{}
	for _, id := range sortedKeys(in.Agent.Spec.Nodes) {
		w := in.Agent.Spec.Nodes[id].Workspace
		if w == nil {
			continue
		}
		p := "/nodes/" + escapeJSONPointer(id) + "/workspace"
		if !containsString(in.Platform.RuntimeDataCapabilities, "workspace") {
			*d = append(*d, diagnostic(DiagnosticEntrypointUnsupported, SeverityError, DiagnosticSourcePlatform, p, "workspace adapter is not enabled"))
		}
		r, ok := in.Profile.Spec.Executors[w.ExecutorSlot]
		if !ok {
			*d = append(*d, diagnostic(DiagnosticResourceMissing, SeverityError, DiagnosticSourceProfile, "/executors/"+escapeJSONPointer(w.ExecutorSlot), "same-name executor resource is missing"))
			continue
		}
		if r.Kind != "sdk_sandbox" {
			*d = append(*d, diagnostic(DiagnosticAdapterUnsupported, SeverityError, DiagnosticSourceProfile, "/executors/"+escapeJSONPointer(w.ExecutorSlot)+"/kind", "executor adapter is unsupported"))
			continue
		}
		out[w.ExecutorSlot] = ManifestExecutorResource{Kind: r.Kind, AdapterVersion: deploymentv1.WorkspaceAdapterVersion}
		resolved[w.ExecutorSlot] = w.ExecutorSlot
	}
	return out, resolved
}
func validateWorkspaceManifest(c ManifestContent, a *agentdomain.Spec) error {
	used := map[string]bool{}
	a.Requirements.Executors = map[string]agentdomain.CapabilityRequirement{}
	for key, r := range c.Resources.Executors {
		if r.Validate() != nil {
			return ErrInvalidManifestContent
		}
		a.Requirements.Executors[key] = agentdomain.CapabilityRequirement{Capability: "workspace"}
	}
	for id, n := range c.AgentPlan.Nodes {
		if n.Workspace == nil {
			continue
		}
		w := n.Workspace
		if n.Kind != agentdomain.NodeKindLLM || w.Validate() != nil || c.Execution.MaxToolCalls < 1 {
			return ErrInvalidManifestContent
		}
		used[w.ExecutorResource] = true
		an := a.Nodes[id]
		an.Workspace = &agentdomain.Workspace{ExecutorSlot: w.ExecutorResource, Tools: append([]string{}, w.Tools...)}
		a.Nodes[id] = an
	}
	if !sameResourceClosure(used, c.Resources.Executors, c.ResolvedRequirements.Executors) {
		return ErrInvalidManifestContent
	}
	return nil
}
