package domain

import (
	"encoding/json"
	"fmt"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

const DiagnosticCallableNameCollision = "DEPLOYMENT_CALLABLE_NAME_COLLISION"

func dataContractDiagnostics(a agentdomain.Spec, p PlatformExecutionContract) []Diagnostic {
	if len(p.RuntimeDataCapabilities) == 0 {
		return pendingDataContractDiagnostics(a)
	}
	// Preserve the legacy no-data compilation path (including direct Go fixtures).
	// Published data declarations still undergo the complete Agent schema checks.
	if p.Version == deploymentv1.WorkerV1PlatformVersion && len(pendingDataContractDiagnostics(a)) == 0 {
		return nil
	}
	raw, err := json.Marshal(a)
	if err != nil {
		return []Diagnostic{diagnostic(DiagnosticInputInvalid, SeverityError, DiagnosticSourceAgent, "", "invalid agent data declaration")}
	}
	if _, report := agentdomain.ValidateForPublication(raw, 1); !report.Valid {
		return []Diagnostic{diagnostic(DiagnosticInputInvalid, SeverityError, DiagnosticSourceAgent, "", "invalid agent data declaration")}
	}
	var out []Diagnostic
	check := func(enabled bool, capability, path string) {
		if enabled && !containsString(p.RuntimeDataCapabilities, capability) {
			out = append(out, diagnostic(DiagnosticEntrypointUnsupported, SeverityError, DiagnosticSourcePlatform, path, "runtime data capability is not part of the fixed platform contract"))
		}
	}
	check(agentUsesStorageRole(a, "memory"), "memory", "/nodes")
	check(agentUsesStorageRole(a, "artifact"), "artifact", "/nodes")
	check(a.Runtime != nil && a.Runtime.Summary != nil && a.Runtime.Summary.Enabled, "summary", "/runtime/summary")
	return out
}
func compileManifestRuntime(a agentdomain.Spec) *ManifestRuntime {
	if a.Runtime == nil || a.Runtime.Summary == nil || !a.Runtime.Summary.Enabled {
		return nil
	}
	s := a.Runtime.Summary
	return &ManifestRuntime{Summary: &ManifestSummary{Enabled: true, ModelResource: s.ModelSlot, EventThreshold: *s.EventThreshold}}
}
func compileNodeData(n *ManifestNode, s agentdomain.Node, id string, p PlatformExecutionContract, d *[]Diagnostic) {
	if s.Memory.Enabled() {
		n.Memory = &ManifestMemory{Resource: "memory", Tools: sortedUnique(s.Memory.Tools)}
		if s.Memory.PreloadLimit != nil {
			v := *s.Memory.PreloadLimit
			n.Memory.PreloadLimit = &v
		}
	}
	if s.Artifact != nil && s.Artifact.Enabled {
		n.Artifact = &ManifestArtifact{Enabled: true, Resource: "artifact"}
	}
	if s.AddSessionSummary != nil && *s.AddSessionSummary {
		v := true
		n.AddSessionSummary = &v
	}
	if n.Memory != nil {
		if err := validateNodeCallableNames(n.CallableEntries, n.Memory.Tools, ProviderCallableNames); err != nil {
			*d = append(*d, diagnostic(DiagnosticCallableNameCollision, SeverityError, DiagnosticSourceAgent, "/nodes/"+escapeJSONPointer(id)+"/memory/tools", "resolved tool registration names collide"))
		}
		if len(n.CallableEntries)+len(n.Memory.Tools) > p.Limits.MaxCallableEntriesPerNode {
			*d = append(*d, diagnostic(DiagnosticLimitExceeded, SeverityError, DiagnosticSourceAgent, "/nodes/"+escapeJSONPointer(id)+"/memory/tools", "combined tool count exceeds platform limit"))
		}
	}
}
func validateNodeCallableNames(entries, memory []string, resolve func([]string) (map[string]string, error)) error {
	names, err := resolve(entries)
	if err != nil {
		return err
	}
	used := map[string]bool{}
	for _, name := range names {
		if used[name] {
			return fmt.Errorf("%w: %s", ErrCallableNameCollision, name)
		}
		used[name] = true
	}
	for _, name := range memory {
		if used[name] {
			return fmt.Errorf("%w: %s", ErrCallableNameCollision, name)
		}
		used[name] = true
	}
	return nil
}
