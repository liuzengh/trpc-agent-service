package domain

import (
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"sort"
)

const (
	DiagnosticBackendUnavailable = "DEPLOYMENT_BACKEND_UNAVAILABLE"
	DiagnosticBackendMismatch    = "DEPLOYMENT_BACKEND_SNAPSHOT_MISMATCH"
	DiagnosticBackendClosure     = "DEPLOYMENT_BACKEND_CLOSURE_INVALID"
)

// BackendRequest is compiler-derived, never part of the user's publish input.
type BackendRequest struct {
	Category, Name, BackendID string
	Revision                  uint64
	Role                      string
	Dimensions                int64
}

func (r BackendRequest) Key() string { return r.Category + "/" + r.Name }
func (r BackendRequest) Path() string {
	return "/" + r.Category + "/" + escapeJSONPointer(r.Name) + "/backend_id"
}

// ManagedBackendRequests follows the existing compiler's actual resource closure.
// Only enabled Agent capabilities select Memory/Artifact. Profile availability
// alone never grants a runtime resource or a tool.
func ManagedBackendRequests(agent agentdomain.Spec, profile profiledomain.Spec) []BackendRequest {
	out := make([]BackendRequest, 0)
	for _, role := range []string{StorageRoleSession, StorageRoleMemory, "artifact"} {
		if role != StorageRoleSession && !agentUsesStorageRole(agent, role) {
			continue
		}
		r, ok := profile.Storage[role]
		if ok && r.Kind.Managed() && r.Kind.Role() == role {
			out = append(out, BackendRequest{Category: "storage", Name: role, BackendID: r.BackendID, Revision: r.BackendRevision, Role: role})
		}
	}
	used := collectUsedRequirements(agent)
	for name := range used.knowledge {
		if _, declared := agent.Requirements.Knowledge[name]; !declared {
			continue
		}
		r, ok := profile.Knowledge[name]
		if ok && r.Kind == profiledomain.KnowledgeKindManaged {
			out = append(out, BackendRequest{Category: "knowledge", Name: name, BackendID: r.BackendID, Revision: r.BackendRevision, Role: "knowledge", Dimensions: r.Embedding.Dimensions})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out
}
func BackendDiagnostic(code string, r BackendRequest) Diagnostic {
	return resourceDiagnostic(code, SeverityError, DiagnosticSourcePlatform, r.Path(), r.Category, r.Name, "selected managed backend could not be resolved to the required fixed execution snapshot")
}
func validBackendMatch(tenant string, r BackendRequest, s datav1.Snapshot) bool {
	if s.ValidateForRole(r.Role) != nil || s.TenantID != tenant || s.BackendID != r.BackendID || s.BackendRevision != r.Revision {
		return false
	}
	switch r.Role {
	case StorageRoleSession:
		return s.Kind == datav1.PostgreSQL || s.Kind == datav1.Redis
	case StorageRoleMemory:
		return s.Kind == datav1.PostgreSQL || s.Kind == datav1.Redis
	case "artifact":
		return s.Kind == datav1.S3
	case "knowledge":
		return s.Kind == datav1.Qdrant && s.Qdrant.Dimensions == r.Dimensions
	default:
		return false
	}
}

// ValidateManagedSnapshots independently verifies adapter output at the pure
// compiler boundary. Missing, extra, foreign, stale and mixed targets fail closed.
func ValidateManagedSnapshots(input CompileInput) []Diagnostic {
	requests := ManagedBackendRequests(input.Agent.Spec, input.Profile.Spec)
	expected := make(map[string]bool, len(requests))
	byID := make(map[string]datav1.Snapshot)
	var diagnostics []Diagnostic
	for _, r := range requests {
		expected[r.Key()] = true
		s, ok := input.ManagedBackends[r.Key()]
		if !ok {
			diagnostics = append(diagnostics, BackendDiagnostic(DiagnosticBackendUnavailable, r))
			continue
		}
		if !validBackendMatch(input.TenantID, r, s) {
			diagnostics = append(diagnostics, BackendDiagnostic(DiagnosticBackendMismatch, r))
			continue
		}
		if previous, ok := byID[r.BackendID]; ok && !samePhysicalBackend(previous, s) {
			diagnostics = append(diagnostics, BackendDiagnostic(DiagnosticBackendMismatch, r))
			continue
		}
		byID[r.BackendID] = s
		if (s.Limits.TimeoutMS+999)/1000 > input.Platform.Execution.MaxRunSeconds {
			diagnostics = append(diagnostics, BackendDiagnostic(DiagnosticLimitExceeded, r))
		}
	}
	for _, key := range sortedKeys(input.ManagedBackends) {
		if !expected[key] {
			diagnostics = append(diagnostics, diagnostic(DiagnosticBackendClosure, SeverityError, DiagnosticSourcePlatform, "/managed_backends", "unrequested backend snapshot is not part of the execution closure"))
		}
	}
	return diagnostics
}

func agentUsesStorageRole(agent agentdomain.Spec, role string) bool {
	for _, n := range agent.Nodes {
		if n.Kind != agentdomain.NodeKindLLM {
			continue
		}
		if role == StorageRoleMemory && n.Memory.Enabled() {
			return true
		}
		if role == "artifact" && n.Artifact != nil && n.Artifact.Enabled {
			return true
		}
	}
	return false
}

// Role changes the authorization scope, not the physical target identity.
func samePhysicalBackend(a, b datav1.Snapshot) bool {
	a = a.Clone()
	b = b.Clone()
	b.Isolation = a.Isolation
	return a.Matches(b)
}
