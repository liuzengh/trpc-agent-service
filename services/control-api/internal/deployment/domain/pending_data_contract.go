package domain

import agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"

// Preserve fail-closed behavior for production Worker V1 and static contracts
// without data capabilities. Explicit tested data contracts use the complete
// compiler path; declaration support alone never enables production execution.
func pendingDataContractDiagnostics(s agentdomain.Spec) []Diagnostic {
	var out []Diagnostic
	reject := func(path string) {
		out = append(out, diagnostic(DiagnosticEntrypointUnsupported, SeverityError, DiagnosticSourceAgent, path, "runtime data declaration awaits the matching manifest compilation contract"))
	}
	if s.Runtime != nil {
		reject("/runtime")
	}
	for _, id := range sortedKeys(s.Nodes) {
		n := s.Nodes[id]
		p := "/nodes/" + escapeJSONPointer(id)
		if n.Memory != nil {
			reject(p + "/memory")
		}
		if n.Artifact != nil {
			reject(p + "/artifact")
		}
		if n.AddSessionSummary != nil {
			reject(p + "/add_session_summary")
		}
	}
	return out
}
