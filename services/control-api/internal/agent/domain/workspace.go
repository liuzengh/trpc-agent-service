package domain

import "strconv"

type Workspace struct {
	ExecutorSlot string   `json:"executor_slot"`
	Tools        []string `json:"tools"`
}

func validateWorkspaceShape(p string, n map[string]any, d *[]Diagnostic) {
	v, exists := n["workspace"]
	if !exists {
		return
	}
	q := p + "/workspace"
	o, ok := dataObject(v, q, []string{"executor_slot", "tools"}, []string{"executor_slot", "tools"}, d)
	if !ok {
		return
	}
	validateIdentifierField(o, "executor_slot", q, d)
	if t, ok := o["tools"]; ok {
		validateStringArray(t, q+"/tools", 1, 2, capabilityPattern, "AGENT_SPEC_DUPLICATE_WORKSPACE_TOOL", d)
		if a, ok := t.([]any); ok {
			for i, v := range a {
				if s, ok := v.(string); ok && s != "workspace_exec" && s != "workspace_save_artifact" {
					*d = append(*d, nodeDiagnostic("AGENT_SPEC_WORKSPACE_TOOL_UNSUPPORTED", SeverityError, q+"/tools/"+strconv.Itoa(i), "", "workspace tool is not supported"))
				}
			}
		}
	}
}
func validateWorkspaceSemantics(s Spec, d *[]Diagnostic) {
	for _, key := range sortedCapabilitySlots(s.Requirements.Executors) {
		if s.Requirements.Executors[key].Capability != "workspace" {
			*d = append(*d, nodeDiagnostic("AGENT_SPEC_EXECUTOR_CAPABILITY_UNSUPPORTED", SeverityError, "/requirements/executors/"+escapeJSONPointer(key)+"/capability", "", "executor capability must be workspace"))
		}
	}
	for _, id := range sortedNodeIDs(s.Nodes) {
		n := s.Nodes[id]
		if n.Workspace == nil {
			continue
		}
		p := "/nodes/" + escapeJSONPointer(id) + "/workspace"
		if _, ok := s.Requirements.Executors[n.Workspace.ExecutorSlot]; !ok {
			*d = append(*d, nodeDiagnostic("AGENT_SPEC_EXECUTOR_SLOT_NOT_FOUND", SeverityError, p+"/executor_slot", id, "executor slot is not declared"))
		}
		for _, name := range n.Workspace.Tools {
			if name == "workspace_save_artifact" && (n.Artifact == nil || !n.Artifact.Enabled) {
				*d = append(*d, nodeDiagnostic("AGENT_SPEC_WORKSPACE_ARTIFACT_REQUIRED", SeverityError, p+"/tools", id, "workspace_save_artifact requires this node artifact.enabled"))
			}
		}
	}
}
