package domain

import (
	"fmt"
	"strconv"
)

func validateSemantics(spec Spec) []Diagnostic {
	var diagnostics []Diagnostic
	validateDataSemantics(spec, &diagnostics)
	validateWorkspaceSemantics(spec, &diagnostics)
	if _, exists := spec.Nodes[spec.Root]; !exists {
		diagnostics = append(diagnostics, Diagnostic{
			Code: "AGENT_SPEC_ROOT_NOT_FOUND", Severity: SeverityError,
			Pointer: "/root", Message: "root node does not exist",
		})
	}
	parents := make(map[string][]string)
	adjacency := make(map[string][]string, len(spec.Nodes))
	for _, id := range sortedNodeIDs(spec.Nodes) {
		node := spec.Nodes[id]
		refs := node.Children
		field := "children"
		if node.Kind == NodeKindLoop {
			refs = []string{node.Body}
			field = "body"
		}
		for index, ref := range refs {
			pointer := fmt.Sprintf("/nodes/%s/%s", escapeJSONPointer(id), field)
			if field == "children" {
				pointer += "/" + strconv.Itoa(index)
			}
			if _, exists := spec.Nodes[ref]; !exists {
				diagnostics = append(diagnostics, nodeDiagnostic(
					"AGENT_SPEC_NODE_REFERENCE_NOT_FOUND", SeverityError, pointer, id,
					"referenced node does not exist",
				))
				continue
			}
			parents[ref] = append(parents[ref], id)
			adjacency[id] = append(adjacency[id], ref)
		}
		if node.Kind == NodeKindLLM {
			validateNodeSlots(id, node, spec.Requirements, &diagnostics)
		}
	}
	if owners := parents[spec.Root]; len(owners) > 0 {
		diagnostics = append(diagnostics, Diagnostic{
			Code: "AGENT_SPEC_ROOT_HAS_PARENT", Severity: SeverityError,
			Pointer: "/root", Message: "root node must not have a parent",
		})
	}
	for _, id := range sortedNodeIDs(spec.Nodes) {
		if len(parents[id]) > 1 {
			diagnostics = append(diagnostics, nodeDiagnostic(
				"AGENT_SPEC_NODE_MULTIPLE_PARENTS", SeverityError,
				"/nodes/"+escapeJSONPointer(id), id, "node has multiple parents",
			))
		}
	}

	state := make(map[string]uint8, len(spec.Nodes))
	cycleReported := false
	var visit func(string, int)
	visit = func(id string, depth int) {
		if state[id] == 1 {
			if !cycleReported {
				diagnostics = append(diagnostics, nodeDiagnostic(
					"AGENT_SPEC_STRUCTURE_CYCLE", SeverityError,
					"/nodes/"+escapeJSONPointer(id), id, "node structure contains a cycle",
				))
				cycleReported = true
			}
			return
		}
		if state[id] == 2 {
			return
		}
		state[id] = 1
		if depth > MaxDepth {
			diagnostics = append(diagnostics, nodeDiagnostic(
				"AGENT_SPEC_LIMIT_EXCEEDED", SeverityError,
				"/nodes/"+escapeJSONPointer(id), id, "node depth exceeds the V1 limit",
			))
		}
		for _, child := range adjacency[id] {
			visit(child, depth+1)
		}
		state[id] = 2
	}
	if _, exists := spec.Nodes[spec.Root]; exists {
		visit(spec.Root, 1)
	}
	for _, id := range sortedNodeIDs(spec.Nodes) {
		if state[id] == 0 {
			diagnostics = append(diagnostics, nodeDiagnostic(
				"AGENT_SPEC_NODE_UNREACHABLE", SeverityError,
				"/nodes/"+escapeJSONPointer(id), id, "node is unreachable from root",
			))
		}
	}
	appendUnusedSlotWarnings(spec, &diagnostics)
	return diagnostics
}

func validateNodeSlots(id string, node Node, requirements Requirements, diagnostics *[]Diagnostic) {
	pointer := "/nodes/" + escapeJSONPointer(id)
	if _, exists := requirements.Models[node.ModelSlot]; !exists {
		*diagnostics = append(*diagnostics, nodeDiagnostic(
			"AGENT_SPEC_MODEL_SLOT_NOT_FOUND", SeverityError, pointer+"/model_slot", id,
			"model slot is not declared",
		))
	}
	for index, slot := range node.ToolSlots {
		if _, exists := requirements.Tools[slot]; !exists {
			*diagnostics = append(*diagnostics, nodeDiagnostic(
				"AGENT_SPEC_TOOL_SLOT_NOT_FOUND", SeverityError,
				fmt.Sprintf("%s/tool_slots/%d", pointer, index), id, "tool slot is not declared",
			))
		}
	}
	for index, slot := range node.KnowledgeSlots {
		if _, exists := requirements.Knowledge[slot]; !exists {
			*diagnostics = append(*diagnostics, nodeDiagnostic(
				"AGENT_SPEC_KNOWLEDGE_SLOT_NOT_FOUND", SeverityError,
				fmt.Sprintf("%s/knowledge_slots/%d", pointer, index), id,
				"knowledge slot is not declared",
			))
		}
	}
}

func appendUnusedSlotWarnings(spec Spec, diagnostics *[]Diagnostic) {
	usedModels := make(map[string]bool)
	if spec.Runtime != nil && spec.Runtime.Summary != nil && spec.Runtime.Summary.Enabled {
		usedModels[spec.Runtime.Summary.ModelSlot] = true
	}
	usedTools := make(map[string]bool)
	usedKnowledge := make(map[string]bool)
	for _, node := range spec.Nodes {
		if node.Kind != NodeKindLLM {
			continue
		}
		usedModels[node.ModelSlot] = true
		for _, slot := range node.ToolSlots {
			usedTools[slot] = true
		}
		for _, slot := range node.KnowledgeSlots {
			usedKnowledge[slot] = true
		}
	}
	appendWarnings := func(values []string, used map[string]bool, kind, code string) {
		for _, slot := range values {
			if !used[slot] {
				*diagnostics = append(*diagnostics, Diagnostic{
					Code: code, Severity: SeverityWarning,
					Pointer: "/requirements/" + kind + "/" + escapeJSONPointer(slot),
					Message: kind + " slot is declared but unused",
				})
			}
		}
	}
	appendWarnings(sortedModelSlots(spec.Requirements.Models), usedModels, "models", "AGENT_SPEC_UNUSED_MODEL_SLOT")
	appendWarnings(sortedCapabilitySlots(spec.Requirements.Tools), usedTools, "tools", "AGENT_SPEC_UNUSED_TOOL_SLOT")
	appendWarnings(sortedCapabilitySlots(spec.Requirements.Knowledge), usedKnowledge, "knowledge", "AGENT_SPEC_UNUSED_KNOWLEDGE_SLOT")
}
