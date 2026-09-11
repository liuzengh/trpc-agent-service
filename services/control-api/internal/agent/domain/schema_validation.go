package domain

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

var (
	identifierPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
	capabilityPattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)
)

func validateSchemaShape(root map[string]any) []Diagnostic {
	var diagnostics []Diagnostic
	validateAllowedFields(root, "", []string{"schema_version", "root", "requirements", "nodes", "runtime"}, &diagnostics)
	requireFields(root, "", []string{"schema_version", "root", "requirements", "nodes"}, &diagnostics)

	if version, exists := root["schema_version"]; exists {
		value, ok := version.(string)
		if !ok {
			addTypeDiagnostic("/schema_version", &diagnostics)
		} else if value != SchemaVersionV1 {
			diagnostics = append(diagnostics, Diagnostic{
				Code: "AGENT_SPEC_UNSUPPORTED_VERSION", Severity: SeverityError,
				Pointer: "/schema_version", Message: "schema_version is not supported",
			})
		}
	}
	validateIdentifierField(root, "root", "", &diagnostics)
	if requirements, exists := root["requirements"]; exists {
		validateRequirements(requirements, &diagnostics)
	}
	if nodes, exists := root["nodes"]; exists {
		validateNodes(nodes, &diagnostics)
	}
	if value, exists := root["runtime"]; exists {
		validateRuntime(value, &diagnostics)
	}
	return diagnostics
}

func validateRequirements(value any, diagnostics *[]Diagnostic) {
	requirements, ok := value.(map[string]any)
	if !ok {
		addTypeDiagnostic("/requirements", diagnostics)
		return
	}
	validateAllowedFields(requirements, "/requirements", []string{"models", "tools", "knowledge", "executors"}, diagnostics)
	requireFields(requirements, "/requirements", []string{"models", "tools", "knowledge"}, diagnostics)
	if v, exists := requirements["executors"]; exists {
		validateRequirementMap(v, "/requirements/executors", 16, false, diagnostics)
	}
	if models, exists := requirements["models"]; exists {
		validateRequirementMap(models, "/requirements/models", MaxModelSlots, true, diagnostics)
	}
	if tools, exists := requirements["tools"]; exists {
		validateRequirementMap(tools, "/requirements/tools", MaxToolSlots, false, diagnostics)
	}
	if knowledge, exists := requirements["knowledge"]; exists {
		validateRequirementMap(knowledge, "/requirements/knowledge", MaxKnowledgeSlots, false, diagnostics)
	}
}

func validateRequirementMap(
	value any,
	pointer string,
	maximum int,
	model bool,
	diagnostics *[]Diagnostic,
) {
	items, ok := value.(map[string]any)
	if !ok {
		addTypeDiagnostic(pointer, diagnostics)
		return
	}
	if len(items) > maximum {
		addLimitDiagnostic(pointer, diagnostics)
	}
	for _, name := range sortedMapKeys(items) {
		itemPointer := pointer + "/" + escapeJSONPointer(name)
		if !identifierPattern.MatchString(name) {
			addIdentifierDiagnostic(itemPointer, diagnostics)
		}
		object, ok := items[name].(map[string]any)
		if !ok {
			addTypeDiagnostic(itemPointer, diagnostics)
			continue
		}
		if model {
			validateAllowedFields(object, itemPointer, []string{"capabilities"}, diagnostics)
			requireFields(object, itemPointer, []string{"capabilities"}, diagnostics)
			if capabilities, exists := object["capabilities"]; exists {
				validateStringArray(capabilities, itemPointer+"/capabilities", 1, 16, capabilityPattern, "", diagnostics)
			}
			continue
		}
		validateAllowedFields(object, itemPointer, []string{"capability"}, diagnostics)
		requireFields(object, itemPointer, []string{"capability"}, diagnostics)
		if capability, exists := object["capability"]; exists {
			text, ok := capability.(string)
			if !ok {
				addTypeDiagnostic(itemPointer+"/capability", diagnostics)
			} else if !capabilityPattern.MatchString(text) {
				addIdentifierDiagnostic(itemPointer+"/capability", diagnostics)
			}
		}
	}
}

func validateNodes(value any, diagnostics *[]Diagnostic) {
	nodes, ok := value.(map[string]any)
	if !ok {
		addTypeDiagnostic("/nodes", diagnostics)
		return
	}
	if len(nodes) == 0 || len(nodes) > MaxNodes {
		addLimitDiagnostic("/nodes", diagnostics)
	}
	for _, id := range sortedMapKeys(nodes) {
		pointer := "/nodes/" + escapeJSONPointer(id)
		if !identifierPattern.MatchString(id) {
			addIdentifierDiagnostic(pointer, diagnostics)
		}
		node, ok := nodes[id].(map[string]any)
		if !ok {
			addTypeDiagnostic(pointer, diagnostics)
			continue
		}
		validateNode(id, pointer, node, diagnostics)
	}
}

func validateNode(id, pointer string, node map[string]any, diagnostics *[]Diagnostic) {
	kindValue, exists := node["kind"]
	if !exists {
		requireFields(node, pointer, []string{"kind"}, diagnostics)
		return
	}
	kind, ok := kindValue.(string)
	if !ok {
		addTypeDiagnostic(pointer+"/kind", diagnostics)
		return
	}
	var allowed, required []string
	switch NodeKind(kind) {
	case NodeKindLLM:
		allowed = []string{"kind", "name", "instruction", "model_slot", "tool_slots", "knowledge_slots", "generation", "memory", "artifact", "add_session_summary", "workspace"}
		required = []string{"kind", "instruction", "model_slot", "tool_slots", "knowledge_slots"}
	case NodeKindSequence, NodeKindParallel:
		allowed = []string{"kind", "name", "children"}
		required = []string{"kind", "children"}
	case NodeKindLoop:
		allowed = []string{"kind", "name", "body", "max_iterations"}
		required = []string{"kind", "body", "max_iterations"}
	default:
		*diagnostics = append(*diagnostics, nodeDiagnostic(
			"AGENT_SPEC_UNSUPPORTED_NODE_KIND", SeverityError, pointer+"/kind", id,
			"node kind is not supported in AgentSpec V1",
		))
		return
	}
	validateAllowedFields(node, pointer, allowed, diagnostics)
	requireFields(node, pointer, required, diagnostics)
	if name, exists := node["name"]; exists {
		text, ok := name.(string)
		if !ok {
			addTypeDiagnostic(pointer+"/name", diagnostics)
		} else if strings.TrimSpace(text) == "" || utf8.RuneCountInString(text) > 128 {
			addLimitDiagnostic(pointer+"/name", diagnostics)
		}
	}
	switch NodeKind(kind) {
	case NodeKindLLM:
		validateLLMNode(pointer, node, diagnostics)
	case NodeKindSequence, NodeKindParallel:
		if children, exists := node["children"]; exists {
			validateStringArray(children, pointer+"/children", 1, MaxChildren, identifierPattern,
				"AGENT_SPEC_DUPLICATE_CHILD", diagnostics)
		}
	case NodeKindLoop:
		validateIdentifierField(node, "body", pointer, diagnostics)
		if iterations, exists := node["max_iterations"]; exists {
			value, ok := integerValue(iterations)
			if !ok {
				addTypeDiagnostic(pointer+"/max_iterations", diagnostics)
			} else if value < 1 || value > MaxLoopIterations {
				addLimitDiagnostic(pointer+"/max_iterations", diagnostics)
			}
		}
	}
}

func validateLLMNode(pointer string, node map[string]any, diagnostics *[]Diagnostic) {
	validateNodeData(pointer, node, diagnostics)
	if instruction, exists := node["instruction"]; exists {
		text, ok := instruction.(string)
		if !ok {
			addTypeDiagnostic(pointer+"/instruction", diagnostics)
		} else if strings.TrimSpace(text) == "" || len([]byte(text)) > MaxInstructionBytes {
			addLimitDiagnostic(pointer+"/instruction", diagnostics)
		}
	}
	validateIdentifierField(node, "model_slot", pointer, diagnostics)
	if slots, exists := node["tool_slots"]; exists {
		validateStringArray(slots, pointer+"/tool_slots", 0, MaxToolSlots, identifierPattern, "", diagnostics)
	}
	if slots, exists := node["knowledge_slots"]; exists {
		validateStringArray(slots, pointer+"/knowledge_slots", 0, MaxKnowledgeSlots, identifierPattern, "", diagnostics)
	}
	if generationValue, exists := node["generation"]; exists {
		generation, ok := generationValue.(map[string]any)
		if !ok {
			addTypeDiagnostic(pointer+"/generation", diagnostics)
			return
		}
		validateAllowedFields(generation, pointer+"/generation", []string{"temperature", "max_output_tokens"}, diagnostics)
		if temperature, exists := generation["temperature"]; exists {
			value, ok := numberValue(temperature)
			if !ok {
				addTypeDiagnostic(pointer+"/generation/temperature", diagnostics)
			} else if value < 0 || value > 2 {
				addLimitDiagnostic(pointer+"/generation/temperature", diagnostics)
			}
		}
		if tokens, exists := generation["max_output_tokens"]; exists {
			value, ok := integerValue(tokens)
			if !ok {
				addTypeDiagnostic(pointer+"/generation/max_output_tokens", diagnostics)
			} else if value < 1 || value > 262144 {
				addLimitDiagnostic(pointer+"/generation/max_output_tokens", diagnostics)
			}
		}
	}
}
