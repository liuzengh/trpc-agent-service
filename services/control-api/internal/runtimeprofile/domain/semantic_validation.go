package domain

import (
	"regexp"
	"strconv"
)

// Matches the existing AgentSpec tool capability grammar; exact matching remains in compilation.
var toolCapabilityPattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)

func validateSemantics(spec Spec) []Diagnostic {
	var diagnostics []Diagnostic
	for _, key := range sortedMapKeys(spec.Models) {
		validateModelSemantics(key, spec.Models[key], &diagnostics)
	}
	for _, key := range sortedMapKeys(spec.Tools) {
		validateToolSemantics(key, spec.Tools[key], &diagnostics)
	}
	for _, key := range sortedMapKeys(spec.Storage) {
		r := spec.Storage[key]
		if r.Kind.Managed() && key != r.Kind.Role() {
			diagnostics = append(diagnostics, errorDiagnostic("RUNTIME_PROFILE_SPEC_INVALID_STORAGE_ROLE", "/storage/"+escapeJSONPointer(key), "managed storage resource must use its explicit runtime role name"))
		}
	}
	return diagnostics
}

func validateModelSemantics(key string, resource ModelResource, diagnostics *[]Diagnostic) {
	pointer := "/models/" + escapeJSONPointer(key) + "/capabilities"
	hasChat := false
	for index, capability := range resource.Capabilities {
		switch capability {
		case CapabilityChat:
			hasChat = true
		case CapabilityToolCall:
		default:
			*diagnostics = append(*diagnostics, resourceDiagnostic(
				"RUNTIME_PROFILE_SPEC_CAPABILITY_KIND_MISMATCH", SeverityError,
				pointer+"/"+strconv.Itoa(index), "model", key,
				"capability is not supported by openai_compatible in RuntimeProfileSpec V1",
			))
		}
	}
	if !hasChat {
		*diagnostics = append(*diagnostics, resourceDiagnostic(
			"RUNTIME_PROFILE_SPEC_CAPABILITY_KIND_MISMATCH", SeverityError,
			pointer, "model", key,
			"openai_compatible capabilities must include chat",
		))
	}
}

func validateToolSemantics(key string, resource ToolResource, diagnostics *[]Diagnostic) {
	if !toolCapabilityPattern.MatchString(resource.Capability) {
		*diagnostics = append(*diagnostics, resourceDiagnostic(
			"RUNTIME_PROFILE_SPEC_CAPABILITY_KIND_MISMATCH", SeverityError,
			"/tools/"+escapeJSONPointer(key)+"/capability", "tool", key,
			"mcp_streamable_http capability must match the AgentSpec capability grammar",
		))
	}
}
