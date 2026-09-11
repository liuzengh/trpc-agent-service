package domain

import (
	"encoding/json"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

func validateAllowedFields(object map[string]any, pointer string, allowed []string, diagnostics *[]Diagnostic) {
	allowedSet := make(map[string]bool, len(allowed))
	for _, field := range allowed {
		allowedSet[field] = true
	}
	for _, field := range sortedMapKeys(object) {
		if !allowedSet[field] {
			*diagnostics = append(*diagnostics, Diagnostic{
				Code: "AGENT_SPEC_UNKNOWN_FIELD", Severity: SeverityError,
				Pointer: pointer + "/" + escapeJSONPointer(field),
				Message: "field is not defined by AgentSpec V1",
			})
		}
	}
}

func requireFields(object map[string]any, pointer string, required []string, diagnostics *[]Diagnostic) {
	for _, field := range required {
		if _, exists := object[field]; !exists {
			*diagnostics = append(*diagnostics, Diagnostic{
				Code: "AGENT_SPEC_REQUIRED_FIELD", Severity: SeverityError,
				Pointer: pointer + "/" + escapeJSONPointer(field),
				Message: "required field is missing",
			})
		}
	}
}

func validateIdentifierField(object map[string]any, field, pointer string, diagnostics *[]Diagnostic) {
	value, exists := object[field]
	if !exists {
		return
	}
	text, ok := value.(string)
	if !ok {
		addTypeDiagnostic(pointer+"/"+field, diagnostics)
		return
	}
	if !identifierPattern.MatchString(text) {
		addIdentifierDiagnostic(pointer+"/"+field, diagnostics)
	}
}

func validateStringArray(
	value any,
	pointer string,
	minimum, maximum int,
	pattern *regexp.Regexp,
	duplicateCode string,
	diagnostics *[]Diagnostic,
) {
	array, ok := value.([]any)
	if !ok {
		addTypeDiagnostic(pointer, diagnostics)
		return
	}
	if len(array) < minimum || len(array) > maximum {
		addLimitDiagnostic(pointer, diagnostics)
	}
	seen := make(map[string]bool)
	for index, item := range array {
		itemPointer := pointer + "/" + strconv.Itoa(index)
		text, ok := item.(string)
		if !ok {
			addTypeDiagnostic(itemPointer, diagnostics)
			continue
		}
		if !pattern.MatchString(text) {
			addIdentifierDiagnostic(itemPointer, diagnostics)
		}
		if seen[text] {
			code := duplicateCode
			if code == "" {
				code = "AGENT_SPEC_LIMIT_EXCEEDED"
			}
			*diagnostics = append(*diagnostics, Diagnostic{
				Code: code, Severity: SeverityError, Pointer: itemPointer,
				Message: "array elements must be unique",
			})
		}
		seen[text] = true
	}
}

func addTypeDiagnostic(pointer string, diagnostics *[]Diagnostic) {
	*diagnostics = append(*diagnostics, Diagnostic{
		Code: "AGENT_SPEC_INVALID_TYPE", Severity: SeverityError,
		Pointer: pointer, Message: "field has an invalid JSON type",
	})
}

func addIdentifierDiagnostic(pointer string, diagnostics *[]Diagnostic) {
	*diagnostics = append(*diagnostics, Diagnostic{
		Code: "AGENT_SPEC_INVALID_IDENTIFIER", Severity: SeverityError,
		Pointer: pointer, Message: "identifier does not match the V1 format",
	})
}

func addLimitDiagnostic(pointer string, diagnostics *[]Diagnostic) {
	*diagnostics = append(*diagnostics, Diagnostic{
		Code: "AGENT_SPEC_LIMIT_EXCEEDED", Severity: SeverityError,
		Pointer: pointer, Message: "field exceeds an AgentSpec V1 limit",
	})
}

func integerValue(value any) (int64, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	parsed, err := strconv.ParseFloat(string(number), 64)
	if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) ||
		math.Trunc(parsed) != parsed || parsed < math.MinInt64 || parsed > math.MaxInt64 {
		return 0, false
	}
	return int64(parsed), true
}

// normalizeIntegerLexemes bridges JSON Schema's mathematical integer type and
// encoding/json's int64 decoder, which otherwise rejects valid values such as
// 3.0. Only fields declared as integer in AgentSpec V1 are rewritten.
func normalizeIntegerLexemes(root map[string]any) {
	normalizeDataIntegers(root)
	nodes, _ := root["nodes"].(map[string]any)
	for _, value := range nodes {
		node, _ := value.(map[string]any)
		if integer, ok := integerValue(node["max_iterations"]); ok {
			node["max_iterations"] = json.Number(strconv.FormatInt(integer, 10))
		}
		generation, _ := node["generation"].(map[string]any)
		if integer, ok := integerValue(generation["max_output_tokens"]); ok {
			generation["max_output_tokens"] = json.Number(strconv.FormatInt(integer, 10))
		}
	}
}

func numberValue(value any) (float64, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	parsed, err := strconv.ParseFloat(string(number), 64)
	return parsed, err == nil && !math.IsInf(parsed, 0) && !math.IsNaN(parsed)
}

func schemaVersionOf(value map[string]any) string {
	if value == nil {
		return ""
	}
	version, _ := value["schema_version"].(string)
	return version
}

func sortedMapKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedNodeIDs(values map[string]Node) []string {
	return sortedMapKeys(values)
}

func sortedModelSlots(values map[string]ModelRequirement) []string {
	return sortedMapKeys(values)
}

func sortedCapabilitySlots(values map[string]CapabilityRequirement) []string {
	return sortedMapKeys(values)
}

func escapeJSONPointer(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}
