package domain

import (
	"encoding/json"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

type resourceContext struct {
	kind string
	key  string
}

func contextualDiagnostic(code string, severity Severity, pointer, message string) Diagnostic {
	context, ok := contextFromPointer(pointer)
	if !ok {
		return Diagnostic{Code: code, Severity: severity, Pointer: pointer, Message: message}
	}
	return resourceDiagnostic(code, severity, pointer, context.kind, context.key, message)
}

func contextFromPointer(pointer string) (resourceContext, bool) {
	parts := strings.Split(strings.TrimPrefix(pointer, "/"), "/")
	if len(parts) < 2 {
		return resourceContext{}, false
	}
	kinds := map[string]string{
		"models": "model", "tools": "tool",
		"knowledge": "knowledge", "storage": "storage",
	}
	kind, ok := kinds[parts[0]]
	if !ok || parts[1] == "" {
		return resourceContext{}, false
	}
	return resourceContext{kind: kind, key: unescapeJSONPointer(parts[1])}, true
}

func validateAllowedFields(object map[string]any, pointer string, allowed []string, diagnostics *[]Diagnostic) {
	allowedSet := make(map[string]bool, len(allowed))
	for _, field := range allowed {
		allowedSet[field] = true
	}
	for _, field := range sortedMapKeys(object) {
		if !allowedSet[field] {
			*diagnostics = append(*diagnostics, contextualDiagnostic(
				"RUNTIME_PROFILE_SPEC_UNKNOWN_FIELD", SeverityError,
				pointer+"/"+escapeJSONPointer(field),
				"field is not defined by RuntimeProfileSpec V1",
			))
		}
	}
}

func requireFields(object map[string]any, pointer string, required []string, diagnostics *[]Diagnostic) {
	for _, field := range required {
		if _, exists := object[field]; !exists {
			*diagnostics = append(*diagnostics, contextualDiagnostic(
				"RUNTIME_PROFILE_SPEC_REQUIRED_FIELD", SeverityError,
				pointer+"/"+escapeJSONPointer(field), "required field is missing",
			))
		}
	}
}

func validateBoundedString(
	object map[string]any,
	field, pointer string,
	minimum, maximum int,
	diagnostics *[]Diagnostic,
) {
	value, exists := object[field]
	if !exists {
		return
	}
	text, ok := value.(string)
	fieldPointer := pointer + "/" + escapeJSONPointer(field)
	if !ok {
		addTypeDiagnostic(fieldPointer, diagnostics)
		return
	}
	length := utf8.RuneCountInString(text)
	if length < minimum || length > maximum {
		addLimitDiagnostic(fieldPointer, diagnostics)
	}
}

func validatePatternString(
	object map[string]any,
	field, pointer string,
	patternName string,
	diagnostics *[]Diagnostic,
) {
	value, exists := object[field]
	if !exists {
		return
	}
	text, ok := value.(string)
	fieldPointer := pointer + "/" + escapeJSONPointer(field)
	if !ok {
		addTypeDiagnostic(fieldPointer, diagnostics)
		return
	}
	valid := false
	switch patternName {
	case "credential":
		valid = credentialIDPattern.MatchString(text)
	case "tool":
		valid = toolNamePattern.MatchString(text)
	}
	if !valid {
		code := "RUNTIME_PROFILE_SPEC_INVALID_IDENTIFIER"
		message := "identifier does not match the RuntimeProfileSpec V1 format"
		if patternName == "credential" {
			code = "RUNTIME_PROFILE_SPEC_CREDENTIAL_ID_INVALID"
			message = "CredentialID does not match the RuntimeProfileSpec V1 format"
		}
		*diagnostics = append(*diagnostics, contextualDiagnostic(
			code, SeverityError, fieldPointer, message,
		))
	}
}

func validateIntegerField(
	object map[string]any,
	field, pointer string,
	minimum, maximum int64,
	diagnostics *[]Diagnostic,
) {
	value, exists := object[field]
	if !exists {
		return
	}
	fieldPointer := pointer + "/" + escapeJSONPointer(field)
	integer, ok := integerValue(value)
	if !ok {
		addTypeDiagnostic(fieldPointer, diagnostics)
		return
	}
	if integer < minimum || integer > maximum {
		addLimitDiagnostic(fieldPointer, diagnostics)
	}
}

func validateBooleanField(object map[string]any, field, pointer string, diagnostics *[]Diagnostic) {
	value, exists := object[field]
	if !exists {
		return
	}
	if _, ok := value.(bool); !ok {
		addTypeDiagnostic(pointer+"/"+escapeJSONPointer(field), diagnostics)
	}
}

func addTypeDiagnostic(pointer string, diagnostics *[]Diagnostic) {
	*diagnostics = append(*diagnostics, contextualDiagnostic(
		"RUNTIME_PROFILE_SPEC_INVALID_TYPE", SeverityError, pointer,
		"field has an invalid JSON type",
	))
}

func addLimitDiagnostic(pointer string, diagnostics *[]Diagnostic) {
	*diagnostics = append(*diagnostics, contextualDiagnostic(
		"RUNTIME_PROFILE_SPEC_LIMIT_EXCEEDED", SeverityError, pointer,
		"field exceeds a RuntimeProfileSpec V1 limit",
	))
}

func integerValue(value any) (int64, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	rational, ok := new(big.Rat).SetString(string(number))
	if !ok || !rational.IsInt() || !rational.Num().IsInt64() {
		return 0, false
	}
	return rational.Num().Int64(), true
}

func normalizeIntegerLexemes(root map[string]any) {
	storage, _ := root["storage"].(map[string]any)
	for _, value := range storage {
		resource, _ := value.(map[string]any)
		destination, _ := resource["destination"].(map[string]any)
		if integer, ok := integerValue(destination["port"]); ok {
			destination["port"] = json.Number(strconv.FormatInt(integer, 10))
		}
	}
	knowledge, _ := root["knowledge"].(map[string]any)
	for _, value := range knowledge {
		resource, _ := value.(map[string]any)
		if integer, ok := integerValue(resource["port"]); ok {
			resource["port"] = json.Number(strconv.FormatInt(integer, 10))
		}
		embedding, _ := resource["embedding"].(map[string]any)
		if integer, ok := integerValue(embedding["dimensions"]); ok {
			embedding["dimensions"] = json.Number(strconv.FormatInt(integer, 10))
		}
	}
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

func escapeJSONPointer(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}

func unescapeJSONPointer(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~1", "/"), "~0", "~")
}
