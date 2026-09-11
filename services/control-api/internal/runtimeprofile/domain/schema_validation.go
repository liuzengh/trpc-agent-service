package domain

import (
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

var (
	resourceKeyPattern  = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)
	credentialIDPattern = regexp.MustCompile(`^crd_[0-9a-f]{32}$`)
	toolNamePattern     = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
)

func validateSchemaShape(root map[string]any) []Diagnostic {
	var diagnostics []Diagnostic
	validateAllowedFields(root, "", []string{
		"schema_version", "credential_protocol_version", "models", "tools", "knowledge", "storage", "executors",
	}, &diagnostics)
	requireFields(root, "", []string{
		"schema_version", "credential_protocol_version", "models", "tools", "knowledge", "storage",
	}, &diagnostics)

	if version, exists := root["schema_version"]; exists {
		value, ok := version.(string)
		if !ok {
			addTypeDiagnostic("/schema_version", &diagnostics)
		} else if value != SchemaVersionV1 {
			diagnostics = append(diagnostics, errorDiagnostic(
				"RUNTIME_PROFILE_SPEC_UNSUPPORTED_VERSION", "/schema_version",
				"schema_version is not supported",
			))
		}
	}
	if version, exists := root["credential_protocol_version"]; exists {
		value, ok := version.(string)
		if !ok {
			addTypeDiagnostic("/credential_protocol_version", &diagnostics)
		} else if value != CredentialProtocolVersionV1 {
			diagnostics = append(diagnostics, errorDiagnostic("RUNTIME_PROFILE_SPEC_UNSUPPORTED_VERSION", "/credential_protocol_version", "credential_protocol_version is not supported"))
		}
	}
	if value, exists := root["executors"]; exists {
		validateResourceMap(value, "executors", "executor", 16, validateExecutorResource, &diagnostics)
	}
	if value, exists := root["models"]; exists {
		validateResourceMap(value, "models", "model", MaxModelResources, validateModelResource, &diagnostics)
	}
	if value, exists := root["tools"]; exists {
		validateResourceMap(value, "tools", "tool", MaxToolResources, validateToolResource, &diagnostics)
	}
	if value, exists := root["knowledge"]; exists {
		validateResourceMap(value, "knowledge", "knowledge", MaxKnowledgeResources, validateKnowledgeResource, &diagnostics)
	}
	if value, exists := root["storage"]; exists {
		validateResourceMap(value, "storage", "storage", MaxStorageResources, validateStorageResource, &diagnostics)
	}
	return diagnostics
}

type resourceValidator func(key, pointer string, object map[string]any, diagnostics *[]Diagnostic)

func validateResourceMap(
	value any,
	collection, resourceKind string,
	maximum int,
	validator resourceValidator,
	diagnostics *[]Diagnostic,
) {
	pointer := "/" + collection
	items, ok := value.(map[string]any)
	if !ok {
		addTypeDiagnostic(pointer, diagnostics)
		return
	}
	if len(items) > maximum {
		addLimitDiagnostic(pointer, diagnostics)
	}
	for _, key := range sortedMapKeys(items) {
		itemPointer := pointer + "/" + escapeJSONPointer(key)
		if !resourceKeyPattern.MatchString(key) {
			*diagnostics = append(*diagnostics, resourceDiagnostic(
				"RUNTIME_PROFILE_SPEC_INVALID_IDENTIFIER", SeverityError,
				itemPointer, resourceKind, key,
				"resource key does not match the RuntimeProfileSpec V1 format",
			))
		}
		object, ok := items[key].(map[string]any)
		if !ok {
			addTypeDiagnostic(itemPointer, diagnostics)
			continue
		}
		validator(key, itemPointer, object, diagnostics)
	}
}

func validateModelResource(_ string, pointer string, object map[string]any, diagnostics *[]Diagnostic) {
	kind, ok := validateResourceKind(object, pointer, string(ModelKindOpenAICompatible), diagnostics)
	if !ok || kind != string(ModelKindOpenAICompatible) {
		return
	}
	validateAllowedFields(object, pointer, []string{
		"kind", "model", "base_url", "api_key_credential_id", "capabilities",
	}, diagnostics)
	requireFields(object, pointer, []string{
		"kind", "model", "base_url", "api_key_credential_id", "capabilities",
	}, diagnostics)
	validateBoundedString(object, "model", pointer, 1, 256, diagnostics)
	validateHTTPURLField(object, "base_url", pointer, diagnostics)
	validatePatternString(object, "api_key_credential_id", pointer, "credential", diagnostics)
	if value, exists := object["capabilities"]; exists {
		validateCapabilities(value, pointer+"/capabilities", diagnostics)
	}
}

func validateToolResource(_ string, pointer string, object map[string]any, diagnostics *[]Diagnostic) {
	kind, ok := validateResourceKind(object, pointer, string(ToolKindMCPStreamableHTTP), diagnostics)
	if !ok || kind != string(ToolKindMCPStreamableHTTP) {
		return
	}
	validateAllowedFields(object, pointer, []string{
		"kind", "server_url", "toolset_name", "tool_name", "auth", "capability",
	}, diagnostics)
	requireFields(object, pointer, []string{
		"kind", "server_url", "toolset_name", "tool_name", "auth", "capability",
	}, diagnostics)
	validateHTTPURLField(object, "server_url", pointer, diagnostics)
	validatePatternString(object, "toolset_name", pointer, "tool", diagnostics)
	validatePatternString(object, "tool_name", pointer, "tool", diagnostics)
	if value, exists := object["auth"]; exists {
		validateToolAuth(value, pointer+"/auth", diagnostics)
	}
	validateBoundedString(object, "capability", pointer, 1, 128, diagnostics)
}

func validateToolAuth(value any, pointer string, diagnostics *[]Diagnostic) {
	object, ok := value.(map[string]any)
	if !ok {
		addTypeDiagnostic(pointer, diagnostics)
		return
	}
	kindValue, exists := object["kind"]
	if !exists {
		requireFields(object, pointer, []string{"kind"}, diagnostics)
		return
	}
	kind, ok := kindValue.(string)
	if !ok {
		addTypeDiagnostic(pointer+"/kind", diagnostics)
		return
	}
	switch AuthKind(kind) {
	case AuthKindNone:
		validateAllowedFields(object, pointer, []string{"kind"}, diagnostics)
		requireFields(object, pointer, []string{"kind"}, diagnostics)
	case AuthKindBearer:
		validateAllowedFields(object, pointer, []string{"kind", "credential_id"}, diagnostics)
		requireFields(object, pointer, []string{"kind", "credential_id"}, diagnostics)
		validatePatternString(object, "credential_id", pointer, "credential", diagnostics)
	default:
		*diagnostics = append(*diagnostics, contextualDiagnostic(
			"RUNTIME_PROFILE_SPEC_UNSUPPORTED_KIND", SeverityError, pointer+"/kind",
			"tool auth kind is not supported in RuntimeProfileSpec V1",
		))
	}
}

func validateKnowledgeResource(_ string, pointer string, object map[string]any, diagnostics *[]Diagnostic) {
	if object["kind"] == string(KnowledgeKindManaged) {
		selection := make(map[string]any, len(object))
		for k, v := range object {
			selection[k] = v
		}
		_, id := object["qdrant_api_key_credential_id"]
		_, digest := object["credential_audience_digest"]
		if id || digest {
			requireFields(object, pointer, []string{"qdrant_api_key_credential_id", "credential_audience_digest"}, diagnostics)
			validatePatternString(object, "qdrant_api_key_credential_id", pointer, "credential", diagnostics)
			v, ok := object["credential_audience_digest"].(string)
			if !ok || !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(v) {
				*diagnostics = append(*diagnostics, errorDiagnostic("RUNTIME_PROFILE_SPEC_INVALID_IDENTIFIER", pointer+"/credential_audience_digest", "invalid credential audience"))
			}
		}
		delete(selection, "qdrant_api_key_credential_id")
		delete(selection, "credential_audience_digest")
		validateManagedSelection(pointer, selection, []string{"kind", "backend_id", "backend_revision", "embedding"}, diagnostics)
		if v, ok := object["embedding"]; ok {
			validateEmbedding(v, pointer+"/embedding", diagnostics)
		}
		return
	}

	kind, ok := validateResourceKind(object, pointer, string(KnowledgeKindQdrantOpenAI), diagnostics)
	if !ok || kind != string(KnowledgeKindQdrantOpenAI) {
		return
	}
	validateAllowedFields(object, pointer, []string{
		"kind", "host", "port", "tls", "collection", "qdrant_api_key_credential_id", "embedding",
	}, diagnostics)
	requireFields(object, pointer, []string{
		"kind", "host", "port", "tls", "collection", "embedding",
	}, diagnostics)
	validateBoundedString(object, "host", pointer, 1, 253, diagnostics)
	validateIntegerField(object, "port", pointer, 1, 65535, diagnostics)
	validateBooleanField(object, "tls", pointer, diagnostics)
	validateBoundedString(object, "collection", pointer, 1, 128, diagnostics)
	if _, exists := object["qdrant_api_key_credential_id"]; exists {
		validatePatternString(object, "qdrant_api_key_credential_id", pointer, "credential", diagnostics)
	}
	if value, exists := object["embedding"]; exists {
		validateEmbedding(value, pointer+"/embedding", diagnostics)
	}
}

func validateEmbedding(value any, pointer string, diagnostics *[]Diagnostic) {
	object, ok := value.(map[string]any)
	if !ok {
		addTypeDiagnostic(pointer, diagnostics)
		return
	}
	validateAllowedFields(object, pointer, []string{
		"model", "base_url", "api_key_credential_id", "dimensions",
	}, diagnostics)
	requireFields(object, pointer, []string{
		"model", "base_url", "api_key_credential_id", "dimensions",
	}, diagnostics)
	validateBoundedString(object, "model", pointer, 1, 256, diagnostics)
	validateHTTPURLField(object, "base_url", pointer, diagnostics)
	validatePatternString(object, "api_key_credential_id", pointer, "credential", diagnostics)
	validateIntegerField(object, "dimensions", pointer, 1, 65536, diagnostics)
}

func validateStorageResource(_ string, pointer string, object map[string]any, diagnostics *[]Diagnostic) {
	if k, ok := object["kind"].(string); ok && StorageKind(k).Managed() {
		selection := object
		if StorageKind(k) == StorageKindManagedArtifact {
			selection = make(map[string]any, len(object))
			for key, v := range object {
				selection[key] = v
			}
			_, a := object["access_key_id_credential_id"]
			_, z := object["secret_access_key_credential_id"]
			_, d := object["credential_audience_digest"]
			if a || z || d {
				if a || z {
					requireFields(object, pointer, []string{"credential_audience_digest"}, diagnostics)
				} else {
					*diagnostics = append(*diagnostics, errorDiagnostic("RUNTIME_PROFILE_SPEC_INVALID_IDENTIFIER", pointer, "artifact credential association is empty"))
				}
				for _, key := range []string{"access_key_id_credential_id", "secret_access_key_credential_id"} {
					if _, ok := object[key]; ok {
						validatePatternString(object, key, pointer, "credential", diagnostics)
					}
				}
				value, ok := object["credential_audience_digest"].(string)
				if !ok || !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(value) {
					*diagnostics = append(*diagnostics, errorDiagnostic("RUNTIME_PROFILE_SPEC_INVALID_IDENTIFIER", pointer+"/credential_audience_digest", "credential audience digest is invalid"))
				}
			}
			delete(selection, "access_key_id_credential_id")
			delete(selection, "secret_access_key_credential_id")
			delete(selection, "credential_audience_digest")
		}

		if StorageKind(k) == StorageKindManagedMemory || StorageKind(k) == StorageKindManagedSession {
			selection = make(map[string]any, len(object))
			for key, v := range object {
				selection[key] = v
			}
			_, hasID := object["dsn_credential_id"]
			_, hasAudience := object["credential_audience_digest"]
			if hasID || hasAudience {
				requireFields(object, pointer, []string{"dsn_credential_id", "credential_audience_digest"}, diagnostics)
				validatePatternString(object, "dsn_credential_id", pointer, "credential", diagnostics)
				value, ok := object["credential_audience_digest"].(string)
				if !ok || !regexp.MustCompile(`^sha256:[0-9a-f]{64}$`).MatchString(value) {
					*diagnostics = append(*diagnostics, errorDiagnostic("RUNTIME_PROFILE_SPEC_INVALID_IDENTIFIER", pointer+"/credential_audience_digest", "credential audience digest is invalid"))
				}
			}
			delete(selection, "dsn_credential_id")
			delete(selection, "credential_audience_digest")
		}
		validateManagedSelection(pointer, selection, []string{"kind", "backend_id", "backend_revision"}, diagnostics)
		return
	}

	kind, ok := validateResourceKind(object, pointer, string(StorageKindPostgresState), diagnostics)
	if !ok || kind != string(StorageKindPostgresState) {
		return
	}
	validateAllowedFields(object, pointer, []string{"kind", "dsn_credential_id", "destination"}, diagnostics)
	requireFields(object, pointer, []string{"kind", "dsn_credential_id", "destination"}, diagnostics)
	validatePatternString(object, "dsn_credential_id", pointer, "credential", diagnostics)
	if value, exists := object["destination"]; exists {
		validateStorageDestination(value, pointer+"/destination", diagnostics)
	}
}

func validateStorageDestination(value any, pointer string, diagnostics *[]Diagnostic) {
	object, ok := value.(map[string]any)
	if !ok {
		addTypeDiagnostic(pointer, diagnostics)
		return
	}
	fields := []string{"host", "port", "database", "username", "sslmode"}
	validateAllowedFields(object, pointer, fields, diagnostics)
	requireFields(object, pointer, fields, diagnostics)
	validateBoundedString(object, "host", pointer, 1, 253, diagnostics)
	validateBoundedString(object, "database", pointer, 1, 128, diagnostics)
	validateBoundedString(object, "username", pointer, 1, 128, diagnostics)
	validateIntegerField(object, "port", pointer, 1, 65535, diagnostics)
	if value, exists := object["sslmode"]; exists {
		mode, ok := value.(string)
		if !ok {
			addTypeDiagnostic(pointer+"/sslmode", diagnostics)
		} else if mode != "disable" && mode != "require" && mode != "verify-full" {
			*diagnostics = append(*diagnostics, contextualDiagnostic("RUNTIME_PROFILE_SPEC_INVALID_VALUE", SeverityError, pointer+"/sslmode", "sslmode must be disable, require, or verify-full"))
		}
	}
}

func validateResourceKind(
	object map[string]any,
	pointer, supported string,
	diagnostics *[]Diagnostic,
) (string, bool) {
	kindValue, exists := object["kind"]
	if !exists {
		requireFields(object, pointer, []string{"kind"}, diagnostics)
		return "", false
	}
	kind, ok := kindValue.(string)
	if !ok {
		addTypeDiagnostic(pointer+"/kind", diagnostics)
		return "", false
	}
	if kind != supported {
		*diagnostics = append(*diagnostics, contextualDiagnostic(
			"RUNTIME_PROFILE_SPEC_UNSUPPORTED_KIND", SeverityError, pointer+"/kind",
			"resource kind is not supported in RuntimeProfileSpec V1",
		))
		return kind, false
	}
	return kind, true
}

func validateCapabilities(value any, pointer string, diagnostics *[]Diagnostic) {
	array, ok := value.([]any)
	if !ok {
		addTypeDiagnostic(pointer, diagnostics)
		return
	}
	if len(array) < 1 || len(array) > MaxCapabilities {
		addLimitDiagnostic(pointer, diagnostics)
	}
	seen := make(map[string]struct{}, len(array))
	for index, item := range array {
		itemPointer := pointer + "/" + strconv.Itoa(index)
		text, ok := item.(string)
		if !ok {
			addTypeDiagnostic(itemPointer, diagnostics)
			continue
		}
		if _, duplicate := seen[text]; duplicate {
			*diagnostics = append(*diagnostics, contextualDiagnostic(
				"RUNTIME_PROFILE_SPEC_DUPLICATE_CAPABILITY", SeverityError,
				itemPointer, "capability must not be repeated",
			))
		}
		seen[text] = struct{}{}
	}
}

func validateHTTPURLField(object map[string]any, field, pointer string, diagnostics *[]Diagnostic) {
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
	if length := utf8.RuneCountInString(text); length < 1 || length > 2048 {
		addLimitDiagnostic(fieldPointer, diagnostics)
		return
	}
	parsed, err := url.Parse(text)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" ||
		parsed.Fragment != "" || parsed.Opaque != "" || strings.ContainsAny(text, "?#") {
		*diagnostics = append(*diagnostics, contextualDiagnostic(
			"RUNTIME_PROFILE_SPEC_INVALID_URL", SeverityError, fieldPointer,
			"URL must be absolute http(s) without userinfo, query, or fragment",
		))
	}
}
