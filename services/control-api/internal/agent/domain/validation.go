package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
)

var sensitiveFields = map[string]struct{}{
	"api_key": {}, "password": {}, "token": {}, "authorization": {},
	"credential": {}, "client_secret": {},
}

// ValidateDraftForStorage performs L0 validation. It deliberately allows an
// incomplete AgentSpec so editors can persist work in progress.
func ValidateDraftForStorage(document json.RawMessage, revision int64) ValidationReport {
	value, diagnostics := decodeDocument(document)
	schemaVersion := schemaVersionOf(value)
	return newReport(schemaVersion, revision, diagnostics)
}

// ValidateForPublication performs L0, schema-shaped L1, and domain-semantic L2
// validation. It returns a canonical immutable document only when valid.
func ValidateForPublication(
	document json.RawMessage,
	revision int64,
) (CanonicalSpec, ValidationReport) {
	value, diagnostics := decodeDocument(document)
	schemaVersion := schemaVersionOf(value)
	if value != nil {
		diagnostics = append(diagnostics, validateSchemaShape(value)...)
	}
	report := newReport(schemaVersion, revision, diagnostics)
	if !report.Valid {
		return CanonicalSpec{}, report
	}

	normalizeIntegerLexemes(value)
	encoded, err := json.Marshal(value)
	if err != nil {
		report = newReport(schemaVersion, revision, []Diagnostic{{
			Code: "AGENT_SPEC_INVALID_JSON", Severity: SeverityError,
			Pointer: "", Message: "AgentSpec could not be decoded",
		}})
		return CanonicalSpec{}, report
	}
	var spec Spec
	if err := json.Unmarshal(encoded, &spec); err != nil {
		report = newReport(schemaVersion, revision, []Diagnostic{{
			Code: "AGENT_SPEC_INVALID_TYPE", Severity: SeverityError,
			Pointer: "", Message: "AgentSpec fields could not be decoded",
		}})
		return CanonicalSpec{}, report
	}

	report = newReport(schemaVersion, revision, validateSemantics(spec))
	if !report.Valid {
		return CanonicalSpec{}, report
	}
	normalized := normalizeSpec(spec)
	canonical, err := canonicalJSON(normalized)
	if err != nil {
		report = newReport(schemaVersion, revision, []Diagnostic{{
			Code: "AGENT_SPEC_INVALID_JSON", Severity: SeverityError,
			Pointer: "", Message: "AgentSpec could not be canonicalized",
		}})
		return CanonicalSpec{}, report
	}
	digestBytes := sha256.Sum256(canonical)
	return CanonicalSpec{
		SchemaVersion: SchemaVersionV1,
		Document:      canonical,
		Digest:        "sha256:" + hex.EncodeToString(digestBytes[:]),
	}, report
}

func decodeDocument(document []byte) (map[string]any, []Diagnostic) {
	if len(document) == 0 {
		return nil, []Diagnostic{{
			Code: "AGENT_SPEC_DOCUMENT_REQUIRED", Severity: SeverityError,
			Pointer: "", Message: "AgentSpec document is required",
		}}
	}
	if len(document) > MaxDocumentBytes {
		return nil, []Diagnostic{{
			Code: "AGENT_SPEC_DOCUMENT_TOO_LARGE", Severity: SeverityError,
			Pointer: "", Message: "AgentSpec document exceeds the V1 size limit",
		}}
	}
	// JCS requires I-JSON-compatible Unicode. encoding/json replaces malformed
	// UTF-8 and unpaired surrogate escapes, which would silently change the
	// document before hashing, so reject those inputs before typed decoding.
	if !utf8.Valid(document) {
		return nil, invalidJSONDiagnostics()
	}
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	value, diagnostics, err := decodeValue(decoder, "")
	if err != nil {
		return nil, []Diagnostic{{
			Code: "AGENT_SPEC_INVALID_JSON", Severity: SeverityError,
			Pointer: "", Message: "AgentSpec must contain one valid JSON value",
		}}
	}
	if token, err := decoder.Token(); !errors.Is(err, io.EOF) || token != nil {
		return nil, []Diagnostic{{
			Code: "AGENT_SPEC_INVALID_JSON", Severity: SeverityError,
			Pointer: "", Message: "AgentSpec must contain exactly one JSON value",
		}}
	}
	if _, err := jcs.Transform(document); err != nil &&
		!containsDiagnosticCode(diagnostics, "AGENT_SPEC_DUPLICATE_KEY") {
		diagnostics = append(diagnostics, invalidJSONDiagnostics()...)
	}
	object, ok := value.(map[string]any)
	if !ok {
		diagnostics = append(diagnostics, Diagnostic{
			Code: "AGENT_SPEC_DOCUMENT_REQUIRED", Severity: SeverityError,
			Pointer: "", Message: "AgentSpec top-level value must be an object",
		})
		return nil, diagnostics
	}
	return object, diagnostics
}

func containsDiagnosticCode(diagnostics []Diagnostic, code string) bool {
	for _, diagnostic := range diagnostics {
		if diagnostic.Code == code {
			return true
		}
	}
	return false
}

func invalidJSONDiagnostics() []Diagnostic {
	return []Diagnostic{{
		Code: "AGENT_SPEC_INVALID_JSON", Severity: SeverityError,
		Pointer: "", Message: "AgentSpec must contain one valid JSON value",
	}}
}

func decodeValue(decoder *json.Decoder, pointer string) (any, []Diagnostic, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, nil, err
	}
	delim, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return token, nil, nil
	}
	switch delim {
	case '{':
		object := make(map[string]any)
		seen := make(map[string]struct{})
		var diagnostics []Diagnostic
		for decoder.More() {
			nameToken, err := decoder.Token()
			if err != nil {
				return nil, diagnostics, err
			}
			name, ok := nameToken.(string)
			if !ok {
				return nil, diagnostics, errors.New("object name is not a string")
			}
			childPointer := pointer + "/" + escapeJSONPointer(name)
			if _, duplicate := seen[name]; duplicate {
				diagnostics = append(diagnostics, Diagnostic{
					Code: "AGENT_SPEC_DUPLICATE_KEY", Severity: SeverityError,
					Pointer: childPointer, Message: "JSON object contains a duplicate key",
				})
			}
			seen[name] = struct{}{}
			if _, sensitive := sensitiveFields[strings.ToLower(name)]; sensitive {
				diagnostics = append(diagnostics, Diagnostic{
					Code: "AGENT_SPEC_SENSITIVE_FIELD", Severity: SeverityError,
					Pointer: childPointer, Message: "credential fields are not allowed in AgentSpec",
				})
			}
			value, nested, err := decodeValue(decoder, childPointer)
			diagnostics = append(diagnostics, nested...)
			if err != nil {
				return nil, diagnostics, err
			}
			object[name] = value
		}
		if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
			return nil, diagnostics, errors.New("unterminated object")
		}
		return object, diagnostics, nil
	case '[':
		array := make([]any, 0)
		var diagnostics []Diagnostic
		for decoder.More() {
			childPointer := pointer + "/" + strconv.Itoa(len(array))
			value, nested, err := decodeValue(decoder, childPointer)
			diagnostics = append(diagnostics, nested...)
			if err != nil {
				return nil, diagnostics, err
			}
			array = append(array, value)
		}
		if end, err := decoder.Token(); err != nil || end != json.Delim(']') {
			return nil, diagnostics, errors.New("unterminated array")
		}
		return array, diagnostics, nil
	default:
		return nil, nil, errors.New("unexpected delimiter")
	}
}
