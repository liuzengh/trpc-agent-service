package domain_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"

	agentspecv1 "github.com/liuzengh/trpc-agent-service/api/schemas/agentspec/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
)

func TestValidFixturesPassPublicSchemaAndDomainValidation(t *testing.T) {
	schema := compilePublicSchema(t)
	files, err := filepath.Glob(fixturePath(t, "valid", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no valid AgentSpec fixtures found")
	}
	for _, file := range files {
		file := file
		t.Run(filepath.Base(file), func(t *testing.T) {
			document := readFixture(t, file)
			instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(document))
			if err != nil {
				t.Fatalf("decode fixture: %v", err)
			}
			if err := schema.Validate(instance); err != nil {
				t.Fatalf("public JSON Schema rejected fixture: %v", err)
			}
			canonical, report := domain.ValidateForPublication(document, 7)
			if !report.Valid {
				t.Fatalf("domain validation report = %#v", report)
			}
			if canonical.SchemaVersion != "v1" || !strings.HasPrefix(canonical.Digest, "sha256:") {
				t.Fatalf("canonical result = %#v", canonical)
			}
			canonicalInstance, err := jsonschema.UnmarshalJSON(bytes.NewReader(canonical.Document))
			if err != nil {
				t.Fatalf("decode canonical document: %v", err)
			}
			if err := schema.Validate(canonicalInstance); err != nil {
				t.Fatalf("canonical document failed public JSON Schema: %v", err)
			}
		})
	}
}

func TestInvalidFixturesReturnStableDiagnostics(t *testing.T) {
	tests := map[string]struct {
		code    string
		pointer string
	}{
		"editor-state.json":          {"AGENT_SPEC_UNKNOWN_FIELD", "/editor_state"},
		"root-not-found.json":        {"AGENT_SPEC_ROOT_NOT_FOUND", "/root"},
		"multiple-parents.json":      {"AGENT_SPEC_NODE_MULTIPLE_PARENTS", "/nodes/shared"},
		"undeclared-model-slot.json": {"AGENT_SPEC_MODEL_SLOT_NOT_FOUND", "/nodes/assistant/model_slot"},
		"unknown-field.json":         {"AGENT_SPEC_UNKNOWN_FIELD", "/nodes/assistant/model_ref"},
	}
	for name, expected := range tests {
		t.Run(name, func(t *testing.T) {
			document := readFixture(t, fixturePath(t, "invalid", name))
			_, report := domain.ValidateForPublication(document, 2)
			if report.Valid {
				t.Fatal("invalid fixture was accepted")
			}
			diagnostic, ok := findDiagnostic(report, expected.code)
			if !ok || diagnostic.Pointer != expected.pointer {
				t.Fatalf("diagnostics = %#v, want %s at %s", report.Diagnostics, expected.code, expected.pointer)
			}
		})
	}
}

func TestDraftStorageAllowsIncompleteObjectButRejectsDuplicateAndSensitiveKeys(t *testing.T) {
	if report := domain.ValidateDraftForStorage(json.RawMessage(`{"nodes":{}}`), 1); !report.Valid {
		t.Fatalf("incomplete draft report = %#v", report)
	}
	for name, test := range map[string]struct {
		document string
		code     string
	}{
		"duplicate":  {`{"root":"a","root":"b"}`, "AGENT_SPEC_DUPLICATE_KEY"},
		"sensitive":  {`{"nested":{"api_key":"plaintext"}}`, "AGENT_SPEC_SENSITIVE_FIELD"},
		"non-object": {`[]`, "AGENT_SPEC_DOCUMENT_REQUIRED"},
	} {
		t.Run(name, func(t *testing.T) {
			report := domain.ValidateDraftForStorage(json.RawMessage(test.document), 1)
			if report.Valid || !hasDiagnostic(report, test.code) {
				t.Fatalf("report = %#v, want %s", report, test.code)
			}
		})
	}
}

func TestDraftStorageRejectsMalformedUnicode(t *testing.T) {
	tests := map[string]json.RawMessage{
		"invalid utf8":   {'{', '"', 'x', '"', ':', '"', 0xff, '"', '}'},
		"lone surrogate": json.RawMessage(`{"x":"\ud800"}`),
	}
	for name, document := range tests {
		t.Run(name, func(t *testing.T) {
			report := domain.ValidateDraftForStorage(document, 1)
			if report.Valid || !hasDiagnostic(report, "AGENT_SPEC_INVALID_JSON") {
				t.Fatalf("report = %#v", report)
			}
		})
	}
}

func TestCanonicalDigestNormalizesSetsButPreservesSequenceOrder(t *testing.T) {
	document := readFixture(t, fixturePath(t, "valid", "sequence-parallel.json"))
	first, firstReport := domain.ValidateForPublication(document, 1)
	if !firstReport.Valid {
		t.Fatalf("first report = %#v", firstReport)
	}
	reorderedSets := bytes.Replace(document,
		[]byte(`["tool_call", "chat"]`), []byte(`["chat", "tool_call"]`), 1)
	second, secondReport := domain.ValidateForPublication(reorderedSets, 1)
	if !secondReport.Valid {
		t.Fatalf("second report = %#v", secondReport)
	}
	if first.Digest != second.Digest || !bytes.Equal(first.Document, second.Document) {
		t.Fatalf("set ordering changed canonical output: %s != %s", first.Digest, second.Digest)
	}

	reorderedChildren := bytes.Replace(document,
		[]byte(`["reviews", "summary"]`), []byte(`["summary", "reviews"]`), 1)
	third, thirdReport := domain.ValidateForPublication(reorderedChildren, 1)
	if !thirdReport.Valid {
		t.Fatalf("third report = %#v", thirdReport)
	}
	if first.Digest == third.Digest {
		t.Fatal("sequence child order did not change digest")
	}
}

func TestPublicSchemaRejectsUnknownFieldFixture(t *testing.T) {
	schema := compilePublicSchema(t)
	document := readFixture(t, fixturePath(t, "invalid", "unknown-field.json"))
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(document))
	if err != nil {
		t.Fatal(err)
	}
	if err := schema.Validate(instance); err == nil {
		t.Fatal("public JSON Schema accepted an unknown field")
	}
}

func TestCanonicalGoldenAndMathematicalIntegers(t *testing.T) {
	document := readFixture(t, fixturePath(t, "valid", "single-llm.json"))
	canonical, report := domain.ValidateForPublication(document, 1)
	if !report.Valid {
		t.Fatalf("report = %#v", report)
	}
	const wantDigest = "sha256:d0848afe3fee57f16f913b957fcb76c124efc5c20280f1f7ff665b8dec8e53d3"
	const wantDocument = `{"nodes":{"assistant":{"instruction":"准确回答用户问题。","kind":"llm","knowledge_slots":[],"model_slot":"primary","name":"通用助手","tool_slots":[]}},"requirements":{"knowledge":{},"models":{"primary":{"capabilities":["chat"]}},"tools":{}},"root":"assistant","schema_version":"v1"}`
	if canonical.Digest != wantDigest || string(canonical.Document) != wantDocument {
		t.Fatalf("canonical = %s / %s", canonical.Digest, canonical.Document)
	}

	loop := readFixture(t, fixturePath(t, "valid", "loop.json"))
	loop = bytes.Replace(loop, []byte(`"max_iterations": 3`), []byte(`"max_iterations": 3.0`), 1)
	canonical, report = domain.ValidateForPublication(loop, 9)
	if !report.Valid || !bytes.Contains(canonical.Document, []byte(`"max_iterations":3`)) {
		t.Fatalf("mathematical integer report/document = %#v / %s", report, canonical.Document)
	}
}

func TestCanonicalizationUsesRFC8785NumberFormatting(t *testing.T) {
	document := readFixture(t, fixturePath(t, "valid", "single-llm.json"))
	document = bytes.Replace(document,
		[]byte(`"knowledge_slots": []`),
		[]byte(`"knowledge_slots": [], "generation":{"temperature":0.000001}`), 1)
	canonical, report := domain.ValidateForPublication(document, 1)
	if !report.Valid {
		t.Fatalf("report = %#v", report)
	}
	if !bytes.Contains(canonical.Document, []byte(`"temperature":0.000001`)) {
		t.Fatalf("canonical number = %s", canonical.Document)
	}

	document = bytes.Replace(document, []byte(`0.000001`), []byte(`0.0000001`), 1)
	canonical, report = domain.ValidateForPublication(document, 1)
	if !report.Valid || !bytes.Contains(canonical.Document, []byte(`"temperature":1e-7`)) {
		t.Fatalf("canonical exponent = %#v / %s", report, canonical.Document)
	}
}

func TestObjectKeyOrderDoesNotChangeDigest(t *testing.T) {
	original := readFixture(t, fixturePath(t, "valid", "single-llm.json"))
	reordered := json.RawMessage(`{"nodes":{"assistant":{"knowledge_slots":[],"tool_slots":[],"model_slot":"primary","instruction":"准确回答用户问题。","name":"通用助手","kind":"llm"}},"root":"assistant","schema_version":"v1","requirements":{"knowledge":{},"tools":{},"models":{"primary":{"capabilities":["chat"]}}}}`)
	first, firstReport := domain.ValidateForPublication(original, 1)
	second, secondReport := domain.ValidateForPublication(reordered, 1)
	if !firstReport.Valid || !secondReport.Valid || first.Digest != second.Digest ||
		!bytes.Equal(first.Document, second.Document) {
		t.Fatalf("key order changed canonical form: %s / %s", first.Digest, second.Digest)
	}
}

func compilePublicSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(agentspecv1.Schema))
	if err != nil {
		t.Fatalf("decode public schema: %v", err)
	}
	compiler := jsonschema.NewCompiler()
	const location = "https://jfsas.dev/schemas/agentspec/v1/agent-spec.schema.json"
	if err := compiler.AddResource(location, document); err != nil {
		t.Fatalf("add public schema: %v", err)
	}
	schema, err := compiler.Compile(location)
	if err != nil {
		t.Fatalf("compile public schema: %v", err)
	}
	return schema
}

func fixturePath(t *testing.T, parts ...string) string {
	t.Helper()
	all := append([]string{"..", "..", "..", "..", "..", "api", "schemas", "agentspec", "v1", "examples"}, parts...)
	return filepath.Join(all...)
}

func readFixture(t *testing.T, path string) json.RawMessage {
	t.Helper()
	document, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return document
}

func hasDiagnostic(report domain.ValidationReport, code string) bool {
	for _, diagnostic := range report.Diagnostics {
		if diagnostic.Code == code {
			return true
		}
	}
	return false
}

func findDiagnostic(report domain.ValidationReport, code string) (domain.Diagnostic, bool) {
	for _, diagnostic := range report.Diagnostics {
		if diagnostic.Code == code {
			return diagnostic, true
		}
	}
	return domain.Diagnostic{}, false
}
