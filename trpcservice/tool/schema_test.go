package tool

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
)

func TestCompileAndValidateDocumentLocalSchema(t *testing.T) {
	raw := json.RawMessage(`{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"type": "object",
		"properties": {
			"text": {"type": "string", "maxLength": 10},
			"count": {"type": "integer", "minimum": 1}
		},
		"required": ["text"],
		"additionalProperties": false,
		"$defs": {"ignored": {"type": "null"}}
	}`)
	sch, err := CompileInputSchema(raw)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if sch.Declaration() == nil || sch.Declaration().Type != "object" {
		t.Fatalf("declaration not derived from the schema: %+v", sch.Declaration())
	}

	cases := []struct {
		name string
		args string
		ok   bool
	}{
		{"valid", `{"text":"hi"}`, true},
		{"valid with count", `{"text":"hi","count":3}`, true},
		{"missing required", `{"count":3}`, false},
		{"wrong type", `{"text":7}`, false},
		{"extra property", `{"text":"hi","other":1}`, false},
		{"max length", `{"text":"` + strings.Repeat("x", 11) + `"}`, false},
		{"not an object", `"hi"`, false},
	}
	for _, c := range cases {
		err := sch.Validate([]byte(c.args))
		if c.ok && err != nil {
			t.Fatalf("%s: validate(%s) = %v, want nil", c.name, c.args, err)
		}
		if !c.ok && err == nil {
			t.Fatalf("%s: validate(%s) = nil, want an error", c.name, c.args)
		}
	}
}

func TestCompileInputSchemaRefusesRemoteRef(t *testing.T) {
	_, err := CompileInputSchema(json.RawMessage(`{
		"type": "object",
		"properties": {"text": {"$ref": "https://attacker.example/steal.json"}}
	}`))
	if err == nil {
		t.Fatal("a remote $ref compiled; the loader is supposed to refuse every off-document reference")
	}
	var ce *CompileError
	if !errors.As(err, &ce) {
		t.Fatalf("error type = %T, want *CompileError", err)
	}
	if !strings.Contains(err.Error(), "refused") && !strings.Contains(err.Error(), "ref") {
		t.Fatalf("error does not mention the refused reference: %v", err)
	}
}

func TestCompileInputSchemaLimits(t *testing.T) {
	deep := strings.Repeat(`{"properties":{"x":`, MaxSchemaDepth+5) + `{"type":"string"}` + strings.Repeat(`}}`, MaxSchemaDepth+5)
	if _, err := CompileInputSchema(json.RawMessage(`{"type":"object","properties":` + deep + `}`)); err == nil {
		t.Fatal("a schema past the depth limit compiled")
	} else if !strings.Contains(err.Error(), "complex") {
		t.Fatalf("depth error = %v, want a complexity complaint", err)
	}

	huge := json.RawMessage(`{"type":"object","properties":{"t":{"type":"string","description":"` +
		strings.Repeat("a", MaxSchemaBytes) + `"}}}`)
	if _, err := CompileInputSchema(huge); err == nil {
		t.Fatal("a schema past the byte limit compiled")
	}

	if _, err := CompileInputSchema(nil); err == nil {
		t.Fatal("an empty schema compiled")
	}
	if _, err := CompileInputSchema(json.RawMessage(`{"type":`)); err == nil {
		t.Fatal("invalid JSON compiled")
	}
}

func TestValidateEnforcesInputSize(t *testing.T) {
	sch, err := CompileInputSchema(json.RawMessage(`{"type":"object"}`))
	if err != nil {
		t.Fatal(err)
	}
	big := []byte(`{"text":"` + strings.Repeat("a", MaxInputBytes) + `"}`)
	if err := sch.Validate(big); err == nil {
		t.Fatal("an over-limit argument payload validated")
	}
	if err := sch.Validate(nil); err == nil {
		t.Fatal("empty arguments validated")
	}
}

// TestMaskArgumentsKeepsSecretsOutOfTheLedger is the ledger half of "secret
// 不出现在日志" — the masked copy is what an operator reads.
func TestMaskArgumentsKeepsSecretsOutOfTheLedger(t *testing.T) {
	raw := []byte(`{"text":"hi","Authorization":"Bearer abc123","nested":{"api_key":"xyz","ok":1},"list":[{"token":"shh"}]}`)
	masked := MaskArguments(raw)
	if masked == nil {
		t.Fatal("masked copy is nil")
	}
	s := string(masked)
	for _, secret := range []string{"abc123", "xyz", "shh"} {
		if strings.Contains(s, secret) {
			t.Fatalf("masked copy still contains %q: %s", secret, s)
		}
	}
	if !strings.Contains(s, "hi") {
		t.Fatalf("masking removed non-secret data: %s", s)
	}
	if got := string(MaskArguments([]byte("not json"))); got != "" {
		t.Fatalf("mask of invalid JSON = %q, want empty", got)
	}
}

func TestShapeOfCountsDepthAndNodes(t *testing.T) {
	var doc any
	if err := json.Unmarshal([]byte(`{"a":{"b":[1,{"c":2}]}}`), &doc); err != nil {
		t.Fatal(err)
	}
	// {"a": {"b": [1, {"c": 2}]}} nests object > object > array > object, so
	// depth 5; the six leaves are the three maps, the one array and the two
	// numbers.
	depth, nodes := shapeOf(doc)
	if depth != 5 {
		t.Fatalf("depth = %d, want 5", depth)
	}
	if nodes != 6 {
		t.Fatalf("nodes = %d, want 6", nodes)
	}
	if d, n := shapeOf("scalar"); d != 1 || n != 1 {
		t.Fatalf("scalar shape = (%d,%d), want (1,1)", d, n)
	}
}

func TestParseRevisionToolsValidates(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		ok   bool
	}{
		{"empty", ``, true},
		{"null", `null`, true},
		{"one pin", `{"pinned":[{"name":"lookup","version":1}]}`, true},
		{"two pins", `{"pinned":[{"name":"a","version":1},{"name":"b","version":2}]}`, true},
		{"duplicate name", `{"pinned":[{"name":"a","version":1},{"name":"a","version":2}]}`, false},
		{"no version", `{"pinned":[{"name":"a"}]}`, false},
		{"empty name", `{"pinned":[{"version":1}]}`, false},
		{"legacy allowed shape", `{"allowed":["echo"]}`, false},
		{"unknown field", `{"pinned":[],"extra":1}`, false},
	}
	for _, c := range cases {
		_, err := ParseRevisionTools(json.RawMessage(c.raw))
		if c.ok && err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if !c.ok && err == nil {
			t.Fatalf("%s: parsed without an error", c.name)
		}
	}
	pins, err := ParseRevisionTools(json.RawMessage(`{"pinned":[{"name":"a","version":3}]}`))
	if err != nil || len(pins) != 1 || pins[0].Name != "a" || pins[0].Version != 3 {
		t.Fatalf("pins = %+v, err = %v", pins, err)
	}
}

func TestResolveSecretHeadersRefusesOutOfAllowlist(t *testing.T) {
	raw := json.RawMessage(`{"headers":{"Authorization":"env:NOT_ALLOWED"}}`)
	resolver := secrets.NewResolver(secrets.AllowedPrefixes{EnvVars: []string{"ALLOWED_ONLY"}})
	if _, err := resolveSecretHeaders(resolver, raw); err == nil {
		t.Fatal("an env ref outside the allowlist resolved")
	}
	if _, err := resolveSecretHeaders(nil, raw); err == nil {
		t.Fatal("refs resolved without a resolver")
	}
	t.Setenv("ALLOWED_ONLY", "s3cret")
	got, err := resolveSecretHeaders(resolver, json.RawMessage(`{"headers":{"Authorization":"env:ALLOWED_ONLY"}}`))
	if err != nil {
		t.Fatalf("allowed ref: %v", err)
	}
	if got["Authorization"] != "s3cret" {
		t.Fatalf("resolved header = %q", got["Authorization"])
	}
	if _, err := resolveSecretHeaders(resolver, json.RawMessage(`{"bogus":{}}`)); err == nil {
		t.Fatal("an unknown secret_refs field parsed")
	}
	if v, err := resolveSecretHeaders(resolver, nil); err != nil || v != nil {
		t.Fatalf("empty refs: %v %v", v, err)
	}
}

func TestClassifyMapsErrorsToOutcomes(t *testing.T) {
	if o, et, _ := classify(nil); o != Succeeded || et != "" {
		t.Fatalf("nil error classified as %v/%s", o, et)
	}
	if o, et, _ := classify(errors.New("plain")); o != Failed || et != "tool_error" {
		t.Fatalf("plain error classified as %v/%s", o, et)
	}
	custom := &CallError{Outcome: Unknown, ErrorType: "transport_ambiguous", Err: fmt.Errorf("boom")}
	if o, et, msg := classify(custom); o != Unknown || et != "transport_ambiguous" || !strings.Contains(msg, "boom") {
		t.Fatalf("CallError classified as %v/%s/%s", o, et, msg)
	}
}
