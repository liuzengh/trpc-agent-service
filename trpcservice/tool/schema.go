package tool

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode/utf8"

	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// Input schema compilation and validation (approved plan, "受控工具"): the
// tool's arguments are the one input a model writes freely, so they are
// checked against a real JSON Schema validator before any callable runs.
//
// The validator is a real library, not a hand-rolled walker, and it is pinned
// to Draft 2020-12 with every remote loader removed. That second half matters
// as much as the first: a schema is authored by an operator and stored in a
// database row, and "$ref": "https://attacker.example/x.json" would otherwise
// turn schema validation into an outbound HTTP request from inside the
// worker. Local references ("#/$defs/...") still work — they never reach the
// loader.
const (
	// MaxSchemaBytes bounds the stored schema; MaxInputBytes bounds one
	// tool's arguments. Both are the approved plan's numbers, applied at the
	// point where the bytes first exist, not after they have been parsed.
	MaxSchemaBytes = 64 << 10
	MaxInputBytes  = 64 << 10
	// MaxSchemaDepth and MaxSchemaNodes bound the shape, because a deeply
	// nested schema can make compilation itself expensive even while small in
	// bytes.
	MaxSchemaDepth = 32
	MaxSchemaNodes = 512
)

// CompileError marks a schema that will never become a tool. It is separate
// from a validation error because the two have opposite fixes: a compile
// error means the operator must republish the binding, a validation error
// means the model sent bad arguments for a perfectly good tool.
type CompileError struct {
	Reason string
}

func (e *CompileError) Error() string { return "tool: invalid input schema: " + e.Reason }

// InputSchema is a compiled schema plus the raw declaration the model sees.
type InputSchema struct {
	compiled    *jsonschema.Schema
	declaration *frameworktool.Schema
}

// CompileInputSchema validates shape limits, then compiles the schema with
// the default draft fixed to 2020-12 and remote references rejected.
func CompileInputSchema(raw json.RawMessage) (*InputSchema, error) {
	if len(raw) == 0 {
		return nil, &CompileError{Reason: "schema is empty"}
	}
	if len(raw) > MaxSchemaBytes {
		return nil, &CompileError{Reason: fmt.Sprintf("schema is %d bytes, limit is %d", len(raw), MaxSchemaBytes)}
	}
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, &CompileError{Reason: "schema is not valid JSON: " + err.Error()}
	}
	if depth, nodes := shapeOf(doc); depth > MaxSchemaDepth || nodes > MaxSchemaNodes {
		return nil, &CompileError{Reason: fmt.Sprintf(
			"schema is too complex: depth %d (limit %d), nodes %d (limit %d)",
			depth, MaxSchemaDepth, nodes, MaxSchemaNodes)}
	}

	// The root is registered under a URL that can never be fetched: any $ref
	// that escapes this document lands on the rejecting loader instead of the
	// network.
	const rootURL = "mem://reliable/input-schema.json"
	c := jsonschema.NewCompiler()
	c.DefaultDraft(jsonschema.Draft2020)
	c.UseLoader(rejectLoader{})
	if err := c.AddResource(rootURL, doc); err != nil {
		return nil, &CompileError{Reason: err.Error()}
	}
	compiled, err := c.Compile(rootURL)
	if err != nil {
		return nil, &CompileError{Reason: truncate(err.Error(), 300)}
	}

	decl := &frameworktool.Schema{}
	if err := json.Unmarshal(raw, decl); err != nil {
		// The raw schema is valid JSON but not expressible in the framework's
		// declaration shape (a scalar root, say). That is a publishable
		// schema for validation purposes but an unusable declaration, so it
		// is refused now rather than at the first model call.
		return nil, &CompileError{Reason: "schema cannot be declared to the model: " + err.Error()}
	}
	return &InputSchema{compiled: compiled, declaration: decl}, nil
}

// Validate checks one argument payload. The returned error is written to be
// shown to a human, not parsed: it carries the validator's own message,
// trimmed to something that fits in a journal detail column.
func (s *InputSchema) Validate(args json.RawMessage) error {
	if len(args) == 0 {
		return fmt.Errorf("tool: arguments are empty")
	}
	if len(args) > MaxInputBytes {
		return fmt.Errorf("tool: arguments are %d bytes, limit is %d", len(args), MaxInputBytes)
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(args))
	if err != nil {
		return fmt.Errorf("tool: arguments are not valid JSON: %w", err)
	}
	if err := s.compiled.Validate(inst); err != nil {
		return fmt.Errorf("tool: arguments do not match the schema: %s", truncate(err.Error(), 400))
	}
	return nil
}

// Declaration returns the framework-facing schema for the tool declaration.
func (s *InputSchema) Declaration() *frameworktool.Schema { return s.declaration }

// rejectLoader turns every attempt to resolve a reference outside the schema
// document itself into an error. The approved plan allows document-local
// $refs and forbids everything else; there is no allowlist mode because there
// is no legitimate remote reference for a stored tool schema.
type rejectLoader struct{}

func (rejectLoader) Load(url string) (any, error) {
	return nil, fmt.Errorf("tool: schema reference %q refused: only document-local $ref is allowed", url)
}

// shapeOf measures depth and node count of a decoded JSON document. Depth is
// the longest chain of objects/arrays, so a scalar is depth 1 — matching how
// a schema's nesting reads to a human, not how a stack frame counts.
func shapeOf(v any) (depth, nodes int) {
	switch t := v.(type) {
	case map[string]any:
		depth, nodes = 1, 1
		for _, child := range t {
			cd, cn := shapeOf(child)
			if cd+1 > depth {
				depth = cd + 1
			}
			nodes += cn
		}
	case []any:
		depth, nodes = 1, 1
		for _, child := range t {
			cd, cn := shapeOf(child)
			if cd+1 > depth {
				depth = cd + 1
			}
			nodes += cn
		}
	default:
		return 1, 1
	}
	return depth, nodes
}

// truncate cuts a string to at most n bytes on a rune boundary: an error
// message carrying unicode from a model-supplied argument must not be sliced
// mid-codepoint into a journal column.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := s[:n]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut + "..."
}
