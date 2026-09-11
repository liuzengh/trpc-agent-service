package datav1

import (
	"bytes"
	"encoding/json"
	jsonschema "github.com/santhosh-tekuri/jsonschema/v6"
	"testing"
)

func TestSchemaMatchesFourWireBranches(t *testing.T) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(Schema))
	if err != nil {
		t.Fatal(err)
	}
	compiler := jsonschema.NewCompiler()
	const location = "https://jfsas.dev/schemas/runtime/data/v1/backend-snapshot.schema.json"
	if err := compiler.AddResource(location, doc); err != nil {
		t.Fatal(err)
	}
	schema, err := compiler.Compile(location)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range fixtures() {
		b, _ := s.Canonical()
		v, err := jsonschema.UnmarshalJSON(bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		if err := schema.Validate(v); err != nil {
			t.Fatal(s.Kind, err)
		}
		var invalid map[string]any
		_ = json.Unmarshal(b, &invalid)
		invalid["password"] = "private"
		if err := schema.Validate(invalid); err == nil {
			t.Fatal("schema accepted credential")
		}
		delete(invalid, "password")
		invalid["adapter"] = "arbitrary"
		if err := schema.Validate(invalid); err == nil {
			t.Fatal("schema accepted arbitrary adapter")
		}
	}
}
