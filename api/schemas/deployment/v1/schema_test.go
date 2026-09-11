package deploymentv1_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/gowebpki/jcs"
	"github.com/santhosh-tekuri/jsonschema/v6"

	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
)

const (
	inputSchemaLocation  = "https://jfsas.dev/schemas/deployment/v1/deployment-input.schema.json"
	manifestLocation     = "https://jfsas.dev/schemas/deployment/v1/runtime-manifest.schema.json"
	manifestViewLocation = "https://jfsas.dev/schemas/deployment/v1/runtime-manifest-view.schema.json"
)

func TestDeploymentSchemasCompile(t *testing.T) {
	for _, test := range []struct {
		name     string
		location string
		document []byte
	}{
		{"input", inputSchemaLocation, deploymentv1.InputSchema},
		{"manifest", manifestLocation, deploymentv1.ManifestSchema},
		{"manifest view", manifestViewLocation, deploymentv1.ManifestViewSchema},
	} {
		t.Run(test.name, func(t *testing.T) {
			compileSchema(t, test.location, test.document)
		})
	}
}

func TestDeploymentValidFixtures(t *testing.T) {
	input := compileSchema(t, inputSchemaLocation, deploymentv1.InputSchema)
	manifest := compileSchema(t, manifestLocation, deploymentv1.ManifestSchema)
	view := compileSchema(t, manifestViewLocation, deploymentv1.ManifestViewSchema)

	for _, test := range []struct {
		name   string
		schema *jsonschema.Schema
	}{
		{"deployment-input.json", input},
		{"runtime-manifest.json", manifest},
		{"runtime-manifest-worker-v1.json", manifest},
		{"runtime-manifest-view-worker-v1.json", view},
		{"runtime-manifest-no-optional-credentials.json", manifest},
		{"runtime-manifest-view.json", view},
		{"runtime-manifest-view-no-optional-credentials.json", view},
	} {
		t.Run(test.name, func(t *testing.T) {
			validateFixture(t, test.schema, filepath.Join("examples", "valid", test.name), true)
		})
	}
}

func TestDeploymentInvalidFixtures(t *testing.T) {
	input := compileSchema(t, inputSchemaLocation, deploymentv1.InputSchema)
	manifest := compileSchema(t, manifestLocation, deploymentv1.ManifestSchema)
	view := compileSchema(t, manifestViewLocation, deploymentv1.ManifestViewSchema)

	for _, test := range []struct {
		name   string
		schema *jsonschema.Schema
	}{
		{"input-environment.json", input},
		{"input-bindings.json", input},
		{"input-latest-selector.json", input},
		{"input-unknown-field.json", input},
		{"manifest-credential-value.json", manifest},
		{"manifest-wrong-purpose.json", manifest},
		{"manifest-storage-legacy-tls.json", manifest},
		{"view-credential-id.json", view},
		{"view-dynamic-credential-state.json", view},
	} {
		t.Run(test.name, func(t *testing.T) {
			validateFixture(t, test.schema, filepath.Join("examples", "invalid", test.name), false)
		})
	}
}

func TestRuntimeManifestDigestCoversCanonicalInternalContent(t *testing.T) {
	for _, name := range []string{
		"runtime-manifest.json",
		"runtime-manifest-worker-v1.json",
		"runtime-manifest-no-optional-credentials.json",
	} {
		t.Run(name, func(t *testing.T) {
			var envelope struct {
				Content       json.RawMessage `json:"content"`
				ContentDigest string          `json:"content_digest"`
			}
			if err := json.Unmarshal(readFixture(t, filepath.Join("examples", "valid", name)), &envelope); err != nil {
				t.Fatalf("decode manifest fixture: %v", err)
			}
			canonical, err := jcs.Transform(envelope.Content)
			if err != nil {
				t.Fatalf("canonicalize manifest content: %v", err)
			}
			sum := sha256.Sum256(canonical)
			got := "sha256:" + hex.EncodeToString(sum[:])
			if got != envelope.ContentDigest {
				t.Fatalf("content digest = %q, want %q", envelope.ContentDigest, got)
			}
		})
	}
}

func TestRuntimeManifestUsesExactProfileCredentialPurposes(t *testing.T) {
	document := decodeFixture(t, filepath.Join("examples", "valid", "runtime-manifest.json"))
	want := []string{"api_key", "bearer_token", "dsn", "embedding_api_key", "qdrant_api_key"}
	var got []string
	walkObjects(document, func(object map[string]any) {
		if _, ok := object["credential_id"]; !ok {
			return
		}
		keys := sortedKeys(object)
		if strings.Join(keys, ",") != "audience_digest,credential_id,purpose" {
			t.Errorf("credential descriptor fields = %v", keys)
		}
		purpose, ok := object["purpose"].(string)
		if !ok {
			t.Errorf("credential purpose is not a string: %#v", object["purpose"])
			return
		}
		got = append(got, purpose)
	})
	sort.Strings(got)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("credential purposes = %v, want %v", got, want)
	}
}

func TestRuntimeManifestStorageDestinationIsFixedAndNonSecret(t *testing.T) {
	document := decodeFixture(t, filepath.Join("examples", "valid", "runtime-manifest.json"))
	destination := nestedObject(t, document,
		"content", "resources", "storage", "session", "destination")
	want := "database,host,port,sslmode,username"
	if got := strings.Join(sortedKeys(destination), ","); got != want {
		t.Fatalf("storage destination fields = %q, want %q", got, want)
	}
}

func TestRuntimeManifestContainsNoCredentialValuesOrDynamicState(t *testing.T) {
	document := decodeFixture(t, filepath.Join("examples", "valid", "runtime-manifest.json"))
	assertNoForbiddenKeys(t, document, map[string]bool{
		"association_token":   true,
		"ciphertext":          true,
		"configured":          true,
		"credential_revision": true,
		"nonce":               true,
		"password":            true,
		"status":              true,
		"value":               true,
	})
}

func TestRuntimeManifestPublicViewContainsNoCredentialIdentityOrDynamicState(t *testing.T) {
	for _, name := range []string{
		"runtime-manifest-view.json",
		"runtime-manifest-view-no-optional-credentials.json",
	} {
		t.Run(name, func(t *testing.T) {
			document := decodeFixture(t, filepath.Join("examples", "valid", name))
			assertNoForbiddenKeys(t, document, map[string]bool{
				"association_token":   true,
				"audience_digest":     true,
				"ciphertext":          true,
				"configured":          true,
				"credential":          true,
				"credential_id":       true,
				"credential_revision": true,
				"nonce":               true,
				"password":            true,
				"purpose":             true,
				"status":              true,
				"value":               true,
			})
		})
	}
}

func TestRuntimeManifestEnvelopeAndContentShareTenant(t *testing.T) {
	document := decodeFixture(t, filepath.Join("examples", "valid", "runtime-manifest.json"))
	content := nestedObject(t, document, "content")
	if document["tenant_id"] != content["tenant_id"] {
		t.Fatalf("envelope tenant %v != content tenant %v", document["tenant_id"], content["tenant_id"])
	}
}

func compileSchema(t *testing.T, location string, document []byte) *jsonschema.Schema {
	t.Helper()
	decoded, err := jsonschema.UnmarshalJSON(bytes.NewReader(document))
	if err != nil {
		t.Fatalf("decode schema %s: %v", location, err)
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(location, decoded); err != nil {
		t.Fatalf("add schema %s: %v", location, err)
	}
	schema, err := compiler.Compile(location)
	if err != nil {
		t.Fatalf("compile schema %s: %v", location, err)
	}
	return schema
}

func validateFixture(t *testing.T, schema *jsonschema.Schema, path string, wantValid bool) {
	t.Helper()
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(readFixture(t, path)))
	if err != nil {
		if wantValid {
			t.Fatalf("decode valid fixture: %v", err)
		}
		return
	}
	err = schema.Validate(instance)
	if wantValid && err != nil {
		t.Fatalf("schema rejected valid fixture: %v", err)
	}
	if !wantValid && err == nil {
		t.Fatal("schema accepted invalid fixture")
	}
}

func readFixture(t *testing.T, path string) []byte {
	t.Helper()
	document, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return document
}

func decodeFixture(t *testing.T, path string) map[string]any {
	t.Helper()
	var document map[string]any
	decoder := json.NewDecoder(bytes.NewReader(readFixture(t, path)))
	decoder.UseNumber()
	if err := decoder.Decode(&document); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return document
}

func nestedObject(t *testing.T, root map[string]any, path ...string) map[string]any {
	t.Helper()
	current := root
	for _, component := range path {
		next, ok := current[component].(map[string]any)
		if !ok {
			t.Fatalf("%s is not an object", strings.Join(path, "/"))
		}
		current = next
	}
	return current
}

func walkObjects(value any, visit func(map[string]any)) {
	switch typed := value.(type) {
	case map[string]any:
		visit(typed)
		for _, child := range typed {
			walkObjects(child, visit)
		}
	case []any:
		for _, child := range typed {
			walkObjects(child, visit)
		}
	}
}

func assertNoForbiddenKeys(t *testing.T, document map[string]any, forbidden map[string]bool) {
	t.Helper()
	walkObjects(document, func(object map[string]any) {
		for key := range object {
			if forbidden[key] || strings.HasSuffix(key, "_credential_id") {
				t.Errorf("forbidden field %q is present", key)
			}
		}
	})
}

func sortedKeys(object map[string]any) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
