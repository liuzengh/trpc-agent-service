package controleventsv1_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"

	controleventsv1 "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
)

const (
	eventSchemaLocation = "https://jfsas.dev/events/control/v1/runtime-manifest-published.schema.json"
	manifestLocation    = "https://jfsas.dev/schemas/deployment/v1/runtime-manifest.schema.json"
)

func TestRuntimeManifestPublishedSchemaCompiles(t *testing.T) {
	compileEventSchema(t)
}

func TestRuntimeManifestPublishedValidFixture(t *testing.T) {
	schema := compileEventSchema(t)
	document := readFixture(t, filepath.Join("examples", "valid", "runtime-manifest-published.json"))
	validateDocument(t, schema, document, true)
	if err := validateEventIdentity(document); err != nil {
		t.Fatalf("event identity: %v", err)
	}
}

func TestRuntimeManifestPublishedRejectsSecretBearingManifest(t *testing.T) {
	schema := compileEventSchema(t)
	document := readFixture(t, filepath.Join("examples", "invalid", "credential-value.json"))
	validateDocument(t, schema, document, false)
}

func TestRuntimeManifestPublishedRejectsMismatchedIdentitySemantically(t *testing.T) {
	schema := compileEventSchema(t)
	document := readFixture(t, filepath.Join("examples", "invalid", "identity-mismatch.json"))
	validateDocument(t, schema, document, true)
	if err := validateEventIdentity(document); err == nil {
		t.Fatal("cross-field validation accepted mismatched event and manifest identities")
	}
}

func TestRuntimeManifestPublishedContainsNoCredentialValuesOrDynamicState(t *testing.T) {
	var document any
	if err := json.Unmarshal(readFixture(t, filepath.Join("examples", "valid", "runtime-manifest-published.json")), &document); err != nil {
		t.Fatalf("decode event fixture: %v", err)
	}
	forbidden := map[string]bool{
		"association_token":   true,
		"ciphertext":          true,
		"configured":          true,
		"credential_revision": true,
		"nonce":               true,
		"password":            true,
		"status":              true,
		"value":               true,
	}
	walkObjects(document, func(object map[string]any) {
		for key := range object {
			if forbidden[key] {
				t.Errorf("event contains forbidden field %q", key)
			}
		}
	})
}

func compileEventSchema(t *testing.T) *jsonschema.Schema {
	t.Helper()
	compiler := jsonschema.NewCompiler()
	for _, resource := range []struct {
		location string
		document []byte
	}{
		{manifestLocation, deploymentv1.ManifestSchema},
		{eventSchemaLocation, controleventsv1.RuntimeManifestPublishedSchema},
	} {
		decoded, err := jsonschema.UnmarshalJSON(bytes.NewReader(resource.document))
		if err != nil {
			t.Fatalf("decode schema %s: %v", resource.location, err)
		}
		if err := compiler.AddResource(resource.location, decoded); err != nil {
			t.Fatalf("add schema %s: %v", resource.location, err)
		}
	}
	schema, err := compiler.Compile(eventSchemaLocation)
	if err != nil {
		t.Fatalf("compile event schema: %v", err)
	}
	return schema
}

func validateDocument(t *testing.T, schema *jsonschema.Schema, document []byte, wantValid bool) {
	t.Helper()
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(document))
	if err != nil {
		if wantValid {
			t.Fatalf("decode event: %v", err)
		}
		return
	}
	err = schema.Validate(instance)
	if wantValid && err != nil {
		t.Fatalf("schema rejected valid event: %v", err)
	}
	if !wantValid && err == nil {
		t.Fatal("schema accepted invalid event")
	}
}

func validateEventIdentity(document []byte) error {
	var event struct {
		TenantID             string `json:"tenant_id"`
		DeploymentID         string `json:"deployment_id"`
		DeploymentRevisionID string `json:"deployment_revision_id"`
		RevisionNumber       int64  `json:"revision_number"`
		OccurredAt           string `json:"occurred_at"`
		Manifest             struct {
			TenantID             string          `json:"tenant_id"`
			DeploymentID         string          `json:"deployment_id"`
			DeploymentRevisionID string          `json:"deployment_revision_id"`
			RevisionNumber       int64           `json:"revision_number"`
			PublishedAt          string          `json:"published_at"`
			Content              json.RawMessage `json:"content"`
		} `json:"manifest"`
	}
	if err := json.Unmarshal(document, &event); err != nil {
		return err
	}
	if event.TenantID != event.Manifest.TenantID ||
		event.DeploymentID != event.Manifest.DeploymentID ||
		event.DeploymentRevisionID != event.Manifest.DeploymentRevisionID ||
		event.RevisionNumber != event.Manifest.RevisionNumber ||
		event.OccurredAt != event.Manifest.PublishedAt {
		return &identityError{"event and manifest envelope differ"}
	}
	var content struct {
		TenantID string `json:"tenant_id"`
	}
	if err := json.Unmarshal(event.Manifest.Content, &content); err != nil {
		return err
	}
	if content.TenantID != event.TenantID {
		return &identityError{"event and manifest content tenants differ"}
	}
	return nil
}

type identityError struct{ message string }

func (e *identityError) Error() string { return e.message }

func readFixture(t *testing.T, path string) []byte {
	t.Helper()
	document, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return document
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

func TestRuntimeManifestPublishedTraceContextIsClosed(t *testing.T) {
	schema := compileEventSchema(t)
	valid := readFixture(t, filepath.Join("examples", "valid", "runtime-manifest-published.json"))
	var document map[string]any
	if err := json.Unmarshal(valid, &document); err != nil {
		t.Fatal(err)
	}
	document["trace_context"] = map[string]any{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	validateDocument(t, schema, encoded, true)
	document["trace_context"].(map[string]any)["baggage"] = strings.Repeat("secret", 2)
	encoded, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	validateDocument(t, schema, encoded, false)
}
