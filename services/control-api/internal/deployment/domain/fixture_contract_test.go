package domain

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gowebpki/jcs"
	"github.com/santhosh-tekuri/jsonschema/v6"

	controleventsv1 "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
)

const fixtureRepositoryRoot = "../../../../.."

// Golden documents must satisfy the consumer's semantic contract, not only the
// more limited JSON Schema shape. Every internal fixture has one exact public
// projection, and every public fixture must be paired with an internal source.
func TestGoldenManifestFixturesMatchDomainAndPublicProjection(t *testing.T) {
	dir := filepath.Join(fixtureRepositoryRoot, "api/schemas/deployment/v1/examples/valid")
	files, err := filepath.Glob(filepath.Join(dir, "runtime-manifest*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("manifest fixtures = %v, error = %v", files, err)
	}
	pairedViews := make(map[string]bool)
	manifestCount := 0
	for _, path := range files {
		if strings.Contains(filepath.Base(path), "-view") {
			continue
		}
		manifestCount++
		t.Run(filepath.Base(path), func(t *testing.T) {
			var manifest RuntimeManifest
			fixtureDecode(t, path, &manifest)
			content, err := ValidateManifestContent(manifest.Content, manifest.ContentDigest)
			if err != nil {
				t.Fatalf("golden manifest violates consumer semantics: %v", err)
			}
			viewPath := strings.Replace(path, "runtime-manifest", "runtime-manifest-view", 1)
			viewRaw := fixtureRead(t, viewPath)
			want, err := jcs.Transform(viewRaw)
			if err != nil {
				t.Fatal(err)
			}
			got := fixtureCanonicalJSON(t, NewPublicManifestView(content))
			if !bytes.Equal(got, want) {
				t.Fatalf("public golden is not the exact internal projection:\ngot %s\nwant %s", got, want)
			}
			pairedViews[viewPath] = true
		})
	}
	if manifestCount < 2 {
		t.Fatal("both credential-presence variants must remain covered")
	}
	for _, path := range files {
		if strings.Contains(filepath.Base(path), "-view") && !pairedViews[path] {
			t.Errorf("public fixture has no passing internal counterpart: %s", path)
		}
	}
}

func TestGoldenEventFixturesEmbedValidManifest(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(fixtureRepositoryRoot, "api/events/control/v1/examples/valid/*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("event fixtures = %v, error = %v", files, err)
	}
	for _, path := range files {
		t.Run(filepath.Base(path), func(t *testing.T) {
			var event RuntimeManifestPublishedEvent
			fixtureDecode(t, path, &event)
			content, err := ValidateManifestContent(event.Manifest.Content, event.Manifest.ContentDigest)
			if err != nil {
				t.Fatalf("embedded golden manifest violates consumer semantics: %v", err)
			}
			if event.SchemaVersion != EventSchemaVersionV1 || event.EventType != ManifestPublishedType ||
				event.EventID == "" || event.TenantID != event.Manifest.TenantID || event.TenantID != content.TenantID ||
				event.DeploymentID != event.Manifest.DeploymentID || event.DeploymentRevisionID != event.Manifest.DeploymentRevisionID ||
				event.RevisionNumber != event.Manifest.RevisionNumber || !event.OccurredAt.Equal(event.Manifest.PublishedAt) {
				t.Fatal("event identity and embedded manifest do not agree")
			}
			var standalone RuntimeManifest
			standaloneName := "runtime-manifest.json"
			if filepath.Base(path) == "runtime-manifest-worker-v1.json" {
				standaloneName = "runtime-manifest-worker-v1.json"
			}
			fixtureDecode(t, filepath.Join(fixtureRepositoryRoot, "api/schemas/deployment/v1/examples/valid", standaloneName), &standalone)
			if !bytes.Equal(fixtureCanonicalJSON(t, event.Manifest), fixtureCanonicalJSON(t, standalone)) {
				t.Fatal("event fixture embeds a different snapshot from the standalone golden manifest")
			}
		})
	}
}

// The opposite direction is also contractual: actual Compiler output and its
// explicit public projection must satisfy the published transport schemas.
func TestCompilerOutputsMatchPublishedSchemas(t *testing.T) {
	const manifestURL = "https://jfsas.dev/schemas/deployment/v1/runtime-manifest.schema.json"
	const viewURL = "https://jfsas.dev/schemas/deployment/v1/runtime-manifest-view.schema.json"
	const eventURL = "https://jfsas.dev/events/control/v1/runtime-manifest-published.schema.json"
	compiler := jsonschema.NewCompiler()
	for url, raw := range map[string][]byte{
		manifestURL: deploymentv1.ManifestSchema,
		viewURL:     deploymentv1.ManifestViewSchema,
		eventURL:    controleventsv1.RuntimeManifestPublishedSchema,
	} {
		value, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		if err := compiler.AddResource(url, value); err != nil {
			t.Fatal(err)
		}
	}
	for _, mode := range []string{"optional", "no-optional", "managed", "data"} {
		optionalCredentials := mode != "no-optional"
		name := mode
		if !optionalCredentials {
			name = "without optional credentials"
		}
		t.Run(name, func(t *testing.T) {
			input := validCompileInput()
			if mode == "managed" {
				input = managedExecutableInput()
			}
			if mode == "data" {
				input = fullDataInput()
			}
			if !optionalCredentials {
				tool := input.Profile.Spec.Tools["search"]
				tool.Auth.Kind, tool.Auth.CredentialID = "none", ""
				input.Profile.Spec.Tools["search"] = tool
				knowledge := input.Profile.Spec.Knowledge["docs"]
				knowledge.QdrantAPIKeyCredentialID = ""
				input.Profile.Spec.Knowledge["docs"] = knowledge
			}
			compiled, report := Compile(input)
			if !report.Valid {
				t.Fatalf("Compile: %#v", report)
			}
			if _, err := ValidateManifestContent(compiled.CanonicalContent, compiled.ContentDigest); err != nil {
				t.Fatalf("compiled output violates consumer semantics: %v", err)
			}
			manifest := RuntimeManifest{
				ID: "rmf_compiler_contract", TenantID: input.TenantID,
				DeploymentID: "dpl_compiler_contract", DeploymentRevisionID: "dpr_compiler_contract", RevisionNumber: 1,
				PublishedAt: time.Date(2026, 9, 5, 0, 0, 0, 0, time.UTC),
				Content:     compiled.CanonicalContent, ContentDigest: compiled.ContentDigest,
			}
			event := RuntimeManifestPublishedEvent{
				SchemaVersion: EventSchemaVersionV1, EventType: ManifestPublishedType, EventID: "evt_compiler_contract",
				TenantID: manifest.TenantID, DeploymentID: manifest.DeploymentID,
				DeploymentRevisionID: manifest.DeploymentRevisionID, RevisionNumber: manifest.RevisionNumber,
				OccurredAt: manifest.PublishedAt, Manifest: manifest,
			}
			for url, value := range map[string]any{manifestURL: manifest, viewURL: NewPublicManifestView(compiled.Content), eventURL: event} {
				schema, err := compiler.Compile(url)
				if err != nil {
					t.Fatal(err)
				}
				decoded, err := jsonschema.UnmarshalJSON(bytes.NewReader(fixtureCanonicalJSON(t, value)))
				if err != nil {
					t.Fatal(err)
				}
				if err := schema.Validate(decoded); err != nil {
					t.Fatalf("Compiler output violates %s: %v", url, err)
				}
			}
		})
	}
}

func fixtureRead(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func fixtureDecode(t *testing.T, path string, target any) {
	t.Helper()
	if err := json.Unmarshal(fixtureRead(t, path), target); err != nil {
		t.Fatal(err)
	}
}

func fixtureCanonicalJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		t.Fatal(err)
	}
	return canonical
}
