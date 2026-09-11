package domain_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

const credentialSpec = `{"schema_version":"v1","credential_protocol_version":"v1","models":{"primary":{"kind":"openai_compatible","model":"model","base_url":"https://models.example.test/v1","api_key_credential_id":"crd_0123456789abcdef0123456789abcdef","capabilities":["chat"]}},"tools":{},"knowledge":{},"storage":{"session":{"kind":"postgres_state","dsn_credential_id":"crd_abcdef0123456789abcdef0123456789","destination":{"host":"db.example.test","port":5432,"database":"agent","username":"agent","sslmode":"verify-full"}}}}`

func TestCanonicalCredentialProtocolRequiresIDsAndStorageDestination(t *testing.T) {
	schema := compilePublicSchema(t)
	document := json.RawMessage(credentialSpec)
	validateWithPublicSchema(t, schema, document, true)
	canonical, report := domain.ValidateForPublication(document, 1)
	if !report.Valid {
		t.Fatalf("credential canonical publication: %#v", report)
	}
	var root map[string]any
	if err := json.Unmarshal(canonical.Document, &root); err != nil {
		t.Fatal(err)
	}
	if root["credential_protocol_version"] != "v1" {
		t.Fatalf("protocol was not canonicalized: %s", canonical.Document)
	}
	tests := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"missing protocol", func(r map[string]any) { delete(r, "credential_protocol_version") }},
		{"unsupported protocol", func(r map[string]any) { r["credential_protocol_version"] = "v2" }},
		{"legacy ref", func(r map[string]any) {
			m := r["models"].(map[string]any)["primary"].(map[string]any)
			delete(m, "api_key_credential_id")
			m["api_key_ref"] = "model-key"
		}},
		{"missing association", func(r map[string]any) {
			delete(r["models"].(map[string]any)["primary"].(map[string]any), "api_key_credential_id")
		}},
		{"user name as ID", func(r map[string]any) {
			r["models"].(map[string]any)["primary"].(map[string]any)["api_key_credential_id"] = "model-key"
		}},
		{"missing destination", func(r map[string]any) {
			delete(r["storage"].(map[string]any)["session"].(map[string]any), "destination")
		}},
		{"invalid port", func(r map[string]any) { destination(r)["port"] = 65536 }},
		{"fractional port", func(r map[string]any) { destination(r)["port"] = 5432.5 }},
		{"invalid sslmode", func(r map[string]any) { destination(r)["sslmode"] = "prefer" }},
		{"unknown options", func(r map[string]any) { destination(r)["options"] = map[string]any{"search_path": "public"} }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var r map[string]any
			if err := json.Unmarshal(document, &r); err != nil {
				t.Fatal(err)
			}
			tt.mutate(r)
			input, _ := json.Marshal(r)
			if report := domain.ValidateDraftForStorage(input, 1); !report.Valid {
				t.Fatalf("incomplete non-secret draft must remain storable: %#v", report)
			}
			validateWithPublicSchema(t, schema, input, false)
			if _, report := domain.ValidateForPublication(input, 1); report.Valid {
				t.Fatal("invalid canonical credential protocol was accepted")
			}
		})
	}
}

func destination(root map[string]any) map[string]any {
	return root["storage"].(map[string]any)["session"].(map[string]any)["destination"].(map[string]any)
}

func TestStorageDestinationCanonicalizationAndClosedShape(t *testing.T) {
	schema := compilePublicSchema(t)
	canonical, report := domain.ValidateForPublication(json.RawMessage(credentialSpec), 1)
	if !report.Valid {
		t.Fatalf("initial canonical Spec: %#v", report)
	}
	equivalent := bytes.Replace([]byte(credentialSpec), []byte(`"port":5432`), []byte(`"port":5.432e3`), 1)
	got, report := domain.ValidateForPublication(equivalent, 1)
	if !report.Valid || got.Digest != canonical.Digest || !bytes.Equal(got.Document, canonical.Document) {
		t.Fatalf("equivalent destination integer changed canonical document: %#v / %s", report, got.Document)
	}
	for _, mode := range []string{"disable", "require", "verify-full"} {
		t.Run(mode, func(t *testing.T) {
			document := bytes.Replace([]byte(credentialSpec), []byte(`"verify-full"`), []byte(`"`+mode+`"`), 1)
			validateWithPublicSchema(t, schema, document, true)
			if _, report := domain.ValidateForPublication(document, 1); !report.Valid {
				t.Fatalf("supported sslmode rejected: %#v", report)
			}
		})
	}
	for _, field := range []string{"host", "port", "database", "username", "sslmode"} {
		t.Run("missing "+field, func(t *testing.T) {
			var root map[string]any
			if err := json.Unmarshal([]byte(credentialSpec), &root); err != nil {
				t.Fatal(err)
			}
			delete(destination(root), field)
			document, _ := json.Marshal(root)
			validateWithPublicSchema(t, schema, document, false)
			_, report := domain.ValidateForPublication(document, 1)
			if report.Valid || !hasDiagnosticAt(report, "RUNTIME_PROFILE_SPEC_REQUIRED_FIELD", "/storage/session/destination/"+field) {
				t.Fatalf("missing destination field accepted: %#v", report)
			}
		})
	}
}

func TestCanonicalKnowledgeCredentialAssociationRemainsOptionalForQdrant(t *testing.T) {
	schema := compilePublicSchema(t)
	var root map[string]any
	if err := json.Unmarshal(readFixture(t, fixturePath(t, "valid", "knowledge-storage.json")), &root); err != nil {
		t.Fatal(err)
	}
	resource := root["knowledge"].(map[string]any)["docs"].(map[string]any)
	delete(resource, "qdrant_api_key_credential_id")
	document, _ := json.Marshal(root)
	validateWithPublicSchema(t, schema, document, true)
	canonical, report := domain.ValidateForPublication(document, 1)
	if !report.Valid {
		t.Fatalf("optional Qdrant credential required: %#v", report)
	}
	if bytes.Contains(canonical.Document, []byte(`"qdrant_api_key_credential_id"`)) {
		t.Fatalf("absent Qdrant credential was invented: %s", canonical.Document)
	}
	delete(resource["embedding"].(map[string]any), "api_key_credential_id")
	document, _ = json.Marshal(root)
	validateWithPublicSchema(t, schema, document, false)
	if _, report := domain.ValidateForPublication(document, 1); report.Valid {
		t.Fatal("embedding credential must remain required")
	}
}
