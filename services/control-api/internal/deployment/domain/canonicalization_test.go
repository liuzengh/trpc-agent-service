package domain

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestCredentialAudienceGoldenValuesMatchProfileV1(t *testing.T) {
	compiled, report := Compile(validCompileInput())
	if !report.Valid {
		t.Fatalf("Compile report = %#v", report)
	}
	want := map[string]string{
		credentialModel:     "sha256:735b091eb7354ee37b2b18a57b023e4e699e13a929d879579d8be3c5544a80a5",
		credentialSearch:    "sha256:c8ddc0025abe5752c999e98099ca7a186f5ef12d871178c0ab6261771488658f",
		credentialQdrant:    "sha256:0c121762a8b7756748db5fc96881244de22c68e1f3e1ad815e8b16740ffbd346",
		credentialEmbedding: "sha256:32072ebcc613b05678dd0d6d32940c13ab10a1f994692d3a38b0c049815855da",
		credentialSession:   "sha256:120b3148a7b58ddc2eac9fbdf494ab28ebd0bda95aa9803be24edfe47cd0212e",
		credentialMemory:    "sha256:120b3148a7b58ddc2eac9fbdf494ab28ebd0bda95aa9803be24edfe47cd0212e",
	}
	for _, use := range compiled.CredentialUses {
		if expected, exists := want[use.CredentialID]; !exists || use.AudienceDigest != expected {
			t.Fatalf("credential use = %#v, expected = %q", use, expected)
		}
		delete(want, use.CredentialID)
	}
	if len(want) != 0 {
		t.Fatalf("missing credential audience cases = %#v", want)
	}
}

func TestManifestClosedUnionsOmitInactiveFields(t *testing.T) {
	compiled, report := Compile(validCompileInput())
	if !report.Valid {
		t.Fatalf("Compile report = %#v", report)
	}
	rootJSON, err := json.Marshal(compiled.Content.AgentPlan.Nodes["main"])
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"instruction", "model_resource", "tool_resources", "body", "max_iterations"} {
		if bytes.Contains(rootJSON, []byte(forbidden)) {
			t.Fatalf("sequence node leaked %q: %s", forbidden, rootJSON)
		}
	}
	llmJSON, err := json.Marshal(compiled.Content.AgentPlan.Nodes["writer"])
	if err != nil {
		t.Fatal(err)
	}
	for _, required := range []string{`"tool_resources":[]`, `"knowledge_resources":[]`, `"callable_entries":[]`} {
		if !bytes.Contains(llmJSON, []byte(required)) {
			t.Fatalf("LLM node did not preserve empty list %q: %s", required, llmJSON)
		}
	}
	noneInput := validCompileInput()
	search := noneInput.Profile.Spec.Tools["search"]
	search.Auth.Kind = "none"
	search.Auth.CredentialID = "must-not-leak"
	noneInput.Profile.Spec.Tools["search"] = search
	noneCompiled, noneReport := Compile(noneInput)
	if !noneReport.Valid {
		t.Fatalf("none auth report = %#v", noneReport)
	}
	authJSON, err := json.Marshal(noneCompiled.Content.Resources.Tools["search"].Auth)
	if err != nil || string(authJSON) != `{"kind":"none"}` {
		t.Fatalf("none auth = %s, %v", authJSON, err)
	}
	for _, use := range noneCompiled.CredentialUses {
		if use.CredentialID == "must-not-leak" {
			t.Fatalf("inactive credential entered use closure: %#v", use)
		}
	}
}

func TestPublicManifestViewFromJSONIsClosedAndRejectsTrailingInput(t *testing.T) {
	compiled, report := Compile(validCompileInput())
	if !report.Valid {
		t.Fatalf("Compile report = %#v", report)
	}
	view, err := PublicManifestViewFromJSON(compiled.CanonicalContent)
	if err != nil {
		t.Fatalf("PublicManifestViewFromJSON: %v", err)
	}
	encoded, err := json.Marshal(view)
	if err != nil || bytes.Contains(encoded, []byte("credential_id")) {
		t.Fatalf("view = %s, %v", encoded, err)
	}
	var jsonbStyle bytes.Buffer
	if err := json.Indent(&jsonbStyle, compiled.CanonicalContent, "", "  "); err != nil {
		t.Fatal(err)
	}
	if _, err := PublicManifestViewFromJSON(jsonbStyle.Bytes()); err != nil {
		t.Fatalf("PublicManifestViewFromJSON rejected a valid non-JCS jsonb representation: %v", err)
	}
	if _, err := PublicManifestViewFromJSON(append(append(json.RawMessage(nil), compiled.CanonicalContent...), []byte(` {}`)...)); err == nil {
		t.Fatal("trailing JSON was accepted")
	}
	unknown := bytes.Replace(compiled.CanonicalContent,
		[]byte(`"schema_version":"v1"`),
		[]byte(`"schema_version":"v1","unknown":true`), 1)
	if _, err := PublicManifestViewFromJSON(unknown); err == nil {
		t.Fatal("unknown top-level field was accepted")
	}
	nodeUnknown := bytes.Replace(compiled.CanonicalContent,
		[]byte(`"instruction":"write"`),
		[]byte(`"instruction":"write","unknown":true`), 1)
	if _, err := PublicManifestViewFromJSON(nodeUnknown); err == nil {
		t.Fatal("unknown node field was accepted")
	}
	duplicate := bytes.Replace(compiled.CanonicalContent,
		[]byte(`"schema_version":"v1"`),
		[]byte(`"schema_version":"v1","schema_version":"v1"`), 1)
	if _, err := PublicManifestViewFromJSON(duplicate); err == nil {
		t.Fatal("duplicate JSON key was accepted")
	}
	invalidCredential := bytes.Replace(compiled.CanonicalContent,
		[]byte(credentialModel), []byte("crd_zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"), 1)
	if _, err := PublicManifestViewFromJSON(invalidCredential); err == nil {
		t.Fatal("invalid internal credential identity was projected as configured")
	}
}

func TestVerifyManifestContentRejectsNonCanonicalAndDigestTampering(t *testing.T) {
	compiled, report := Compile(validCompileInput())
	if !report.Valid {
		t.Fatalf("Compile report = %#v", report)
	}
	if _, err := VerifyManifestContent(compiled.CanonicalContent, compiled.ContentDigest); err != nil {
		t.Fatal(err)
	}
	pretty := json.RawMessage(strings.ReplaceAll(string(compiled.CanonicalContent), `,"`, ",\n\""))
	if bytes.Equal(pretty, compiled.CanonicalContent) {
		t.Fatal("test did not change manifest bytes")
	}
	if _, err := VerifyManifestContent(pretty, compiled.ContentDigest); err == nil {
		t.Fatal("non-canonical bytes were accepted")
	}
	if _, err := ValidateManifestContent(pretty, compiled.ContentDigest); err != nil {
		t.Fatalf("equivalent jsonb representation was rejected: %v", err)
	}
	if _, err := VerifyManifestContent(compiled.CanonicalContent, "sha256:"+strings.Repeat("0", 64)); err == nil {
		t.Fatal("wrong digest was accepted")
	}
}
