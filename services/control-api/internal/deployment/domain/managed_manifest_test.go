package domain

import (
	"bytes"
	"encoding/json"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"strings"
	"testing"
)

func managedExecutableInput() CompileInput {
	in := managedInput()
	in.Platform.StorageAdapters[profiledomain.StorageKindManagedSession] = AdapterContract{Version: StorageAdapterManagedSessionV1}
	in.Platform.StorageAdapters[profiledomain.StorageKindManagedMemory] = AdapterContract{Version: StorageAdapterManagedMemoryV1}
	in.Platform.KnowledgeAdapters[profiledomain.KnowledgeKindManaged] = KnowledgeAdapterContract{Version: KnowledgeAdapterManagedV1, CreatesCallable: true}
	in.Platform.Digest, _ = in.Platform.CalculateDigest()
	return in
}
func TestCompileManagedManifestRoundTripAndRedactedView(t *testing.T) {
	in := managedExecutableInput()
	compiled, report := Compile(in)
	if !report.Valid {
		t.Fatal(report.Diagnostics)
	}
	if _, err := deploymentv1.DecodeManifestContent(compiled.CanonicalContent); err != nil {
		t.Fatal("shared managed decode", err)
	}
	if _, err := VerifyManifestContent(compiled.CanonicalContent, compiled.ContentDigest); err != nil {
		t.Fatal("compiled manifest fails persisted validation", err)
	}
	if compiled.Content.Resources.Storage["session"].Backend.Redis.Host != "redis.internal" || compiled.Content.Resources.Knowledge["docs"].Backend.Qdrant.Collection != "docs" {
		t.Fatal("missing fixed targets")
	}
	if _, extra := compiled.Content.Resources.Storage["artifact"]; extra {
		t.Fatal("undeclared artifact exposure")
	}
	for _, use := range compiled.CredentialUses {
		if use.Purpose == CredentialPurposeDSN || use.Purpose == CredentialPurposeQdrantAPIKey {
			t.Fatal("platform credential leaked into Profile credential use", use.Purpose)
		}
	}
	view := NewPublicManifestView(compiled.Content)
	public, err := json.Marshal(view)
	if err != nil {
		t.Fatal(err)
	}
	for _, private := range []string{"redis.internal", "pg.internal", "qdrant.internal", `"vector_name"`, `"username"`, `"dsn"`, `"credential_id"`} {
		if bytes.Contains(public, []byte(private)) {
			t.Fatalf("public view leaked %s", private)
		}
	}
	if !bytes.Contains(public, []byte(`"backend_id":"redis"`)) {
		t.Fatal("missing public logical backend identity")
	}
	// Mutating caller-owned compiler input or public output never changes content.
	snapshot := in.ManagedBackends["storage/session"]
	snapshot.Redis.Host = "mutated.internal"
	in.ManagedBackends["storage/session"] = snapshot
	view.Resources.Storage["session"].Backend.BackendID = "mutated"
	if compiled.Content.Resources.Storage["session"].Backend.Redis.Host != "redis.internal" || compiled.Content.Resources.Storage["session"].Backend.BackendID != "redis" {
		t.Fatal("alias escaped into fixed content")
	}
	// SQL jsonb-style reserialization preserves semantic validation and Digest.
	var tree any
	_ = json.Unmarshal(compiled.CanonicalContent, &tree)
	raw, _ := json.MarshalIndent(tree, "", " ")
	if _, err := ValidateManifestContent(raw, compiled.ContentDigest); err != nil {
		t.Fatal(err)
	}
}
func TestManagedManifestRejectsRehashedTampering(t *testing.T) {
	compiled, report := Compile(managedExecutableInput())
	if !report.Valid {
		t.Fatal(report.Diagnostics)
	}
	for _, mutation := range []func(*ManifestContent){
		func(c *ManifestContent) { c.Resources.Storage["session"].Backend.TenantID = "other" },
		func(c *ManifestContent) { c.Resources.Storage["session"].Backend.Redis.Host = "foreign.internal" },
		func(c *ManifestContent) { c.Resources.Knowledge["docs"].Backend.Qdrant.Dimensions++ },
		func(c *ManifestContent) {
			r := c.Resources.Storage["session"]
			r.AdapterVersion = "arbitrary"
			c.Resources.Storage["session"] = r
		},
		func(c *ManifestContent) {
			c.Resources.Storage["session"].Backend.Limits.TimeoutMS = 60000
			c.Execution.MaxRunSeconds = 1
		},
	} {
		c := normalizeManifestContent(compiled.Content)
		mutation(&c)
		_, raw, digest, err := CanonicalizeManifest(c)
		if err != nil {
			continue
		}
		if _, err := ValidateManifestContent(raw, digest); err == nil {
			t.Fatal("accepted modified and rehashed invalid snapshot")
		}
	}
	raw := strings.Replace(string(compiled.CanonicalContent), `"kind":"managed_session"`, `"kind":"managed_session","destination":{}`, 1)
	if _, err := DecodeManifestContent([]byte(raw)); err == nil {
		t.Fatal("accepted inactive destination field")
	}
	raw = strings.Replace(string(compiled.CanonicalContent), `"host":"redis.internal"`, `"host":"redis.internal","password":"secret"`, 1)
	if _, err := DecodeManifestContent([]byte(raw)); err == nil {
		t.Fatal("accepted credential field in backend")
	}
}

func TestUnusedManagedMemoryAndArtifactNeverEnterCompileClosure(t *testing.T) {
	in := managedExecutableInput()
	compiled, report := Compile(in)
	if !report.Valid {
		t.Fatal(report.Diagnostics)
	}
	for _, role := range []string{"memory", "artifact"} {
		if _, ok := compiled.Content.Resources.Storage[role]; ok {
			t.Fatal("unused role exposed", role)
		}
	}
}
