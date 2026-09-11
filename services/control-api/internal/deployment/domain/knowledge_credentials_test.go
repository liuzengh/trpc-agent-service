package domain

import (
	"encoding/json"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"testing"
)

func knowledgeCredentialInput() CompileInput {
	in := workerSummaryInput()
	n := in.Agent.Spec.Nodes["assistant"]
	n.KnowledgeSlots = []string{"docs"}
	in.Agent.Spec.Nodes["assistant"] = n
	in.Agent.Spec.Requirements.Knowledge = map[string]agentdomain.CapabilityRequirement{"docs": {Capability: "knowledge.search"}}
	m := in.Profile.Spec.Models["primary"]
	m.Capabilities = []string{"chat", "tool_call"}
	in.Profile.Spec.Models["primary"] = m
	b := datav1.Snapshot{SchemaVersion: "v1", TenantID: in.TenantID, BackendID: "vectors", BackendRevision: 1, Kind: datav1.Qdrant, Adapter: "managed-qdrant-v1", Isolation: "tenant-profile-resource-v1", Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1024}, Qdrant: &datav1.QdrantTarget{Endpoint: "https://qdrant.internal", Collection: "docs", VectorName: "content", Dimensions: 3, Distance: "cosine"}}
	digest, _ := b.Digest()
	in.ManagedBackends = map[string]datav1.Snapshot{"knowledge/docs": b}
	in.Profile.Spec.Knowledge = map[string]profiledomain.KnowledgeResource{"docs": {Kind: profiledomain.KnowledgeKindManaged, BackendID: b.BackendID, BackendRevision: 1, QdrantAPIKeyCredentialID: credentialMemory, CredentialAudienceDigest: digest, Embedding: profiledomain.EmbeddingResource{Model: "embed", BaseURL: "https://embed.example", Dimensions: 3, APIKeyCredentialID: "crd_99999999999999999999999999999999"}}}
	in.Platform.Execution.AllowedEndpointHosts = append(in.Platform.Execution.AllowedEndpointHosts, "qdrant.internal", "embed.example")
	in.Platform.Digest, _ = in.Platform.CalculateDigest()
	return in
}
func TestManagedKnowledgeCompiledCredentialClosure(t *testing.T) {
	in := knowledgeCredentialInput()
	m, r := Compile(in)
	if !r.Valid {
		t.Fatal(r.Diagnostics)
	}
	k := m.Content.Resources.Knowledge["docs"]
	digest, _ := k.Backend.Digest()
	if k.Credential == nil || k.Credential.Purpose != CredentialPurposeQdrantAPIKey || k.Credential.AudienceDigest != digest || k.Embedding.Credential.AudienceDigest != audienceDigest(profiledomain.KnowledgeKindManaged, "https://embed.example") {
		t.Fatal("knowledge credentials")
	}
	if len(m.CredentialUses) != 5 {
		t.Fatal("uses", m.CredentialUses)
	}
	if _, err := ValidateManifestContent(m.CanonicalContent, m.ContentDigest); err != nil {
		t.Fatal(err)
	}
	wire, err := deploymentv1.DecodeManifestContent(m.CanonicalContent)
	if err != nil {
		t.Fatal(err)
	}
	if err = deploymentv1.ValidateWorkerV1(wire, in.Platform.Digest); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*datav1.QdrantTarget){func(q *datav1.QdrantTarget) { q.Endpoint = "https://other.internal" }, func(q *datav1.QdrantTarget) { q.Collection = "other" }, func(q *datav1.QdrantTarget) { q.VectorName = "other" }, func(q *datav1.QdrantTarget) { q.Dimensions = 4 }} {
		c := normalizeManifestContent(m.Content)
		mutate(c.Resources.Knowledge["docs"].Backend.Qdrant)
		_, raw, d, err := CanonicalizeManifest(c)
		if err != nil {
			continue
		}
		if _, err = ValidateManifestContent(raw, d); err == nil {
			t.Fatal("target mutation")
		}
	}
	k.Credential = nil
	m.Content.Resources.Knowledge["docs"] = k
	_, raw, d, err := CanonicalizeManifest(m.Content)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ValidateManifestContent(raw, d); err != nil {
		t.Fatal("historical read", err)
	}
	wire, err = deploymentv1.DecodeManifestContent(raw)
	if err != nil {
		t.Fatal(err)
	}
	if deploymentv1.ValidateWorkerV1(wire, "") == nil {
		t.Fatal("historical executable")
	}
	var tree map[string]any
	_ = json.Unmarshal(raw, &tree)
	tree["resources"].(map[string]any)["knowledge"].(map[string]any)["docs"].(map[string]any)["credential"] = nil
	bad, _ := json.Marshal(tree)
	if _, err = DecodeManifestContent(bad); err == nil {
		t.Fatal("null credential accepted")
	}
}
func TestManagedKnowledgeNewCompileRequiresCredential(t *testing.T) {
	in := knowledgeCredentialInput()
	k := in.Profile.Spec.Knowledge["docs"]
	k.QdrantAPIKeyCredentialID = ""
	k.CredentialAudienceDigest = ""
	in.Profile.Spec.Knowledge["docs"] = k
	if m, r := Compile(in); r.Valid || len(m.CanonicalContent) > 0 {
		t.Fatal("missing key compiled")
	}
}
