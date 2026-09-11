package application

import (
	"context"
	"encoding/json"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	agentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/agent/domain"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	profileapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	profiledomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"testing"
)

type knowledgeVaultStub struct {
	calls int
	last  profileapp.OwnerKnowledgeCredentialsCommand
}

func (v *knowledgeVaultStub) ResolveKnowledgeForOwner(_ context.Context, c profileapp.OwnerKnowledgeCredentialsCommand) (profileapp.CredentialBatch, error) {
	v.calls++
	v.last = c
	return profileapp.CredentialBatch{Credentials: []profileapp.ResolvedCredential{{Use: c.Uses[0], Value: []byte("qdrant")}, {Use: c.Uses[1], Value: []byte("embed")}}}, nil
}

type knowledgeBackendStub struct {
	calls int
	last  KnowledgeRequest
}

func (b *knowledgeBackendStub) ImportKnowledge(_ context.Context, c KnowledgeRequest) (KnowledgeResult, error) {
	b.calls++
	b.last = c
	return KnowledgeResult{Documents: 2}, nil
}
func TestKnowledgeImportDerivesFixedPublicationResource(t *testing.T) {
	h := newHarness(t)
	created := h.mustCreate(t)
	var a agentdomain.Spec
	_ = json.Unmarshal(h.agent.version.Spec, &a)
	n := a.Nodes[a.Root]
	n.KnowledgeSlots = []string{"docs"}
	a.Nodes[a.Root] = n
	a.Requirements.Knowledge = map[string]agentdomain.CapabilityRequirement{"docs": {Capability: "knowledge.search"}}
	h.agent.version.Spec, _ = json.Marshal(a)
	b := datav1.Snapshot{SchemaVersion: "v1", TenantID: "tenant-1", BackendID: "qdrant", BackendRevision: 1, Kind: datav1.Qdrant, Adapter: "managed-qdrant-v1", Isolation: "tenant-profile-resource-v1", Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1024}, Qdrant: &datav1.QdrantTarget{Endpoint: "https://qdrant.private", Collection: "docs", VectorName: "content", Dimensions: 3, Distance: "cosine"}}
	digest, _ := b.Digest()
	m := h.profile.revisionSpec.Models["primary"]
	m.Capabilities = []string{"chat", "tool_call"}
	h.profile.revisionSpec.Models["primary"] = m
	h.profile.revisionSpec.Knowledge = map[string]profiledomain.KnowledgeResource{"docs": {Kind: profiledomain.KnowledgeKindManaged, BackendID: b.BackendID, BackendRevision: 1, QdrantAPIKeyCredentialID: "crd_88888888888888888888888888888888", CredentialAudienceDigest: digest, Embedding: profiledomain.EmbeddingResource{Model: "embed", BaseURL: "https://embed.private", Dimensions: 3, APIKeyCredentialID: "crd_99999999999999999999999999999999"}}}
	h.profile.refresh(t)
	p := &h.service.deps.Platform
	p.KnowledgeAdapters[profiledomain.KnowledgeKindManaged] = domain.KnowledgeAdapterContract{Version: domain.KnowledgeAdapterManagedV1, CreatesCallable: true}
	p.Execution.AllowedEndpointHosts = append(p.Execution.AllowedEndpointHosts, "qdrant.private", "embed.private")
	p.Digest, _ = p.CalculateDigest()
	h.service.deps.ManagedBackends = backendMapResolver{"knowledge/docs": b}
	published, err := h.service.PublishDeploymentRevision(context.Background(), PublishDeploymentCommand{TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner", IdempotencyKey: "knowledge", Input: deploymentInput()})
	if err != nil {
		t.Fatal(err)
	}
	vault := &knowledgeVaultStub{}
	backend := &knowledgeBackendStub{}
	h.service.deps.KnowledgeCredentials = vault
	h.service.deps.KnowledgeBackend = backend
	c := KnowledgeImportCommand{TenantID: "tenant-1", DeploymentID: created.ID, ActorUserID: "owner", RevisionNumber: 1, Resource: "docs", Name: "note.txt", Text: "hello"}
	result, err := h.service.ImportKnowledge(context.Background(), c)
	if err != nil || result.Documents != 2 {
		t.Fatal(err)
	}
	if backend.last.ManifestRef != published.Published.Manifest.ID || backend.last.Resource != "docs" || backend.last.Operation != "import" || vault.last.ResourceName != "docs" || vault.last.Uses[0].AudienceDigest != digest {
		t.Fatal("scope was not fixed")
	}
	c.ActorUserID = "member"
	if _, err = h.service.ImportKnowledge(context.Background(), c); err != ErrTenantForbidden || vault.calls != 1 {
		t.Fatal("member", err)
	}
	c.ActorUserID = "owner"
	c.Resource = "other"
	if _, err = h.service.ImportKnowledge(context.Background(), c); err != ErrKnowledgeForbidden {
		t.Fatal("resource", err)
	}
	c.Resource = "docs"
	c.Text = string(make([]byte, 1025))
	if _, err = h.service.ImportKnowledge(context.Background(), c); err != ErrKnowledgeInvalid {
		t.Fatal("NUL", err)
	}
}
