package application_test

import (
	"context"
	"errors"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"testing"
)

type knowledgeTargets struct{ offline bool }

func (*knowledgeTargets) CheckBackend(context.Context, string, string, uint64, string) error {
	return nil
}
func (k *knowledgeTargets) ResolveManagedCredentialAudience(_ context.Context, tenant, id string, revision uint64, role string) (string, error) {
	if k.offline || tenant != "tnt_a" || id != "vectors" || role != "knowledge" {
		return "", errors.New("target unavailable")
	}
	return knowledgeTarget(tenant, id, revision).Digest()
}
func knowledgeTarget(tenant, id string, revision uint64) datav1.Snapshot {
	return datav1.Snapshot{SchemaVersion: "v1", TenantID: tenant, BackendID: id, BackendRevision: revision, Kind: datav1.Qdrant, Adapter: "managed-qdrant-v1", Isolation: "tenant-profile-resource-v1", Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1024}, Qdrant: &datav1.QdrantTarget{Endpoint: "https://qdrant.internal", Collection: "docs", VectorName: "content", Dimensions: 3, Distance: "cosine"}}
}
func knowledgeCommand() application.SaveCredentialDraftCommand {
	c := modelCredentialCommand(1, "knowledge-save", "model-key")
	c.Write.Config.Knowledge = map[string]application.KnowledgeConfig{"docs": {Kind: domain.KnowledgeKindManaged, BackendID: "vectors", BackendRevision: 1, Embedding: application.EmbeddingConfig{Model: "embed", BaseURL: "https://embed.example", Dimensions: 3}}}
	q, e := "qdrant-private", "embed-private"
	c.Write.Credentials["knowledge"] = map[string]map[string]application.CredentialAction{"docs": {"qdrant_api_key": {Action: "replace", Value: &q}, "embedding_api_key": {Action: "replace", Value: &e}}}
	return c
}
func TestManagedKnowledgeCredentialSaveRotateRebind(t *testing.T) {
	targets := &knowledgeTargets{}
	h := newCredentialHarnessWithBackend(t, targets, targets)
	c := knowledgeCommand()
	h.save(t, c)
	k := h.spec(t).Knowledge["docs"]
	digest, _ := knowledgeTarget("tnt_a", "vectors", 1).Digest()
	if k.QdrantAPIKeyCredentialID == "" || k.CredentialAudienceDigest != digest {
		t.Fatal("target binding")
	}
	publishCredentialHarness(t, h)
	read, err := h.service.GetCredentialRevision(context.Background(), "tnt_a", "rpf_a", "usr_owner", 1)
	if err != nil {
		t.Fatal(err)
	}
	uses := []application.CredentialUse{{CredentialID: k.QdrantAPIKeyCredentialID, Purpose: "qdrant_api_key", AudienceDigest: digest}, {CredentialID: k.Embedding.APIKeyCredentialID, Purpose: "embedding_api_key", AudienceDigest: h.store.records[k.Embedding.APIKeyCredentialID].AudienceDigest}}
	request, v := executionForHarness(h, uses)
	consumer := credentialConsumerService(h, v, h.cipher)
	batch, err := consumer.ResolveForAttempt(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	batch.Clear()
	targets.offline = true
	value := "rotated-qdrant"
	state := read.CredentialStates["knowledge"]["docs"]["qdrant_api_key"]
	_, err = h.service.UpdateUsedProfileCredential(context.Background(), application.UpdateUsedCredentialCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", IdempotencyKey: "rotate-qdrant", Update: application.CredentialUpdate{Target: application.PublishedCredentialTarget{ProfileRevisionNumber: 1, Category: "knowledge", ResourceName: "docs", PurposeField: "qdrant_api_key", AssociationToken: state.AssociationToken}, Action: "replace", ExpectedCredentialRevision: 1, Value: &value}})
	if err != nil {
		t.Fatal(err)
	}
	batch, err = consumer.ResolveForAttempt(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	batch.Clear()
	targets.offline = false
	c.Write.ExpectedDraftRevision = 2
	c.IdempotencyKey = "knowledge-keep"
	r := c.Write.Config.Knowledge["docs"]
	r.BackendRevision = 2
	c.Write.Config.Knowledge["docs"] = r
	c.Write.Credentials = application.CredentialActions{}
	if _, err = h.service.SaveCredentialDraft(context.Background(), c); !errors.Is(err, domain.ErrCredentialAssociation) {
		t.Fatal("keep rebound", err)
	}
	c.Write.Credentials = application.CredentialActions{"knowledge": {"docs": {"qdrant_api_key": {Action: "replace", Value: &value}}}}
	h.save(t, c)
	if h.spec(t).Knowledge["docs"].Embedding.APIKeyCredentialID != k.Embedding.APIKeyCredentialID {
		t.Fatal("embedding key rebound to vector target")
	}
	c.Write.ExpectedDraftRevision = 3
	c.IdempotencyKey = "knowledge-clear"
	c.Write.Credentials = application.CredentialActions{"knowledge": {"docs": {"qdrant_api_key": {Action: "clear"}}}}
	targets.offline = true
	h.save(t, c)
	if h.spec(t).Knowledge["docs"].CredentialAudienceDigest != "" {
		t.Fatal("clear")
	}
}

func TestKnowledgeOwnerResolveBindsResource(t *testing.T) {
	targets := &knowledgeTargets{}
	h := newCredentialHarnessWithBackend(t, targets, targets)
	h.save(t, knowledgeCommand())
	publishCredentialHarness(t, h)
	k := h.spec(t).Knowledge["docs"]
	uses := []application.CredentialUse{{CredentialID: k.QdrantAPIKeyCredentialID, Purpose: "qdrant_api_key", AudienceDigest: k.CredentialAudienceDigest}, {CredentialID: k.Embedding.APIKeyCredentialID, Purpose: "embedding_api_key", AudienceDigest: h.store.records[k.Embedding.APIKeyCredentialID].AudienceDigest}}
	c := application.OwnerKnowledgeCredentialsCommand{CheckProfileCredentialsCommand: application.CheckProfileCredentialsCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", ProfileRevisionNumber: 1, Uses: uses}, ResourceName: "docs"}
	targets.offline = true
	b, err := h.service.ResolveKnowledgeForOwner(context.Background(), c)
	if err != nil || len(b.Credentials) != 2 {
		t.Fatal(err)
	}
	b.Clear()
	c.ResourceName = "other"
	if b, err = h.service.ResolveKnowledgeForOwner(context.Background(), c); err == nil || len(b.Credentials) > 0 {
		t.Fatal("resource bypass")
	}
	c.ResourceName = "docs"
	c.ActorUserID = "usr_member"
	if _, err = h.service.ResolveKnowledgeForOwner(context.Background(), c); err == nil {
		t.Fatal("member")
	}
}
