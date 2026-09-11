package postgresadapter_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/adapter/outbound/credentialcrypto"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
)

func TestProfileCredentialConsumerRoundTripAgainstPostgreSQL(t *testing.T) {
	store, _ := credentialPostgres(t)
	ctx := context.Background()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	cipher, err := credentialcrypto.New(key)
	if err != nil {
		t.Fatal(err)
	}
	verifier := &postgresExecutionVerifier{}
	service := application.NewService(application.Dependencies{
		Store: store, Credentials: store, Cipher: cipher, TenantAccess: postgresCredentialAccess{}, OwnerAccess: postgresCredentialAccess{}, ExecutionVerifier: verifier,
		NewProfileID:  func() (string, error) { return "unused-profile", nil },
		NewRevisionID: func() (string, error) { return "credential-revision-roundtrip", nil },
		NewCredentialID: func() (string, error) {
			data := make([]byte, 16)
			if _, err := rand.Read(data); err != nil {
				return "", err
			}
			return "crd_" + hex.EncodeToString(data), nil
		}, Now: time.Now,
	})
	input, err := application.DecodeProfileWrite([]byte(`{
		"expected_draft_revision":1,"credential_protocol_version":"v1",
		"config":{
			"models":{"primary":{"kind":"openai_compatible","model":"test-model","base_url":"https://model.example.test/v1","capabilities":["chat"]}},
			"tools":{"search":{"kind":"mcp_streamable_http","server_url":"https://mcp.example.test/rpc","toolset_name":"web","tool_name":"search_web","auth":{"kind":"bearer"},"capability":"web.search"}},
			"knowledge":{"docs":{"kind":"qdrant_openai","host":"qdrant.example.test","port":6334,"tls":true,"collection":"docs","embedding":{"model":"test-embedding","base_url":"https://embedding.example.test/v1","dimensions":1536}}},
			"storage":{"session":{"kind":"postgres_state","destination":{"host":"state.example.test","port":5432,"database":"agent_state","username":"runtime_user","sslmode":"require"}}}
		},
		"credentials":{
			"models":{"primary":{"api_key":{"action":"replace","value":"roundtrip-private-initial"}}},
			"tools":{"search":{"bearer_token":{"action":"replace","value":"roundtrip-private-mcp"}}},
			"knowledge":{"docs":{"qdrant_api_key":{"action":"replace","value":"roundtrip-private-qdrant"},"embedding_api_key":{"action":"replace","value":"roundtrip-private-embedding"}}},
			"storage":{"session":{"dsn":{"action":"replace","value":"postgres://runtime_user:roundtrip-private-storage@state.example.test:5432/agent_state?sslmode=require"}}}
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.SaveCredentialDraft(ctx, application.SaveCredentialDraftCommand{TenantID: "tenant-a", ProfileID: "profile-a", ActorUserID: "actor", IdempotencyKey: "roundtrip-save", Write: input}); err != nil {
		t.Fatal(err)
	}
	published, err := service.PublishProfileRevision(ctx, application.PublishProfileRevisionCommand{TenantID: "tenant-a", ProfileID: "profile-a", ActorUserID: "actor", ExpectedRevision: 2})
	if err != nil {
		t.Fatal(err)
	}
	var spec domain.Spec
	if err := json.Unmarshal(published.Revision.Spec, &spec); err != nil {
		t.Fatal(err)
	}
	if spec.Storage["session"].Destination != (domain.StorageDestination{Host: "state.example.test", Port: 5432, Database: "agent_state", Username: "runtime_user", SSLMode: "require"}) {
		t.Fatal("DSN non-secret destination was not fixed independently in the published Spec")
	}
	fixtures := []struct {
		id, category, resourceName, purpose, value string
	}{
		{spec.Models["primary"].APIKeyCredentialID, "models", "primary", "api_key", "roundtrip-private-initial"},
		{spec.Tools["search"].Auth.CredentialID, "tools", "search", "bearer_token", "roundtrip-private-mcp"},
		{spec.Knowledge["docs"].QdrantAPIKeyCredentialID, "knowledge", "docs", "qdrant_api_key", "roundtrip-private-qdrant"},
		{spec.Knowledge["docs"].Embedding.APIKeyCredentialID, "knowledge", "docs", "embedding_api_key", "roundtrip-private-embedding"},
		{spec.Storage["session"].DSNCredentialID, "storage", "session", "dsn", "roundtrip-private-storage"},
	}
	uses := make([]application.CredentialUse, 0, len(fixtures))
	if err := store.WithinProfile(ctx, "tenant-a", "profile-a", func(tx application.CredentialTransaction) error {
		for _, fixture := range fixtures {
			record, err := tx.GetCredential(ctx, fixture.id)
			if err != nil {
				return err
			}
			if record.Category != fixture.category || record.ResourceName != fixture.resourceName || record.Purpose != fixture.purpose || len(record.Ciphertext) == 0 || strings.Contains(string(record.Ciphertext), fixture.value) {
				t.Fatalf("stored credential has wrong scope, purpose or plaintext: %s/%s/%s", fixture.category, fixture.resourceName, fixture.purpose)
			}
			uses = append(uses, application.CredentialUse{CredentialID: record.ID, Purpose: record.Purpose, AudienceDigest: record.AudienceDigest})
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := service.CheckUsable(ctx, application.CheckProfileCredentialsCommand{TenantID: "tenant-a", ProfileID: "profile-a", ActorUserID: "actor", ProfileRevisionNumber: published.Revision.RevisionNumber, Uses: uses}); err != nil {
		t.Fatal(err)
	}
	request := application.ExecutionAuthorizationRequest{WorkloadIdentity: "worker-one", ExecutionToken: "strict-attempt-one", ManifestID: "manifest-one", ManifestDigest: "sha256:" + strings.Repeat("a", 64)}
	verifier.request = request
	verifier.authorization = application.ExecutionAuthorization{TenantID: "tenant-a", ProfileID: "profile-a", ProfileRevisionNumber: published.Revision.RevisionNumber, RunID: "run-one", AttemptID: "attempt-one", WorkerID: "worker-one", LeaseEpoch: 1, ExpiresAt: time.Now().Add(time.Minute), ManifestID: request.ManifestID, ManifestDigest: request.ManifestDigest, AllowedUses: uses}
	command := application.ResolveAttemptCommand{Authorization: request, Uses: uses}
	first, err := service.ResolveForAttempt(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Clear()
	if len(first.Credentials) != len(fixtures) {
		t.Fatal("database/cipher/consumer did not reconstruct all five credential uses")
	}
	for i, credential := range first.Credentials {
		if credential.Use != uses[i] || string(credential.Value) != fixtures[i].value || credential.CredentialRevision != 1 {
			t.Fatalf("resolved %s credential lost its scope, purpose, value or revision", fixtures[i].purpose)
		}
	}
	view, err := service.GetCredentialRevision(ctx, "tenant-a", "profile-a", "actor", published.Revision.RevisionNumber)
	if err != nil {
		t.Fatal(err)
	}
	value := "roundtrip-private-rotated"
	update := application.UpdateUsedCredentialCommand{TenantID: "tenant-a", ProfileID: "profile-a", ActorUserID: "actor", IdempotencyKey: "roundtrip-rotate", Update: application.CredentialUpdate{Target: application.PublishedCredentialTarget{ProfileRevisionNumber: published.Revision.RevisionNumber, Category: "models", ResourceName: "primary", PurposeField: "api_key", AssociationToken: view.CredentialStates["models"]["primary"]["api_key"].AssociationToken}, Action: "replace", ExpectedCredentialRevision: 1, Value: &value}}
	if _, err := service.UpdateUsedProfileCredential(ctx, update); err != nil {
		t.Fatal(err)
	}
	command.Authorization.ExecutionToken = "strict-attempt-two"
	verifier.request = command.Authorization
	verifier.authorization.AttemptID = "attempt-two"
	verifier.authorization.LeaseEpoch = 2
	second, err := service.ResolveForAttempt(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Clear()
	if string(second.Credentials[0].Value) != value || second.Credentials[0].CredentialRevision != 2 || string(first.Credentials[0].Value) != "roundtrip-private-initial" {
		t.Fatal("new attempt did not observe rotation independently")
	}
	for i := 1; i < len(fixtures); i++ {
		if second.Credentials[i].Use != uses[i] || string(second.Credentials[i].Value) != fixtures[i].value || second.Credentials[i].CredentialRevision != 1 {
			t.Fatalf("model rotation changed unrelated %s credential", fixtures[i].purpose)
		}
	}
	update.IdempotencyKey = "roundtrip-clear"
	update.Update.Action = "clear"
	update.Update.ExpectedCredentialRevision = 2
	update.Update.Value = nil
	if _, err := service.UpdateUsedProfileCredential(ctx, update); err != nil {
		t.Fatal(err)
	}
	command.Authorization.ExecutionToken = "strict-attempt-three"
	verifier.request = command.Authorization
	verifier.authorization.AttemptID = "attempt-three"
	verifier.authorization.LeaseEpoch = 3
	if batch, err := service.ResolveForAttempt(ctx, command); !errors.Is(err, domain.ErrCredentialUnavailable) || len(batch.Credentials) != 0 {
		t.Fatalf("cleared credential resolved: %v", err)
	}
	unchanged, err := service.GetCredentialRevision(ctx, "tenant-a", "profile-a", "actor", published.Revision.RevisionNumber)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.SpecDigest != published.Revision.SpecDigest || string(second.Credentials[0].Value) != value {
		t.Fatal("clear mutated snapshot or previously initialized batch")
	}
}

type postgresCredentialAccess struct{}

func (postgresCredentialAccess) IsActiveMember(_ context.Context, tenant, actor string) (bool, error) {
	return tenant == "tenant-a" && actor == "actor", nil
}
func (postgresCredentialAccess) IsActiveOwner(_ context.Context, tenant, actor string) (bool, error) {
	return tenant == "tenant-a" && actor == "actor", nil
}

// No inference from untrusted fields: this fake stands in only for the future
// execution owner and returns a preconfigured exact current attempt grant.
type postgresExecutionVerifier struct {
	request       application.ExecutionAuthorizationRequest
	authorization application.ExecutionAuthorization
}

func (v *postgresExecutionVerifier) VerifyAttempt(_ context.Context, request application.ExecutionAuthorizationRequest) (application.ExecutionAuthorization, error) {
	if request != v.request {
		return application.ExecutionAuthorization{}, application.ErrExecutionUnauthorized
	}
	auth := v.authorization
	auth.AllowedUses = append([]application.CredentialUse(nil), auth.AllowedUses...)
	return auth, nil
}
