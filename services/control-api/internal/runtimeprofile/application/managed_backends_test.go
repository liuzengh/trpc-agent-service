package application_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"strings"
	"testing"
)

type backendStub struct {
	deny  bool
	calls []string
}

func (b *backendStub) CheckBackend(_ context.Context, tenant, id string, rev uint64, role string) error {
	b.calls = append(b.calls, tenant+"/"+id+"/"+role)
	if b.deny || rev != 1 {
		return errors.New("private backend authorization detail")
	}
	return nil
}
func managedCommand() application.SaveCredentialDraftCommand {
	c := modelCredentialCommand(1, "managed-write", "model-secret")
	c.Write.Config.Storage = map[string]application.StorageConfig{"session": {Kind: domain.StorageKindManagedSession, BackendID: "redis", BackendRevision: 1}, "memory": {Kind: domain.StorageKindManagedMemory, BackendID: "pg", BackendRevision: 1}, "artifact": {Kind: domain.StorageKindManagedArtifact, BackendID: "s3", BackendRevision: 1}}
	c.Write.Config.Knowledge = map[string]application.KnowledgeConfig{"docs": {Kind: domain.KnowledgeKindManaged, BackendID: "qdrant", BackendRevision: 1, Embedding: application.EmbeddingConfig{Model: "embed", BaseURL: "https://embed.example", Dimensions: 3}}}
	secret := "embedding-secret"
	c.Write.Credentials["knowledge"] = map[string]map[string]application.CredentialAction{"docs": {"embedding_api_key": {Action: "replace", Value: &secret}}}
	return c
}
func TestManagedProfileSavePublishAndReceipt(t *testing.T) {
	b := &backendStub{}
	h := newCredentialHarnessWithBackend(t, b)
	c := managedCommand()
	h.save(t, c)
	if len(b.calls) != 4 || len(h.store.records) != 2 {
		t.Fatal(b.calls, len(h.store.records))
	}
	spec := h.spec(t)
	if spec.Storage["session"].BackendID != "redis" || spec.Storage["session"].DSNCredentialID != "" {
		t.Fatal(spec.Storage)
	}
	report, err := h.service.ValidateProfileDraft(context.Background(), application.ValidateProfileDraftCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", ExpectedRevision: 2})
	if err != nil || !report.Valid {
		t.Fatal(report, err)
	}
	pub, err := h.service.PublishProfileRevision(context.Background(), application.PublishProfileRevisionCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", ExpectedRevision: 2})
	if err != nil || !pub.Created {
		t.Fatal(pub, err)
	}
	b.deny = true
	if _, err := h.service.SaveCredentialDraft(context.Background(), c); err != nil {
		t.Fatal("receipt replay must precede changed catalog", err)
	}
	replay, err := h.service.PublishProfileRevision(context.Background(), application.PublishProfileRevisionCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", ExpectedRevision: 2})
	if err != nil || replay.Created || replay.Revision.SpecDigest != pub.Revision.SpecDigest {
		t.Fatal("immutable replay", err)
	}
}
func TestManagedBackendDeniedNoMutation(t *testing.T) {
	for _, b := range []*backendStub{nil, {deny: true}} {
		var access application.BackendAccess
		if b != nil {
			access = b
		}
		h := newCredentialHarnessWithBackend(t, access)
		before := h.snapshot(t)
		if _, err := h.service.SaveCredentialDraft(context.Background(), managedCommand()); !errors.Is(err, application.ErrManagedBackend) {
			t.Fatal(err)
		}
		if before != h.snapshot(t) {
			t.Fatal("denied save changed state")
		}
	}
}
func TestManagedWriteRejectsPlatformCredentialsAndInactiveFields(t *testing.T) {
	c := managedCommand()
	raw, err := json.Marshal(c.Write)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := application.DecodeProfileWrite(raw); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"host":"",`, `"destination":{},`, `"dsn_credential_id":"crd_00000000000000000000000000000001",`} {
		bad := strings.Replace(string(raw), `"kind":"managed_session"`, key+`"kind":"managed_session"`, 1)
		if _, err := application.DecodeProfileWrite([]byte(bad)); err == nil {
			t.Fatal("accepted inactive", key)
		}
	}
	secret := "platform-secret"
	c.Write.Credentials["storage"] = map[string]map[string]application.CredentialAction{"session": {"dsn": {Action: "replace", Value: &secret}}}
	raw, _ = json.Marshal(c.Write)
	if _, err := application.DecodeProfileWrite(raw); err == nil {
		t.Fatal("accepted platform DSN")
	}
}

func TestManagedPublishRechecksBackendAfterDraftSave(t *testing.T) {
	b := &backendStub{}
	h := newCredentialHarnessWithBackend(t, b)
	h.save(t, managedCommand())
	before := h.snapshot(t)
	b.deny = true
	report, err := h.service.ValidateProfileDraft(context.Background(), application.ValidateProfileDraftCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", ExpectedRevision: 2})
	if err != nil || report.Valid || len(report.Diagnostics) != 1 || report.Diagnostics[0].Code != "RUNTIME_PROFILE_BACKEND_UNAVAILABLE" {
		t.Fatal(report, err)
	}
	if _, err := h.service.PublishProfileRevision(context.Background(), application.PublishProfileRevisionCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", ExpectedRevision: 2}); !errors.Is(err, application.ErrManagedBackend) {
		t.Fatal(err)
	}
	if before != h.snapshot(t) {
		t.Fatal("failed publish changed draft or credential records")
	}
}
