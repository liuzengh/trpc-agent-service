package application_test

import (
	"context"
	"errors"
	datav1 "github.com/liuzengh/trpc-agent-service/api/runtime/data/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"testing"
)

type artifactTargets struct{ offline bool }

func (*artifactTargets) CheckBackend(context.Context, string, string, uint64, string) error {
	return nil
}
func (a *artifactTargets) ResolveManagedCredentialAudience(_ context.Context, tenant, id string, revision uint64, role string) (string, error) {
	if a.offline || tenant != "tnt_a" || id != "artifacts" || role != "artifact" {
		return "", errors.New("target unavailable")
	}
	return artifactTarget(tenant, id, revision).Digest()
}
func artifactTarget(tenant, id string, revision uint64) datav1.Snapshot {
	return datav1.Snapshot{SchemaVersion: "v1", TenantID: tenant, BackendID: id, BackendRevision: revision, Kind: datav1.S3, Adapter: "managed-s3-v1", Isolation: "tenant-artifact-v1", Limits: datav1.Limits{TimeoutMS: 1000, MaxConcurrency: 1, MaxBytes: 1024}, S3: &datav1.S3Target{Endpoint: "https://s3.internal", Bucket: "artifacts", Region: "local", Versioning: "disabled"}}
}
func artifactCommand() application.SaveCredentialDraftCommand {
	c := modelCredentialCommand(1, "artifact-save", "model-password")
	c.Write.Config.Storage = map[string]application.StorageConfig{"artifact": {Kind: domain.StorageKindManagedArtifact, BackendID: "artifacts", BackendRevision: 1}}
	a, z := "access-private", "secret-private"
	c.Write.Credentials["storage"] = map[string]map[string]application.CredentialAction{"artifact": {"access_key_id": {Action: "replace", Value: &a}, "secret_access_key": {Action: "replace", Value: &z}}}
	return c
}
func TestArtifactPasswordSaveResolveRotateAndRebind(t *testing.T) {
	targets := &artifactTargets{}
	h := newCredentialHarnessWithBackend(t, targets, targets)
	c := artifactCommand()
	h.save(t, c)
	spec := h.spec(t)
	r := spec.Storage["artifact"]
	digest, _ := artifactTarget("tnt_a", "artifacts", 1).Digest()
	if r.AccessKeyIDCredentialID == "" || r.SecretAccessKeyCredentialID == "" || r.CredentialAudienceDigest != digest {
		t.Fatal("association missing")
	}
	publishCredentialHarness(t, h)
	read, err := h.service.GetCredentialRevision(context.Background(), "tnt_a", "rpf_a", "usr_owner", 1)
	if err != nil {
		t.Fatal(err)
	}
	uses := []application.CredentialUse{{CredentialID: r.AccessKeyIDCredentialID, Purpose: "access_key_id", AudienceDigest: digest}, {CredentialID: r.SecretAccessKeyCredentialID, Purpose: "secret_access_key", AudienceDigest: digest}}
	request, verifier := executionForHarness(h, uses)
	consumer := credentialConsumerService(h, verifier, h.cipher)
	batch, err := consumer.ResolveForAttempt(context.Background(), request)
	if err != nil || len(batch.Credentials) != 2 {
		t.Fatal("resolve", err)
	}
	batch.Clear()
	targets.offline = true
	for _, purpose := range []string{"access_key_id", "secret_access_key"} {
		state := read.CredentialStates["storage"]["artifact"][purpose]
		value := "rotated-" + purpose
		_, err = h.service.UpdateUsedProfileCredential(context.Background(), application.UpdateUsedCredentialCommand{TenantID: "tnt_a", ProfileID: "rpf_a", ActorUserID: "usr_owner", IdempotencyKey: "rotate-" + purpose, Update: application.CredentialUpdate{Target: application.PublishedCredentialTarget{ProfileRevisionNumber: 1, Category: "storage", ResourceName: "artifact", PurposeField: purpose, AssociationToken: state.AssociationToken}, Action: "replace", ExpectedCredentialRevision: 1, Value: &value}})
		if err != nil {
			t.Fatal(err)
		}
	}
	batch, err = consumer.ResolveForAttempt(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range batch.Credentials {
		if string(v.Value) != "rotated-"+v.Use.Purpose {
			t.Fatal("rotation")
		}
	}
	batch.Clear()
	targets.offline = false
	c.Write.ExpectedDraftRevision = 2
	c.IdempotencyKey = "new-target"
	cfg := c.Write.Config.Storage["artifact"]
	cfg.BackendRevision = 2
	c.Write.Config.Storage["artifact"] = cfg
	c.Write.Credentials = application.CredentialActions{}
	if _, err = h.service.SaveCredentialDraft(context.Background(), c); !errors.Is(err, domain.ErrCredentialAssociation) {
		t.Fatal("keep changed target", err)
	}
	a, z := "new-access", "new-secret"
	c.Write.Credentials = application.CredentialActions{"storage": {"artifact": {"access_key_id": {Action: "replace", Value: &a}, "secret_access_key": {Action: "replace", Value: &z}}}}
	h.save(t, c)
	c.Write.ExpectedDraftRevision = 3
	c.IdempotencyKey = "clear-one"
	c.Write.Credentials = application.CredentialActions{"storage": {"artifact": {"secret_access_key": {Action: "clear"}}}}
	h.save(t, c)
	if r := h.spec(t).Storage["artifact"]; r.AccessKeyIDCredentialID == "" || r.SecretAccessKeyCredentialID != "" {
		t.Fatal("partial draft clear")
	}
	c.Write.ExpectedDraftRevision = 4
	c.IdempotencyKey = "clear-all"
	c.Write.Credentials = application.CredentialActions{"storage": {"artifact": {"access_key_id": {Action: "clear"}}}}
	targets.offline = true
	h.save(t, c)
	if h.spec(t).Storage["artifact"].CredentialAudienceDigest != "" {
		t.Fatal("clear retained target")
	}
}
