package application_test

import (
	"bytes"
	"context"
	"encoding/json"
	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/domain"
	"strings"
	"testing"
)

type artifactFinalAuth struct {
	uses []application.CredentialUse
	err  error
}

func (a *artifactFinalAuth) AuthorizeFinalArtifact(context.Context, string, application.ResolveFinalArtifactInput) ([]application.CredentialUse, error) {
	return a.uses, a.err
}
func finalInput() application.ResolveFinalArtifactInput {
	return application.ResolveFinalArtifactInput{Final: executionv1.FinalRequest{IntentID: "intent", Digest: "sha256:" + strings.Repeat("a", 64), AdmissionID: "admission", RunID: "run", AttemptID: "attempt", CompletionID: "completion", ExecutionGeneration: 1, Sequence: 1}, TenantID: "tnt_a", ManifestID: "manifest", ManifestDigest: "sha256:" + strings.Repeat("b", 64), DeploymentID: "deployment", DeploymentRevisionID: "revision", ProfileID: "rpf_a", ProfileRevisionNumber: 1}
}
func TestFinalArtifactResolveOnlyFixedArtifactCredentials(t *testing.T) {
	targets := &artifactTargets{}
	h := newCredentialHarnessWithBackend(t, targets, targets)
	h.save(t, artifactCommand())
	publishCredentialHarness(t, h)
	r := h.spec(t).Storage["artifact"]
	auth := &artifactFinalAuth{uses: []application.CredentialUse{{CredentialID: r.AccessKeyIDCredentialID, Purpose: "access_key_id", AudienceDigest: r.CredentialAudienceDigest}, {CredentialID: r.SecretAccessKeyCredentialID, Purpose: "secret_access_key", AudienceDigest: r.CredentialAudienceDigest}}}
	s := application.NewService(application.Dependencies{Store: h.store.base, Credentials: h.store, Cipher: h.cipher, OwnerAccess: h.owners, TenantAccess: accessStub{}, NewProfileID: func() (string, error) { return "unused", nil }, NewRevisionID: func() (string, error) { return "unused", nil }, NewCredentialID: func() (string, error) { return "unused", nil }, FinalArtifacts: auth})
	b, e := s.ResolveForFinalArtifact(context.Background(), "worker", finalInput())
	if e != nil || len(b.Credentials) != 2 || b.LeaseEpoch != 0 {
		t.Fatal(e, b)
	}
	b.Clear()
	for _, c := range b.Credentials {
		if len(c.Value) > 0 {
			t.Fatal("not cleared")
		}
	}
	originalRecord := h.store.records[r.AccessKeyIDCredentialID]
	cleared := originalRecord
	cleared.Status = domain.CredentialCleared
	h.store.records[r.AccessKeyIDCredentialID] = cleared
	if b, e := s.ResolveForFinalArtifact(context.Background(), "worker", finalInput()); e == nil || len(b.Credentials) > 0 {
		t.Fatal("cleared credential accepted")
	}
	h.store.records[r.AccessKeyIDCredentialID] = originalRecord
	auth.uses[0].Purpose = "api_key"
	if b, e := s.ResolveForFinalArtifact(context.Background(), "worker", finalInput()); e == nil || len(b.Credentials) > 0 {
		t.Fatal("model key accepted")
	}
	auth.err = application.ErrExecutionUnauthorized
	if _, e := s.ResolveForFinalArtifact(context.Background(), "worker", finalInput()); e == nil {
		t.Fatal("denial ignored")
	}
}
func TestFinalArtifactDecodeClosed(t *testing.T) {
	b, _ := json.Marshal(finalInput())
	if _, e := application.DecodeResolveFinalArtifact(b); e != nil {
		t.Fatal(e)
	}
	for _, extra := range []string{`"uses":[],`, `"execution_token":"old",`, `"worker_id":"spoof",`} {
		bad := bytes.Replace(b, []byte(`{"final":`), []byte(`{`+extra+`"final":`), 1)
		if _, e := application.DecodeResolveFinalArtifact(bad); e == nil {
			t.Fatal("accepted", extra)
		}
	}
	bad := bytes.Replace(b, []byte(`"final":{`), []byte(`"final":null,"extra":{`), 1)
	if _, e := application.DecodeResolveFinalArtifact(bad); e == nil {
		t.Fatal("null")
	}
}
