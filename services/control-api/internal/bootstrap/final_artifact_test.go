package bootstrap

import (
	"context"
	"crypto/sha256"
	"fmt"
	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	d "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	profileapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
	"os"
	"strings"
	"testing"
)

type finalVerifierStub struct {
	proof executionv1.FinalResponse
	err   error
}

func (s finalVerifierStub) VerifyFinal(context.Context, executionv1.FinalRequest) (executionv1.FinalResponse, error) {
	return s.proof, s.err
}

type finalReaderStub struct {
	p     d.PublishedRevision
	calls *int
}

func (s finalReaderStub) GetPublishedRevisionByManifest(context.Context, string, string) (d.PublishedRevision, error) {
	*s.calls++
	return s.p, nil
}
func TestFinalArtifactAuthorizationExactImmutableBinding(t *testing.T) {
	b, e := os.ReadFile("testdata/final-artifact-manifest.json")
	if e != nil {
		t.Fatal(e)
	}
	sum := sha256.Sum256(b)
	digest := fmt.Sprintf("sha256:%x", sum)
	content, e := d.ValidateManifestContent(b, digest)
	if e != nil {
		t.Fatal(e)
	}
	in := profileapp.ResolveFinalArtifactInput{Final: executionv1.FinalRequest{IntentID: "intent", Digest: "sha256:" + strings.Repeat("a", 64), AdmissionID: "admission", RunID: "run", AttemptID: "attempt", CompletionID: "completion", ExecutionGeneration: 1, Sequence: 1}, TenantID: content.TenantID, ManifestID: "manifest", ManifestDigest: digest, DeploymentID: "deployment", DeploymentRevisionID: "revision", ProfileID: content.Sources.Profile.ProfileID, ProfileRevisionNumber: content.Sources.Profile.RevisionNumber}
	p := d.PublishedRevision{Revision: d.DeploymentRevision{ID: in.DeploymentRevisionID, TenantID: in.TenantID, DeploymentID: in.DeploymentID}, Manifest: d.RuntimeManifest{ID: in.ManifestID, TenantID: in.TenantID, DeploymentRevisionID: in.DeploymentRevisionID, ContentDigest: digest, Content: b}}
	proof := executionv1.FinalResponse{FinalRequest: in.Final, TenantID: in.TenantID, ManifestDigest: digest}
	calls := 0
	a := finalArtifactAuthorization{verifier: finalVerifierStub{proof: proof}, manifests: finalReaderStub{p: p, calls: &calls}}
	uses, e := a.AuthorizeFinalArtifact(context.Background(), "worker", in)
	if e != nil || len(uses) != 2 || uses[0].Purpose != "access_key_id" || uses[1].Purpose != "secret_access_key" {
		t.Fatal(e, uses)
	}
	for name, mutate := range map[string]func(*profileapp.ResolveFinalArtifactInput){"tenant": func(i *profileapp.ResolveFinalArtifactInput) { i.TenantID = "other" }, "manifest": func(i *profileapp.ResolveFinalArtifactInput) { i.ManifestID = "other" }, "digest": func(i *profileapp.ResolveFinalArtifactInput) { i.ManifestDigest = "sha256:" + strings.Repeat("b", 64) }, "deployment": func(i *profileapp.ResolveFinalArtifactInput) { i.DeploymentID = "other" }, "revision": func(i *profileapp.ResolveFinalArtifactInput) { i.DeploymentRevisionID = "other" }, "profile": func(i *profileapp.ResolveFinalArtifactInput) { i.ProfileID = "other" }, "profile rev": func(i *profileapp.ResolveFinalArtifactInput) { i.ProfileRevisionNumber++ }, "attempt": func(i *profileapp.ResolveFinalArtifactInput) { i.Final.AttemptID = "other" }, "completion": func(i *profileapp.ResolveFinalArtifactInput) { i.Final.CompletionID = "other" }} {
		t.Run(name, func(t *testing.T) {
			bad := in
			mutate(&bad)
			if _, e := a.AuthorizeFinalArtifact(context.Background(), "worker", bad); e == nil {
				t.Fatal("accepted mismatch")
			}
		})
	}
	calls = 0
	a.verifier = finalVerifierStub{err: profileapp.ErrExecutionUnauthorized}
	if _, e := a.AuthorizeFinalArtifact(context.Background(), "worker", in); e == nil || calls != 0 {
		t.Fatal("unproven Final read database")
	}
}
