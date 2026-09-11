package bootstrap

import (
	"context"

	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/artifactstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/finalartifacthttp"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	manifest "github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/manifest/domain"
)

type replyArtifactPublicationReader interface {
	Read(context.Context, string, string) (manifest.Publication, error)
}
type finalArtifactResolver interface {
	Resolve(context.Context, finalartifacthttp.Request, []domain.CredentialUse) (artifactstore.Credentials, error)
}
type replyArtifactCredentialResolver struct {
	projection replyArtifactPublicationReader
	client     finalArtifactResolver
}

func (q replyArtifactCredentialResolver) ResolveReplyArtifact(ctx context.Context, r proof.ReplyArtifactRequest, accepted domain.AcceptedArtifact, p domain.Plan) (artifactstore.Credentials, error) {
	var zero artifactstore.Credentials
	run, f := accepted.Run, accepted.Final
	route := run.Request.Route
	final := proof.FinalRequest{IntentID: f.IntentID, Digest: f.Digest, AdmissionID: f.AdmissionID, RunID: f.RunID, AttemptID: f.AttemptID, CompletionID: f.CompletionID, ExecutionGeneration: f.ExecutionGeneration, Sequence: f.Sequence}
	if r.Validate() != nil || final.Validate() != nil || r.IntentID != f.IntentID || r.RunID != f.RunID || r.CompletionID != f.CompletionID || accepted.Attachment.Name != r.Name || accepted.Attachment.Version != r.Version || run.Status != domain.Succeeded || run.Request.RunID != f.RunID || run.Request.AdmissionID != f.AdmissionID || run.CurrentAttemptID != f.AttemptID || run.Generation != f.ExecutionGeneration || route.TenantID != f.TenantID || route.ManifestDigest != f.ManifestDigest || p.TenantID != route.TenantID || p.ManifestID != route.ManifestRef || p.ManifestDigest != route.ManifestDigest || p.DeploymentRevisionID != route.DeploymentRevisionID || p.Artifact == nil {
		return zero, finalartifacthttp.ErrDenied
	}
	if q.projection == nil || q.client == nil {
		return zero, finalartifacthttp.ErrUnavailable
	}
	publication, err := q.projection.Read(ctx, route.TenantID, route.ManifestRef)
	if err != nil {
		return zero, finalartifacthttp.ErrUnavailable
	}
	if publication.TenantID != route.TenantID || publication.ManifestID != route.ManifestRef || publication.DeploymentRevisionID != route.DeploymentRevisionID || publication.ContentDigest != route.ManifestDigest || domain.Digest(publication.Envelope) != publication.EnvelopeDigest {
		return zero, finalartifacthttp.ErrDenied
	}
	envelope, err := protocol.DecodeRuntimeManifest(publication.Envelope)
	if err != nil || envelope.ID != p.ManifestID || envelope.TenantID != p.TenantID || envelope.DeploymentRevisionID != p.DeploymentRevisionID || envelope.ContentDigest != p.ManifestDigest {
		return zero, finalartifacthttp.ErrDenied
	}
	content, err := protocol.VerifyRuntimeManifest(envelope)
	if err != nil || content.Sources.Profile.ProfileID != p.ProfileID || content.Sources.Profile.RevisionNumber != p.ProfileRevision {
		return zero, finalartifacthttp.ErrDenied
	}
	resource, ok := content.Resources.Storage[content.StorageRoles["artifact"]]
	if !ok || resource.Credentials == nil || resource.Backend == nil {
		return zero, finalartifacthttp.ErrDenied
	}
	use := func(v protocol.CredentialUse) domain.CredentialUse {
		return domain.CredentialUse{CredentialID: v.CredentialID, Purpose: v.Purpose, AudienceDigest: v.AudienceDigest}
	}
	publishedDigest, err := resource.Backend.Digest()
	planDigest, planErr := p.Artifact.Backend.Digest()
	if err != nil || planErr != nil || publishedDigest != planDigest || use(resource.Credentials.AccessKeyID) != p.Artifact.AccessKeyID || use(resource.Credentials.SecretAccessKey) != p.Artifact.SecretAccessKey {
		return zero, finalartifacthttp.ErrDenied
	}
	request := finalartifacthttp.Request{Final: final, TenantID: p.TenantID, ManifestID: p.ManifestID, ManifestDigest: p.ManifestDigest, DeploymentID: envelope.DeploymentID, DeploymentRevisionID: p.DeploymentRevisionID, ProfileID: p.ProfileID, ProfileRevisionNumber: p.ProfileRevision}
	return q.client.Resolve(ctx, request, []domain.CredentialUse{p.Artifact.AccessKeyID, p.Artifact.SecretAccessKey})
}
