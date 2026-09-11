package bootstrap

import (
	"context"
	"errors"
	executionv1 "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	deploymentapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	deploymentdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	profileapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/runtimeprofile/application"
)

type finalProofVerifier interface {
	VerifyFinal(context.Context, executionv1.FinalRequest) (executionv1.FinalResponse, error)
}
type finalManifestReader interface {
	GetPublishedRevisionByManifest(context.Context, string, string) (deploymentdomain.PublishedRevision, error)
}
type finalArtifactAuthorization struct {
	verifier  finalProofVerifier
	manifests finalManifestReader
}

func (a finalArtifactAuthorization) AuthorizeFinalArtifact(ctx context.Context, worker string, in profileapp.ResolveFinalArtifactInput) ([]profileapp.CredentialUse, error) {
	deny := profileapp.ErrExecutionUnauthorized
	if worker == "" || in.Validate() != nil || a.verifier == nil || a.manifests == nil {
		return nil, deny
	}
	proof, e := a.verifier.VerifyFinal(ctx, in.Final)
	if e != nil {
		return nil, e
	}
	if proof.Validate() != nil || proof.FinalRequest != in.Final || proof.TenantID != in.TenantID || proof.ManifestDigest != in.ManifestDigest {
		return nil, deny
	}
	p, e := a.manifests.GetPublishedRevisionByManifest(ctx, in.TenantID, in.ManifestID)
	if errors.Is(e, deploymentapp.ErrDeploymentRevisionNotFound) {
		return nil, deny
	}
	if e != nil {
		return nil, profileapp.ErrExecutionDependencyUnavailable
	}
	if p.Manifest.ID != in.ManifestID || p.Manifest.TenantID != in.TenantID || p.Manifest.DeploymentRevisionID != in.DeploymentRevisionID || p.Manifest.ContentDigest != in.ManifestDigest || p.Revision.ID != in.DeploymentRevisionID || p.Revision.TenantID != in.TenantID || p.Revision.DeploymentID != in.DeploymentID {
		return nil, deny
	}
	content, e := deploymentdomain.ValidateManifestContent(p.Manifest.Content, p.Manifest.ContentDigest)
	if e != nil {
		return nil, deny
	}
	if content.TenantID != in.TenantID || content.Sources.Profile.ProfileID != in.ProfileID || content.Sources.Profile.RevisionNumber != in.ProfileRevisionNumber {
		return nil, deny
	}
	r, ok := content.Resources.Storage["artifact"]
	if !ok || content.StorageRoles["artifact"] != "artifact" || r.Kind != "managed_artifact" || r.Credentials == nil || r.Backend == nil {
		return nil, deny
	}
	enabled := false
	for _, n := range content.AgentPlan.Nodes {
		if n.Artifact != nil && n.Artifact.Enabled && n.Artifact.Resource == "artifact" {
			enabled = true
		}
	}
	if !enabled {
		return nil, deny
	}
	uses := []profileapp.CredentialUse{{CredentialID: r.Credentials.AccessKeyID.CredentialID, Purpose: r.Credentials.AccessKeyID.Purpose, AudienceDigest: r.Credentials.AccessKeyID.AudienceDigest}, {CredentialID: r.Credentials.SecretAccessKey.CredentialID, Purpose: r.Credentials.SecretAccessKey.Purpose, AudienceDigest: r.Credentials.SecretAccessKey.AudienceDigest}}
	return uses, nil
}
