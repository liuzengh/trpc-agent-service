package runtimeadapter

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/artifactstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/trpcagent"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
)

func validateArtifactPlan(p domain.Plan) error {
	a := p.Artifact
	if a == nil {
		return nil
	}
	d, err := a.Backend.Digest()
	if err != nil || a.Backend.ValidateForRole("artifact") != nil || a.Backend.TenantID != p.TenantID || p.MaxToolCalls < 1 {
		return application.ErrManifestInvalid
	}
	for purpose, u := range map[string]domain.CredentialUse{"access_key_id": a.AccessKeyID, "secret_access_key": a.SecretAccessKey} {
		if u.CredentialID == "" || u.Purpose != purpose || u.AudienceDigest != d {
			return application.ErrManifestInvalid
		}
	}
	return nil
}
func (f *Factory) prepareArtifact(ctx context.Context, g domain.Grant, p domain.Plan, batch map[domain.CredentialUse]string) (*artifactstore.Store, error) {
	a := p.Artifact
	s, err := artifactstore.Open(ctx, f.options.ArtifactPool, a.Backend, artifactstore.Credentials{AccessKeyID: batch[a.AccessKeyID], SecretAccessKey: batch[a.SecretAccessKey]}, p.TenantID, trpcagent.ArtifactSessionInfo(p.TenantID, g.Run.SessionID))
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, application.ErrDependency
	}
	return s, nil
}
