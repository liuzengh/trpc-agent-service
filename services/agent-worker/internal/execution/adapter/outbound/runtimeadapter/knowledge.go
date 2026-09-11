package runtimeadapter

import (
	"context"
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/knowledgestore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"net/url"
	"strings"
)

func validateKnowledgePlan(p domain.Plan) error {
	k := p.Knowledge
	if k == nil {
		return nil
	}
	b := k.Backend
	d, err := b.Digest()
	ep, e := url.Parse(k.EmbeddingEndpoint)
	if err != nil || b.ValidateForRole("knowledge") != nil || b.TenantID != p.TenantID || k.Resource == "" || p.MaxToolCalls < 1 || k.Dimensions != b.Qdrant.Dimensions || k.EmbeddingModel == "" || k.Credential.CredentialID == "" || k.Credential.Purpose != "qdrant_api_key" || k.Credential.AudienceDigest != d || k.EmbeddingCredential.CredentialID == "" || k.EmbeddingCredential.Purpose != "embedding_api_key" || k.EmbeddingCredential.AudienceDigest != protocol.CredentialAudienceDigest("managed_knowledge", k.EmbeddingEndpoint) || e != nil || (ep.Scheme != "http" && ep.Scheme != "https") || ep.Hostname() == "" || ep.User != nil || ep.RawQuery != "" || ep.ForceQuery || strings.Contains(k.EmbeddingEndpoint, "#") {
		return application.ErrManifestInvalid
	}
	return nil
}
func (f *Factory) prepareKnowledge(ctx context.Context, p domain.Plan, batch map[domain.CredentialUse]string) (*knowledgestore.Store, error) {
	k := p.Knowledge
	s, err := knowledgestore.Open(ctx, k.Backend, knowledgestore.EmbedderConfig{Model: k.EmbeddingModel, BaseURL: k.EmbeddingEndpoint, APIKey: batch[k.EmbeddingCredential], Dimensions: int(k.Dimensions)}, batch[k.Credential], knowledgestore.Scope{TenantID: p.TenantID, ProfileID: p.ProfileID, ResourceID: k.Resource})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, application.ErrDependency
	}
	return s, nil
}
