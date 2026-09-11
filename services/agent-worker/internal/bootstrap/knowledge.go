package bootstrap

import (
	"context"
	"errors"
	proof "github.com/liuzengh/trpc-agent-service/api/runtime/execution/v1"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/inbound/httpadapter"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/knowledgestore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/trpcagent"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/application"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/domain"
	"strings"
	"unicode/utf8"
)

type knowledgeQueries struct{ manifests application.ManifestReader }

func (q knowledgeQueries) ImportKnowledge(ctx context.Context, r proof.KnowledgeRequest) (proof.KnowledgeResponse, error) {
	var out proof.KnowledgeResponse
	if r.TenantID == "" || r.ManifestRef == "" || !domain.DigestValid(r.ManifestDigest) || r.DeploymentRevisionID == "" || r.Resource == "" || r.Operation != "import" || !trpcagent.ValidArtifactName(r.Name) || strings.TrimSpace(r.Text) == "" || !utf8.ValidString(r.Text) || strings.ContainsRune(r.Text, 0) {
		return out, httpadapter.ErrArtifactInvalid
	}
	if len(r.Text) > proof.MaxKnowledgeTextBytes {
		return out, httpadapter.ErrArtifactCapacity
	}
	p, err := q.manifests.Resolve(ctx, domain.Route{TenantID: r.TenantID, ManifestRef: r.ManifestRef, ManifestDigest: r.ManifestDigest, DeploymentRevisionID: r.DeploymentRevisionID})
	if errors.Is(err, application.ErrManifestInvalid) || errors.Is(err, application.ErrManifestUnsupported) {
		return out, httpadapter.ErrAttemptDenied
	}
	if errors.Is(err, application.ErrManifestContractMismatch) {
		// Release skew is temporary: the same call succeeds after this Worker
		// runs the release that compiled the Manifest.
		return out, httpadapter.ErrUnavailable
	}
	if err != nil {
		return out, httpadapter.ErrUnavailable
	}
	k := p.Knowledge
	if len(p.Nodes) > 0 {
		// Composite plans carry the exact selected resource union, not the
		// legacy single-leaf pointer or the Profile's full resource catalog.
		k = nil
		if selected, ok := p.Knowledges[r.Resource]; ok {
			k = &selected
		}
	}
	if p.TenantID != r.TenantID || p.ManifestID != r.ManifestRef || p.ManifestDigest != r.ManifestDigest || p.DeploymentRevisionID != r.DeploymentRevisionID || k == nil || k.Resource != r.Resource {
		return out, httpadapter.ErrAttemptDenied
	}
	if int64(len(r.Text)) > k.Backend.Limits.MaxBytes {
		return out, httpadapter.ErrArtifactCapacity
	}
	store, err := knowledgestore.Open(ctx, k.Backend, knowledgestore.EmbedderConfig{Model: k.EmbeddingModel, BaseURL: k.EmbeddingEndpoint, APIKey: r.Credentials.EmbeddingAPIKey, Dimensions: int(k.Dimensions)}, r.Credentials.QdrantAPIKey, knowledgestore.Scope{TenantID: p.TenantID, ProfileID: p.ProfileID, ResourceID: k.Resource})
	if err != nil {
		return out, httpadapter.ErrUnavailable
	}
	defer store.Close()
	imported, err := store.ImportText(ctx, r.Name, r.Text)
	if errors.Is(err, knowledgestore.ErrCapacity) {
		return out, httpadapter.ErrArtifactCapacity
	}
	if err != nil || imported.Documents < 1 {
		return out, httpadapter.ErrUnavailable
	}
	return proof.KnowledgeResponse{Documents: imported.Documents}, nil
}
