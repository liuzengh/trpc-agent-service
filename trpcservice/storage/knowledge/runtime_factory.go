package knowledge

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	serviceqdrant "github.com/liuzengh/trpc-agent-service/trpcservice/storage/knowledge/qdrant"
	upstream "trpc.group/trpc-go/trpc-agent-go/knowledge"
)

// ScopedKnowledgeFactory creates one immutable framework Knowledge instance
// per published Knowledge ref. The service owns version admission and scope;
// trpc-agent-go owns embedding, retrieval and reranking.
type ScopedKnowledgeFactory interface {
	KnowledgeFor(context.Context, string, profile.VersionedRef, int64) (upstream.Knowledge, error)
}

// RuntimeFactory joins the published ingestion manifest, the profile-pinned
// framework Embedder and the read-only VectorStore facade over the same Qdrant
// collection used by durable ingestion/migration.
type RuntimeFactory struct {
	// Adapter is a compatibility seam for a fixed test binding. Production uses
	// Backends so Qdrant routing follows the frozen tenant Config Snapshot.
	Adapter   *serviceqdrant.Adapter
	Backends  KnowledgeBackendResolver
	Manifests IngestionStore
	Embedders EmbedderResolver
}

func (f RuntimeFactory) KnowledgeFor(ctx context.Context, tenantID string, ref profile.VersionedRef, configVersion int64) (upstream.Knowledge, error) {
	if (f.Adapter == nil && f.Backends == nil) || f.Manifests == nil || tenantID == "" || ref.ID == "" || ref.Version < 1 || configVersion < 1 {
		return nil, runtime.ErrCapabilityUnsupported
	}
	adapter := f.Adapter
	if f.Backends != nil {
		var err error
		adapter, err = f.Backends.ResolveKnowledgeBackend(ctx, tenantID, configVersion)
		if err != nil {
			return nil, err
		}
	}
	if adapter == nil {
		return nil, runtime.ErrCapabilityUnsupported
	}
	manifest, err := f.Manifests.GetManifest(ctx, tenantID, ref.ID, ref.Version)
	if err != nil {
		return nil, err
	}
	if manifest.TenantID != tenantID || manifest.KnowledgeID != ref.ID || manifest.Version != ref.Version {
		return nil, runtime.ErrTenantScope
	}
	if manifest.State != ManifestPublished || manifest.EmbedderProfileID == "" || manifest.EmbedderVersion < 1 ||
		manifest.VectorCollectionGeneration == "" || manifest.VectorCollectionGeneration != adapter.VectorGeneration() {
		return nil, runtime.ErrCapabilityUnsupported
	}
	embedder, err := f.Embedders.Resolve(ctx, tenantID, manifest.EmbedderProfileID, manifest.EmbedderVersion)
	if err != nil {
		return nil, err
	}
	if embedder == nil || embedder.GetDimensions() != adapter.VectorSize() {
		return nil, runtime.ErrVersionMismatch
	}
	store, err := serviceqdrant.NewReadOnlyVectorStore(adapter, serviceqdrant.RuntimeScope{
		TenantID: tenantID, KnowledgeID: ref.ID, KnowledgeVersion: ref.Version,
		EmbedderProfile: manifest.EmbedderProfileID, EmbedderVersion: manifest.EmbedderVersion,
		VectorGeneration: manifest.VectorCollectionGeneration,
	})
	if err != nil {
		return nil, err
	}
	return upstream.New(upstream.WithVectorStore(store), upstream.WithEmbedder(embedder)), nil
}

var _ ScopedKnowledgeFactory = RuntimeFactory{}
