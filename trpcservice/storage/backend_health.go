package storage

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/backendhealth"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

type healthArtifactService struct {
	agentartifact.Service
	registry *backendhealth.Registry
	key      backendhealth.Key
}

func observeArtifactService(service agentartifact.Service, registry *backendhealth.Registry, key backendhealth.Key) agentartifact.Service {
	if service == nil || registry == nil || !backendhealth.ShouldProtect(key.Driver) {
		return service
	}
	return &healthArtifactService{Service: service, registry: registry, key: key}
}

func (s *healthArtifactService) SaveArtifact(ctx context.Context, info agentartifact.SessionInfo, filename string, artifact *agentartifact.Artifact) (int, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "save_artifact", func(ctx context.Context) (int, error) {
		return s.Service.SaveArtifact(ctx, info, filename, artifact)
	})
}

func (s *healthArtifactService) LoadArtifact(ctx context.Context, info agentartifact.SessionInfo, filename string, version *int) (*agentartifact.Artifact, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "load_artifact", func(ctx context.Context) (*agentartifact.Artifact, error) {
		return s.Service.LoadArtifact(ctx, info, filename, version)
	})
}

func (s *healthArtifactService) ListArtifactKeys(ctx context.Context, info agentartifact.SessionInfo) ([]string, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "list_artifacts", func(ctx context.Context) ([]string, error) {
		return s.Service.ListArtifactKeys(ctx, info)
	})
}

func (s *healthArtifactService) DeleteArtifact(ctx context.Context, info agentartifact.SessionInfo, filename string) error {
	return backendhealth.Do(ctx, s.registry, s.key, "delete_artifact", func(ctx context.Context) error {
		return s.Service.DeleteArtifact(ctx, info, filename)
	})
}

func (s *healthArtifactService) ListVersions(ctx context.Context, info agentartifact.SessionInfo, filename string) ([]int, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "list_artifact_versions", func(ctx context.Context) ([]int, error) {
		return s.Service.ListVersions(ctx, info, filename)
	})
}

type healthVectorStore struct {
	vectorstore.VectorStore
	registry *backendhealth.Registry
	key      backendhealth.Key
}

func observeVectorStore(store vectorstore.VectorStore, registry *backendhealth.Registry, key backendhealth.Key) vectorstore.VectorStore {
	if store == nil || registry == nil || !backendhealth.ShouldProtect(key.Driver) {
		return store
	}
	return &healthVectorStore{VectorStore: store, registry: registry, key: key}
}

func (s *healthVectorStore) Add(ctx context.Context, doc *document.Document, embedding []float64) error {
	return backendhealth.Do(ctx, s.registry, s.key, "vector_add", func(ctx context.Context) error {
		return s.VectorStore.Add(ctx, doc, embedding)
	})
}

func (s *healthVectorStore) Get(ctx context.Context, id string) (*document.Document, []float64, error) {
	if s.registry == nil {
		return s.VectorStore.Get(ctx, id)
	}
	permit, err := s.registry.Begin(ctx, s.key, "vector_get")
	if err != nil {
		return nil, nil, err
	}
	doc, embedding, err := s.VectorStore.Get(ctx, id)
	permit.Done(err)
	return doc, embedding, err
}

func (s *healthVectorStore) Update(ctx context.Context, doc *document.Document, embedding []float64) error {
	return backendhealth.Do(ctx, s.registry, s.key, "vector_update", func(ctx context.Context) error {
		return s.VectorStore.Update(ctx, doc, embedding)
	})
}

func (s *healthVectorStore) Delete(ctx context.Context, id string) error {
	return backendhealth.Do(ctx, s.registry, s.key, "vector_delete", func(ctx context.Context) error {
		return s.VectorStore.Delete(ctx, id)
	})
}

func (s *healthVectorStore) Search(ctx context.Context, query *vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "vector_search", func(ctx context.Context) (*vectorstore.SearchResult, error) {
		return s.VectorStore.Search(ctx, query)
	})
}

func (s *healthVectorStore) DeleteByFilter(ctx context.Context, options ...vectorstore.DeleteOption) error {
	return backendhealth.Do(ctx, s.registry, s.key, "vector_delete_by_filter", func(ctx context.Context) error {
		return s.VectorStore.DeleteByFilter(ctx, options...)
	})
}

func (s *healthVectorStore) UpdateByFilter(ctx context.Context, options ...vectorstore.UpdateByFilterOption) (int64, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "vector_update_by_filter", func(ctx context.Context) (int64, error) {
		return s.VectorStore.UpdateByFilter(ctx, options...)
	})
}

func (s *healthVectorStore) Count(ctx context.Context, options ...vectorstore.CountOption) (int, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "vector_count", func(ctx context.Context) (int, error) {
		return s.VectorStore.Count(ctx, options...)
	})
}

func (s *healthVectorStore) GetMetadata(ctx context.Context, options ...vectorstore.GetMetadataOption) (map[string]vectorstore.DocumentMetadata, error) {
	return backendhealth.Value(ctx, s.registry, s.key, "vector_metadata", func(ctx context.Context) (map[string]vectorstore.DocumentMetadata, error) {
		return s.VectorStore.GetMetadata(ctx, options...)
	})
}
