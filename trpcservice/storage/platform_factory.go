package storage

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	agentknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/source"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type PlatformStoreFactory struct {
	postgres  *PostgresPlatformStore
	artifacts *ArtifactBackends
	profiles  BackendProfileStore
	secrets   credential.SecretResolver
	framework frameworkKnowledgeConfig
}

func NewPlatformStoreFactory(database *sql.DB, databaseURL string, knowledge config.KnowledgeConfig, providers []config.ModelProviderConfig, profiles BackendProfileStore, secrets credential.SecretResolver, httpClient *http.Client) (*PlatformStoreFactory, error) {
	postgres, err := NewPostgresPlatformStore(database)
	if err != nil {
		return nil, err
	}
	postgresArtifacts, err := NewPostgresArtifactService(database)
	if err != nil {
		return nil, err
	}
	if secrets == nil {
		return nil, fmt.Errorf("storage secret resolver is required")
	}
	if profiles == nil {
		return nil, fmt.Errorf("backend profile store is required")
	}
	providerCatalog := make(map[string]config.ModelProviderConfig, len(providers))
	for _, provider := range providers {
		providerCatalog[provider.ID] = provider
	}
	return &PlatformStoreFactory{
		postgres: postgres, artifacts: newArtifactBackends(postgresArtifacts), profiles: profiles, secrets: secrets,
		framework: frameworkKnowledgeConfig{
			databaseURL: databaseURL, knowledge: knowledge, providers: providerCatalog, secrets: secrets, httpClient: httpClient,
		},
	}, nil
}

func (f *PlatformStoreFactory) ArtifactDrivers() []string {
	if f == nil || f.artifacts == nil {
		return nil
	}
	return f.artifacts.DriverNames()
}

// ArtifactService resolves the framework artifact service for one immutable
// tenant application configuration from the registered backend catalog.
func (f *PlatformStoreFactory) ArtifactService(ctx context.Context, tenantConfig config.TenantConfig) (agentartifact.Service, error) {
	backend, err := f.profiles.ResolveTenantBackend(ctx, tenantConfig.TenantID, BackendDomainArtifact, tenantConfig.Storage.Artifact.ProfileID)
	if err != nil {
		return nil, fmt.Errorf("resolve tenant %q artifact backend profile: %w", tenantConfig.AppName(), err)
	}
	connection := ""
	if reference := strings.TrimSpace(backend.ConnectionRef); reference != "" {
		value, err := f.secrets.Resolve(ctx, reference)
		if err != nil {
			return nil, fmt.Errorf("resolve tenant %q artifact configuration: %w", tenantConfig.AppName(), err)
		}
		connection = value
	}
	service, err := f.artifacts.Open(ctx, backend.Driver, connection)
	if err != nil {
		return nil, fmt.Errorf("tenant %q: %w", tenantConfig.AppName(), err)
	}
	return service, nil
}

// Knowledge returns the framework-native knowledge surface for one tenant
// application. The immutable tenant configuration selects the registered
// pgvector or Qdrant backend; the agent framework owns retrieval semantics.
func (f *PlatformStoreFactory) Knowledge(ctx context.Context, tenantConfig config.TenantConfig, configuredModel model.Model) (agentknowledge.Knowledge, error) {
	backend, err := f.profiles.ResolveTenantBackend(ctx, tenantConfig.TenantID, BackendDomainKnowledge, tenantConfig.Storage.Knowledge.ProfileID)
	if err != nil {
		return nil, fmt.Errorf("resolve tenant %q Knowledge backend profile: %w", tenantConfig.AppName(), err)
	}
	return f.framework.newKnowledge(ctx, tenantConfig, backend, configuredModel)
}

type KnowledgeLoadRequest struct {
	TenantID, AppCode, DocumentID string
	JobID, Owner                  string
	Backend                       config.BackendConfig
}

// LoadKnowledgeSource delegates reading, structured chunking, embedding, and
// persistence to BuiltinKnowledge.Load. The platform only supplies scope and
// the durable-job fence metadata.
func (f *PlatformStoreFactory) LoadKnowledgeSource(ctx context.Context, request KnowledgeLoadRequest, src source.Source) (int, error) {
	metadata := map[string]any{
		knowledgeParentMetadataKey: request.DocumentID,
		knowledgeJobMetadataKey:    request.JobID,
		knowledgeOwnerMetadataKey:  request.Owner,
	}
	store, err := f.framework.newVectorStore(ctx, request.TenantID, request.AppCode, request.Backend, metadata)
	if err != nil {
		return 0, err
	}
	embedder, err := f.framework.newEmbedder(ctx)
	if err != nil {
		_ = store.Close()
		return 0, err
	}
	knowledge := agentknowledge.New(
		agentknowledge.WithSources([]source.Source{src}),
		agentknowledge.WithVectorStore(store),
		agentknowledge.WithEmbedder(embedder),
	)
	defer knowledge.Close()
	if err := store.DeleteByFilter(ctx, vectorstore.WithDeleteFilter(map[string]any{knowledgeParentMetadataKey: request.DocumentID})); err != nil {
		return 0, fmt.Errorf("replace existing framework knowledge vectors: %w", err)
	}
	if err := knowledge.Load(ctx); err != nil {
		return 0, fmt.Errorf("load framework knowledge source: %w", err)
	}
	count, err := store.Count(ctx, vectorstore.WithCountFilter(map[string]any{knowledgeParentMetadataKey: request.DocumentID}))
	if err != nil {
		return 0, fmt.Errorf("count framework knowledge vectors: %w", err)
	}
	return count, nil
}

func (f *PlatformStoreFactory) CountKnowledgeDocument(ctx context.Context, tenantID, appCode, documentID string, backend config.BackendConfig) (int, error) {
	store, err := f.framework.newVectorStore(ctx, tenantID, appCode, normalizedKnowledgeBackendConfig(backend), nil)
	if err != nil {
		return 0, err
	}
	defer store.Close()
	return store.Count(ctx, vectorstore.WithCountFilter(map[string]any{knowledgeParentMetadataKey: documentID}))
}

// ListKnowledgeDocuments returns the platform-owned lifecycle projection. It
// deliberately does not read framework VectorStore content.
func (f *PlatformStoreFactory) ListKnowledgeDocuments(ctx context.Context, tenantID, appCode string) ([]KnowledgeDocument, error) {
	return f.postgres.ListKnowledgeDocuments(ctx, tenantID, appCode)
}

// DeleteKnowledgeDocument removes both framework RAG vectors and the platform
// lifecycle projection. Vector deletion goes through the same tenant-scoped
// framework VectorStore adapter used by retrieval.
func (f *PlatformStoreFactory) DeleteKnowledgeDocument(ctx context.Context, tenantConfig config.TenantConfig, documentID string) error {
	backend, err := f.profiles.ResolveTenantBackend(ctx, tenantConfig.TenantID, BackendDomainKnowledge, tenantConfig.Storage.Knowledge.ProfileID)
	if err != nil {
		return fmt.Errorf("resolve tenant %q Knowledge backend profile: %w", tenantConfig.AppName(), err)
	}
	store, err := f.framework.newVectorStore(ctx, tenantConfig.TenantID, tenantConfig.AppCode, normalizedKnowledgeBackendConfig(backend), nil)
	if err != nil {
		return err
	}
	defer store.Close()
	if err := store.DeleteByFilter(ctx, vectorstore.WithDeleteFilter(map[string]any{
		knowledgeParentMetadataKey: documentID,
	})); err != nil {
		return fmt.Errorf("delete framework knowledge vectors: %w", err)
	}
	return f.postgres.DeleteKnowledgeDocument(ctx, tenantConfig.TenantID, tenantConfig.AppCode, documentID)
}
