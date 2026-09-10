//go:build integration

package qdrant_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	frameworkknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	frameworkqdrant "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant"
)

const (
	postgresTestDSNEnv = "TRPC_AGENT_SERVICE_POSTGRES_TEST_DSN"
	qdrantTestHostEnv  = "TRPC_AGENT_SERVICE_QDRANT_TEST_HOST"
	qdrantTestPortEnv  = "TRPC_AGENT_SERVICE_QDRANT_TEST_PORT"
)

func TestScopedKnowledgeUsesQdrantFilterAndSQLAuthority(t *testing.T) {
	host := os.Getenv(qdrantTestHostEnv)
	if host == "" {
		t.Fatalf("%s is required", qdrantTestHostEnv)
	}
	port := 6334
	if value := os.Getenv(qdrantTestPortEnv); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			t.Fatalf("parse qdrant port: %v", err)
		}
		port = parsed
	}
	pool := openKnowledgeIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	metadata, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := metadata.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	id := fmt.Sprintf("qdrant-%d", time.Now().UnixNano())
	scope := tenant.Scope{TenantID: id, AppID: "knowledge"}
	createKnowledgeTenant(t, ctx, metadata, scope, id)
	v1 := knowledgeConfig(scope, "v1")
	if err := metadata.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID:            scope.TenantID,
		AppID:               scope.AppID,
		Name:                "Knowledge",
		ActiveConfigVersion: v1.Version,
		Status:              tenant.StatusActive,
	}, v1); err != nil {
		t.Fatalf("create app: %v", err)
	}
	baseID := "handbook"
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.knowledge_base (tenant_id, app_id, knowledge_base_id, status)
VALUES ($1, $2, $3, 'ACTIVE')`, scope.TenantID, scope.AppID, baseID); err != nil {
		t.Fatalf("create base: %v", err)
	}
	v2 := v1
	v2.Version = "v2"
	v2.KnowledgeBaseIDs = []string{baseID}
	if err := metadata.InsertAppConfigVersion(ctx, v2); err != nil {
		t.Fatalf("insert bound config: %v", err)
	}

	if _, err := pool.Exec(ctx, `
INSERT INTO platform.knowledge_document (
    tenant_id, app_id, knowledge_base_id, document_id, version,
    status, index_generation
) VALUES ($1, $2, $3, $4, 1, 'AVAILABLE', $5)`,
		scope.TenantID, scope.AppID, baseID, "employee-handbook", "g1"); err != nil {
		t.Fatalf("create document: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.knowledge_chunk (
    tenant_id, app_id, knowledge_base_id, document_id, document_version,
    index_generation, chunk_id, status
) VALUES ($1, $2, $3, $4, 1, $5, $6, 'AVAILABLE')`,
		scope.TenantID, scope.AppID, baseID, "employee-handbook", "g1", "1"); err != nil {
		t.Fatalf("create chunk: %v", err)
	}

	vectorStore, err := frameworkqdrant.New(ctx,
		frameworkqdrant.WithHost(host),
		frameworkqdrant.WithPort(port),
		frameworkqdrant.WithCollectionName("knowledge-integration-"+strconv.FormatInt(time.Now().UnixNano(), 10)),
		frameworkqdrant.WithDimension(3),
	)
	if err != nil {
		t.Fatalf("new qdrant store: %v", err)
	}
	defer vectorStore.Close()
	allowed := knowledgeVectorDocument(scope, baseID, "employee-handbook", "1", "1", "g1", "allowed")
	foreign := knowledgeVectorDocument(tenant.Scope{TenantID: "other", AppID: scope.AppID}, baseID, "employee-handbook", "1", "foreign", "g1", "foreign")
	if err := vectorStore.Add(ctx, allowed, []float64{1, 0, 0}); err != nil {
		t.Fatalf("add allowed vector: %v", err)
	}
	if err := vectorStore.Add(ctx, foreign, []float64{1, 0, 0}); err != nil {
		t.Fatalf("add foreign vector: %v", err)
	}
	inner := frameworkknowledge.New(
		frameworkknowledge.WithVectorStore(vectorStore),
		frameworkknowledge.WithEmbedder(fixedEmbedder{}),
	)
	service, err := platformknowledge.NewScopedKnowledge(inner, metadata, scope, v2.Version, []string{baseID})
	if err != nil {
		t.Fatalf("new scoped knowledge: %v", err)
	}
	result, err := service.Search(ctx, &frameworkknowledge.SearchRequest{Query: "policy", MaxResults: 10})
	if err != nil {
		t.Fatalf("search knowledge: %v", err)
	}
	if len(result.Documents) != 1 || result.Documents[0].Document.ID != allowed.ID {
		t.Fatalf("scoped result = %#v, want only %q", result.Documents, allowed.ID)
	}

	if _, err := pool.Exec(ctx, `
UPDATE platform.knowledge_base
SET status = 'DELETED', updated_at = now()
WHERE tenant_id = $1 AND app_id = $2 AND knowledge_base_id = $3`, scope.TenantID, scope.AppID, baseID); err != nil {
		t.Fatalf("delete base: %v", err)
	}
	result, err = service.Search(ctx, &frameworkknowledge.SearchRequest{Query: "policy", MaxResults: 10})
	if err != nil {
		t.Fatalf("search deleted base: %v", err)
	}
	if len(result.Documents) != 0 {
		t.Fatalf("deleted base result = %#v, want no documents", result.Documents)
	}
}

type fixedEmbedder struct{}

func (fixedEmbedder) GetEmbedding(context.Context, string) ([]float64, error) {
	return []float64{1, 0, 0}, nil
}

func (fixedEmbedder) GetEmbeddingWithUsage(context.Context, string) ([]float64, map[string]any, error) {
	return []float64{1, 0, 0}, nil, nil
}

func (fixedEmbedder) GetDimensions() int { return 3 }

func openKnowledgeIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv(postgresTestDSNEnv)
	if dsn == "" {
		t.Fatalf("%s is required", postgresTestDSNEnv)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse postgres dsn: %v", err)
	}
	if !strings.HasSuffix(config.ConnConfig.Database, "_test") {
		t.Fatalf("postgres integration database %q must end in _test", config.ConnConfig.Database)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("open postgres pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func createKnowledgeTenant(t *testing.T, ctx context.Context, store *platformpostgres.Store, scope tenant.Scope, name string) {
	t.Helper()
	if err := store.CreateTenant(ctx, tenant.Tenant{
		ID:     scope.TenantID,
		Name:   name,
		Status: tenant.StatusActive,
	}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
}

func knowledgeConfig(scope tenant.Scope, version string) tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: scope.TenantID,
		AppID:    scope.AppID,
		Version:  version,
		Model: tenant.ModelConfig{
			Provider:  "openai",
			Model:     "gpt-4.1-mini",
			APIKeyRef: tenant.SecretRef{Name: "model-key", Version: "1"},
		},
		Tools: tenant.ToolPolicy{},
		BackendConfig: tenant.BackendConfig{
			Name: "shared",
			Session: tenant.BackendRef{
				Kind:     tenant.BackendSQL,
				Provider: "postgres",
				Name:     "session-postgres",
				Options:  map[string]string{"schema": "agent"},
			},
		},
		SecretRefs: []tenant.SecretRef{{Name: "model-key", Version: "1"}},
	}
}

func knowledgeVectorDocument(scope tenant.Scope, baseID, documentID, documentVersion, chunkID, generation, id string) *document.Document {
	return &document.Document{
		ID:      id,
		Content: "content " + id,
		Metadata: map[string]any{
			platformknowledge.MetadataTenantID:        scope.TenantID,
			platformknowledge.MetadataAppID:           scope.AppID,
			platformknowledge.MetadataKnowledgeBaseID: baseID,
			platformknowledge.MetadataDocumentID:      documentID,
			platformknowledge.MetadataDocumentVersion: documentVersion,
			platformknowledge.MetadataChunkID:         chunkID,
			platformknowledge.MetadataIndexGeneration: generation,
		},
	}
}
