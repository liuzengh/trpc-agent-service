//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestKnowledgeMigrationCatalogUsesSQLAuthority(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tenantID := fmt.Sprintf("knowledge-catalog-%d", time.Now().UnixNano())
	appID := "search"
	baseID := "handbook"
	config := integrationAppConfig("v1", "knowledge-catalog-model")
	config.TenantID = tenantID
	config.AppID = appID
	config.KnowledgeBaseIDs = []string{baseID}
	config.BackendConfig.Knowledge = tenant.BackendRef{
		Kind:     tenant.BackendVector,
		Provider: "qdrant",
		Name:     "qdrant-source",
		Options: map[string]string{
			"embedding_model":      "embedding-model",
			"embedding_dimensions": "3",
			"embedding_profile":    "catalog-source",
			"index_generation":     "generation-1",
		},
	}
	if err := store.CreateTenant(ctx, tenant.Tenant{ID: tenantID, Name: tenantID, Status: tenant.StatusActive}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID:            tenantID,
		AppID:               appID,
		Name:                "Search",
		ActiveConfigVersion: config.Version,
		Status:              tenant.StatusActive,
	}, config); err != nil {
		t.Fatalf("create app: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.data_migration WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.knowledge_chunk WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.knowledge_document WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.knowledge_base WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.app_config_version WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.agent_app WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.tenant WHERE tenant_id = $1`, tenantID)
	})

	if _, err := pool.Exec(ctx, `
INSERT INTO platform.knowledge_base (tenant_id, app_id, knowledge_base_id, status)
VALUES ($1, $2, $3, 'ACTIVE')`, tenantID, appID, baseID); err != nil {
		t.Fatalf("create knowledge base: %v", err)
	}
	targetConfig := config.Clone()
	targetConfig.Version = "v2"
	targetConfig.BackendConfig.Knowledge.Name = "qdrant-target"
	targetConfig.BackendConfig.Knowledge.Options["embedding_profile"] = "catalog-target"
	if err := store.InsertAppConfigVersion(ctx, targetConfig); err != nil {
		t.Fatalf("insert target config: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.knowledge_document (
    tenant_id, app_id, knowledge_base_id, document_id, version,
    status, index_generation
) VALUES ($1, $2, $3, $4, 3, 'AVAILABLE', 'generation-1')`, tenantID, appID, baseID, "doc-1"); err != nil {
		t.Fatalf("create knowledge document: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO platform.knowledge_chunk (
    tenant_id, app_id, knowledge_base_id, document_id, document_version,
    index_generation, chunk_id, status
) VALUES ($1, $2, $3, $4, 3, 'generation-1', $5, 'AVAILABLE')`, tenantID, appID, baseID, "doc-1", "chunk-1"); err != nil {
		t.Fatalf("create knowledge chunk: %v", err)
	}
	record := migration.Record{
		ID:                  "knowledge-catalog-migration",
		TenantID:            tenantID,
		AppID:               appID,
		Domain:              migration.DomainKnowledge,
		SourceConfigVersion: config.Version,
		TargetConfigVersion: "v2",
		Status:              migration.StatusPending,
	}
	if err := store.CreateDataMigration(ctx, record); err != nil {
		t.Fatalf("create knowledge data migration: %v", err)
	}
	refs, err := store.ListKnowledgeMigrationChunks(ctx, record)
	if err != nil {
		t.Fatalf("list knowledge migration chunks: %v", err)
	}
	want := platformknowledge.ChunkRef{
		Scope:           tenant.Scope{TenantID: tenantID, AppID: appID},
		ConfigVersion:   config.Version,
		KnowledgeBaseID: baseID,
		DocumentID:      "doc-1",
		DocumentVersion: "3",
		ChunkID:         "chunk-1",
		IndexGeneration: "generation-1",
	}
	if len(refs) != 1 || refs[0] != want {
		t.Fatalf("knowledge migration refs = %#v, want %#v", refs, []platformknowledge.ChunkRef{want})
	}
}
