package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
)

type staticKnowledgeSource struct {
	documents []*document.Document
}

func (s staticKnowledgeSource) ReadDocuments(context.Context) ([]*document.Document, error) {
	return s.documents, nil
}

func (staticKnowledgeSource) Name() string                { return "support-faq" }
func (staticKnowledgeSource) Type() string                { return "text" }
func (staticKnowledgeSource) GetMetadata() map[string]any { return map[string]any{"kind": "faq"} }

func TestPlatformStoreFactoryRequiresCoreDependencies(t *testing.T) {
	t.Parallel()
	database, err := sql.Open("pgx", "postgres://invalid")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	profiles := NewMemoryBackendProfileStore()
	secrets := knowledgeSecretResolver{values: map[string]string{}}
	if _, err := NewPlatformStoreFactory(nil, "", config.KnowledgeConfig{}, nil, profiles, secrets, http.DefaultClient); err == nil {
		t.Fatal("NewPlatformStoreFactory(nil database) error = nil")
	}
	if _, err := NewPlatformStoreFactory(database, "", config.KnowledgeConfig{}, nil, nil, secrets, http.DefaultClient); err == nil {
		t.Fatal("NewPlatformStoreFactory(nil profiles) error = nil")
	}
	if _, err := NewPlatformStoreFactory(database, "", config.KnowledgeConfig{}, nil, profiles, nil, http.DefaultClient); err == nil {
		t.Fatal("NewPlatformStoreFactory(nil secrets) error = nil")
	}
}

func TestPlatformStoreFactoryUsesAuthorizedPostgresArtifactBackend(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	ctx := context.Background()
	const tenantID = "support-factory-test"
	const appCode = "support"
	if _, err := database.ExecContext(ctx, "INSERT INTO tenants (id) VALUES ($1) ON CONFLICT (id) DO NOTHING", tenantID); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	if _, err := database.ExecContext(ctx, "INSERT INTO applications (tenant_id, app_code, status) VALUES ($1,$2,'active') ON CONFLICT (tenant_id, app_code) DO NOTHING", tenantID, appCode); err != nil {
		t.Fatalf("seed application: %v", err)
	}
	if _, err := database.ExecContext(ctx, "DELETE FROM artifacts WHERE tenant_id=$1 AND app_code=$2", tenantID, appCode); err != nil {
		t.Fatalf("clear artifacts: %v", err)
	}

	profiles := NewMemoryBackendProfileStore()
	if err := profiles.ReplaceTenantBackendProfiles(ctx, tenantID, []string{"platform-postgres", "platform-pgvector"}); err != nil {
		t.Fatal(err)
	}
	factory, err := NewPlatformStoreFactory(
		database,
		dsn,
		config.KnowledgeConfig{},
		nil,
		profiles,
		knowledgeSecretResolver{values: map[string]string{}},
		http.DefaultClient,
	)
	if err != nil {
		t.Fatalf("NewPlatformStoreFactory() error = %v", err)
	}
	drivers := factory.ArtifactDrivers()
	if len(drivers) != 4 || drivers[0] != ArtifactDriverInMemory || drivers[1] != ArtifactDriverPostgres || drivers[2] != ArtifactDriverS3 || drivers[3] != ArtifactDriverCOS {
		t.Fatalf("ArtifactDrivers() = %v", drivers)
	}

	tenantConfig := config.TenantConfig{
		TenantID: tenantID,
		AppCode:  appCode,
		Storage:  config.StoragePolicy{Artifact: config.BackendProfileRef{ProfileID: "platform-postgres"}},
	}
	service, err := factory.ArtifactService(ctx, tenantConfig)
	if err != nil {
		t.Fatalf("ArtifactService() error = %v", err)
	}
	info := agentartifact.SessionInfo{AppName: tenantConfig.AppName(), UserID: "support-user", SessionID: "session-1"}
	version, err := service.SaveArtifact(ctx, info, "answer.txt", &agentartifact.Artifact{Data: []byte("support answer"), MimeType: "text/plain", Name: "answer.txt"})
	if err != nil || version != 0 {
		t.Fatalf("SaveArtifact() = %d, %v", version, err)
	}
	loaded, err := service.LoadArtifact(ctx, info, "answer.txt", nil)
	if err != nil || loaded == nil || string(loaded.Data) != "support answer" {
		t.Fatalf("LoadArtifact() = %#v, %v", loaded, err)
	}
}

func TestPlatformStoreFactoryKnowledgeLifecycle(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })

	ctx := context.Background()
	const (
		tenantID   = "support-factory-knowledge"
		appCode    = "support"
		documentID = "faq"
	)
	if _, err := database.ExecContext(ctx, "INSERT INTO tenants (id) VALUES ($1) ON CONFLICT (id) DO NOTHING", tenantID); err != nil {
		t.Fatalf("seed tenant: %v", err)
	}
	if _, err := database.ExecContext(ctx, "INSERT INTO applications (tenant_id, app_code, status) VALUES ($1,$2,'active') ON CONFLICT (tenant_id, app_code) DO NOTHING", tenantID, appCode); err != nil {
		t.Fatalf("seed application: %v", err)
	}
	if _, err := database.ExecContext(ctx, "DELETE FROM knowledge_vectors WHERE metadata->>'tenant_id'=$1 AND metadata->>'app_code'=$2", tenantID, appCode); err != nil {
		t.Fatalf("clear knowledge vectors: %v", err)
	}
	if _, err := database.ExecContext(ctx, "DELETE FROM knowledge_documents WHERE tenant_id=$1 AND app_code=$2", tenantID, appCode); err != nil {
		t.Fatalf("clear knowledge documents: %v", err)
	}
	if _, err := database.ExecContext(ctx, "DELETE FROM knowledge_ingest_jobs WHERE tenant_id=$1 AND app_code=$2", tenantID, appCode); err != nil {
		t.Fatalf("clear knowledge ingest jobs: %v", err)
	}
	t.Cleanup(func() {
		_, _ = database.ExecContext(context.Background(), "DELETE FROM knowledge_vectors WHERE metadata->>'tenant_id'=$1 AND metadata->>'app_code'=$2", tenantID, appCode)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM knowledge_ingest_jobs WHERE tenant_id=$1 AND app_code=$2", tenantID, appCode)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM knowledge_documents WHERE tenant_id=$1 AND app_code=$2", tenantID, appCode)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM applications WHERE tenant_id=$1 AND app_code=$2", tenantID, appCode)
		_, _ = database.ExecContext(context.Background(), "DELETE FROM tenants WHERE id=$1", tenantID)
	})

	profiles := NewMemoryBackendProfileStore()
	if err := profiles.ReplaceTenantBackendProfiles(ctx, tenantID, []string{"platform-pgvector"}); err != nil {
		t.Fatal(err)
	}
	embeddingServer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/embeddings" {
			http.NotFound(writer, request)
			return
		}
		embedding := make([]float64, knowledgeEmbeddingDimensions)
		embedding[0] = 1
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"object": "list",
			"data":   []map[string]any{{"object": "embedding", "index": 0, "embedding": embedding}},
			"model":  "text-embedding-test",
			"usage":  map[string]any{"prompt_tokens": 1, "total_tokens": 1},
		})
	}))
	t.Cleanup(embeddingServer.Close)
	knowledgeConfig := config.KnowledgeConfig{
		EmbeddingProviderID: "embedding",
		EmbeddingModel:      "text-embedding-test",
		EmbeddingDimensions: knowledgeEmbeddingDimensions,
	}
	factory, err := NewPlatformStoreFactory(
		database, dsn, knowledgeConfig,
		[]config.ModelProviderConfig{{ID: "embedding", BaseURL: embeddingServer.URL, APIKeyRef: "env:EMBEDDING_KEY"}},
		profiles,
		knowledgeSecretResolver{values: map[string]string{"env:EMBEDDING_KEY": "test-key"}},
		http.DefaultClient,
	)
	if err != nil {
		t.Fatalf("NewPlatformStoreFactory() error = %v", err)
	}
	t.Cleanup(func() {
		if err := factory.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	if _, err := factory.MemoryEmbedder(ctx); err != nil {
		t.Fatalf("MemoryEmbedder() error = %v", err)
	}
	tenantConfig := config.TenantConfig{
		TenantID: tenantID, AppCode: appCode,
		Storage: config.StoragePolicy{Knowledge: config.BackendProfileRef{ProfileID: "platform-pgvector"}},
	}
	knowledge, err := factory.Knowledge(ctx, tenantConfig, nil)
	if err != nil {
		t.Fatalf("Knowledge() error = %v", err)
	}
	if knowledge == nil {
		t.Fatal("Knowledge() returned nil")
	}

	const (
		jobID = "support-factory-job"
		owner = "support-factory-worker"
	)
	if _, err := database.ExecContext(ctx, `
INSERT INTO knowledge_documents (tenant_id,app_code,document_id,name,status,total_chunks,metadata)
VALUES ($1,$2,$3,'FAQ','indexing',0,'{"kind":"faq"}'::jsonb)`, tenantID, appCode, documentID); err != nil {
		t.Fatalf("seed knowledge document: %v", err)
	}
	if _, err := database.ExecContext(ctx, `
INSERT INTO knowledge_ingest_jobs
    (id,tenant_id,app_code,document_id,name,filename,source_data,backend_driver,status,attempts,lease_owner,lease_until)
VALUES ($1,$2,$3,$4,'FAQ','faq.txt','fixture'::bytea,'pgvector','running',1,$5,NOW()+INTERVAL '5 minutes')`,
		jobID, tenantID, appCode, documentID, owner); err != nil {
		t.Fatalf("seed knowledge ingest job: %v", err)
	}
	count, err := factory.LoadKnowledgeSource(ctx, KnowledgeLoadRequest{
		TenantID: tenantID, AppCode: appCode, DocumentID: documentID, JobID: jobID, Owner: owner,
		Backend: config.BackendConfig{Driver: "pgvector"},
	}, staticKnowledgeSource{documents: []*document.Document{{ID: documentID, Name: "FAQ", Content: "退款申请提交后会进入人工审核。"}}})
	if err != nil || count < 1 {
		t.Fatalf("LoadKnowledgeSource() = %d, %v", count, err)
	}
	if _, err := database.ExecContext(ctx, `UPDATE knowledge_documents SET status='ready', total_chunks=$4 WHERE tenant_id=$1 AND app_code=$2 AND document_id=$3`, tenantID, appCode, documentID, count); err != nil {
		t.Fatalf("publish knowledge document: %v", err)
	}
	documents, err := factory.ListKnowledgeDocuments(ctx, tenantID, appCode)
	if err != nil || len(documents) != 1 || documents[0].DocumentID != documentID {
		t.Fatalf("ListKnowledgeDocuments() = %+v, %v", documents, err)
	}
	if storedCount, err := factory.CountKnowledgeDocument(ctx, tenantID, appCode, documentID, config.BackendConfig{Driver: "pgvector"}); err != nil || storedCount != count {
		t.Fatalf("CountKnowledgeDocument() = %d, %v; want %d", storedCount, err, count)
	}
	if err := factory.DeleteKnowledgeDocument(ctx, tenantConfig, documentID); err != nil {
		t.Fatalf("DeleteKnowledgeDocument() error = %v", err)
	}
	documents, err = factory.ListKnowledgeDocuments(ctx, tenantID, appCode)
	if err != nil || len(documents) != 0 {
		t.Fatalf("documents after delete = %+v, %v", documents, err)
	}
}

func TestPlatformStoreFactoryRejectsUnauthorizedArtifactProfile(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	database, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	factory, err := NewPlatformStoreFactory(
		database, dsn, config.KnowledgeConfig{}, nil,
		NewMemoryBackendProfileStore(), knowledgeSecretResolver{values: map[string]string{}}, http.DefaultClient,
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = factory.ArtifactService(context.Background(), config.TenantConfig{
		TenantID: "support-unauthorized", AppCode: "support",
		Storage: config.StoragePolicy{Artifact: config.BackendProfileRef{ProfileID: "platform-postgres"}},
	})
	if !errors.Is(err, ErrBackendProfileUnauthorized) {
		t.Fatalf("ArtifactService() error = %v, want ErrBackendProfileUnauthorized", err)
	}
}

func TestNilPlatformStoreFactoryHasNoArtifactDrivers(t *testing.T) {
	t.Parallel()
	var factory *PlatformStoreFactory
	if drivers := factory.ArtifactDrivers(); drivers != nil {
		t.Fatalf("ArtifactDrivers() = %v, want nil", drivers)
	}
}
