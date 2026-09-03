package storage

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
)

func TestKnowledgeRouterQdrantIntegration(t *testing.T) {
	host := os.Getenv("TEST_QDRANT_HOST")
	if host == "" {
		t.Skip("TEST_QDRANT_HOST is not set")
	}
	port, _ := strconv.Atoi(os.Getenv("TEST_QDRANT_PORT"))
	if port == 0 {
		port = 6334
	}
	data := controlplane.DefaultBootstrapData()
	data.BackendBindings = append(data.BackendBindings, controlplane.BackendBinding{
		ID: "qdrant-knowledge", TenantID: "tutorial-tenant", AppID: "tutorial-app",
		ResourceType: "knowledge", BackendType: "qdrant", MigrationState: "active", Version: 1,
		Config: json.RawMessage(`{"host":"` + host + `","port":` + strconv.Itoa(port) + `,"collection_name":"trpc_agent_integration","dimensions":64,"max_results":5}`),
	})
	data.Revisions[0].KnowledgeConfig = json.RawMessage(`{
        "enabled":true,"max_results":3,"chunk_size":200,
        "embedding":{"provider":"hash","dimensions":64}
    }`)
	data.Revisions[0].Checksum = controlplane.RevisionChecksum(data.Revisions[0])
	repository := controlplane.NewMemoryRepository(data)
	router, err := NewKnowledgeRouter(repository, secret.StaticStore{})
	if err != nil {
		t.Fatalf("new router: %v", err)
	}
	t.Cleanup(func() {
		_ = router.Close()
		_ = repository.Close()
	})
	scope := runtimecontext.TutorialScope()
	revision := data.Revisions[0]
	if _, err := router.UpsertDocument(context.Background(), scope, revision, KnowledgeDocument{
		ID: "qdrant-handbook", Name: "Qdrant handbook",
		Content: "enterprise refund policy permits returns within thirty days",
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	kb, _, err := router.KnowledgeForRevision(context.Background(), scope, revision)
	if err != nil {
		t.Fatalf("knowledge: %v", err)
	}
	result, err := kb.Search(context.Background(), &knowledge.SearchRequest{
		Query: "enterprise refund policy", MaxResults: 3,
	})
	if err != nil || result.Document == nil {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
