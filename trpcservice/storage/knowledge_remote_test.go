package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
)

func TestKnowledgeRouterRemoteQdrantIntegration(t *testing.T) {
	host := os.Getenv("TEST_QDRANT_HOST")
	if host == "" {
		t.Skip("isolated Qdrant not configured")
	}
	port, _ := strconv.Atoi(os.Getenv("TEST_QDRANT_PORT"))
	if port == 0 {
		t.Fatal("isolated Qdrant port required")
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"data":[{"embedding":[1,0,0]}],"usage":{"prompt_tokens":3}}`)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	collection := fmt.Sprintf("remote_scope_%x", time.Now().UnixNano())
	var routers []*KnowledgeRouter
	var revisions []controlplane.AgentRevision
	var scopes []runtimecontext.Scope
	for _, identity := range [][2]string{{"tenant-a", "app-a"}, {"tenant-a", "app-b"}, {"tenant-b", "app-a"}} {
		scope, _ := runtimecontext.NewScope(identity[0], identity[1], identity[0]+"-"+identity[1]+"-v1", "http", "test-binding")
		ctx := runtimecontext.WithStorageScope(ctx, scope.StorageScope)
		rev := controlplane.AgentRevision{ID: scope.RevisionID, TenantID: scope.TenantID, AppID: scope.AppID}
		rev.KnowledgeConfig, _ = json.Marshal(map[string]any{"enabled": true, "embedding": map[string]any{"provider": "openai", "model": "test-model", "base_url": server.URL, "dimensions": 3, "secret_ref": "test://embedding"}})
		backendConfig, _ := json.Marshal(map[string]any{"host": host, "port": port, "collection_name": collection, "dimensions": 3})
		data := controlplane.DefaultBootstrapData()
		data.Revisions = append(data.Revisions, rev)
		data.BackendBindings = append(data.BackendBindings, controlplane.BackendBinding{ID: scope.RevisionID + "-backend", TenantID: scope.TenantID, AppID: scope.AppID, ResourceType: "knowledge", BackendType: "qdrant", MigrationState: "active", Version: 1, Config: backendConfig})
		repo := controlplane.NewMemoryRepository(data)
		t.Cleanup(func() { _ = repo.Close() })
		router, err := NewKnowledgeRouter(repo, secret.StaticStore{"test://embedding": "synthetic-key"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = router.Close() })
		_, err = router.UpsertDocument(ctx, scope, rev, KnowledgeDocument{ID: "same-document-id", Content: scope.StorageScope + " private synthetic document", Metadata: map[string]any{"tenant_id": "forged", "app_id": "forged"}})
		if err != nil {
			t.Fatal(err)
		}
		routers = append(routers, router)
		revisions = append(revisions, rev)
		scopes = append(scopes, scope)
	}
	for i, router := range routers {
		ctx := runtimecontext.WithStorageScope(ctx, scopes[i].StorageScope)
		kb, enabled, err := router.KnowledgeForRevision(ctx, scopes[i], revisions[i])
		if err != nil || !enabled {
			t.Fatal(err)
		}
		result, err := kb.Search(ctx, &knowledge.SearchRequest{Query: "all documents", MaxResults: 20, SearchFilter: &knowledge.SearchFilter{Metadata: map[string]any{"tenant_id": "tenant-b", "app_id": "app-b"}}})
		if err != nil || len(result.Documents) != 1 || result.Document.Content != scopes[i].StorageScope+" private synthetic document" {
			t.Fatal("shared collection scope filter failed", err)
		}
	}
}
