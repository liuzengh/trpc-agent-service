//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	qdrantclient "github.com/qdrant/go-client/qdrant"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/knowledgeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/runtimeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
)

func TestRuntimePlaneProfilesDriveRealKnowledgeAndArtifactServices(t *testing.T) {
	db := openDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	tenantID := "runtime-plane-" + uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants(id,name,status,config) VALUES($1,'runtime plane integration','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM artifact_versions WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, tenantID)
	})

	qdrantHost := requiredEnv(t, "TEST_QDRANT_HOST")
	qdrantPort := requiredIntEnv(t, "TEST_QDRANT_GRPC_PORT")
	collection := "tsa_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	qdrantAdmin, err := qdrantclient.NewClient(&qdrantclient.Config{Host: qdrantHost, Port: qdrantPort, PoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = qdrantAdmin.DeleteCollection(context.Background(), collection)
		_ = qdrantAdmin.Close()
	})
	seedStore, err := knowledgeplane.NewQdrantScopedStore(ctx, tenantID, "support-app", knowledgeplane.QdrantConfig{
		Host: qdrantHost, Port: qdrantPort, Collection: collection, Dimension: 3, AllowInsecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := seedStore.Add(ctx, &document.Document{
		ID: "refund-policy", Content: "Refunds settle in three business days.", Metadata: map[string]any{"kind": "faq"},
	}, []float64{1, 0, 0}); err != nil {
		_ = seedStore.Close()
		t.Fatal(err)
	}
	if err := seedStore.Close(); err != nil {
		t.Fatal(err)
	}

	var embeddingCalls atomic.Int32
	embeddingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/embeddings" || r.Header.Get("Authorization") != "Bearer embedding-local-secret" {
			http.Error(w, "invalid embedding request", http.StatusBadRequest)
			return
		}
		embeddingCalls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"object": "list", "model": "embedding-test",
			"data":  []map[string]any{{"object": "embedding", "index": 0, "embedding": []float64{1, 0, 0}}},
			"usage": map[string]int{"prompt_tokens": 3, "total_tokens": 3},
		})
	}))
	defer embeddingServer.Close()

	minioEndpoint := requiredEnv(t, "TEST_MINIO_ENDPOINT")
	minioAccess := requiredEnv(t, "TEST_MINIO_ACCESS_KEY")
	minioSecret := requiredEnv(t, "TEST_MINIO_SECRET_KEY")
	bucket := "tsa-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	minioAdmin, err := minio.New(minioEndpoint, &minio.Options{Creds: credentials.NewStaticV4(minioAccess, minioSecret, "")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = minioAdmin.RemoveBucket(context.Background(), bucket) })

	manifest := fmt.Sprintf(`[
      {"id":"knowledge-local","backend":"qdrant","endpoint":%q,"tls":false,"allowInsecure":true,
       "collection":%q,"dimension":3,"embeddingEndpoint":%q,"embeddingModel":"embedding-test",
       "embeddingAPIKeyEnv":"EMBEDDING_API_KEY","tenantIds":[%q]},
      {"id":"artifact-local","backend":"s3","endpoint":%q,"tls":false,"allowInsecure":true,
       "bucket":%q,"region":"us-east-1","accessKeyEnv":"MINIO_ACCESS_KEY","secretKeyEnv":"MINIO_SECRET_KEY",
       "createBucket":true,"maxBytes":1048576,"tenantIds":[%q]}
    ]`, qdrantEndpoint(qdrantHost, qdrantPort), collection, embeddingServer.URL+"/v1", tenantID,
		minioEndpoint, bucket, tenantID)
	secrets := map[string]string{
		"EMBEDDING_API_KEY": "embedding-local-secret", "MINIO_ACCESS_KEY": minioAccess, "MINIO_SECRET_KEY": minioSecret,
	}
	catalog, err := runtimeplane.LoadProfiles(manifest, func(name string) (string, bool) {
		value, ok := secrets[name]
		return value, ok
	})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := runtimeplane.NewProfileResolver(catalog, db)
	if err != nil {
		t.Fatal(err)
	}
	tenantSnapshot := &tenant.Tenant{ID: tenantID, Storage: tenant.StorageConfig{
		KnowledgeBackend: "qdrant", KnowledgeProfile: "knowledge-local",
		ArtifactBackend: "s3", ArtifactProfile: "artifact-local",
	}}
	lease, err := resolver.Acquire(ctx, runtimeplane.Request{
		Tenant: tenantSnapshot, AgentAppID: "support-app", NeedKnowledge: true, NeedArtifact: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release() })

	result, err := lease.Knowledge.Search(ctx, &knowledge.SearchRequest{Query: "When does a refund settle?", MaxResults: 3})
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Document == nil || result.Document.ID != "refund-policy" ||
		!strings.Contains(result.Text, "three business days") || embeddingCalls.Load() != 1 {
		t.Fatalf("knowledge result=%#v embeddingCalls=%d", result, embeddingCalls.Load())
	}
	info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}
	version, err := lease.Artifact.SaveArtifact(ctx, info, "answer.txt", &artifact.Artifact{
		Data: []byte("durable answer"), MimeType: "text/plain",
	})
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := lease.Artifact.LoadArtifact(ctx, info, "answer.txt", &version)
	if err != nil || loaded == nil || string(loaded.Data) != "durable answer" {
		t.Fatalf("artifact=%#v error=%v", loaded, err)
	}
	if err := lease.Artifact.DeleteArtifact(ctx, info, "answer.txt"); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
}

func qdrantEndpoint(host string, port int) string {
	return (&url.URL{Host: fmt.Sprintf("%s:%d", host, port)}).Host
}
