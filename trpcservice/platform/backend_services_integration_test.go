package platform

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

func TestS3ObjectIntegrationProfile(t *testing.T) {
	endpoint := os.Getenv("TRPC_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("TRPC_TEST_S3_ENDPOINT not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bucket := "trpc-test-" + stableID(time.Now().String())
	access, secret := os.Getenv("TRPC_TEST_S3_ACCESS_KEY"), os.Getenv("TRPC_TEST_S3_SECRET_KEY")
	client := awss3.New(awss3.Options{Region: "us-east-1", BaseEndpoint: aws.String(endpoint), UsePathStyle: true, Credentials: credentials.NewStaticCredentialsProvider(access, secret, "")})
	if _, err := client.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		objects, _ := client.ListObjectsV2(cleanup, &awss3.ListObjectsV2Input{Bucket: aws.String(bucket)})
		if objects != nil {
			for _, object := range objects.Contents {
				_, _ = client.DeleteObject(cleanup, &awss3.DeleteObjectInput{Bucket: aws.String(bucket), Key: object.Key})
			}
		}
		_, _ = client.DeleteBucket(cleanup, &awss3.DeleteBucketInput{Bucket: aws.String(bucket)})
	})
	config := backendSelection{Backend: "inmemory", Object: ObjectBackendConfig{Bucket: bucket, Endpoint: endpoint, Region: "us-east-1", AccessKeyEnv: "TRPC_TEST_S3_ACCESS_KEY", SecretKeyEnv: "TRPC_TEST_S3_SECRET_KEY"}}
	store, err := newBackendStore(config)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDataStore(store)
	if err := store.AppendSessionEvent(ctx, SessionEvent{TenantID: "t", SessionID: "s", IdempotencyKey: "out", Type: "message.completed", Payload: []byte(`{"output":"real object content"}`)}); err != nil {
		t.Fatal(err)
	}
	item, err := store.(ArtifactStore).PutArtifact(ctx, Artifact{TenantID: "t", SessionID: "s", Name: "reply", ContentRef: "session-event://t/s/out"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := store.(*compositeStore).LoadArtifactContent(ctx, "t", item.ID)
	if err != nil || string(data) != "real object content" {
		t.Fatalf("content=%s err=%v", data, err)
	}
}

func TestQdrantGenerationIntegrationProfile(t *testing.T) {
	host := os.Getenv("TRPC_TEST_QDRANT_HOST")
	if host == "" {
		t.Skip("TRPC_TEST_QDRANT_HOST not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	base := NewInMemoryStore()
	config := VectorBackendConfig{Backend: "qdrant", Host: host, Port: 16334, Dimensions: 2, Generation: "test-" + stableID(time.Now().String())}
	makeStore := func() *compositeStore {
		return &compositeStore{DataStore: base, knowledge: base, embedder: fixtureEmbedder{}, vectorConfig: config, vectors: map[string]vectorstore.VectorStore{}}
	}
	first := makeStore()
	defer first.Close()
	if _, err := base.PutKnowledge(ctx, KnowledgeRecord{TenantID: "tenant-a", AgentAppID: "app", Source: "doc", Content: "remote content"}); err != nil {
		t.Fatal(err)
	}
	if err := ReindexKnowledge(ctx, first, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	second := makeStore()
	defer second.Close()
	remote, err := second.vectorStore(ctx, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	count, err := remote.Count(ctx)
	if err != nil || count != 1 {
		t.Fatalf("remote count=%d err=%v", count, err)
	}
	if err := ReindexKnowledge(ctx, second, "tenant-a"); err != nil {
		t.Fatal(err)
	}
	count, err = remote.Count(ctx)
	if err != nil || count != 1 {
		t.Fatalf("reindex duplicated count=%d err=%v", count, err)
	}
	records, err := second.SearchKnowledge(ctx, "tenant-a", "app", "content")
	if err != nil || len(records) != 1 {
		t.Fatalf("records=%v err=%v", records, err)
	}
}
