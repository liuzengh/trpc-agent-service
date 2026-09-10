package s3

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"testing"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

// TestS3ArtifactLiveConformance is opt-in. It targets a local MinIO or another
// S3-compatible endpoint and is intentionally skipped in default CI.
func TestS3ArtifactLiveConformance(t *testing.T) {
	endpoint := strings.TrimSpace(os.Getenv("S3_RUNTIME_TEST_ENDPOINT"))
	bucket := strings.TrimSpace(os.Getenv("S3_RUNTIME_TEST_BUCKET"))
	accessKey := os.Getenv("S3_RUNTIME_TEST_ACCESS_KEY")
	secretKey := os.Getenv("S3_RUNTIME_TEST_SECRET_KEY")
	requireLiveS3Config(t, endpoint, bucket, accessKey, secretKey)
	region := strings.TrimSpace(os.Getenv("S3_RUNTIME_TEST_REGION"))
	if region == "" {
		region = "us-east-1"
	}
	allowInsecure := strings.HasPrefix(strings.ToLower(endpoint), "http://")
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), func(options *awsconfig.LoadOptions) error {
		options.Region = region
		options.Credentials = credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")
		return nil
	})
	if err != nil {
		t.Fatalf("load S3 config: %v", err)
	}
	store, err := NewFromConfig(cfg, bucket, testLiveTenant, endpoint, true, allowInsecure, Options{})
	if err != nil {
		t.Fatalf("construct S3 store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Probe(context.Background()); err != nil {
		t.Fatalf("probe S3 store: %v", err)
	}

	artifact := runtimestorage.ArtifactRecord{TenantID: testLiveTenant, ArtifactID: "live-artifact", SessionID: "live-session", Name: "attachment.txt", MimeType: "text/plain", Content: []byte("live attachment")}
	put, err := store.PutArtifact(context.Background(), artifact)
	if err != nil {
		t.Fatalf("put artifact: %v", err)
	}
	if put.Version < 1 {
		t.Fatalf("put artifact version = %d", put.Version)
	}
	object, err := store.PutObject(context.Background(), testLiveTenant, "attachments/live.txt", bytes.NewReader([]byte("media")), "text/plain")
	if err != nil {
		t.Fatalf("put object: %v", err)
	}
	if object.Size != int64(len("media")) {
		t.Fatalf("put object info = %#v", object)
	}

	// Recreate the provider to prove bytes are durable beyond one Store/client.
	_ = store.Close()
	store, err = NewFromConfig(cfg, bucket, testLiveTenant, endpoint, true, allowInsecure, Options{})
	if err != nil {
		t.Fatalf("recreate S3 store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.DeleteArtifact(context.Background(), testLiveTenant, artifact.ArtifactID)
		_ = store.DeleteObject(context.Background(), testLiveTenant, "attachments/live.txt")
		_ = store.Close()
	})
	got, err := store.GetArtifact(context.Background(), testLiveTenant, artifact.ArtifactID)
	if err != nil || string(got.Content) != string(artifact.Content) {
		t.Fatalf("get artifact after recreate = %#v, %v", got, err)
	}
	reader, info, err := store.GetObject(context.Background(), testLiveTenant, "attachments/live.txt")
	if err != nil {
		t.Fatalf("get object after recreate: %v", err)
	}
	data, readErr := io.ReadAll(reader)
	closeErr := reader.Close()
	if readErr != nil || closeErr != nil || string(data) != "media" || info.Size != int64(len(data)) {
		t.Fatalf("attachment reader = %q, %#v, %v, %v", data, info, readErr, closeErr)
	}
}

func requireLiveS3Config(t *testing.T, endpoint, bucket, accessKey, secretKey string) {
	t.Helper()
	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		t.Skip("S3_RUNTIME_TEST_ENDPOINT, S3_RUNTIME_TEST_BUCKET, S3_RUNTIME_TEST_ACCESS_KEY, and S3_RUNTIME_TEST_SECRET_KEY are required")
	}
}

const testLiveTenant = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
