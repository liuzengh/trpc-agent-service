package s3_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore"
	objects3 "github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore/s3"
)

// TestMinIOObjectStoreContract is intentionally opt-in for a developer, but
// the backend CI job supplies every input and rejects any skip via
// scripts/test_no_skip.sh. It proves the same versioned S3 contract used by
// production works against a live MinIO server.
func TestMinIOObjectStoreContract(t *testing.T) {
	if os.Getenv("TRPC_OBJECTSTORE_SMOKE") != "1" {
		t.Skip("TRPC_OBJECTSTORE_SMOKE=1 is required")
	}
	endpoint, bucket := os.Getenv("TRPC_S3_ENDPOINT"), os.Getenv("TRPC_S3_BUCKET")
	if endpoint == "" || bucket == "" || os.Getenv("TRPC_S3_ACCESS_KEY") == "" || os.Getenv("TRPC_S3_SECRET_KEY") == "" {
		t.Fatal("MinIO object-store configuration is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	credentials := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: os.Getenv("TRPC_S3_ACCESS_KEY"), SecretAccessKey: os.Getenv("TRPC_S3_SECRET_KEY"), Source: "minio-contract"}, nil
	})
	config := aws.Config{Region: "us-east-1", Credentials: credentials}
	raw := awss3.NewFromConfig(config, func(value *awss3.Options) {
		value.UsePathStyle, value.BaseEndpoint = true, aws.String(endpoint)
	})
	if _, err := raw.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		var exists *types.BucketAlreadyOwnedByYou
		if !errors.As(err, &exists) {
			t.Fatalf("create MinIO bucket: %v", err)
		}
	}
	if _, err := raw.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{Bucket: aws.String(bucket), VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}}); err != nil {
		t.Fatalf("enable MinIO bucket versioning: %v", err)
	}
	store, err := objects3.NewFromConfig(config, bucket, endpoint, true, objects3.Options{MaxBytes: 1 << 20, AllowInsecure: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Probe(ctx); err != nil {
		t.Fatalf("versioned MinIO capability probe: %v", err)
	}
	content := []byte("minio object-store integration")
	digest := sha256.Sum256(content)
	artifactID := "a1_" + base64.RawURLEncoding.EncodeToString(digest[:])
	objectKey, err := objectstore.StableKey("minio-contract", artifactID)
	if err != nil {
		t.Fatal(err)
	}
	value := objectstore.Object{TenantID: "minio-contract", ObjectKey: objectKey, ContentDigest: hex.EncodeToString(digest[:]), Content: content}
	if _, err := store.PutObject(ctx, value); err != nil {
		t.Fatalf("put object: %v", err)
	}
	loaded, err := store.GetObject(ctx, value.TenantID, value.ObjectKey)
	if err != nil || !bytes.Equal(loaded.Content, content) || loaded.ContentDigest != value.ContentDigest {
		t.Fatalf("get object=%+v err=%v", loaded, err)
	}
	collision := value
	collision.Content = []byte("unexpected replacement")
	collisionDigest := sha256.Sum256(collision.Content)
	collision.ContentDigest = hex.EncodeToString(collisionDigest[:])
	if _, err := store.PutObject(ctx, collision); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("idempotent collision=%v", err)
	}
	if err := store.DeleteObject(ctx, value.TenantID, value.ObjectKey, value.ContentDigest); err != nil {
		t.Fatalf("guarded delete: %v", err)
	}
}
