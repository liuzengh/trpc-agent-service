package integration_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/scanner/clamav"
	"github.com/liuzengh/trpc-agent-service/trpcservice/preprocess/scanner/httpdlp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore"
	objects3 "github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore/s3"
)

func TestLiveObjectStoreAndScannerProviderSmoke(t *testing.T) {
	if os.Getenv("TRPC_PROVIDER_SMOKE") != "1" {
		t.Skip("requires explicit real provider smoke")
	}
	required := []string{"TRPC_S3_ENDPOINT", "TRPC_S3_BUCKET", "TRPC_S3_ACCESS_KEY", "TRPC_S3_SECRET_KEY",
		"TRPC_CLAMAV_ADDR", "TRPC_DLP_ENDPOINT", "TRPC_DLP_BEARER_TOKEN",
		"TRPC_DLP_REJECT_SAMPLE", "TRPC_DLP_UNKNOWN_SAMPLE"}
	for _, key := range required {
		if os.Getenv(key) == "" {
			t.Fatalf("%s is required", key)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	credentials := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: os.Getenv("TRPC_S3_ACCESS_KEY"), SecretAccessKey: os.Getenv("TRPC_S3_SECRET_KEY"),
			Source: "scoped-provider-smoke"}, nil
	})
	cfg := aws.Config{Region: envOr("TRPC_S3_REGION", "us-east-1"), Credentials: credentials, HTTPClient: http.DefaultClient}
	objects, err := objects3.NewFromConfig(cfg, os.Getenv("TRPC_S3_BUCKET"), os.Getenv("TRPC_S3_ENDPOINT"), true,
		objects3.Options{MaxBytes: 1 << 20, AllowInsecure: os.Getenv("TRPC_S3_ALLOW_INSECURE") == "1"})
	if err != nil {
		t.Fatal(err)
	}
	if err := objects.Probe(ctx); err != nil {
		t.Fatalf("object store capability probe: %v", err)
	}
	value := smokeObject(t)
	if _, err := objects.PutObject(ctx, value); err != nil {
		t.Fatalf("object put: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_ = objects.DeleteObject(cleanupCtx, value.TenantID, value.ObjectKey, value.ContentDigest)
	})
	loaded, err := objects.GetObject(ctx, value.TenantID, value.ObjectKey)
	if err != nil || loaded.ContentDigest != value.ContentDigest {
		t.Fatalf("object get digest=%q err=%v", loaded.ContentDigest, err)
	}
	collision := value
	collision.Content = []byte("provider smoke collision")
	collisionSum := sha256.Sum256(collision.Content)
	collision.ContentDigest = hex.EncodeToString(collisionSum[:])
	if _, err := objects.PutObject(ctx, collision); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("object overwrite collision: %v", err)
	}
	rawS3 := awss3.NewFromConfig(cfg, func(options *awss3.Options) {
		options.UsePathStyle = true
		options.BaseEndpoint = aws.String(os.Getenv("TRPC_S3_ENDPOINT"))
	})
	replacement := []byte("newer external object version")
	replacementSum := sha256.Sum256(replacement)
	replacementDigest := hex.EncodeToString(replacementSum[:])
	put, err := rawS3.PutObject(ctx, &awss3.PutObjectInput{Bucket: aws.String(os.Getenv("TRPC_S3_BUCKET")),
		Key: aws.String(value.ObjectKey), Body: bytes.NewReader(replacement),
		Metadata: map[string]string{"trpc-content-sha256": replacementDigest}})
	if err != nil || put == nil || aws.ToString(put.VersionId) == "" {
		t.Fatalf("create replacement object version: output=%+v err=%v", put, err)
	}
	if err := objects.DeleteObject(ctx, value.TenantID, value.ObjectKey, replacementDigest); err != nil {
		t.Fatalf("delete precise replacement version: %v", err)
	}
	restored, err := objects.GetObject(ctx, value.TenantID, value.ObjectKey)
	if err != nil || restored.ContentDigest != value.ContentDigest {
		t.Fatalf("precise version delete removed prior version: digest=%q err=%v", restored.ContentDigest, err)
	}
	malware := clamav.Scanner{Address: os.Getenv("TRPC_CLAMAV_ADDR"), DialTimeout: 10 * time.Second,
		CommandTimeout: 10 * time.Second, ScanTimeout: 60 * time.Second, MaxBytes: 1 << 20}
	if err := malware.Probe(ctx); err != nil {
		t.Fatalf("clamav probe: %v", err)
	}
	malwareResult, err := malware.ScanMedia(ctx, value.Content, "text/plain")
	if err != nil || malwareResult.Verdict != preprocess.ScanClean || malwareResult.Version == "" {
		t.Fatalf("clamav result=%+v err=%v", malwareResult, err)
	}
	eicar := []byte("X5O!P%@AP[4\\PZX54(P^)7CC)7}$" + "EICAR-STANDARD-ANTIVIRUS-TEST-FILE!$H+H*")
	rejectedMalware, err := malware.ScanMedia(ctx, eicar, "text/plain")
	if err != nil || rejectedMalware.Verdict != preprocess.ScanRejected || rejectedMalware.Version == "" {
		t.Fatalf("clamav rejection=%+v err=%v", rejectedMalware, err)
	}
	dlp := httpdlp.Scanner{Endpoint: os.Getenv("TRPC_DLP_ENDPOINT"), ProbeTenantID: value.TenantID,
		Timeout: 10 * time.Second, MaxBytes: 1 << 20,
		Authorize: func(_ context.Context, _ string, request *http.Request) error {
			request.Header.Set("Authorization", "Bearer "+os.Getenv("TRPC_DLP_BEARER_TOKEN"))
			return nil
		}}
	if err := dlp.Probe(ctx); err != nil {
		t.Fatalf("dlp probe: %v", err)
	}
	dlpResult, err := dlp.ScanMediaInput(ctx, value.TenantID, value.Content, "text/plain")
	if err != nil || dlpResult.Verdict != preprocess.ScanClean || dlpResult.Version == "" {
		t.Fatalf("dlp result=%+v err=%v", dlpResult, err)
	}
	dlpRejected, err := dlp.ScanMediaInput(ctx, value.TenantID, []byte(os.Getenv("TRPC_DLP_REJECT_SAMPLE")), "text/plain")
	if err != nil || dlpRejected.Verdict != preprocess.ScanRejected || dlpRejected.Version == "" {
		t.Fatalf("dlp rejection=%+v err=%v", dlpRejected, err)
	}
	dlpUnknown, err := dlp.ScanMediaInput(ctx, value.TenantID, []byte(os.Getenv("TRPC_DLP_UNKNOWN_SAMPLE")), "text/plain")
	if !errors.Is(err, runtime.ErrBackendUnavailable) || dlpUnknown.Verdict != preprocess.ScanUnknown || dlpUnknown.Version == "" {
		t.Fatalf("dlp unknown=%+v err=%v", dlpUnknown, err)
	}
	if err := objects.DeleteObject(ctx, value.TenantID, value.ObjectKey, value.ContentDigest); err != nil {
		t.Fatalf("guarded object delete: %v", err)
	}
}

func smokeObject(t *testing.T) objectstore.Object {
	t.Helper()
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	tenantID := "provider-smoke-" + hex.EncodeToString(random)
	artifactDigest := sha256.Sum256(random)
	artifactID := "a1_" + base64.RawURLEncoding.EncodeToString(artifactDigest[:])
	key, err := objectstore.StableKey(tenantID, artifactID)
	if err != nil {
		t.Fatal(err)
	}
	content := []byte("trpc-agent-service provider smoke")
	digest := sha256.Sum256(content)
	return objectstore.Object{TenantID: tenantID, ObjectKey: key, ContentDigest: hex.EncodeToString(digest[:]), Content: content}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
