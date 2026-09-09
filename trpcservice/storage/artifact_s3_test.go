package storage

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"trpc.group/trpc-go/trpc-agent-go/artifact"

	"github.com/liuzengh/trpc-agent-service/trpcservice/testenv"
)

// testS3Endpoint and testS3Bucket address the MinIO instance the S3
// integration tests run against; they skip when it is unreachable.
var (
	testS3Endpoint = testenv.S3Endpoint()
	testS3Bucket   = "artifacts"
)

// staticSecrets is a test-time config.SecretResolver backed by a map.
type staticSecrets map[string]string

// Resolve implements config.SecretResolver.
func (s staticSecrets) Resolve(_ context.Context, ref string) (string, error) {
	v, ok := s[ref]
	if !ok {
		return "", fmt.Errorf("unknown secret ref %q", ref)
	}
	return v, nil
}

// s3OrSkip builds a service against the compose MinIO with a unique key
// prefix per test; unreachable MinIO skips instead of failing.
func s3OrSkip(t *testing.T) *S3ArtifactService {
	t.Helper()
	svc, err := NewS3ArtifactService(S3ArtifactConfig{
		Endpoint:     testS3Endpoint,
		AccessKeyRef: "minio-access",
		SecretKeyRef: "minio-secret",
		Bucket:       testS3Bucket,
		// Unique prefix per run keeps tests isolated without per-test buckets.
		Prefix: fmt.Sprintf("test/%d/", time.Now().UnixNano()),
		Secrets: staticSecrets{
			"minio-access": "trpc",
			"minio-secret": "trpc-dev-only",
		},
	})
	if err != nil {
		t.Skipf("minio unavailable (%v) — set TRPC_TEST_S3_ENDPOINT (default %s), skipping integration test", err, testS3Endpoint)
	}
	t.Cleanup(func() { cleanupS3Prefix(svc) })
	return svc
}

// cleanupS3Prefix removes every object the test wrote under its prefix.
func cleanupS3Prefix(svc *S3ArtifactService) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for obj := range svc.client.ListObjects(ctx, svc.bucket,
		minio.ListObjectsOptions{Prefix: svc.prefix, Recursive: true}) {
		if obj.Err != nil {
			continue
		}
		_ = svc.client.RemoveObject(ctx, svc.bucket, obj.Key, minio.RemoveObjectOptions{})
	}
}

func testS3Session(sessionID string) artifact.SessionInfo {
	return artifact.SessionInfo{AppName: "app-test", UserID: "u1", SessionID: sessionID}
}

func TestS3SaveLoadRoundTrip(t *testing.T) {
	svc := s3OrSkip(t)
	ctx := context.Background()
	info := testS3Session("s-roundtrip")

	v, err := svc.SaveArtifact(ctx, info, "report.md",
		&artifact.Artifact{Data: []byte("v0 data"), MimeType: "text/markdown"})
	if err != nil {
		t.Fatal(err)
	}
	if v != 0 {
		t.Fatalf("first save should return revision 0, got %d", v)
	}

	art, err := svc.LoadArtifact(ctx, info, "report.md", nil)
	if err != nil {
		t.Fatal(err)
	}
	if art == nil {
		t.Fatal("LoadArtifact returned nil for a saved artifact")
	}
	if string(art.Data) != "v0 data" || art.MimeType != "text/markdown" || art.Name != "report.md" {
		t.Errorf("unexpected artifact: %+v", art)
	}

	// A second save bumps the revision and becomes the latest.
	v, err = svc.SaveArtifact(ctx, info, "report.md",
		&artifact.Artifact{Data: []byte("v1 data"), MimeType: "text/markdown"})
	if err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Fatalf("second save should return revision 1, got %d", v)
	}
	art, err = svc.LoadArtifact(ctx, info, "report.md", nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(art.Data) != "v1 data" {
		t.Errorf("latest load should return v1 data, got %q", art.Data)
	}
}

func TestS3LoadSpecificVersion(t *testing.T) {
	svc := s3OrSkip(t)
	ctx := context.Background()
	info := testS3Session("s-versioned")

	for i, data := range []string{"first", "second", "third"} {
		v, err := svc.SaveArtifact(ctx, info, "data.bin",
			&artifact.Artifact{Data: []byte(data), MimeType: "application/octet-stream"})
		if err != nil {
			t.Fatal(err)
		}
		if v != i {
			t.Fatalf("save %d returned revision %d", i, v)
		}
	}

	zero := 0
	art, err := svc.LoadArtifact(ctx, info, "data.bin", &zero)
	if err != nil {
		t.Fatal(err)
	}
	if art == nil || string(art.Data) != "first" {
		t.Errorf("load v0 should return %q, got %+v", "first", art)
	}

	// A version that was never written reads as not found.
	five := 5
	art, err = svc.LoadArtifact(ctx, info, "data.bin", &five)
	if err != nil {
		t.Fatal(err)
	}
	if art != nil {
		t.Errorf("load of missing version should return nil, got %+v", art)
	}
}

func TestS3LoadMissing(t *testing.T) {
	svc := s3OrSkip(t)
	ctx := context.Background()

	art, err := svc.LoadArtifact(ctx, testS3Session("s-empty"), "nope.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	if art != nil {
		t.Errorf("load of missing artifact should return nil, got %+v", art)
	}
}

func TestS3ListVersions(t *testing.T) {
	svc := s3OrSkip(t)
	ctx := context.Background()
	info := testS3Session("s-listversions")

	versions, err := svc.ListVersions(ctx, info, "missing.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 0 {
		t.Errorf("missing artifact should have no versions, got %v", versions)
	}

	for i := 0; i < 3; i++ {
		if _, err := svc.SaveArtifact(ctx, info, "f.txt",
			&artifact.Artifact{Data: []byte{byte(i)}, MimeType: "text/plain"}); err != nil {
			t.Fatal(err)
		}
	}
	versions, err = svc.ListVersions(ctx, info, "f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(versions, []int{0, 1, 2}) {
		t.Errorf("expected ascending versions [0 1 2], got %v", versions)
	}
}

func TestS3ListArtifactKeys(t *testing.T) {
	svc := s3OrSkip(t)
	ctx := context.Background()
	info := testS3Session("s-listkeys")
	other := testS3Session("s-listkeys-other")

	// Two files in the session (a.txt with two versions) plus one file in
	// another session that must not leak in.
	for name, data := range map[string]string{"a.txt": "a0", "b.txt": "b0"} {
		if _, err := svc.SaveArtifact(ctx, info, name,
			&artifact.Artifact{Data: []byte(data), MimeType: "text/plain"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.SaveArtifact(ctx, info, "a.txt",
		&artifact.Artifact{Data: []byte("a1"), MimeType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SaveArtifact(ctx, other, "c.txt",
		&artifact.Artifact{Data: []byte("c0"), MimeType: "text/plain"}); err != nil {
		t.Fatal(err)
	}

	keys, err := svc.ListArtifactKeys(ctx, info)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(keys, []string{"a.txt", "b.txt"}) {
		t.Errorf("expected [a.txt b.txt], got %v", keys)
	}

	keys, err = svc.ListArtifactKeys(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(keys, []string{"c.txt"}) {
		t.Errorf("expected [c.txt] for the other session, got %v", keys)
	}
}

func TestS3DeleteArtifact(t *testing.T) {
	svc := s3OrSkip(t)
	ctx := context.Background()
	info := testS3Session("s-delete")

	for i := 0; i < 2; i++ {
		if _, err := svc.SaveArtifact(ctx, info, "gone.txt",
			&artifact.Artifact{Data: []byte{byte(i)}, MimeType: "text/plain"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := svc.SaveArtifact(ctx, info, "kept.txt",
		&artifact.Artifact{Data: []byte("k"), MimeType: "text/plain"}); err != nil {
		t.Fatal(err)
	}

	if err := svc.DeleteArtifact(ctx, info, "gone.txt"); err != nil {
		t.Fatal(err)
	}

	art, err := svc.LoadArtifact(ctx, info, "gone.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	if art != nil {
		t.Errorf("deleted artifact should load as nil, got %+v", art)
	}
	versions, err := svc.ListVersions(ctx, info, "gone.txt")
	if err != nil {
		t.Fatal(err)
	}
	if len(versions) != 0 {
		t.Errorf("deleted artifact should have no versions, got %v", versions)
	}
	keys, err := svc.ListArtifactKeys(ctx, info)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(keys, []string{"kept.txt"}) {
		t.Errorf("only kept.txt should remain, got %v", keys)
	}

	// Deleting a missing artifact is a no-op, not an error.
	if err := svc.DeleteArtifact(ctx, info, "never-existed.txt"); err != nil {
		t.Errorf("delete of missing artifact should be a no-op: %v", err)
	}
}
