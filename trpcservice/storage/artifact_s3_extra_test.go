package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

// SaveMedia stores inbound IM media under the media-scoped session tree and
// returns the compact "s3://bucket/key" reference the agent sees.
func TestS3SaveMediaReference(t *testing.T) {
	svc := s3OrSkip(t)
	ctx := context.Background()

	channel := "wecom"
	msgID := fmt.Sprintf("msg-%d", time.Now().UnixNano())
	ref, err := svc.SaveMedia(ctx, channel, msgID, "image.png", "image/png", []byte("png-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	want := "s3://" + testS3Bucket + "/" + svc.prefix + "inbound-media/" + channel + "/" + msgID + "/image.png/v0"
	if ref != want {
		t.Fatalf("media reference mismatch:\n got %s\nwant %s", ref, want)
	}

	// The reference resolves back to the bytes through the artifact path.
	info := artifact.SessionInfo{AppName: "inbound-media", UserID: channel, SessionID: msgID}
	art, err := svc.LoadArtifact(ctx, info, "image.png", nil)
	if err != nil {
		t.Fatal(err)
	}
	if art == nil || string(art.Data) != "png-bytes" || art.MimeType != "image/png" {
		t.Fatalf("loaded media mismatch: %+v", art)
	}

	// A second save of the same message bumps the revision inside the media
	// tree, and the reference reflects it.
	ref, err = svc.SaveMedia(ctx, channel, msgID, "image.png", "image/png", []byte("png-v2"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(ref, "/image.png/v1") {
		t.Fatalf("second media save must be revision 1, got %s", ref)
	}
}

// Invalid session info, filenames and nil artifacts are rejected before any
// network round trip.
func TestS3Validation(t *testing.T) {
	svc := s3OrSkip(t)
	ctx := context.Background()
	good := testS3Session("s-validate")
	badSession := artifact.SessionInfo{AppName: "app", UserID: ""} // missing fields

	if _, err := svc.SaveArtifact(ctx, badSession, "f.txt",
		&artifact.Artifact{Data: []byte("d")}); err == nil {
		t.Fatal("an incomplete session must be rejected")
	}
	if _, err := svc.SaveArtifact(ctx, good, "  ",
		&artifact.Artifact{Data: []byte("d")}); err == nil {
		t.Fatal("a blank filename must be rejected")
	}
	if _, err := svc.SaveArtifact(ctx, good, "bad\x00name",
		&artifact.Artifact{Data: []byte("d")}); err == nil {
		t.Fatal("a filename with a NUL byte must be rejected")
	}
	if _, err := svc.SaveArtifact(ctx, good, "f.txt", nil); err == nil {
		t.Fatal("a nil artifact must be rejected")
	}
	if _, err := svc.LoadArtifact(ctx, badSession, "f.txt", nil); err == nil {
		t.Fatal("LoadArtifact must validate the session")
	}
	if _, err := svc.LoadArtifact(ctx, good, "  ", nil); err == nil {
		t.Fatal("LoadArtifact must validate the filename")
	}
	if _, err := svc.ListVersions(ctx, badSession, "f.txt"); err == nil {
		t.Fatal("ListVersions must validate the session")
	}
	if _, err := svc.ListVersions(ctx, good, "bad\x00name"); err == nil {
		t.Fatal("ListVersions must validate the filename")
	}
	if _, err := svc.ListArtifactKeys(ctx, badSession); err == nil {
		t.Fatal("ListArtifactKeys must validate the session")
	}

	// parseArtifactVersion accepts only non-negative "v{n}" segments.
	if _, ok := parseArtifactVersion("v0"); !ok {
		t.Fatal("v0 must parse")
	}
	if _, ok := parseArtifactVersion("v12"); !ok {
		t.Fatal("v12 must parse")
	}
	for _, seg := range []string{"", "x1", "v", "v-1", "v1x"} {
		if _, ok := parseArtifactVersion(seg); ok {
			t.Fatalf("%q must not parse as a version", seg)
		}
	}
}

// Misconfiguration fails fast at startup with a named cause.
func TestS3NewServiceValidation(t *testing.T) {
	goodSecrets := staticSecrets{"minio-access": "trpc", "minio-secret": "trpc-dev-only"}

	if _, err := NewS3ArtifactService(S3ArtifactConfig{}); err == nil || !strings.Contains(err.Error(), "Endpoint") {
		t.Fatalf("missing endpoint/bucket must be rejected: %v", err)
	}
	if _, err := NewS3ArtifactService(S3ArtifactConfig{Endpoint: testS3Endpoint, Bucket: testS3Bucket}); err == nil ||
		!strings.Contains(err.Error(), "AccessKeyRef") {
		t.Fatalf("missing credential refs must be rejected: %v", err)
	}
	if _, err := NewS3ArtifactService(S3ArtifactConfig{
		Endpoint: testS3Endpoint, Bucket: testS3Bucket,
		AccessKeyRef: "a", SecretKeyRef: "b",
	}); err == nil || !strings.Contains(err.Error(), "Secrets") {
		t.Fatalf("a missing secrets resolver must be rejected: %v", err)
	}
	if _, err := NewS3ArtifactService(S3ArtifactConfig{
		Endpoint: testS3Endpoint, Bucket: testS3Bucket,
		AccessKeyRef: "nope", SecretKeyRef: "b", Secrets: goodSecrets,
	}); err == nil || !strings.Contains(err.Error(), "access key") {
		t.Fatalf("an unresolvable access key must be rejected: %v", err)
	}
	if _, err := NewS3ArtifactService(S3ArtifactConfig{
		Endpoint: testS3Endpoint, Bucket: testS3Bucket,
		AccessKeyRef: "minio-access", SecretKeyRef: "nope", Secrets: goodSecrets,
	}); err == nil || !strings.Contains(err.Error(), "secret key") {
		t.Fatalf("an unresolvable secret key must be rejected: %v", err)
	}
	if _, err := NewS3ArtifactService(S3ArtifactConfig{
		Endpoint: "localhost:1", Bucket: "no-such-bucket",
		AccessKeyRef: "minio-access", SecretKeyRef: "minio-secret", Secrets: goodSecrets,
	}); err == nil || !strings.Contains(err.Error(), "check bucket") {
		t.Fatalf("an unreachable endpoint must fail the bucket check: %v", err)
	}
}

// A missing bucket is created at startup (fail fast with a usable service);
// the configured prefix is normalized to end with a slash.
func TestS3NewServiceCreatesBucket(t *testing.T) {
	bucket := fmt.Sprintf("artifacts-test-%d", time.Now().UnixNano())
	svc, err := NewS3ArtifactService(S3ArtifactConfig{
		Endpoint:     testS3Endpoint,
		AccessKeyRef: "minio-access",
		SecretKeyRef: "minio-secret",
		Bucket:       bucket,
		Prefix:       "media", // no trailing slash on purpose
		Secrets: staticSecrets{
			"minio-access": "trpc",
			"minio-secret": "trpc-dev-only",
		},
	})
	if err != nil {
		t.Skipf("minio unavailable (%v) — set TRPC_TEST_S3_ENDPOINT (default %s), skipping integration test", err, testS3Endpoint)
	}
	t.Cleanup(func() { _ = svc.client.RemoveBucket(context.Background(), bucket) })

	if svc.prefix != "media/" {
		t.Fatalf("prefix must be normalized to end with '/', got %q", svc.prefix)
	}

	// The fresh bucket serves a save/load round trip.
	ctx := context.Background()
	info := testS3Session("s-newbucket")
	v, err := svc.SaveArtifact(ctx, info, "f.txt", &artifact.Artifact{Data: []byte("data"), MimeType: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	if v != 0 {
		t.Fatalf("first save in a fresh bucket must be v0, got %d", v)
	}
	exists, err := svc.client.BucketExists(ctx, bucket)
	if err != nil || !exists {
		t.Fatalf("bucket must exist after startup (exists=%v err=%v)", exists, err)
	}
}
