package minio

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// The client is tested against a real MinIO for the reason the signing is
// hand-written: a wrong canonical request is rejected by the server with a
// signed error, and no in-process stub would reproduce that gate. Start one:
//
//	docker run -d --name tas-minio-test -p 9010:9000 \
//	  -e MINIO_ROOT_USER=tasminio -e MINIO_ROOT_PASSWORD=tasminiopw \
//	  minio/minio:RELEASE.2024-08-17T01-24-54Z server /data
//	MINIO_TEST_ENDPOINT=http://127.0.0.1:9010 MINIO_TEST_USER=tasminio \
//	  MINIO_TEST_PASSWORD=tasminiopw go test ./trpcservice/storage/minio/ -v
func newTestClient(t *testing.T) *Client {
	t.Helper()
	endpoint := os.Getenv("MINIO_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("MINIO_TEST_ENDPOINT not set; skipping real-minio integration test")
	}
	c, err := New(endpoint,
		os.Getenv("MINIO_TEST_USER"), os.Getenv("MINIO_TEST_PASSWORD"),
		"us-east-1", "tas-test-bucket")
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	return c
}

func TestBucketAndObjectRoundTrip(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	if err := c.EnsureBucket(ctx); err != nil {
		t.Fatalf("ensure bucket: %v", err)
	}
	// Idempotent: a second ensure is the normal case on every process start.
	if err := c.EnsureBucket(ctx); err != nil {
		t.Fatalf("ensure bucket again: %v", err)
	}

	key := "acme/assistant/docs/seed-1/g1/chunks.txt"
	content := []byte("# 标题\n\n正文 with ascii and 中文 mixed — and an em dash.\n")
	if err := c.Put(ctx, key, content, "text/plain"); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := c.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != string(content) {
		t.Fatalf("round trip mismatch:\n got %q\nwant %q", got, content)
	}
	// Overwrite is an ordinary put.
	if err := c.Put(ctx, key, []byte("second"), "text/plain"); err != nil {
		t.Fatal(err)
	}
	if got, _ := c.Get(ctx, key); string(got) != "second" {
		t.Fatalf("overwrite read = %q", got)
	}

	exists, err := c.Exists(ctx, key)
	if err != nil || !exists {
		t.Fatalf("exists before delete = %v, %v", exists, err)
	}
	if err := c.Delete(ctx, key); err != nil {
		t.Fatalf("delete: %v", err)
	}
	// Deleting a missing key is success — a cleanup job that runs twice must
	// not fail on its second run.
	if err := c.Delete(ctx, key); err != nil {
		t.Fatalf("second delete: %v", err)
	}
	exists, err = c.Exists(ctx, key)
	if err != nil || exists {
		t.Fatalf("exists after delete = %v, %v", exists, err)
	}
	if _, err := c.Get(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get after delete = %v, want ErrNotFound", err)
	}
}

func TestKeyCharsetIsEnforced(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()
	bad := []string{
		"", "a//b", "../escape", "with space.txt", "汉字.txt", "tab\tkey",
	}
	for _, key := range bad {
		if err := ValidateKey(key); err == nil {
			t.Fatalf("key %q passed validation", key)
		}
		if err := c.Put(ctx, key, []byte("x"), ""); err == nil {
			t.Fatalf("put with key %q succeeded", key)
		}
	}
	long := strings.Repeat("a", 513)
	if err := ValidateKey(long); err == nil {
		t.Fatal("over-long key passed validation")
	}
}

func TestWrongCredentialsAreRejected(t *testing.T) {
	endpoint := os.Getenv("MINIO_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("MINIO_TEST_ENDPOINT not set")
	}
	c, err := New(endpoint, "tasminio", "wrong-password", "us-east-1", "tas-test-bucket")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = c.Put(ctx, "acme/x.txt", []byte("x"), "text/plain")
	if err == nil {
		t.Fatal("a put with wrong credentials succeeded; the signer is not being checked by the server")
	}
}
