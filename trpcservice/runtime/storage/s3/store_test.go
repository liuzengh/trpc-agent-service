package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	awss3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

func TestStoreObjectContractAndTenantIsolation(t *testing.T) {
	store, client := newTestStore(t, "tenant-a", Options{})
	other, _ := newTestStoreWithClient(t, client, "tenant-b", Options{})
	ctx := context.Background()
	testObjectPutAndRead(t, store, client, ctx)
	testObjectKeyS3Limit(t, store, client, ctx)
	testObjectTenantIsolation(t, store, other, ctx)
}

func testObjectPutAndRead(t *testing.T, store *Store, client *fakeS3, ctx context.Context) runtimestorage.ObjectInfo {
	t.Helper()
	info, err := store.PutObject(ctx, "tenant-a", "media/report.txt", strings.NewReader("first"), "text/plain")
	if err != nil {
		t.Fatal(err)
	}
	if info.TenantID != "tenant-a" || info.ObjectKey != "media/report.txt" || info.Size != 5 || info.ETag != hexDigest([]byte("first")) {
		t.Fatalf("PutObject() = %#v", info)
	}
	retry, err := store.PutObject(ctx, "tenant-a", "media/report.txt", strings.NewReader("first"), "text/plain")
	if err != nil || !retry.CreatedAt.Equal(info.CreatedAt) || client.putCalls != 1 {
		t.Fatalf("idempotent PutObject() = %#v, %v, writes=%d", retry, err, client.putCalls)
	}
	replacement, err := store.PutObject(ctx, "tenant-a", "media/report.txt", strings.NewReader("second"), "text/plain")
	if err != nil || !replacement.CreatedAt.Equal(info.CreatedAt) || replacement.ETag != hexDigest([]byte("second")) || client.putCalls != 2 {
		t.Fatalf("replacement PutObject() = %#v, %v, writes=%d", replacement, err, client.putCalls)
	}
	body, got, err := store.GetObject(ctx, "tenant-a", "media/report.txt")
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(body)
	if closeErr := body.Close(); readErr != nil || closeErr != nil || string(data) != "second" || got != replacement {
		t.Fatalf("GetObject() = %q, %#v, %v, %v", data, got, readErr, closeErr)
	}
	return info
}

func testObjectTenantIsolation(t *testing.T, store, other *Store, ctx context.Context) {
	t.Helper()
	if _, _, err := other.GetObject(ctx, "tenant-b", "media/report.txt"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("cross-tenant GetObject() = %v", err)
	}
	if _, err := store.PutObject(ctx, "tenant-b", "media/report.txt", strings.NewReader("second"), "text/plain"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("foreign PutObject() = %v", err)
	}
	if err := other.DeleteObject(ctx, "tenant-b", "media/report.txt"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("cross-tenant DeleteObject() = %v", err)
	}
	if err := store.DeleteObject(ctx, "tenant-a", "media/report.txt"); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteObject(ctx, "tenant-a", "media/report.txt"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing DeleteObject() = %v", err)
	}
}

func TestStoreArtifactContractAndDefensiveCopies(t *testing.T) {
	store, _ := newTestStore(t, "tenant-a", Options{})
	ctx := context.Background()
	stored := testArtifactVersioning(t, store, ctx)
	testArtifactReads(t, store, ctx, stored)
	testArtifactListingAndDeletion(t, store, ctx)
}

func testObjectKeyS3Limit(t *testing.T, store *Store, client *fakeS3, ctx context.Context) {
	t.Helper()
	longKey := strings.Repeat("k", 1024)
	if _, err := store.PutObject(ctx, "tenant-a", longKey, strings.NewReader("long"), "text/plain"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("long object key PutObject() = %v", err)
	}
	if _, _, err := store.GetObject(ctx, "tenant-a", longKey); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("long object key GetObject() = %v", err)
	}
	if err := store.DeleteObject(ctx, "tenant-a", longKey); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("long object key DeleteObject() = %v", err)
	}
	if client.putCalls != 2 {
		t.Fatalf("long object key made %d remote writes", client.putCalls)
	}
}

func TestStoreArtifactRejectsIDAboveS3ByteLimit(t *testing.T) {
	store, client := newTestStore(t, "tenant-a", Options{})
	ctx := context.Background()
	artifactID := strings.Repeat("界", 256)
	if _, err := store.PutArtifact(ctx, testArtifact(artifactID, "session", "content")); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("long artifact ID PutArtifact() = %v", err)
	}
	if _, err := store.GetArtifact(ctx, "tenant-a", artifactID); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("long artifact ID GetArtifact() = %v", err)
	}
	if err := store.DeleteArtifact(ctx, "tenant-a", artifactID); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("long artifact ID DeleteArtifact() = %v", err)
	}
	if client.count() != 0 {
		t.Fatalf("long artifact ID made %d remote writes", client.count())
	}
}

func testArtifactVersioning(t *testing.T, store *Store, ctx context.Context) runtimestorage.ArtifactRecord {
	t.Helper()
	stored := putArtifact(t, store, ctx, testArtifact("b", "session-a", "first"))
	if stored.Version != 1 || stored.CreatedAt.IsZero() || stored.UpdatedAt.IsZero() {
		t.Fatalf("first PutArtifact() = %#v", stored)
	}
	if retry := putArtifact(t, store, ctx, testArtifact("b", "session-a", "first")); retry.Version != 1 || !retry.UpdatedAt.Equal(stored.UpdatedAt) {
		t.Fatalf("idempotent PutArtifact() = %#v", retry)
	}
	updated := putArtifact(t, store, ctx, testArtifact("b", "session-a", "second"))
	if updated.Version != 2 || !updated.CreatedAt.Equal(stored.CreatedAt) || !updated.UpdatedAt.After(stored.UpdatedAt) {
		t.Fatalf("replacement PutArtifact() = %#v", updated)
	}
	putArtifact(t, store, ctx, testArtifact("a", "session-a", "other"))
	return stored
}

func testArtifactReads(t *testing.T, store *Store, ctx context.Context, stored runtimestorage.ArtifactRecord) {
	t.Helper()
	got, err := store.GetArtifact(ctx, "tenant-a", "b")
	if err != nil || string(got.Content) != "second" || got.Version != 2 {
		t.Fatalf("GetArtifact() = %#v, %v", got, err)
	}
	got.Content[0] = 'X'
	again, err := store.GetArtifact(ctx, "tenant-a", "b")
	if err != nil || string(again.Content) != "second" {
		t.Fatalf("GetArtifact() defensive copy = %#v, %v", again, err)
	}
	if !stored.CreatedAt.Before(again.UpdatedAt) {
		t.Fatalf("artifact timestamps = %#v", again)
	}
}

func testArtifactListingAndDeletion(t *testing.T, store *Store, ctx context.Context) {
	t.Helper()
	values, err := store.ListArtifacts(ctx, "tenant-a", "session-a")
	if err != nil || len(values) != 2 || values[0].ArtifactID != "a" || values[1].ArtifactID != "b" {
		t.Fatalf("ListArtifacts() = %#v, %v", values, err)
	}
	values[0].Content[0] = 'X'
	if got, err := store.GetArtifact(ctx, "tenant-a", "a"); err != nil || string(got.Content) != "other" {
		t.Fatalf("ListArtifacts() defensive copy = %#v, %v", got, err)
	}
	if err := store.DeleteArtifact(ctx, "tenant-a", "a"); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteArtifact(ctx, "tenant-a", "a"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing DeleteArtifact() = %v", err)
	}
}

func putArtifact(t *testing.T, store *Store, ctx context.Context, value runtimestorage.ArtifactRecord) runtimestorage.ArtifactRecord {
	t.Helper()
	result, err := store.PutArtifact(ctx, value)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestStoreArtifactFailsClosedForCorruptMetadata(t *testing.T) {
	store, client := newTestStore(t, "tenant-a", Options{})
	ctx := context.Background()
	if _, err := store.PutArtifact(ctx, testArtifact("artifact", "session", "content")); err != nil {
		t.Fatal(err)
	}
	key := store.remoteKey("artifacts", "artifact")
	client.mutate(key, func(value *fakeObject) { value.Metadata[metadataName] = "%%%" })
	if _, err := store.GetArtifact(ctx, "tenant-a", "artifact"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("corrupt GetArtifact() = %v", err)
	}
	if _, err := store.PutArtifact(ctx, testArtifact("artifact", "session", "replacement")); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("corrupt PutArtifact() = %v", err)
	}
	client.mutate(key, func(value *fakeObject) {
		value.Metadata = validArtifactMetadataFor("session", "name.txt", "text/plain", []byte("content"), 1, value.CreatedAt)
	})
	client.mutate(key, func(value *fakeObject) { value.ContentType = "application/json" })
	if _, err := store.GetArtifact(ctx, "tenant-a", "artifact"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("mismatched MIME GetArtifact() = %v", err)
	}
}

func TestStoreRejectsInvalidAndOversizedTransfersWithoutWrites(t *testing.T) {
	store, client := newTestStore(t, "tenant-a", Options{MaxBytes: 3})
	ctx := context.Background()
	for _, call := range []func() error{
		func() error {
			_, err := store.PutObject(ctx, "tenant-a", "../invalid", strings.NewReader("x"), "text/plain")
			return err
		},
		func() error {
			_, err := store.PutObject(ctx, "tenant-a", "object", strings.NewReader("four"), "text/plain")
			return err
		},
		func() error {
			_, err := store.PutArtifact(ctx, runtimestorage.ArtifactRecord{TenantID: "tenant-a", ArtifactID: "a", Content: []byte("four")})
			return err
		},
	} {
		if err := call(); !errors.Is(err, runtimestorage.ErrInvalid) {
			t.Fatalf("invalid transfer error = %v", err)
		}
	}
	if client.count() != 0 {
		t.Fatalf("invalid transfer made %d remote writes", client.count())
	}
	largeMetadata := testArtifact("metadata", "session", "content")
	largeMetadata.Name = strings.Repeat("名", 512)
	largeMetadata.Content = []byte("x")
	if _, err := store.PutArtifact(ctx, largeMetadata); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("oversized S3 metadata PutArtifact() = %v", err)
	}
	if _, err := store.PutObject(ctx, "tenant-a", "object", failingReader{}, "text/plain"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("failed reader error = %v", err)
	}
	if client.count() != 0 {
		t.Fatalf("failed transfer made %d remote writes", client.count())
	}
}

func TestStoreRejectsArtifactIDsBeyondSharedLimit(t *testing.T) {
	store, client := newTestStore(t, "tenant-a", Options{})
	ctx := context.Background()
	artifactID := strings.Repeat("a", 257)
	value := testArtifact(artifactID, "session", "content")
	if _, err := store.PutArtifact(ctx, value); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("oversized artifact ID PutArtifact() = %v", err)
	}
	if _, err := store.GetArtifact(ctx, "tenant-a", artifactID); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("oversized artifact ID GetArtifact() = %v", err)
	}
	if err := store.DeleteArtifact(ctx, "tenant-a", artifactID); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("oversized artifact ID DeleteArtifact() = %v", err)
	}
	if client.count() != 0 {
		t.Fatalf("oversized artifact ID made %d remote writes", client.count())
	}
}

func TestStoreListArtifactsSharesReadDeadlineAcrossItems(t *testing.T) {
	store, client := newTestStore(t, "tenant-a", Options{ReadTimeout: time.Second})
	if _, err := store.PutArtifact(context.Background(), testArtifact("artifact", "session", "content")); err != nil {
		t.Fatal(err)
	}
	var listDeadline time.Time
	client.listHook = func(ctx context.Context, _ *awss3.ListObjectsV2Input) (*awss3.ListObjectsV2Output, error) {
		var ok bool
		listDeadline, ok = ctx.Deadline()
		if !ok {
			t.Fatal("ListArtifacts() context has no deadline")
		}
		return &awss3.ListObjectsV2Output{Contents: []awss3types.Object{{Key: awssdk.String(store.remoteKey("artifacts", "artifact"))}}}, nil
	}
	client.getHook = func(ctx context.Context) error {
		getDeadline, ok := ctx.Deadline()
		if !ok || !getDeadline.Equal(listDeadline) {
			t.Errorf("artifact read restarted deadline: list=%v get=%v", listDeadline, getDeadline)
		}
		return nil
	}
	if _, err := store.ListArtifacts(context.Background(), "tenant-a", ""); err != nil {
		t.Fatalf("ListArtifacts() = %v", err)
	}
}

func TestStoreCancellationAndTimeoutWinOverRemoteResults(t *testing.T) {
	store, client := newTestStore(t, "tenant-a", Options{ReadTimeout: time.Millisecond, WriteTimeout: time.Second})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.PutObject(canceled, "tenant-a", "object", strings.NewReader("x"), "text/plain"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled PutObject() = %v", err)
	}
	ctx, stop := context.WithCancel(context.Background())
	if _, err := store.PutObject(ctx, "tenant-a", "object", &cancelOnFirstRead{Reader: strings.NewReader("x"), cancel: stop}, "text/plain"); !errors.Is(err, context.Canceled) {
		t.Fatalf("read cancellation PutObject() = %v", err)
	}
	client.getHook = func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}
	if _, _, err := store.GetObject(context.Background(), "tenant-a", "object"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("read timeout GetObject() = %v", err)
	}
	if client.count() != 0 {
		t.Fatalf("canceled transfer made %d remote writes", client.count())
	}
}

func TestStoreRejectsRepeatedListTokensAndUnexpectedKeys(t *testing.T) {
	store, client := newTestStore(t, "tenant-a", Options{})
	prefix := store.remoteKey("artifacts", "") + "/"
	client.listHook = func(_ context.Context, input *awss3.ListObjectsV2Input) (*awss3.ListObjectsV2Output, error) {
		if awssdk.ToString(input.Prefix) != prefix {
			t.Fatalf("List prefix = %q", awssdk.ToString(input.Prefix))
		}
		return &awss3.ListObjectsV2Output{IsTruncated: awssdk.Bool(true), NextContinuationToken: awssdk.String("again")}, nil
	}
	if _, err := store.ListArtifacts(context.Background(), "tenant-a", ""); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("repeated list token error = %v", err)
	}
	client.listHook = func(context.Context, *awss3.ListObjectsV2Input) (*awss3.ListObjectsV2Output, error) {
		return &awss3.ListObjectsV2Output{Contents: []awss3types.Object{{Key: awssdk.String("tenants/foreign/artifacts/value")}}}, nil
	}
	if _, err := store.ListArtifacts(context.Background(), "tenant-a", ""); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("foreign listed key error = %v", err)
	}
}

func TestStoreProbeCloseAndConstructionBoundaries(t *testing.T) {
	if _, err := New(nil, "artifact-bucket", "tenant-a", Options{}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("nil client New() = %v", err)
	}
	client := newFakeS3()
	if _, err := New(client, "artifact-bucket", "tenant-a", Options{MaxBytes: math.MaxInt64}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("MaxBytes=math.MaxInt64 New() = %v", err)
	}
	if _, err := New(client, "UPPERCASE", "tenant-a", Options{}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid bucket New() = %v", err)
	}
	for _, endpoint := range []string{"http://minio:9000", "https://minio:9000/path", "https://user:password@minio:9000", "https://minio:9000?secret=value"} {
		_, err := NewFromConfig(awssdk.Config{}, "artifact-bucket", "tenant-a", endpoint, true, false, Options{})
		if !errors.Is(err, runtimestorage.ErrInvalid) {
			t.Fatalf("NewFromConfig(%q) = %v", endpoint, err)
		}
	}
	store, client := newTestStore(t, "tenant-a", Options{})
	client.probeErr = errors.New("unavailable")
	if err := store.Probe(context.Background()); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("Probe() = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutArtifact(context.Background(), testArtifact("after-close", "session", "content")); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("closed PutArtifact() = %v", err)
	}
}

func TestStoreNewFromConfigAndTransferHelpers(t *testing.T) {
	if maxDuration(time.Second, 2*time.Second) != 2*time.Second || maxDuration(2*time.Second, time.Second) != 2*time.Second {
		t.Fatal("maxDuration() did not select the longer duration")
	}
	for _, test := range []struct {
		name          string
		endpoint      string
		allowInsecure bool
		wantStore     bool
	}{
		{name: "https", endpoint: "https://s3.example.test", wantStore: true},
		{name: "http opt-in", endpoint: "http://minio:9000", allowInsecure: true, wantStore: true},
		{name: "empty endpoint defaults", wantStore: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			options := Options{ConnectTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second}
			if test.name == "empty endpoint defaults" {
				options = Options{}
			}
			store, err := NewFromConfig(awssdk.Config{}, "artifact-bucket", "tenant-a", test.endpoint, true, test.allowInsecure, options)
			if (store != nil) != test.wantStore || err != nil {
				t.Fatalf("NewFromConfig() = %v, %v", store, err)
			}
			if store != nil {
				t.Cleanup(func() { _ = store.Close() })
			}
		})
	}
	if store, err := NewFromConfig(awssdk.Config{}, "BAD_BUCKET", "tenant-a", "https://s3.example.test", true, false, Options{}); store != nil || !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid bucket NewFromConfig() = %v, %v", store, err)
	}

	store, _ := newTestStore(t, "tenant-a", Options{})
	if _, _, err := store.operationContext(nil, time.Second); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("nil operation context = %v", err)
	}
}

func TestStoreRemoteErrorsAreRedactedAndFailClosed(t *testing.T) {
	store, client := newTestStore(t, "tenant-a", Options{})
	ctx := context.Background()
	client.headErr = errors.New("remote access-key=secret")
	if _, err := store.PutObject(ctx, "tenant-a", "object", strings.NewReader("x"), "text/plain"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("head error PutObject() = %v", err)
	}
	client.headErr = nil
	client.putErr = errors.New("remote bucket=secret")
	if _, err := store.PutObject(ctx, "tenant-a", "object", strings.NewReader("x"), "text/plain"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("put error PutObject() = %v", err)
	}
	client.putErr = nil
	client.headNil = true
	if _, err := store.PutObject(ctx, "tenant-a", "other", strings.NewReader("x"), "text/plain"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("nil head PutObject() = %v", err)
	}
	client.headNil = false
	if _, err := store.PutObject(ctx, "tenant-a", "object", strings.NewReader("x"), "text/plain"); err != nil {
		t.Fatal(err)
	}
	client.getErr = errors.New("remote object secret")
	if _, _, err := store.GetObject(ctx, "tenant-a", "object"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("get error GetObject() = %v", err)
	}
	client.getErr = nil
	client.deleteErr = errors.New("remote delete secret")
	if err := store.DeleteObject(ctx, "tenant-a", "object"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("delete error DeleteObject() = %v", err)
	}
	client.deleteErr = nil
	client.listErr = errors.New("remote list secret")
	if _, err := store.ListArtifacts(ctx, "tenant-a", ""); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("list error ListArtifacts() = %v", err)
	}
	client.listErr = nil
	client.listNil = true
	if _, err := store.ListArtifacts(ctx, "tenant-a", ""); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("nil list ListArtifacts() = %v", err)
	}
	if err := store.Probe(nil); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("nil Probe() = %v", err)
	}
}

func TestStoreGetObjectRejectsInvalidRemoteResponses(t *testing.T) {
	store, client := newTestStore(t, "tenant-a", Options{})
	ctx := context.Background()
	digest := hexDigest([]byte("x"))
	created := time.Now().UTC().Format(time.RFC3339Nano)
	tests := []struct {
		name string
		out  *awss3.GetObjectOutput
	}{
		{name: "nil output"},
		{name: "digest mismatch", out: &awss3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("x")), ContentLength: awssdk.Int64(1), ContentType: awssdk.String("text/plain"), Metadata: map[string]string{metadataDigest: "bad", metadataCreated: created}}},
		{name: "invalid content type", out: &awss3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("x")), ContentLength: awssdk.Int64(1), ContentType: awssdk.String("\n"), Metadata: map[string]string{metadataDigest: digest, metadataCreated: created}}},
		{name: "invalid created time", out: &awss3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("x")), ContentLength: awssdk.Int64(1), ContentType: awssdk.String("text/plain"), Metadata: map[string]string{metadataDigest: digest, metadataCreated: "invalid"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client.getOutput = test.out
			client.getOutputSet = true
			defer func() {
				client.getOutput = nil
				client.getOutputSet = false
			}()
			if _, _, err := store.GetObject(ctx, "tenant-a", "object"); !errors.Is(err, runtimestorage.ErrStorage) {
				t.Fatalf("GetObject() = %v", err)
			}
		})
	}
	for _, test := range []struct {
		name     string
		tenantID string
		object   string
		ctx      context.Context
	}{
		{name: "foreign tenant", tenantID: "tenant-b", object: "object", ctx: ctx},
		{name: "invalid key", tenantID: "tenant-a", object: "../object", ctx: ctx},
		{name: "nil context", tenantID: "tenant-a", object: "object"},
	} {
		t.Run("invalid/"+test.name, func(t *testing.T) {
			if _, _, err := store.GetObject(test.ctx, test.tenantID, test.object); !errors.Is(err, runtimestorage.ErrInvalid) {
				t.Fatalf("GetObject() = %v", err)
			}
		})
	}
}

func TestStoreReadS3BodyRejectsMalformedResponses(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name string
		out  *awss3.GetObjectOutput
	}{
		{name: "nil output"},
		{name: "nil body", out: &awss3.GetObjectOutput{ContentLength: awssdk.Int64(1)}},
		{name: "nil length", out: &awss3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("x"))}},
		{name: "negative length", out: &awss3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("x")), ContentLength: awssdk.Int64(-1)}},
		{name: "oversized length", out: &awss3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("x")), ContentLength: awssdk.Int64(2)}},
		{name: "read error", out: &awss3.GetObjectOutput{Body: errorReadCloser{}, ContentLength: awssdk.Int64(1)}},
		{name: "close error", out: &awss3.GetObjectOutput{Body: closeErrorReader{Reader: strings.NewReader("x")}, ContentLength: awssdk.Int64(1)}},
		{name: "length mismatch", out: &awss3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("xy")), ContentLength: awssdk.Int64(1)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if data, err := readS3Body(ctx, test.out, 1); data != nil || !errors.Is(err, runtimestorage.ErrStorage) {
				t.Fatalf("readS3Body() = %q, %v", data, err)
			}
		})
	}
	valid, err := readS3Body(ctx, &awss3.GetObjectOutput{Body: io.NopCloser(strings.NewReader("x")), ContentLength: awssdk.Int64(1)}, 1)
	if err != nil || string(valid) != "x" {
		t.Fatalf("valid readS3Body() = %q, %v", valid, err)
	}
}

func TestStoreMetadataHelpersFailClosed(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Nanosecond)
	if artifactVersion(map[string]string{metadataVersion: "0"}) != 0 || artifactVersion(map[string]string{metadataVersion: "01"}) != 0 || artifactVersion(map[string]string{metadataVersion: "bad"}) != 0 {
		t.Fatal("invalid artifact versions were accepted")
	}
	if decodeMetadata("%") != "" {
		t.Fatal("invalid metadata decoded")
	}
	if _, ok := decodeMetadataValue("%"); ok {
		t.Fatal("invalid metadata value decoded")
	}
	if got := artifactTime("invalid", nil); got.IsZero() {
		t.Fatal("artifactTime() returned zero fallback")
	}
	if got := artifactTime("invalid", &now); !got.Equal(now) {
		t.Fatalf("artifactTime() fallback = %v, want %v", got, now)
	}
	if _, ok := parseArtifactTime("invalid"); ok {
		t.Fatal("invalid artifact time parsed")
	}
	if validDigest("bad") || validDigest(strings.Repeat("g", 64)) || !validDigest(strings.Repeat("a", 64)) {
		t.Fatal("validDigest() boundary mismatch")
	}
}

func TestTranslateClassifiesOnlySafeStorageErrors(t *testing.T) {
	if !errors.Is(translate(&fakeAPIError{code: "NoSuchKey"}), runtimestorage.ErrNotFound) {
		t.Fatal("NoSuchKey was not classified as not found")
	}
	if !errors.Is(translate(&fakeAPIError{code: "PreconditionFailed"}), runtimestorage.ErrConflict) {
		t.Fatal("PreconditionFailed was not classified as conflict")
	}
	if !errors.Is(translate(errors.New("remote access-key=secret")), runtimestorage.ErrStorage) {
		t.Fatal("unknown remote error was not redacted")
	}
}

func testArtifact(id, sessionID, content string) runtimestorage.ArtifactRecord {
	return runtimestorage.ArtifactRecord{TenantID: "tenant-a", ArtifactID: id, SessionID: sessionID, Name: "name.txt", MimeType: "text/plain", Content: []byte(content)}
}

func newTestStore(t *testing.T, tenantID string, options Options) (*Store, *fakeS3) {
	t.Helper()
	client := newFakeS3()
	return newTestStoreWithClient(t, client, tenantID, options)
}

func newTestStoreWithClient(t *testing.T, client *fakeS3, tenantID string, options Options) (*Store, *fakeS3) {
	t.Helper()
	store, err := New(client, "artifact-bucket", tenantID, options)
	if err != nil {
		t.Fatal(err)
	}
	return store, client
}

type fakeS3 struct {
	mu           sync.Mutex
	objects      map[string]fakeObject
	putCalls     int
	probeErr     error
	putErr       error
	getErr       error
	headErr      error
	deleteErr    error
	listErr      error
	headNil      bool
	listNil      bool
	getHook      func(context.Context) error
	listHook     func(context.Context, *awss3.ListObjectsV2Input) (*awss3.ListObjectsV2Output, error)
	getOutput    *awss3.GetObjectOutput
	getOutputSet bool
}

type fakeObject struct {
	Body        []byte
	ContentType string
	Metadata    map[string]string
	CreatedAt   time.Time
}

func newFakeS3() *fakeS3 { return &fakeS3{objects: make(map[string]fakeObject)} }

func (client *fakeS3) PutObject(ctx context.Context, input *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if client.putErr != nil {
		return nil, client.putErr
	}
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	client.putCalls++
	client.objects[awssdk.ToString(input.Key)] = fakeObject{Body: append([]byte(nil), data...), ContentType: awssdk.ToString(input.ContentType), Metadata: copyMetadata(input.Metadata), CreatedAt: time.Now().UTC()}
	return &awss3.PutObjectOutput{}, nil
}

func (client *fakeS3) GetObject(ctx context.Context, input *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	if client.getErr != nil {
		return nil, client.getErr
	}
	if client.getOutputSet {
		return client.getOutput, nil
	}
	if client.getHook != nil {
		if err := client.getHook(ctx); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client.mu.Lock()
	value, ok := client.objects[awssdk.ToString(input.Key)]
	client.mu.Unlock()
	if !ok {
		return nil, &fakeAPIError{code: "NoSuchKey"}
	}
	created := value.CreatedAt
	return &awss3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(append([]byte(nil), value.Body...))), ContentLength: awssdk.Int64(int64(len(value.Body))), ContentType: awssdk.String(value.ContentType), Metadata: copyMetadata(value.Metadata), LastModified: &created}, nil
}

func (client *fakeS3) HeadObject(ctx context.Context, input *awss3.HeadObjectInput, _ ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if client.headErr != nil {
		return nil, client.headErr
	}
	if client.headNil {
		return nil, nil
	}
	client.mu.Lock()
	value, ok := client.objects[awssdk.ToString(input.Key)]
	client.mu.Unlock()
	if !ok {
		return nil, &fakeAPIError{code: "NoSuchKey"}
	}
	created := value.CreatedAt
	return &awss3.HeadObjectOutput{ContentLength: awssdk.Int64(int64(len(value.Body))), ContentType: awssdk.String(value.ContentType), Metadata: copyMetadata(value.Metadata), LastModified: &created}, nil
}

func (client *fakeS3) DeleteObject(ctx context.Context, input *awss3.DeleteObjectInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if client.deleteErr != nil {
		return nil, client.deleteErr
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	delete(client.objects, awssdk.ToString(input.Key))
	return &awss3.DeleteObjectOutput{}, nil
}

func (client *fakeS3) ListObjectsV2(ctx context.Context, input *awss3.ListObjectsV2Input, _ ...func(*awss3.Options)) (*awss3.ListObjectsV2Output, error) {
	if client.listHook != nil {
		return client.listHook(ctx, input)
	}
	if client.listErr != nil {
		return nil, client.listErr
	}
	if client.listNil {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	prefix := awssdk.ToString(input.Prefix)
	client.mu.Lock()
	keys := make([]string, 0)
	for key := range client.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	client.mu.Unlock()
	sort.Strings(keys)
	contents := make([]awss3types.Object, 0, len(keys))
	for _, key := range keys {
		contents = append(contents, awss3types.Object{Key: awssdk.String(key)})
	}
	return &awss3.ListObjectsV2Output{Contents: contents}, nil
}

func (client *fakeS3) HeadBucket(ctx context.Context, _ *awss3.HeadBucketInput, _ ...func(*awss3.Options)) (*awss3.HeadBucketOutput, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if client.probeErr != nil {
		return nil, client.probeErr
	}
	return &awss3.HeadBucketOutput{}, nil
}

func (client *fakeS3) count() int {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.putCalls
}

func (client *fakeS3) mutate(key string, update func(*fakeObject)) {
	client.mu.Lock()
	defer client.mu.Unlock()
	value := client.objects[key]
	update(&value)
	client.objects[key] = value
}

func copyMetadata(value map[string]string) map[string]string {
	copy := make(map[string]string, len(value))
	for key, item := range value {
		copy[key] = item
	}
	return copy
}

func validArtifactMetadataFor(sessionID, name, mimeType string, content []byte, version int64, timestamp time.Time) map[string]string {
	return map[string]string{
		metadataDigest:  hexDigest(content),
		metadataSession: encodeMetadata(sessionID),
		metadataName:    encodeMetadata(name),
		metadataMime:    encodeMetadata(mimeType),
		metadataVersion: "1",
		metadataCreated: timestamp.Format(time.RFC3339Nano),
		metadataUpdated: timestamp.Format(time.RFC3339Nano),
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("reader failure") }

type errorReadCloser struct{}

func (errorReadCloser) Read([]byte) (int, error) { return 0, errors.New("reader failure") }
func (errorReadCloser) Close() error             { return nil }

type closeErrorReader struct{ io.Reader }

func (closeErrorReader) Close() error { return errors.New("close failure") }

type cancelOnFirstRead struct {
	io.Reader
	cancel context.CancelFunc
	once   sync.Once
}

func (reader *cancelOnFirstRead) Read(value []byte) (int, error) {
	count, err := reader.Reader.Read(value)
	reader.once.Do(reader.cancel)
	return count, err
}

type fakeAPIError struct{ code string }

func (err *fakeAPIError) Error() string                 { return err.code }
func (err *fakeAPIError) ErrorCode() string             { return err.code }
func (err *fakeAPIError) ErrorMessage() string          { return err.code }
func (err *fakeAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultClient }
