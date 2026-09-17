package s3

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"strings"
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore/contracttest"
)

func TestStoreContract(t *testing.T) {
	contracttest.Run(t, func(t testing.TB) objectstore.Store {
		store, err := New(newFakeAPI(), "artifact-test", Options{MaxBytes: 1024})
		if err != nil {
			t.Fatal(err)
		}
		return store
	})
}

func TestPutDoesNotReadBeforeConditionalCreate(t *testing.T) {
	api := newFakeAPI()
	store, err := New(api, "artifact-test", Options{})
	if err != nil {
		t.Fatal(err)
	}
	value := testObject(t, "tenant-a", "safe")
	if _, err := store.PutObject(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if api.getCalls != 0 {
		t.Fatalf("initial put performed %d reads", api.getCalls)
	}
	if _, err := store.PutObject(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if api.getCalls != 1 {
		t.Fatalf("conflict resolution reads=%d", api.getCalls)
	}
}

func TestProbeRequiresVersioning(t *testing.T) {
	api := newFakeAPI()
	api.versioning = types.BucketVersioningStatusSuspended
	store, err := New(api, "artifact-test", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Probe(context.Background()); err != runtime.ErrCapabilityUnsupported {
		t.Fatalf("probe err=%v", err)
	}
}

func TestPutRequiresReturnedVersion(t *testing.T) {
	api := newFakeAPI()
	api.omitPutVersion = true
	store, err := New(api, "artifact-test", Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutObject(context.Background(), testObject(t, "tenant-a", "safe")); err != runtime.ErrCapabilityUnsupported {
		t.Fatalf("put err=%v", err)
	}
}

func TestEndpointRequiresHTTPSOrExplicitLocalOptIn(t *testing.T) {
	if _, err := NewFromConfig(awssdk.Config{}, "artifact-test", "http://minio.invalid", true, Options{}); err != runtime.ErrInvalidEnvelope {
		t.Fatalf("insecure endpoint err=%v", err)
	}
	if _, err := NewFromConfig(awssdk.Config{}, "artifact-test", "http://minio.invalid", true,
		Options{AllowInsecure: true}); err != nil {
		t.Fatalf("explicit insecure endpoint err=%v", err)
	}
}

func testObject(t *testing.T, tenantID, content string) objectstore.Object {
	t.Helper()
	artifactSum := sha256.Sum256([]byte("artifact"))
	artifactID := "a1_" + base64.RawURLEncoding.EncodeToString(artifactSum[:])
	key, err := objectstore.StableKey(tenantID, artifactID)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte(content))
	return objectstore.Object{TenantID: tenantID, ObjectKey: key, ContentDigest: hex.EncodeToString(digest[:]), Content: []byte(content)}
}

type fakeObject struct {
	content, digest, version string
	created                  time.Time
}

type fakeAPI struct {
	objects        map[string]fakeObject
	versioning     types.BucketVersioningStatus
	omitPutVersion bool
	getCalls       int
}

func newFakeAPI() *fakeAPI {
	return &fakeAPI{objects: make(map[string]fakeObject), versioning: types.BucketVersioningStatusEnabled}
}

func (f *fakeAPI) PutObject(_ context.Context, in *awss3.PutObjectInput, _ ...func(*awss3.Options)) (*awss3.PutObjectOutput, error) {
	key := awssdk.ToString(in.Key)
	if _, ok := f.objects[key]; ok {
		return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "exists"}
	}
	content, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	value := fakeObject{content: string(content), digest: in.Metadata[digestMetadataKey], version: "v1", created: time.Now().UTC()}
	f.objects[key] = value
	if f.omitPutVersion {
		return &awss3.PutObjectOutput{}, nil
	}
	return &awss3.PutObjectOutput{VersionId: awssdk.String(value.version)}, nil
}

func (f *fakeAPI) GetObject(_ context.Context, in *awss3.GetObjectInput, _ ...func(*awss3.Options)) (*awss3.GetObjectOutput, error) {
	f.getCalls++
	value, ok := f.objects[awssdk.ToString(in.Key)]
	if !ok || (in.VersionId != nil && awssdk.ToString(in.VersionId) != value.version) {
		return nil, &smithy.GenericAPIError{Code: "NoSuchKey", Message: "missing"}
	}
	return &awss3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(value.content)), ContentLength: awssdk.Int64(int64(len(value.content))),
		LastModified: &value.created, Metadata: map[string]string{digestMetadataKey: value.digest}, VersionId: awssdk.String(value.version)}, nil
}

func (f *fakeAPI) HeadObject(_ context.Context, in *awss3.HeadObjectInput, _ ...func(*awss3.Options)) (*awss3.HeadObjectOutput, error) {
	value, ok := f.objects[awssdk.ToString(in.Key)]
	if !ok {
		return nil, &smithy.GenericAPIError{Code: "NotFound", Message: "missing"}
	}
	return &awss3.HeadObjectOutput{ContentLength: awssdk.Int64(int64(len(value.content))), LastModified: &value.created,
		Metadata: map[string]string{digestMetadataKey: value.digest}, VersionId: awssdk.String(value.version)}, nil
}

func (f *fakeAPI) DeleteObject(_ context.Context, in *awss3.DeleteObjectInput, _ ...func(*awss3.Options)) (*awss3.DeleteObjectOutput, error) {
	key := awssdk.ToString(in.Key)
	value, ok := f.objects[key]
	if !ok || awssdk.ToString(in.VersionId) != value.version {
		return nil, &smithy.GenericAPIError{Code: "NoSuchVersion", Message: "missing"}
	}
	delete(f.objects, key)
	return &awss3.DeleteObjectOutput{}, nil
}

func (f *fakeAPI) GetBucketVersioning(context.Context, *awss3.GetBucketVersioningInput, ...func(*awss3.Options)) (*awss3.GetBucketVersioningOutput, error) {
	return &awss3.GetBucketVersioningOutput{Status: f.versioning}, nil
}
