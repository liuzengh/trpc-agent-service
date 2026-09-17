package integration

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	serviceruntime "github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	storageartifact "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact"
	artifactpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/storage/artifact/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore"
	objects3 "github.com/liuzengh/trpc-agent-service/trpcservice/storage/objectstore/s3"
)

// TestComposeArtifactObjectStoreTenantIsolation verifies the composed write
// path: PostgreSQL holds tenant-scoped immutable metadata while both tenants
// share one versioned MinIO bucket. It must be impossible to read tenant A's
// object with tenant B's authority, even when the opaque object key is known.
func TestComposeArtifactObjectStoreTenantIsolation(t *testing.T) {
	if os.Getenv("TRPC_ARTIFACT_E2E") != "1" {
		t.Skip("TRPC_ARTIFACT_E2E=1 is required")
	}
	db := runtimeTestDB(t)
	objects := composeMinIOObjectStore(t)
	store := artifactpostgres.NewWithObjectStore(db, objects)
	stamp := time.Now().UTC().UnixNano()
	// tenant_id must satisfy tenant_id_format_ck (t_ + 26-char Crockford ULID)
	// and agent_app_id must satisfy agent_app_agent_app_id_check (app_ + 26-char
	// Crockford ULID); tenant_key / agent_app_key must be lowercase kebab-case.
	// Random IDs keep reruns collision-free on shared databases.
	tenantA, tenantB := composeCrockfordID(t, "t_"), composeCrockfordID(t, "t_")
	appA, appB := composeCrockfordID(t, "app_"), composeCrockfordID(t, "app_")
	requestA, requestB := fmt.Sprintf("request-a-%d", stamp), fmt.Sprintf("request-b-%d", stamp)
	for _, value := range []struct{ tenantID, appID, appKey, requestID string }{
		{tenantA, appA, "artifact-acceptance-a", requestA}, {tenantB, appB, "artifact-acceptance-b", requestB},
	} {
		prepareComposeArtifactTenant(t, db, value.tenantID, value.appID, value.appKey, value.requestID)
	}
	t.Cleanup(func() {
		for _, value := range []struct{ tenantID string }{{tenantA}, {tenantB}} {
			_, _ = db.ExecContext(context.Background(), `DELETE FROM artifact_object_upload WHERE tenant_id=$1`, value.tenantID)
			_, _ = db.ExecContext(context.Background(), `DELETE FROM media_artifact WHERE tenant_id=$1`, value.tenantID)
			_, _ = db.ExecContext(context.Background(), `DELETE FROM inbox WHERE tenant_id=$1`, value.tenantID)
			_, _ = db.ExecContext(context.Background(), `DELETE FROM agent_app WHERE tenant_id=$1`, value.tenantID)
			_, _ = db.ExecContext(context.Background(), `DELETE FROM tenant WHERE tenant_id=$1`, value.tenantID)
		}
	})

	recordA := composeArtifactRecord(t, tenantA, requestA, []byte("tenant-a-private-artifact"))
	recordB := composeArtifactRecord(t, tenantB, requestB, []byte("tenant-b-private-artifact"))
	storedA, err := store.PutArtifact(context.Background(), recordA)
	if err != nil {
		t.Fatalf("put tenant A artifact: %v", err)
	}
	storedB, err := store.PutArtifact(context.Background(), recordB)
	if err != nil {
		t.Fatalf("put tenant B artifact: %v", err)
	}
	t.Cleanup(func() {
		for _, value := range []storageartifact.Record{storedA, storedB} {
			key, keyErr := objectstore.StableKey(value.TenantID, value.ArtifactID)
			if keyErr == nil {
				_ = objects.DeleteObject(context.Background(), value.TenantID, key, value.ContentDigest)
			}
		}
	})

	keyA, err := objectstore.StableKey(tenantA, storedA.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := objectstore.StableKey(tenantB, storedB.ArtifactID)
	if err != nil {
		t.Fatal(err)
	}
	if keyA == keyB || strings.Contains(keyA, tenantA) || strings.Contains(keyB, tenantB) {
		t.Fatalf("object keys must be opaque and tenant-distinct: %q / %q", keyA, keyB)
	}
	if got, err := objects.GetObject(context.Background(), tenantA, keyA); err != nil || string(got.Content) != string(recordA.Content) {
		t.Fatalf("read tenant A object=%#v err=%v", got, err)
	}
	if _, err := objects.GetObject(context.Background(), tenantB, keyA); !errors.Is(err, serviceruntime.ErrTenantScope) {
		t.Fatalf("cross-tenant object read error=%v, want tenant scope", err)
	}
	if _, err := store.GetArtifact(context.Background(), tenantB, storedA.ArtifactID); !errors.Is(err, serviceruntime.ErrNotFound) {
		t.Fatalf("cross-tenant metadata read error=%v, want not found", err)
	}
}

func composeMinIOObjectStore(t *testing.T) objectstore.Store {
	t.Helper()
	endpoint, bucket := os.Getenv("TRPC_S3_ENDPOINT"), os.Getenv("TRPC_S3_BUCKET")
	accessKey, secretKey := os.Getenv("TRPC_S3_ACCESS_KEY"), os.Getenv("TRPC_S3_SECRET_KEY")
	if endpoint == "" || bucket == "" || accessKey == "" || secretKey == "" {
		t.Fatal("MinIO Compose configuration is required")
	}
	credentials := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: accessKey, SecretAccessKey: secretKey, Source: "compose-artifact-acceptance"}, nil
	})
	config := aws.Config{Region: "us-east-1", Credentials: credentials}
	raw := awss3.NewFromConfig(config, func(value *awss3.Options) {
		value.UsePathStyle, value.BaseEndpoint = true, aws.String(endpoint)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := raw.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(bucket)}); err != nil {
		var exists *types.BucketAlreadyOwnedByYou
		if !errors.As(err, &exists) {
			t.Fatalf("create MinIO bucket: %v", err)
		}
	}
	if _, err := raw.PutBucketVersioning(ctx, &awss3.PutBucketVersioningInput{Bucket: aws.String(bucket),
		VersioningConfiguration: &types.VersioningConfiguration{Status: types.BucketVersioningStatusEnabled}}); err != nil {
		t.Fatalf("enable MinIO bucket versioning: %v", err)
	}
	store, err := objects3.NewFromConfig(config, bucket, endpoint, true, objects3.Options{MaxBytes: 1 << 20, AllowInsecure: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Probe(ctx); err != nil {
		t.Fatalf("MinIO versioned bucket probe: %v", err)
	}
	return store
}

// composeCrockfordID mints a constraint-conformant identifier: prefix followed
// by 26 Crockford-Base32 characters (no I/L/O/U), with the first character in
// [0-7] to match the migration CHECK regexes for tenant_id and agent_app_id.
func composeCrockfordID(t *testing.T, prefix string) string {
	t.Helper()
	const alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	raw := make([]byte, 26)
	if _, err := crand.Read(raw); err != nil {
		t.Fatalf("read random bytes: %v", err)
	}
	chars := make([]byte, 26)
	chars[0] = '0' + raw[0]%8
	for index := 1; index < 26; index++ {
		chars[index] = alphabet[int(raw[index])%32]
	}
	return prefix + string(chars)
}

func prepareComposeArtifactTenant(t *testing.T, db *sql.DB, tenantID, appID, appKey, requestID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `INSERT INTO tenant(tenant_id,tenant_key,display_name) VALUES($1,$2,$3)`, tenantID, appKey, "Artifact Acceptance"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO agent_app(tenant_id,agent_app_id,agent_app_key,display_name) VALUES($1,$2,$3,'Artifact App')`, tenantID, appID, appKey); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO inbox(tenant_id,channel,external_account_id,external_message_id,request_id,agent_app_id,session_id,state,payload_ref,payload_digest,key_version)
VALUES($1,'artifact','account',$2,$2,$3,'session','dispatch_pending','inbound://artifact',repeat('a',64),1)`, tenantID, requestID, appID); err != nil {
		t.Fatal(err)
	}
}

func composeArtifactRecord(t *testing.T, tenantID, requestID string, content []byte) storageartifact.Record {
	t.Helper()
	source := sha256.Sum256([]byte("source/" + requestID))
	sourceDigest := hex.EncodeToString(source[:])
	artifactID, artifactRef, err := storageartifact.StableIdentity(tenantID, requestID, 0, sourceDigest)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(content)
	return storageartifact.Record{TenantID: tenantID, RequestID: requestID, ArtifactID: artifactID, ArtifactRef: artifactRef, Ordinal: 0,
		SourceDigest: sourceDigest, ContentDigest: hex.EncodeToString(digest[:]), MediaType: "text/plain", Kind: "file", Content: content,
		MalwareScanVersion: "compose-acceptance", DLPVersion: "compose-acceptance"}
}
