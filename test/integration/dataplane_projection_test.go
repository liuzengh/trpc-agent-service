//go:build integration

package integration

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	qdrantclient "github.com/qdrant/go-client/qdrant"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/artifactplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/datamigration"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/dataprojection"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/knowledgeplane"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
)

func TestKnowledgeProjectionUsesRealPostgresFenceAndQdrantDataPlane(t *testing.T) {
	db := openDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tenantID := "projection-" + uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants(id,name,status,config) VALUES($1,'projection integration','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	job, fence := createProjectionLease(t, db, tenantID, datamigration.DomainKnowledge)
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM data_migration_records WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM data_migrations WHERE id=$1`, job.ID)
		_, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, tenantID)
	})

	host := requiredEnv(t, "TEST_QDRANT_HOST")
	port := requiredIntEnv(t, "TEST_QDRANT_GRPC_PORT")
	collection := "tsa_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	adminClient, err := qdrantclient.NewClient(&qdrantclient.Config{Host: host, Port: port, PoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = adminClient.DeleteCollection(context.Background(), collection)
		_ = adminClient.Close()
	})
	store, err := knowledgeplane.NewQdrantScopedStore(ctx, tenantID, "support", knowledgeplane.QdrantConfig{
		Host: host, Port: port, Collection: collection, Dimension: 3,
		AllowInsecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	projector, err := dataprojection.NewKnowledgeProjector(func(context.Context, string, string) (*knowledgeplane.ScopedStore, error) {
		return store, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	target, err := dataprojection.NewTarget(db, projector)
	if err != nil {
		t.Fatal(err)
	}
	doc := &document.Document{ID: "faq-1", Content: "Refunds take three days.", Metadata: map[string]any{"kind": "faq"}}
	record, err := dataprojection.NewKnowledgeRecord("support", doc, []float64{1, 0, 0}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Upsert(ctx, tenantID, datamigration.DomainKnowledge, fence, []datamigration.Record{record}); err != nil {
		t.Fatal(err)
	}
	// The same migration batch can be delivered again after an ambiguous
	// process failure. The committed projection marker makes this a no-op.
	if err := target.Upsert(ctx, tenantID, datamigration.DomainKnowledge, fence, []datamigration.Record{record}); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	got, embedding, err := store.Get(ctx, "faq-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Content != "Refunds take three days." || got.Metadata["kind"] != "faq" || len(embedding) != 3 || embedding[0] != 1 {
		t.Fatalf("projected knowledge = %#v embedding=%#v", got, embedding)
	}
}

func TestArtifactProjectionUsesRealPostgresFenceAndMinIODataPlane(t *testing.T) {
	db := openDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tenantID := "artifact-projection-" + uuid.NewString()
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants(id,name,status,config) VALUES($1,'artifact projection integration','active','{}')`, tenantID); err != nil {
		t.Fatal(err)
	}
	job, fence := createProjectionLease(t, db, tenantID, datamigration.DomainArtifact)
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM data_migration_records WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM artifact_versions WHERE tenant_id=$1`, tenantID)
		_, _ = db.Exec(`DELETE FROM data_migrations WHERE id=$1`, job.ID)
		_, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, tenantID)
	})

	endpoint := requiredEnv(t, "TEST_MINIO_ENDPOINT")
	accessKey := requiredEnv(t, "TEST_MINIO_ACCESS_KEY")
	secretKey := requiredEnv(t, "TEST_MINIO_SECRET_KEY")
	bucket := "tsa-" + strings.ReplaceAll(uuid.NewString(), "-", "")
	objects, err := artifactplane.NewMinIOStore(ctx, artifactplane.MinIOConfig{
		Endpoint: endpoint, AccessKey: accessKey, SecretKey: secretKey,
		Bucket: bucket, AllowInsecure: true, CreateBucket: true, MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	adminClient, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, "")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adminClient.RemoveBucket(context.Background(), bucket) })
	service, err := artifactplane.NewService(tenantID, db, objects, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	projector, err := dataprojection.NewArtifactProjector(func(context.Context, string) (dataprojection.ArtifactVersionStore, error) {
		return service, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	target, err := dataprojection.NewTarget(db, projector)
	if err != nil {
		t.Fatal(err)
	}
	info := artifact.SessionInfo{AppName: "support", UserID: "owner-1", SessionID: "session-1"}
	record, err := dataprojection.NewArtifactRecord(info, "report.txt", 7,
		&artifact.Artifact{Data: []byte("immutable report"), MimeType: "text/plain"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Upsert(ctx, tenantID, datamigration.DomainArtifact, fence, []datamigration.Record{record}); err != nil {
		t.Fatal(err)
	}
	if err := target.Upsert(ctx, tenantID, datamigration.DomainArtifact, fence, []datamigration.Record{record}); err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
	version := 7
	loaded, err := service.LoadArtifact(ctx, info, "report.txt", &version)
	if err != nil || loaded == nil || string(loaded.Data) != "immutable report" {
		t.Fatalf("loaded=%#v error=%v", loaded, err)
	}
	tombstone, err := dataprojection.NewArtifactTombstone(info, "report.txt", 7, "text/plain", 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Upsert(ctx, tenantID, datamigration.DomainArtifact, fence, []datamigration.Record{tombstone}); err != nil {
		t.Fatal(err)
	}
	loaded, err = service.LoadArtifact(ctx, info, "report.txt", &version)
	if err != nil || loaded != nil {
		t.Fatalf("tombstoned load=%#v error=%v", loaded, err)
	}
}

func createProjectionLease(t *testing.T, db *sql.DB, tenantID string, domain datamigration.Domain) (datamigration.Job, datamigration.LeaseFence) {
	t.Helper()
	ctx := context.Background()
	job, err := datamigration.NewJob("migration-"+uuid.NewString(), tenantID, domain, "source-a", "target-a", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	store := datamigration.NewPostgresStore(db)
	if err := store.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	job, err = store.Claim(ctx, job.ID, "projection-integration", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	phase := datamigration.PhaseSnapshotCopy
	job, err = store.Advance(ctx, job.ID, job.LeaseOwner, job.LeaseVersion, datamigration.JobPatch{Phase: &phase})
	if err != nil {
		t.Fatal(err)
	}
	return job, datamigration.LeaseFence{MigrationID: job.ID, Owner: job.LeaseOwner, Version: job.LeaseVersion}
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		t.Fatalf("%s is required for integration-tag tests", name)
	}
	return value
}

func requiredIntEnv(t *testing.T, name string) int {
	t.Helper()
	value, err := strconv.Atoi(requiredEnv(t, name))
	if err != nil || value < 1 || value > 65535 {
		t.Fatalf("%s is invalid", name)
	}
	return value
}
