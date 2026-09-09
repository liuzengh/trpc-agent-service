//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
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
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/migrationruntime"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/runtimeplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
	qdrantstore "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/qdrant"
)

type onlineDataPlaneFixture struct {
	db                               *sql.DB
	ctx                              context.Context
	tenantID                         string
	runtime                          *migrationruntime.Runtime
	catalog                          *runtimeplane.Catalog
	knowledge                        vectorstore.VectorStore
	sourceKnowledge, targetKnowledge vectorstore.VectorStore
	artifacts                        artifact.Service
	sourceArtifacts, targetArtifacts *artifactplane.Service
	minio                            *minio.Client
	sourceBucket, targetBucket       string
}

func newOnlineDataPlaneFixture(t *testing.T) *onlineDataPlaneFixture {
	t.Helper()
	db := openDatabase(t)
	gateDB := openMigrationGateDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	f := &onlineDataPlaneFixture{db: db, ctx: ctx, tenantID: "online-plane-" + uuid.NewString()}
	qhost, qport := requiredEnv(t, "TEST_QDRANT_HOST"), requiredIntEnv(t, "TEST_QDRANT_GRPC_PORT")
	qadmin, err := qdrantclient.NewClient(&qdrantclient.Config{Host: qhost, Port: qport, PoolSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	stem := strings.ReplaceAll(uuid.NewString(), "-", "")
	collections := []string{"live_source_" + stem, "live_target_" + stem, "live_bad_" + stem}
	t.Cleanup(func() {
		for _, collection := range collections {
			_ = qadmin.DeleteCollection(context.Background(), collection)
		}
		_ = qadmin.Close()
	})
	f.sourceBucket, f.targetBucket = "live-source-"+stem, "live-target-"+stem
	endpoint, access, secret := requiredEnv(t, "TEST_MINIO_ENDPOINT"), requiredEnv(t, "TEST_MINIO_ACCESS_KEY"), requiredEnv(t, "TEST_MINIO_SECRET_KEY")
	f.minio, err = minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(access, secret, "")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for _, bucket := range []string{f.sourceBucket, f.targetBucket} {
			for object := range f.minio.ListObjects(cleanup, bucket, minio.ListObjectsOptions{Recursive: true}) {
				if object.Err == nil {
					_ = f.minio.RemoveObject(cleanup, bucket, object.Key, minio.RemoveObjectOptions{})
				}
			}
			_ = f.minio.RemoveBucket(cleanup, bucket)
		}
	})
	var definitions []map[string]any
	for index, id := range []string{"knowledge-source", "knowledge-target", "knowledge-bad"} {
		dimension := 3
		if index == 2 {
			dimension = 4
		}
		definitions = append(definitions, map[string]any{"id": id, "backend": "qdrant", "endpoint": qdrantEndpoint(qhost, qport), "tls": false, "allowInsecure": true, "collection": collections[index], "dimension": dimension,
			"embeddingEndpoint": "http://127.0.0.1:18099/v1", "embeddingModel": "embedding-test", "embeddingAPIKeyEnv": "EMBEDDING_KEY", "tenantIds": []string{f.tenantID}})
	}
	alias := map[string]any{}
	for key, value := range definitions[0] {
		alias[key] = value
	}
	alias["id"] = "knowledge-alias"
	definitions = append(definitions, alias)
	for index, id := range []string{"artifact-source", "artifact-target"} {
		bucket := f.sourceBucket
		if index == 1 {
			bucket = f.targetBucket
		}
		definitions = append(definitions, map[string]any{"id": id, "backend": "s3", "endpoint": endpoint, "tls": false, "allowInsecure": true, "bucket": bucket,
			"accessKeyEnv": "MINIO_ACCESS_KEY", "secretKeyEnv": "MINIO_SECRET_KEY", "createBucket": true, "maxBytes": 1048576, "tenantIds": []string{f.tenantID}})
	}
	manifest, err := json.Marshal(definitions)
	if err != nil {
		t.Fatal(err)
	}
	f.catalog, err = runtimeplane.LoadProfiles(string(manifest), func(name string) (string, bool) {
		switch name {
		case "EMBEDDING_KEY":
			return "local-test-only", true
		case "MINIO_ACCESS_KEY":
			return access, true
		case "MINIO_SECRET_KEY":
			return secret, true
		}
		return "", false
	})
	if err != nil {
		t.Fatal(err)
	}
	profiles, err := storage.LoadBackendProfiles(`[{"id":"unused-platform","backend":"postgres","connectionEnv":"PLATFORM_DB","allowInsecure":true}]`, func(string) (string, bool) { return os.LookupEnv("TEST_DATABASE_URL") })
	if err != nil {
		t.Fatal(err)
	}
	value := &tenant.Tenant{ID: f.tenantID, ConfigVersion: 1, Storage: tenant.StorageConfig{KnowledgeBackend: "qdrant", KnowledgeProfile: "knowledge-source", ArtifactBackend: "s3", ArtifactProfile: "artifact-source"}}
	config, err := json.Marshal(map[string]any{"storage": value.Storage})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, `INSERT INTO tenants(id,name,status,config,config_version) VALUES($1,'live data plane','active',$2,1)`, f.tenantID, config); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = db.Exec(`DELETE FROM audit_logs WHERE tenant_id=$1`, f.tenantID)
		_, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, f.tenantID)
	})
	f.runtime, err = migrationruntime.New(migrationruntime.Options{DB: db, GateDB: gateDB, StorageProfiles: profiles, DataPlaneProfiles: f.catalog, BatchSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.runtime.Close() })
	f.sourceKnowledge, err = f.catalog.OpenKnowledgeStore(ctx, f.tenantID, "support", "knowledge-source")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.sourceKnowledge.Close() })
	f.targetKnowledge, err = f.catalog.OpenKnowledgeStore(ctx, f.tenantID, "support", "knowledge-target")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.targetKnowledge.Close() })
	f.knowledge, err = f.runtime.DataPlane.DecorateKnowledge(ctx, f.tenantID, "support", "knowledge-source", f.sourceKnowledge)
	if err != nil {
		t.Fatal(err)
	}
	f.sourceArtifacts, err = f.catalog.OpenArtifactService(ctx, f.tenantID, "artifact-source", db)
	if err != nil {
		t.Fatal(err)
	}
	f.targetArtifacts, err = f.catalog.OpenArtifactService(ctx, f.tenantID, "artifact-target", db)
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := runtimeplane.NewProfileResolver(f.catalog, db, f.runtime.DataPlaneOption())
	if err != nil {
		t.Fatal(err)
	}
	lease, err := resolver.Acquire(ctx, runtimeplane.Request{Tenant: value, AgentAppID: "support", NeedArtifact: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release() })
	f.artifacts = lease.Artifact
	return f
}

func (f *onlineDataPlaneFixture) create(t *testing.T, domain datamigration.Domain, target string) (string, error) {
	t.Helper()
	backend, source := "qdrant", "knowledge-source"
	if domain == datamigration.DomainArtifact {
		backend, source = "s3", "artifact-source"
	}
	id := "online-" + uuid.NewString()
	_, err := f.runtime.Coordinator.Create(f.ctx, datamigration.LiveRequest{ID: id, TenantID: f.tenantID, Domain: domain, SourceProfile: source, TargetProfile: target, SourceBackend: backend, TargetBackend: backend, ExpectedConfigVersion: 1, Actor: "integration-operator", Reason: "verify production online data plane"})
	return id, err
}
func (f *onlineDataPlaneFixture) advance(t *testing.T, id string, phase datamigration.Phase) {
	t.Helper()
	for step := 0; step < 100; step++ {
		job, err := f.runtime.Coordinator.Get(f.ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if job.Phase == phase {
			return
		}
		if _, err = f.runtime.Coordinator.RunOnce(f.ctx, id); err != nil {
			t.Fatalf("phase %s: %v", job.Phase, err)
		}
	}
	t.Fatalf("did not reach %s", phase)
}
func addOnlineDocument(t *testing.T, ctx context.Context, store vectorstore.VectorStore, id, kind string) {
	t.Helper()
	if err := store.Add(ctx, &document.Document{ID: id, Content: id, Metadata: map[string]any{"kind": kind}}, []float64{1, 0, 0}); err != nil {
		t.Fatal(err)
	}
}
func assertOnlineDocument(t *testing.T, ctx context.Context, store vectorstore.VectorStore, id string, present bool) {
	t.Helper()
	doc, _, err := store.Get(ctx, id)
	if !present {
		if !errors.Is(err, qdrantstore.ErrNotFound) && !(err == nil && doc == nil) {
			t.Fatalf("deleted %s remains: doc=%v err=%v", id, doc, err)
		}
		return
	}
	if err != nil || doc == nil || doc.ID != id {
		t.Fatalf("missing %s: %v", id, err)
	}
}

func TestOnlineKnowledgeMigrationUsesProductionCaptureAndProfileRouting(t *testing.T) {
	for _, finish := range []string{"rollback", "complete"} {
		t.Run(finish, func(t *testing.T) {
			f := newOnlineDataPlaneFixture(t)
			for _, target := range []string{"knowledge-alias", "knowledge-bad"} {
				if _, err := f.create(t, datamigration.DomainKnowledge, target); err == nil {
					t.Fatalf("accepted incompatible or aliased target %s", target)
				}
			}
			addOnlineDocument(t, f.ctx, f.knowledge, "keep", "retain")
			addOnlineDocument(t, f.ctx, f.knowledge, "remove-a", "remove")
			addOnlineDocument(t, f.ctx, f.knowledge, "remove-b", "remove")
			id, err := f.create(t, datamigration.DomainKnowledge, "knowledge-target")
			if err != nil {
				t.Fatal(err)
			}
			f.advance(t, id, datamigration.PhaseValidate)
			addOnlineDocument(t, f.ctx, f.knowledge, "during-validation", "retain")
			original, embedding, err := f.knowledge.Get(f.ctx, "keep")
			if err != nil || original == nil {
				t.Fatalf("load document before unsupported update: %v", err)
			}
			if updated, err := f.knowledge.UpdateByFilter(f.ctx, vectorstore.WithUpdateByFilterDocumentIDs([]string{"keep"}), vectorstore.WithUpdateByFilterUpdates(map[string]any{"content": "updated"})); err == nil || !strings.Contains(err.Error(), "UpdateByFilter is not implemented for Qdrant") || updated != 0 {
				t.Fatalf("unsupported Qdrant batch update count=%d error=%v", updated, err)
			}
			for _, store := range []vectorstore.VectorStore{f.sourceKnowledge, f.targetKnowledge} {
				actual, actualEmbedding, err := store.Get(f.ctx, "keep")
				if err != nil || !reflect.DeepEqual(actual, original) || !reflect.DeepEqual(actualEmbedding, embedding) {
					t.Fatalf("unsupported update changed source or target: doc=%+v embedding=%v err=%v", actual, actualEmbedding, err)
				}
			}
			updatedDoc := original.Clone()
			updatedDoc.Content = "updated"
			if err := f.knowledge.Add(f.ctx, updatedDoc, embedding); err != nil {
				t.Fatal(err)
			}
			for _, store := range []vectorstore.VectorStore{f.sourceKnowledge, f.targetKnowledge} {
				actual, actualEmbedding, err := store.Get(f.ctx, "keep")
				if err != nil || actual == nil || actual.Content != "updated" || !reflect.DeepEqual(actualEmbedding, embedding) {
					t.Fatalf("late upsert was not mirrored: doc=%+v embedding=%v err=%v", actual, actualEmbedding, err)
				}
			}
			if err := f.knowledge.DeleteByFilter(f.ctx, vectorstore.WithDeleteFilter(map[string]any{"kind": "remove"})); err != nil {
				t.Fatal(err)
			}
			f.advance(t, id, datamigration.PhaseRollbackWindow)
			for _, removed := range []string{"remove-a", "remove-b"} {
				assertOnlineDocument(t, f.ctx, f.targetKnowledge, removed, false)
			}
			addOnlineDocument(t, f.ctx, f.knowledge, "after-cutover", "retain")
			assertOnlineDocument(t, f.ctx, f.targetKnowledge, "after-cutover", true)
			if finish == "rollback" {
				if _, err := f.runtime.Coordinator.Rollback(f.ctx, id, "integration-operator", "verify no lost writes"); err != nil {
					t.Fatal(err)
				}
				assertOnlineDocument(t, f.ctx, f.sourceKnowledge, "after-cutover", true)
			} else {
				if _, err := f.runtime.Coordinator.Complete(f.ctx, id, "integration-operator", "accept target"); err != nil {
					t.Fatal(err)
				}
				addOnlineDocument(t, f.ctx, f.knowledge, "target-only", "retain")
				assertOnlineDocument(t, f.ctx, f.targetKnowledge, "target-only", true)
				assertOnlineDocument(t, f.ctx, f.sourceKnowledge, "target-only", false)
				if err := f.knowledge.Delete(f.ctx, "target-only"); err != nil {
					t.Fatal(err)
				}
				assertOnlineDocument(t, f.ctx, f.targetKnowledge, "target-only", false)
			}
		})
	}
}

func TestOnlineArtifactMigrationPreservesVersionsAndPropagatesTombstones(t *testing.T) {
	for _, finish := range []string{"rollback", "complete"} {
		t.Run(finish, func(t *testing.T) {
			f := newOnlineDataPlaneFixture(t)
			info := artifact.SessionInfo{AppName: "support", UserID: "user", SessionID: "session"}
			save := func(file, body string) int {
				t.Helper()
				version, err := f.artifacts.SaveArtifact(f.ctx, info, file, &artifact.Artifact{MimeType: "text/plain", Data: []byte(body)})
				if err != nil {
					t.Fatal(err)
				}
				return version
			}
			for _, body := range []string{"v0", "v1"} {
				save("report.txt", body)
			}
			save("cycle.txt", "old")
			save("deleted-before.txt", "removed")
			if err := f.artifacts.DeleteArtifact(f.ctx, info, "deleted-before.txt"); err != nil {
				t.Fatal(err)
			}
			id, err := f.create(t, datamigration.DomainArtifact, "artifact-target")
			if err != nil {
				t.Fatal(err)
			}
			f.advance(t, id, datamigration.PhaseValidate)
			if _, err := f.artifacts.SaveArtifact(f.ctx, info, "retry-mime", &artifact.Artifact{MimeType: "text/plain"}); err == nil {
				t.Fatal("empty artifact unexpectedly saved")
			}
			if version, err := f.artifacts.SaveArtifact(f.ctx, info, "retry-mime", &artifact.Artifact{MimeType: "application/json", Data: []byte("{}")}); err != nil || version != 0 {
				t.Fatalf("retry with different MIME version=%d err=%v", version, err)
			}
			save("report.txt", "v2")
			save("interrupted-delete.txt", "remove after recovery")
			interrupted, err := dataprojection.NewArtifactTombstone(info, "interrupted-delete.txt", 0, "text/plain", 1)
			if err != nil {
				t.Fatal(err)
			}
			// Reproduce a process loss after durable intent and SQL tombstone,
			// before either bucket's object deletion has completed.
			tx, err := f.db.BeginTx(f.ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			if _, err := tx.ExecContext(f.ctx, `INSERT INTO data_migration_live_intents(migration_id,key_hash,record_key)
				SELECT migration_id,key_hash,record_key FROM data_migration_live_journal
				WHERE migration_id=$1 AND record_key=$2 ORDER BY sequence DESC LIMIT 1`, id, interrupted.Key); err != nil {
				t.Fatal(err)
			}
			var interruptedKey string
			if err := tx.QueryRowContext(f.ctx, `UPDATE artifact_versions SET deleted_at=clock_timestamp()
				WHERE tenant_id=$1 AND filename='interrupted-delete.txt' AND version=0 RETURNING object_key`, f.tenantID).Scan(&interruptedKey); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if _, err := f.minio.StatObject(f.ctx, f.sourceBucket, interruptedKey, minio.StatObjectOptions{}); err != nil {
				t.Fatalf("source object missing before recovery: %v", err)
			}
			if value, err := f.artifacts.LoadArtifact(f.ctx, info, "interrupted-delete.txt", nil); err != nil || value != nil {
				t.Fatalf("read failed to recover interrupted source deletion: %v %v", value, err)
			}
			for _, bucket := range []string{f.sourceBucket, f.targetBucket} {
				if _, err := f.minio.StatObject(f.ctx, bucket, interruptedKey, minio.StatObjectOptions{}); minio.ToErrorResponse(err).Code != "NoSuchKey" {
					t.Fatalf("recovery did not delete object from %s: %v", bucket, err)
				}
			}
			if err := f.artifacts.DeleteArtifact(f.ctx, info, "cycle.txt"); err != nil {
				t.Fatal(err)
			}
			if version := save("cycle.txt", "new"); version != 1 {
				t.Fatalf("recreated version=%d", version)
			}
			f.advance(t, id, datamigration.PhaseRollbackWindow)
			save("report.txt", "v3")
			for version := 0; version < 4; version++ {
				value, err := f.targetArtifacts.LoadArtifact(f.ctx, info, "report.txt", &version)
				if err != nil || value == nil || string(value.Data) != fmt.Sprintf("v%d", version) {
					t.Fatalf("target history version %d: value=%v err=%v", version, value, err)
				}
			}
			zero := 0
			if value, err := f.targetArtifacts.LoadArtifact(f.ctx, info, "cycle.txt", &zero); err != nil || value != nil {
				t.Fatalf("deleted original version visible: %v %v", value, err)
			}
			if finish == "rollback" {
				if _, err := f.runtime.Coordinator.Rollback(f.ctx, id, "integration-operator", "verify object rollback"); err != nil {
					t.Fatal(err)
				}
				version := 3
				value, err := f.artifacts.LoadArtifact(f.ctx, info, "report.txt", &version)
				if err != nil || value == nil || string(value.Data) != "v3" {
					t.Fatalf("rollback lost latest version: %v %v", value, err)
				}
			} else {
				if _, err := f.runtime.Coordinator.Complete(f.ctx, id, "integration-operator", "accept object target"); err != nil {
					t.Fatal(err)
				}
				version := save("target-only.txt", "new target data")
				value, err := f.targetArtifacts.LoadArtifact(f.ctx, info, "target-only.txt", &version)
				if err != nil || value == nil {
					t.Fatalf("cached writer missed target: %v", err)
				}
				var objectKey string
				if err := f.db.QueryRowContext(f.ctx, `SELECT object_key FROM artifact_versions WHERE tenant_id=$1 AND filename='target-only.txt'`, f.tenantID).Scan(&objectKey); err != nil {
					t.Fatal(err)
				}
				if _, err := f.minio.StatObject(f.ctx, f.sourceBucket, objectKey, minio.StatObjectOptions{}); err == nil {
					t.Fatal("completed migration still wrote old bucket")
				}
				if err := f.artifacts.DeleteArtifact(f.ctx, info, "target-only.txt"); err != nil {
					t.Fatal(err)
				}
				if _, err := f.minio.StatObject(f.ctx, f.targetBucket, objectKey, minio.StatObjectOptions{}); err == nil {
					t.Fatal("target-only deletion retained body")
				}
			}
		})
	}
}
