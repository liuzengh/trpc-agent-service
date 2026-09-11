package postgresadapter

import (
	"context"
	"encoding/json"
	"errors"
	controleventsv1 "github.com/liuzengh/trpc-agent-service/api/events/control/v1"
	deploymentv1 "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/deployment/domain"
	"strings"
	"testing"
	"time"
)

type captureManifestPublisher struct {
	payload []byte
	id      string
	err     error
}

func (p *captureManifestPublisher) PublishManifest(_ context.Context, id string, raw []byte) error {
	p.id = id
	p.payload = append([]byte(nil), raw...)
	return p.err
}
func TestManifestDistributionAgainstPostgreSQL(t *testing.T) {
	store, pool := deploymentPostgres(t)
	ctx := context.Background()
	key := []byte(strings.Repeat("k", 32))
	empty, err := application.ExportManifestPage(ctx, store, "", 1, key)
	if err != nil || !empty.Complete || empty.SnapshotUpper != nil || len(empty.Events) != 0 {
		t.Fatalf("empty export: %#v %v", empty, err)
	}
	create := createCommitFixture()
	create.Deployment.ID = "dep_1"
	create.Receipt.DeploymentID = "dep_1"
	create.Receipt.Result, _ = json.Marshal(createReceiptResult{Deployment: create.Deployment})
	if _, err = store.CreateDeployment(ctx, create); err != nil {
		t.Fatal(err)
	}
	commit := publicationCommitFixture()
	if _, err = store.CommitPublication(ctx, commit); err != nil {
		t.Fatal(err)
	}
	first, found, err := store.ClaimManifest(ctx)
	if err != nil || !found {
		t.Fatalf("claim: %v %v", found, err)
	}
	if _, again, err := store.ClaimManifest(ctx); err != nil || again {
		t.Fatalf("concurrent claim acquired: %v %v", again, err)
	}
	if err = store.FinishManifest(ctx, first, false, "MANIFEST_PUBLISH_UNAVAILABLE", time.Second); err != nil {
		t.Fatal(err)
	}
	if err = store.FinishManifest(ctx, first, true, "", 0); !errors.Is(err, application.ErrManifestOutboxLeaseLost) {
		t.Fatalf("stale claim accepted: %v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE control_outbox SET available_at=clock_timestamp()-interval '1 second' WHERE id='evt_1'`); err != nil {
		t.Fatal(err)
	}
	publisher := &captureManifestPublisher{}
	relay, err := application.NewManifestRelay(store, publisher)
	if err != nil {
		t.Fatal(err)
	}
	if found, err = relay.Step(ctx); err != nil || !found {
		t.Fatalf("relay: %v %v", found, err)
	}
	event, err := controleventsv1.DecodeRuntimeManifestPublishedEvent(publisher.payload)
	if err != nil || event.EventID != "evt_1" {
		t.Fatalf("published event: %v", err)
	}
	var status string
	pool.QueryRow(ctx, `SELECT status FROM control_outbox WHERE id='evt_1'`).Scan(&status)
	if status != "PUBLISHED" {
		t.Fatalf("status %s", status)
	}
	page, err := application.ExportManifestPage(ctx, store, "", 1, key)
	if err != nil || !page.Complete || page.SnapshotUpper == nil || len(page.Events) != 1 || page.NextCursor != "" {
		t.Fatalf("export: %#v %v", page, err)
	}
	// Add another immutable publication event to test stable paging independently
	// of Deployment CAS. The same original manifest remains the exported truth.
	event.EventID = "evt_2"
	event.DeploymentID = "dep_2"
	event.Manifest.DeploymentID = "dep_2"
	event.DeploymentRevisionID = "dpr_2"
	event.Manifest.DeploymentRevisionID = "dpr_2"
	raw, _ := json.Marshal(event)
	digest, err := controleventsv1.ManifestEventDigest(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO control_outbox(tenant_id,id,aggregate_type,aggregate_id,aggregate_revision,event_type,schema_version,payload_jsonb,payload_digest,status,attempt_count,available_at,created_at,updated_at) VALUES('tnt_1','evt_2','deployment','dep_2',1,'RuntimeManifestPublished.v1','v1',$1::jsonb,$2,'PENDING',0,clock_timestamp(),clock_timestamp(),clock_timestamp())`, raw, digest); err != nil {
		t.Fatal(err)
	}
	page, err = application.ExportManifestPage(ctx, store, "", 1, key)
	if err != nil || page.Complete || page.NextCursor == "" || len(page.Events) != 1 {
		t.Fatalf("first page: %#v %v", page, err)
	}
	next, err := application.ExportManifestPage(ctx, store, page.NextCursor, 1, key)
	if err != nil || !next.Complete || len(next.Events) != 1 || !next.SnapshotUpper.CreatedAt.Equal(page.SnapshotUpper.CreatedAt) || next.SnapshotUpper.EventID != page.SnapshotUpper.EventID || next.SnapshotUpper.TenantID != page.SnapshotUpper.TenantID {
		t.Fatalf("next page: %#v %v", next, err)
	}
	if _, err = application.ExportManifestPage(ctx, store, page.NextCursor+"tampered", 1, key); !errors.Is(err, application.ErrManifestExportCursor) {
		t.Fatalf("cursor tampering accepted: %v", err)
	}
}

func TestWorkerV1PublicationTransactionAgainstPostgreSQL(t *testing.T) {
	store, _ := deploymentPostgres(t)
	ctx := context.Background()
	create := createCommitFixture()
	create.Deployment.ID = "dep_1"
	create.Receipt.DeploymentID = "dep_1"
	create.Receipt.Result, _ = json.Marshal(createReceiptResult{Deployment: create.Deployment})
	if _, err := store.CreateDeployment(ctx, create); err != nil {
		t.Fatal(err)
	}
	contract := domain.WorkerV1PlatformExecutionContract()
	commit := publicationCommitFixtureWithPlatform(contract)
	result, err := store.CommitPublication(ctx, commit)
	if err != nil || !result.Created {
		t.Fatalf("new contract publication: %v", err)
	}
	published, err := store.GetPublishedRevision(ctx, "tnt_1", "dep_1", 1)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(published.Manifest)
	m, err := deploymentv1.DecodeRuntimeManifest(raw)
	if err != nil {
		t.Fatal(err)
	}
	c, err := deploymentv1.VerifyRuntimeManifest(m)
	if err != nil {
		t.Fatal(err)
	}
	if err = deploymentv1.ValidateWorkerV1(c, contract.Digest); err != nil {
		t.Fatal(err)
	}
	claim, found, err := store.ClaimManifest(ctx)
	if err != nil || !found {
		t.Fatalf("outbox: %v", err)
	}
	event, err := controleventsv1.DecodeRuntimeManifestPublishedEvent(claim.Payload)
	if err != nil || event.Manifest.ContentDigest != published.ManifestDigest {
		t.Fatalf("publication identity: %v", err)
	}
}
