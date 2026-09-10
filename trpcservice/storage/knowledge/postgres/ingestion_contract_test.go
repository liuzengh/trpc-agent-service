package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	knowledgedriverpg "github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/knowledge/contracttest"
)

// TestIngestionPostgreSQL16 runs the shared ingestion contract against a real
// PostgreSQL 16 instance using the seeded tenant from the migration fixture.
func TestIngestionPostgreSQL16(t *testing.T) {
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("requires explicit disposable PostgreSQL migration test")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var databaseName string
	var major int
	if err := db.QueryRow(`SELECT current_database(),current_setting('server_version_num')::int/10000`).Scan(&databaseName, &major); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(databaseName, "trpc_agent_service_test_") || major != 16 {
		t.Fatalf("refusing database=%q PostgreSQL=%d", databaseName, major)
	}
	tenantID := "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	contracttest.Suite(t, New(db), tenantID)
	assertVerifiedProbeSource(t, db, tenantID)
	assertAgentRevisionRejectsUnpublishedKnowledge(t, db, tenantID)
}

func assertVerifiedProbeSource(t *testing.T, db *sql.DB, tenantID string) {
	t.Helper()
	probes, err := knowledgedriverpg.NewProbeSource(db).Probes(context.Background(), tenantID, "qdrant-snapshot-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, probe := range probes {
		if probe.KnowledgeID == "kb6" && probe.KnowledgeVersion == 1 && probe.ProbeID == "kb6:1:p1" && len(probe.Expected) == 1 && probe.Expected[0].ChunkID == "c1" {
			return
		}
	}
	t.Fatalf("verified durable probe missing: %#v", probes)
}

func assertAgentRevisionRejectsUnpublishedKnowledge(t *testing.T, db *sql.DB, tenantID string) {
	t.Helper()
	now := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)
	store := New(db)
	ctx := context.Background()
	if _, err := store.BeginManifest(ctx, knowledge.BeginManifestInput{
		TenantID: tenantID, KnowledgeID: "kb-unpublished", Version: 1, SourceURI: "file:///kb/unpublished",
		SourceDigest: strings.Repeat("a", 64), ChunkingPipelineVersion: "chunk-v1", EmbedderProfileID: "embedder-a",
		EmbedderVersion: 1, VectorCollectionGeneration: "gen-1", MetadataSchema: []string{"title"}, ContentWatermark: "w1", CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	const appID = "app_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	if _, err := db.ExecContext(ctx, `INSERT INTO public.agent_app_revision(
tenant_id,agent_app_id,revision,state,draft_version,agent_kind,schema_version,instruction,model_profile_id,model_profile_version)
VALUES($1,$2,2,'draft',1,'llm',1,'probe','model',1)`, tenantID, appID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO public.agent_app_revision_knowledge(
tenant_id,agent_app_id,revision,knowledge_id,knowledge_version) VALUES($1,$2,2,'kb-unpublished',1)`, tenantID, appID); err != nil {
		t.Fatal(err)
	}
	_, err := db.ExecContext(ctx, `UPDATE public.agent_app_revision
SET state='published',content_digest=repeat('b',64),published_at=now()
WHERE tenant_id=$1 AND agent_app_id=$2 AND revision=2`, tenantID, appID)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("unpublished knowledge reference was accepted: %v", err)
	}
}
