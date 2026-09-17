// Package contracttest runs the shared Knowledge ingestion state machine
// contract against any IngestionStore implementation.
package contracttest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/knowledge"
)

var base = time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)

// Suite runs the ingestion/version-publish contract against any IngestionStore
// implementation. tenantID must reference an existing tenant for stores that
// enforce a tenant foreign key (the PostgreSQL implementation).
func Suite(t *testing.T, store knowledge.IngestionStore, tenantID string) {
	t.Helper()
	// The PostgreSQL store records migration intents and correctly rejects an
	// intent older than the active migration. Keep this suite's fixture clock in
	// the future of the setup migration while retaining deterministic offsets.
	base = time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)
	ctx := context.Background()

	t.Run("manifest idempotent and collision", func(t *testing.T) {
		in := manifestInput(tenantID, "kb1", 1)
		first, err := store.BeginManifest(ctx, in)
		if err != nil {
			t.Fatal(err)
		}
		again, err := store.BeginManifest(ctx, in)
		if err != nil || !reflect.DeepEqual(again, first) {
			t.Fatalf("replay: again=%+v err=%v", again, err)
		}
		clashing := in
		clashing.SourceDigest = "a" + in.SourceDigest[1:]
		if _, err := store.BeginManifest(ctx, clashing); !errors.Is(err, runtime.ErrIdempotencyCollision) {
			t.Fatalf("collision: got %v", err)
		}
	})

	t.Run("chunk idempotent and cross-tenant rejected", func(t *testing.T) {
		if _, err := store.BeginManifest(ctx, manifestInput(tenantID, "kb2", 1)); err != nil {
			t.Fatal(err)
		}
		chunk := chunkRecord(tenantID, "kb2", 1, "c1")
		if _, err := store.StageChunk(ctx, chunk); err != nil {
			t.Fatal(err)
		}
		if _, err := store.StageChunk(ctx, chunk); err != nil {
			t.Fatalf("chunk replay: %v", err)
		}
		foreign := chunk
		foreign.TenantID = "other"
		if _, err := store.StageChunk(ctx, foreign); !errors.Is(err, runtime.ErrNotFound) && !errors.Is(err, runtime.ErrTenantScope) && !errors.Is(err, runtime.ErrInvariantViolation) {
			t.Fatalf("cross-tenant chunk: got %v", err)
		}
	})

	t.Run("indexing requires full chunk set", func(t *testing.T) {
		if _, err := store.BeginManifest(ctx, manifestInput(tenantID, "kb3", 1)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.StageChunk(ctx, chunkRecord(tenantID, "kb3", 1, "c1")); err != nil {
			t.Fatal(err)
		}
		if _, err := store.BeginIndexing(ctx, tenantID, "kb3", 1, 2, base.Add(time.Hour)); !errors.Is(err, runtime.ErrVersionConflict) {
			t.Fatalf("under-count indexing: got %v", err)
		}
	})

	t.Run("verifying requires all chunks indexed", func(t *testing.T) {
		if _, err := store.BeginManifest(ctx, manifestInput(tenantID, "kb4", 1)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.StageChunk(ctx, chunkRecord(tenantID, "kb4", 1, "c1")); err != nil {
			t.Fatal(err)
		}
		if _, err := store.BeginIndexing(ctx, tenantID, "kb4", 1, 1, base.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.BeginVerifying(ctx, tenantID, "kb4", 1, verificationDigest(tenantID, "kb4", 1), base.Add(2*time.Hour)); !errors.Is(err, runtime.ErrVersionConflict) {
			t.Fatalf("unindexed verifying: got %v", err)
		}
	})

	t.Run("publish requires all probes verified", func(t *testing.T) {
		if _, err := store.BeginManifest(ctx, manifestInput(tenantID, "kb5", 1)); err != nil {
			t.Fatal(err)
		}
		chunk := chunkRecord(tenantID, "kb5", 1, "c1")
		if _, err := store.StageChunk(ctx, chunk); err != nil {
			t.Fatal(err)
		}
		if _, err := store.BeginIndexing(ctx, tenantID, "kb5", 1, 1, base.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := store.MarkChunkIndexed(ctx, tenantID, "kb5", 1, "c1", base.Add(90*time.Minute)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.BeginVerifying(ctx, tenantID, "kb5", 1, verificationDigest(tenantID, "kb5", 1), base.Add(2*time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.PublishVersion(ctx, tenantID, "kb5", 1, base.Add(3*time.Hour)); !errors.Is(err, runtime.ErrVersionConflict) {
			t.Fatalf("zero-probe publish: got %v", err)
		}
		if _, err := store.RecordProbe(ctx, probeRecord(tenantID, "kb5", 1, "p1")); err != nil {
			t.Fatal(err)
		}
		if _, err := store.PublishVersion(ctx, tenantID, "kb5", 1, base.Add(3*time.Hour)); !errors.Is(err, runtime.ErrVersionConflict) {
			t.Fatalf("unverified publish: got %v", err)
		}
	})

	t.Run("full lifecycle publishes", func(t *testing.T) {
		if _, err := store.BeginManifest(ctx, manifestInput(tenantID, "kb6", 1)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.StageChunk(ctx, chunkRecord(tenantID, "kb6", 1, "c1")); err != nil {
			t.Fatal(err)
		}
		if _, err := store.BeginIndexing(ctx, tenantID, "kb6", 1, 1, base.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if err := store.MarkChunkIndexed(ctx, tenantID, "kb6", 1, "c1", base.Add(90*time.Minute)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.BeginVerifying(ctx, tenantID, "kb6", 1, verificationDigest(tenantID, "kb6", 1), base.Add(2*time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.RecordProbe(ctx, probeRecord(tenantID, "kb6", 1, "p1")); err != nil {
			t.Fatal(err)
		}
		if err := store.MarkProbeVerified(ctx, tenantID, "kb6", 1, "p1"); err != nil {
			t.Fatal(err)
		}
		published, err := store.PublishVersion(ctx, tenantID, "kb6", 1, base.Add(3*time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		if published.State != knowledge.ManifestPublished {
			t.Fatalf("state=%s", published.State)
		}
		if _, err := store.PublishVersion(ctx, tenantID, "kb6", 1, base.Add(4*time.Hour)); !errors.Is(err, runtime.ErrVersionConflict) {
			t.Fatalf("double publish: got %v", err)
		}
	})

	t.Run("index and probe state gates cannot be bypassed", func(t *testing.T) {
		if _, err := store.BeginManifest(ctx, manifestInput(tenantID, "kb8", 1)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.StageChunk(ctx, chunkRecord(tenantID, "kb8", 1, "c1")); err != nil {
			t.Fatal(err)
		}
		if err := store.MarkChunkIndexed(ctx, tenantID, "kb8", 1, "c1", base.Add(time.Hour)); !errors.Is(err, runtime.ErrVersionConflict) {
			t.Fatalf("staging indexed: got %v", err)
		}
		if _, err := store.RecordProbe(ctx, probeRecord(tenantID, "kb8", 1, "p1")); !errors.Is(err, runtime.ErrVersionConflict) {
			t.Fatalf("staging probe: got %v", err)
		}
	})

	t.Run("state machine advances forward only", func(t *testing.T) {
		if _, err := store.BeginManifest(ctx, manifestInput(tenantID, "kb7", 1)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.StageChunk(ctx, chunkRecord(tenantID, "kb7", 1, "c1")); err != nil {
			t.Fatal(err)
		}
		if _, err := store.FailVersion(ctx, tenantID, "kb7", 1, base.Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.BeginIndexing(ctx, tenantID, "kb7", 1, 1, base.Add(2*time.Hour)); !errors.Is(err, runtime.ErrVersionConflict) {
			t.Fatalf("failed->indexing: got %v", err)
		}
	})
}

func manifestInput(tenantID, knowledgeID string, version int64) knowledge.BeginManifestInput {
	return knowledge.BeginManifestInput{
		TenantID: tenantID, KnowledgeID: knowledgeID, Version: version,
		SourceURI: "file:///kb/" + knowledgeID, SourceDigest: digestString("source-" + knowledgeID),
		ChunkingPipelineVersion: "chunk-v1", EmbedderProfileID: "embedder-a", EmbedderVersion: 3,
		VectorCollectionGeneration: "gen-1", MetadataSchema: []string{"title"},
		ContentWatermark: "w1", CreatedAt: base,
	}
}

func chunkRecord(tenantID, knowledgeID string, version int64, chunkID string) knowledge.ChunkRecord {
	content := "content-of-" + chunkID
	metadata := map[string]string{"title": "t-" + chunkID}
	return knowledge.ChunkRecord{
		TenantID: tenantID, KnowledgeID: knowledgeID, KnowledgeVersion: version, ChunkID: chunkID,
		SourceDigest: digestString("source-" + knowledgeID), ContentDigest: digestString(content),
		MetadataDigest:     digestString(metadataJSON(metadata)),
		EmbeddingProfileID: "embedder-a", EmbeddingVersion: 3, VectorGeneration: "gen-1",
		Content: content, Metadata: metadata, Vector: []float32{0.1, 0.2, 0.3}, CreatedAt: base,
	}
}

func probeRecord(tenantID, knowledgeID string, version int64, probeID string) knowledge.ProbeRecord {
	return knowledge.ProbeRecord{
		TenantID: tenantID, KnowledgeID: knowledgeID, KnowledgeVersion: version, ProbeID: probeID,
		Query: "query-" + probeID, ExpectedChunks: []string{"c1"}, MinRecallPPM: 500000, CreatedAt: base,
	}
}

func metadataJSON(metadata map[string]string) string {
	raw, _ := json.Marshal(metadata)
	return string(raw)
}

func digestString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func verificationDigest(tenantID, knowledgeID string, version int64) string {
	digest, err := knowledge.VerificationDigest([]knowledge.ChunkRecord{chunkRecord(tenantID, knowledgeID, version, "c1")})
	if err != nil {
		panic(err)
	}
	return digest
}
