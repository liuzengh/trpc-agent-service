package knowledge

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/knowledgedriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

// ManifestState is the durable ingestion/publish state of a Knowledge version.
type ManifestState string

const (
	ManifestStaging   ManifestState = "staging"
	ManifestIndexing  ManifestState = "indexing"
	ManifestVerifying ManifestState = "verifying"
	ManifestPublished ManifestState = "published"
	ManifestFailed    ManifestState = "failed"
)

// Manifest is the ingestion task entry and published-version authority. Its
// identity fields are immutable; state advances forward only through the
// staging -> indexing -> verifying -> published machine.
type Manifest struct {
	TenantID    string
	KnowledgeID string
	Version     int64

	SourceURI                  string
	SourceDigest               string
	ChunkingPipelineVersion    string
	EmbedderProfileID          string
	EmbedderVersion            int64
	VectorCollectionGeneration string
	MetadataSchema             []string
	ContentWatermark           string

	State              ManifestState
	ChunkTotal         int64
	VerificationDigest string

	CreatedAt     time.Time
	UpdatedAt     time.Time
	RecordVersion int64
}

// ChunkRecord is the durable authority for one ingested chunk. Source/content/
// metadata digests must already match the payload before storage; the store
// refuses NaN/Inf vectors and cross-tenant coordinates.
type ChunkRecord struct {
	TenantID         string
	KnowledgeID      string
	KnowledgeVersion int64
	ChunkID          string

	SourceDigest   string
	ContentDigest  string
	MetadataDigest string

	EmbeddingProfileID string
	EmbeddingVersion   int64
	VectorGeneration   string

	Content  string
	Metadata map[string]string
	Vector   []float32

	IndexedAt time.Time
	CreatedAt time.Time
}

// ProbeRecord is the durable authority for one retrieval sample probe.
type ProbeRecord struct {
	TenantID         string
	KnowledgeID      string
	KnowledgeVersion int64
	ProbeID          string

	Query          string
	ExpectedChunks []string
	MinRecallPPM   int64
	Verified       bool
	CreatedAt      time.Time
}

// BeginManifestInput is the immutable identity of a new ingestion task.
type BeginManifestInput struct {
	TenantID     string
	KnowledgeID  string
	Version      int64
	SourceURI    string
	SourceDigest string

	ChunkingPipelineVersion    string
	EmbedderProfileID          string
	EmbedderVersion            int64
	VectorCollectionGeneration string
	MetadataSchema             []string
	ContentWatermark           string
	CreatedAt                  time.Time
}

// IngestionStore is the durable ingestion/version-publish surface. Every
// implementation must enforce the same state machine: identity immutable,
// transitions forward only, chunk-total frozen once set, publish blocked until
// every chunk is indexed and every probe is verified.
type IngestionStore interface {
	BeginManifest(context.Context, BeginManifestInput) (Manifest, error)
	StageChunk(context.Context, ChunkRecord) (ChunkRecord, error)
	BeginIndexing(context.Context, string, string, int64, int64, time.Time) (Manifest, error)
	MarkChunkIndexed(context.Context, string, string, int64, string, time.Time) error
	BeginVerifying(context.Context, string, string, int64, string, time.Time) (Manifest, error)
	RecordProbe(context.Context, ProbeRecord) (ProbeRecord, error)
	MarkProbeVerified(context.Context, string, string, int64, string) error
	PublishVersion(context.Context, string, string, int64, time.Time) (Manifest, error)
	FailVersion(context.Context, string, string, int64, time.Time) (Manifest, error)
	GetManifest(context.Context, string, string, int64) (Manifest, error)
}

// ChunkImage converts a ChunkRecord into the migration-side canonical image
// shared with the backend-migration driver. Ingested chunks are the first
// revision of an upsert; later overwrites are a separate concern.
func (c ChunkRecord) ChunkImage() knowledgedriver.ChunkImage {
	return knowledgedriver.ChunkImage{
		Key: knowledgedriver.ChunkKey{
			TenantID:         c.TenantID,
			KnowledgeID:      c.KnowledgeID,
			KnowledgeVersion: c.KnowledgeVersion,
			ChunkID:          c.ChunkID,
		},
		Revision:           1,
		Operation:          knowledgedriver.OperationUpsert,
		SourceDigest:       c.SourceDigest,
		ContentDigest:      c.ContentDigest,
		MetadataDigest:     c.MetadataDigest,
		EmbeddingProfileID: c.EmbeddingProfileID,
		EmbeddingVersion:   c.EmbeddingVersion,
		VectorGeneration:   c.VectorGeneration,
		Content:            c.Content,
		Metadata:           c.Metadata,
		Vector:             c.Vector,
	}
}

// MutationDigest returns the canonical image digest used for the forward
// mutation intent recorded during an active migration. It shares the migration
// driver's digest algorithm so ingestion and migration agree on one chunk.
func (c ChunkRecord) MutationDigest() (string, error) {
	digest, err := knowledgedriver.ImageDigest(c.ChunkImage())
	if err != nil {
		return "", err
	}
	return digest, nil
}

// VerificationDigest is the scope-reconciliation digest of a chunk set. It is
// computed at verify time and recomputed before publish; a mismatch means the
// candidate set drifted and publish must fail closed.
func VerificationDigest(chunks []ChunkRecord) (string, error) {
	if len(chunks) == 0 {
		return "", runtime.ErrInvariantViolation
	}
	digests := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		digest, err := chunk.MutationDigest()
		if err != nil {
			return "", err
		}
		digests = append(digests, digest)
	}
	sort.Strings(digests)
	return hashDigests(digests...), nil
}

func hashDigests(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = fmt.Fprintf(hash, "%d:", len(part))
		_, _ = hash.Write([]byte(part))
	}
	return hex.EncodeToString(hash.Sum(nil))
}
