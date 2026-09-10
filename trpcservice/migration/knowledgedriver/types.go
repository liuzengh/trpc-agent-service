// Package knowledgedriver migrates immutable Knowledge chunks between exact
// tenant backend bindings.
package knowledgedriver

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
)

const Domain = "knowledge"

type Operation string
type MutationState string
type MutationDirection string

const (
	OperationUpsert Operation = "upsert"
	OperationDelete Operation = "delete"

	MutationPending  MutationState = "pending"
	MutationApplying MutationState = "applying"
	MutationApplied  MutationState = "applied"

	DirectionForward MutationDirection = "forward"
	DirectionReverse MutationDirection = "reverse"
)

type ChunkKey struct {
	TenantID         string `json:"tenant_id"`
	KnowledgeID      string `json:"knowledge_id"`
	KnowledgeVersion int64  `json:"knowledge_version"`
	ChunkID          string `json:"chunk_id"`
}

// ChunkImage is backend-neutral. Digests fix the ingestion facts while the
// content, metadata, and vector let an adapter reconstruct the target record.
type ChunkImage struct {
	Key                ChunkKey          `json:"key"`
	Revision           int64             `json:"revision"`
	Operation          Operation         `json:"operation"`
	SourceDigest       string            `json:"source_digest"`
	ContentDigest      string            `json:"content_digest"`
	MetadataDigest     string            `json:"metadata_digest"`
	EmbeddingProfileID string            `json:"embedding_profile_id"`
	EmbeddingVersion   int64             `json:"embedding_version"`
	VectorGeneration   string            `json:"vector_generation"`
	Content            string            `json:"content,omitempty"`
	Metadata           map[string]string `json:"metadata,omitempty"`
	Vector             []float32         `json:"vector,omitempty"`
}

type Mutation struct {
	TenantID, MigrationID, MutationID string
	Epoch                             int64
	Direction                         MutationDirection
	Key                               ChunkKey
	Operation                         Operation
	SourceRevision                    int64
	MutationDigest                    string
	State                             MutationState
	Attempt                           int
	LeaseOwner                        string
	LeaseUntil, NotBefore             time.Time
	LastErrorClass                    string
	TargetRevision                    int64
	TargetDigest                      string
	CreatedAt, AppliedAt              time.Time
	Version                           int64
}

type RecordRequest struct {
	TenantID, MigrationID, MutationID string
	Epoch                             int64
	ConfigVersion                     int64
	Direction                         MutationDirection
	Key                               ChunkKey
	Operation                         Operation
	SourceRevision                    int64
	MutationDigest                    string
	CreatedAt                         time.Time
}

type ClaimRequest struct {
	TenantID, MigrationID, WorkerID string
	Limit                           int
	Now                             time.Time
	Lease                           time.Duration
}

type CompleteRequest struct {
	TenantID, MigrationID, MutationID, WorkerID string
	Key                                         ChunkKey
	ExpectedVersion, TargetRevision             int64
	TargetDigest                                string
	At                                          time.Time
}

type RetryRequest struct {
	TenantID, MigrationID, MutationID, WorkerID string
	Key                                         ChunkKey
	ExpectedVersion                             int64
	ErrorClass                                  string
	NotBefore, At                               time.Time
}

type MutationLedger interface {
	Claim(context.Context, ClaimRequest) ([]Mutation, error)
	MarkApplied(context.Context, CompleteRequest) (Mutation, error)
	MarkRetry(context.Context, RetryRequest) (Mutation, error)
	Outstanding(context.Context, string, string) (int64, error)
}

type Recorder interface {
	Record(context.Context, RecordRequest) (Mutation, error)
}

type SnapshotReader interface {
	LoadChunk(context.Context, ChunkKey) (ChunkImage, error)
}

type ApplyRequest struct {
	TenantID, MigrationID, MutationID string
	Epoch                             int64
	Image                             ChunkImage
	ImageDigest                       string
}

type ApplyResult struct {
	Revision int64
	Digest   string
}

// ReplicaWriter must reject a MutationID collision and never let an older
// revision replace a newer chunk or tombstone.
type ReplicaWriter interface {
	ApplyChunk(context.Context, ApplyRequest) (ApplyResult, error)
}

type PageRequest struct {
	TenantID, SnapshotWatermark, After string
	Limit                              int
}

type Page struct {
	Chunks         []ChunkImage
	NextCheckpoint string
	Complete       bool
}

type BackfillSource interface {
	PageChunks(context.Context, PageRequest) (Page, error)
}

type Fingerprint struct {
	Count, DivergenceCount int64
	Digest, Watermark      string
}

type Inventory interface {
	Fingerprint(context.Context, string, string) (Fingerprint, error)
}

type Probe struct {
	ProbeID          string
	TenantID         string
	KnowledgeID      string
	KnowledgeVersion int64
	Query            string
	Expected         []ChunkKey
	MinRecallPPM     int64
}

type ProbeSource interface {
	Probes(context.Context, string, string) ([]Probe, error)
}

type SearchRequest struct {
	TenantID, KnowledgeID string
	KnowledgeVersion      int64
	Query                 string
}

type SearchTarget interface {
	Search(context.Context, SearchRequest) ([]ChunkKey, error)
}

type Driver struct {
	Authority                        migration.Repository
	Ledger                           MutationLedger
	Source                           SnapshotReader
	Backfill                         BackfillSource
	Target                           ReplicaWriter
	ReverseSource                    SnapshotReader
	ReverseReplica                   ReplicaWriter
	SourceInventory, TargetInventory Inventory
	ProbeSource                      ProbeSource
	SearchTarget                     SearchTarget
}

type RepairRequest struct {
	TenantID, MigrationID, WorkerID string
	Limit                           int
	Now                             time.Time
	Lease, RetryDelay               time.Duration
}

type RepairResult struct{ Claimed, Applied, Retried int }

type BackfillRequest struct {
	TenantID, MigrationID string
	Limit                 int
	At                    time.Time
}
