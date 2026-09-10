package storage

import (
	"context"
	"io"
	"time"
)

// MemoryRecord is a tenant-scoped durable memory entry. Content and metadata
// are copied at repository boundaries so callers never share mutable state.
type MemoryRecord struct {
	TenantID  string
	MemoryID  string
	UserID    string
	SessionID string
	Content   string
	Topics    []string
	Metadata  map[string]any
	Embedding []float64
	Version   int64
	DeletedAt *time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

// MemoryInput contains the caller-selected fields for a memory write.
type MemoryInput struct {
	TenantID  string
	MemoryID  string
	UserID    string
	SessionID string
	Content   string
	Topics    []string
	Metadata  map[string]any
	Embedding []float64
}

// MemorySearchResult is one memory ordered by relevance.
type MemorySearchResult struct {
	Memory MemoryRecord
	Score  float64
}

// MemoryStore is the tenant-scoped memory contract. Implementations may make
// vector indexing asynchronous, but PutMemory is durable before it returns.
type MemoryStore interface {
	PutMemory(context.Context, MemoryInput) (MemoryRecord, error)
	GetMemory(context.Context, string, string) (MemoryRecord, error)
	ListMemories(context.Context, string, string, int) ([]MemoryRecord, error)
	SearchMemories(context.Context, string, string, string, int) ([]MemorySearchResult, error)
	DeleteMemory(context.Context, string, string) error
	EnqueueMemoryIndex(context.Context, MemoryRecord) error
}

// SummaryRecord is the latest summary for one tenant/session/filter branch.
type SummaryRecord struct {
	TenantID  string
	SessionID string
	FilterKey string
	Text      string
	EventSeq  int64
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// SummaryStore persists versioned, filter-aware summaries.
type SummaryStore interface {
	PutSummary(context.Context, SummaryRecord) (SummaryRecord, error)
	GetSummary(context.Context, string, string, string) (SummaryRecord, error)
	EnqueueSummary(context.Context, SummaryRecord) error
}

// KnowledgeDocument is the source-of-truth document metadata used by RAG.
// Embeddings are optional because vector indexing can be asynchronous.
type KnowledgeDocument struct {
	TenantID   string
	DocumentID string
	Content    string
	Metadata   map[string]any
	Embedding  []float64
	Version    int64
	Digest     string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// KnowledgeSearchResult is one document ordered by vector relevance.
type KnowledgeSearchResult struct {
	Document KnowledgeDocument
	Score    float64
}

// KnowledgeStore is the tenant-scoped knowledge source contract.
type KnowledgeStore interface {
	PutKnowledge(context.Context, KnowledgeDocument) (KnowledgeDocument, error)
	GetKnowledge(context.Context, string, string) (KnowledgeDocument, error)
	SearchKnowledge(context.Context, string, []float64, int) ([]KnowledgeSearchResult, error)
	DeleteKnowledge(context.Context, string, string) error
}

// ArtifactRecord is the durable metadata and content for a tenant artifact.
type ArtifactRecord struct {
	TenantID   string
	ArtifactID string
	SessionID  string
	Name       string
	MimeType   string
	Content    []byte
	Version    int64
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// ArtifactStore persists tenant-scoped binary artifacts.
type ArtifactStore interface {
	PutArtifact(context.Context, ArtifactRecord) (ArtifactRecord, error)
	GetArtifact(context.Context, string, string) (ArtifactRecord, error)
	ListArtifacts(context.Context, string, string) ([]ArtifactRecord, error)
	DeleteArtifact(context.Context, string, string) error
}

// AuditRecord is an append-only tenant audit fact. Payload must be redacted by
// the caller before it reaches a store.
type AuditRecord struct {
	TenantID   string
	AuditID    string
	EventType  string
	Payload    map[string]any
	OccurredAt time.Time
}

// AuditStore is the tenant-scoped append/read audit contract.
type AuditStore interface {
	AppendAudit(context.Context, AuditRecord) (AuditRecord, error)
	ListAudit(context.Context, string, time.Time, int) ([]AuditRecord, error)
}

// VectorSource identifies the producer namespace of an indexed vector.
//
// A document identifier is only unique within a source namespace; keeping the
// namespace explicit prevents memory and knowledge records with the same ID
// from overwriting one another.
type VectorSource string

const (
	// VectorSourceGeneric is the default namespace for callers that do not
	// need a more specific source.
	VectorSourceGeneric VectorSource = "generic"
	// VectorSourceMemory contains projections of durable memory records.
	VectorSourceMemory VectorSource = "memory"
	// VectorSourceKnowledge contains projections of knowledge documents.
	VectorSourceKnowledge VectorSource = "knowledge"
)

// VectorRecord is an indexed vector document. The source and tenant remain
// part of the key even when the underlying provider supports namespaces.
type VectorRecord struct {
	TenantID   string
	Source     VectorSource
	DocumentID string
	Content    string
	Metadata   map[string]any
	Embedding  []float64
	Version    int64
	UpdatedAt  time.Time
}

// VectorSearchResult is one vector result and its similarity score.
type VectorSearchResult struct {
	Record VectorRecord
	Score  float64
}

// VectorStore is the minimal tenant-scoped vector adapter contract.
type VectorStore interface {
	UpsertVector(context.Context, VectorRecord) error
	SearchVectors(context.Context, string, []float64, int) ([]VectorSearchResult, error)
	DeleteVector(context.Context, string, string) error
	Close() error
}

// ObjectInfo describes an object stored outside SQL row state.
type ObjectInfo struct {
	TenantID    string
	ObjectKey   string
	ContentType string
	Size        int64
	ETag        string
	CreatedAt   time.Time
}

// ObjectStore is a tenant-scoped object storage adapter contract.
type ObjectStore interface {
	PutObject(context.Context, string, string, io.Reader, string) (ObjectInfo, error)
	GetObject(context.Context, string, string) (io.ReadCloser, ObjectInfo, error)
	DeleteObject(context.Context, string, string) error
	Close() error
}
