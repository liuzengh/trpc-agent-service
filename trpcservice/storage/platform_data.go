package storage

import (
	"context"
	"time"
)

// KnowledgeDocument is the platform-owned management projection for one
// framework knowledge source. Searchable content and embeddings live only in
// the framework VectorStore; this projection tracks lifecycle and display
// metadata for the console.
type KnowledgeDocument struct {
	TenantID, AppCode, DocumentID string
	Name                          string
	Status                        string
	TotalChunks                   int
	Metadata                      map[string]string
	UpdatedAt                     time.Time
}

// KnowledgeProjectionStore owns only the platform management projection. RAG
// retrieval is intentionally absent from this interface and is provided by
// trpc-agent-go knowledge.Knowledge instead.
type KnowledgeProjectionStore interface {
	ListKnowledgeDocuments(context.Context, string, string) ([]KnowledgeDocument, error)
	DeleteKnowledgeDocument(context.Context, string, string, string) error
}
