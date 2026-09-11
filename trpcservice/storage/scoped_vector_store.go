package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

const (
	knowledgeTenantMetadataKey   = "tenant_id"
	knowledgeAppMetadataKey      = "app_code"
	knowledgeParentMetadataKey   = "parent_document_id"
	knowledgeJobMetadataKey      = "ingest_job_id"
	knowledgeOwnerMetadataKey    = "ingest_owner"
	knowledgeVectorTable         = "knowledge_vectors"
	knowledgeEmbeddingDimensions = 1536
)

// scopedVectorStore is the only platform adapter around the framework vector
// store. It makes tenant/application scope structural, while delegating every
// persistence and retrieval operation to trpc-agent-go.
type scopedVectorStore struct {
	delegate      vectorstore.VectorStore
	prefix        string
	scopeMetadata map[string]any
	writeMetadata map[string]any
}

func newScopedVectorStore(delegate vectorstore.VectorStore, tenantID, appCode string, metadata map[string]any) (*scopedVectorStore, error) {
	if delegate == nil {
		return nil, errors.New("framework vector store is required")
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(appCode) == "" {
		return nil, errors.New("knowledge tenant and application are required")
	}
	hash := sha256.Sum256([]byte(tenantID + "\x00" + appCode))
	scope := map[string]any{knowledgeTenantMetadataKey: tenantID, knowledgeAppMetadataKey: appCode}
	writes := make(map[string]any, len(scope)+len(metadata))
	for key, value := range scope {
		writes[key] = value
	}
	for key, value := range metadata {
		writes[key] = value
	}
	return &scopedVectorStore{
		delegate: delegate, prefix: hex.EncodeToString(hash[:12]) + ":",
		scopeMetadata: scope, writeMetadata: writes,
	}, nil
}

func (s *scopedVectorStore) Add(ctx context.Context, doc *document.Document, embedding []float64) error {
	return s.delegate.Add(ctx, s.scopedDocument(doc), embedding)
}

func (s *scopedVectorStore) Get(ctx context.Context, id string) (*document.Document, []float64, error) {
	doc, embedding, err := s.delegate.Get(ctx, s.scopeID(id))
	return s.unscopedDocument(doc), embedding, err
}

func (s *scopedVectorStore) Update(ctx context.Context, doc *document.Document, embedding []float64) error {
	return s.delegate.Update(ctx, s.scopedDocument(doc), embedding)
}

func (s *scopedVectorStore) Delete(ctx context.Context, id string) error {
	return s.delegate.Delete(ctx, s.scopeID(id))
}

func (s *scopedVectorStore) Search(ctx context.Context, query *vectorstore.SearchQuery) (*vectorstore.SearchResult, error) {
	if query == nil {
		return s.delegate.Search(ctx, nil)
	}
	copyQuery := *query
	if query.Filter == nil {
		copyQuery.Filter = &vectorstore.SearchFilter{Metadata: s.scopedMetadata(nil)}
	} else {
		filter := *query.Filter
		filter.IDs = s.scopeIDs(query.Filter.IDs)
		filter.Metadata = s.scopedMetadata(query.Filter.Metadata)
		copyQuery.Filter = &filter
	}
	result, err := s.delegate.Search(ctx, &copyQuery)
	if err != nil || result == nil {
		return result, err
	}
	for _, scored := range result.Results {
		if scored != nil {
			scored.Document = s.unscopedDocument(scored.Document)
		}
	}
	return result, nil
}

func (s *scopedVectorStore) DeleteByFilter(ctx context.Context, opts ...vectorstore.DeleteOption) error {
	config := vectorstore.ApplyDeleteOptions(opts...)
	return s.delegate.DeleteByFilter(ctx,
		vectorstore.WithDeleteDocumentIDs(s.scopeIDs(config.DocumentIDs)),
		vectorstore.WithDeleteFilter(s.scopedMetadata(config.Filter)),
		vectorstore.WithDeleteAll(config.DeleteAll),
	)
}

func (s *scopedVectorStore) UpdateByFilter(ctx context.Context, opts ...vectorstore.UpdateByFilterOption) (int64, error) {
	config, err := vectorstore.ApplyUpdateByFilterOptions(opts...)
	if err != nil {
		return 0, err
	}
	if len(config.DocumentIDs) == 0 {
		return 0, errors.New("scoped vector updates require document IDs")
	}
	updates := make(map[string]any, len(config.Updates)+len(s.writeMetadata))
	for key, value := range config.Updates {
		updates[key] = value
	}
	for key, value := range s.writeMetadata {
		updates["metadata."+key] = value
	}
	return s.delegate.UpdateByFilter(ctx,
		vectorstore.WithUpdateByFilterDocumentIDs(s.scopeIDs(config.DocumentIDs)),
		vectorstore.WithUpdateByFilterCondition(config.FilterCondition),
		vectorstore.WithUpdateByFilterUpdates(updates),
	)
}

func (s *scopedVectorStore) Count(ctx context.Context, opts ...vectorstore.CountOption) (int, error) {
	config := vectorstore.ApplyCountOptions(opts...)
	return s.delegate.Count(ctx, vectorstore.WithCountFilter(s.scopedMetadata(config.Filter)))
}

func (s *scopedVectorStore) GetMetadata(ctx context.Context, opts ...vectorstore.GetMetadataOption) (map[string]vectorstore.DocumentMetadata, error) {
	config, err := vectorstore.ApplyGetMetadataOptions(opts...)
	if err != nil {
		return nil, err
	}
	options := []vectorstore.GetMetadataOption{
		vectorstore.WithGetMetadataIDs(s.scopeIDs(config.IDs)),
		vectorstore.WithGetMetadataFilter(s.scopedMetadata(config.Filter)),
	}
	if config.Limit > 0 {
		options = append(options, vectorstore.WithGetMetadataLimit(config.Limit), vectorstore.WithGetMetadataOffset(config.Offset))
	}
	metadata, err := s.delegate.GetMetadata(ctx, options...)
	if err != nil {
		return nil, err
	}
	result := make(map[string]vectorstore.DocumentMetadata, len(metadata))
	for id, item := range metadata {
		result[s.unscopeID(id)] = item
	}
	return result, nil
}

func (s *scopedVectorStore) Close() error { return s.delegate.Close() }

func (s *scopedVectorStore) scopedDocument(doc *document.Document) *document.Document {
	if doc == nil {
		return nil
	}
	copyDoc := *doc
	copyDoc.ID = s.scopeID(doc.ID)
	copyDoc.Metadata = make(map[string]any, len(doc.Metadata)+len(s.writeMetadata))
	for key, value := range doc.Metadata {
		copyDoc.Metadata[key] = value
	}
	for key, value := range s.writeMetadata {
		copyDoc.Metadata[key] = value
	}
	return &copyDoc
}

func (s *scopedVectorStore) unscopedDocument(doc *document.Document) *document.Document {
	if doc == nil {
		return nil
	}
	copyDoc := *doc
	copyDoc.ID = s.unscopeID(doc.ID)
	return &copyDoc
}

func (s *scopedVectorStore) scopeID(id string) string {
	return s.prefix + strings.TrimPrefix(id, s.prefix)
}

func (s *scopedVectorStore) unscopeID(id string) string { return strings.TrimPrefix(id, s.prefix) }

func (s *scopedVectorStore) scopeIDs(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	result := make([]string, len(ids))
	for index, id := range ids {
		result[index] = s.scopeID(id)
	}
	return result
}

func (s *scopedVectorStore) scopedMetadata(metadata map[string]any) map[string]any {
	result := make(map[string]any, len(metadata)+len(s.scopeMetadata))
	for key, value := range metadata {
		result[key] = value
	}
	for key, value := range s.scopeMetadata {
		result[key] = value
	}
	return result
}

var _ vectorstore.VectorStore = (*scopedVectorStore)(nil)
